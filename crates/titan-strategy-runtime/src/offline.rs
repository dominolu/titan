use crate::{DepthBatchInfoV1, depth_book::HostDepthBook};
use std::{collections::BTreeMap, sync::Arc};

use crate::{
    CONTEXT_FLAG_STOP_READY, CallbackCommandStaging, EventKind, LoadError, LoadedNativeStrategy,
    ManifestCapabilities, StagedCommand, StrategyInstance, StrategyRuntimeContext,
    StrategyStateSnapshot, TitanAccountStateEventView, TitanAccountView, TitanActiveOrderView,
    TitanBalanceEventView, TitanBalanceView, TitanCancelEventView, TitanDepthView, TitanFillView,
    TitanMarketView, TitanOrderEventView, TitanPositionEventView, TitanPositionView, TitanTickView,
    TitanTimerView, public_state_identity,
};

/// Host-independent callback adapter used by deterministic backtests and replay tools.
///
/// It deliberately owns only strategy-visible projections and command staging. Matching, latency,
/// fees and exchange semantics remain the responsibility of the backtest engine. This keeps the
/// exact same signed `.titan` compiled_strategy and callback contract in offline and live execution.
pub struct OfflineAdapter {
    instance: StrategyInstance,
    staging: CallbackCommandStaging,
    markets: Vec<TitanMarketView>,
    positions: Vec<TitanPositionView>,
    balances: Vec<TitanBalanceView>,
    accounts: Vec<TitanAccountView>,
    active_orders: Vec<TitanActiveOrderView>,
    last_error_code: i32,
    stop_barrier: bool,
    recovery_order_results: bool,
    host_depth: bool,
    failed: bool,
    depth_books: BTreeMap<u32, HostDepthBook>,
    stopping: bool,
    stopped: bool,
}

pub struct OfflineConfig {
    pub compiled_strategy: Arc<LoadedNativeStrategy>,
    pub strategy_instance_id: u64,
    pub generation: u64,
    pub command_capacity: usize,
    pub markets: Vec<TitanMarketView>,
    pub positions: Vec<TitanPositionView>,
    pub balances: Vec<TitanBalanceView>,
    pub accounts: Vec<TitanAccountView>,
    pub active_orders: Vec<TitanActiveOrderView>,
    /// Frozen object table for this offline run. Offline inputs build it with the same builder as
    /// the live service; absent means the callback sees an empty table.
    pub object_table: Option<crate::ObjectTable>,
}

impl OfflineAdapter {
    pub fn new(config: OfflineConfig) -> Result<Self, LoadError> {
        let OfflineConfig {
            compiled_strategy,
            strategy_instance_id,
            generation,
            command_capacity,
            markets,
            positions,
            balances,
            accounts,
            active_orders,
            object_table,
        } = config;

        // The live runtime and the replay must publish the same deprecated unit slots; otherwise a
        // strategy that still reads `market.tick_size` sees two different worlds in two paths.
        let mut markets = markets;
        if !crate::strategy::apply_deprecated_unit_placeholders(&mut markets) {
            return Err(LoadError::Contract("deprecated_units_already_claimed"));
        }
        let mut instance = compiled_strategy.instantiate(strategy_instance_id, generation)?;
        if let Some(table) = object_table {
            instance.attach_object_table(table);
        }
        Ok(Self {
            last_error_code: 0,
            failed: false,
            host_depth: compiled_strategy
                .manifest
                .capabilities
                .contains(ManifestCapabilities::HOST_DEPTH_100),
            depth_books: BTreeMap::new(),
            stop_barrier: compiled_strategy
                .manifest
                .capabilities
                .contains(ManifestCapabilities::STOP_BARRIER),
            recovery_order_results: compiled_strategy
                .manifest
                .capabilities
                .contains(ManifestCapabilities::RECOVERY_ORDER_RESULTS),
            stopping: false,
            stopped: false,
            instance,
            staging: CallbackCommandStaging::new(
                command_capacity,
                strategy_instance_id,
                generation,
            )?,
            markets,
            positions,
            balances,
            accounts,
            active_orders,
        })
    }

    pub fn start(&mut self, now_ns: i64) -> Result<(), LoadError> {
        self.invoke(EventKind::Start, now_ns, |_| {})?;
        self.instance.start()
    }

    /// Last ABI error code reported by the strategy. Accessors set -2 when a required public view
    /// is missing, which is the fastest way to explain an offline start refusal.
    pub fn last_error_code(&self) -> i32 {
        self.last_error_code
    }

    pub fn stop(&mut self, now_ns: i64) -> Result<Vec<StagedCommand>, LoadError> {
        self.stopping = true;
        let commands = self.invoke(EventKind::Stop, now_ns, |_| {})?;
        if !self.stop_barrier {
            self.instance.stop();
            self.stopped = true;
        }
        Ok(commands)
    }

    /// Freezes the current typed state exactly as the online host does before a checkpoint.
    ///
    /// The returned snapshot carries the instance identity, generation, binding digest and state
    /// checksum, so an offline replay can only resume from a snapshot that belongs to it.
    pub fn freeze_checkpoint(
        &self,
        checkpoint_id: u64,
    ) -> Result<StrategyStateSnapshot, LoadError> {
        self.instance.freeze_state(checkpoint_id)
    }

    /// Offline counterpart of the online host's restore-then-recover sequence.
    ///
    /// Must be called on a fresh adapter (before [`Self::start`]); the instance has to be in the
    /// `Ready` state, which is where the online host restores a checkpoint. The steps mirror
    /// `NativeRuntime` exactly:
    ///
    /// 1. restore the verified snapshot bytes and identity;
    /// 2. when the strategy declares `on_recover`, invoke it with the reconciled recovery orders as
    ///    the event payload (only for `RECOVERY_ORDER_RESULTS` strategies) and then complete the
    ///    reconciliation boundary;
    /// 3. otherwise complete a plain restore.
    ///
    /// A `Recover` callback must not issue commands. The online host fails the runtime when it
    /// tries; the adapter returns the same stable error instead of staging anything.
    pub fn recover(
        &mut self,
        now_ns: i64,
        snapshot: &StrategyStateSnapshot,
        recovery_orders: &[TitanActiveOrderView],
        new_generation: u64,
    ) -> Result<(), LoadError> {
        self.instance.restore_state(snapshot)?;
        let reconciled = self.instance.has_callback(EventKind::Recover);
        if reconciled {
            let expose_orders = self.recovery_order_results;
            let commands = match self.invoke(EventKind::Recover, now_ns, |context| {
                if expose_orders {
                    context.event_payload_ptr = recovery_orders.as_ptr().cast();
                    context.event_payload_len = std::mem::size_of_val(recovery_orders) as u64;
                }
            }) {
                Ok(commands) => commands,
                Err(_) => {
                    // The online host reports this as a callback failure and fails closed. A
                    // strategy that tries to trade while the command gate is closed (the normal
                    // recovery state) surfaces here, not as a staged command.
                    self.failed = true;
                    return Err(LoadError::Contract("recovery_callback_failed"));
                }
            };
            if !commands.is_empty() || self.staging.attempted_command() {
                // Fail closed: a strategy that trades during recovery is not "recovering with a
                // warning"; later callbacks must not run as if restore succeeded.
                self.failed = true;
                return Err(LoadError::Contract("recovery_callback_attempted_command"));
            }
        }
        let identity = public_state_identity(
            &self.accounts,
            &self.active_orders,
            &self.positions,
            &self.balances,
        );
        if reconciled {
            self.instance
                .complete_reconciled_restore(identity, new_generation)?;
        } else {
            self.instance.complete_restore(identity, new_generation)?;
        }
        Ok(())
    }

    /// Barrier-enabled strategies remain callable until their final business confirmation.
    pub fn is_stopped(&self) -> bool {
        self.stopped
    }

    pub fn business_fault(&self) -> Option<u32> {
        self.instance.business_fault()
    }

    pub fn update_public_state(
        &mut self,
        markets: Vec<TitanMarketView>,
        positions: Vec<TitanPositionView>,
        balances: Vec<TitanBalanceView>,
        accounts: Vec<TitanAccountView>,
        active_orders: Vec<TitanActiveOrderView>,
    ) {
        self.markets = markets;
        self.positions = positions;
        self.balances = balances;
        self.accounts = accounts;
        self.active_orders = active_orders;
    }

    pub fn on_ticks(
        &mut self,
        now_ns: i64,
        ticks: &[TitanTickView],
    ) -> Result<Vec<StagedCommand>, LoadError> {
        self.invoke(EventKind::Tick, now_ns, |context| {
            context.ticks_ptr = ticks.as_ptr();
            context.ticks_len = ticks.len() as u64;
        })
    }

    pub fn on_depth(
        &mut self,
        now_ns: i64,
        depth: &[TitanDepthView],
    ) -> Result<Vec<StagedCommand>, LoadError> {
        if self.host_depth {
            return Err(LoadError::Contract(
                "HOST_DEPTH_100 replay requires on_depth_batch with explicit source metadata",
            ));
        }
        self.invoke(EventKind::Depth, now_ns, |context| {
            context.depth_ptr = depth.as_ptr();
            context.depth_len = depth.len() as u64;
        })
    }

    /// Replay raw depth with explicit snapshot/epoch/gap evidence. Legacy depth
    /// slices cannot prove these facts and are rejected for host-book strategies.
    pub fn on_depth_batch(
        &mut self,
        now_ns: i64,
        mut info: DepthBatchInfoV1,
        depth: &[TitanDepthView],
    ) -> Result<Vec<StagedCommand>, LoadError> {
        if !self.host_depth
            || info.version != 1
            || depth.iter().any(|item| item.asset_no != info.asset_no)
        {
            return Err(LoadError::Contract("invalid host depth replay metadata"));
        }
        let book = self.depth_books.entry(info.asset_no).or_default();
        let mut projected = Vec::with_capacity(200);
        if info.valid == 0 {
            if info.stream_epoch == 0 {
                info.stream_epoch = book.epoch();
            }
            if !book.invalidate_at(info.stream_epoch, info.publication_sequence) {
                return Ok(Vec::new());
            }
        } else {
            let snapshot = info.reset != 0;
            if !book
                .apply(&mut info, snapshot, depth, &mut projected)
                .map_err(LoadError::Contract)?
            {
                return Ok(Vec::new());
            }
        }
        self.invoke(EventKind::Depth, now_ns, |context| {
            context.depth_ptr = projected.as_ptr();
            context.depth_len = projected.len() as u64;
            context.event_payload_ptr = (&info as *const DepthBatchInfoV1).cast();
            context.event_payload_len = std::mem::size_of::<DepthBatchInfoV1>() as u64;
        })
    }

    pub fn on_fills(
        &mut self,
        now_ns: i64,
        fills: &[TitanFillView],
    ) -> Result<Vec<StagedCommand>, LoadError> {
        self.invoke(EventKind::Fill, now_ns, |context| {
            context.fills_ptr = fills.as_ptr();
            context.fills_len = fills.len() as u64;
        })
    }

    pub fn on_orders(
        &mut self,
        now_ns: i64,
        orders: &[TitanOrderEventView],
    ) -> Result<Vec<StagedCommand>, LoadError> {
        self.invoke(EventKind::Order, now_ns, |context| {
            context.order_events_ptr = orders.as_ptr();
            context.order_events_len = orders.len() as u64;
        })
    }

    pub fn on_cancels(
        &mut self,
        now_ns: i64,
        cancels: &[TitanCancelEventView],
    ) -> Result<Vec<StagedCommand>, LoadError> {
        self.invoke(EventKind::Cancel, now_ns, |context| {
            context.cancel_events_ptr = cancels.as_ptr();
            context.cancel_events_len = cancels.len() as u64;
        })
    }

    pub fn on_positions(
        &mut self,
        now_ns: i64,
        positions: &[TitanPositionEventView],
    ) -> Result<Vec<StagedCommand>, LoadError> {
        self.invoke(EventKind::Position, now_ns, |context| {
            context.position_events_ptr = positions.as_ptr();
            context.position_events_len = positions.len() as u64;
        })
    }

    pub fn on_balances(
        &mut self,
        now_ns: i64,
        balances: &[TitanBalanceEventView],
    ) -> Result<Vec<StagedCommand>, LoadError> {
        self.invoke(EventKind::Balance, now_ns, |context| {
            context.balance_events_ptr = balances.as_ptr();
            context.balance_events_len = balances.len() as u64;
        })
    }

    pub fn on_account_states(
        &mut self,
        now_ns: i64,
        states: &[TitanAccountStateEventView],
    ) -> Result<Vec<StagedCommand>, LoadError> {
        self.invoke(EventKind::AccountState, now_ns, |context| {
            context.account_state_events_ptr = states.as_ptr();
            context.account_state_events_len = states.len() as u64;
        })
    }

    pub fn on_timer(
        &mut self,
        now_ns: i64,
        timer: &TitanTimerView,
    ) -> Result<Vec<StagedCommand>, LoadError> {
        self.invoke(EventKind::Timer, now_ns, |context| {
            context.timer_ptr = timer;
            context.timer_len = 1;
        })
    }

    pub fn state_bytes(&self) -> &[u8] {
        self.instance.state.as_bytes()
    }

    /// Typed state decoded from the last callback. Offline fixtures use unused `i64` slots as
    /// progress markers, so a failed start can report *which* step stopped it instead of only the
    /// bare callback code.
    pub fn state_i64(&self, byte_offset: usize) -> Option<i64> {
        let bytes = self.state_bytes();
        let end = byte_offset.checked_add(8)?;
        let raw: [u8; 8] = bytes.get(byte_offset..end)?.try_into().ok()?;
        Some(i64::from_le_bytes(raw))
    }

    fn invoke(
        &mut self,
        event: EventKind,
        now_ns: i64,
        configure: impl FnOnce(&mut StrategyRuntimeContext),
    ) -> Result<Vec<StagedCommand>, LoadError> {
        if self.failed {
            return Err(LoadError::Contract("depth_execution_failed"));
        }
        // The ABI requires an exact struct size and version; callbacks reject zeroes as invalid.
        let (markets, positions, balances, accounts, orders) = (
            &self.markets,
            &self.positions,
            &self.balances,
            &self.accounts,
            &self.active_orders,
        );
        let mut context = crate::strategy::callback_context(
            now_ns,
            event.schema_version(),
            markets,
            positions,
            balances,
            accounts,
            orders,
            self.instance.object_table(),
        );
        configure(&mut context);
        let mut commands = match crate::strategy::invoke_staged(
            &mut self.instance,
            &mut self.staging,
            &mut context,
            event,
            true,
            &self.active_orders,
            self.stopping,
        ) {
            Ok(commands) => commands,
            Err(error) => {
                self.last_error_code = context.last_error_code;
                return Err(error);
            }
        };
        self.last_error_code = context.last_error_code;
        if self.instance.business_fault().is_some() {
            self.stopping = true;
            // Keep the staged cancels that make the failure safe; refuse only new risk.
            commands.retain(|command| {
                !matches!(command, StagedCommand::Submit { request, .. } if request.reduce_only == 0)
            });
        }
        if self.host_depth {
            let ready = !self.markets.is_empty()
                && self.markets.iter().all(|m| {
                    self.depth_books
                        .get(&m.asset_no)
                        .is_some_and(|book| book.ready)
                });
            if let Err(reason) = crate::depth_execution::validate_depth_commands(
                ready,
                &commands,
                &self.positions,
                &self.active_orders,
            ) {
                self.staging.clear_committed();
                self.failed = true;
                return Err(LoadError::Contract(reason));
            }
        }
        self.staging.clear_committed();
        if self.stopping
            && self.stop_barrier
            && context.flags & CONTEXT_FLAG_STOP_READY != 0
            && commands.is_empty()
            && self
                .active_orders
                .iter()
                .all(|order| matches!(order.status, 4 | 6..=8))
        {
            self.instance.stop();
            self.stopped = true;
        }
        Ok(commands)
    }
}
