use std::sync::Arc;

use crate::{
    CallbackCommandStagingV13, StagedCommandV13, StrategyArtifactV13, StrategyInstanceV13,
    StrategyRuntimeContextV13, TitanAccountStateEventView, TitanAccountView, TitanActiveOrderView,
    TitanBalanceEventView, TitanBalanceView, TitanCancelEventView, TitanDepthView, TitanFillView,
    TitanMarketView, TitanOrderEventView, TitanPositionEventView, TitanPositionView, TitanTickView,
    TitanTimerView, V13EventKind, V13LoadError,
};

/// Host-independent ABI V13 callback adapter used by deterministic backtests and replay tools.
///
/// It deliberately owns only strategy-visible projections and command staging. Matching, latency,
/// fees and exchange semantics remain the responsibility of the backtest engine. This keeps the
/// exact same signed `.titan` artifact and callback contract in offline and live execution.
pub struct OfflineV13Adapter {
    instance: StrategyInstanceV13,
    staging: CallbackCommandStagingV13,
    markets: Vec<TitanMarketView>,
    positions: Vec<TitanPositionView>,
    balances: Vec<TitanBalanceView>,
    accounts: Vec<TitanAccountView>,
    active_orders: Vec<TitanActiveOrderView>,
}

pub struct OfflineV13Config {
    pub artifact: Arc<StrategyArtifactV13>,
    pub strategy_instance_id: u64,
    pub generation: u64,
    pub command_capacity: usize,
    pub markets: Vec<TitanMarketView>,
    pub positions: Vec<TitanPositionView>,
    pub balances: Vec<TitanBalanceView>,
    pub accounts: Vec<TitanAccountView>,
    pub active_orders: Vec<TitanActiveOrderView>,
}

impl OfflineV13Adapter {
    pub fn new(config: OfflineV13Config) -> Result<Self, V13LoadError> {
        let OfflineV13Config {
            artifact,
            strategy_instance_id,
            generation,
            command_capacity,
            markets,
            positions,
            balances,
            accounts,
            active_orders,
        } = config;

        Ok(Self {
            instance: artifact.instantiate(strategy_instance_id, generation)?,
            staging: CallbackCommandStagingV13::new(
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

    pub fn start(&mut self, now_ns: i64) -> Result<(), V13LoadError> {
        self.invoke(V13EventKind::Start, now_ns, |_| {})?;
        self.instance.start()
    }

    pub fn stop(&mut self, now_ns: i64) -> Result<Vec<StagedCommandV13>, V13LoadError> {
        let commands = self.invoke(V13EventKind::Stop, now_ns, |_| {})?;
        self.instance.stop();
        Ok(commands)
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
    ) -> Result<Vec<StagedCommandV13>, V13LoadError> {
        self.invoke(V13EventKind::Tick, now_ns, |context| {
            context.ticks_ptr = ticks.as_ptr();
            context.ticks_len = ticks.len() as u64;
        })
    }

    pub fn on_depth(
        &mut self,
        now_ns: i64,
        depth: &[TitanDepthView],
    ) -> Result<Vec<StagedCommandV13>, V13LoadError> {
        self.invoke(V13EventKind::Depth, now_ns, |context| {
            context.depth_ptr = depth.as_ptr();
            context.depth_len = depth.len() as u64;
        })
    }

    pub fn on_fills(
        &mut self,
        now_ns: i64,
        fills: &[TitanFillView],
    ) -> Result<Vec<StagedCommandV13>, V13LoadError> {
        self.invoke(V13EventKind::Fill, now_ns, |context| {
            context.fills_ptr = fills.as_ptr();
            context.fills_len = fills.len() as u64;
        })
    }

    pub fn on_orders(
        &mut self,
        now_ns: i64,
        orders: &[TitanOrderEventView],
    ) -> Result<Vec<StagedCommandV13>, V13LoadError> {
        self.invoke(V13EventKind::Order, now_ns, |context| {
            context.order_events_ptr = orders.as_ptr();
            context.order_events_len = orders.len() as u64;
        })
    }

    pub fn on_cancels(
        &mut self,
        now_ns: i64,
        cancels: &[TitanCancelEventView],
    ) -> Result<Vec<StagedCommandV13>, V13LoadError> {
        self.invoke(V13EventKind::Cancel, now_ns, |context| {
            context.cancel_events_ptr = cancels.as_ptr();
            context.cancel_events_len = cancels.len() as u64;
        })
    }

    pub fn on_positions(
        &mut self,
        now_ns: i64,
        positions: &[TitanPositionEventView],
    ) -> Result<Vec<StagedCommandV13>, V13LoadError> {
        self.invoke(V13EventKind::Position, now_ns, |context| {
            context.position_events_ptr = positions.as_ptr();
            context.position_events_len = positions.len() as u64;
        })
    }

    pub fn on_balances(
        &mut self,
        now_ns: i64,
        balances: &[TitanBalanceEventView],
    ) -> Result<Vec<StagedCommandV13>, V13LoadError> {
        self.invoke(V13EventKind::Balance, now_ns, |context| {
            context.balance_events_ptr = balances.as_ptr();
            context.balance_events_len = balances.len() as u64;
        })
    }

    pub fn on_account_states(
        &mut self,
        now_ns: i64,
        states: &[TitanAccountStateEventView],
    ) -> Result<Vec<StagedCommandV13>, V13LoadError> {
        self.invoke(V13EventKind::AccountState, now_ns, |context| {
            context.account_state_events_ptr = states.as_ptr();
            context.account_state_events_len = states.len() as u64;
        })
    }

    pub fn on_timer(
        &mut self,
        now_ns: i64,
        timer: &TitanTimerView,
    ) -> Result<Vec<StagedCommandV13>, V13LoadError> {
        self.invoke(V13EventKind::Timer, now_ns, |context| {
            context.timer_ptr = timer;
            context.timer_len = 1;
        })
    }

    pub fn state_bytes(&self) -> &[u8] {
        self.instance.state.as_bytes()
    }

    fn invoke(
        &mut self,
        event: V13EventKind,
        now_ns: i64,
        configure: impl FnOnce(&mut StrategyRuntimeContextV13),
    ) -> Result<Vec<StagedCommandV13>, V13LoadError> {
        let mut context = crate::v13::callback_context_v13(
            now_ns,
            event.schema_version(),
            &self.markets,
            &self.positions,
            &self.balances,
            &self.accounts,
            &self.active_orders,
        );
        configure(&mut context);
        let commands = crate::v13::invoke_staged_v13(
            &mut self.instance,
            &mut self.staging,
            &mut context,
            event,
            true,
            &self.active_orders,
        )?;
        self.staging.clear_committed();
        Ok(commands)
    }
}
