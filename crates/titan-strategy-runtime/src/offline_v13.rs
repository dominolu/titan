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

impl OfflineV13Adapter {
    pub fn new(
        artifact: Arc<StrategyArtifactV13>,
        strategy_instance_id: u64,
        generation: u64,
        command_capacity: usize,
        markets: Vec<TitanMarketView>,
        positions: Vec<TitanPositionView>,
        balances: Vec<TitanBalanceView>,
        accounts: Vec<TitanAccountView>,
        active_orders: Vec<TitanActiveOrderView>,
    ) -> Result<Self, V13LoadError> {
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
        let mut context = StrategyRuntimeContextV13 {
            now_ns,
            markets_ptr: self.markets.as_ptr(),
            markets_len: self.markets.len() as u64,
            positions_ptr: self.positions.as_ptr(),
            positions_len: self.positions.len() as u64,
            balances_ptr: self.balances.as_ptr(),
            balances_len: self.balances.len() as u64,
            accounts_ptr: self.accounts.as_ptr(),
            accounts_len: self.accounts.len() as u64,
            active_orders_ptr: self.active_orders.as_ptr(),
            active_orders_len: self.active_orders.len() as u64,
            ..StrategyRuntimeContextV13::default()
        };
        configure(&mut context);
        self.staging
            .begin_callback(self.instance.command_gate_open(), &self.active_orders);
        self.staging.bind_context(&mut context);
        if let Err(error) = self.instance.invoke(event, &mut context) {
            self.staging.finish_callback(-1);
            return Err(error);
        }
        let commands = self.staging.finish_callback(0).to_vec();
        self.staging.clear_committed();
        Ok(commands)
    }
}
