//! Rust-owned event runtime with a stable, extensible C callback ABI.
//!
//! Rust owns event ordering, time, market state and matching. Foreign strategy code
//! (including Numba) only receives callbacks with one context pointer; it never owns the
//! event loop.

use std::{ffi::c_void, ptr::NonNull};

use crate::{market_data::Bar, types::Event};

pub const STRATEGY_ABI_VERSION: u32 = 5;
pub const EVENT_SLOT_COUNT: usize = 32;

/// Stable callback identifiers. New event kinds must use a previously unused number;
/// existing values are ABI and data-log compatibility commitments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum StrategyEventKind {
    Start = 0,
    Order = 1,
    Filled = 2,
    Position = 3,
    Funding = 4,
    Bar = 5,
    Tick = 6,
    Timer = 7,
    Error = 8,
    Stop = 9,
}

impl StrategyEventKind {
    #[inline(always)]
    pub const fn slot(self) -> usize {
        self as usize
    }
}

/// Flat fill payload suitable for zero-copy foreign access.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct FillEvent {
    pub asset_no: u64,
    pub order_id: u64,
    pub exch_ts: i64,
    pub local_ts: i64,
    pub price: f64,
    pub qty: f64,
    pub side: i8,
    pub maker: u8,
    pub _reserved: [u8; 6],
}

/// Flat order-response payload. One entry corresponds to one response received by the
/// local engine; partial fills are never collapsed into an order snapshot.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct OrderEvent {
    pub asset_no: u64,
    pub order_id: u64,
    pub exch_ts: i64,
    pub local_ts: i64,
    pub price: f64,
    pub qty: f64,
    pub exec_price: f64,
    pub exec_qty: f64,
    pub side: i8,
    pub status: u8,
    pub request: u8,
    pub maker: u8,
    pub _reserved: [u8; 4],
}

/// Read-only top-of-book state refreshed by Rust before every market callback.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
#[repr(C)]
pub struct MarketState {
    pub best_bid: f64,
    pub best_ask: f64,
    pub best_bid_qty: f64,
    pub best_ask_qty: f64,
    pub tick_size: f64,
    pub lot_size: f64,
}

pub const ORDER_COMMAND_SUBMIT: u8 = 1;
pub const ORDER_COMMAND_CANCEL: u8 = 2;

/// Preallocated command written by a foreign callback and consumed synchronously by Rust
/// after the callback returns. Modification is intentionally absent; use cancel/replace.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct OrderCommand {
    pub kind: u8,
    pub side: i8,
    pub time_in_force: u8,
    pub order_type: u8,
    pub _reserved: [u8; 4],
    pub asset_no: u64,
    pub order_id: u64,
    pub price: f64,
    pub qty: f64,
}

impl Default for OrderCommand {
    fn default() -> Self {
        Self {
            kind: 0,
            side: 0,
            time_in_force: 0,
            order_type: 0,
            _reserved: [0; 4],
            asset_no: 0,
            order_id: 0,
            price: 0.0,
            qty: 0.0,
        }
    }
}

/// One asset's bar in a global same-timeframe close batch.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct BarItem {
    pub asset_no: u64,
    pub bar: Bar,
}

/// Bar input record used by materialized Bar feeds and manifests.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct TimedBarItem {
    pub asset_no: u64,
    pub timeframe_ns: i64,
    pub bar: Bar,
}

/// Read-only ring metadata exposed to foreign callbacks.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct BarHistoryView {
    pub asset_no: u64,
    pub timeframe_ns: i64,
    pub bars_ptr: *const Bar,
    pub capacity: usize,
    pub len: usize,
    pub next: usize,
}

/// One normalized market event in a global TickBatch.
#[derive(Debug, Clone, PartialEq)]
#[repr(C)]
pub struct TickItem {
    pub asset_no: u64,
    pub event: Event,
}

/// Context passed as the single callback argument.
///
/// All pointer-backed views are read-only and valid only until the callback returns.
/// Calling back into `advance` or retaining a view across callbacks is invalid. `generation`
/// changes before every callback and can be checked by debug bindings.
#[derive(Debug)]
#[repr(C)]
pub struct StrategyRuntimeContext {
    pub abi_version: u32,
    pub struct_size: u32,
    pub event_kind: u32,
    pub stop_requested: u32,
    /// Runtime delivery clock. Backtests normally use local/delivery time; live mode uses
    /// the runtime's current local clock.
    pub now: i64,
    pub generation: u64,
    pub user_data: *mut c_void,
    pub bot_ptr: *mut c_void,

    pub ticks_ptr: *const TickItem,
    pub num_ticks: usize,
    pub bars_ptr: *const BarItem,
    pub num_bars: usize,
    pub bar_timeframe_ns: i64,
    pub bar_close_ts: i64,
    pub fills_ptr: *const FillEvent,
    pub num_fills: usize,
    pub orders_ptr: *const OrderEvent,
    pub num_orders: usize,
    pub histories_ptr: *const BarHistoryView,
    pub num_histories: usize,

    /// Event-specific POD payload for current and future event kinds.
    pub payload_ptr: *const c_void,
    pub payload_len: usize,
    pub state_f64_ptr: *mut f64,
    pub state_f64_len: usize,
    pub state_i64_ptr: *mut i64,
    pub state_i64_len: usize,
    pub commands_ptr: *mut OrderCommand,
    pub num_commands: usize,
    pub command_capacity: usize,
    pub positions_ptr: *const f64,
    pub num_positions: usize,
    pub markets_ptr: *const MarketState,
    pub num_markets: usize,
    pub last_error: i64,
}

impl Default for StrategyRuntimeContext {
    fn default() -> Self {
        Self {
            abi_version: STRATEGY_ABI_VERSION,
            struct_size: size_of::<Self>() as u32,
            event_kind: StrategyEventKind::Start as u32,
            stop_requested: 0,
            now: 0,
            generation: 0,
            user_data: std::ptr::null_mut(),
            bot_ptr: std::ptr::null_mut(),
            ticks_ptr: std::ptr::null(),
            num_ticks: 0,
            bars_ptr: std::ptr::null(),
            num_bars: 0,
            bar_timeframe_ns: 0,
            bar_close_ts: 0,
            fills_ptr: std::ptr::null(),
            num_fills: 0,
            orders_ptr: std::ptr::null(),
            num_orders: 0,
            histories_ptr: std::ptr::null(),
            num_histories: 0,
            payload_ptr: std::ptr::null(),
            payload_len: 0,
            state_f64_ptr: std::ptr::null_mut(),
            state_f64_len: 0,
            state_i64_ptr: std::ptr::null_mut(),
            state_i64_len: 0,
            commands_ptr: std::ptr::null_mut(),
            num_commands: 0,
            command_capacity: 0,
            positions_ptr: std::ptr::null(),
            num_positions: 0,
            markets_ptr: std::ptr::null(),
            num_markets: 0,
            last_error: 0,
        }
    }
}

impl StrategyRuntimeContext {
    #[inline]
    pub fn request_stop(&mut self) {
        self.stop_requested = 1;
    }

    fn clear_views(&mut self) {
        self.ticks_ptr = std::ptr::null();
        self.num_ticks = 0;
        self.bars_ptr = std::ptr::null();
        self.num_bars = 0;
        self.bar_timeframe_ns = 0;
        self.bar_close_ts = 0;
        self.fills_ptr = std::ptr::null();
        self.num_fills = 0;
        self.orders_ptr = std::ptr::null();
        self.num_orders = 0;
        self.payload_ptr = std::ptr::null();
        self.payload_len = 0;
    }
}

/// Native callback generated by a Numba `cfunc` bridge or supplied by another native
/// strategy implementation. Zero means success; non-zero requests an error stop.
pub type StrategyCallback = unsafe extern "C" fn(*mut StrategyRuntimeContext) -> i32;

#[derive(Clone)]
pub struct CallbackRegistry {
    callbacks: [Option<StrategyCallback>; EVENT_SLOT_COUNT],
}

impl Default for CallbackRegistry {
    fn default() -> Self {
        Self {
            callbacks: [None; EVENT_SLOT_COUNT],
        }
    }
}

impl CallbackRegistry {
    pub fn set(&mut self, kind: StrategyEventKind, callback: StrategyCallback) {
        self.callbacks[kind.slot()] = Some(callback);
    }

    pub fn set_custom(
        &mut self,
        event_id: u32,
        callback: StrategyCallback,
    ) -> Result<(), RuntimeError> {
        let slot = event_id as usize;
        if slot >= EVENT_SLOT_COUNT {
            return Err(RuntimeError::InvalidEventId(event_id));
        }
        self.callbacks[slot] = Some(callback);
        Ok(())
    }

    fn get(&self, event_id: u32) -> Option<StrategyCallback> {
        self.callbacks.get(event_id as usize).copied().flatten()
    }
}

/// Borrowed event payload returned by a Rust event source.
pub enum RuntimePayload<'a> {
    None,
    Ticks(&'a [TickItem]),
    Bars {
        timeframe_ns: i64,
        close_ts: i64,
        bars: &'a [BarItem],
    },
    Fills(&'a [FillEvent]),
    Orders(&'a [OrderEvent]),
    Pod {
        ptr: NonNull<c_void>,
        len: usize,
    },
}

pub struct RuntimeEvent<'a> {
    pub kind: u32,
    pub now: i64,
    pub payload: RuntimePayload<'a>,
}

/// Rust-owned source/scheduler contract. Implementations own their event queue, clock,
/// market state and matching engine. Events yielded at the same logical time must already
/// follow the documented deterministic priority.
pub trait RuntimeEventSource {
    type Error: std::error::Error + Send + Sync + 'static;

    fn next_event(&mut self) -> Result<Option<RuntimeEvent<'_>>, Self::Error>;

    /// Called after the foreign callback returns. Bar sources commit the current close to
    /// history here, which makes `history[-1]` mean the preceding bar during `on_bar`.
    fn after_callback(
        &mut self,
        _kind: u32,
        _ctx: &mut StrategyRuntimeContext,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("invalid strategy event id {0}")]
    InvalidEventId(u32),
    #[error("strategy callback for event {event_id} returned error code {code}")]
    Callback { event_id: u32, code: i32 },
    #[error("event source failed: {0}")]
    Source(String),
}

/// Runs an event source entirely under Rust ownership. Foreign code is entered only for
/// registered event callbacks and always receives the same single context pointer.
pub fn run_event_runtime<S: RuntimeEventSource>(
    source: &mut S,
    callbacks: &CallbackRegistry,
    ctx: &mut StrategyRuntimeContext,
) -> Result<(), RuntimeError> {
    ctx.clear_views();
    let start_result = dispatch(StrategyEventKind::Start as u32, callbacks, ctx).and_then(|()| {
        source
            .after_callback(StrategyEventKind::Start as u32, ctx)
            .map_err(|error| RuntimeError::Source(error.to_string()))
    });

    let result = match start_result {
        Err(error) => Err(error),
        Ok(()) => loop {
            if ctx.stop_requested != 0 {
                break Ok(());
            }
            let event = match source.next_event() {
                Ok(event) => event,
                Err(error) => break Err(RuntimeError::Source(error.to_string())),
            };
            let Some(event) = event else {
                break Ok(());
            };
            populate_context(ctx, &event);
            let kind = event.kind;
            if let Err(error) = dispatch(kind, callbacks, ctx) {
                break Err(error);
            }
            if let Err(error) = source.after_callback(kind, ctx) {
                break Err(RuntimeError::Source(error.to_string()));
            }
        },
    };

    // Give the strategy one native error notification before stopping. The original error
    // remains authoritative if on_error itself fails.
    let error_hook = if result.is_err() {
        if ctx.last_error == 0 {
            ctx.last_error = -1;
        }
        ctx.clear_views();
        dispatch(StrategyEventKind::Error as u32, callbacks, ctx).and_then(|()| {
            source
                .after_callback(StrategyEventKind::Error as u32, ctx)
                .map_err(|error| RuntimeError::Source(error.to_string()))
        })
    } else {
        Ok(())
    };

    // Stop is guaranteed exactly once even when an earlier callback failed.
    ctx.clear_views();
    let stop_result = dispatch(StrategyEventKind::Stop as u32, callbacks, ctx);
    let stop_hook = source
        .after_callback(StrategyEventKind::Stop as u32, ctx)
        .map_err(|error| RuntimeError::Source(error.to_string()));
    result.and(error_hook).and(stop_result).and(stop_hook)
}

fn populate_context(ctx: &mut StrategyRuntimeContext, event: &RuntimeEvent<'_>) {
    ctx.clear_views();
    ctx.event_kind = event.kind;
    ctx.now = event.now;
    match &event.payload {
        RuntimePayload::None => {}
        RuntimePayload::Ticks(ticks) => {
            ctx.ticks_ptr = ticks.as_ptr();
            ctx.num_ticks = ticks.len();
        }
        RuntimePayload::Bars {
            timeframe_ns,
            close_ts,
            bars,
        } => {
            ctx.bars_ptr = bars.as_ptr();
            ctx.num_bars = bars.len();
            ctx.bar_timeframe_ns = *timeframe_ns;
            ctx.bar_close_ts = *close_ts;
        }
        RuntimePayload::Fills(fills) => {
            ctx.fills_ptr = fills.as_ptr();
            ctx.num_fills = fills.len();
        }
        RuntimePayload::Orders(orders) => {
            ctx.orders_ptr = orders.as_ptr();
            ctx.num_orders = orders.len();
        }
        RuntimePayload::Pod { ptr, len } => {
            ctx.payload_ptr = ptr.as_ptr();
            ctx.payload_len = *len;
        }
    }
}

fn dispatch(
    event_id: u32,
    callbacks: &CallbackRegistry,
    ctx: &mut StrategyRuntimeContext,
) -> Result<(), RuntimeError> {
    ctx.event_kind = event_id;
    ctx.generation = ctx.generation.wrapping_add(1);
    let Some(callback) = callbacks.get(event_id) else {
        return Ok(());
    };
    // Safety: registration requires the caller to provide a valid native callback with the
    // declared ABI. `ctx` remains pinned for the duration of this synchronous invocation.
    let code = unsafe { callback(ctx) };
    if code == 0 {
        Ok(())
    } else {
        ctx.last_error = code as i64;
        Err(RuntimeError::Callback { event_id, code })
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;

    #[derive(Default)]
    struct State {
        calls: Vec<u32>,
        history_commits: usize,
    }

    unsafe extern "C" fn record(ctx: *mut StrategyRuntimeContext) -> i32 {
        let ctx = unsafe { &mut *ctx };
        let state = unsafe { &mut *(ctx.user_data as *mut State) };
        state.calls.push(ctx.event_kind);
        0
    }

    unsafe extern "C" fn fail_start(ctx: *mut StrategyRuntimeContext) -> i32 {
        let ctx = unsafe { &mut *ctx };
        let state = unsafe { &mut *(ctx.user_data as *mut State) };
        state.calls.push(ctx.event_kind);
        -7
    }

    struct Source {
        events: Vec<(u32, i64)>,
        next: usize,
        state: *mut State,
    }

    impl RuntimeEventSource for Source {
        type Error = Infallible;

        fn next_event(&mut self) -> Result<Option<RuntimeEvent<'_>>, Self::Error> {
            let Some(&(kind, now)) = self.events.get(self.next) else {
                return Ok(None);
            };
            self.next += 1;
            Ok(Some(RuntimeEvent {
                kind,
                now,
                payload: RuntimePayload::None,
            }))
        }

        fn after_callback(
            &mut self,
            kind: u32,
            _ctx: &mut StrategyRuntimeContext,
        ) -> Result<(), Self::Error> {
            if kind == StrategyEventKind::Bar as u32 {
                unsafe { (*self.state).history_commits += 1 };
            }
            Ok(())
        }
    }

    #[test]
    fn rust_owns_loop_and_dispatches_extensible_callbacks() {
        let mut state = State::default();
        let mut source = Source {
            events: vec![
                (StrategyEventKind::Filled as u32, 1),
                (StrategyEventKind::Bar as u32, 2),
                (StrategyEventKind::Tick as u32, 2),
                (10, 3),
            ],
            next: 0,
            state: &mut state,
        };
        let mut callbacks = CallbackRegistry::default();
        for kind in [
            StrategyEventKind::Start,
            StrategyEventKind::Filled,
            StrategyEventKind::Bar,
            StrategyEventKind::Tick,
            StrategyEventKind::Stop,
        ] {
            callbacks.set(kind, record);
        }
        callbacks.set_custom(10, record).unwrap();
        let mut ctx = StrategyRuntimeContext {
            user_data: (&mut state as *mut State).cast(),
            ..Default::default()
        };

        run_event_runtime(&mut source, &callbacks, &mut ctx).unwrap();

        assert_eq!(
            state.calls,
            vec![
                StrategyEventKind::Start as u32,
                StrategyEventKind::Filled as u32,
                StrategyEventKind::Bar as u32,
                StrategyEventKind::Tick as u32,
                10,
                StrategyEventKind::Stop as u32,
            ]
        );
        assert_eq!(state.history_commits, 1);
    }

    #[test]
    fn stop_is_dispatched_when_start_fails() {
        let mut state = State::default();
        let mut source = Source {
            events: Vec::new(),
            next: 0,
            state: &mut state,
        };
        let mut callbacks = CallbackRegistry::default();
        callbacks.set(StrategyEventKind::Start, fail_start);
        callbacks.set(StrategyEventKind::Stop, record);
        let mut ctx = StrategyRuntimeContext {
            user_data: (&mut state as *mut State).cast(),
            ..Default::default()
        };

        assert!(run_event_runtime(&mut source, &callbacks, &mut ctx).is_err());
        assert_eq!(
            state.calls,
            vec![
                StrategyEventKind::Start as u32,
                StrategyEventKind::Stop as u32
            ]
        );
    }
}
