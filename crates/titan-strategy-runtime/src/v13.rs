//! Standalone native Strategy ABI V13 artifact and state runtime.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Cursor, Read},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use libloading::Library;
use sha2::{Digest, Sha256};

pub const STRATEGY_ABI_V13: u32 = 13;
pub const V13_CALLBACK_COUNT: usize = 12;
pub const V13_ABI_FINGERPRINT: [u8; 32] = [
    0x5d, 0xfa, 0xa5, 0x07, 0x3d, 0x7e, 0xb2, 0x27, 0xb1, 0xdd, 0x3b, 0xf4, 0xf1, 0x61, 0x7a, 0x76,
    0xf5, 0xe3, 0x09, 0xf6, 0xfe, 0xab, 0x33, 0xe0, 0x8f, 0xf3, 0xc8, 0x07, 0x55, 0x18, 0x88, 0xd0,
];

#[derive(Clone, Debug, PartialEq)]
enum CborValue {
    Integer(i128),
    Bytes(Vec<u8>),
    Text(String),
    Array(Vec<CborValue>),
    Map(Vec<(CborValue, CborValue)>),
    Bool(bool),
    Null,
    Float(f64),
}

impl CborValue {
    fn as_text(&self) -> Option<&str> {
        if let Self::Text(value) = self {
            Some(value)
        } else {
            None
        }
    }
    fn as_bytes(&self) -> Option<&[u8]> {
        if let Self::Bytes(value) = self {
            Some(value)
        } else {
            None
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum V13CallbackCode {
    Ok = 0,
    HandlerError = -1,
    InvalidContext = -2,
    StateSchemaMismatch = -3,
    CommandError = -4,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum V13EventKind {
    Start = 0,
    Tick = 1,
    Bar = 2,
    Depth = 3,
    Fill = 4,
    Order = 5,
    Cancel = 6,
    Position = 7,
    Balance = 8,
    AccountState = 9,
    Timer = 10,
    Stop = 11,
}

impl V13EventKind {
    fn index(self) -> usize {
        self as usize
    }

    fn from_manifest_name(value: &str) -> Result<Self, V13LoadError> {
        Ok(match value {
            "bbo" => Self::Tick,
            "bar" => Self::Bar,
            "depth" => Self::Depth,
            "fill" => Self::Fill,
            "order" => Self::Order,
            "cancel" => Self::Cancel,
            "position" => Self::Position,
            "balance" => Self::Balance,
            "account_state" => Self::AccountState,
            "timer" => Self::Timer,
            _ => return Err(V13LoadError::Contract("unknown subscription event")),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum V13EventQos {
    Latest,
    ReliableOrdered,
    BestEffort,
}

impl V13EventQos {
    fn from_manifest_name(value: &str) -> Result<Self, V13LoadError> {
        Ok(match value {
            "latest" => Self::Latest,
            "reliable_ordered" => Self::ReliableOrdered,
            "best_effort" => Self::BestEffort,
            _ => return Err(V13LoadError::Contract("unknown subscription QoS")),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventSubscriptionV13 {
    pub event: V13EventKind,
    pub handler: Arc<str>,
    pub schema_version: u32,
    pub qos: V13EventQos,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StrategyCapabilitiesV13(pub u64);

impl StrategyCapabilitiesV13 {
    pub const MARKET_DATA: Self = Self(1 << 0);
    pub const ACCOUNT_DATA: Self = Self(1 << 1);
    pub const ORDER_EXECUTION: Self = Self(1 << 2);
    pub const TIMER: Self = Self(1 << 3);

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

macro_rules! v13_view {
    ($name:ident { $($field:ident: $type:ty),* $(,)? }) => {
        #[derive(Clone, Copy, Debug, Default)]
        #[repr(C)]
        pub struct $name { $(pub $field: $type),* }
    };
}

v13_view!(TitanTickView {
    asset_no: u32,
    kind: u8,
    side: u8,
    reserved: [u8; 2],
    exchange_ts_ns: i64,
    receive_ts_ns: i64,
    price_ticks: i64,
    qty_lots: i64,
    source_sequence: u64
});
v13_view!(TitanBarView {
    asset_no: u32,
    reserved: u32,
    timeframe_ns: i64,
    open_ts_ns: i64,
    close_ts_ns: i64,
    open_ticks: i64,
    high_ticks: i64,
    low_ticks: i64,
    close_ticks: i64,
    volume_lots: i64
});
v13_view!(TitanDepthView {
    asset_no: u32,
    level: u32,
    exchange_ts_ns: i64,
    receive_ts_ns: i64,
    price_ticks: i64,
    qty_lots: i64,
    source_sequence: u64,
    side: u8,
    action: u8,
    is_snapshot: u8,
    reserved: [u8; 5]
});
v13_view!(TitanFillView {
    order_id: u64,
    asset_no: u32,
    account_no: u32,
    fill_price_ticks: i64,
    fill_qty_lots: i64,
    cumulative_filled_lots: i64,
    exchange_ts_ns: i64,
    receive_ts_ns: i64,
    account_sequence: u64,
    side: u8,
    liquidity: u8,
    final_fill: u8,
    reserved: [u8; 5]
});
v13_view!(TitanOrderEventView {
    order_id: u64,
    asset_no: u32,
    account_no: u32,
    price_ticks: i64,
    qty_lots: i64,
    cumulative_filled_lots: i64,
    event_ts_ns: i64,
    account_sequence: u64,
    status: u8,
    reason: u8,
    reserved: [u8; 6]
});
v13_view!(TitanCancelEventView {
    order_id: u64,
    asset_no: u32,
    account_no: u32,
    event_ts_ns: i64,
    account_sequence: u64,
    request_result: u8,
    final_status: u8,
    reserved: [u8; 6]
});
v13_view!(TitanPositionEventView {
    asset_no: u32,
    account_no: u32,
    qty_lots: i64,
    average_price_ticks: i64,
    realized_pnl_ticks: i64,
    event_ts_ns: i64,
    account_sequence: u64
});
v13_view!(TitanBalanceEventView {
    account_no: u32,
    currency_no: u32,
    total_units: i64,
    available_units: i64,
    event_ts_ns: i64,
    account_sequence: u64
});
v13_view!(TitanAccountStateEventView {
    account_no: u32,
    reserved0: u32,
    account_epoch: u64,
    event_ts_ns: i64,
    account_sequence: u64,
    state: u8,
    reason: u8,
    reserved: [u8; 6]
});
v13_view!(TitanTimerView {
    timer_id: u64,
    scheduled_ts_ns: i64,
    fired_ts_ns: i64
});
v13_view!(TitanMarketView {
    asset_no: u32,
    flags: u32,
    best_bid_ticks: i64,
    best_bid_qty_lots: i64,
    best_ask_ticks: i64,
    best_ask_qty_lots: i64,
    tick_size: i64,
    lot_size: i64,
    source_sequence: u64
});
v13_view!(TitanPositionView {
    asset_no: u32,
    account_no: u32,
    qty_lots: i64,
    average_price_ticks: i64,
    realized_pnl_ticks: i64,
    account_sequence: u64
});
v13_view!(TitanBalanceView {
    account_no: u32,
    currency_no: u32,
    total_units: i64,
    available_units: i64,
    account_sequence: u64
});
v13_view!(TitanAccountView {
    account_no: u32,
    reserved: u32,
    account_epoch: u64,
    account_sequence: u64,
    state: u8,
    reason: u8,
    reserved2: [u8; 6]
});
v13_view!(TitanActiveOrderView {
    order_id: u64,
    asset_no: u32,
    account_no: u32,
    price_ticks: i64,
    qty_lots: i64,
    cumulative_filled_lots: i64,
    created_ts_ns: i64,
    updated_ts_ns: i64,
    account_sequence: u64,
    side: u8,
    order_type: u8,
    time_in_force: u8,
    status: u8,
    reduce_only: u8,
    reserved: [u8; 3]
});
v13_view!(TitanSubmitOrderRequest {
    asset_no: u32,
    account_no: u32,
    price_ticks: i64,
    qty_lots: i64,
    trigger_price_ticks: i64,
    gtd_expiry_ns: i64,
    side: u8,
    order_type: u8,
    time_in_force: u8,
    reduce_only: u8,
    trigger_kind: u8,
    reserved: [u8; 3]
});
v13_view!(TitanCancelOrderRequest {
    order_id: u64,
    asset_no: u32,
    account_no: u32
});

pub type TitanSubmitOrderFn =
    unsafe extern "C" fn(*mut std::ffi::c_void, *const TitanSubmitOrderRequest, *mut u64) -> i32;
pub type TitanCancelOrderFn =
    unsafe extern "C" fn(*mut std::ffi::c_void, *const TitanCancelOrderRequest, *mut u64) -> i32;

#[derive(Clone, Copy, Debug)]
pub enum StagedCommandV13 {
    Submit {
        order_id: u64,
        request: TitanSubmitOrderRequest,
    },
    Cancel {
        command_id: u64,
        request: TitanCancelOrderRequest,
    },
}

pub struct CallbackCommandStagingV13 {
    commands: Vec<StagedCommandV13>,
    capacity: usize,
    next_order_id: u64,
    next_command_id: u64,
    gate_open: bool,
    active_orders_ptr: *const TitanActiveOrderView,
    active_orders_len: usize,
}

// The borrowed active-order pointer is installed and cleared on the same single-writer strategy
// lane around a synchronous callback. The staging value is never accessed concurrently.
unsafe impl Send for CallbackCommandStagingV13 {}

impl CallbackCommandStagingV13 {
    pub fn new(
        capacity: usize,
        strategy_instance_id: u64,
        generation: u64,
    ) -> Result<Self, V13LoadError> {
        if capacity == 0 || strategy_instance_id == 0 || generation == 0 {
            return Err(V13LoadError::Contract(
                "invalid command staging configuration",
            ));
        }
        Ok(Self {
            commands: Vec::with_capacity(capacity),
            capacity,
            next_order_id: 1,
            next_command_id: 1,
            gate_open: false,
            active_orders_ptr: std::ptr::null(),
            active_orders_len: 0,
        })
    }

    pub fn begin_callback(&mut self, gate_open: bool, active_orders: &[TitanActiveOrderView]) {
        self.commands.clear();
        self.gate_open = gate_open;
        self.active_orders_ptr = active_orders.as_ptr();
        self.active_orders_len = active_orders.len();
    }

    pub fn bind_context(&mut self, context: &mut StrategyRuntimeContextV13) {
        context.command_context = (self as *mut Self).cast();
        context.submit_order = Some(stage_submit_order_v13);
        context.cancel_order = Some(stage_cancel_order_v13);
        context.last_error_code = 0;
    }

    pub fn finish_callback(&mut self, callback_code: i32) -> &[StagedCommandV13] {
        self.active_orders_ptr = std::ptr::null();
        self.active_orders_len = 0;
        if callback_code != 0 {
            self.commands.clear();
        }
        &self.commands
    }

    pub fn clear_committed(&mut self) {
        self.commands.clear();
    }

    fn active_orders(&self) -> &[TitanActiveOrderView] {
        if self.active_orders_ptr.is_null() {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.active_orders_ptr, self.active_orders_len) }
        }
    }
}

unsafe extern "C" fn stage_submit_order_v13(
    context: *mut std::ffi::c_void,
    request: *const TitanSubmitOrderRequest,
    order_id_out: *mut u64,
) -> i32 {
    if context.is_null() || request.is_null() || order_id_out.is_null() {
        return -1;
    }
    let staging = unsafe { &mut *context.cast::<CallbackCommandStagingV13>() };
    let request = unsafe { *request };
    if !staging.gate_open
        || staging.commands.len() >= staging.capacity
        || request.qty_lots <= 0
        || !matches!(request.side, 1 | 2)
        || request.order_type == 0
        || request.time_in_force == 0
    {
        return -2;
    }
    let order_id = staging.next_order_id;
    staging.next_order_id = staging.next_order_id.checked_add(1).unwrap_or(0);
    if order_id == 0 || staging.next_order_id == 0 {
        return -3;
    }
    staging
        .commands
        .push(StagedCommandV13::Submit { order_id, request });
    unsafe {
        *order_id_out = order_id;
    }
    0
}

unsafe extern "C" fn stage_cancel_order_v13(
    context: *mut std::ffi::c_void,
    request: *const TitanCancelOrderRequest,
    command_id_out: *mut u64,
) -> i32 {
    if context.is_null() || request.is_null() || command_id_out.is_null() {
        return -1;
    }
    let staging = unsafe { &mut *context.cast::<CallbackCommandStagingV13>() };
    let request = unsafe { *request };
    if !staging.gate_open || staging.commands.len() >= staging.capacity {
        return -2;
    }
    if !staging.active_orders().iter().any(|order| {
        order.order_id == request.order_id
            && order.account_no == request.account_no
            && order.asset_no == request.asset_no
    }) {
        return -4;
    }
    let command_id = staging.next_command_id;
    staging.next_command_id = staging.next_command_id.checked_add(1).unwrap_or(0);
    if command_id == 0 || staging.next_command_id == 0 {
        return -3;
    }
    staging.commands.push(StagedCommandV13::Cancel {
        command_id,
        request,
    });
    unsafe {
        *command_id_out = command_id;
    }
    0
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct StrategyRuntimeContextV13 {
    pub struct_size: u32,
    pub abi_version: u32,
    pub event_kind: u32,
    pub event_schema_version: u32,
    pub flags: u32,
    pub reserved0: u32,
    pub now_ns: i64,
    pub generation: u64,
    pub strategy_instance_id: u64,
    pub state_ptr: *mut u8,
    pub state_len: u64,
    pub state_alignment: u32,
    pub state_schema_version: u32,
    pub state_schema_hash: [u8; 32],
    pub ticks_ptr: *const TitanTickView,
    pub ticks_len: u64,
    pub bars_ptr: *const TitanBarView,
    pub bars_len: u64,
    pub depth_ptr: *const TitanDepthView,
    pub depth_len: u64,
    pub fills_ptr: *const TitanFillView,
    pub fills_len: u64,
    pub order_events_ptr: *const TitanOrderEventView,
    pub order_events_len: u64,
    pub cancel_events_ptr: *const TitanCancelEventView,
    pub cancel_events_len: u64,
    pub position_events_ptr: *const TitanPositionEventView,
    pub position_events_len: u64,
    pub balance_events_ptr: *const TitanBalanceEventView,
    pub balance_events_len: u64,
    pub account_state_events_ptr: *const TitanAccountStateEventView,
    pub account_state_events_len: u64,
    pub timer_ptr: *const TitanTimerView,
    pub timer_len: u64,
    pub markets_ptr: *const TitanMarketView,
    pub markets_len: u64,
    pub positions_ptr: *const TitanPositionView,
    pub positions_len: u64,
    pub balances_ptr: *const TitanBalanceView,
    pub balances_len: u64,
    pub accounts_ptr: *const TitanAccountView,
    pub accounts_len: u64,
    pub active_orders_ptr: *const TitanActiveOrderView,
    pub active_orders_len: u64,
    pub event_payload_ptr: *const std::ffi::c_void,
    pub event_payload_len: u64,
    pub command_context: *mut std::ffi::c_void,
    pub submit_order: Option<TitanSubmitOrderFn>,
    pub cancel_order: Option<TitanCancelOrderFn>,
    pub last_error_code: i32,
    pub reserved: u32,
}

impl Default for StrategyRuntimeContextV13 {
    fn default() -> Self {
        // All-zero is the defined empty borrowed-view representation. Function pointers are None.
        unsafe { std::mem::zeroed() }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct NativeStrategyDescriptorV13 {
    pub struct_size: u32,
    pub abi_version: u32,
    pub abi_fingerprint: [u8; 32],
    pub state_schema_version: u32,
    pub state_alignment: u32,
    pub state_len: u64,
    pub state_schema_hash: [u8; 32],
    pub callback_mask: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StateSchemaIdentityV13 {
    pub version: u32,
    pub hash: [u8; 32],
    pub byte_len: usize,
    pub alignment: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlignedStateMemoryV13 {
    words: Vec<u64>,
    byte_len: usize,
    schema: StateSchemaIdentityV13,
}

impl AlignedStateMemoryV13 {
    pub fn from_bytes(
        schema: StateSchemaIdentityV13,
        initial: &[u8],
    ) -> Result<Self, V13LoadError> {
        if schema.byte_len == 0
            || schema.byte_len != initial.len()
            || schema.alignment == 0
            || schema.alignment > 8
            || !schema.alignment.is_power_of_two()
        {
            return Err(V13LoadError::State("invalid state length or alignment"));
        }
        let mut words = vec![0_u64; schema.byte_len.div_ceil(8)];
        unsafe {
            std::ptr::copy_nonoverlapping(
                initial.as_ptr(),
                words.as_mut_ptr().cast(),
                initial.len(),
            );
        }
        Ok(Self {
            words,
            byte_len: initial.len(),
            schema,
        })
    }

    pub fn schema(&self) -> StateSchemaIdentityV13 {
        self.schema
    }
    pub fn as_ptr(&self) -> *const u8 {
        self.words.as_ptr().cast()
    }
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.words.as_mut_ptr().cast()
    }
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.byte_len) }
    }
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.byte_len) }
    }
    pub fn clone_for_instance(&self) -> Self {
        Self::from_bytes(self.schema, self.as_bytes()).expect("validated state remains valid")
    }
}

#[derive(Debug, thiserror::Error)]
pub enum V13LoadError {
    #[error("artifact I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("artifact CBOR is invalid: {0}")]
    Cbor(String),
    #[error("artifact contract violation: {0}")]
    Contract(&'static str),
    #[error("state memory is invalid: {0}")]
    State(&'static str),
    #[error("native library load failed: {0}")]
    Native(#[from] libloading::Error),
    #[error("artifact signature is invalid")]
    Signature,
    #[error("bundle is invalid: {0}")]
    Bundle(&'static str),
    #[error("native callback {event:?} returned {code}")]
    Callback { event: V13EventKind, code: i32 },
}

#[derive(Clone, Debug)]
pub struct ArtifactManifestV13 {
    pub strategy_id: Arc<str>,
    pub strategy_version: Arc<str>,
    pub target_triple: Arc<str>,
    pub cpu_baseline: Arc<str>,
    pub native_library: Arc<str>,
    pub callback_mask: u64,
    pub parameter_schema: Arc<serde_json::Value>,
    pub parameters_digest: [u8; 32],
    pub subscriptions: Arc<[EventSubscriptionV13]>,
    pub capabilities: StrategyCapabilitiesV13,
    pub state: StateSchemaIdentityV13,
    pub initial_state: Arc<[u8]>,
    pub native_digest: [u8; 32],
    pub artifact_digest: [u8; 32],
    pub signature: Option<ArtifactSignatureV13>,
}

#[derive(Clone, Debug)]
pub struct ArtifactSignatureV13 {
    pub algorithm: Arc<str>,
    pub key_id: Arc<str>,
    pub value: Arc<[u8]>,
}

#[derive(Clone, Default)]
pub struct V13TrustPolicy {
    pub require_signature: bool,
    pub ed25519_keys: BTreeMap<Arc<str>, [u8; 32]>,
}

pub type StrategyCallbackV13 = unsafe extern "C" fn(*mut StrategyRuntimeContextV13) -> i32;

#[derive(Clone)]
pub struct CallbackRegistryV13 {
    callbacks: [Option<StrategyCallbackV13>; V13_CALLBACK_COUNT],
}

impl CallbackRegistryV13 {
    pub fn invoke(
        &self,
        event: V13EventKind,
        context: &mut StrategyRuntimeContextV13,
    ) -> Result<(), V13LoadError> {
        context.event_kind = event as u32;
        let Some(callback) = self.callbacks[event.index()] else {
            return Ok(());
        };
        let code = unsafe { callback(context) };
        if code == 0 {
            Ok(())
        } else {
            Err(V13LoadError::Callback { event, code })
        }
    }
}

struct NativeLibraryLeaseV13 {
    _library: Library,
}
unsafe impl Send for NativeLibraryLeaseV13 {}
unsafe impl Sync for NativeLibraryLeaseV13 {}

pub struct StrategyArtifactV13 {
    pub manifest: ArtifactManifestV13,
    pub descriptor: NativeStrategyDescriptorV13,
    pub callbacks: CallbackRegistryV13,
    pub initial_state: AlignedStateMemoryV13,
    _lease: Arc<NativeLibraryLeaseV13>,
}

impl StrategyArtifactV13 {
    pub fn instantiate(
        &self,
        strategy_instance_id: u64,
        generation: u64,
    ) -> Result<StrategyInstanceV13, V13LoadError> {
        if strategy_instance_id == 0 || generation == 0 {
            return Err(V13LoadError::Contract(
                "instance id and generation must be nonzero",
            ));
        }
        Ok(StrategyInstanceV13 {
            strategy_instance_id,
            generation,
            state: self.initial_state.clone_for_instance(),
            callbacks: self.callbacks.clone(),
            strategy_id: self.manifest.strategy_id.clone(),
            strategy_version: self.manifest.strategy_version.clone(),
            artifact_digest: self.manifest.artifact_digest,
            binding_digest: [0; 32],
            lifecycle: StrategyInstanceLifecycleV13::Ready,
            command_gate_open: false,
            event_committed_sequence: 0,
            public_state_identity: [0; 32],
            _lease: self._lease.clone(),
        })
    }

    pub fn instantiate_with_config(
        &self,
        config: &StrategyInstanceConfigV13,
        generation: u64,
    ) -> Result<StrategyInstanceV13, V13LoadError> {
        if config.max_commands_per_callback == 0 || config.max_handler_duration.is_zero() {
            return Err(V13LoadError::Contract("runtime limits must be positive"));
        }
        validate_local_numbers(&config.markets)?;
        validate_local_numbers(&config.accounts)?;
        let mut instance = self.instantiate(config.strategy_instance_id, generation)?;
        instance.binding_digest = binding_digest(config);
        Ok(instance)
    }
}

#[derive(Clone, Debug)]
pub struct StrategyInstanceConfigV13 {
    pub strategy_instance_id: u64,
    pub markets: Vec<(u32, u32)>,
    pub accounts: Vec<(u32, u32)>,
    pub routing_keys: Vec<u64>,
    pub max_commands_per_callback: usize,
    pub max_handler_duration: std::time::Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StrategyInstanceLifecycleV13 {
    Ready,
    Running,
    Restoring,
    Stopped,
    Faulted,
}

pub struct StrategyInstanceV13 {
    pub strategy_instance_id: u64,
    pub generation: u64,
    pub state: AlignedStateMemoryV13,
    callbacks: CallbackRegistryV13,
    strategy_id: Arc<str>,
    strategy_version: Arc<str>,
    artifact_digest: [u8; 32],
    binding_digest: [u8; 32],
    lifecycle: StrategyInstanceLifecycleV13,
    command_gate_open: bool,
    event_committed_sequence: u64,
    public_state_identity: [u8; 32],
    _lease: Arc<NativeLibraryLeaseV13>,
}

impl StrategyInstanceV13 {
    pub fn invoke(
        &mut self,
        event: V13EventKind,
        context: &mut StrategyRuntimeContextV13,
    ) -> Result<(), V13LoadError> {
        let allowed = match event {
            V13EventKind::Start => self.lifecycle == StrategyInstanceLifecycleV13::Ready,
            V13EventKind::Stop => matches!(
                self.lifecycle,
                StrategyInstanceLifecycleV13::Ready | StrategyInstanceLifecycleV13::Running
            ),
            _ => self.lifecycle == StrategyInstanceLifecycleV13::Running,
        };
        if !allowed {
            return Err(V13LoadError::Contract(
                "callback is not allowed in the current lifecycle state",
            ));
        }
        context.struct_size = std::mem::size_of::<StrategyRuntimeContextV13>() as u32;
        context.abi_version = STRATEGY_ABI_V13;
        context.strategy_instance_id = self.strategy_instance_id;
        context.generation = self.generation;
        context.state_ptr = self.state.as_mut_ptr();
        context.state_len = self.state.schema.byte_len as u64;
        context.state_alignment = self.state.schema.alignment;
        context.state_schema_version = self.state.schema.version;
        context.state_schema_hash = self.state.schema.hash;
        if let Err(error) = self.callbacks.invoke(event, context) {
            self.command_gate_open = false;
            self.lifecycle = StrategyInstanceLifecycleV13::Faulted;
            return Err(error);
        }
        Ok(())
    }

    pub fn start(&mut self) -> Result<(), V13LoadError> {
        if self.lifecycle != StrategyInstanceLifecycleV13::Ready {
            return Err(V13LoadError::Contract("instance is not ready"));
        }
        self.command_gate_open = true;
        self.lifecycle = StrategyInstanceLifecycleV13::Running;
        Ok(())
    }

    pub fn stop(&mut self) {
        self.command_gate_open = false;
        self.lifecycle = StrategyInstanceLifecycleV13::Stopped;
    }

    pub fn command_gate_open(&self) -> bool {
        self.command_gate_open
    }

    pub fn update_checkpoint_boundary(
        &mut self,
        event_committed_sequence: u64,
        public_state_identity: [u8; 32],
    ) {
        self.event_committed_sequence = event_committed_sequence;
        self.public_state_identity = public_state_identity;
    }

    pub fn freeze_state(
        &self,
        checkpoint_id: u64,
    ) -> Result<StrategyStateSnapshotV13, V13LoadError> {
        if checkpoint_id == 0 {
            return Err(V13LoadError::Contract("checkpoint id must be nonzero"));
        }
        let mut snapshot = StrategyStateSnapshotV13 {
            checkpoint_id,
            strategy_instance_id: self.strategy_instance_id,
            generation: self.generation,
            event_committed_sequence: self.event_committed_sequence,
            strategy_id: self.strategy_id.clone(),
            strategy_version: self.strategy_version.clone(),
            artifact_digest: self.artifact_digest,
            binding_digest: self.binding_digest,
            abi_version: STRATEGY_ABI_V13,
            state_schema_version: self.state.schema.version,
            state_schema_hash: self.state.schema.hash,
            state_alignment: self.state.schema.alignment,
            state_bytes: Arc::from(self.state.as_bytes()),
            public_state_identity: self.public_state_identity,
            checksum: [0; 32],
        };
        snapshot.checksum = snapshot_checksum(&snapshot);
        Ok(snapshot)
    }

    pub fn restore_state(
        &mut self,
        snapshot: &StrategyStateSnapshotV13,
    ) -> Result<(), V13LoadError> {
        if !matches!(
            self.lifecycle,
            StrategyInstanceLifecycleV13::Ready | StrategyInstanceLifecycleV13::Stopped
        ) {
            return Err(V13LoadError::Contract(
                "restore requires a stopped instance safe point",
            ));
        }
        if snapshot.checksum != snapshot_checksum(snapshot)
            || snapshot.strategy_instance_id != self.strategy_instance_id
            || snapshot.strategy_id != self.strategy_id
            || snapshot.strategy_version != self.strategy_version
            || snapshot.artifact_digest != self.artifact_digest
            || snapshot.binding_digest != self.binding_digest
            || snapshot.abi_version != STRATEGY_ABI_V13
            || snapshot.state_schema_version != self.state.schema.version
            || snapshot.state_schema_hash != self.state.schema.hash
            || snapshot.state_alignment != self.state.schema.alignment
            || snapshot.state_bytes.len() != self.state.schema.byte_len
        {
            return Err(V13LoadError::Contract("snapshot identity mismatch"));
        }
        self.command_gate_open = false;
        self.lifecycle = StrategyInstanceLifecycleV13::Restoring;
        self.state
            .as_bytes_mut()
            .copy_from_slice(&snapshot.state_bytes);
        self.event_committed_sequence = snapshot.event_committed_sequence;
        self.public_state_identity = snapshot.public_state_identity;
        Ok(())
    }

    pub fn complete_restore(
        &mut self,
        rebuilt_public_state_identity: [u8; 32],
        new_generation: u64,
    ) -> Result<(), V13LoadError> {
        if self.lifecycle != StrategyInstanceLifecycleV13::Restoring
            || rebuilt_public_state_identity != self.public_state_identity
            || new_generation <= self.generation
        {
            self.lifecycle = StrategyInstanceLifecycleV13::Faulted;
            self.command_gate_open = false;
            return Err(V13LoadError::Contract("public-state reconcile failed"));
        }
        self.generation = new_generation;
        self.lifecycle = StrategyInstanceLifecycleV13::Ready;
        self.command_gate_open = false;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct StrategyStateSnapshotV13 {
    pub checkpoint_id: u64,
    pub strategy_instance_id: u64,
    pub generation: u64,
    pub event_committed_sequence: u64,
    pub strategy_id: Arc<str>,
    pub strategy_version: Arc<str>,
    pub artifact_digest: [u8; 32],
    pub binding_digest: [u8; 32],
    pub abi_version: u32,
    pub state_schema_version: u32,
    pub state_schema_hash: [u8; 32],
    pub state_alignment: u32,
    pub state_bytes: Arc<[u8]>,
    pub public_state_identity: [u8; 32],
    pub checksum: [u8; 32],
}

fn validate_local_numbers(values: &[(u32, u32)]) -> Result<(), V13LoadError> {
    let mut numbers: Vec<u32> = values.iter().map(|(number, _)| *number).collect();
    numbers.sort_unstable();
    if numbers.iter().copied().ne(0..numbers.len() as u32) {
        return Err(V13LoadError::Contract(
            "local bindings must be contiguous from zero",
        ));
    }
    Ok(())
}

fn binding_digest(config: &StrategyInstanceConfigV13) -> [u8; 32] {
    let mut markets = config.markets.clone();
    let mut accounts = config.accounts.clone();
    let mut routes = config.routing_keys.clone();
    markets.sort_unstable();
    accounts.sort_unstable();
    routes.sort_unstable();
    routes.dedup();
    let mut digest = Sha256::new();
    digest.update(b"titan.strategy.binding.v13");
    for (number, identity) in markets {
        digest.update(number.to_le_bytes());
        digest.update(identity.to_le_bytes());
    }
    digest.update(u64::MAX.to_le_bytes());
    for (number, identity) in accounts {
        digest.update(number.to_le_bytes());
        digest.update(identity.to_le_bytes());
    }
    digest.update(u64::MAX.to_le_bytes());
    for route in routes {
        digest.update(route.to_le_bytes());
    }
    digest.finalize().into()
}

fn snapshot_checksum(snapshot: &StrategyStateSnapshotV13) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"titan.strategy.snapshot.v13");
    for value in [
        snapshot.checkpoint_id,
        snapshot.strategy_instance_id,
        snapshot.generation,
        snapshot.event_committed_sequence,
    ] {
        digest.update(value.to_le_bytes());
    }
    digest.update((snapshot.strategy_id.len() as u64).to_le_bytes());
    digest.update(snapshot.strategy_id.as_bytes());
    digest.update((snapshot.strategy_version.len() as u64).to_le_bytes());
    digest.update(snapshot.strategy_version.as_bytes());
    digest.update(snapshot.artifact_digest);
    digest.update(snapshot.binding_digest);
    digest.update(snapshot.abi_version.to_le_bytes());
    digest.update(snapshot.state_schema_version.to_le_bytes());
    digest.update(snapshot.state_schema_hash);
    digest.update(snapshot.state_alignment.to_le_bytes());
    digest.update((snapshot.state_bytes.len() as u64).to_le_bytes());
    digest.update(&snapshot.state_bytes);
    digest.update(snapshot.public_state_identity);
    digest.finalize().into()
}

pub fn public_state_identity_v13(
    accounts: &[TitanAccountView],
    active_orders: &[TitanActiveOrderView],
    positions: &[TitanPositionView],
    balances: &[TitanBalanceView],
) -> [u8; 32] {
    let mut accounts = accounts.to_vec();
    accounts.sort_by_key(|value| value.account_no);
    let mut active_orders = active_orders.to_vec();
    active_orders.sort_by_key(|value| (value.account_no, value.order_id));
    let mut positions = positions.to_vec();
    positions.sort_by_key(|value| (value.account_no, value.asset_no));
    let mut balances = balances.to_vec();
    balances.sort_by_key(|value| (value.account_no, value.currency_no));
    let mut digest = Sha256::new();
    digest.update(b"titan.public-state.v13");
    digest.update(1_u32.to_le_bytes());
    digest.update((accounts.len() as u64).to_le_bytes());
    for value in accounts {
        digest.update(value.account_no.to_le_bytes());
        digest.update(value.account_epoch.to_le_bytes());
        digest.update(value.account_sequence.to_le_bytes());
        digest.update([value.state, value.reason]);
    }
    digest.update((active_orders.len() as u64).to_le_bytes());
    for value in active_orders {
        digest.update(value.order_id.to_le_bytes());
        digest.update(value.asset_no.to_le_bytes());
        digest.update(value.account_no.to_le_bytes());
        digest.update(value.price_ticks.to_le_bytes());
        digest.update(value.qty_lots.to_le_bytes());
        digest.update(value.cumulative_filled_lots.to_le_bytes());
        digest.update(value.created_ts_ns.to_le_bytes());
        digest.update(value.updated_ts_ns.to_le_bytes());
        digest.update(value.account_sequence.to_le_bytes());
        digest.update([
            value.side,
            value.order_type,
            value.time_in_force,
            value.status,
            value.reduce_only,
        ]);
    }
    digest.update((positions.len() as u64).to_le_bytes());
    for value in positions {
        digest.update(value.account_no.to_le_bytes());
        digest.update(value.asset_no.to_le_bytes());
        digest.update(value.qty_lots.to_le_bytes());
        digest.update(value.average_price_ticks.to_le_bytes());
        digest.update(value.realized_pnl_ticks.to_le_bytes());
        digest.update(value.account_sequence.to_le_bytes());
    }
    digest.update((balances.len() as u64).to_le_bytes());
    for value in balances {
        digest.update(value.account_no.to_le_bytes());
        digest.update(value.currency_no.to_le_bytes());
        digest.update(value.total_units.to_le_bytes());
        digest.update(value.available_units.to_le_bytes());
        digest.update(value.account_sequence.to_le_bytes());
    }
    digest.finalize().into()
}

pub struct NativeArtifactLoaderV13 {
    cache_dir: PathBuf,
    trust: V13TrustPolicy,
    max_bundle_entry_bytes: u64,
}

impl NativeArtifactLoaderV13 {
    pub fn new(cache_dir: PathBuf, trust: V13TrustPolicy) -> Self {
        Self {
            cache_dir,
            trust,
            max_bundle_entry_bytes: 64 * 1024 * 1024,
        }
    }

    pub fn inspect(&self, artifact_path: &Path) -> Result<ArtifactManifestV13, V13LoadError> {
        let (manifest_bytes, library) = self.read_pair_or_bundle(artifact_path)?;
        let mut value = parse_cbor(&manifest_bytes)?;
        let signature = parse_signature(map_get(&value, "signature")?)?;
        map_remove(&mut value, "signature")?;
        let unsigned = canonical_cbor(&value)?;
        let artifact_digest: [u8; 32] = Sha256::digest(&unsigned).into();
        self.verify_signature(signature.as_ref(), &unsigned)?;
        let manifest = parse_manifest(&value, signature, artifact_digest)?;
        if <[u8; 32]>::from(Sha256::digest(&library)) != manifest.native_digest {
            return Err(V13LoadError::Contract("native digest mismatch"));
        }
        if manifest.target_triple.as_ref() != host_target_triple() {
            return Err(V13LoadError::Contract(
                "artifact target does not match host",
            ));
        }
        Ok(manifest)
    }

    pub fn load(&self, artifact_path: &Path) -> Result<StrategyArtifactV13, V13LoadError> {
        let manifest = self.inspect(artifact_path)?;
        let (_, library_bytes) = self.read_pair_or_bundle(artifact_path)?;
        fs::create_dir_all(&self.cache_dir)?;
        let cached = self
            .cache_dir
            .join(format!("{}.so", hex(&manifest.native_digest)));
        let cache_valid = match fs::read(&cached) {
            Ok(bytes) => <[u8; 32]>::from(Sha256::digest(&bytes)) == manifest.native_digest,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        if !cache_valid {
            let temporary = cached.with_extension(format!("{}.tmp", std::process::id()));
            fs::write(&temporary, &library_bytes)?;
            fs::rename(temporary, &cached)?;
        }
        let library = unsafe { Library::new(&cached)? };
        let abi_version = unsafe {
            library.get::<unsafe extern "C" fn() -> u32>(b"titan_strategy_abi_version\0")?
        };
        if unsafe { abi_version() } != STRATEGY_ABI_V13 {
            return Err(V13LoadError::Contract("native ABI version mismatch"));
        }
        let descriptor_fn = unsafe {
            library.get::<unsafe extern "C" fn() -> *const NativeStrategyDescriptorV13>(
                b"titan_strategy_descriptor\0",
            )?
        };
        let descriptor_ptr = unsafe { descriptor_fn() };
        if descriptor_ptr.is_null() {
            return Err(V13LoadError::Contract("native descriptor is null"));
        }
        let descriptor = unsafe { *descriptor_ptr };
        validate_descriptor(&descriptor, &manifest)?;
        let mut callbacks = [None; V13_CALLBACK_COUNT];
        for (index, name) in CALLBACK_NAMES.iter().enumerate() {
            let declared = manifest.callback_mask & (1 << index) != 0;
            let symbol = unsafe {
                library.get::<StrategyCallbackV13>(format!("titan_strategy_{name}\0").as_bytes())
            };
            match (declared, symbol) {
                (true, Ok(value)) => callbacks[index] = Some(*value),
                (true, Err(_)) => {
                    return Err(V13LoadError::Contract(
                        "declared callback symbol is missing",
                    ));
                }
                (false, Ok(_)) => {
                    return Err(V13LoadError::Contract(
                        "undeclared callback symbol is exported",
                    ));
                }
                (false, Err(_)) => {}
            }
        }
        let initial_state =
            AlignedStateMemoryV13::from_bytes(manifest.state, &manifest.initial_state)?;
        Ok(StrategyArtifactV13 {
            manifest,
            descriptor,
            callbacks: CallbackRegistryV13 { callbacks },
            initial_state,
            _lease: Arc::new(NativeLibraryLeaseV13 { _library: library }),
        })
    }

    fn verify_signature(
        &self,
        signature: Option<&ArtifactSignatureV13>,
        payload: &[u8],
    ) -> Result<(), V13LoadError> {
        let Some(signature) = signature else {
            return if self.trust.require_signature {
                Err(V13LoadError::Signature)
            } else {
                Ok(())
            };
        };
        if signature.algorithm.as_ref() != "ed25519" {
            return Err(V13LoadError::Signature);
        }
        let key = self
            .trust
            .ed25519_keys
            .get(&signature.key_id)
            .ok_or(V13LoadError::Signature)?;
        let key = VerifyingKey::from_bytes(key).map_err(|_| V13LoadError::Signature)?;
        let signature =
            Signature::from_slice(&signature.value).map_err(|_| V13LoadError::Signature)?;
        key.verify(payload, &signature)
            .map_err(|_| V13LoadError::Signature)
    }

    fn read_pair_or_bundle(&self, path: &Path) -> Result<(Vec<u8>, Vec<u8>), V13LoadError> {
        if path.extension().and_then(|value| value.to_str()) == Some("titan") {
            let bytes = fs::read(path)?;
            let mut archive = zip::ZipArchive::new(Cursor::new(bytes))
                .map_err(|_| V13LoadError::Bundle("invalid zip"))?;
            if archive.len() != 2 {
                return Err(V13LoadError::Bundle("bundle must contain two files"));
            }
            let mut manifest = None;
            let mut library = None;
            let mut names = BTreeSet::new();
            for index in 0..archive.len() {
                let mut entry = archive
                    .by_index(index)
                    .map_err(|_| V13LoadError::Bundle("invalid entry"))?;
                let name = entry.name().to_owned();
                if !safe_entry_name(&name)
                    || !names.insert(name.clone())
                    || entry.size() > self.max_bundle_entry_bytes
                {
                    return Err(V13LoadError::Bundle("unsafe entry"));
                }
                let mut bytes = Vec::with_capacity(entry.size() as usize);
                entry.read_to_end(&mut bytes)?;
                if name.ends_with(".manifest.cbor") {
                    manifest = Some(bytes);
                } else if name.ends_with(".so") {
                    library = Some(bytes);
                } else {
                    return Err(V13LoadError::Bundle("unknown entry"));
                }
            }
            return Ok((
                manifest.ok_or(V13LoadError::Bundle("manifest missing"))?,
                library.ok_or(V13LoadError::Bundle("library missing"))?,
            ));
        }
        let manifest_path = if path.to_string_lossy().ends_with(".manifest.cbor") {
            path.to_path_buf()
        } else {
            path.with_extension("manifest.cbor")
        };
        let manifest = fs::read(&manifest_path)?;
        let value = parse_cbor(&manifest)?;
        let library_name = value_text(map_get(&value, "native_library")?)?;
        if !safe_entry_name(library_name) {
            return Err(V13LoadError::Contract("unsafe native library name"));
        }
        let library = fs::read(
            manifest_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(library_name),
        )?;
        Ok((manifest, library))
    }
}

const CALLBACK_NAMES: [&str; V13_CALLBACK_COUNT] = [
    "on_start",
    "on_tick",
    "on_bar",
    "on_depth",
    "on_fill",
    "on_order",
    "on_cancel",
    "on_position",
    "on_balance",
    "on_account_state",
    "on_timer",
    "on_stop",
];

fn validate_descriptor(
    descriptor: &NativeStrategyDescriptorV13,
    manifest: &ArtifactManifestV13,
) -> Result<(), V13LoadError> {
    if descriptor.struct_size as usize != std::mem::size_of::<NativeStrategyDescriptorV13>()
        || descriptor.abi_version != STRATEGY_ABI_V13
        || descriptor.abi_fingerprint != V13_ABI_FINGERPRINT
        || descriptor.state_schema_version != manifest.state.version
        || descriptor.state_alignment != manifest.state.alignment
        || descriptor.state_len != manifest.state.byte_len as u64
        || descriptor.state_schema_hash != manifest.state.hash
        || descriptor.callback_mask != manifest.callback_mask
        || descriptor.callback_mask >> V13_CALLBACK_COUNT != 0
    {
        return Err(V13LoadError::Contract(
            "native descriptor does not match manifest",
        ));
    }
    Ok(())
}

fn parse_manifest(
    value: &CborValue,
    signature: Option<ArtifactSignatureV13>,
    artifact_digest: [u8; 32],
) -> Result<ArtifactManifestV13, V13LoadError> {
    if value_u64(map_get(value, "artifact_format_version")?)? != 1
        || value_u64(map_get(value, "abi_version")?)? != 13
        || value_bytes32(map_get(value, "abi_fingerprint")?)? != V13_ABI_FINGERPRINT
    {
        return Err(V13LoadError::Contract("manifest ABI identity mismatch"));
    }
    let state_len = value_u64(map_get(value, "state_len")?)? as usize;
    let initial_state = value_bytes(map_get(value, "initial_state")?)?;
    if initial_state.len() != state_len {
        return Err(V13LoadError::Contract("initial state length mismatch"));
    }
    let subscriptions = value_array(map_get(value, "subscriptions")?)?
        .iter()
        .map(|subscription| {
            let event = V13EventKind::from_manifest_name(value_text(map_get(subscription, "event")?)?)?;
            let handler: Arc<str> = Arc::from(value_text(map_get(subscription, "handler")?)?);
            let schema_version = u32::try_from(value_u64(map_get(subscription, "schema_version")?)?)
                .map_err(|_| V13LoadError::Contract("subscription schema version overflow"))?;
            if schema_version == 0 || handler.as_ref() != CALLBACK_NAMES[event.index()] {
                return Err(V13LoadError::Contract("subscription handler contract mismatch"));
            }
            let qos = V13EventQos::from_manifest_name(value_text(map_get(subscription, "qos")?)?)?;
            if matches!(
                event,
                V13EventKind::Fill
                    | V13EventKind::Order
                    | V13EventKind::Cancel
                    | V13EventKind::Position
                    | V13EventKind::Balance
                    | V13EventKind::AccountState
            ) && qos != V13EventQos::ReliableOrdered
            {
                return Err(V13LoadError::Contract("account subscription must be reliable ordered"));
            }
            Ok(EventSubscriptionV13 { event, handler, schema_version, qos })
        })
        .collect::<Result<Vec<_>, V13LoadError>>()?;
    let mut seen = BTreeSet::new();
    if subscriptions.iter().any(|item| !seen.insert(item.event as u32)) {
        return Err(V13LoadError::Contract("duplicate subscription event"));
    }
    let mut capabilities = StrategyCapabilitiesV13::default();
    for capability in value_array(map_get(value, "capabilities")?)? {
        capabilities.0 |= match value_text(capability)? {
            "market_data" => StrategyCapabilitiesV13::MARKET_DATA.0,
            "account_data" => StrategyCapabilitiesV13::ACCOUNT_DATA.0,
            "order_execution" => StrategyCapabilitiesV13::ORDER_EXECUTION.0,
            "timer" => StrategyCapabilitiesV13::TIMER.0,
            _ => return Err(V13LoadError::Contract("unknown strategy capability")),
        };
    }
    if subscriptions.iter().any(|item| matches!(item.event, V13EventKind::Tick | V13EventKind::Bar | V13EventKind::Depth))
        && !capabilities.contains(StrategyCapabilitiesV13::MARKET_DATA)
    {
        return Err(V13LoadError::Contract("market subscription lacks capability"));
    }
    if subscriptions.iter().any(|item| matches!(item.event, V13EventKind::Fill | V13EventKind::Order | V13EventKind::Cancel | V13EventKind::Position | V13EventKind::Balance | V13EventKind::AccountState))
        && !capabilities.contains(StrategyCapabilitiesV13::ACCOUNT_DATA)
    {
        return Err(V13LoadError::Contract("account subscription lacks capability"));
    }
    Ok(ArtifactManifestV13 {
        strategy_id: Arc::from(value_text(map_get(value, "strategy_id")?)?),
        strategy_version: Arc::from(value_text(map_get(value, "strategy_version")?)?),
        target_triple: Arc::from(value_text(map_get(value, "target_triple")?)?),
        cpu_baseline: Arc::from(value_text(map_get(value, "cpu_baseline")?)?),
        native_library: Arc::from(value_text(map_get(value, "native_library")?)?),
        callback_mask: value_u64(map_get(value, "callback_mask")?)?,
        parameter_schema: Arc::new(cbor_to_json(map_get(value, "parameter_schema")?)?),
        parameters_digest: value_bytes32(map_get(value, "parameters_digest")?)?,
        subscriptions: subscriptions.into(),
        capabilities,
        state: StateSchemaIdentityV13 {
            version: value_u64(map_get(value, "state_schema_version")?)? as u32,
            hash: value_bytes32(map_get(value, "state_schema_hash")?)?,
            byte_len: state_len,
            alignment: value_u64(map_get(value, "state_alignment")?)? as u32,
        },
        initial_state: initial_state.into(),
        native_digest: value_bytes32(map_get(value, "native_digest")?)?,
        artifact_digest,
        signature,
    })
}

fn parse_signature(value: &CborValue) -> Result<Option<ArtifactSignatureV13>, V13LoadError> {
    if matches!(value, CborValue::Null) {
        return Ok(None);
    }
    Ok(Some(ArtifactSignatureV13 {
        algorithm: Arc::from(value_text(map_get(value, "algorithm")?)?),
        key_id: Arc::from(value_text(map_get(value, "key_id")?)?),
        value: value_bytes(map_get(value, "value")?)?.into(),
    }))
}

fn map(value: &CborValue) -> Result<&Vec<(CborValue, CborValue)>, V13LoadError> {
    if let CborValue::Map(value) = value {
        Ok(value)
    } else {
        Err(V13LoadError::Contract("expected map"))
    }
}
fn map_get<'a>(value: &'a CborValue, name: &str) -> Result<&'a CborValue, V13LoadError> {
    map(value)?
        .iter()
        .find_map(|(key, value)| (key.as_text() == Some(name)).then_some(value))
        .ok_or(V13LoadError::Contract("manifest field is missing"))
}
fn map_remove(value: &mut CborValue, name: &str) -> Result<(), V13LoadError> {
    let CborValue::Map(values) = value else {
        return Err(V13LoadError::Contract("expected map"));
    };
    let index = values
        .iter()
        .position(|(key, _)| key.as_text() == Some(name))
        .ok_or(V13LoadError::Contract("manifest field is missing"))?;
    values.remove(index);
    Ok(())
}
fn value_text(value: &CborValue) -> Result<&str, V13LoadError> {
    value
        .as_text()
        .ok_or(V13LoadError::Contract("expected text"))
}
fn value_bytes(value: &CborValue) -> Result<Vec<u8>, V13LoadError> {
    value
        .as_bytes()
        .map(ToOwned::to_owned)
        .ok_or(V13LoadError::Contract("expected bytes"))
}
fn value_bytes32(value: &CborValue) -> Result<[u8; 32], V13LoadError> {
    value_bytes(value)?
        .try_into()
        .map_err(|_| V13LoadError::Contract("expected 32 bytes"))
}
fn value_u64(value: &CborValue) -> Result<u64, V13LoadError> {
    let CborValue::Integer(value) = value else {
        return Err(V13LoadError::Contract("expected integer"));
    };
    u64::try_from(*value).map_err(|_| V13LoadError::Contract("expected uint64"))
}

fn value_array(value: &CborValue) -> Result<&[CborValue], V13LoadError> {
    if let CborValue::Array(value) = value {
        Ok(value)
    } else {
        Err(V13LoadError::Contract("expected array"))
    }
}

fn cbor_to_json(value: &CborValue) -> Result<serde_json::Value, V13LoadError> {
    Ok(match value {
        CborValue::Integer(value) => {
            let number = serde_json::Number::from_i128(*value)
                .ok_or(V13LoadError::Contract("JSON integer overflow"))?;
            serde_json::Value::Number(number)
        }
        CborValue::Text(value) => serde_json::Value::String(value.clone()),
        CborValue::Array(values) => serde_json::Value::Array(
            values.iter().map(cbor_to_json).collect::<Result<Vec<_>, _>>()?,
        ),
        CborValue::Map(values) => {
            let mut output = serde_json::Map::new();
            for (key, value) in values {
                let key = key
                    .as_text()
                    .ok_or(V13LoadError::Contract("JSON object key must be text"))?;
                output.insert(key.to_owned(), cbor_to_json(value)?);
            }
            serde_json::Value::Object(output)
        }
        CborValue::Bool(value) => serde_json::Value::Bool(*value),
        CborValue::Null => serde_json::Value::Null,
        CborValue::Float(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .ok_or(V13LoadError::Contract("non-finite JSON float"))?,
        CborValue::Bytes(_) => return Err(V13LoadError::Contract("bytes are not valid JSON schema values")),
    })
}

fn canonical_cbor(value: &CborValue) -> Result<Vec<u8>, V13LoadError> {
    let mut output = Vec::new();
    encode_cbor(value, &mut output)?;
    Ok(output)
}
fn cbor_head(major: u8, value: u64, out: &mut Vec<u8>) {
    match value {
        0..=23 => out.push(major << 5 | value as u8),
        24..=0xff => {
            out.extend([major << 5 | 24, value as u8]);
        }
        0x100..=0xffff => {
            out.push(major << 5 | 25);
            out.extend((value as u16).to_be_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            out.push(major << 5 | 26);
            out.extend((value as u32).to_be_bytes());
        }
        _ => {
            out.push(major << 5 | 27);
            out.extend(value.to_be_bytes());
        }
    }
}
fn encode_cbor(value: &CborValue, out: &mut Vec<u8>) -> Result<(), V13LoadError> {
    match value {
        CborValue::Integer(value) => {
            if let Ok(value) = u64::try_from(*value) {
                cbor_head(0, value, out);
            } else {
                cbor_head(1, (-1 - *value) as u64, out);
            }
        }
        CborValue::Bytes(value) => {
            cbor_head(2, value.len() as u64, out);
            out.extend(value);
        }
        CborValue::Text(value) => {
            cbor_head(3, value.len() as u64, out);
            out.extend(value.as_bytes());
        }
        CborValue::Array(values) => {
            cbor_head(4, values.len() as u64, out);
            for value in values {
                encode_cbor(value, out)?;
            }
        }
        CborValue::Map(values) => {
            let mut encoded = values
                .iter()
                .map(|(key, value)| Ok((canonical_cbor(key)?, canonical_cbor(value)?)))
                .collect::<Result<Vec<_>, V13LoadError>>()?;
            encoded.sort_by(|left, right| {
                left.0
                    .len()
                    .cmp(&right.0.len())
                    .then_with(|| left.0.cmp(&right.0))
            });
            cbor_head(5, encoded.len() as u64, out);
            for (key, value) in encoded {
                out.extend(key);
                out.extend(value);
            }
        }
        CborValue::Bool(false) => out.push(0xf4),
        CborValue::Bool(true) => out.push(0xf5),
        CborValue::Null => out.push(0xf6),
        CborValue::Float(value) => {
            out.push(0xfb);
            out.extend(value.to_be_bytes());
        }
    }
    Ok(())
}

fn parse_cbor(data: &[u8]) -> Result<CborValue, V13LoadError> {
    let (value, offset) = parse_cbor_at(data, 0)?;
    if offset != data.len() {
        return Err(V13LoadError::Cbor("trailing bytes".into()));
    }
    Ok(value)
}

fn cbor_argument(data: &[u8], offset: usize, additional: u8) -> Result<(u64, usize), V13LoadError> {
    if additional < 24 {
        return Ok((u64::from(additional), offset));
    }
    let width = match additional {
        24 => 1,
        25 => 2,
        26 => 4,
        27 => 8,
        _ => return Err(V13LoadError::Cbor("indefinite CBOR is forbidden".into())),
    };
    let end = offset
        .checked_add(width)
        .ok_or_else(|| V13LoadError::Cbor("CBOR overflow".into()))?;
    let bytes = data
        .get(offset..end)
        .ok_or_else(|| V13LoadError::Cbor("truncated CBOR".into()))?;
    let mut padded = [0_u8; 8];
    padded[8 - width..].copy_from_slice(bytes);
    let value = u64::from_be_bytes(padded);
    if (additional == 24 && value < 24)
        || (additional == 25 && value <= 0xff)
        || (additional == 26 && value <= 0xffff)
        || (additional == 27 && value <= 0xffff_ffff)
    {
        return Err(V13LoadError::Cbor("non-minimal integer".into()));
    }
    Ok((value, end))
}

fn parse_cbor_at(data: &[u8], mut offset: usize) -> Result<(CborValue, usize), V13LoadError> {
    let initial = *data
        .get(offset)
        .ok_or_else(|| V13LoadError::Cbor("truncated CBOR".into()))?;
    offset += 1;
    let major = initial >> 5;
    let additional = initial & 31;
    if major == 7 {
        return match additional {
            20 => Ok((CborValue::Bool(false), offset)),
            21 => Ok((CborValue::Bool(true), offset)),
            22 => Ok((CborValue::Null, offset)),
            27 => {
                let end = offset + 8;
                let bytes: [u8; 8] = data
                    .get(offset..end)
                    .ok_or_else(|| V13LoadError::Cbor("truncated float".into()))?
                    .try_into()
                    .unwrap();
                Ok((CborValue::Float(f64::from_be_bytes(bytes)), end))
            }
            _ => Err(V13LoadError::Cbor("unsupported simple value".into())),
        };
    }
    let (argument, mut offset) = cbor_argument(data, offset, additional)?;
    match major {
        0 => Ok((CborValue::Integer(i128::from(argument)), offset)),
        1 => Ok((CborValue::Integer(-1 - i128::from(argument)), offset)),
        2 | 3 => {
            let size = usize::try_from(argument)
                .map_err(|_| V13LoadError::Cbor("string too large".into()))?;
            let end = offset
                .checked_add(size)
                .ok_or_else(|| V13LoadError::Cbor("string overflow".into()))?;
            let bytes = data
                .get(offset..end)
                .ok_or_else(|| V13LoadError::Cbor("truncated string".into()))?;
            if major == 2 {
                Ok((CborValue::Bytes(bytes.to_vec()), end))
            } else {
                Ok((
                    CborValue::Text(
                        std::str::from_utf8(bytes)
                            .map_err(|_| V13LoadError::Cbor("invalid UTF-8".into()))?
                            .to_owned(),
                    ),
                    end,
                ))
            }
        }
        4 => {
            let mut values = Vec::new();
            for _ in 0..argument {
                let (value, next) = parse_cbor_at(data, offset)?;
                values.push(value);
                offset = next;
            }
            Ok((CborValue::Array(values), offset))
        }
        5 => {
            let mut values = Vec::new();
            let mut previous: Option<(usize, Vec<u8>)> = None;
            for _ in 0..argument {
                let start = offset;
                let (key, next) = parse_cbor_at(data, offset)?;
                let encoded = data[start..next].to_vec();
                let order = (encoded.len(), encoded.clone());
                if previous.as_ref().is_some_and(|value| order <= *value) {
                    return Err(V13LoadError::Cbor("map keys are not deterministic".into()));
                }
                previous = Some(order);
                offset = next;
                let (value, next) = parse_cbor_at(data, offset)?;
                values.push((key, value));
                offset = next;
            }
            Ok((CborValue::Map(values), offset))
        }
        _ => Err(V13LoadError::Cbor("unsupported CBOR major type".into())),
    }
}

fn safe_entry_name(name: &str) -> bool {
    let path = Path::new(name);
    !name.is_empty()
        && path
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
}
fn host_target_triple() -> &'static str {
    #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
    {
        "x86_64-unknown-linux-gnu"
    }
    #[cfg(all(target_arch = "aarch64", target_os = "linux"))]
    {
        "aarch64-unknown-linux-gnu"
    }
    #[cfg(all(target_arch = "aarch64", target_os = "macos"))]
    {
        "aarch64-apple-darwin"
    }
    #[cfg(not(any(
        all(target_arch = "x86_64", target_os = "linux"),
        all(target_arch = "aarch64", target_os = "linux"),
        all(target_arch = "aarch64", target_os = "macos")
    )))]
    {
        "unsupported"
    }
}
fn hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

const _: () = {
    assert!(std::mem::size_of::<StrategyRuntimeContextV13>() == 392);
    assert!(std::mem::align_of::<StrategyRuntimeContextV13>() == 8);
    assert!(std::mem::size_of::<NativeStrategyDescriptorV13>() == 96);
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligned_state_is_independent_and_hides_tail_padding() {
        let schema = StateSchemaIdentityV13 {
            version: 1,
            hash: [7; 32],
            byte_len: 13,
            alignment: 8,
        };
        let initial: Vec<u8> = (0..13).collect();
        let state = AlignedStateMemoryV13::from_bytes(schema, &initial).unwrap();
        assert_eq!(state.as_ptr() as usize % 8, 0);
        assert_eq!(state.as_bytes(), initial);
        let mut clone = state.clone_for_instance();
        clone.as_bytes_mut()[0] = 99;
        assert_eq!(state.as_bytes()[0], 0);
    }

    #[test]
    fn command_staging_is_atomic_and_cancel_checks_ownership() {
        let active = [TitanActiveOrderView {
            order_id: 9,
            asset_no: 2,
            account_no: 1,
            ..Default::default()
        }];
        let mut staging = CallbackCommandStagingV13::new(2, 5, 1).unwrap();
        let mut context = StrategyRuntimeContextV13::default();
        staging.begin_callback(true, &active);
        staging.bind_context(&mut context);
        let request = TitanSubmitOrderRequest {
            account_no: 1,
            asset_no: 2,
            qty_lots: 3,
            price_ticks: 100,
            side: 1,
            order_type: 1,
            time_in_force: 1,
            ..Default::default()
        };
        let mut order_id = 0;
        assert_eq!(
            unsafe {
                context.submit_order.unwrap()(context.command_context, &request, &mut order_id)
            },
            0
        );
        assert_eq!(order_id, 1);
        let cancel = TitanCancelOrderRequest {
            order_id: 9,
            account_no: 1,
            asset_no: 2,
        };
        let mut command_id = 0;
        assert_eq!(
            unsafe {
                context.cancel_order.unwrap()(context.command_context, &cancel, &mut command_id)
            },
            0
        );
        assert_eq!(
            staging.finish_callback(-1).len(),
            0,
            "failed callbacks discard the whole batch"
        );
    }

    #[test]
    fn public_identity_is_order_independent_but_fact_sensitive() {
        let first = TitanActiveOrderView {
            order_id: 2,
            account_no: 1,
            qty_lots: 3,
            ..Default::default()
        };
        let second = TitanActiveOrderView {
            order_id: 1,
            account_no: 1,
            qty_lots: 4,
            ..Default::default()
        };
        let left = public_state_identity_v13(&[], &[first, second], &[], &[]);
        let right = public_state_identity_v13(&[], &[second, first], &[], &[]);
        assert_eq!(left, right);
        let changed = TitanActiveOrderView {
            qty_lots: 5,
            ..second
        };
        assert_ne!(
            left,
            public_state_identity_v13(&[], &[first, changed], &[], &[])
        );
    }
}
