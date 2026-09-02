use crate::Id128;

pub const ORDER_CHANGED_EVENT: &str = "titan.account.OrderChanged";
pub const FILL_EVENT: &str = "titan.account.Fill";
pub const POSITION_CHANGED_EVENT: &str = "titan.account.PositionChanged";
pub const BALANCE_CHANGED_EVENT: &str = "titan.account.BalanceChanged";
pub const COMMAND_RESULT_EVENT: &str = "titan.account.CommandResult";
pub const RECONCILE_STARTED_EVENT: &str = "titan.account.ReconcileStarted";
pub const RECONCILE_COMPLETED_EVENT: &str = "titan.account.ReconcileCompleted";
pub const STREAM_STATE_CHANGED_EVENT: &str = "titan.account.StreamStateChanged";
pub const STREAM_INVALIDATED_EVENT: &str = "titan.account.StreamInvalidated";
pub const ACCOUNT_EVENT_SCHEMA_VERSION: u32 = 1;
pub const FILL_EVENT_SCHEMA_VERSION: u32 = 2;

pub mod event_kind {
    pub const ORDER_CHANGED: u16 = 1;
    pub const FILL: u16 = 2;
    pub const POSITION_CHANGED: u16 = 3;
    pub const BALANCE_CHANGED: u16 = 4;
    pub const COMMAND_RESULT: u16 = 5;
    pub const RECONCILE_STARTED: u16 = 6;
    pub const RECONCILE_COMPLETED: u16 = 7;
    pub const STREAM_STATE_CHANGED: u16 = 8;
    pub const STREAM_INVALIDATED: u16 = 9;
}

pub const ACCOUNT_EVENT_TYPES: [&str; 9] = [
    ORDER_CHANGED_EVENT,
    FILL_EVENT,
    POSITION_CHANGED_EVENT,
    BALANCE_CHANGED_EVENT,
    COMMAND_RESULT_EVENT,
    RECONCILE_STARTED_EVENT,
    RECONCILE_COMPLETED_EVENT,
    STREAM_STATE_CHANGED_EVENT,
    STREAM_INVALIDATED_EVENT,
];

pub fn is_control_event(event_type: &str) -> bool {
    matches!(
        event_type,
        RECONCILE_STARTED_EVENT
            | RECONCILE_COMPLETED_EVENT
            | STREAM_STATE_CHANGED_EVENT
            | STREAM_INVALIDATED_EVENT
    )
}

pub fn account_event_layout(event_type: &str) -> Option<(u16, usize)> {
    account_event_layout_version(event_type, ACCOUNT_EVENT_SCHEMA_VERSION)
}

pub fn account_event_layout_version(event_type: &str, schema_version: u32) -> Option<(u16, usize)> {
    Some(match event_type {
        ORDER_CHANGED_EVENT => (event_kind::ORDER_CHANGED, OrderChangedV1::ENCODED_LEN),
        FILL_EVENT if schema_version == FILL_EVENT_SCHEMA_VERSION => {
            (event_kind::FILL, FillV2::ENCODED_LEN)
        }
        FILL_EVENT if schema_version == ACCOUNT_EVENT_SCHEMA_VERSION => {
            (event_kind::FILL, FillV1::ENCODED_LEN)
        }
        POSITION_CHANGED_EVENT => (event_kind::POSITION_CHANGED, PositionChangedV1::ENCODED_LEN),
        BALANCE_CHANGED_EVENT => (event_kind::BALANCE_CHANGED, BalanceChangedV1::ENCODED_LEN),
        COMMAND_RESULT_EVENT => (event_kind::COMMAND_RESULT, CommandResultV1::ENCODED_LEN),
        RECONCILE_STARTED_EVENT => (
            event_kind::RECONCILE_STARTED,
            ReconcileStartedV1::ENCODED_LEN,
        ),
        RECONCILE_COMPLETED_EVENT => (
            event_kind::RECONCILE_COMPLETED,
            ReconcileCompletedV1::ENCODED_LEN,
        ),
        STREAM_STATE_CHANGED_EVENT => (
            event_kind::STREAM_STATE_CHANGED,
            StreamStateChangedV1::ENCODED_LEN,
        ),
        STREAM_INVALIDATED_EVENT => (
            event_kind::STREAM_INVALIDATED,
            StreamInvalidatedV1::ENCODED_LEN,
        ),
        _ => return None,
    })
}

pub mod event_flags {
    pub const SNAPSHOT: u16 = 1 << 0;
    pub const UPSERT: u16 = 1 << 1;
    pub const DELETE: u16 = 1 << 2;
    pub const EXTERNAL: u16 = 1 << 3;
    pub const FINAL: u16 = 1 << 4;
    pub const SYNTHETIC: u16 = 1 << 5;
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AccountEventHeaderV1 {
    pub account_id: u32,
    pub kind: u16,
    pub flags: u16,
    pub account_generation: u64,
    pub account_epoch: u64,
    pub account_version: u64,
    pub exchange_ts: i64,
    pub receive_ts: i64,
}

impl AccountEventHeaderV1 {
    pub const ENCODED_LEN: usize = 48;
    pub fn encode_into(self, out: &mut [u8]) -> Result<(), &'static str> {
        let out = out
            .get_mut(..Self::ENCODED_LEN)
            .ok_or("account event header buffer is too small")?;
        put_u32(out, 0, self.account_id);
        put_u16(out, 4, self.kind);
        put_u16(out, 6, self.flags);
        put_u64(out, 8, self.account_generation);
        put_u64(out, 16, self.account_epoch);
        put_u64(out, 24, self.account_version);
        put_i64(out, 32, self.exchange_ts);
        put_i64(out, 40, self.receive_ts);
        Ok(())
    }
}

pub fn decode_account_event_header(input: &[u8]) -> Result<AccountEventHeaderV1, &'static str> {
    if input.len() < AccountEventHeaderV1::ENCODED_LEN {
        return Err("account event payload is shorter than its header");
    }
    Ok(AccountEventHeaderV1 {
        account_id: get_u32(input, 0),
        kind: get_u16(input, 4),
        flags: get_u16(input, 6),
        account_generation: get_u64(input, 8),
        account_epoch: get_u64(input, 16),
        account_version: get_u64(input, 24),
        exchange_ts: get_i64(input, 32),
        receive_ts: get_i64(input, 40),
    })
}

pub trait AccountEventPayload {
    const EVENT_TYPE: &'static str;
    const SCHEMA_VERSION: u32 = ACCOUNT_EVENT_SCHEMA_VERSION;
    const ENCODED_LEN: usize;
    fn header(&self) -> AccountEventHeaderV1;
    fn encode_into(&self, output: &mut [u8]) -> Result<(), &'static str>;
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OrderChangedV1 {
    pub header: AccountEventHeaderV1,
    pub asset_id: u32,
    pub side: u8,
    pub order_type: u8,
    pub time_in_force: u8,
    pub status: u8,
    pub price_ticks: i64,
    pub quantity_lots: i64,
    pub filled_quantity_lots: i64,
    pub average_price_ticks: i64,
    pub client_order_id: Id128,
    pub venue_order_id: Id128,
    pub command_id: Id128,
}

impl AccountEventPayload for OrderChangedV1 {
    const EVENT_TYPE: &'static str = ORDER_CHANGED_EVENT;
    const ENCODED_LEN: usize = 136;
    fn header(&self) -> AccountEventHeaderV1 {
        self.header
    }
    fn encode_into(&self, out: &mut [u8]) -> Result<(), &'static str> {
        require(out, Self::ENCODED_LEN)?;
        self.header.encode_into(out)?;
        put_u32(out, 48, self.asset_id);
        out[52] = self.side;
        out[53] = self.order_type;
        out[54] = self.time_in_force;
        out[55] = self.status;
        put_i64(out, 56, self.price_ticks);
        put_i64(out, 64, self.quantity_lots);
        put_i64(out, 72, self.filled_quantity_lots);
        put_i64(out, 80, self.average_price_ticks);
        put_id(out, 88, self.client_order_id);
        put_id(out, 104, self.venue_order_id);
        put_id(out, 120, self.command_id);
        Ok(())
    }
}

impl OrderChangedV1 {
    pub fn decode(input: &[u8]) -> Result<Self, &'static str> {
        require(input, Self::ENCODED_LEN)?;
        Ok(Self {
            header: decode_account_event_header(input)?,
            asset_id: get_u32(input, 48),
            side: input[52],
            order_type: input[53],
            time_in_force: input[54],
            status: input[55],
            price_ticks: get_i64(input, 56),
            quantity_lots: get_i64(input, 64),
            filled_quantity_lots: get_i64(input, 72),
            average_price_ticks: get_i64(input, 80),
            client_order_id: get_id(input, 88),
            venue_order_id: get_id(input, 104),
            command_id: get_id(input, 120),
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FillV1 {
    pub header: AccountEventHeaderV1,
    pub asset_id: u32,
    pub side: u8,
    pub liquidity: u8,
    pub price_ticks: i64,
    pub quantity_lots: i64,
    pub fee_amount_units: i64,
    pub fee_currency_id: u32,
    pub realized_pnl_units: i64,
    pub trade_id: Id128,
    pub venue_order_id: Id128,
    pub client_order_id: Id128,
    pub command_id: Id128,
}

impl AccountEventPayload for FillV1 {
    const EVENT_TYPE: &'static str = FILL_EVENT;
    const ENCODED_LEN: usize = 156;
    fn header(&self) -> AccountEventHeaderV1 {
        self.header
    }
    fn encode_into(&self, out: &mut [u8]) -> Result<(), &'static str> {
        require(out, Self::ENCODED_LEN)?;
        self.header.encode_into(out)?;
        put_u32(out, 48, self.asset_id);
        out[52] = self.side;
        out[53] = self.liquidity;
        out[54..56].fill(0);
        put_i64(out, 56, self.price_ticks);
        put_i64(out, 64, self.quantity_lots);
        put_i64(out, 72, self.fee_amount_units);
        put_u32(out, 80, self.fee_currency_id);
        put_i64(out, 84, self.realized_pnl_units);
        put_id(out, 92, self.trade_id);
        put_id(out, 108, self.venue_order_id);
        put_id(out, 124, self.client_order_id);
        put_id(out, 140, self.command_id);
        Ok(())
    }
}

impl FillV1 {
    pub fn decode(i: &[u8]) -> Result<Self, &'static str> {
        require(i, Self::ENCODED_LEN)?;
        Ok(Self {
            header: decode_account_event_header(i)?,
            asset_id: get_u32(i, 48),
            side: i[52],
            liquidity: i[53],
            price_ticks: get_i64(i, 56),
            quantity_lots: get_i64(i, 64),
            fee_amount_units: get_i64(i, 72),
            fee_currency_id: get_u32(i, 80),
            realized_pnl_units: get_i64(i, 84),
            trade_id: get_id(i, 92),
            venue_order_id: get_id(i, 108),
            client_order_id: get_id(i, 124),
            command_id: get_id(i, 140),
        })
    }
}

/// Fill@2 carries both the current execution delta and the order cumulative quantity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FillV2 {
    pub header: AccountEventHeaderV1,
    pub asset_id: u32,
    pub side: u8,
    pub liquidity: u8,
    pub price_ticks: i64,
    pub last_fill_quantity_lots: i64,
    pub cumulative_filled_quantity_lots: i64,
    pub fee_amount_units: i64,
    pub fee_currency_id: u32,
    pub realized_pnl_units: i64,
    pub trade_id: Id128,
    pub venue_order_id: Id128,
    pub client_order_id: Id128,
    pub command_id: Id128,
}

impl AccountEventPayload for FillV2 {
    const EVENT_TYPE: &'static str = FILL_EVENT;
    const SCHEMA_VERSION: u32 = FILL_EVENT_SCHEMA_VERSION;
    const ENCODED_LEN: usize = 164;

    fn header(&self) -> AccountEventHeaderV1 {
        self.header
    }

    fn encode_into(&self, out: &mut [u8]) -> Result<(), &'static str> {
        require(out, Self::ENCODED_LEN)?;
        self.header.encode_into(out)?;
        put_u32(out, 48, self.asset_id);
        out[52] = self.side;
        out[53] = self.liquidity;
        out[54..56].fill(0);
        put_i64(out, 56, self.price_ticks);
        put_i64(out, 64, self.last_fill_quantity_lots);
        put_i64(out, 72, self.cumulative_filled_quantity_lots);
        put_i64(out, 80, self.fee_amount_units);
        put_u32(out, 88, self.fee_currency_id);
        put_i64(out, 92, self.realized_pnl_units);
        put_id(out, 100, self.trade_id);
        put_id(out, 116, self.venue_order_id);
        put_id(out, 132, self.client_order_id);
        put_id(out, 148, self.command_id);
        Ok(())
    }
}

impl FillV2 {
    pub fn decode(input: &[u8]) -> Result<Self, &'static str> {
        require(input, Self::ENCODED_LEN)?;
        Ok(Self {
            header: decode_account_event_header(input)?,
            asset_id: get_u32(input, 48),
            side: input[52],
            liquidity: input[53],
            price_ticks: get_i64(input, 56),
            last_fill_quantity_lots: get_i64(input, 64),
            cumulative_filled_quantity_lots: get_i64(input, 72),
            fee_amount_units: get_i64(input, 80),
            fee_currency_id: get_u32(input, 88),
            realized_pnl_units: get_i64(input, 92),
            trade_id: get_id(input, 100),
            venue_order_id: get_id(input, 116),
            client_order_id: get_id(input, 132),
            command_id: get_id(input, 148),
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PositionChangedV1 {
    pub header: AccountEventHeaderV1,
    pub asset_id: u32,
    pub position_side: u8,
    pub margin_type: u8,
    pub quantity_lots: i64,
    pub entry_price_ticks: i64,
    pub liquidation_price_ticks: i64,
    pub realized_pnl_units: i64,
    pub unrealized_pnl_units: i64,
    pub margin_currency_id: u32,
}
impl AccountEventPayload for PositionChangedV1 {
    const EVENT_TYPE: &'static str = POSITION_CHANGED_EVENT;
    const ENCODED_LEN: usize = 100;
    fn header(&self) -> AccountEventHeaderV1 {
        self.header
    }
    fn encode_into(&self, o: &mut [u8]) -> Result<(), &'static str> {
        require(o, Self::ENCODED_LEN)?;
        self.header.encode_into(o)?;
        put_u32(o, 48, self.asset_id);
        o[52] = self.position_side;
        o[53] = self.margin_type;
        o[54..56].fill(0);
        put_i64(o, 56, self.quantity_lots);
        put_i64(o, 64, self.entry_price_ticks);
        put_i64(o, 72, self.liquidation_price_ticks);
        put_i64(o, 80, self.realized_pnl_units);
        put_i64(o, 88, self.unrealized_pnl_units);
        put_u32(o, 92, self.margin_currency_id);
        Ok(())
    }
}
impl PositionChangedV1 {
    pub fn decode(i: &[u8]) -> Result<Self, &'static str> {
        require(i, Self::ENCODED_LEN)?;
        Ok(Self {
            header: decode_account_event_header(i)?,
            asset_id: get_u32(i, 48),
            position_side: i[52],
            margin_type: i[53],
            quantity_lots: get_i64(i, 56),
            entry_price_ticks: get_i64(i, 64),
            liquidation_price_ticks: get_i64(i, 72),
            realized_pnl_units: get_i64(i, 80),
            unrealized_pnl_units: get_i64(i, 88),
            margin_currency_id: get_u32(i, 92),
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BalanceChangedV1 {
    pub header: AccountEventHeaderV1,
    pub currency_id: u32,
    pub wallet_units: i64,
    pub available_units: i64,
    pub margin_units: i64,
    pub unrealized_pnl_units: i64,
}
impl AccountEventPayload for BalanceChangedV1 {
    const EVENT_TYPE: &'static str = BALANCE_CHANGED_EVENT;
    const ENCODED_LEN: usize = 84;
    fn header(&self) -> AccountEventHeaderV1 {
        self.header
    }
    fn encode_into(&self, o: &mut [u8]) -> Result<(), &'static str> {
        require(o, Self::ENCODED_LEN)?;
        self.header.encode_into(o)?;
        put_u32(o, 48, self.currency_id);
        put_i64(o, 52, self.wallet_units);
        put_i64(o, 60, self.available_units);
        put_i64(o, 68, self.margin_units);
        put_i64(o, 76, self.unrealized_pnl_units);
        Ok(())
    }
}
impl BalanceChangedV1 {
    pub fn decode(i: &[u8]) -> Result<Self, &'static str> {
        require(i, Self::ENCODED_LEN)?;
        Ok(Self {
            header: decode_account_event_header(i)?,
            currency_id: get_u32(i, 48),
            wallet_units: get_i64(i, 52),
            available_units: get_i64(i, 60),
            margin_units: get_i64(i, 68),
            unrealized_pnl_units: get_i64(i, 76),
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CommandResultV1 {
    pub header: AccountEventHeaderV1,
    pub command_id: Id128,
    pub client_order_id: Id128,
    pub outcome: u8,
    pub final_result: u8,
    pub reason_code: u32,
}
impl AccountEventPayload for CommandResultV1 {
    const EVENT_TYPE: &'static str = COMMAND_RESULT_EVENT;
    const ENCODED_LEN: usize = 86;
    fn header(&self) -> AccountEventHeaderV1 {
        self.header
    }
    fn encode_into(&self, o: &mut [u8]) -> Result<(), &'static str> {
        require(o, Self::ENCODED_LEN)?;
        self.header.encode_into(o)?;
        put_id(o, 48, self.command_id);
        put_id(o, 64, self.client_order_id);
        o[80] = self.outcome;
        o[81] = self.final_result;
        put_u32(o, 82, self.reason_code);
        Ok(())
    }
}
impl CommandResultV1 {
    pub fn decode(i: &[u8]) -> Result<Self, &'static str> {
        require(i, Self::ENCODED_LEN)?;
        Ok(Self {
            header: decode_account_event_header(i)?,
            command_id: get_id(i, 48),
            client_order_id: get_id(i, 64),
            outcome: i[80],
            final_result: i[81],
            reason_code: get_u32(i, 82),
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReconcileV1 {
    pub header: AccountEventHeaderV1,
    pub terminal_version: u64,
    pub scope: u8,
    pub success: u8,
}
macro_rules! reconcile_payload {
    ($name:ident,$event:expr) => {
        #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
        pub struct $name(pub ReconcileV1);
        impl AccountEventPayload for $name {
            const EVENT_TYPE: &'static str = $event;
            const ENCODED_LEN: usize = 58;
            fn header(&self) -> AccountEventHeaderV1 {
                self.0.header
            }
            fn encode_into(&self, o: &mut [u8]) -> Result<(), &'static str> {
                require(o, Self::ENCODED_LEN)?;
                self.0.header.encode_into(o)?;
                put_u64(o, 48, self.0.terminal_version);
                o[56] = self.0.scope;
                o[57] = self.0.success;
                Ok(())
            }
        }
        impl $name {
            pub fn decode(i: &[u8]) -> Result<Self, &'static str> {
                require(i, Self::ENCODED_LEN)?;
                Ok(Self(ReconcileV1 {
                    header: decode_account_event_header(i)?,
                    terminal_version: get_u64(i, 48),
                    scope: i[56],
                    success: i[57],
                }))
            }
        }
    };
}
reconcile_payload!(ReconcileStartedV1, RECONCILE_STARTED_EVENT);
reconcile_payload!(ReconcileCompletedV1, RECONCILE_COMPLETED_EVENT);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StreamStateV1 {
    pub header: AccountEventHeaderV1,
    pub state: u8,
    pub reason_code: u32,
}
macro_rules! stream_payload {
    ($name:ident,$event:expr) => {
        #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
        pub struct $name(pub StreamStateV1);
        impl AccountEventPayload for $name {
            const EVENT_TYPE: &'static str = $event;
            const ENCODED_LEN: usize = 53;
            fn header(&self) -> AccountEventHeaderV1 {
                self.0.header
            }
            fn encode_into(&self, o: &mut [u8]) -> Result<(), &'static str> {
                require(o, Self::ENCODED_LEN)?;
                self.0.header.encode_into(o)?;
                o[48] = self.0.state;
                put_u32(o, 49, self.0.reason_code);
                Ok(())
            }
        }
        impl $name {
            pub fn decode(i: &[u8]) -> Result<Self, &'static str> {
                require(i, Self::ENCODED_LEN)?;
                Ok(Self(StreamStateV1 {
                    header: decode_account_event_header(i)?,
                    state: i[48],
                    reason_code: get_u32(i, 49),
                }))
            }
        }
    };
}
stream_payload!(StreamStateChangedV1, STREAM_STATE_CHANGED_EVENT);
stream_payload!(StreamInvalidatedV1, STREAM_INVALIDATED_EVENT);

fn require(input: &[u8], len: usize) -> Result<(), &'static str> {
    if input.len() < len {
        Err("account event buffer is too small")
    } else {
        Ok(())
    }
}
fn put_u16(o: &mut [u8], p: usize, v: u16) {
    o[p..p + 2].copy_from_slice(&v.to_le_bytes())
}
fn put_u32(o: &mut [u8], p: usize, v: u32) {
    o[p..p + 4].copy_from_slice(&v.to_le_bytes())
}
fn put_u64(o: &mut [u8], p: usize, v: u64) {
    o[p..p + 8].copy_from_slice(&v.to_le_bytes())
}
fn put_i64(o: &mut [u8], p: usize, v: i64) {
    o[p..p + 8].copy_from_slice(&v.to_le_bytes())
}
fn put_id(o: &mut [u8], p: usize, v: Id128) {
    o[p..p + 16].copy_from_slice(&v.0)
}
fn get_u16(i: &[u8], p: usize) -> u16 {
    u16::from_le_bytes(i[p..p + 2].try_into().unwrap())
}
fn get_u32(i: &[u8], p: usize) -> u32 {
    u32::from_le_bytes(i[p..p + 4].try_into().unwrap())
}
fn get_u64(i: &[u8], p: usize) -> u64 {
    u64::from_le_bytes(i[p..p + 8].try_into().unwrap())
}
fn get_i64(i: &[u8], p: usize) -> i64 {
    i64::from_le_bytes(i[p..p + 8].try_into().unwrap())
}
fn get_id(i: &[u8], p: usize) -> Id128 {
    Id128(i[p..p + 16].try_into().unwrap())
}
