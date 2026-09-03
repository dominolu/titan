use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use titan_account_plugin::{
    AccountCommandReceipt, AccountExecutionService, AccountHandle, AssetId, CancelOrderCommand,
    Id128, SubmitOrderCommand,
};
use titan_plugin_engine::TraceContext;
use titan_runtime_abi::{ORDER_COMMAND_CANCEL, ORDER_COMMAND_SUBMIT, OrderCommand};

use crate::*;

pub trait StrategyCommandGateway: Send + Sync {
    fn execute(
        &self,
        strategy: StrategyHandle,
        command: OrderCommand,
        trace: TraceContext,
    ) -> LocalResult<AccountCommandReceipt>;
    fn metadata(&self, strategy: StrategyHandle) -> StrategyCommandMetadata;
    fn restore_metadata(
        &self,
        strategy: StrategyHandle,
        metadata: &StrategyCommandMetadata,
    ) -> LocalResult<()>;
    fn cancel_owned_orders(&self, strategy: StrategyHandle) -> LocalResult<()>;
}

#[derive(Clone, Debug, Default)]
pub struct StrategyCommandMetadata {
    pub owned_orders: Arc<[StrategyOwnedOrder]>,
    pub pending_command_ids: Arc<[Id128]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StrategyOwnedOrder {
    pub client_order_id: Id128,
    pub local_account_no: u32,
    pub local_asset_no: u64,
    pub account: AccountHandle,
    pub asset_id: u32,
}

pub struct StandardStrategyCommandGateway {
    strategy: StrategyHandle,
    capabilities: StrategyCapabilities,
    command_gate: Arc<StrategyCommandGate>,
    accounts: BTreeMap<u32, (AccountHandle, BTreeMap<u64, u32>)>,
    execution: Arc<dyn AccountExecutionService>,
    owned_orders: Mutex<Vec<StrategyOwnedOrder>>,
    pending_commands: Mutex<Vec<Id128>>,
}

impl StandardStrategyCommandGateway {
    pub fn new(
        strategy: StrategyHandle,
        capabilities: StrategyCapabilities,
        command_gate: Arc<StrategyCommandGate>,
        accounts: &[ResolvedAccountBinding],
        execution: Arc<dyn AccountExecutionService>,
    ) -> Self {
        let accounts = accounts
            .iter()
            .map(|binding| {
                (
                    binding.local_account_no,
                    (
                        binding.account,
                        binding
                            .tradable_assets
                            .iter()
                            .map(|asset| (u64::from(asset.local_asset_no), asset.asset_id))
                            .collect(),
                    ),
                )
            })
            .collect();
        Self {
            strategy,
            capabilities,
            command_gate,
            accounts,
            execution,
            owned_orders: Mutex::new(Vec::new()),
            pending_commands: Mutex::new(Vec::new()),
        }
    }
}

impl StrategyCommandGateway for StandardStrategyCommandGateway {
    fn execute(
        &self,
        strategy: StrategyHandle,
        command: OrderCommand,
        trace: TraceContext,
    ) -> LocalResult<AccountCommandReceipt> {
        if strategy != self.strategy || self.command_gate.owner() != strategy {
            return Err(gateway_error(
                StrategyErrorKind::StaleHandle,
                "owner_mismatch",
            ));
        }
        if !self.command_gate.is_open() {
            return Err(gateway_error(
                StrategyErrorKind::InvalidState,
                "command_gate_closed",
            ));
        }
        let (account, assets) = self
            .accounts
            .get(&command.local_account_no)
            .ok_or_else(|| {
                gateway_error(StrategyErrorKind::InvalidDefinition, "account_not_bound")
            })?;
        let asset_id = *assets.get(&command.asset_no).ok_or_else(|| {
            gateway_error(StrategyErrorKind::InvalidDefinition, "asset_not_bound")
        })?;
        let required = match command.kind {
            ORDER_COMMAND_SUBMIT => {
                if !command.qty.is_finite() || command.qty <= 0.0 || !command.price.is_finite() {
                    return Err(gateway_error(
                        StrategyErrorKind::InvalidDefinition,
                        "invalid_numeric_command",
                    ));
                }
                StrategyCapabilities::SUBMIT_ORDER
            }
            ORDER_COMMAND_CANCEL => {
                if command.order_id == 0 {
                    return Err(gateway_error(
                        StrategyErrorKind::InvalidDefinition,
                        "invalid_order_id",
                    ));
                }
                StrategyCapabilities::CANCEL_ORDER
            }
            _ => {
                return Err(gateway_error(
                    StrategyErrorKind::InvalidDefinition,
                    "invalid_command_kind",
                ));
            }
        };
        if !self.capabilities.contains(required) {
            return Err(gateway_error(
                StrategyErrorKind::UnsupportedCapability,
                "command_not_authorized",
            ));
        }
        let command_id = owner_id(strategy, command.order_id, command.kind);
        let client_order_id = owner_id(strategy, command.order_id, 0);
        {
            let mut pending = self
                .pending_commands
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if pending.contains(&command_id) {
                return Err(gateway_error(
                    StrategyErrorKind::InvalidDefinition,
                    "duplicate_command_id",
                ));
            }
            pending.push(command_id);
        }
        let result = match command.kind {
            ORDER_COMMAND_SUBMIT => self
                .execution
                .submit(
                    *account,
                    SubmitOrderCommand {
                        command_id,
                        client_order_id: Some(client_order_id),
                        asset_id: AssetId(asset_id),
                        side: command.side as u8,
                        order_type: command.order_type,
                        time_in_force: command.time_in_force,
                        price_ticks: exact_i64(command.price, "price")?,
                        quantity_lots: exact_i64(command.qty, "quantity")?,
                        trace,
                    },
                )
                .map_err(account_error),
            ORDER_COMMAND_CANCEL => self
                .execution
                .cancel(
                    *account,
                    CancelOrderCommand {
                        command_id,
                        asset_id: AssetId(asset_id),
                        client_order_id: Some(client_order_id),
                        venue_order_id: None,
                        trace,
                    },
                )
                .map_err(account_error),
            _ => unreachable!(),
        };
        self.pending_commands
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|id| *id != command_id);
        if result.is_ok() && command.kind == ORDER_COMMAND_SUBMIT {
            let mut owned = self.owned_orders.lock().unwrap_or_else(|p| p.into_inner());
            if !owned
                .iter()
                .any(|order| order.client_order_id == client_order_id)
            {
                owned.push(StrategyOwnedOrder {
                    client_order_id,
                    local_account_no: command.local_account_no,
                    local_asset_no: command.asset_no,
                    account: *account,
                    asset_id,
                });
            }
        }
        result
    }

    fn metadata(&self, strategy: StrategyHandle) -> StrategyCommandMetadata {
        if strategy != self.strategy {
            return StrategyCommandMetadata::default();
        }
        StrategyCommandMetadata {
            owned_orders: self
                .owned_orders
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone()
                .into(),
            pending_command_ids: self
                .pending_commands
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone()
                .into(),
        }
    }

    fn restore_metadata(
        &self,
        strategy: StrategyHandle,
        metadata: &StrategyCommandMetadata,
    ) -> LocalResult<()> {
        if strategy != self.strategy {
            return Err(gateway_error(
                StrategyErrorKind::StaleHandle,
                "owner_mismatch",
            ));
        }
        if !metadata.pending_command_ids.is_empty() {
            return Err(gateway_error(
                StrategyErrorKind::CheckpointFailed,
                "pending_commands_require_reconciliation",
            ));
        }
        for order in metadata.owned_orders.iter() {
            let Some((account, assets)) = self.accounts.get(&order.local_account_no) else {
                return Err(gateway_error(
                    StrategyErrorKind::CheckpointFailed,
                    "checkpoint_account_not_bound",
                ));
            };
            if account != &order.account
                || assets.get(&order.local_asset_no).copied() != Some(order.asset_id)
            {
                return Err(gateway_error(
                    StrategyErrorKind::CheckpointFailed,
                    "checkpoint_asset_not_bound",
                ));
            }
        }
        *self.owned_orders.lock().unwrap_or_else(|p| p.into_inner()) =
            metadata.owned_orders.to_vec();
        Ok(())
    }

    fn cancel_owned_orders(&self, strategy: StrategyHandle) -> LocalResult<()> {
        if strategy != self.strategy {
            return Err(gateway_error(
                StrategyErrorKind::StaleHandle,
                "owner_mismatch",
            ));
        }
        let owned = self
            .owned_orders
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        for owned_order in owned {
            let trace = TraceContext::default();
            let client_order_id = owned_order.client_order_id;
            let order_id = u64::from_le_bytes(client_order_id.0[8..16].try_into().unwrap());
            self.execution
                .cancel(
                    owned_order.account,
                    CancelOrderCommand {
                        command_id: owner_id(strategy, order_id, 0xfe),
                        asset_id: AssetId(owned_order.asset_id),
                        client_order_id: Some(client_order_id),
                        venue_order_id: None,
                        trace,
                    },
                )
                .map_err(account_error)?;
        }
        Ok(())
    }
}

fn owner_id(strategy: StrategyHandle, order_id: u64, discriminator: u8) -> Id128 {
    let mut value = [0_u8; 16];
    value[..4].copy_from_slice(&strategy.strategy_id.0.to_le_bytes());
    value[4..12].copy_from_slice(&strategy.generation.to_le_bytes());
    let folded = (order_id as u32) ^ ((order_id >> 32) as u32);
    value[12..].copy_from_slice(&folded.to_le_bytes());
    value[15] ^= discriminator;
    Id128(value)
}

fn exact_i64(value: f64, field: &'static str) -> LocalResult<i64> {
    if value < i64::MIN as f64 || value > i64::MAX as f64 || value.fract() != 0.0 {
        return Err(gateway_error(StrategyErrorKind::InvalidDefinition, field));
    }
    Ok(value as i64)
}

fn account_error(error: titan_account_plugin::AccountError) -> StrategyError {
    gateway_error(
        if error.kind == titan_account_plugin::AccountErrorKind::QueueFull {
            StrategyErrorKind::ExecutionQueueFull
        } else {
            StrategyErrorKind::Internal
        },
        "account_execution_failed",
    )
}

fn gateway_error(kind: StrategyErrorKind, code: &'static str) -> StrategyError {
    StrategyError::new(
        kind,
        "command_gateway",
        code,
        "strategy command was rejected",
    )
}
