//! One committed account image. Callers hold the same lock for facts and snapshot boundaries.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use titan_account_service as account;

const TERMINAL_ORDER_HISTORY: usize = 1024;

#[derive(Default)]
pub(crate) struct AccountProjection {
    pub epoch: u64,
    orders_version: u64,
    positions_version: u64,
    balances_version: u64,
    orders: BTreeMap<(u32, [u8; 16]), account::OrderSnapshot>,
    positions: BTreeMap<u32, account::PositionSnapshot>,
    balances: BTreeMap<u32, account::BalanceSnapshot>,
    terminal_orders: VecDeque<(u32, [u8; 16])>,
    order_times: BTreeMap<(u32, [u8; 16]), i64>,
    position_times: BTreeMap<u32, i64>,
    balance_times: BTreeMap<u32, i64>,
}

impl AccountProjection {
    pub fn cumulative(&self, asset_id: u32, venue_order_id: account::Id128) -> i64 {
        self.orders
            .get(&(asset_id, venue_order_id.0))
            .map_or(0, |order| order.filled_quantity_lots)
    }

    pub fn balance_image(&self, currency_id: u32) -> Option<&account::BalanceSnapshot> {
        self.balances.get(&currency_id)
    }
    pub fn position_image(&self, asset_id: u32) -> Option<&account::PositionSnapshot> {
        self.positions.get(&asset_id)
    }
    pub fn retained_ids(&self) -> std::collections::HashSet<account::Id128> {
        self.orders
            .values()
            .flat_map(|order| [order.client_order_id, order.venue_order_id])
            .collect()
    }
    pub fn position_is_stale(&self, asset: u32, ts: i64) -> bool {
        self.position_times
            .get(&asset)
            .is_some_and(|previous| ts > 0 && ts < *previous)
    }
    pub fn balance_is_stale(&self, currency: u32, ts: i64) -> bool {
        self.balance_times
            .get(&currency)
            .is_some_and(|previous| ts > 0 && ts < *previous)
    }

    pub fn order_is_stale(
        &self,
        asset: u32,
        id: account::Id128,
        timestamp: i64,
        cumulative: i64,
    ) -> bool {
        self.order_times
            .get(&(asset, id.0))
            .is_some_and(|previous| timestamp > 0 && timestamp < *previous)
            || cumulative < self.cumulative(asset, id)
    }

    pub fn order_time(&self, value: &account::OrderSnapshot) -> i64 {
        self.order_times
            .get(&(value.asset_id.0, value.venue_order_id.0))
            .copied()
            .unwrap_or(0)
    }
    pub fn position_time(&self, value: &account::PositionSnapshot) -> i64 {
        self.position_times
            .get(&value.asset_id.0)
            .copied()
            .unwrap_or(0)
    }
    pub fn balance_time(&self, value: &account::BalanceSnapshot) -> i64 {
        self.balance_times
            .get(&value.currency_id.0)
            .copied()
            .unwrap_or(0)
    }

    pub fn order(&mut self, event: &account::OrderChangedV1) {
        let key = (event.asset_id, event.venue_order_id.0);
        self.order_times.insert(key, event.header.exchange_ts);
        let reduce_only = event.header.flags & account::event_flags::REDUCE_ONLY != 0
            || self.orders.get(&key).is_some_and(|value| value.reduce_only);
        let was_terminal = self
            .orders
            .get(&key)
            .is_some_and(|value| !matches!(value.status, 1 | 5));
        self.orders.insert(
            key,
            account::OrderSnapshot {
                asset_id: account::AssetId(event.asset_id),
                side: event.side,
                order_type: event.order_type,
                time_in_force: event.time_in_force,
                status: event.status,
                price_ticks: event.price_ticks,
                quantity_lots: event.quantity_lots,
                filled_quantity_lots: event.filled_quantity_lots,
                reduce_only,
                client_order_id: event.client_order_id,
                venue_order_id: event.venue_order_id,
                command_id: event.command_id,
            },
        );
        if !matches!(event.status, 1 | 5) && !was_terminal {
            self.terminal_orders.push_back(key);
        }
        while self.terminal_orders.len() > TERMINAL_ORDER_HISTORY {
            if let Some(key) = self.terminal_orders.pop_front()
                && self
                    .orders
                    .get(&key)
                    .is_some_and(|value| !matches!(value.status, 1 | 5))
            {
                self.orders.remove(&key);
                self.order_times.remove(&key);
            }
        }
        self.epoch = event.header.account_epoch;
        self.orders_version = event.header.account_version;
    }

    pub fn position(&mut self, event: &account::PositionChangedV1) -> bool {
        if self.position_is_stale(event.asset_id, event.header.exchange_ts) {
            return false;
        }
        self.position_times
            .insert(event.asset_id, event.header.exchange_ts);
        self.positions.insert(
            event.asset_id,
            account::PositionSnapshot {
                asset_id: account::AssetId(event.asset_id),
                position_side: event.position_side,
                margin_type: event.margin_type,
                quantity_lots: event.quantity_lots,
                entry_price_ticks: event.entry_price_ticks,
                liquidation_price_ticks: event.liquidation_price_ticks,
                realized_pnl_units: event.realized_pnl_units,
                unrealized_pnl_units: event.unrealized_pnl_units,
                margin_currency_id: account::CurrencyId(event.margin_currency_id),
            },
        );
        self.epoch = event.header.account_epoch;
        self.positions_version = event.header.account_version;
        true
    }

    pub fn balance(&mut self, event: &account::BalanceChangedV1) -> bool {
        if self.balance_is_stale(event.currency_id, event.header.exchange_ts) {
            return false;
        }
        self.balance_times
            .insert(event.currency_id, event.header.exchange_ts);
        self.balances.insert(
            event.currency_id,
            account::BalanceSnapshot {
                currency_id: account::CurrencyId(event.currency_id),
                wallet_units: event.wallet_units,
                available_units: event.available_units,
                margin_units: event.margin_units,
                unrealized_pnl_units: event.unrealized_pnl_units,
            },
        );
        self.epoch = event.header.account_epoch;
        self.balances_version = event.header.account_version;
        true
    }

    pub fn orders(&self, filter: account::OrderFilter) -> (u64, Arc<[account::OrderSnapshot]>) {
        (
            self.orders_version,
            self.orders
                .values()
                .filter(|order| {
                    filter.asset_id.is_none_or(|asset| asset == order.asset_id)
                        && (filter.include_final || matches!(order.status, 1 | 5))
                })
                .cloned()
                .collect::<Vec<_>>()
                .into(),
        )
    }

    pub fn positions(
        &self,
        filter: account::PositionFilter,
    ) -> (u64, Arc<[account::PositionSnapshot]>) {
        (
            self.positions_version,
            self.positions
                .values()
                .filter(|value| filter.asset_id.is_none_or(|asset| asset == value.asset_id))
                .cloned()
                .collect::<Vec<_>>()
                .into(),
        )
    }

    pub fn balances(&self) -> (u64, Arc<[account::BalanceSnapshot]>) {
        (
            self.balances_version,
            self.balances.values().cloned().collect::<Vec<_>>().into(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_versions_follow_their_own_facts_and_history_is_bounded() {
        let mut image = AccountProjection::default();
        for id in 1..=TERMINAL_ORDER_HISTORY + 20 {
            image.order(&account::OrderChangedV1 {
                header: account::AccountEventHeaderV1 {
                    account_epoch: 2,
                    account_version: id as u64,
                    ..Default::default()
                },
                venue_order_id: account::Id128((id as u128).to_le_bytes()),
                status: 3,
                ..Default::default()
            });
        }
        let orders_version = image.orders_version;
        image.position(&account::PositionChangedV1 {
            header: account::AccountEventHeaderV1 {
                account_epoch: 2,
                account_version: 3000,
                ..Default::default()
            },
            quantity_lots: 5,
            ..Default::default()
        });
        assert_eq!(image.orders_version, orders_version);
        assert_eq!(image.positions_version, 3000);
        assert_eq!(
            image
                .orders(account::OrderFilter {
                    asset_id: None,
                    include_final: true
                })
                .1
                .len(),
            TERMINAL_ORDER_HISTORY
        );
    }

    #[test]
    fn signed_position_updates_replace_the_asset_image() {
        let mut image = AccountProjection::default();
        for (side, qty) in [(1, 5), (2, -3), (0, 0)] {
            image.position(&account::PositionChangedV1 {
                asset_id: 7,
                position_side: side,
                quantity_lots: qty,
                ..Default::default()
            });
        }
        let (_, positions) = image.positions(account::PositionFilter::default());
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].quantity_lots, 0);
    }

    #[test]
    fn older_buffered_facts_do_not_overwrite_reconciled_images() {
        let mut image = AccountProjection::default();
        let header = account::AccountEventHeaderV1 {
            exchange_ts: 100,
            account_epoch: 2,
            account_version: 5,
            ..Default::default()
        };
        assert!(image.position(&account::PositionChangedV1 {
            header,
            asset_id: 7,
            quantity_lots: 20,
            ..Default::default()
        }));
        assert!(!image.position(&account::PositionChangedV1 {
            header: account::AccountEventHeaderV1 {
                exchange_ts: 90,
                account_version: 6,
                ..header
            },
            asset_id: 7,
            quantity_lots: 10,
            ..Default::default()
        }));
        assert_eq!(image.positions(account::PositionFilter::default()).0, 5);
        assert_eq!(
            image.positions(account::PositionFilter::default()).1[0].quantity_lots,
            20
        );
    }

    #[test]
    fn concurrent_snapshots_capture_matching_data_and_version() {
        let image = Arc::new(std::sync::Mutex::new(AccountProjection::default()));
        let writer_image = image.clone();
        let writer = std::thread::spawn(move || {
            for version in 1..=1000 {
                writer_image
                    .lock()
                    .unwrap()
                    .position(&account::PositionChangedV1 {
                        header: account::AccountEventHeaderV1 {
                            account_epoch: 1,
                            account_version: version,
                            ..Default::default()
                        },
                        asset_id: 7,
                        quantity_lots: version as i64,
                        ..Default::default()
                    });
            }
        });
        for _ in 0..1000 {
            let image = image.lock().unwrap();
            let (version, items) = image.positions(account::PositionFilter::default());
            if let Some(position) = items.first() {
                assert_eq!(position.quantity_lots, version as i64);
                assert_eq!(image.epoch, 1);
            }
        }
        writer.join().unwrap();
        assert_eq!(
            image
                .lock()
                .unwrap()
                .positions(account::PositionFilter::default())
                .0,
            1000
        );
    }
}
