use std::sync::{Arc, Mutex};

use crate::{
    binancefutures::{BinanceFuturesError, msg::stream::OrderTradeUpdate},
    connector::GetOrders,
    utils::{RefSymbolOrderId, SymbolOrderId},
};
use chrono::Utc;
use hashbrown::HashMap;
use hftbacktest::types::{Order, Status};

#[derive(Debug)]
struct OrderExt {
    symbol: String,
    order: Order,
    removed_by_ws: bool,
    removed_by_rest: bool,
}

pub type SharedOrderManager = Arc<Mutex<OrderManager>>;

pub type ClientOrderId = String;

/// Tracks orders created by the AccountPlugin REST facade so private-stream Order/Fill updates can
/// be correlated back to the deterministic owner client id. Deletions that are terminal in the
/// private stream are kept for GC (rather than immediately removed) so the connector never emits a
/// duplicate publication if a stale frame arrives from another channel.
#[derive(Default, Debug)]
pub struct OrderManager {
    prefix: String,
    orders: HashMap<ClientOrderId, OrderExt>,
    order_id_map: HashMap<SymbolOrderId, ClientOrderId>,
}

impl OrderManager {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            orders: Default::default(),
            order_id_map: Default::default(),
        }
    }

    pub fn update_from_ws(
        &mut self,
        resp: &OrderTradeUpdate,
    ) -> Result<Option<Order>, BinanceFuturesError> {
        if !self.orders.contains_key(&resp.order.client_order_id)
            && !resp.order.client_order_id.starts_with(&self.prefix)
        {
            return Err(BinanceFuturesError::PrefixUnmatched);
        }
        let order_ext = self
            .orders
            .get_mut(&resp.order.client_order_id)
            .ok_or(BinanceFuturesError::OrderNotFound)?;

        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;
        if resp.transaction_time * 1_000_000 >= order_ext.order.exch_timestamp {
            order_ext.order.qty = resp.order.original_qty;
            order_ext.order.leaves_qty =
                resp.order.original_qty - resp.order.order_filled_accumulated_qty;
            order_ext.order.side = resp.order.side;
            order_ext.order.time_in_force = resp.order.time_in_force;
            order_ext.order.exch_timestamp = resp.transaction_time * 1_000_000;
            order_ext.order.status = resp.order.order_status;
            order_ext.order.exec_price_tick =
                (resp.order.last_filled_price / order_ext.order.tick_size).round() as i64;
            order_ext.order.exec_qty = resp.order.order_last_filled_qty;
            order_ext.order.order_type = resp.order.order_type;
        }

        let result = if already_removed {
            None
        } else {
            Some(order_ext.order.clone())
        };

        if order_ext.order.status != Status::New
            && order_ext.order.status != Status::PartiallyFilled
        {
            order_ext.removed_by_ws = true;
            if !already_removed {
                self.order_id_map.remove(&RefSymbolOrderId::new(
                    &order_ext.symbol,
                    order_ext.order.order_id,
                ));
            }

            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                self.orders.remove(&resp.order.client_order_id).unwrap();
            }
        }

        Ok(result)
    }

    pub fn track_managed_order(
        &mut self,
        symbol: &str,
        client_order_id: &str,
        order: Order,
    ) -> bool {
        if self.orders.contains_key(client_order_id) {
            return false;
        }
        self.order_id_map.insert(
            SymbolOrderId::new(symbol.to_owned(), order.order_id),
            client_order_id.to_owned(),
        );
        self.orders.insert(
            client_order_id.to_owned(),
            OrderExt {
                symbol: symbol.to_owned(),
                order,
                removed_by_ws: false,
                removed_by_rest: false,
            },
        );
        true
    }

    /// Removes terminal orders that were only deleted by one channel after a staleness threshold.
    pub fn gc(&mut self) {
        let now = Utc::now().timestamp_nanos_opt().unwrap();
        let stale_ts = now - 300_000_000_000;
        let stale_ids: Vec<(_, _)> = self
            .orders
            .iter()
            .filter(|&(_, wrapper)| {
                wrapper.order.status != Status::New
                    && wrapper.order.status != Status::PartiallyFilled
                    && wrapper.order.status != Status::Unsupported
                    && wrapper.order.exch_timestamp < stale_ts
            })
            .map(|(client_order_id, wrapper)| {
                (
                    client_order_id.clone(),
                    SymbolOrderId::new(wrapper.symbol.clone(), wrapper.order.order_id),
                )
            })
            .collect();
        for (client_order_id, order_id) in stale_ids.iter() {
            if self.order_id_map.contains_key(order_id) {
                self.order_id_map.remove(order_id).unwrap();
            }
            self.orders.remove(client_order_id);
        }
    }
}

impl GetOrders for OrderManager {
    fn orders(&self, symbol: Option<String>) -> Vec<Order> {
        self.orders
            .iter()
            .filter(|(_, order)| {
                symbol.as_ref().map(|s| order.symbol == *s).unwrap_or(true) && order.order.active()
            })
            .map(|(_, order)| &order.order)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hftbacktest::types::{OrdType, Side, TimeInForce};

    fn ws_update(client_order_id: &str, transaction_time: i64, status: &str) -> OrderTradeUpdate {
        serde_json::from_str(
            &serde_json::json!({
                "E": transaction_time,
                "T": transaction_time,
                "o": {
                    "s": "BTCUSDT",
                    "c": client_order_id,
                    "S": "BUY",
                    "o": "LIMIT",
                    "f": "GTC",
                    "q": "1",
                    "p": "100",
                    "ap": "100",
                    "sp": "0",
                    "x": "TRADE",
                    "X": status,
                    "i": 7,
                    "l": "0.5",
                    "z": "0.5",
                    "L": "100",
                    "T": transaction_time,
                    "t": 11
                }
            })
            .to_string(),
        )
        .unwrap()
    }

    #[test]
    fn tracked_rest_order_flows_through_private_ws_without_legacy_prefix() {
        let mut manager = OrderManager::new("t-");
        let client_order_id = "0123456789abcdef0123456789abcdef";
        let order = {
            let mut order = Order::new(
                9,
                100,
                1.0,
                1.0,
                Side::Buy,
                OrdType::Limit,
                TimeInForce::GTC,
            );
            order.status = Status::New;
            order
        };
        assert!(manager.track_managed_order("BTCUSDT", client_order_id, order));

        let updated = manager
            .update_from_ws(&ws_update(client_order_id, 20, "FILLED"))
            .unwrap()
            .unwrap();
        assert_eq!(updated.status, Status::Filled);
        assert_eq!(updated.leaves_qty, 0.5);
    }
}
