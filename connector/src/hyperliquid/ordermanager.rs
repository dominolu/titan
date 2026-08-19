use std::sync::{Arc, Mutex};

use chrono::Utc;
use hashbrown::HashMap;
use hftbacktest::types::{Order, OrderId, Status};
use rand::Rng;

use crate::{
    connector::GetOrders,
    hyperliquid::{
        HyperliquidError,
        msg::{OrderState, OrderStatus},
    },
    utils::{RefSymbolOrderId, SymbolOrderId},
};

#[derive(Debug)]
struct OrderExt {
    symbol: String,
    order: Order,
    oid: Option<u64>,
    removed_by_ws: bool,
    removed_by_rest: bool,
}

pub type SharedOrderManager = Arc<Mutex<OrderManager>>;

pub type Cloid = String;

fn from_str_to_status(status: &str) -> Status {
    match status {
        "open" => Status::New,
        "filled" => Status::Filled,
        "canceled" => Status::Canceled,
        "rejected" => Status::Rejected,
        _ => Status::Unsupported,
    }
}

/// Hyperliquid has two channels for order state: the synchronous `POST /exchange` response and
/// the asynchronous `orderUpdates` WebSocket channel. Deletions (cancel/fill) are confirmed by
/// both channels before the order is removed from memory, preventing ghost orders.
#[derive(Default, Debug)]
pub struct OrderManager {
    orders: HashMap<Cloid, OrderExt>,
    order_id_map: HashMap<SymbolOrderId, Cloid>,
}

impl OrderManager {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn prepare_cloid(&mut self, symbol: String, order: Order) -> Option<String> {
        let symbol_order_id = SymbolOrderId::new(symbol.clone(), order.order_id);
        if self.order_id_map.contains_key(&symbol_order_id) {
            return None;
        }

        let mut rng = rand::rng();
        // Hyperliquid expects the cloid as "0x" + 32 lowercase hex chars (16 bytes). The exchange
        // normalizes this value when rebuilding the msgpack action for signature verification, so
        // the exact format must match the official SDK's Cloid.
        let cloid = format!("0x{:032x}", rng.random::<u128>());
        if self.orders.contains_key(&cloid) {
            return None;
        }

        self.order_id_map.insert(symbol_order_id, cloid.clone());
        self.orders.insert(
            cloid.clone(),
            OrderExt {
                symbol,
                order,
                oid: None,
                removed_by_ws: false,
                removed_by_rest: false,
            },
        );
        Some(cloid)
    }

    pub fn get_cloid(&self, symbol: &str, order_id: OrderId) -> Option<String> {
        self.order_id_map
            .get(&RefSymbolOrderId::new(symbol, order_id))
            .cloned()
    }

    pub fn get_oid(&self, symbol: &str, order_id: OrderId) -> Option<u64> {
        let cloid = self.get_cloid(symbol, order_id)?;
        self.orders.get(&cloid).and_then(|ext| ext.oid)
    }

    /// Updates the order with an `orderUpdates` WebSocket event.
    pub fn update_from_ws(
        &mut self,
        state: &OrderState,
        status: &str,
    ) -> Result<Option<Order>, HyperliquidError> {
        let cloid = match state.cloid.clone() {
            Some(cloid) if self.orders.contains_key(&cloid) => cloid,
            // Orders placed without a client id (e.g. by another tool) must be matched by oid.
            _ => self
                .orders
                .iter()
                .find(|(_, ext)| state.oid != 0 && ext.oid == Some(state.oid))
                .map(|(cloid, _)| cloid.clone())
                .ok_or(HyperliquidError::OrderNotFound)?,
        };
        let order_ext = self
            .orders
            .get_mut(&cloid)
            .ok_or(HyperliquidError::OrderNotFound)?;
        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;

        if state.oid != 0 {
            order_ext.oid = Some(state.oid);
        }
        if (state.timestamp * 1_000_000) as i64 >= order_ext.order.exch_timestamp {
            order_ext.order.status = from_str_to_status(status);
            order_ext.order.exec_qty = state.filled.parse().unwrap_or(0.0);
            order_ext.order.leaves_qty =
                state.sz.parse::<f64>().unwrap_or(0.0) - order_ext.order.exec_qty;
            order_ext.order.exec_price_tick = (state.avg_px.parse::<f64>().unwrap_or(0.0)
                / order_ext.order.tick_size)
                .round() as i64;
            order_ext.order.exch_timestamp = (state.timestamp * 1_000_000) as i64;
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
                self.orders.remove(&cloid).unwrap();
            }
        }

        Ok(result)
    }

    /// Updates the order with the synchronous `POST /exchange` order response.
    pub fn update_from_exchange_submit(
        &mut self,
        cloid: &str,
        status: &OrderStatus,
    ) -> Result<Option<Order>, HyperliquidError> {
        let order_ext = self
            .orders
            .get_mut(cloid)
            .ok_or(HyperliquidError::OrderNotFound)?;
        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;

        order_ext.order.req = Status::None;
        match status {
            OrderStatus::Resting { resting } => {
                order_ext.oid = Some(resting.oid);
                if !already_removed {
                    order_ext.order.status = Status::New;
                }
            }
            OrderStatus::Filled { filled } => {
                order_ext.oid = Some(filled.oid);
                order_ext.order.status = Status::Filled;
                order_ext.order.exec_qty = filled.total_sz.parse().unwrap_or(0.0);
                order_ext.order.leaves_qty = 0.0;
                order_ext.order.exec_price_tick = (filled.avg_px.parse::<f64>().unwrap_or(0.0)
                    / order_ext.order.tick_size)
                    .round() as i64;
                order_ext.order.exch_timestamp = Utc::now().timestamp_nanos_opt().unwrap();
                order_ext.removed_by_rest = true;
            }
            OrderStatus::Error { .. } => {
                order_ext.order.status = Status::Rejected;
                order_ext.removed_by_rest = true;
            }
        }

        let result = if already_removed {
            None
        } else {
            Some(order_ext.order.clone())
        };

        if order_ext.removed_by_rest {
            if !already_removed {
                self.order_id_map.remove(&RefSymbolOrderId::new(
                    &order_ext.symbol,
                    order_ext.order.order_id,
                ));
            }
            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                self.orders.remove(cloid).unwrap();
            }
        }

        Ok(result)
    }

    /// Updates the order with the synchronous `POST /exchange` cancel response.
    pub fn update_from_exchange_cancel(
        &mut self,
        cloid: &str,
        success: bool,
    ) -> Result<Option<Order>, HyperliquidError> {
        let order_ext = self
            .orders
            .get_mut(cloid)
            .ok_or(HyperliquidError::OrderNotFound)?;
        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;
        order_ext.order.req = Status::None;
        if success {
            order_ext.order.status = Status::Canceled;
            order_ext.removed_by_rest = true;
        }

        let result = if already_removed {
            None
        } else {
            Some(order_ext.order.clone())
        };

        if order_ext.removed_by_rest {
            if !already_removed {
                self.order_id_map.remove(&RefSymbolOrderId::new(
                    &order_ext.symbol,
                    order_ext.order.order_id,
                ));
            }
            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                self.orders.remove(cloid).unwrap();
            }
        }

        Ok(result)
    }

    pub fn update_cancel_fail(&mut self, cloid: &str) -> Option<Order> {
        let order_ext = self.orders.get_mut(cloid)?;
        order_ext.order.req = Status::None;
        Some(order_ext.order.clone())
    }

    pub fn cancel_all(&mut self, symbol: &str) -> Vec<Order> {
        let mut removed_order_ids = Vec::new();
        let mut removed_orders = Vec::new();
        for (cloid, order_ext) in &mut self.orders {
            if order_ext.symbol != symbol {
                continue;
            }
            let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;
            order_ext.removed_by_rest = true;
            order_ext.order.status = Status::Canceled;
            order_ext.order.req = Status::None;
            order_ext.order.exch_timestamp = Utc::now().timestamp_nanos_opt().unwrap();
            if !already_removed {
                self.order_id_map
                    .remove(&RefSymbolOrderId::new(symbol, order_ext.order.order_id));
                removed_orders.push(order_ext.order.clone());
            }
            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                removed_order_ids.push(cloid.clone());
            }
        }
        for cloid in removed_order_ids {
            self.orders.remove(&cloid).unwrap();
        }
        removed_orders
    }

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
            .map(|(cloid, wrapper)| {
                (
                    cloid.clone(),
                    SymbolOrderId::new(wrapper.symbol.clone(), wrapper.order.order_id),
                )
            })
            .collect();
        for (cloid, order_id) in stale_ids.iter() {
            self.order_id_map.remove(order_id);
            self.orders.remove(cloid);
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
    use crate::hyperliquid::msg::{Filled, Resting};
    use hftbacktest::types::{OrdType, Side, TimeInForce};

    fn test_order(order_id: u64) -> Order {
        Order::new(
            order_id,
            300_000,
            0.1,
            1.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        )
    }

    fn order_state(cloid: Option<String>, oid: u64) -> OrderState {
        OrderState {
            coin: "BTC".to_string(),
            side: "B".to_string(),
            limit_px: "30000".to_string(),
            sz: "1.0".to_string(),
            oid,
            timestamp: 1_000,
            orig_sz: "1.0".to_string(),
            filled: "0".to_string(),
            avg_px: "0".to_string(),
            cloid,
            reduce_only: false,
            tif: Some("Gtc".to_string()),
        }
    }

    #[test]
    fn test_update_from_ws_matches_by_cloid() {
        let mut manager = OrderManager::new();
        let cloid = manager
            .prepare_cloid("BTC".to_string(), test_order(1))
            .unwrap();
        manager
            .update_from_exchange_submit(
                &cloid,
                &OrderStatus::Resting {
                    resting: Resting { oid: 42 },
                },
            )
            .unwrap();

        let result = manager
            .update_from_ws(&order_state(Some(cloid), 42), "open")
            .unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().status, Status::New);
    }

    #[test]
    fn test_update_from_ws_falls_back_to_oid() {
        let mut manager = OrderManager::new();
        let cloid = manager
            .prepare_cloid("BTC".to_string(), test_order(1))
            .unwrap();
        manager
            .update_from_exchange_submit(
                &cloid,
                &OrderStatus::Resting {
                    resting: Resting { oid: 42 },
                },
            )
            .unwrap();

        // The exchange may not echo the cloid (e.g. orders placed by other tools).
        let result = manager
            .update_from_ws(&order_state(None, 42), "filled")
            .unwrap();
        let order = result.expect("order should be updated by oid");
        assert_eq!(order.status, Status::Filled);
        // The update carries the state's filled quantity ("0" in this test).
        assert_eq!(order.exec_qty, 0.0);
        assert_eq!(order.leaves_qty, 1.0);
    }

    #[test]
    fn test_dual_channel_removal() {
        let mut manager = OrderManager::new();
        let cloid = manager
            .prepare_cloid("BTC".to_string(), test_order(1))
            .unwrap();
        let mut order = test_order(1);
        order.status = Status::New;

        // REST confirms the cancel.
        let rest_update = manager.update_from_exchange_cancel(&cloid, true).unwrap();
        assert!(rest_update.is_some());
        assert_eq!(manager.orders.len(), 1);

        // WS confirms the cancel: the order is removed and no further update is published.
        let ws_update = manager
            .update_from_ws(&order_state(Some(cloid), 42), "canceled")
            .unwrap();
        assert!(ws_update.is_none());
        assert!(manager.orders.is_empty());
    }

    #[test]
    fn test_submit_filled_status() {
        let mut manager = OrderManager::new();
        let cloid = manager
            .prepare_cloid("BTC".to_string(), test_order(1))
            .unwrap();
        let order = manager
            .update_from_exchange_submit(
                &cloid,
                &OrderStatus::Filled {
                    filled: Filled {
                        total_sz: "0.001".to_string(),
                        avg_px: "64200".to_string(),
                        oid: 42,
                    },
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(order.status, Status::Filled);
        assert_eq!(order.exec_qty, 0.001);
        assert_eq!(order.leaves_qty, 0.0);
        // A filled order is removed from the lookup table (terminal state).
        assert!(manager.get_oid("BTC", 1).is_none());
    }

    #[test]
    fn test_submit_error_status() {
        let mut manager = OrderManager::new();
        let cloid = manager
            .prepare_cloid("BTC".to_string(), test_order(1))
            .unwrap();
        let order = manager
            .update_from_exchange_submit(
                &cloid,
                &OrderStatus::Error {
                    error: "insufficient balance".to_string(),
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(order.status, Status::Rejected);
        assert_eq!(order.req, Status::None);
    }

    #[test]
    fn test_cancel_failure_keeps_status() {
        let mut manager = OrderManager::new();
        let cloid = manager
            .prepare_cloid("BTC".to_string(), test_order(1))
            .unwrap();
        let order = manager
            .update_from_exchange_cancel(&cloid, false)
            .unwrap()
            .unwrap();
        assert_eq!(order.status, Status::None);
        assert!(manager.orders.contains_key(&cloid));
    }

    #[test]
    fn test_dual_channel_removal_ws_first() {
        let mut manager = OrderManager::new();
        let cloid = manager
            .prepare_cloid("BTC".to_string(), test_order(1))
            .unwrap();

        // WS confirms the cancel first.
        let ws_update = manager
            .update_from_ws(&order_state(Some(cloid.clone()), 42), "canceled")
            .unwrap();
        assert!(ws_update.is_some());
        assert_eq!(ws_update.unwrap().status, Status::Canceled);
        assert_eq!(manager.orders.len(), 1);

        // REST then confirms: the order is removed and no further update is published.
        let rest_update = manager.update_from_exchange_cancel(&cloid, true).unwrap();
        assert!(rest_update.is_none());
        assert!(manager.orders.is_empty());
    }

    #[test]
    fn test_gc_removes_stale_orders() {
        let mut manager = OrderManager::new();

        // A: filled long ago (stale, exch_timestamp 0).
        let mut stale = test_order(1);
        stale.status = Status::New;
        let stale_cloid = manager.prepare_cloid("BTC".to_string(), stale).unwrap();
        let stale_state = OrderState {
            coin: "BTC".to_string(),
            side: "B".to_string(),
            limit_px: "63000".to_string(),
            sz: "1.0".to_string(),
            oid: 42,
            timestamp: 0,
            orig_sz: "1.0".to_string(),
            filled: "1.0".to_string(),
            avg_px: "63000".to_string(),
            cloid: Some(stale_cloid.clone()),
            reduce_only: false,
            tif: Some("Gtc".to_string()),
        };
        manager.update_from_ws(&stale_state, "filled").unwrap();

        // B: still active (New), must survive gc.
        let mut fresh = test_order(2);
        fresh.status = Status::New;
        manager.prepare_cloid("BTC".to_string(), fresh);

        manager.gc();

        assert!(!manager.orders.contains_key(&stale_cloid));
        assert_eq!(manager.orders.len(), 1);
        // The stale order is also gone from the lookup table.
        assert!(manager.get_cloid("BTC", 1).is_none());
    }

    #[test]
    fn test_orders_filters_active_and_symbol() {
        let mut manager = OrderManager::new();

        let mut btc_active = test_order(1);
        btc_active.status = Status::New;
        let mut btc_filled = test_order(2);
        btc_filled.status = Status::Filled;
        let mut eth_active = test_order(3);
        eth_active.status = Status::New;

        manager.prepare_cloid("BTC".to_string(), btc_active);
        manager.prepare_cloid("BTC".to_string(), btc_filled);
        manager.prepare_cloid("ETH".to_string(), eth_active);

        let all = manager.orders(None);
        assert_eq!(all.len(), 2);
        let btc = manager.orders(Some("BTC".to_string()));
        assert_eq!(btc.len(), 1);
        assert_eq!(btc[0].order_id, 1);
    }

    #[test]
    fn test_prepare_cloid_rejects_duplicate() {
        let mut manager = OrderManager::new();
        assert!(
            manager
                .prepare_cloid("BTC".to_string(), test_order(1))
                .is_some()
        );
        assert!(
            manager
                .prepare_cloid("BTC".to_string(), test_order(1))
                .is_none()
        );
        // A different bot order id for the same symbol is fine.
        assert!(
            manager
                .prepare_cloid("BTC".to_string(), test_order(2))
                .is_some()
        );
    }

    #[test]
    fn test_cloid_format() {
        let mut manager = OrderManager::new();
        let cloid = manager
            .prepare_cloid("BTC".to_string(), test_order(1))
            .unwrap();
        assert!(cloid.starts_with("0x"));
        assert_eq!(cloid.len(), 34);
        assert!(hex::decode(&cloid[2..]).is_ok());
    }

    #[test]
    fn test_update_cancel_fail_clears_req() {
        let mut manager = OrderManager::new();
        let mut order = test_order(1);
        order.req = Status::Canceled;
        let cloid = manager.prepare_cloid("BTC".to_string(), order).unwrap();
        let updated = manager.update_cancel_fail(&cloid).unwrap();
        assert_eq!(updated.req, Status::None);
    }

    #[test]
    fn test_from_str_to_status_mapping() {
        assert_eq!(from_str_to_status("open"), Status::New);
        assert_eq!(from_str_to_status("filled"), Status::Filled);
        assert_eq!(from_str_to_status("canceled"), Status::Canceled);
        assert_eq!(from_str_to_status("rejected"), Status::Rejected);
        assert_eq!(from_str_to_status("unknown"), Status::Unsupported);
    }
}
