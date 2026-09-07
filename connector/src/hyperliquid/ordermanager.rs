use std::sync::{Arc, Mutex};

use chrono::Utc;
use hashbrown::HashMap;
use hftbacktest::types::{Order, Status};

use crate::{
    connector::GetOrders,
    hyperliquid::{HyperliquidError, msg::OrderState},
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

/// Tracks AccountPlugin REST orders so the `orderUpdates` channel can be correlated back to the
/// deterministic owner cloid.
#[derive(Default, Debug)]
pub struct OrderManager {
    orders: HashMap<Cloid, OrderExt>,
    order_id_map: HashMap<SymbolOrderId, Cloid>,
}

impl OrderManager {
    pub fn new() -> Self {
        Default::default()
    }

    fn normalize_cloid(client_order_id: &str) -> String {
        if client_order_id.starts_with("0x") {
            client_order_id.to_owned()
        } else {
            format!("0x{client_order_id}")
        }
    }

    pub fn track_managed_order(
        &mut self,
        symbol: &str,
        client_order_id: &str,
        order: Order,
    ) -> bool {
        let cloid = Self::normalize_cloid(client_order_id);
        if self.orders.contains_key(&cloid) {
            return false;
        }
        self.order_id_map.insert(
            SymbolOrderId::new(symbol.to_owned(), order.order_id),
            cloid.clone(),
        );
        self.orders.insert(
            cloid,
            OrderExt {
                symbol: symbol.to_owned(),
                order,
                oid: None,
                removed_by_ws: false,
                removed_by_rest: false,
            },
        );
        true
    }

    pub fn update_from_ws(
        &mut self,
        state: &OrderState,
        status: &str,
        status_timestamp: u64,
    ) -> Result<Option<Order>, HyperliquidError> {
        let cloid = match state.cloid.as_deref().map(Self::normalize_cloid) {
            Some(cloid) if self.orders.contains_key(&cloid) => cloid,
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
        let next_status = from_str_to_status(status);
        let current_oid = order_ext.oid;

        // Hyperliquid implements modify as cancel-old + open-new with the same cloid. The two
        // updates are not guaranteed to arrive in lifecycle order. Once the replacement oid is
        // open, a delayed terminal update for the old oid must not close the replacement order.
        let terminal = next_status != Status::New
            && next_status != Status::PartiallyFilled
            && next_status != Status::Unsupported;
        if terminal && state.oid != 0 && current_oid.is_some() && current_oid != Some(state.oid) {
            return Ok(None);
        }
        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;
        let update_ts = if status_timestamp > 0 {
            status_timestamp
        } else {
            state.timestamp
        } * 1_000_000;

        if state.oid != 0 {
            order_ext.oid = Some(state.oid);
        }
        if (update_ts as i64) >= order_ext.order.exch_timestamp {
            let original_qty = state.orig_sz.parse().unwrap_or(order_ext.order.qty);
            let leaves_qty = state.sz.parse().unwrap_or(original_qty);
            let derived_filled = (original_qty - leaves_qty).max(0.0);
            order_ext.order.price_tick = (state.limit_px.parse::<f64>().unwrap_or(0.0)
                / order_ext.order.tick_size)
                .round() as i64;
            order_ext.order.qty = original_qty;
            order_ext.order.exec_qty = state.filled.parse().unwrap_or(derived_filled);
            order_ext.order.status = if next_status == Status::New && order_ext.order.exec_qty > 0.0
            {
                Status::PartiallyFilled
            } else {
                next_status
            };
            order_ext.order.leaves_qty = leaves_qty;
            order_ext.order.exec_price_tick = (state.avg_px.parse::<f64>().unwrap_or(0.0)
                / order_ext.order.tick_size)
                .round() as i64;
            order_ext.order.exch_timestamp = update_ts as i64;
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
    fn tracked_order_flows_through_private_ws_without_legacy_prefix() {
        let mut manager = OrderManager::new();
        let client_order_id = "0123456789abcdef0123456789abcdef";
        let mut order = test_order(1);
        order.status = Status::New;
        assert!(manager.track_managed_order("BTC", client_order_id, order));
        let result = manager
            .update_from_ws(
                &order_state(Some(format!("0x{client_order_id}")), 42),
                "open",
                2_000,
            )
            .unwrap();
        assert_eq!(result.unwrap().status, Status::New);
    }

    #[test]
    fn tracked_order_matches_unprefixed_cloid() {
        let mut manager = OrderManager::new();
        let client_order_id = "fedcba9876543210fedcba9876543210";
        let mut order = test_order(99);
        order.status = Status::New;
        assert!(manager.track_managed_order("BTC", client_order_id, order));
        let result = manager
            .update_from_ws(
                &order_state(Some(client_order_id.to_string()), 123),
                "filled",
                3_000,
            )
            .unwrap();
        let order = result.unwrap();
        assert_eq!(order.order_id, 99);
        assert_eq!(order.status, Status::Filled);
    }

    #[test]
    fn test_gc_removes_stale_orders() {
        let mut manager = OrderManager::new();
        let stale_cloid = "0x0123456789abcdef0123456789abcdef";
        let mut stale = test_order(1);
        stale.status = Status::New;
        assert!(manager.track_managed_order("BTC", stale_cloid, stale));
        let mut stale_state = order_state(Some(stale_cloid.to_string()), 42);
        stale_state.timestamp = 0;
        stale_state.filled = "1.0".to_string();
        manager
            .update_from_ws(&stale_state, "filled", 2_000)
            .unwrap()
            .unwrap();

        let mut fresh = test_order(2);
        fresh.status = Status::New;
        assert!(manager.track_managed_order("BTC", "fresh", fresh));
        manager.gc();
        assert!(!manager.orders.contains_key(stale_cloid));
        assert_eq!(manager.orders.len(), 1);
    }

    #[test]
    fn test_update_from_ws_uses_status_timestamp_fallback() {
        let mut manager = OrderManager::new();
        let mut order = test_order(1);
        order.status = Status::New;
        order.exch_timestamp = 1_000_000_000;
        assert!(manager.track_managed_order("BTC", "fallback", order));

        let mut state = order_state(Some("fallback".into()), 10);
        state.timestamp = 1_500;
        state.filled = "1.0".to_string();

        let updated = manager
            .update_from_ws(&state, "filled", 0)
            .unwrap()
            .unwrap();
        assert_eq!(updated.status, Status::Filled);
        assert_eq!(updated.exch_timestamp, 1_500_000_000);
    }

    #[test]
    fn test_update_from_ws_ignores_out_of_order_timestamp() {
        let mut manager = OrderManager::new();
        let mut order = test_order(1);
        order.status = Status::Filled;
        order.exch_timestamp = 5_000_000_000;
        assert!(manager.track_managed_order("BTC", "stale", order));

        let mut state = order_state(Some("stale".into()), 11);
        state.timestamp = 3_000;
        state.filled = "0.3".to_string();

        let updated = manager
            .update_from_ws(&state, "open", 3_000)
            .unwrap()
            .unwrap();
        assert_eq!(updated.status, Status::Filled);
        assert_eq!(updated.exch_timestamp, 5_000_000_000);
    }

    #[test]
    fn amend_does_not_let_delayed_old_cancel_close_replacement() {
        let mut manager = OrderManager::new();
        let cloid = "0xcccccccccccccccccccccccccccccccc";
        let mut order = test_order(1);
        order.status = Status::New;
        assert!(manager.track_managed_order("BTC", cloid, order));

        manager
            .update_from_ws(&order_state(Some(cloid.into()), 100), "open", 1_000)
            .unwrap()
            .unwrap();
        manager
            .update_from_ws(&order_state(Some(cloid.into()), 200), "open", 2_000)
            .unwrap()
            .unwrap();

        let stale_cancel = manager
            .update_from_ws(&order_state(Some(cloid.into()), 100), "canceled", 3_000)
            .unwrap();
        assert!(stale_cancel.is_none());
        assert_eq!(manager.orders(None).len(), 1);

        let replacement_cancel = manager
            .update_from_ws(&order_state(Some(cloid.into()), 200), "canceled", 4_000)
            .unwrap()
            .unwrap();
        assert_eq!(replacement_cancel.status, Status::Canceled);
        assert!(manager.orders(None).is_empty());
    }

    #[test]
    fn open_order_with_executed_quantity_is_partially_filled() {
        let mut manager = OrderManager::new();
        let cloid = "0xdddddddddddddddddddddddddddddddd";
        let mut order = test_order(1);
        order.status = Status::New;
        assert!(manager.track_managed_order("BTC", cloid, order));

        let mut state = order_state(Some(cloid.into()), 100);
        state.filled = "0.25".to_string();
        state.sz = "0.75".to_string();
        let updated = manager
            .update_from_ws(&state, "open", 1_000)
            .unwrap()
            .unwrap();
        assert_eq!(updated.status, Status::PartiallyFilled);
        assert_eq!(updated.exec_qty, 0.25);
        assert_eq!(updated.leaves_qty, 0.75);
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
        manager.track_managed_order("BTC", "btc-active", btc_active);
        manager.track_managed_order("BTC", "btc-filled", btc_filled);
        manager.track_managed_order("ETH", "eth-active", eth_active);
        let all = manager.orders(None);
        assert_eq!(all.len(), 2);
        assert_eq!(manager.orders(Some("BTC".to_string())).len(), 1);
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
