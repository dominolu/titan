//! Solana venue 订单状态机（与 EVM venue 同构）。
//!
//! 链上没有"挂单"：一次 swap 是一笔交易，状态机为
//! `New(pending) -> Filled | Rejected(reverted) | Canceled(dropped/超时)`。
//! key 是 AccountPlugin 的确定性 client_order_id；签名（signature）单独存放
//! 并作为 `venue_order_id` 上报（装不进 u64 的本地 order_id）。

use std::sync::{Arc, Mutex};

use chrono::Utc;
use hashbrown::HashMap as FastHashMap;
use hftbacktest::types::{Order, Status};

use crate::connector::GetOrders;

#[derive(Debug, Clone)]
pub struct TrackedOrder {
    pub symbol: String,
    pub order: Order,
    /// 广播后的交易签名（base58）。None = 尚未广播成功。
    pub tx_hash: Option<String>,
    pub submitted_at_ms: i64,
}

#[derive(Debug, Default)]
pub struct OrderManager {
    orders: std::collections::HashMap<String, TrackedOrder>,
    tx_index: FastHashMap<String, String>,
}

pub type SharedOrderManager = Arc<Mutex<OrderManager>>;

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

impl OrderManager {
    pub fn new() -> Self {
        Default::default()
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
        self.orders.insert(
            client_order_id.to_string(),
            TrackedOrder {
                symbol: symbol.to_owned(),
                order,
                tx_hash: None,
                submitted_at_ms: now_ms(),
            },
        );
        true
    }

    pub fn by_client_order_id(&self, client_order_id: &str) -> Option<&TrackedOrder> {
        self.orders.get(client_order_id)
    }

    pub fn client_order_id_by_tx(&self, tx_hash: &str) -> Option<&String> {
        self.tx_index.get(tx_hash)
    }

    pub fn attach_tx(&mut self, client_order_id: &str, signature: &str) {
        if let Some(entry) = self.orders.get_mut(client_order_id) {
            entry.tx_hash = Some(signature.to_string());
            self.tx_index
                .insert(signature.to_string(), client_order_id.to_string());
        }
    }

    /// 终态回填。返回是否发生转移（重复通知返回 None）。
    pub fn finalize(
        &mut self,
        client_order_id: &str,
        status: Status,
        exec_qty: f64,
        exec_price: f64,
    ) -> Option<Order> {
        let entry = self.orders.get_mut(client_order_id)?;
        if !matches!(entry.order.status, Status::New | Status::PartiallyFilled) {
            return None;
        }
        entry.order.status = status;
        entry.order.req = Status::None;
        entry.order.exec_qty = exec_qty;
        entry.order.leaves_qty = (entry.order.qty - exec_qty).max(0.0);
        if exec_price > 0.0 {
            entry.order.exec_price_tick = (exec_price / entry.order.tick_size).round() as i64;
        }
        entry.order.exch_timestamp = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(entry.order.exch_timestamp);
        Some(entry.order.clone())
    }

    pub fn fail_pending(&mut self, symbol: &str) -> Vec<Order> {
        let ids: Vec<String> = self
            .orders
            .iter()
            .filter(|(_, entry)| entry.symbol == symbol)
            .map(|(id, _)| id.clone())
            .collect();
        ids.iter()
            .filter_map(|id| self.finalize(id, Status::Rejected, 0.0, 0.0))
            .collect()
    }

    pub fn gc(&mut self) {
        let stale_before = now_ms() - 300_000;
        let stale: Vec<String> = self
            .orders
            .iter()
            .filter(|(_, entry)| {
                !matches!(entry.order.status, Status::New | Status::PartiallyFilled)
                    && entry.submitted_at_ms < stale_before
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in stale {
            self.remove(&id);
        }
    }

    pub fn remove(&mut self, client_order_id: &str) {
        if let Some(entry) = self.orders.remove(client_order_id) {
            if let Some(tx) = &entry.tx_hash {
                self.tx_index.remove(tx);
            }
        }
    }

    pub fn orders_snapshot(&self) -> Vec<(String, TrackedOrder)> {
        self.orders
            .iter()
            .map(|(id, entry)| (id.clone(), entry.clone()))
            .collect()
    }
}

impl GetOrders for OrderManager {
    fn orders(&self, symbol: Option<String>) -> Vec<Order> {
        self.orders
            .iter()
            .filter(|(_, entry)| {
                symbol.as_ref().map(|s| entry.symbol == *s).unwrap_or(true) && entry.order.active()
            })
            .map(|(_, entry)| entry.order.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hftbacktest::types::{OrdType, Side, TimeInForce};

    fn order(id: u64) -> Order {
        let mut order = Order::new(
            id,
            30_000,
            0.1,
            1.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        order.status = Status::New;
        order
    }

    #[test]
    fn track_attach_finalize_lifecycle() {
        let mut mgr = OrderManager::new();
        assert!(mgr.track_managed_order("TST/WSOL", "abc", order(1)));
        mgr.attach_tx("abc", "SIG");
        assert_eq!(mgr.client_order_id_by_tx("SIG").unwrap(), "abc");
        let finalized = mgr.finalize("abc", Status::Filled, 1.0, 30.5).unwrap();
        assert_eq!(finalized.status, Status::Filled);
        assert!(mgr.finalize("abc", Status::Filled, 1.0, 30.5).is_none());
        assert!(mgr.orders(None).is_empty());
    }
}
