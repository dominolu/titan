//! EVM venue 订单状态机。
//!
//! 链上没有"挂单"概念：一次 swap 就是一笔交易，状态机是
//! `New(pending) -> Filled | Rejected(reverted) | Canceled(dropped/超时)`。
//! key 是 AccountPlugin 的确定性 client_order_id；`order_id` 是 hftbacktest 本地
//! 自增 id（tx hash 装不进 u64），tx hash 单独存放在 [`TrackedOrder`] 并作为
//! `venue_order_id` 上报。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use hashbrown::HashMap as FastHashMap;
use hftbacktest::types::{Order, Status};

use crate::connector::GetOrders;
use crate::evm::types::PoolConfig;

#[derive(Debug, Clone)]
pub struct TrackedOrder {
    pub symbol: String,
    /// hftbacktest 本地订单（order_id 为本地自增 id）。
    pub order: Order,
    /// 广播后的交易哈希（小写 0x 前缀）。None = 尚未广播成功。
    pub tx_hash: Option<String>,
    /// 提交时间（ms），用于 dropped 判定。
    pub submitted_at_ms: i64,
}

#[derive(Debug, Default)]
pub struct OrderManager {
    /// client_order_id -> 追踪条目。
    orders: HashMap<String, TrackedOrder>,
    /// tx hash -> client_order_id（收据回填用）。
    tx_index: FastHashMap<String, String>,
    /// 本地自增 order_id -> client_order_id（GetOrders 侧 id 对齐）。
    local_index: FastHashMap<u64, String>,
}

pub type SharedOrderManager = Arc<Mutex<OrderManager>>;

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

impl OrderManager {
    pub fn new() -> Self {
        Default::default()
    }

    /// AccountPlugin 通过 `Connector::track_managed_order` 在发单前注册。
    pub fn track_managed_order(
        &mut self,
        symbol: &str,
        client_order_id: &str,
        order: Order,
    ) -> bool {
        if self.orders.contains_key(client_order_id) {
            return false;
        }
        self.local_index
            .insert(order.order_id, client_order_id.to_string());
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

    /// brokerapi 侧快照：全部追踪条目（含 client_order_id）。
    pub fn orders_snapshot(&self) -> Vec<(String, TrackedOrder)> {
        self.orders
            .iter()
            .map(|(id, entry)| (id.clone(), entry.clone()))
            .collect()
    }

    /// 广播成功后回填 tx hash。
    pub fn attach_tx(&mut self, client_order_id: &str, tx_hash: &str) {
        if let Some(entry) = self.orders.get_mut(client_order_id) {
            entry.tx_hash = Some(tx_hash.to_string());
            self.tx_index
                .insert(tx_hash.to_string(), client_order_id.to_string());
        }
    }

    /// 终态回填：status 必须是终态（Filled/Rejected/Canceled）。
    /// 返回是否发生了状态转移（重复通知返回 false）。
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

    /// 按符号清理（cancel_all 语义的本地镜像：pending 视为失败落地）。
    pub fn fail_pending(&mut self, symbol: &str) -> Vec<Order> {
        let mut finalized = Vec::new();
        let ids: Vec<String> = self
            .orders
            .iter()
            .filter(|(_, entry)| entry.symbol == symbol)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            if let Some(order) = self.finalize(&id, Status::Rejected, 0.0, 0.0) {
                finalized.push(order);
            }
        }
        finalized
    }

    /// 回收终态超过 5 分钟的条目。
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
            self.local_index.remove(&entry.order.order_id);
            if let Some(tx) = &entry.tx_hash {
                self.tx_index.remove(tx);
            }
        }
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

/// 简化的 brokerapi 侧视图：client_order_id -> 池配置 + 请求参数。
#[derive(Debug, Clone)]
pub struct PendingSwap {
    pub pool: PoolConfig,
    pub side: crate::api::ApiSide,
    pub base_qty: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::Address;
    use hftbacktest::types::{OrdType, Side, TimeInForce};

    use crate::api::ApiSide;
    use crate::evm::types::PoolConfig;

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

    fn pool() -> PoolConfig {
        PoolConfig {
            symbol: "WETH/USDC".to_string(),
            pair_address: Address::repeat_byte(0x03),
            base_token: Address::repeat_byte(0x01),
            quote_token: Address::repeat_byte(0x02),
            base_decimals: 18,
            quote_decimals: 6,
        }
    }

    #[test]
    fn track_attach_finalize_lifecycle() {
        let mut mgr = OrderManager::new();
        assert!(mgr.track_managed_order("WETH/USDC", "abc", order(1)));
        assert!(
            !mgr.track_managed_order("WETH/USDC", "abc", order(2)),
            "duplicate rejected"
        );

        mgr.attach_tx("abc", "0xdead");
        assert_eq!(mgr.client_order_id_by_tx("0xdead").unwrap(), "abc");
        assert_eq!(
            mgr.by_client_order_id("abc").unwrap().tx_hash.as_deref(),
            Some("0xdead")
        );

        let finalized = mgr.finalize("abc", Status::Filled, 1.0, 3001.5).unwrap();
        assert_eq!(finalized.status, Status::Filled);
        assert_eq!(finalized.exec_qty, 1.0);
        assert_eq!(finalized.exec_price_tick, 30015);
        // 重复终态通知不产生第二次转移。
        assert!(mgr.finalize("abc", Status::Filled, 1.0, 3001.5).is_none());
        // 终态后 GetOrders 不再返回。
        assert!(mgr.orders(None).is_empty());
    }

    #[test]
    fn tx_index_cleared_on_remove() {
        let mut mgr = OrderManager::new();
        mgr.track_managed_order("WETH/USDC", "abc", order(1));
        mgr.attach_tx("abc", "0xdead");
        mgr.remove("abc");
        assert!(mgr.client_order_id_by_tx("0xdead").is_none());
    }

    #[test]
    fn fail_pending_finalizes_all_new_orders_for_symbol() {
        let mut mgr = OrderManager::new();
        mgr.track_managed_order("WETH/USDC", "a", order(1));
        mgr.track_managed_order("OTHER", "b", order(2));
        let failed = mgr.fail_pending("WETH/USDC");
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].status, Status::Rejected);
        assert_eq!(mgr.orders(Some("OTHER".to_string())).len(), 1);
    }

    #[test]
    fn pending_swap_request_shape_is_stable() {
        // PendingSwap 只做数据搬运，锁定字段避免未来误改。
        let p = PendingSwap {
            pool: pool(),
            side: ApiSide::Buy,
            base_qty: 1.5,
        };
        assert_eq!(p.pool.symbol, "WETH/USDC");
        assert_eq!(p.base_qty, 1.5);
    }
}
