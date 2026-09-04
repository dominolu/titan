use std::sync::{Arc, Mutex};

use chrono::Utc;
use hashbrown::HashMap;
use hftbacktest::types::{Order, Side, Status};

use crate::{
    connector::GetOrders,
    okx::{OkxError, msg::stream::OrderUpdate},
    utils::{RefSymbolOrderId, SymbolOrderId},
};

#[derive(Debug)]
struct OrderExt {
    symbol: String,
    order: Order,
    removed_by_ws: bool,
    removed_by_rest: bool,
}

pub type SharedOrderManager = Arc<Mutex<OrderManager>>;

pub type ClientOrderId = String;

fn from_str_to_status(state: &str) -> Status {
    match state {
        "live" => Status::New,
        "partially_filled" => Status::PartiallyFilled,
        "filled" => Status::Filled,
        "canceled" | "mmp_canceled" => Status::Canceled,
        "rejected" => Status::Rejected,
        "order_failed" => Status::Expired,
        _ => Status::Unsupported,
    }
}

/// Tracks orders created by the AccountPlugin REST facade so the private `orders` channel can be
/// correlated back to the deterministic owner client id. Terminal private-stream facts are kept for
/// GC (rather than removed immediately) so no duplicate publication can follow a stale frame.
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

    /// Updates the order state with the private `orders` channel and returns the order to publish.
    pub fn update_from_ws(&mut self, data: &OrderUpdate) -> Result<Option<Order>, OkxError> {
        if !self.orders.contains_key(&data.cl_ord_id) && !data.cl_ord_id.starts_with(&self.prefix) {
            return Err(OkxError::PrefixUnmatched);
        }
        let order_ext = self
            .orders
            .get_mut(&data.cl_ord_id)
            .ok_or(OkxError::OrderNotFound)?;

        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;
        let exch_ts = data.u_time.parse::<i64>().unwrap_or(0) * 1_000_000;
        if exch_ts >= order_ext.order.exch_timestamp {
            order_ext.order.status = from_str_to_status(&data.state);
            order_ext.order.exec_qty = data.acc_fill_sz.parse().unwrap_or(0.0);
            order_ext.order.leaves_qty =
                data.sz.parse::<f64>().unwrap_or(0.0) - order_ext.order.exec_qty;
            order_ext.order.exec_price_tick = (data.avg_px.parse::<f64>().unwrap_or(0.0)
                / order_ext.order.tick_size)
                .round() as i64;
            order_ext.order.exch_timestamp = exch_ts;
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
                self.orders.remove(&data.cl_ord_id).unwrap();
            }
        }

        Ok(result)
    }

    pub fn cancel_all(&mut self, symbol: &str, pos_side: Option<&str>) -> Vec<Order> {
        let mut removed_order_ids = Vec::new();
        let mut removed_orders = Vec::new();
        for (client_order_id, order_ext) in &mut self.orders {
            if order_ext.symbol != symbol {
                continue;
            }
            let side_matches = match pos_side {
                Some("long") => order_ext.order.side == Side::Buy,
                Some("short") => order_ext.order.side == Side::Sell,
                _ => true,
            };
            if !side_matches {
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
                removed_order_ids.push(client_order_id.clone());
            }
        }
        for order_id in removed_order_ids {
            self.orders.remove(&order_id).unwrap();
        }
        removed_orders
    }

    /// Resolves orders deleted by one channel but not confirmed by the other after a threshold.
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
            self.order_id_map.remove(order_id);
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
    use hftbacktest::types::{OrdType, TimeInForce};

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

    fn order_update(cl_ord_id: &str, state: &str) -> OrderUpdate {
        OrderUpdate {
            inst_id: "BTC-USDT-SWAP".to_string(),
            ord_id: "1".to_string(),
            cl_ord_id: cl_ord_id.to_string(),
            side: "buy".to_string(),
            ord_type: "limit".to_string(),
            px: "30000".to_string(),
            sz: "1".to_string(),
            state: state.to_string(),
            acc_fill_sz: "0".to_string(),
            fill_px: "".to_string(),
            avg_px: "0".to_string(),
            u_time: "1000".to_string(),
            c_time: "1000".to_string(),
            pos_side: "net".to_string(),
            td_mode: "cross".to_string(),
        }
    }

    #[test]
    fn tracked_rest_order_flows_through_private_ws_without_legacy_prefix() {
        let mut manager = OrderManager::new("t-");
        let client_order_id = "0123456789abcdef0123456789abcdef";
        let mut order = test_order(3);
        order.status = Status::New;
        assert!(manager.track_managed_order("BTC-USDT-SWAP", client_order_id, order));

        let updated = manager
            .update_from_ws(&order_update(client_order_id, "filled"))
            .unwrap()
            .unwrap();
        assert_eq!(updated.status, Status::Filled);
    }

    #[test]
    fn test_cancel_all_respects_pos_side() {
        let mut manager = OrderManager::new("");
        let mut buy = test_order(1);
        buy.side = Side::Buy;
        let mut sell = test_order(2);
        sell.side = Side::Sell;
        manager.track_managed_order("BTC-USDT-SWAP", "buyer", buy);
        manager.track_managed_order("BTC-USDT-SWAP", "seller", sell);

        let canceled = manager.cancel_all("BTC-USDT-SWAP", Some("short"));
        assert_eq!(canceled.len(), 1);
        assert_eq!(canceled[0].side, Side::Sell);
    }
}
