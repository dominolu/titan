use std::sync::{Arc, Mutex};

use chrono::Utc;
use hashbrown::HashMap;
use hftbacktest::types::{Order, OrderId, Side, Status};
use tracing::error;

use crate::{
    connector::GetOrders,
    okx::{
        OkxError,
        msg::{rest::OrderResult, stream::OrderUpdate},
    },
    utils::{RefSymbolOrderId, SymbolOrderId, generate_rand_string},
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
        "canceled" => Status::Canceled,
        "mmp_canceled" => Status::Canceled,
        "rejected" => Status::Rejected,
        "order_failed" => Status::Expired,
        _ => Status::Unsupported,
    }
}

/// OKX has separated channels for REST APIs and WebSocket. Order responses are delivered
/// through these channels, with no guaranteed order of transmission. To prevent duplicate handling
/// of order responses, such as order deletion due to cancellation or fill, OrderManager manages the
/// order states before transmitting the responses to a live bot.
///
/// Deletions must be confirmed by both channels. If not, differences in response times could result
/// in attempts to update an order that has already been deleted, potentially creating a ghost order
/// unintentionally.
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

    pub fn prepare_client_order_id(&mut self, symbol: String, order: Order) -> Option<String> {
        let symbol_order_id = SymbolOrderId::new(symbol.clone(), order.order_id);
        if self.order_id_map.contains_key(&symbol_order_id) {
            return None;
        }

        let client_order_id = format!("{}{}", self.prefix, generate_rand_string(16));
        if self.orders.contains_key(&client_order_id) {
            return None;
        }

        self.order_id_map
            .insert(symbol_order_id, client_order_id.clone());
        self.orders.insert(
            client_order_id.clone(),
            OrderExt {
                symbol,
                order,
                removed_by_ws: false,
                removed_by_rest: false,
            },
        );
        Some(client_order_id)
    }

    pub fn get_client_order_id(&self, symbol: &str, order_id: OrderId) -> Option<String> {
        self.order_id_map
            .get(&RefSymbolOrderId::new(symbol, order_id))
            .cloned()
    }

    /// Updates the order state with the data received from the private WebSocket stream
    /// (`orders` channel). Returns the order to be published when the update is meaningful.
    pub fn update_from_ws(&mut self, data: &OrderUpdate) -> Result<Option<Order>, OkxError> {
        if !data.cl_ord_id.starts_with(&self.prefix) {
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

    /// Updates the order state with the REST order placement response.
    pub fn update_from_rest_submit(
        &mut self,
        client_order_id: &ClientOrderId,
        resp: &OrderResult,
    ) -> Result<Option<Order>, OkxError> {
        let order_ext = self
            .orders
            .get_mut(client_order_id)
            .ok_or(OkxError::OrderNotFound)?;
        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;

        order_ext.order.req = Status::None;
        if resp.s_code != "0" {
            // The exchange rejected the order placement (e.g. insufficient margin).
            order_ext.order.status = Status::Expired;
            order_ext.removed_by_rest = true;
            if !already_removed {
                self.order_id_map.remove(&RefSymbolOrderId::new(
                    &order_ext.symbol,
                    order_ext.order.order_id,
                ));
            }
        } else if !already_removed {
            // The order is now resting on the book. Set New explicitly so the bot sees the order
            // as active even if the WebSocket stream has not delivered the first update yet.
            order_ext.order.status = Status::New;
        }
        let result = if already_removed {
            None
        } else {
            Some(order_ext.order.clone())
        };
        if order_ext.removed_by_ws && order_ext.removed_by_rest {
            self.orders.remove(client_order_id).unwrap();
        }

        Ok(result)
    }

    /// Updates the order state with the REST cancel response.
    pub fn update_from_rest_cancel(
        &mut self,
        client_order_id: &ClientOrderId,
    ) -> Result<Option<Order>, OkxError> {
        let order_ext = self
            .orders
            .get_mut(client_order_id)
            .ok_or(OkxError::OrderNotFound)?;
        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;

        order_ext.order.req = Status::None;
        order_ext.order.status = Status::Canceled;
        order_ext.removed_by_rest = true;
        if !already_removed {
            self.order_id_map.remove(&RefSymbolOrderId::new(
                &order_ext.symbol,
                order_ext.order.order_id,
            ));
        }
        let result = if already_removed {
            None
        } else {
            Some(order_ext.order.clone())
        };
        if order_ext.removed_by_ws && order_ext.removed_by_rest {
            self.orders.remove(client_order_id).unwrap();
        }

        Ok(result)
    }

    /// Updates the order state when the REST cancel request fails.
    pub fn update_cancel_fail(
        &mut self,
        client_order_id: &ClientOrderId,
        error: &OkxError,
    ) -> Option<Order> {
        let order_ext = self.orders.get_mut(client_order_id)?;
        match error {
            OkxError::OrderError { code, .. } if code == "51401" => {
                // The order no longer exists; it may have already been filled or canceled.
                // The order status cannot be determined from this error.
                order_ext.order.req = Status::None;
            }
            error => {
                error!(?error, "cancel error");
            }
        }
        Some(order_ext.order.clone())
    }

    pub fn update_submit_fail(
        &mut self,
        client_order_id: &ClientOrderId,
        error: &OkxError,
    ) -> Option<Order> {
        let order_ext = self.orders.get_mut(client_order_id)?;
        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;
        match error {
            OkxError::OrderError { code, .. } => {
                error!(code, "submit error");
            }
            error => {
                error!(?error, "submit error");
            }
        }
        order_ext.order.req = Status::None;
        order_ext.order.status = Status::Expired;
        order_ext.removed_by_rest = true;
        if !already_removed {
            self.order_id_map.remove(&RefSymbolOrderId::new(
                &order_ext.symbol,
                order_ext.order.order_id,
            ));
        }
        let result = if already_removed {
            None
        } else {
            Some(order_ext.order.clone())
        };
        if order_ext.removed_by_ws && order_ext.removed_by_rest {
            self.orders.remove(client_order_id).unwrap();
        }
        result
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
    use crate::okx::msg::stream::OrderUpdate;
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

    fn order_result(s_code: &str) -> OrderResult {
        OrderResult {
            cl_ord_id: String::new(),
            ord_id: String::new(),
            s_code: s_code.to_string(),
            s_msg: String::new(),
        }
    }

    #[test]
    fn test_submit_fail_publishes_expired_status() {
        let mut manager = OrderManager::new("");
        let cl_ord_id = manager
            .prepare_client_order_id("BTC-USDT-SWAP".to_string(), test_order(1))
            .unwrap();
        let error = OkxError::OrderError {
            code: "51000".to_string(),
            msg: "insufficient margin".to_string(),
        };
        let order = manager.update_submit_fail(&cl_ord_id, &error).unwrap();
        assert_eq!(order.status, Status::Expired);
        assert_eq!(order.req, Status::None);
    }

    #[test]
    fn test_exchange_rejection_publishes_expired_status() {
        let mut manager = OrderManager::new("");
        let cl_ord_id = manager
            .prepare_client_order_id("BTC-USDT-SWAP".to_string(), test_order(1))
            .unwrap();
        let order = manager
            .update_from_rest_submit(&cl_ord_id, &order_result("51000"))
            .unwrap()
            .unwrap();
        assert_eq!(order.status, Status::Expired);
        assert_eq!(order.req, Status::None);
    }

    #[test]
    fn test_dual_channel_removal() {
        let mut manager = OrderManager::new("");
        let cl_ord_id = manager
            .prepare_client_order_id("BTC-USDT-SWAP".to_string(), test_order(1))
            .unwrap();

        let rest_update = manager.update_from_rest_cancel(&cl_ord_id).unwrap();
        assert!(rest_update.is_some());
        assert_eq!(rest_update.unwrap().status, Status::Canceled);
        assert_eq!(manager.orders.len(), 1);

        let ws_update = manager
            .update_from_ws(&order_update(&cl_ord_id, "canceled"))
            .unwrap();
        assert!(ws_update.is_none());
        assert!(manager.orders.is_empty());
    }

    #[test]
    fn test_cancel_all_respects_pos_side() {
        let mut manager = OrderManager::new("");
        let mut buy = test_order(1);
        buy.side = Side::Buy;
        let mut sell = test_order(2);
        sell.side = Side::Sell;
        manager.prepare_client_order_id("BTC-USDT-SWAP".to_string(), buy);
        manager.prepare_client_order_id("BTC-USDT-SWAP".to_string(), sell);

        let canceled = manager.cancel_all("BTC-USDT-SWAP", Some("short"));
        assert_eq!(canceled.len(), 1);
        assert_eq!(canceled[0].side, Side::Sell);
        // The sell order is removed from the lookup table; both orders remain in memory until the
        // WebSocket channel confirms the cancellation (dual-channel design).
        assert!(
            manager
                .order_id_map
                .get(&SymbolOrderId::new("BTC-USDT-SWAP".to_string(), 2))
                .is_none()
        );
        assert!(
            manager
                .order_id_map
                .contains_key(&SymbolOrderId::new("BTC-USDT-SWAP".to_string(), 1))
        );
        assert_eq!(manager.orders.len(), 2);
    }
}
