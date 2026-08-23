use std::collections::BTreeMap;

use crate::types::{OrderId, Side};

use super::TriggerKind;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ConditionalOrder {
    pub order_id: OrderId,
    pub side: Side,
    pub trigger_kind: TriggerKind,
    pub trigger_price: f64,
    pub gtd_expiry_ts: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConditionalAction {
    Trigger {
        order_id: OrderId,
        trigger_kind: TriggerKind,
    },
    Expire {
        order_id: OrderId,
    },
}

#[derive(Default)]
pub struct ConditionalOrderBook {
    orders: BTreeMap<OrderId, ConditionalOrder>,
}

impl ConditionalOrderBook {
    pub fn insert(&mut self, order: ConditionalOrder) -> bool {
        order.trigger_price.is_finite()
            && order.trigger_price > 0.0
            && self.orders.insert(order.order_id, order).is_none()
    }

    pub fn cancel(&mut self, order_id: OrderId) -> bool {
        self.orders.remove(&order_id).is_some()
    }

    pub fn evaluate(
        &mut self,
        exchange_ts: i64,
        market_price: f64,
        out: &mut Vec<ConditionalAction>,
    ) {
        let start = out.len();
        for (id, order) in &self.orders {
            if order
                .gtd_expiry_ts
                .is_some_and(|expiry| exchange_ts >= expiry)
            {
                out.push(ConditionalAction::Expire { order_id: *id });
            } else {
                let triggered = match order.side {
                    Side::Buy => market_price >= order.trigger_price,
                    Side::Sell => market_price <= order.trigger_price,
                    _ => false,
                };
                if triggered {
                    out.push(ConditionalAction::Trigger {
                        order_id: *id,
                        trigger_kind: order.trigger_kind,
                    });
                }
            }
        }
        for index in start..out.len() {
            let order_id = match out[index] {
                ConditionalAction::Trigger { order_id, .. }
                | ConditionalAction::Expire { order_id } => order_id,
            };
            self.orders.remove(&order_id);
        }
    }

    /// Evaluates a completed market range without inventing an intrabar path. Buy stops observe
    /// the high, sell stops observe the low; callers decide when the triggered child becomes
    /// eligible for matching (the Bar runtime uses the next open).
    pub fn evaluate_range(
        &mut self,
        exchange_ts: i64,
        low: f64,
        high: f64,
        out: &mut Vec<ConditionalAction>,
    ) {
        let start = out.len();
        for (id, order) in &self.orders {
            if order
                .gtd_expiry_ts
                .is_some_and(|expiry| exchange_ts >= expiry)
            {
                out.push(ConditionalAction::Expire { order_id: *id });
            } else {
                let triggered = match order.side {
                    Side::Buy => high >= order.trigger_price,
                    Side::Sell => low <= order.trigger_price,
                    _ => false,
                };
                if triggered {
                    out.push(ConditionalAction::Trigger {
                        order_id: *id,
                        trigger_kind: order.trigger_kind,
                    });
                }
            }
        }
        for index in start..out.len() {
            let order_id = match out[index] {
                ConditionalAction::Trigger { order_id, .. }
                | ConditionalAction::Expire { order_id } => order_id,
            };
            self.orders.remove(&order_id);
        }
    }

    pub fn next_expiry_ts(&self) -> Option<i64> {
        self.orders
            .values()
            .filter_map(|order| order.gtd_expiry_ts)
            .min()
    }

    pub fn expire_due(&mut self, now: i64, out: &mut Vec<ConditionalAction>) {
        let start = out.len();
        for (id, order) in &self.orders {
            if order.gtd_expiry_ts.is_some_and(|expiry| expiry <= now) {
                out.push(ConditionalAction::Expire { order_id: *id });
            }
        }
        for index in start..out.len() {
            let ConditionalAction::Expire { order_id } = out[index] else {
                continue;
            };
            self.orders.remove(&order_id);
        }
    }

    pub fn reset(&mut self) {
        self.orders.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gtd_expiry_precedes_trigger_and_order_is_stable() {
        let mut book = ConditionalOrderBook::default();
        assert!(book.insert(ConditionalOrder {
            order_id: 1,
            side: Side::Buy,
            trigger_kind: TriggerKind::StopMarket,
            trigger_price: 101.0,
            gtd_expiry_ts: Some(10),
        }));
        let mut actions = Vec::new();
        book.evaluate(10, 102.0, &mut actions);
        assert_eq!(actions, [ConditionalAction::Expire { order_id: 1 }]);
    }
}
