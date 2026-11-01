//! Shared live/replay admission while a required depth view is unavailable.
use crate::{StagedCommandV13, TitanActiveOrderView, TitanPositionView};
use std::collections::BTreeMap;

pub(crate) fn validate_depth_commands(
    ready: bool,
    commands: &[StagedCommandV13],
    positions: &[TitanPositionView],
    orders: &[TitanActiveOrderView],
) -> Result<(), &'static str> {
    if ready {
        return Ok(());
    }
    let mut reserved = BTreeMap::<(u32, u32, u8, u8), i128>::new();
    for order in orders.iter().filter(|o| !matches!(o.status, 4 | 6..=8)) {
        *reserved
            .entry((
                order.account_no,
                order.asset_no,
                order.position_side,
                order.side,
            ))
            .or_default() +=
            (i128::from(order.qty_lots) - i128::from(order.cumulative_filled_lots)).max(0);
    }
    for command in commands {
        let StagedCommandV13::Submit { request, .. } = command else {
            continue;
        };
        if request.reduce_only != 1 {
            return Err("depth_unavailable_for_opening");
        }
        let position = positions
            .iter()
            .find(|p| {
                p.account_no == request.account_no
                    && p.asset_no == request.asset_no
                    && p.position_side == request.position_side
            })
            .ok_or("depth_reduction_position_unproven")?;
        let closing_side = match (position.position_side, position.qty_lots) {
            (0, q) if q > 0 => 2,
            (0, q) if q < 0 => 1,
            (1, q) if q != 0 => 2,
            (2, q) if q != 0 => 1,
            _ => return Err("depth_reduction_position_unproven"),
        };
        if request.side != closing_side || request.qty_lots <= 0 {
            return Err("depth_reduction_direction_invalid");
        }
        let used = reserved
            .entry((
                request.account_no,
                request.asset_no,
                request.position_side,
                closing_side,
            ))
            .or_default();
        *used += i128::from(request.qty_lots);
        if *used > i128::from(position.qty_lots).abs() {
            return Err("depth_reduction_quantity_unproven");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TitanCancelOrderRequest, TitanSubmitOrderRequest};
    fn submit(qty: i64, reduce: u8, side: u8, position_side: u8) -> StagedCommandV13 {
        StagedCommandV13::Submit {
            order_id: 10,
            request: TitanSubmitOrderRequest {
                qty_lots: qty,
                reduce_only: reduce,
                side,
                position_side,
                ..Default::default()
            },
        }
    }
    #[test]
    fn unavailable_depth_retains_cancel_and_proven_reduction() {
        let p = TitanPositionView {
            qty_lots: 5,
            ..Default::default()
        };
        let commands = [
            StagedCommandV13::Cancel {
                command_id: 2,
                request: TitanCancelOrderRequest::default(),
            },
            submit(3, 1, 2, 0),
        ];
        assert!(validate_depth_commands(false, &commands, &[p], &[]).is_ok());
        assert!(validate_depth_commands(false, &[submit(1, 0, 1, 0)], &[p], &[]).is_err());
        assert!(validate_depth_commands(true, &[submit(1, 0, 1, 0)], &[], &[]).is_ok());
    }
    #[test]
    fn unproven_wrong_side_and_excess_reductions_are_rejected() {
        let p = TitanPositionView {
            qty_lots: 5,
            ..Default::default()
        };
        assert!(validate_depth_commands(false, &[submit(1, 1, 2, 0)], &[], &[]).is_err());
        assert!(validate_depth_commands(false, &[submit(1, 1, 1, 0)], &[p], &[]).is_err());
        assert!(validate_depth_commands(false, &[submit(6, 1, 2, 0)], &[p], &[]).is_err());
        assert!(
            validate_depth_commands(false, &[submit(3, 1, 2, 0), submit(3, 1, 2, 0)], &[p], &[])
                .is_err()
        );
    }
    #[test]
    fn pending_cancel_does_not_release_reduction_reservation() {
        let p = TitanPositionView {
            qty_lots: 5,
            ..Default::default()
        };
        let order = TitanActiveOrderView {
            qty_lots: 4,
            cumulative_filled_lots: 1,
            side: 2,
            status: 5,
            ..Default::default()
        };
        assert!(validate_depth_commands(false, &[submit(2, 1, 2, 0)], &[p], &[order]).is_ok());
        assert!(validate_depth_commands(false, &[submit(3, 1, 2, 0)], &[p], &[order]).is_err());
    }
    #[test]
    fn net_and_hedge_positions_keep_their_identity() {
        for (position_side, qty, side) in [(0, -5, 1), (1, 5, 2), (2, -5, 1), (2, 5, 1)] {
            let p = TitanPositionView {
                position_side,
                qty_lots: qty,
                ..Default::default()
            };
            assert!(
                validate_depth_commands(false, &[submit(5, 1, side, position_side)], &[p], &[])
                    .is_ok()
            );
        }
    }
}
