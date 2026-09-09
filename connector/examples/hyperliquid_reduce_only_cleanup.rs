use anyhow::{Context, Result, ensure};
use connector::{
    api::{ApiOrderType, ApiPositionSide, ApiSide, ApiTimeInForce, BrokerApi, UnifiedOrderRequest},
    hyperliquid::client::HyperliquidClient,
};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    ensure!(
        std::env::var("HL_CONFIRM_REDUCE_ONLY").as_deref() == Ok("YES"),
        "HL_CONFIRM_REDUCE_ONLY=YES is required"
    );
    let key_hex = std::env::var("HL_PRIVATE_KEY").context("HL_PRIVATE_KEY is required")?;
    let account = std::env::var("HL_ACCOUNT_ADDRESS").context("HL_ACCOUNT_ADDRESS is required")?;
    let symbol = std::env::var("HL_CLEANUP_SYMBOL").unwrap_or_else(|_| "BTC".to_owned());
    let qty: f64 = std::env::var("HL_CLEANUP_QTY")
        .context("HL_CLEANUP_QTY is required")?
        .parse()
        .context("HL_CLEANUP_QTY must be numeric")?;
    let side = match std::env::var("HL_CLEANUP_SIDE").as_deref() {
        Ok("buy") => ApiSide::Buy,
        Ok("sell") => ApiSide::Sell,
        _ => anyhow::bail!("HL_CLEANUP_SIDE must be buy or sell"),
    };
    ensure!(qty.is_finite() && qty > 0.0, "cleanup quantity must be positive");
    let key_hex = key_hex.trim().strip_prefix("0x").unwrap_or(key_hex.trim());
    let mut key = [0_u8; 32];
    hex::decode_to_slice(key_hex, &mut key).context("private key must be 32-byte hex")?;
    let client = HyperliquidClient::new(
        "https://api.hyperliquid.xyz/info",
        "https://api.hyperliquid.xyz/exchange",
    )
    .with_signer(key, account, true);
    let order = BrokerApi::submit_order(
        &client,
        &UnifiedOrderRequest {
            symbol,
            side,
            order_type: ApiOrderType::Market,
            price: None,
            qty,
            time_in_force: ApiTimeInForce::IOC,
            reduce_only: true,
            position_side: Some(ApiPositionSide::Unknown),
            client_order_id: None,
            stop_price: None,
        },
    )
    .await
    .context("submit reduce-only cleanup")?;
    println!("cleanup_order_status={:?} executed_qty={}", order.status, order.executed_qty);
    Ok(())
}
