//! Solana venue 实盘探针：通过统一 [`BrokerApi`] 跑通"行情 → 买入 → 确认 → 卖出 → 对账"
//! 的完整链上套利环路（真实主网交易）。
//!
//! 流程：
//! 1. 构造 Solana venue（真实 keypair + Raydium V4 池）；
//! 2. `run()` 启动 WS 行情后端，等待 vault 储备加载（轮询 ticker）；
//! 3. `get_ticker` / `get_order_book` / `get_account` 检查行情与账户；
//! 4. BUY base_qty（WSOL→token）→ 轮询 `get_order` 等确认；
//! 5. SELL 相同数量（token→WSOL）→ 等确认；
//! 6. 复核余额与订单历史。
//!
//! 运行：`cargo run -p connector --example solana_swap_probe --release`
//! 成本：两次池手续费（约 0.25% × 名义额）+ 交易费，金额由 QTY_TOKEN 控制（默认约 0.005 SOL 名义）。

use connector::api::{
    ApiOrderStatus, ApiOrderType, ApiSide, ApiTimeInForce, BrokerApi, UnifiedOrderRequest,
};
use connector::connector::{Connector, ConnectorBuilder, PublishSender};
use connector::solana::Solana;

const RPC_URL: &str = "https://solana-rpc.publicnode.com";
const WS_URL: &str = "wss://solana-rpc.publicnode.com";

/// 交易池：FoRGER/WSOL（Raydium V4，2026-09 实盘验证）。
const AMM_ID: &str = "DuYCVcXhgDUpwMPTLdGkdaR3jfSXTu53874mjayoMHAd";
const BASE_MINT: &str = "FoRGERiW7odcCBGU1bztZi16osPBHjxharvDathL5eds";
const QUOTE_MINT: &str = "So11111111111111111111111111111111111111112";
const BASE_VAULT: &str = "CHEg2jyU7oGwuG7JjCB5ktUrbL8RqshuuxThv9S8RFCi";
const QUOTE_VAULT: &str = "CzQVmNkzZfpyaAJ3gmJXD3uiqMe9SGirARncMQsGxnnX";

/// 本轮 round-trip 的 base 数量（6dp 代币；约 0.005 SOL 名义）。
const QTY_TOKEN: f64 = 200_000.0;

const SYMBOL: &str = "FORGER/WSOL";

fn config_str() -> String {
    let keypair = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../.secrets/sol_test_keypair.json"
    );
    format!(
        r#"rpc_url = "{RPC_URL}"
ws_url = "{WS_URL}"
keypair_path = "{keypair}"
priority_fee_micro_lamports = 100000
slippage_bps = 200
fee_bps = 25

[[pools]]
symbol = "{SYMBOL}"
amm_id = "{AMM_ID}"
base_mint = "{BASE_MINT}"
quote_mint = "{QUOTE_MINT}"
base_vault = "{BASE_VAULT}"
quote_vault = "{QUOTE_VAULT}"
base_decimals = 6
quote_decimals = 9
"#
    )
}

async fn wait_final(
    api: &dyn BrokerApi,
    client_order_id: &str,
    label: &str,
) -> Result<connector::api::OrderInfo, Box<dyn std::error::Error>> {
    let started = std::time::Instant::now();
    loop {
        let order = api.get_order(SYMBOL, None, Some(client_order_id)).await?;
        if !matches!(order.status, ApiOrderStatus::New) {
            println!(
                "[{label}] 终态 {:?} executed={} sig={}",
                order.status, order.executed_qty, order.order_id
            );
            return Ok(order);
        }
        if started.elapsed() > std::time::Duration::from_secs(90) {
            return Err(format!("{label} confirmation timeout").into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(700)).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // WS(rustls) 需要显式选择 crypto provider。
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();
    tracing_subscriber::fmt::init();
    let mut venue = Solana::build_from(&config_str())?;
    let publisher: PublishSender = connector::connector::direct_publish_sender(|_| {});
    venue.run(publisher);

    // BrokerApi 通过 trait object 获取（与策略层使用方式一致）。
    let api = venue.broker_api().expect("broker api");
    let api: &dyn BrokerApi = api.as_ref();

    // 1. 连通性 + 行情加载（WS 后端初始 HTTP 刷新后即有储备）。
    api.ping().await?;
    println!("== Solana venue 实盘探针 ==");
    let mut ticker = None;
    for _ in 0..30 {
        match api.get_ticker(SYMBOL).await {
            Ok(t) if t.last_price > 0.0 => {
                ticker = Some(t);
                break;
            }
            _ => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
        }
    }
    let ticker = ticker.ok_or("market state failed to load within 15s")?;
    println!("[1] ticker: last={:.8} SOL/token", ticker.last_price);
    let book = api.get_order_book(SYMBOL, 3).await?;
    println!(
        "[1] book: bid={:?} ask={:?}",
        book.bids.first().map(|l| l.price),
        book.asks.first().map(|l| l.price)
    );
    let account_before = api.get_account().await?;
    let sol_before = account_before
        .balances
        .iter()
        .find(|b| b.asset == "SOL")
        .map(|b| b.wallet_balance)
        .unwrap_or(0.0);
    println!("[1] 余额: {sol_before:.9} SOL");

    // 2. 买入 QTY_TOKEN（WSOL → token）。
    let buy_id = format!("probe-buy-{}", chrono::Utc::now().timestamp_millis());
    let buy_req = UnifiedOrderRequest {
        symbol: SYMBOL.to_string(),
        side: ApiSide::Buy,
        order_type: ApiOrderType::Market,
        price: None,
        qty: QTY_TOKEN,
        time_in_force: ApiTimeInForce::IOC,
        reduce_only: false,
        position_side: None,
        client_order_id: Some(buy_id.clone()),
        stop_price: None,
    };
    println!("[2] 买入 {} tokens ...", QTY_TOKEN);
    let placed = api.submit_order(&buy_req).await?;
    println!("    sig={}", placed.order_id);
    let buy = wait_final(api, &buy_id, "2-BUY").await?;
    if buy.status != ApiOrderStatus::Filled {
        return Err(format!("buy failed: {:?}", buy.status).into());
    }

    // 3. 卖出相同数量（token → WSOL，自动解包）。
    let sell_id = format!("probe-sell-{}", chrono::Utc::now().timestamp_millis());
    let sell_req = UnifiedOrderRequest {
        symbol: SYMBOL.to_string(),
        side: ApiSide::Sell,
        order_type: ApiOrderType::Market,
        price: None,
        qty: QTY_TOKEN,
        time_in_force: ApiTimeInForce::IOC,
        reduce_only: false,
        position_side: None,
        client_order_id: Some(sell_id.clone()),
        stop_price: None,
    };
    println!("[3] 卖出 {} tokens ...", QTY_TOKEN);
    api.submit_order(&sell_req).await?;
    let sell = wait_final(api, &sell_id, "3-SELL").await?;
    if sell.status != ApiOrderStatus::Filled {
        return Err(format!("sell failed: {:?}", sell.status).into());
    }

    // 4. 对账。
    let account_after = api.get_account().await?;
    let sol_after = account_after
        .balances
        .iter()
        .find(|b| b.asset == "SOL")
        .map(|b| b.wallet_balance)
        .unwrap_or(0.0);
    println!("[4] round-trip 完成：");
    println!(
        "    SOL 变动: {sol_before:.9} -> {sol_after:.9}（{} lamports）",
        ((sol_after - sol_before) * 1e9) as i64
    );
    let history = api.get_order_history(SYMBOL, 5).await?;
    for order in &history {
        println!(
            "    {} {:?} qty={} sig={}",
            order.client_order_id, order.status, order.qty, order.order_id
        );
    }
    Ok(())
}
