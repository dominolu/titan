//! Solana REST（JSON-RPC over HTTP）实盘探针。
//!
//! 全程使用本仓库自带的 Rust 栈（ed25519-dalek 签名 + reqwest RPC），不依赖
//! solana CLI，作为方案 B（Solana venue）的 REST 通道验收：
//!
//! 1. 加载 `.secrets/sol_test_keypair.json`（solana-keygen 64 字节格式）；
//! 2. `getBalance` / `getLatestBlockhash` / 版本与 slot 探测；
//! 3. 组装一笔系统程序自转账（1000 lamports，仅消耗手续费）；
//! 4. 本地 ed25519 签名 → `sendTransaction`（base58）→ 轮询 `getSignatureStatuses` 确认。
//!
//! 运行：`cargo run -p connector --example solana_rest_probe`
//! （可带参数覆盖 RPC：`cargo run ... -- https://solana-rpc.publicnode.com`）

use std::time::{Duration, Instant};

use ed25519_dalek::{Signer, SigningKey};
use reqwest::Client;
use serde_json::{Value, json};

const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";
const TRANSFER_LAMPORTS: u64 = 1_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rpc = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "https://solana-rpc.publicnode.com".to_string());
    let client = Client::builder().timeout(Duration::from_secs(15)).build()?;
    let keypair_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../.secrets/sol_test_keypair.json"
    );
    let bytes: Vec<u8> = serde_json::from_slice(&std::fs::read(keypair_path)?)?;
    if bytes.len() != 64 {
        return Err(format!("keypair must be 64 bytes, got {}", bytes.len()).into());
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&bytes[..32]);
    let signing = SigningKey::from_bytes(&seed);
    let pubkey = signing.verifying_key().to_bytes();
    if pubkey != bytes[32..] {
        return Err("keypair file inconsistent: derived pubkey mismatch".into());
    }
    let pubkey_b58 = bs58::encode(pubkey).into_string();
    println!("== Solana REST 实盘探针 ==");
    println!("rpc    : {rpc}");
    println!("pubkey : {pubkey_b58}");

    // 1. 节点版本 + slot
    let version = rpc_call(&client, &rpc, "getVersion", json!([])).await?;
    println!("\n[1] 节点版本: {}", version["result"]);
    let slot = rpc_call(
        &client,
        &rpc,
        "getSlot",
        json!([{"commitment":"confirmed"}]),
    )
    .await?;
    println!("[1] 当前 slot: {}", slot["result"]);

    // 2. 余额
    let bal = rpc_call(&client, &rpc, "getBalance", json!([pubkey_b58])).await?;
    let lamports = bal["result"]["value"].as_u64().unwrap_or(0);
    println!(
        "[2] 余额: {:.9} SOL ({lamports} lamports)",
        lamports as f64 / 1e9
    );

    // 3. 取 recent blockhash（finalized：负载均衡的多节点环境下所有后端都认识它，
    //    且仍在 150 块（~90s）有效窗口内；confirmed 哈希在公共 RPC 上偶发 "Blockhash not found"）
    let bh = rpc_call(
        &client,
        &rpc,
        "getLatestBlockhash",
        json!([{"commitment": "finalized"}]),
    )
    .await?;
    let blockhash_b58 = bh["result"]["value"]["blockhash"]
        .as_str()
        .ok_or("no blockhash in response")?
        .to_string();
    let blockhash_vec = bs58::decode(&blockhash_b58)
        .into_vec()
        .map_err(|e| format!("bad blockhash b58: {e}"))?;
    let blockhash: [u8; 32] = blockhash_vec
        .try_into()
        .map_err(|v: Vec<u8>| format!("blockhash must be 32 bytes, got {}", v.len()))?;
    println!("[3] blockhash: {blockhash_b58}");

    // 4. 组装 legacy transfer-to-self 交易并本地签名
    //    消息格式：header(3) + 账户数(1) + 账户(64) + blockhash(32) + 指令数(1) + 指令
    //    指令：programIdIndex(1) + 账户列表(1) + 数据(12) = transfer 变体(4) + lamports(8)
    let system_program_vec = bs58::decode(SYSTEM_PROGRAM)
        .into_vec()
        .map_err(|e| format!("bad system program b58: {e}"))?;
    let system_program: [u8; 32] = system_program_vec
        .try_into()
        .map_err(|v: Vec<u8>| format!("system program must be 32 bytes, got {}", v.len()))?;
    let mut message = Vec::with_capacity(1 + 3 + 1 + 64 + 32 + 1 + 1 + 1 + 12);
    message.extend_from_slice(&[1u8, 0, 1]); // 需 1 个签名；无只读签名账户；1 个只读未签名账户
    message.push(2); // 账户键数量
    message.extend_from_slice(&pubkey);
    message.extend_from_slice(&system_program);
    message.extend_from_slice(&blockhash);
    message.push(1); // 指令数量
    message.push(1); // programIdIndex -> system program
    message.push(2); // 指令账户数量：transfer 需要 source + destination（自转也都要列）
    message.push(0); // source -> payer
    message.push(0); // destination -> payer
    message.push(12); // 指令数据长度
    message.extend_from_slice(&2u32.to_le_bytes()); // SystemInstruction::Transfer
    message.extend_from_slice(&TRANSFER_LAMPORTS.to_le_bytes());

    let signature = signing.sign(&message).to_bytes();
    let mut tx = Vec::with_capacity(1 + 64 + message.len());
    tx.push(1); // 签名数量
    tx.extend_from_slice(&signature);
    tx.extend_from_slice(&message);
    let tx_b58 = bs58::encode(&tx).into_string();
    let sig_b58 = bs58::encode(signature).into_string();
    println!(
        "[4] 已签名: transfer {TRANSFER_LAMPORTS} lamports -> self，tx {} 字节",
        tx.len()
    );

    // 5. 广播
    let sent = rpc_call(
        &client,
        &rpc,
        "sendTransaction",
        json!([tx_b58, {"encoding": "base58", "skipPreflight": false, "maxRetries": 1}]),
    )
    .await?;
    if sent.get("error").is_some() {
        eprintln!("[5] 广播失败: {}", serde_json::to_string_pretty(&sent)?);
        return Err("sendTransaction rejected".into());
    }
    let landed_sig = sent["result"].as_str().unwrap_or(&sig_b58);
    println!("[5] 广播成功: {landed_sig}");
    println!("    浏览器: https://solscan.io/tx/{landed_sig}");

    // 6. 轮询确认
    let started = Instant::now();
    loop {
        let st = rpc_call(
            &client,
            &rpc,
            "getSignatureStatuses",
            json!([[sig_b58], {"searchTransactionHistory": false}]),
        )
        .await?;
        let status = &st["result"]["value"][0];
        if !status.is_null() {
            let confirmations = status["confirmations"].as_u64().unwrap_or(0);
            let err = &status["err"];
            if err.is_null() {
                println!(
                    "[6] 已上链（confirmations={confirmations}，耗时 {:.2}s）",
                    started.elapsed().as_secs_f64()
                );
            } else {
                eprintln!("[6] 链上失败: {err}");
                return Err("transaction failed on chain".into());
            }
            break;
        }
        if started.elapsed() > Duration::from_secs(60) {
            return Err("confirmation timeout (60s)".into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // 7. 复核余额
    let bal = rpc_call(&client, &rpc, "getBalance", json!([pubkey_b58])).await?;
    let after = bal["result"]["value"].as_u64().unwrap_or(0);
    println!(
        "[7] 余额复核: {:.9} SOL（消耗 {} lamports = 手续费 {} + 转出 {} 自转回）",
        after as f64 / 1e9,
        lamports - after,
        5_000,
        TRANSFER_LAMPORTS
    );
    Ok(())
}

async fn rpc_call(
    client: &Client,
    url: &str,
    method: &str,
    params: Value,
) -> Result<Value, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let resp = client
        .post(url)
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await?;
    let elapsed = started.elapsed().as_secs_f64();
    let body: Value = resp.json().await?;
    if body.get("error").is_some() {
        eprintln!("    {method} rpc error: {}", body["error"]);
    } else {
        println!("    {method} ok ({elapsed:.3}s)");
    }
    Ok(body)
}
