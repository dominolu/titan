use anyhow::anyhow;
use clap::Parser;
use tokio::{self, select, signal};
use tracing::info;

mod binance;
mod binancefuturescm;
mod binancefuturesum;
mod bybit;
mod error;
mod file;
mod hyperliquid;
mod pipeline;
mod throttler;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path for the files where collected data will be written.
    path: String,

    /// Name of the exchange
    exchange: String,

    /// Symbols for which data will be collected.
    symbols: Vec<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let (writer_tx, mut writer) = pipeline::start(args.path, 8192);
    let finish = writer_tx.clone();

    let handle = match args.exchange.as_str() {
        "binancefutures" | "binancefuturesum" => {
            let streams = [
                "$symbol@trade",
                "$symbol@bookTicker",
                "$symbol@depth@0ms",
                // "$symbol@@markPrice@1s"
            ]
            .iter()
            .map(|stream| stream.to_string())
            .collect();

            tokio::spawn(binancefuturesum::run_collection(
                streams,
                args.symbols,
                writer_tx,
            ))
        }
        "binancefuturescm" => {
            let streams = [
                "$symbol@trade",
                "$symbol@bookTicker",
                "$symbol@depth@0ms",
                // "$symbol@@markPrice@1s"
            ]
            .iter()
            .map(|stream| stream.to_string())
            .collect();

            tokio::spawn(binancefuturescm::run_collection(
                streams,
                args.symbols,
                writer_tx,
            ))
        }
        "binance" | "binancespot" => {
            let streams = ["$symbol@trade", "$symbol@bookTicker", "$symbol@depth@100ms"]
                .iter()
                .map(|stream| stream.to_string())
                .collect();

            tokio::spawn(binance::run_collection(streams, args.symbols, writer_tx))
        }
        "bybit" => {
            let topics = [
                "orderbook.1.$symbol",
                "orderbook.50.$symbol",
                "orderbook.500.$symbol",
                "publicTrade.$symbol",
            ]
            .iter()
            .map(|topic| topic.to_string())
            .collect();

            tokio::spawn(bybit::run_collection(topics, args.symbols, writer_tx))
        }
        "hyperliquid" => {
            let subscriptions = ["trades", "l2Book", "bbo"]
                .iter()
                .map(|sub| sub.to_string())
                .collect();

            tokio::spawn(hyperliquid::run_collection(
                subscriptions,
                args.symbols,
                writer_tx,
            ))
        }
        exchange => {
            return Err(anyhow!("{exchange} is not supported."));
        }
    };

    let outcome = select! {
            signal = shutdown_signal() => {
                info!(signal = ?signal, "shutdown signal received");
                signal.map_err(anyhow::Error::from)
            }
            result = &mut writer => { handle.abort(); let _ = handle.await; result??; return Ok(()); }
    };
    handle.abort();
    let _ = handle.await;
    finish.finish().await?;
    writer.await??;
    outcome
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        select! { result = signal::ctrl_c() => result, _ = terminate.recv() => Ok(()) }
    }
    #[cfg(not(unix))]
    {
        signal::ctrl_c().await
    }
}
