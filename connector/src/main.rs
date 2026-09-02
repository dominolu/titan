use std::{
    collections::{HashMap, hash_map::Entry},
    fs::read_to_string,
    panic,
    process::exit,
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use clap::Parser;
use hftbacktest::{
    live::ipc::{
        MAX_FEED_BATCH_EVENTS, TO_ALL,
        iceoryx::{ChannelError, IceoryxBuilder},
        instrument_id,
    },
    prelude::*,
};
use iceoryx2::{
    node::NodeBuilder,
    prelude::{SignalHandlingMode, ipc},
};
use tokio::{runtime::Builder, select, signal, sync::Notify};
use tracing::error;

use crate::{
    binancefutures::BinanceFutures,
    binancespot::BinanceSpot,
    bybit::Bybit,
    connector::{Connector, ConnectorBuilder, GetOrders, PublishEvent},
};

#[cfg(feature = "hyperliquid")]
use crate::hyperliquid::Hyperliquid;
#[cfg(feature = "okx")]
use crate::okx::Okx;

#[cfg(feature = "binancefutures")]
pub mod binancefutures;
#[cfg(feature = "binancespot")]
pub mod binancespot;
#[cfg(feature = "bybit")]
pub mod bybit;
#[cfg(feature = "hyperliquid")]
pub mod hyperliquid;
#[cfg(feature = "okx")]
pub mod okx;

mod api;
mod connector;
mod market_event;
//mod fuse;
mod utils;

struct Position {
    qty: f64,
    exch_ts: i64,
}

fn run_receive_task(
    name: &str,
    tx: crate::connector::PublishSender,
    connector: &mut Box<dyn Connector>,
    shutting_down: &AtomicBool,
) -> Result<(), ChannelError> {
    let node = NodeBuilder::new()
        .signal_handling_mode(SignalHandlingMode::Disabled)
        .create::<ipc::Service>()
        .map_err(|error| ChannelError::BuildError(error.to_string()))?;
    let bot_rx = IceoryxBuilder::new(name).bot(false).receiver()?;
    while !shutting_down.load(Ordering::Acquire) {
        let cycle_time = Duration::from_nanos(1000);
        match node.wait(cycle_time) {
            Ok(()) => {
                while let Some((id, ev)) = bot_rx.receive()? {
                    match ev {
                        LiveRequest::Order {
                            symbol: asset,
                            order,
                        } => match order.req {
                            Status::New => {
                                // Requests to the Connector submit the new order.
                                connector.submit(asset, order, tx.clone());
                            }
                            Status::Canceled => {
                                // Requests to the Connector cancel the order.
                                connector.cancel(asset, order, tx.clone());
                            }
                            status => {
                                error!(?status, "An invalid request was received from the bot.");
                            }
                        },
                        LiveRequest::RegisterInstrument {
                            symbol,
                            tick_size,
                            lot_size,
                        } => {
                            // Makes prepare the publisher thread to also add the instrument.
                            tx.send(PublishEvent::RegisterInstrument {
                                id,
                                symbol: symbol.clone(),
                                tick_size,
                                lot_size,
                            })
                            .unwrap();
                            // Requests to the Connector subscribe to the necessary feeds for the
                            // instrument.
                            connector.register(symbol);
                        }
                    }
                }
            }
            Err(_error) => {
                break;
            }
        }
    }
    Ok(())
}

async fn run_publish_task(
    name: &str,
    order_manager: Arc<Mutex<dyn GetOrders>>,
    mut rx: crate::connector::PublishReceiver,
    shutdown_signal: Arc<Notify>,
) -> Result<(), ChannelError> {
    let mut depth = HashMap::new();
    let mut position: HashMap<String, Position> = HashMap::new();
    let bot_tx = IceoryxBuilder::new(name).bot(false).sender()?;

    loop {
        select! {
            _ = shutdown_signal.notified() => {
                break;
            }
            Some(msg) = rx.recv() => {
                match msg {
                    PublishEvent::QueueOverflow { .. } => {
                        error!("bounded connector queue overflowed; market state requires resync");
                    }
                    PublishEvent::RegisterInstrument {
                        id,
                        symbol,
                        tick_size,
                        lot_size,
                    } => {
                        // Sends the current state (orders, position, and market depth) to the bot that
                        // requested to add this instrument in batch mode.
                        bot_tx.send(id, &LiveEvent::BatchStart)?;

                        for order in order_manager.lock().unwrap().orders(Some(symbol.clone())) {
                            bot_tx.send(
                                id,
                                &LiveEvent::Order {
                                    symbol: symbol.clone(),
                                    order,
                                },
                            )?;
                        }

                        if let Some(position) = position.get(&symbol) {
                            bot_tx.send(
                                id,
                                &LiveEvent::Position {
                                    symbol: symbol.clone(),
                                    qty: position.qty,
                                    exch_ts: position.exch_ts,
                                },
                            )?;
                        }

                        match depth.entry(symbol) {
                            Entry::Occupied(mut entry) => {
                                let depth_: &mut FusedHashMapMarketDepth = entry.get_mut();
                                let snapshot = depth_.snapshot();
                                for event in snapshot {
                                    bot_tx.send(
                                        id,
                                        &LiveEvent::Feed {
                                            symbol: entry.key().clone(),
                                            event,
                                        },
                                    )?;
                                }
                            }
                            Entry::Vacant(entry) => {
                                entry.insert(FusedHashMapMarketDepth::new(tick_size, lot_size));
                            }
                        }

                        bot_tx.send(id, &LiveEvent::BatchEnd)?;
                    }
                    PublishEvent::LiveEvent(ev) => {
                        // The live event will only be published if the result is true.
                        for ev in handle_ev(ev, &mut depth, &mut position) {
                            bot_tx.send(TO_ALL, &ev)?;
                        }
                    }
                    PublishEvent::FeedBatch { symbol, events, .. } => {
                        let mut fused = Vec::with_capacity(events.len());
                        for event in events {
                            fused.extend(handle_feed_event(&symbol, event, &mut depth));
                        }
                        let instrument_id = instrument_id(&symbol);
                        for events in fused.chunks(MAX_FEED_BATCH_EVENTS) {
                            bot_tx.send_feed_batch(TO_ALL, instrument_id, events)?;
                        }
                    }
                    PublishEvent::StreamInvalidated { symbol, epoch } => {
                        error!(%symbol, epoch, "connector invalidated market stream");
                    }
                    PublishEvent::BatchStart(id) => {
                        bot_tx.send(id, &LiveEvent::BatchStart)?;
                    }
                    PublishEvent::BatchEnd(id) => {
                        bot_tx.send(id, &LiveEvent::BatchEnd)?;
                    }
                    PublishEvent::PrivateStreamReady => {}
                }
            }
        }
    }
    Ok(())
}

fn handle_feed_event(
    symbol: &str,
    event: Event,
    depth: &mut HashMap<String, FusedHashMapMarketDepth>,
) -> Vec<Event> {
    if event.is(BUY_EVENT | DEPTH_EVENT) {
        let Some(depth) = depth.get_mut(symbol) else {
            return vec![];
        };
        depth.update_bid_depth(event)
    } else if event.is(SELL_EVENT | DEPTH_EVENT) {
        let Some(depth) = depth.get_mut(symbol) else {
            return vec![];
        };
        depth.update_ask_depth(event)
    } else if event.is(BUY_EVENT | DEPTH_BBO_EVENT) {
        let Some(depth) = depth.get_mut(symbol) else {
            return vec![];
        };
        depth.update_best_bid(event)
    } else if event.is(SELL_EVENT | DEPTH_BBO_EVENT) {
        let Some(depth) = depth.get_mut(symbol) else {
            return vec![];
        };
        depth.update_best_ask(event)
    } else {
        if event.is(DEPTH_CLEAR_EVENT)
            && let Some(depth) = depth.get_mut(symbol)
        {
            depth.clear_depth(Side::None, 0.0, 0);
        }
        vec![event]
    }
}

/// Maintains the market depth for all added instruments, allowing another bot to request the same
/// instrument and publishing the market depth snapshot, and fuses the market depth from different
/// streams, such as L1 or L2 with varying depths and update frequencies, to provide the most
/// granular and frequent updates.
///
/// Returns true when the received live event needs to be published; otherwise, it does not.
/// For example, publication is unnecessary if the received market depth data is outdated by more
/// recent data from a different stream due to fusion.
fn handle_ev(
    ev: LiveEvent,
    depth: &mut HashMap<String, FusedHashMapMarketDepth>,
    position: &mut HashMap<String, Position>,
) -> Vec<LiveEvent> {
    match &ev {
        LiveEvent::Feed { symbol, event } => {
            return handle_feed_event(symbol, event.clone(), depth)
                .into_iter()
                .map(|event| LiveEvent::Feed {
                    symbol: symbol.clone(),
                    event,
                })
                .collect();
        }
        LiveEvent::Position {
            symbol,
            qty,
            exch_ts,
        } => {
            if position.contains_key(symbol) {
                let position = position.get_mut(symbol).unwrap();
                return if *exch_ts >= position.exch_ts {
                    position.qty = *qty;
                    vec![ev]
                } else {
                    vec![]
                };
            } else {
                position.insert(
                    symbol.clone(),
                    Position {
                        qty: *qty,
                        exch_ts: *exch_ts,
                    },
                );
                return vec![ev];
            }
        }
        _ => {}
    }
    vec![ev]
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the connector, used when connecting the bot to the connector.
    name: String,

    /// Connector
    /// * binancefutures: Binance USD-m Futures
    /// * bybit: Bybit Linear Futures
    /// * okx: OKX V5 Swap
    /// * hyperliquid: Hyperliquid Perpetual
    connector: String,

    /// Connector's configuration file path.
    config: String,
}

#[tokio::main]
async fn main() {
    // Ensures that the main thread will terminate if any of its child threads panics.
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        orig_hook(panic_info);
        exit(1);
    }));

    let args = Args::parse();

    tracing_subscriber::fmt::init();

    // Listen for shut down signal and notify publish task.
    let shutdown_signal = Arc::new(Notify::new());
    let shutting_down = Arc::new(AtomicBool::new(false));
    let shutdown_signal_ = shutdown_signal.clone();
    let shutting_down_ = shutting_down.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            // Wait for either SIGINT (CTRL+C) or SIGTERM on Unix.
            let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
            select! {
                _ = signal::ctrl_c() => {},
                _ = sigterm.recv() => {},
            }
        }
        #[cfg(not(unix))]
        {
            // Non-Unix platforms only has SIGINT.
            if let Err(error) = signal::ctrl_c().await {
                error!(?error, "Couldn't listen for shutdown signal.");
            }
        }
        shutting_down_.store(true, Ordering::Release);
        shutdown_signal_.notify_waiters();
    });

    let (pub_tx, pub_rx) =
        crate::connector::publish_channel(crate::connector::DEFAULT_PUBLISH_QUEUE_CAPACITY);

    let config = read_to_string(&args.config)
        .map_err(|error| {
            error!(
                ?error,
                config = args.config,
                "An error occurred while reading the configuration file."
            );
        })
        .unwrap();

    let mut connector: Box<dyn Connector> = match args.connector.as_str() {
        "binancefutures" => {
            let mut connector = BinanceFutures::build_from(&config)
                .map_err(|error| {
                    error!(?error, "Couldn't build the BinanceFutures connector.");
                })
                .unwrap();
            connector.run(pub_tx.clone());
            Box::new(connector)
        }
        "bybit" => {
            let mut connector = Bybit::build_from(&config)
                .map_err(|error| {
                    error!(?error, "Couldn't build the Bybit connector.");
                })
                .unwrap();
            connector.run(pub_tx.clone());
            Box::new(connector)
        }
        "binancespot" => {
            let mut connector = BinanceSpot::build_from(&config)
                .map_err(|error| {
                    error!(?error, "Couldn't build the Bybit connector.");
                })
                .unwrap();
            connector.run(pub_tx.clone());
            Box::new(connector)
        }
        #[cfg(feature = "okx")]
        "okx" => {
            let mut connector = Okx::build_from(&config)
                .map_err(|error| {
                    error!(?error, "Couldn't build the OKX connector.");
                })
                .unwrap();
            connector.run(pub_tx.clone());
            Box::new(connector)
        }
        #[cfg(feature = "hyperliquid")]
        "hyperliquid" => {
            let mut connector = Hyperliquid::build_from(&config)
                .map_err(|error| {
                    error!(?error, "Couldn't build the Hyperliquid connector.");
                })
                .unwrap();
            connector.run(pub_tx.clone());
            Box::new(connector)
        }
        connector => {
            error!(%connector, "This connector doesn't exist.");
            exit(1);
        }
    };

    let name = args.name.clone();
    let order_manager = connector.order_manager();
    let handle = thread::spawn(move || {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();

        rt.block_on(async move {
            run_publish_task(&name, order_manager, pub_rx, shutdown_signal)
                .await
                .map_err(|error: ChannelError| {
                    error!(
                        ?error,
                        "An error occurred while sending a live event to the bots."
                    );
                })
                .unwrap();
        });
    });

    let name = args.name;
    run_receive_task(&name, pub_tx, &mut connector, &shutting_down)
        .map_err(|error| {
            error!(
                ?error,
                "An error occurred while receiving a request from the bots."
            );
        })
        .unwrap();
    if let Err(error) = connector.shutdown().await {
        error!(%error, "failed to cancel every open order during connector shutdown");
    }
    let _ = handle.join();
}
