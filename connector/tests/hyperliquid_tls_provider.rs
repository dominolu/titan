#![cfg(feature = "hyperliquid")]

use std::{sync::Barrier, thread};

use connector::hyperliquid::client::HyperliquidClient;

// Keep this regression in its own integration-test binary: another test must not
// initialize the process-wide provider before the direct client constructors run.
#[test]
fn concurrent_client_construction_initializes_tls_in_a_fresh_process() {
    assert!(rustls::crypto::CryptoProvider::get_default().is_none());

    let start = Barrier::new(8);
    thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    start.wait();
                    let _client = HyperliquidClient::new(
                        "http://127.0.0.1:9/info",
                        "http://127.0.0.1:9/exchange",
                    );
                })
            })
            .collect();
        for worker in workers {
            worker.join().expect("direct client construction panicked");
        }
    });

    assert!(rustls::crypto::CryptoProvider::get_default().is_some());
}
