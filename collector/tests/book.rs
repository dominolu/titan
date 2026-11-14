use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use collector::book::{
    BlockHeader, BlockInput, BookWriter, Change, ClosedSlot, DepthMessage, Feed, FeedMessage,
    FeedMode, GapMessage, GapReason, Ladder, Level, MarketMeta, SegmentHeader, SegmentReader,
    SegmentWriter, Side, SnapshotMessage, build_data_block, decode_block, decode_data_payload,
    encode_block, encode_compressed_block, recover_segment, run_market,
};
use collector::file::{MarketEventWriter, TradeRecord, TradeSide};
use std::collections::VecDeque;

struct FixtureFeed(VecDeque<FeedMessage>);

impl Feed for FixtureFeed {
    async fn next(&mut self) -> Option<FeedMessage> {
        self.0.pop_front()
    }
}

fn test_dir() -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("titan-book-{}-{stamp}", std::process::id()));
    fs::create_dir_all(&path).unwrap();
    path
}

fn segment_header() -> SegmentHeader {
    SegmentHeader {
        market_id: 1,
        flags: 0,
        segment_start_local_ns: 100,
        segment_start_exch_ns: 200,
        segment_end_local_ns: 0,
        segment_end_exch_ns: 0,
        tick_units: 1,
        price_decimals: 2,
        qty_decimals: 3,
        grid_ms: 100,
        block_duration_ms: 60_000,
        depth_requested: 100,
        depth_available: 100,
        venue_id: 1,
        session_epoch: 1,
        qty_step_units: 1,
        encoding_flags: 1,
        feed_mode: FeedMode::Diff,
        symbol: "BTCUSDT".into(),
    }
}

#[test]
fn segment_roundtrip_and_indexed_read() {
    let dir = test_dir();
    let path = dir.join("segment.tnmd");
    let block = encode_block(&BlockInput {
        header: BlockHeader::data(1, 100, 200, 2).unwrap(),
        payload: vec![9, 8],
    })
    .unwrap();
    let mut writer = SegmentWriter::create(&path, &segment_header()).unwrap();
    writer.append(&block).unwrap();
    writer.seal(110, 210).unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.block_count(), 1);
    assert_eq!(reader.read_block(0).unwrap().payload[..2], [9, 8]);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn recovery_truncates_an_incomplete_tail_and_rebuilds_index() {
    let dir = test_dir();
    let path = dir.join("open.tnmd");
    let block = encode_block(&BlockInput {
        header: BlockHeader::data(1, 100, 200, 1).unwrap(),
        payload: vec![5],
    })
    .unwrap();
    let mut writer = SegmentWriter::create(&path, &segment_header()).unwrap();
    writer.append(&block).unwrap();
    // Drop an open segment as if the process stopped before sealing.
    drop(writer);
    let open_path = PathBuf::from(format!("{}.tmp", path.display()));
    let mut file = OpenOptions::new().append(true).open(&open_path).unwrap();
    file.write_all(b"TMB1\0\0").unwrap();
    file.sync_all().unwrap();
    let report = recover_segment(&open_path).unwrap();
    assert_eq!(report.blocks_found, 1);
    assert_eq!(report.truncated_bytes, 6);
    assert!(PathBuf::from(format!("{}.idx", open_path.display())).exists());
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn recovery_after_abrupt_process_exit_keeps_synced_blocks() {
    const CRASH_PATH: &str = "TITAN_BOOK_CRASH_TEST_PATH";
    if let Some(path) = std::env::var_os(CRASH_PATH) {
        let path = PathBuf::from(path);
        let block = encode_block(&BlockInput {
            header: BlockHeader::data(1, 100, 200, 1).unwrap(),
            payload: vec![7],
        })
        .unwrap();
        let mut writer =
            SegmentWriter::create_with_sync_interval(&path, &segment_header(), 1).unwrap();
        writer.append(&block).unwrap();
        // Bypass Rust destructors to model process death before segment sealing.
        std::process::exit(137);
    }

    let dir = test_dir();
    let path = dir.join("crashed.tnmd");
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "recovery_after_abrupt_process_exit_keeps_synced_blocks",
            "--nocapture",
        ])
        .env(CRASH_PATH, &path)
        .status()
        .unwrap();
    assert_eq!(child.code(), Some(137));
    let open_path = PathBuf::from(format!("{}.tmp", path.display()));
    let report = recover_segment(&open_path).unwrap();
    assert_eq!(report.blocks_found, 1);
    assert_eq!(report.truncated_bytes, 0);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
#[ignore = "server-side performance and capacity acceptance probe"]
fn benchmark_segment_compression_and_sequential_read() {
    const BLOCKS: usize = 500;
    const SLOTS_PER_BLOCK: usize = 600;
    let dir = test_dir();
    let path = dir.join("benchmark.tnmd");
    let mut event_writer = MarketEventWriter::new();
    let mut writer =
        SegmentWriter::create_with_sync_interval(&path, &segment_header(), 60).unwrap();
    let meta = MarketMeta {
        market_id: 1,
        venue_id: 2,
        symbol: "BTCUSDT".into(),
        feed_mode: FeedMode::Diff,
        tick_units: 1,
        price_decimals: 2,
        qty_step_units: 1,
        qty_decimals: 3,
        grid_ms: 100,
        block_duration_ms: 60_000,
        depth_requested: 100,
        depth_available: 100,
        session_epoch: 1,
    };
    let bids = (1..=100)
        .rev()
        .map(|tick| Level { tick, qty: 100 })
        .collect::<Vec<_>>();
    let asks = (101..=200)
        .map(|tick| Level { tick, qty: 100 })
        .collect::<Vec<_>>();
    let started = std::time::Instant::now();
    let mut raw_bytes = 0u64;
    for index in 0..BLOCKS {
        let start_ns = index as i64 * 60_000_000_000;
        let seq_base = index as u64 * SLOTS_PER_BLOCK as u64;
        let slots = (0..SLOTS_PER_BLOCK)
            .map(|slot| {
                let exch_ts_ns = start_ns + slot as i64 * 100_000_000 + 1_000_000;
                ClosedSlot {
                    slot: index as i64 * SLOTS_PER_BLOCK as i64 + slot as i64,
                    exch_ts_ns,
                    local_ts_ns: exch_ts_ns + 2_000,
                    seq_first_prev: Some(seq_base + slot as u64),
                    seq_last: seq_base + slot as u64 + 1,
                    n_msgs: 1,
                    n_msgs_saturated: false,
                    late_events: 0,
                    changes: (96..=100)
                        .map(|tick| Change {
                            side: Side::Bid,
                            tick,
                            qty: 2 + ((slot + tick as usize) % 10) as u64,
                        })
                        .collect(),
                    snapshot_bids: bids.clone(),
                    snapshot_asks: asks.clone(),
                }
            })
            .collect::<Vec<_>>();
        for slot in &slots {
            event_writer
                .write_trade(
                    &dir,
                    "binance",
                    "spot",
                    "BTCUSDT",
                    &TradeRecord {
                        exch_ts_ns: slot.exch_ts_ns,
                        local_ts_ns: slot.local_ts_ns,
                        side: TradeSide::Buy,
                        price: "100.00".into(),
                        qty: "0.001".into(),
                        trade_id: Some(slot.seq_last.to_string()),
                    },
                )
                .unwrap();
        }
        let block = build_data_block(
            &meta,
            start_ns,
            start_ns + 60_000_000_000,
            start_ns,
            100,
            seq_base,
            seq_base + SLOTS_PER_BLOCK as u64,
            SLOTS_PER_BLOCK as u16,
            bids.clone(),
            asks.clone(),
            &slots,
        )
        .unwrap();
        raw_bytes = raw_bytes.saturating_add(encode_block(&block).unwrap().len() as u64);
        writer
            .append(&encode_compressed_block(&block, 3).unwrap())
            .unwrap();
    }
    writer.seal(BLOCKS as i64, BLOCKS as i64).unwrap();
    event_writer.finish().unwrap();
    let write_elapsed = started.elapsed();
    let encoded_bytes = fs::metadata(&path).unwrap().len();
    let reader = SegmentReader::open(&path).unwrap();
    let read_started = std::time::Instant::now();
    let mut records_read = 0usize;
    for index in 0..reader.block_count() {
        let block = reader.read_block(index).unwrap();
        let payload = decode_data_payload(&block).unwrap();
        let mut ladder = Ladder::from_snapshot(&payload.bids, &payload.asks).unwrap();
        for record in &payload.records {
            ladder.apply(&record.changes).unwrap();
        }
        records_read += payload.records.len();
    }
    let read_elapsed = read_started.elapsed();
    let records_total = BLOCKS * SLOTS_PER_BLOCK;
    assert_eq!(records_read, records_total);
    let read_records_s = records_read as f64 / read_elapsed.as_secs_f64();
    let mut pending = vec![dir.join("trade")];
    let mut trade_bytes = 0u64;
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "gz") {
                trade_bytes = trade_bytes.saturating_add(fs::metadata(path).unwrap().len());
            }
        }
    }
    let projected_daily_bytes =
        (encoded_bytes + trade_bytes) as f64 / records_total as f64 * 864_000.0;
    eprintln!(
        "probe blocks={BLOCKS} records={records_total} raw_book_bytes={raw_bytes} encoded_book_bytes={encoded_bytes} encoded_trade_bytes={trade_bytes} bytes_per_slot={:.2} projected_book_trade_mb_per_market_day={:.1} projected_23_market_gb_per_day={:.3} compression_ratio={:.3} write_records_s={:.0} read_records_s={read_records_s:.0} read_mb_s={:.1}",
        encoded_bytes as f64 / records_total as f64,
        projected_daily_bytes / 1_000_000.0,
        projected_daily_bytes * 23.0 / 1_000_000_000.0,
        encoded_bytes as f64 / raw_bytes as f64,
        records_total as f64 / write_elapsed.as_secs_f64(),
        encoded_bytes as f64 / read_elapsed.as_secs_f64() / 1_000_000.0,
    );
    assert!(
        read_records_s >= 1_000_000.0,
        "replay throughput below 1M records/s"
    );
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn block_decoder_rejects_payload_corruption() {
    let mut block = encode_block(&BlockInput {
        header: BlockHeader::data(1, 100, 200, 1).unwrap(),
        payload: vec![3],
    })
    .unwrap();
    block[128] ^= 0xff;
    assert!(decode_block(&block).is_err());
}

#[test]
fn reader_skips_a_corrupt_block_and_reads_the_following_block() {
    let dir = test_dir();
    let path = dir.join("damaged.tnmd");
    let first = encode_block(&BlockInput {
        header: BlockHeader::data(1, 100, 200, 1).unwrap(),
        payload: vec![1],
    })
    .unwrap();
    let second = encode_block(&BlockInput {
        header: BlockHeader::data(1, 200, 300, 1).unwrap(),
        payload: vec![2],
    })
    .unwrap();
    let mut writer = SegmentWriter::create(&path, &segment_header()).unwrap();
    writer.append(&first).unwrap();
    writer.append(&second).unwrap();
    writer.seal(300, 400).unwrap();
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    use std::os::unix::fs::FileExt;
    file.write_at(&[0xff], 256).unwrap();
    file.sync_all().unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.block_count(), 2);
    assert!(reader.read_block(0).is_err());
    assert_eq!(reader.read_block(1).unwrap().payload[0], 2);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn reader_resynchronizes_after_a_damaged_block_header() {
    let dir = test_dir();
    let path = dir.join("header-damaged.tnmd");
    let first = encode_block(&BlockInput {
        header: BlockHeader::data(1, 100, 200, 1).unwrap(),
        payload: vec![1],
    })
    .unwrap();
    let second = encode_block(&BlockInput {
        header: BlockHeader::data(1, 200, 300, 1).unwrap(),
        payload: vec![2],
    })
    .unwrap();
    let mut writer = SegmentWriter::create(&path, &segment_header()).unwrap();
    writer.append(&first).unwrap();
    writer.append(&second).unwrap();
    writer.seal(300, 400).unwrap();
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    use std::os::unix::fs::FileExt;
    file.write_at(&[0xff], 128).unwrap();
    file.sync_all().unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.block_count(), 2);
    assert!(reader.read_block(0).is_err());
    assert_eq!(reader.read_block(1).unwrap().payload[0], 2);
    assert_eq!(reader.find_block(350), Some(1));
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn compressed_segment_appends_and_decodes_blocks() {
    let dir = test_dir();
    let path = dir.join("compressed.tnmd");
    let header = segment_header();
    let raw = BlockInput {
        header: BlockHeader::data(1, 100, 200, 64).unwrap(),
        payload: vec![7; 64],
    };
    let compressed = encode_compressed_block(&raw, 3).unwrap();
    let mut writer = SegmentWriter::create(&path, &header).unwrap();
    writer.append(&compressed).unwrap();
    writer.seal(200, 300).unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    let decoded = reader.read_block(0).unwrap();
    assert_ne!(decoded.header.block_flags & (1 << 5), 0);
    assert_eq!(decoded.payload, raw.payload);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn book_writer_rotates_and_seals_compressed_segments() {
    let dir = test_dir();
    let meta = MarketMeta {
        market_id: 7,
        venue_id: 2,
        symbol: "ETHUSDT".into(),
        feed_mode: FeedMode::Diff,
        tick_units: 1,
        price_decimals: 2,
        qty_step_units: 1,
        qty_decimals: 3,
        grid_ms: 100,
        block_duration_ms: 60_000,
        depth_requested: 20,
        depth_available: 20,
        session_epoch: 3,
    };
    let mut writer = BookWriter::new(&dir, "venue", "ETHUSDT", meta, 60_000, 3).unwrap();
    let block = BlockInput {
        header: BlockHeader::data(7, 1_000_000_000, 2_000_000_000, 1).unwrap(),
        payload: vec![4, 5, 6],
    };
    writer.append(&block).unwrap();
    writer.finish(2_000_000_000, 3_000_000_000).unwrap();
    let paths = std::fs::read_dir(
        dir.join("book/venue=venue/instrument=ETHUSDT/symbol=ETHUSDT/date=1970-01-01"),
    )
    .unwrap()
    .map(|entry| entry.unwrap().path())
    .collect::<Vec<_>>();
    let segment = paths
        .into_iter()
        .find(|p| p.extension().is_some_and(|x| x == "tnmd"))
        .unwrap();
    let reader = SegmentReader::open(&segment).unwrap();
    assert_eq!(reader.block_count(), 1);
    assert_eq!(reader.read_block(0).unwrap().payload, block.payload);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn book_writer_rotates_on_utc_segment_boundaries() {
    let dir = test_dir();
    let meta = MarketMeta {
        market_id: 7,
        venue_id: 2,
        symbol: "ETHUSDT".into(),
        feed_mode: FeedMode::Diff,
        tick_units: 1,
        price_decimals: 2,
        qty_step_units: 1,
        qty_decimals: 3,
        grid_ms: 100,
        block_duration_ms: 60_000,
        depth_requested: 20,
        depth_available: 20,
        session_epoch: 3,
    };
    let mut writer = BookWriter::new(&dir, "venue", "ETHUSDT", meta, 60_000, 3).unwrap();
    for (start, end) in [
        (30_000_000_000i64, 31_000_000_000i64),
        (59_000_000_000, 59_500_000_000),
        (60_000_000_000, 61_000_000_000),
    ] {
        let block = BlockInput {
            header: BlockHeader::data(7, start, end, 1).unwrap(),
            payload: vec![1, 2, 3],
        };
        writer.append(&block).unwrap();
    }
    writer.finish(61_000_000_000, 61_000_000_000).unwrap();
    let mut segments = fs::read_dir(
        dir.join("book/venue=venue/instrument=ETHUSDT/symbol=ETHUSDT/date=1970-01-01"),
    )
    .unwrap()
    .map(|entry| entry.unwrap().path())
    .filter(|path| path.extension().is_some_and(|ext| ext == "tnmd"))
    .collect::<Vec<_>>();
    segments.sort();
    assert_eq!(segments.len(), 2);
    assert_eq!(SegmentReader::open(&segments[0]).unwrap().block_count(), 2);
    assert_eq!(SegmentReader::open(&segments[1]).unwrap().block_count(), 1);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn book_writer_places_gap_at_the_head_of_a_new_epoch() {
    let dir = test_dir();
    let meta = MarketMeta {
        market_id: 7,
        venue_id: 2,
        symbol: "ETHUSDT".into(),
        feed_mode: FeedMode::Diff,
        tick_units: 1,
        price_decimals: 2,
        qty_step_units: 1,
        qty_decimals: 3,
        grid_ms: 100,
        block_duration_ms: 60_000,
        depth_requested: 20,
        depth_available: 20,
        session_epoch: 3,
    };
    let mut writer = BookWriter::new(&dir, "venue", "ETHUSDT", meta, 60_000, 3).unwrap();
    writer
        .append(&BlockInput {
            header: BlockHeader::data(7, 1_000_000_000, 2_000_000_000, 1).unwrap(),
            payload: vec![1],
        })
        .unwrap();
    writer
        .append_gap(
            3_000_000_000,
            4_000_000_000,
            5_000_000_000,
            6_000_000_000,
            GapReason::SequenceBreak,
            10,
            12,
            b"sequence discontinuity",
        )
        .unwrap();
    writer.finish(5_000_000_000, 6_000_000_000).unwrap();
    let directory = dir.join("book/venue=venue/instrument=ETHUSDT/symbol=ETHUSDT/date=1970-01-01");
    let segments = std::fs::read_dir(&directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|x| x == "tnmd"))
        .collect::<Vec<_>>();
    assert_eq!(segments.len(), 2);
    let gap_segment = segments
        .iter()
        .find(|path| path.to_string_lossy().contains("-e4"))
        .unwrap();
    let reader = SegmentReader::open(gap_segment).unwrap();
    let gap = reader.read_block(0).unwrap();
    assert_eq!(gap.header.kind, collector::book::BlockKind::Gap);
    assert_eq!(gap.header.control_reason, GapReason::SequenceBreak as u16);
    assert_eq!(gap.payload, b"sequence discontinuity");
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn run_market_records_normalized_diff_slots_and_replays_them() {
    let dir = test_dir();
    let meta = MarketMeta {
        market_id: 3,
        venue_id: 1,
        symbol: "BTCUSDT".into(),
        feed_mode: FeedMode::Diff,
        tick_units: 1,
        price_decimals: 0,
        qty_step_units: 1,
        qty_decimals: 0,
        grid_ms: 100,
        block_duration_ms: 1_000,
        depth_requested: 2,
        depth_available: 2,
        session_epoch: 0,
    };
    let messages = VecDeque::from([
        FeedMessage::Snapshot(SnapshotMessage {
            exch_ts_ns: 10_000_000,
            local_ts_ns: 10_001_000,
            bids: vec![Level { tick: 100, qty: 2 }],
            asks: vec![Level { tick: 101, qty: 3 }],
            last_seq: Some(10),
        }),
        FeedMessage::Trade(TradeRecord {
            exch_ts_ns: 15_000_000,
            local_ts_ns: 15_001_000,
            side: TradeSide::Buy,
            price: "100".into(),
            qty: "0.5".into(),
            trade_id: Some("trade-1".into()),
        }),
        FeedMessage::Depth(DepthMessage {
            exch_ts_ns: 20_000_000,
            local_ts_ns: 20_001_000,
            first_prev_seq: Some(10),
            first_seq: Some(11),
            last_seq: 11,
            changes: vec![Change {
                side: collector::book::Side::Bid,
                tick: 100,
                qty: 4,
            }],
        }),
        FeedMessage::Depth(DepthMessage {
            exch_ts_ns: 150_000_000,
            local_ts_ns: 150_001_000,
            first_prev_seq: Some(11),
            first_seq: Some(12),
            last_seq: 12,
            changes: vec![Change {
                side: collector::book::Side::Bid,
                tick: 99,
                qty: 2,
            }],
        }),
        FeedMessage::Depth(DepthMessage {
            exch_ts_ns: 250_000_000,
            local_ts_ns: 250_001_000,
            first_prev_seq: Some(12),
            first_seq: Some(13),
            last_seq: 13,
            changes: vec![Change {
                side: collector::book::Side::Ask,
                tick: 102,
                qty: 3,
            }],
        }),
    ]);
    let settings = collector::book::RecorderSettings {
        root: dir.clone(),
        grid_ms: 100,
        depth_levels: 2,
        block_duration_ms: 1_000,
        segment_duration_ms: 60_000,
        compression_level: 3,
        compression: "none".into(),
        fsync_interval_blocks: 1,
        oi_poll_ms: 60_000,
        validation_interval_ms: 60_000,
        rest_validation_log_messages: 2_048,
        rest_validation_min_samples: 5,
        daily_archive: false,
        daily_archive_prune: false,
        daily_archive_grace_secs: 120,
        daily_archive_group_json: false,
    };
    run_market(
        meta,
        &dir,
        "binance",
        "spot",
        &settings,
        FixtureFeed(messages),
    )
    .await
    .unwrap();
    let directory = dir.join("book/venue=binance/instrument=spot/symbol=BTCUSDT/date=1970-01-01");
    let segment = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|x| x == "tnmd"))
        .unwrap();
    let reader = SegmentReader::open(&segment).unwrap();
    assert_eq!(reader.header.encoding_flags, 1);
    // All changed slots inside one 1 s block must be packed into one block.
    assert_eq!(reader.block_count(), 1);
    let block = reader.read_block(0).unwrap();
    assert_eq!(block.header.n_slots, 3);
    let payload = decode_data_payload(&block).unwrap();
    assert_eq!(payload.bids, vec![Level { tick: 100, qty: 2 }]);
    assert_eq!(payload.asks, vec![Level { tick: 101, qty: 3 }]);
    assert_eq!(payload.records.len(), 3);
    assert_eq!(
        payload.records[0].changes,
        vec![Change {
            side: collector::book::Side::Bid,
            tick: 100,
            qty: 4,
        }]
    );
    assert_eq!(
        payload.records[1].changes,
        vec![Change {
            side: collector::book::Side::Bid,
            tick: 99,
            qty: 2,
        }]
    );
    assert_eq!(
        payload.records[2].changes,
        vec![Change {
            side: collector::book::Side::Ask,
            tick: 102,
            qty: 3,
        }]
    );
    let mut ladder = Ladder::from_snapshot(&payload.bids, &payload.asks).unwrap();
    for record in &payload.records {
        ladder.apply(&record.changes).unwrap();
    }
    assert_eq!(
        ladder.top_n(collector::book::Side::Bid, 10),
        vec![Level { tick: 100, qty: 4 }, Level { tick: 99, qty: 2 },]
    );
    assert_eq!(
        ladder.top_n(collector::book::Side::Ask, 10),
        vec![Level { tick: 101, qty: 3 }, Level { tick: 102, qty: 3 },]
    );
    let event_report = collector::file::inspect_market_events(&dir, None).unwrap();
    assert_eq!(event_report.files, 1);
    assert_eq!(event_report.records, 1);
    assert!(!dir.join("aux").exists());
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn run_market_starts_a_new_epoch_with_gap_before_resynced_data() {
    let dir = test_dir();
    let meta = MarketMeta {
        market_id: 9,
        venue_id: 1,
        symbol: "SOLUSDT".into(),
        feed_mode: FeedMode::Diff,
        tick_units: 1,
        price_decimals: 0,
        qty_step_units: 1,
        qty_decimals: 0,
        grid_ms: 100,
        block_duration_ms: 1_000,
        depth_requested: 2,
        depth_available: 2,
        session_epoch: 0,
    };
    let messages = VecDeque::from([
        FeedMessage::Snapshot(SnapshotMessage {
            exch_ts_ns: 10_000_000,
            local_ts_ns: 10_001_000,
            bids: vec![Level { tick: 100, qty: 1 }],
            asks: vec![Level { tick: 101, qty: 1 }],
            last_seq: Some(10),
        }),
        FeedMessage::Depth(DepthMessage {
            exch_ts_ns: 20_000_000,
            local_ts_ns: 20_001_000,
            first_prev_seq: Some(10),
            first_seq: Some(11),
            last_seq: 11,
            changes: vec![Change {
                side: collector::book::Side::Bid,
                tick: 100,
                qty: 2,
            }],
        }),
        FeedMessage::Depth(DepthMessage {
            exch_ts_ns: 110_000_000,
            local_ts_ns: 110_001_000,
            first_prev_seq: Some(11),
            first_seq: Some(12),
            last_seq: 12,
            changes: vec![],
        }),
        FeedMessage::Gap(GapMessage {
            start_local_ns: 120_000_000,
            start_exch_ns: 120_000_000,
            end_local_ns: 200_000_000,
            end_exch_ns: 200_000_000,
            reason: GapReason::Disconnect,
            last_seq: 12,
            next_seq: 100,
            detail: "websocket reconnect".into(),
        }),
        FeedMessage::Snapshot(SnapshotMessage {
            exch_ts_ns: 210_000_000,
            local_ts_ns: 210_001_000,
            bids: vec![Level { tick: 99, qty: 3 }],
            asks: vec![Level { tick: 102, qty: 2 }],
            last_seq: Some(100),
        }),
        FeedMessage::Depth(DepthMessage {
            exch_ts_ns: 220_000_000,
            local_ts_ns: 220_001_000,
            first_prev_seq: Some(100),
            first_seq: Some(101),
            last_seq: 101,
            changes: vec![Change {
                side: collector::book::Side::Bid,
                tick: 99,
                qty: 4,
            }],
        }),
        FeedMessage::Depth(DepthMessage {
            exch_ts_ns: 310_000_000,
            local_ts_ns: 310_001_000,
            first_prev_seq: Some(101),
            first_seq: Some(102),
            last_seq: 102,
            changes: vec![],
        }),
    ]);
    let settings = collector::book::RecorderSettings {
        root: dir.clone(),
        grid_ms: 100,
        depth_levels: 2,
        block_duration_ms: 1_000,
        segment_duration_ms: 60_000,
        compression_level: 3,
        compression: "zstd".into(),
        fsync_interval_blocks: 1,
        oi_poll_ms: 60_000,
        validation_interval_ms: 60_000,
        rest_validation_log_messages: 2_048,
        rest_validation_min_samples: 5,
        daily_archive: false,
        daily_archive_prune: false,
        daily_archive_grace_secs: 120,
        daily_archive_group_json: false,
    };
    run_market(
        meta,
        &dir,
        "binance",
        "spot",
        &settings,
        FixtureFeed(messages),
    )
    .await
    .unwrap();
    let directory = dir.join("book/venue=binance/instrument=spot/symbol=SOLUSDT/date=1970-01-01");
    let segment = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.extension().is_some_and(|x| x == "tnmd") && path.to_string_lossy().contains("-e1")
        })
        .unwrap();
    let reader = SegmentReader::open(&segment).unwrap();
    assert_eq!(reader.header.session_epoch, 1);
    assert_eq!(reader.block_count(), 2);
    assert_eq!(
        reader.read_block(0).unwrap().header.kind,
        collector::book::BlockKind::Gap
    );
    assert_eq!(
        reader.read_block(1).unwrap().header.kind,
        collector::book::BlockKind::Data
    );
    fs::remove_dir_all(dir).unwrap();
}
