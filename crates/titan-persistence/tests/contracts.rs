use serde_json::json;
use std::{
    fs,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use titan_persistence::*;

fn fact(run: &str, sequence: u64) -> FactEnvelope {
    let mut scope = FactScope::account(FactEnvironment::Backtest, "account-a");
    scope.run_id = Some(run.into());
    scope.strategy_id = Some("strategy-a".into());
    FactEnvelope::new(
        scope,
        run,
        "venue-stream",
        sequence,
        "fill",
        -100,
        json!({"price_ticks":"11","quantity_lots":"2","fee_currency":"USDT","fee_units":"-3"}),
    )
}
fn request(run: &str) -> HistoryRequest {
    HistoryRequest {
        filter: HistoryFilter {
            environment: FactEnvironment::Backtest,
            account: "account-a".into(),
            strategy_id: None,
            run_id: Some(run.into()),
            kind: None,
        },
        cursor: None,
        limit: 2,
        newest_first: false,
    }
}

#[test]
fn stable_cursor_excludes_new_rows_and_rejects_changed_scope_or_store() {
    let dir = tempfile::tempdir().unwrap();
    let archive = SqliteFactArchive::open(dir.path().join("history.db")).unwrap();
    archive
        .append(&(1..=4).map(|i| fact("run-a", i)).collect::<Vec<_>>())
        .unwrap();
    let mut req = request("run-a");
    let first = archive.query().page(&req).unwrap();
    assert_eq!(first.items.len(), 2);
    archive.append(&[fact("run-a", 5)]).unwrap();
    req.cursor = first.next_cursor;
    let second = archive.query().page(&req).unwrap();
    assert_eq!(second.items.len(), 2);
    assert!(second.next_cursor.is_none());
    assert_eq!(second.upper_row, "4");
    req.filter.account = "other".into();
    assert!(archive.query().page(&req).is_err());
    req.filter.account = "account-a".into();
    let other = SqliteFactArchive::open(dir.path().join("other.db")).unwrap();
    assert!(other.query().page(&req).is_err());
}
#[test]
fn recording_dedup_business_dedup_conflict_and_run_isolation_are_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let archive = SqliteFactArchive::open(dir.path().join("h.db")).unwrap();
    let mut one = fact("one", 1);
    one.business_key = Some("venue/account/instrument/trade-7".into());
    one.business_sha256 = Some(sha256(b"native immutable facts"));
    assert_eq!(archive.append(&[one.clone()]).unwrap().inserted, 1);
    assert_eq!(archive.append(&[one.clone()]).unwrap().replayed, 1);
    let mut replay = one.clone();
    replay.recording_id = "reconnect".into();
    assert_eq!(archive.append(&[replay.clone()]).unwrap().replayed, 1);
    replay.source_sequence = 2;
    replay.business_sha256 = Some(sha256(b"contradiction"));
    assert_eq!(archive.append(&[replay]).unwrap().conflicts, 1);
    let mut two = one.clone();
    two.recording_id = "two".into();
    two.scope.run_id = Some("two".into());
    assert_eq!(archive.append(&[two]).unwrap().replayed, 0);
    let mut changed = one;
    changed.payload["quantity_lots"] = json!("99");
    assert_eq!(archive.append(&[changed]).unwrap().conflicts, 1);
    let page = archive.query().page(&request("one")).unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].fact.payload["quantity_lots"], "2");
    assert_ne!(page.recording["gap_count"], "0");
}
#[test]
fn transaction_failure_never_advances_durable_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("h.db");
    let archive = SqliteFactArchive::open(&path).unwrap();
    let db = rusqlite::Connection::open(path).unwrap();
    db.execute_batch("CREATE TRIGGER fail_watermark BEFORE INSERT ON archive_watermarks BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert!(archive.append(&[fact("run", 1)]).is_err());
    assert_eq!(
        db.query_row("SELECT count(*) FROM archive_records", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        db.query_row("SELECT count(*) FROM archive_business", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    db.execute_batch("DROP TRIGGER fail_watermark").unwrap();
    archive.append(&[fact("run", 1)]).unwrap();
    assert_eq!(
        archive.query().page(&request("run")).unwrap().items.len(),
        1
    );
}
#[test]
fn source_gaps_and_unfinished_recording_remain_visible() {
    let dir = tempfile::tempdir().unwrap();
    let archive = SqliteFactArchive::open(dir.path().join("h.db")).unwrap();
    archive.append(&[fact("run", 1), fact("run", 3)]).unwrap();
    archive.finish("run", true).unwrap();
    let p = archive.query().page(&request("run")).unwrap();
    assert_eq!(p.recording["gap_count"], "1");
    assert_eq!(p.recording["received_segment_complete"], false);
    archive.append(&[fact("unfinished", 1)]).unwrap();
    assert_eq!(
        archive
            .query()
            .page(&request("unfinished"))
            .unwrap()
            .recording["unclosed_sources"],
        "1"
    );
}
#[test]
fn sampled_publications_can_skip_but_repeated_reads_do_not_create_records() {
    let dir = tempfile::tempdir().unwrap();
    let archive = SqliteFactArchive::open(dir.path().join("h.db")).unwrap();
    let mut a = fact("run", 3);
    a.sampled = true;
    a.kind = "business_observation".into();
    let mut b = a.clone();
    b.source_sequence = 9;
    archive.append(&[a.clone(), a, b.clone(), b]).unwrap();
    archive.finish("run", true).unwrap();
    let p = archive.query().page(&request("run")).unwrap();
    assert_eq!(p.items.len(), 2);
    assert_eq!(p.recording["gap_count"], "0");
}
#[test]
fn full_stream_applies_backpressure_and_failure_is_sticky() {
    struct Slow {
        calls: Arc<AtomicUsize>,
    }
    impl FactArchive for Slow {
        fn append(&self, _: &[FactEnvelope]) -> Result<AppendReceipt> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(5));
            if n == 2 {
                return Err(failure("injected_full"));
            }
            Ok(AppendReceipt::default())
        }
        fn finish(&self, _: &str, _: bool) -> Result<()> {
            panic!("failed stream cannot seal")
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let mut stream = FactStream::new(
        Box::new(Slow {
            calls: calls.clone(),
        }),
        "run".into(),
        2,
    )
    .unwrap();
    let start = std::time::Instant::now();
    for n in 1..=6 {
        stream.record(fact("run", n)).unwrap();
        assert!(stream.buffered_len() <= 2);
    }
    assert!(stream.record(fact("run", 7)).is_err());
    assert!(start.elapsed() >= std::time::Duration::from_millis(15));
    assert!(stream.finish(true).is_err());
    assert!(stream.record(fact("run", 8)).is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}
#[test]
fn parquet_preserves_u64_boundaries_negative_time_and_original_json() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("facts.parquet");
    let expected = [fact("run", u64::MAX), fact("run", 0)];
    let mut writer = columnar::FactParquetWriter::create(&path).unwrap();
    writer.write_chunk(&expected).unwrap();
    assert!(!path.exists());
    writer.finish().unwrap();
    let mut got = Vec::new();
    assert_eq!(
        columnar::scan_facts(&path, |r| {
            got.push(r);
            Ok(())
        })
        .unwrap(),
        2
    );
    assert_eq!(got, expected);
    assert!(columnar::FactParquetWriter::create(&path).is_err());
    assert_eq!(
        serde_json::to_value(&got[0]).unwrap()["source_sequence"],
        u64::MAX.to_string()
    );
}
#[test]
fn parquet_market_preserves_sub_tick_price_signed_zero_and_unsigned_ids() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("market.parquet");
    let original = titan_domain_types::Event {
        ev: u64::MAX,
        exch_ts: -2,
        local_ts: -1,
        px: 26000.019999999997,
        qty: 0.000000001,
        order_id: u64::MAX,
        ival: i64::MIN,
        fval: -0.0,
    };
    columnar::write_market(&path, std::slice::from_ref(&original)).unwrap();
    columnar::scan_market(&path, |got| {
        assert_eq!(got, original);
        assert_eq!(got.fval.to_bits(), original.fval.to_bits());
        Ok(())
    })
    .unwrap();
}
#[test]
fn large_full_run_is_bounded_and_sqlite_parquet_have_identical_facts() {
    let dir = tempfile::tempdir().unwrap();
    let scope = fact("run", 1).scope;
    let mut run =
        RunRecorder::open(dir.path(), scope, "run".into(), json!({"model":"test"})).unwrap();
    for n in 0..1000 {
        run.record("order", n, json!({"order_id":n.to_string()}))
            .unwrap();
        assert!(run.buffered_len() <= 64);
    }
    run.finish(
        1000,
        json!({"engine_status":"FAILED","error":"explicit engine failure","residual_position":"1"}),
    )
    .unwrap();
    let mut count = 0;
    columnar::scan_facts(&dir.path().join("facts.parquet"), |_| {
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 1002);
    let db = rusqlite::Connection::open(dir.path().join("history.db")).unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM archive_records", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        count
    );
    assert!(
        db.query_row("SELECT clean FROM archive_sources", [], |r| r
            .get::<_, bool>(0))
            .unwrap()
    );
}
fn legacy_bundle(dir: &Path) -> Vec<u8> {
    fs::write(
        dir.join("result.json"),
        b"{\"returns\":[],\"status\":\"FAILED\"}",
    )
    .unwrap();
    let bytes = fs::read(dir.join("result.json")).unwrap();
    serde_json::to_vec(&json!({"schema_version":1,"run_id":"run","strategy_id":"strategy","files":[{"path":"result.json","bytes":bytes.len(),"sha256":sha256(&bytes)}]})).unwrap()
}
#[test]
fn original_bundle_bytes_publish_last_and_registration_retries_without_engine_work() {
    let dir = tempfile::tempdir().unwrap();
    let storage = tempfile::tempdir().unwrap();
    let bytes = legacy_bundle(dir.path());
    let r = commit_result_bundle(dir.path(), storage.path(), &bytes).unwrap();
    assert_eq!(fs::read(dir.path().join("manifest.json")).unwrap(), bytes);
    assert_eq!(
        commit_result_bundle(dir.path(), storage.path(), &bytes).unwrap(),
        r
    );
    let store = LocalObjectStore::new(storage.path().join("objects")).unwrap();
    store.open_verified(&r).unwrap();
    let mut changed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    changed["run_id"] = json!("other");
    changed["files"][0]["sha256"] = json!("0".repeat(64));
    assert!(
        commit_result_bundle(
            dir.path(),
            storage.path(),
            &serde_json::to_vec(&changed).unwrap()
        )
        .is_err()
    );
}
#[test]
fn incomplete_tampered_and_escaping_bundle_never_publishes_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let storage = tempfile::tempdir().unwrap();
    let bytes = legacy_bundle(dir.path());
    fs::write(dir.path().join("result.json"), b"changed").unwrap();
    assert!(commit_result_bundle(dir.path(), storage.path(), &bytes).is_err());
    assert!(!dir.path().join("manifest.json").exists());
    let manifest = PublishedManifest {
        schema_version: 1,
        logical_id: "x".into(),
        kind: "bundle".into(),
        files: vec![PublishedFile {
            path: "../escape".into(),
            object: ObjectRef {
                sha256: "0".repeat(64),
                bytes: 0,
            },
        }],
        metadata: json!({}),
    };
    assert!(manifest.validate().is_err());
}
#[test]
fn object_digest_checks_idempotence_no_overwrite_and_isolated_materialization() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(dir.path().join("objects")).unwrap();
    let one = store.put_bytes(b"original").unwrap();
    assert_eq!(store.put_bytes(b"original").unwrap(), one);
    let output = dir.path().join("restored.json");
    store.materialize(&one, &output).unwrap();
    assert!(store.materialize(&one, &output).is_err());
    fs::write(&output, b"local changed").unwrap();
    store.open_verified(&one).unwrap();
    let mut wrong = one;
    wrong.sha256 = "../escape".into();
    assert!(store.open_verified(&wrong).is_err());
}
#[test]
fn crash_before_first_chunk_leaves_durable_source_evidence() {
    let dir = tempfile::tempdir().unwrap();
    {
        let scope = fact("run", 1).scope;
        let _unfinished =
            RunRecorder::open(dir.path(), scope, "run".into(), json!({"model":"test"})).unwrap();
    }
    let page = SqliteHistoryQuery::new(dir.path().join("history.db"))
        .page(&request("run"))
        .unwrap();
    assert!(page.items.is_empty());
    assert_eq!(page.recording["unclosed_sources"], "1");
    assert_eq!(page.recording["received_segment_complete"], false);
    assert!(!dir.path().join("facts.parquet").exists());
}
#[test]
fn catalog_failure_retains_original_proof_for_registration_without_rerunning_engine() {
    let dir = tempfile::tempdir().unwrap();
    let storage = tempfile::tempdir().unwrap();
    let bytes = legacy_bundle(dir.path());
    ResultCatalog::open(&storage.path().join("catalog.db")).unwrap();
    let db = rusqlite::Connection::open(storage.path().join("catalog.db")).unwrap();
    db.execute_batch("CREATE TRIGGER fail_registration BEFORE INSERT ON object_catalog BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert!(commit_result_bundle(dir.path(), storage.path(), &bytes).is_err());
    assert!(!dir.path().join("manifest.json").exists());
    assert_eq!(
        fs::read(dir.path().join("manifest.pending.json")).unwrap(),
        bytes
    );
    db.execute_batch("DROP TRIGGER fail_registration").unwrap();
    let proof = fs::read(dir.path().join("manifest.pending.json")).unwrap();
    commit_result_bundle(dir.path(), storage.path(), &proof).unwrap();
    assert_eq!(fs::read(dir.path().join("manifest.json")).unwrap(), bytes);
}
#[test]
fn owner_lease_prevents_competing_recording_and_sealed_recording_cannot_append() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history.db");
    let a = SqliteFactArchive::open(&path).unwrap();
    let b = SqliteFactArchive::open(&path).unwrap();
    let scope = fact("run", 1).scope;
    a.begin_recording("run", &scope).unwrap();
    assert!(b.begin_recording("run", &scope).is_err());
    a.append(&[fact("run", 1)]).unwrap();
    a.finish("run", true).unwrap();
    assert!(a.append(&[fact("run", 2)]).is_err());
    assert_eq!(a.append(&[fact("run", 1)]).unwrap().replayed, 1);
}
#[test]
fn parquet_schema_name_alone_does_not_authorize_different_columns() {
    use parquet::{
        file::{properties::WriterProperties, writer::SerializedFileWriter},
        schema::parser::parse_message_type,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.parquet");
    SerializedFileWriter::new(
        fs::File::create(&path).unwrap(),
        Arc::new(
            parse_message_type("message titan_market_v1 { REQUIRED INT64 reordered; }").unwrap(),
        ),
        Arc::new(WriterProperties::default()),
    )
    .unwrap()
    .close()
    .unwrap();
    assert!(columnar::scan_market(&path, |_| Ok(())).is_err());
}

#[test]
fn future_archive_schema_is_rejected_by_both_read_and_write_without_migration() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history.db");
    let archive = SqliteFactArchive::open(&path).unwrap();
    archive.append(&[fact("run", 1)]).unwrap();
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("UPDATE archive_meta SET schema_version=2; DROP TABLE archive_conflicts;")
        .unwrap();
    assert!(archive.query().page(&request("run")).is_err());
    assert!(SqliteFactArchive::open(&path).is_err());
    assert!(
        !db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='archive_conflicts')",
            [],
            |r| r.get::<_, bool>(0)
        )
        .unwrap()
    );
}

#[test]
fn catalog_history_and_account_database_roles_cannot_be_mixed() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = dir.path().join("catalog.db");
    ResultCatalog::open(&catalog).unwrap();
    assert!(SqliteFactArchive::open(&catalog).is_err());
    let db = rusqlite::Connection::open(&catalog).unwrap();
    assert!(
        !db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='archive_meta')",
            [],
            |r| r.get::<_, bool>(0)
        )
        .unwrap()
    );
    let old_history = dir.path().join("old-history.db");
    rusqlite::Connection::open(&old_history)
        .unwrap()
        .execute_batch("CREATE TABLE facts(id INTEGER PRIMARY KEY);")
        .unwrap();
    assert!(ResultCatalog::open(&old_history).is_err());
}

#[test]
fn complete_recording_does_not_claim_sampled_equity_is_continuous() {
    let dir = tempfile::tempdir().unwrap();
    let mut run = RunRecorder::open(
        dir.path(),
        fact("run", 1).scope,
        "run".into(),
        json!({"audit":"full"}),
    )
    .unwrap();
    run.record(
        "equity_sample",
        60,
        json!({"currency":"USDT","equity":"100.000001"}),
    )
    .unwrap();
    run.record("fill", 61, json!({"quantity_lots":"1"}))
        .unwrap();
    run.finish(62, json!({"outcome":"completed"})).unwrap();
    let query = SqliteHistoryQuery::new(dir.path().join("history.db"));
    let mut selection = request("run");
    selection.limit = 20;
    let page = query.page(&selection).unwrap();
    assert_eq!(page.recording["received_segment_complete"], true);
    assert!(
        page.items
            .iter()
            .find(|i| i.fact.kind == "equity_sample")
            .unwrap()
            .fact
            .sampled
    );
    assert!(
        !page
            .items
            .iter()
            .find(|i| i.fact.kind == "fill")
            .unwrap()
            .fact
            .sampled
    );
}

#[test]
fn latest_samples_refresh_without_losing_fixed_bounds_or_direction_identity() {
    let dir = tempfile::tempdir().unwrap();
    let archive = SqliteFactArchive::open(dir.path().join("history.db")).unwrap();
    archive
        .append(&(1..=25).map(|n| fact("run", n)).collect::<Vec<_>>())
        .unwrap();
    let mut req = request("run");
    req.newest_first = true;
    let first = archive.query().page(&req).unwrap();
    assert_eq!(
        first
            .items
            .iter()
            .map(|i| i.fact.source_sequence)
            .collect::<Vec<_>>(),
        [25, 24]
    );
    archive.append(&[fact("run", 26)]).unwrap();
    req.cursor = first.next_cursor;
    let next = archive.query().page(&req).unwrap();
    assert_eq!(
        next.items
            .iter()
            .map(|i| i.fact.source_sequence)
            .collect::<Vec<_>>(),
        [23, 22]
    );
    assert_eq!(next.upper_row, "25");
    req.newest_first = false;
    assert!(archive.query().page(&req).is_err());
    req.newest_first = true;
    req.cursor = None;
    assert_eq!(
        archive.query().page(&req).unwrap().items[0]
            .fact
            .source_sequence,
        26
    );
}

#[test]
fn interrupted_staging_cleanup_preserves_owned_sessions_and_content() {
    let dir = tempfile::tempdir().unwrap();
    let stage = dir.path().join(".s3-stage");
    fs::create_dir(&stage).unwrap();
    let abandoned = stage.join(format!("session-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&abandoned).unwrap();
    fs::write(abandoned.join(".tmp-incomplete"), b"partial").unwrap();
    let owned = stage.join(format!("session-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&owned).unwrap();
    let lease = fs::File::create(owned.join(".owner.lock")).unwrap();
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        assert_eq!(
            unsafe { libc::flock(lease.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
    }
    fs::write(owned.join(".tmp-active"), b"active").unwrap();
    assert_eq!(cleanup_s3_staging(dir.path()).unwrap(), 1);
    assert!(owned.join(".tmp-active").is_file());
    assert!(!abandoned.exists());
    drop(lease);
    assert_eq!(cleanup_s3_staging(dir.path()).unwrap(), 1);
}

#[test]
fn control_catalogue_cannot_be_reopened_as_history_or_object_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("storage");
    StorageControl::initialize(
        &root,
        StorageProfile {
            schema_version: 1,
            postgres: None,
            s3: None,
            budget: Default::default(),
        },
    )
    .unwrap();
    assert!(SqliteFactArchive::open(root.join("control.db")).is_err());
    assert!(ResultCatalog::open(&root.join("control.db")).is_err());
    let db = rusqlite::Connection::open(root.join("control.db")).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM sqlite_master WHERE name IN ('archive_meta','object_catalog')",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
}

#[test]
fn initial_background_registration_does_not_invalidate_local_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("storage");
    let path = dir.path().join("history.db");
    let control = StorageControl::initialize(
        &root,
        StorageProfile {
            schema_version: 1,
            postgres: None,
            s3: None,
            budget: Default::default(),
        },
    )
    .unwrap();
    let archive = SqliteFactArchive::open(&path).unwrap();
    archive
        .append(&(1..=4).map(|n| fact("run", n)).collect::<Vec<_>>())
        .unwrap();
    let q = RoutedHistoryQuery::new(root, path.clone());
    let mut req = request("run");
    let first = q.page(&req).unwrap();
    control.register_source(&path).unwrap();
    req.cursor = first.next_cursor;
    assert_eq!(q.page(&req).unwrap().items.len(), 2);
}

#[test]
fn missing_control_database_cannot_fall_back_or_silently_reinitialize() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("storage");
    let path = dir.path().join("history.db");
    let profile = StorageProfile {
        schema_version: 1,
        postgres: None,
        s3: None,
        budget: Default::default(),
    };
    StorageControl::initialize(&root, profile.clone()).unwrap();
    let archive = SqliteFactArchive::open(&path).unwrap();
    archive.append(&[fact("run", 1)]).unwrap();
    fs::rename(root.join("control.db"), root.join("control.saved.db")).unwrap();
    assert!(StorageControl::exists(&root));
    assert!(object_store(&root).is_err());
    assert!(
        RoutedHistoryQuery::new(root.clone(), path.clone())
            .page(&request("run"))
            .is_err()
    );
    assert!(ProjectionWorker::start(root.clone(), None).is_err());
    assert!(StorageControl::initialize(&root, profile).is_err());
    assert!(archive.append(&[fact("run", 2)]).is_err());
    assert!(!root.join("control.db").exists());
}

#[test]
fn a_registered_source_path_cannot_be_replaced_with_another_store_identity() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("storage");
    let path = dir.path().join("history.db");
    let control = StorageControl::initialize(
        &root,
        StorageProfile {
            schema_version: 1,
            postgres: None,
            s3: None,
            budget: Default::default(),
        },
    )
    .unwrap();
    let a = SqliteFactArchive::open(&path).unwrap();
    a.append(&[fact("run", 1)]).unwrap();
    drop(a);
    control.register_source(&path).unwrap();
    let other = dir.path().join("other.db");
    let b = SqliteFactArchive::open(&other).unwrap();
    b.append(&[fact("run", 1)]).unwrap();
    drop(b);
    fs::rename(&path, dir.path().join("old.db")).unwrap();
    fs::rename(other, &path).unwrap();
    assert!(control.register_source(&path).is_err());
    assert!(
        RoutedHistoryQuery::new(root, path)
            .page(&request("run"))
            .is_err()
    );
}

#[test]
fn concurrent_cold_registration_and_publication_do_not_lose_committed_catalog_entries() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("storage");
    let control = StorageControl::initialize(
        &root,
        StorageProfile {
            schema_version: 1,
            postgres: None,
            s3: None,
            budget: Default::default(),
        },
    )
    .unwrap();
    let path = dir.path().join("history.db");
    let archive = SqliteFactArchive::open(&path).unwrap();
    archive.append(&[fact("run", 1)]).unwrap();
    drop(archive);
    drop(control);
    let handles = (0..3)
        .map(|lane| {
            let root = root.clone();
            let path = path.clone();
            std::thread::spawn(move || {
                for iteration in 0..30 {
                    let control = StorageControl::open(&root).unwrap();
                    control.register_source(&path).unwrap();
                    let store = object_store(&root).unwrap();
                    let object = store.put_bytes(b"original").unwrap();
                    let publication = PublishedManifest {
                        schema_version: 1,
                        logical_id: format!("parallel-{lane}-{iteration}"),
                        kind: "concurrent-test".into(),
                        files: vec![PublishedFile {
                            path: "result.json".into(),
                            object,
                        }],
                        metadata: json!({}),
                    };
                    let catalog = ResultCatalog::open(&root.join("catalog.db")).unwrap();
                    publication.publish(store.as_ref(), &catalog).unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(
        ResultCatalog::entries(&root.join("catalog.db"))
            .unwrap()
            .len(),
        90
    );
}
