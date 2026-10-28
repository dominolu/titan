//! Real drivers only. Run explicitly against the labelled loopback services on 192.168.3.88.
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use serde_json::json;
use std::{fs, io::Read};
use titan_persistence::*;
fn profile() -> StorageProfile {
    let p = std::env::var("TITAN_M3_PROFILE").expect("explicit loopback profile required");
    let profile: StorageProfile = serde_json::from_slice(&fs::read(p).unwrap()).unwrap();
    profile.validate().unwrap();
    assert!(profile.postgres.as_ref().unwrap().allow_insecure_loopback);
    assert!(
        profile
            .s3
            .as_ref()
            .unwrap()
            .endpoint
            .starts_with("http://127.0.0.1:")
    );
    profile
}
fn client(p: &StorageProfile) -> aws_sdk_s3::Client {
    let s = p.s3.as_ref().unwrap();
    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(s.region.clone()))
            .endpoint_url(&s.endpoint)
            .force_path_style(true)
            .credentials_provider(Credentials::new(
                std::env::var(&s.access_key_env).unwrap(),
                std::env::var(&s.secret_key_env).unwrap(),
                None,
                None,
                "isolated-test",
            ))
            .build(),
    )
}
fn fact(n: u64) -> FactEnvelope {
    let mut scope = FactScope::account(FactEnvironment::Backtest, "contract-account");
    scope.run_id = Some("contract".into());
    let mut f = FactEnvelope::new(
        scope,
        "contract",
        "venue",
        n,
        "fill",
        i64::MIN,
        json!({"u64":u64::MAX.to_string(),"negative":"-123.456789012345678901","missing":null,"price":1.0000000000000002}),
    );
    f.received_at_ns = i64::MAX;
    f
}
fn request() -> HistoryRequest {
    HistoryRequest {
        filter: HistoryFilter {
            environment: FactEnvironment::Backtest,
            account: "contract-account".into(),
            strategy_id: None,
            run_id: Some("contract".into()),
            kind: None,
        },
        cursor: None,
        limit: 2,
        newest_first: false,
    }
}
fn all(q: &dyn HistoryQuery) -> Vec<serde_json::Value> {
    let mut req = request();
    let mut result = Vec::new();
    loop {
        let page = q.page(&req).unwrap();
        result.extend(
            page.items
                .iter()
                .map(|r| serde_json::to_value(&r.fact).unwrap()),
        );
        if page.next_cursor.is_none() {
            break;
        }
        req.cursor = page.next_cursor;
    }
    result
}
fn pg(p: &StorageProfile) -> postgres::Client {
    std::env::var(&p.postgres.as_ref().unwrap().dsn_env)
        .unwrap()
        .parse::<postgres::Config>()
        .unwrap()
        .connect(postgres::NoTls)
        .unwrap()
}
fn record(cases: &mut Vec<String>, name: &str) {
    cases.push(name.into());
    println!("M3_CASE {name}: passed");
}
#[test]
#[ignore = "requires explicit isolated real PostgreSQL and S3 services on build server"]
fn real_driver_contract_and_bidirectional_cutover() {
    let output = std::path::PathBuf::from(
        std::env::var("TITAN_M3_OUTPUT").expect("fresh evidence directory required"),
    );
    fs::create_dir(&output).unwrap();
    let mut p = profile();
    p.s3.as_mut().unwrap().prefix = format!("contract-{}", uuid::Uuid::new_v4());
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let sdk = {
        let _entered = rt.enter();
        client(&p)
    };
    rt.block_on(async {
        let s = p.s3.as_ref().unwrap();
        let r = sdk.create_bucket().bucket(&s.bucket).send().await;
        if let Err(e) = r {
            println!(
                "create_bucket status={:?} code={:?} error={}",
                e.raw_response().map(|r| r.status().as_u16()),
                e.as_service_error().and_then(|e| e.meta().code()),
                e
            );
            assert!(
                e.raw_response()
                    .is_some_and(|r| matches!(r.status().as_u16(), 409))
            );
        }
    });
    let mut cases = Vec::new();
    let root = output.join("storage");
    let control = StorageControl::initialize(&root, p.clone()).unwrap();
    assert_eq!(
        control.control_id,
        StorageControl::initialize(&root, p.clone())
            .unwrap()
            .control_id
    );
    let archive_path = output.join("history.db");
    let archive = SqliteFactArchive::open(&archive_path).unwrap();
    archive
        .append(&(1..=7).map(fact).collect::<Vec<_>>())
        .unwrap();
    let mut replay = fact(8);
    replay.business_key = Some("trade-7".into());
    replay.business_sha256 = Some(sha256(b"native-economics"));
    archive.append(&[replay.clone()]).unwrap();
    replay.source_sequence = 9;
    archive.append(&[replay.clone()]).unwrap();
    replay.source_sequence = 10;
    replay.business_sha256 = Some(sha256(b"contradiction"));
    assert_eq!(archive.append(&[replay]).unwrap().conflicts, 1);
    let mut boundary = fact(u64::MAX);
    boundary.stream_id = "boundary".into();
    archive.append(&[boundary]).unwrap();
    archive.finish("contract", true).unwrap();
    let source = control.register_source(&archive_path).unwrap().source_id;
    let projection = PostgresProjection::open(p.postgres.clone().unwrap()).unwrap();
    let mut db = pg(&p);
    let fail_fn = format!("titan_fail_{}", uuid::Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE FUNCTION {fail_fn}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.source_id='{source}' THEN RAISE EXCEPTION 'injected checkpoint failure'; END IF; RETURN NEW; END $$;CREATE TRIGGER {fail_fn} BEFORE UPDATE ON titan_projection_sources FOR EACH ROW EXECUTE FUNCTION {fail_fn}();")).unwrap();
    assert!(projection.sync_source(&archive_path, 2).is_err());
    assert!(projection.progress(&source).unwrap().is_none());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM titan_fact_records WHERE source_id=$1",
            &[&source]
        )
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.batch_execute(&format!(
        "DROP TRIGGER {fail_fn} ON titan_projection_sources;DROP FUNCTION {fail_fn}();"
    ))
    .unwrap();
    record(&mut cases, "postgres_batch_and_checkpoint_atomic_rollback");
    let handles = (0..2)
        .map(|_| {
            let projection = projection.clone();
            let path = archive_path.clone();
            std::thread::spawn(move || {
                for _ in 0..10 {
                    projection.sync_source(&path, 2).unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    for h in handles {
        h.join().unwrap();
    }
    let sqlite = all(&archive.query());
    assert_eq!(sqlite, all(&projection.query(&source)));
    assert_eq!(
        projection.digest(&source, 10).unwrap(),
        ArchiveSnapshot::open(&archive_path)
            .unwrap()
            .digest(10)
            .unwrap()
    );
    record(&mut cases, "sqlite_postgres_numeric_dedup_gap_query_parity");
    let routed = RoutedHistoryQuery::new(root.clone(), archive_path.clone());
    let before = routed.page(&request()).unwrap().next_cursor.unwrap();
    let plan = control
        .prepare_history(&archive_path, HistoryBackend::Postgres)
        .unwrap();
    assert_eq!(
        control.history_route(&source).unwrap().backend,
        HistoryBackend::Sqlite
    );
    let mut late = fact(11);
    late.recording_id = "late-11".into();
    archive.append(&[late]).unwrap();
    archive.finish("late-11", true).unwrap();
    let committed = control.activate(&plan.id).unwrap();
    assert_eq!(committed.phase, "committed");
    assert_eq!(committed.id, control.activate(&plan.id).unwrap().id);
    let mut stale = request();
    stale.cursor = Some(before);
    assert!(routed.page(&stale).is_err());
    assert_eq!(all(&archive.query()), all(&routed));
    record(
        &mut cases,
        "history_resume_final_increment_and_stale_cursor",
    );
    let local_db = rusqlite::Connection::open(&archive_path).unwrap();
    mark_gap(
        &local_db,
        "contract",
        "contract-account",
        "metadata-only-gap",
        "injected without new facts",
    )
    .unwrap();
    let pending = routed.page(&request()).unwrap();
    assert_eq!(pending.recording["projection_metadata_pending"], true);
    assert_eq!(pending.recording["received_segment_complete"], false);
    control.sync_source(&archive_path).unwrap();
    assert_eq!(
        routed.page(&request()).unwrap().recording["projection_metadata_pending"],
        false
    );
    record(
        &mut cases,
        "metadata_only_gap_is_visible_before_next_projection",
    );

    let upper = ArchiveSnapshot::open(&archive_path)
        .unwrap()
        .metadata
        .upper_row;
    let pg_cursor = routed.page(&request()).unwrap().next_cursor.unwrap();
    control.prune_history(&archive_path, upper).unwrap();
    assert!(archive.query().page(&request()).is_err());
    assert_eq!(all(&projection.query(&source)), all(&routed));
    // Purged IDs must never be reused by subsequent appends, including after restart.
    let mut late = fact(12);
    late.recording_id = "late-12".into();
    archive.append(&[late]).unwrap();
    archive.finish("late-12", true).unwrap();
    assert!(
        ArchiveSnapshot::open(&archive_path)
            .unwrap()
            .metadata
            .upper_row
            > upper
    );
    let back = control
        .prepare_history(&archive_path, HistoryBackend::Sqlite)
        .unwrap();
    control.activate(&back.id).unwrap();
    assert_eq!(all(&archive.query()), all(&projection.query(&source)));
    stale.cursor = Some(pg_cursor);
    assert!(routed.page(&stale).is_err());
    record(&mut cases, "history_prune_increment_backfill_and_rollback");
    // A target that lost any acknowledged row cannot be activated.
    let missing = control
        .prepare_history(&archive_path, HistoryBackend::Postgres)
        .unwrap();
    let raw=db.query_one("DELETE FROM titan_fact_records WHERE source_id=$1 AND row_id=1 RETURNING recording,stream,epoch,sequence,environment,account,strategy,run,kind,payload,sha256,replay",&[&source]).unwrap();
    assert!(control.activate(&missing.id).is_err());
    assert_eq!(
        control.history_route(&source).unwrap().backend,
        HistoryBackend::Sqlite
    );
    db.execute(
        "INSERT INTO titan_fact_records VALUES($1,1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
        &[
            &source,
            &raw.get::<_, String>(0),
            &raw.get::<_, String>(1),
            &raw.get::<_, String>(2),
            &raw.get::<_, Vec<u8>>(3),
            &raw.get::<_, String>(4),
            &raw.get::<_, String>(5),
            &raw.get::<_, Option<String>>(6),
            &raw.get::<_, Option<String>>(7),
            &raw.get::<_, String>(8),
            &raw.get::<_, Vec<u8>>(9),
            &raw.get::<_, String>(10),
            &raw.get::<_, bool>(11),
        ],
    )
    .unwrap();
    record(&mut cases, "history_missing_range_refuses_activation");
    let frozen_dir = output.join("snapshot-bundle");
    fs::create_dir(&frozen_dir).unwrap();
    let frozen_path = frozen_dir.join("history.db");
    let frozen = SqliteFactArchive::open(&frozen_path).unwrap();
    let mut rows = (1..=4).map(fact).collect::<Vec<_>>();
    for row in &mut rows {
        row.recording_id = "snapshot-contract".into();
        row.scope.run_id = Some("snapshot-contract".into());
    }
    frozen.append(&rows).unwrap();
    frozen.finish("snapshot-contract", true).unwrap();
    drop(frozen);
    let original = fs::read(&frozen_path).unwrap();
    fs::write(frozen_dir.join("result.json"), b"{}").unwrap();
    let manifest=serde_json::to_vec(&json!({"schema_version":1,"run_id":"snapshot-contract","files":[{"path":"result.json","sha256":sha256(b"{}"),"bytes":2},{"path":"history.db","sha256":sha256(&original),"bytes":original.len()}]})).unwrap();
    let frozen_ref = commit_result_bundle(&frozen_dir, &root, &manifest).unwrap();
    let source_path = history_query_path(&frozen_path);
    assert_ne!(source_path, frozen_path);
    let cut = control
        .prepare_history(&frozen_path, HistoryBackend::Postgres)
        .unwrap();
    control.activate(&cut.id).unwrap();
    control.prune_history(&frozen_path, 4).unwrap();
    assert_eq!(
        ArchiveSnapshot::open(&source_path)
            .unwrap()
            .rows(0, 4, 10)
            .unwrap()
            .len(),
        0
    );
    assert_eq!(fs::read(&frozen_path).unwrap(), original);
    let cut = control
        .prepare_history(&frozen_path, HistoryBackend::Sqlite)
        .unwrap();
    control.activate(&cut.id).unwrap();
    assert_eq!(
        ArchiveSnapshot::open(&source_path)
            .unwrap()
            .rows(0, 4, 10)
            .unwrap()
            .len(),
        4
    );
    assert_eq!(fs::read(&frozen_path).unwrap(), original);
    materialize_result_bundle(&root, &frozen_ref, &output.join("frozen-materialized")).unwrap();
    assert_eq!(
        fs::read(output.join("frozen-materialized/history.db")).unwrap(),
        original
    );
    record(
        &mut cases,
        "published_v1_sqlite_snapshot_is_immutable_across_prune_and_backfill",
    );

    let objects = object_store(&root).unwrap();
    let a = objects.put_bytes(b"original result bytes").unwrap();
    let bundle = PublishedManifest {
        schema_version: 1,
        logical_id: "object-contract".into(),
        kind: "test".into(),
        files: vec![PublishedFile {
            path: "nested/result.json".into(),
            object: a.clone(),
        }],
        metadata: json!({}),
    };
    let cat = ResultCatalog::open(&root.join("catalog.db")).unwrap();
    let m = bundle.publish(objects.as_ref(), &cat).unwrap();
    let obj_plan = control.prepare_objects(ObjectBackend::S3).unwrap();
    objects.put_bytes(b"late object").unwrap();
    control.activate(&obj_plan.id).unwrap();
    assert!(objects.open_verified(&a).is_err());
    let remote = object_store(&root).unwrap();
    let mut bytes = Vec::new();
    remote
        .open_verified(&a)
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    assert_eq!(bytes, b"original result bytes");
    assert_eq!(remote.put_bytes(&bytes).unwrap(), a);
    let dup = (0..4)
        .map(|_| {
            let store = S3ObjectStore::connect(
                p.s3.clone().unwrap(),
                p.budget.clone(),
                &root.join(".concurrent"),
                None,
            )
            .unwrap();
            std::thread::spawn(move || store.put_bytes(b"parallel conditional object").unwrap())
        })
        .collect::<Vec<_>>();
    let refs = dup
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect::<Vec<_>>();
    assert!(refs.iter().all(|r| r == &refs[0]));
    record(&mut cases, "s3_conditional_concurrent_original_bytes");
    assert!(control.evict_local_objects().unwrap() >= 3);
    assert!(
        !root
            .join("objects")
            .join(&a.sha256[..2])
            .join(&a.sha256)
            .exists()
    );
    let local_plan = control.prepare_objects(ObjectBackend::Filesystem).unwrap();
    control.activate(&local_plan.id).unwrap();
    let local = object_store(&root).unwrap();
    PublishedManifest::load(local.as_ref(), &m)
        .unwrap()
        .verify_objects(local.as_ref())
        .unwrap();
    record(&mut cases, "objects_evict_s3_to_filesystem_rollback");
    // A catalog root whose referenced file disappeared must refuse cutover even if inventories match.
    fs::remove_file(root.join("objects").join(&a.sha256[..2]).join(&a.sha256)).unwrap();
    assert!(control.prepare_objects(ObjectBackend::S3).is_err());
    assert_eq!(
        control.object_route().unwrap().backend,
        ObjectBackend::Filesystem
    );
    record(
        &mut cases,
        "objects_missing_catalog_reference_refuses_activation",
    );
    let mut other = p.clone();
    other.s3.as_mut().unwrap().prefix = p.s3.as_ref().unwrap().prefix.clone();
    assert!(StorageControl::initialize(&output.join("foreign-storage"), other).is_err());
    record(&mut cases, "s3_namespace_owner_isolation");
    let store = S3ObjectStore::connect(
        p.s3.clone().unwrap(),
        p.budget.clone(),
        &root.join(".fault"),
        None,
    )
    .unwrap();
    let corrupted = store.put_bytes(b"will be corrupted").unwrap();
    let s = p.s3.as_ref().unwrap();
    rt.block_on(
        sdk.put_object()
            .bucket(&s.bucket)
            .key(format!(
                "{}/objects/{}/{}",
                s.prefix,
                &corrupted.sha256[..2],
                corrupted.sha256
            ))
            .body(aws_sdk_s3::primitives::ByteStream::from_static(
                b"will be CORRUPTED",
            ))
            .send(),
    )
    .unwrap();
    assert!(store.open_verified(&corrupted).is_err());
    record(
        &mut cases,
        "s3_hash_corruption_rejected_without_local_fallback",
    );
    let mut tls = p.s3.clone().unwrap();
    tls.allow_insecure_loopback = false;
    assert!(S3ObjectStore::connect(tls, p.budget.clone(), &root.join(".tls"), None).is_err());
    let mut unsafe_pg = p.postgres.clone().unwrap();
    unsafe_pg.allow_insecure_loopback = false;
    assert!(PostgresProjection::open(unsafe_pg).is_err());
    record(
        &mut cases,
        "insecure_transport_requires_explicit_loopback_policy",
    );
    let mut budget = p.budget.clone();
    budget.max_object_bytes = 4;
    budget.max_local_object_bytes = 8;
    let bounded = LocalObjectStore::with_budget(output.join("quota"), budget.clone()).unwrap();
    assert!(bounded.put_bytes(b"12345").is_err());
    budget.max_database_bytes = 4096;
    assert!(check_storage_budget(&archive_path, &budget).is_err());
    record(
        &mut cases,
        "local_object_and_history_quota_refuse_without_success",
    );
    let checksum = projection
        .digest(
            &source,
            ArchiveSnapshot::open(&archive_path)
                .unwrap()
                .metadata
                .upper_row,
        )
        .unwrap();
    fs::write(output.join("results.json"),serde_json::to_vec_pretty(&json!({"passed":true,"cases":cases,"history_digest":checksum,"source_id":source,"target_id":projection.target_id,"storage":control.inspect().unwrap(),"recovery_backend":"local_sqlite"})).unwrap()).unwrap();
}
