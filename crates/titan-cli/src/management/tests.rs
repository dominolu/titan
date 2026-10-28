use super::{contract::*, nonce, store::Store};
use serde_json::json;

fn input(key: &str) -> SubmitOperation {
    SubmitOperation {
        idempotency_key: key.into(),
        node_id: "build".into(),
        action: Action::Describe {
            parameter_preset_id: "grid".into(),
            parameters: json!({"order_qty_lots":2}),
        },
    }
}

#[test]
fn strategy_identity_groups_aliases_without_using_source_bytes_or_runtime_ids() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("strategy.py");
    std::fs::write(&source, "first").unwrap();
    let mut template = super::config::StrategyParameterPreset {
        strategy_id: None,
        legacy_research_id: None,
        strategy_name: Some("网格策略".into()),
        source: source.clone(),
        parameters: temp.path().join("parameters.json"),
        layout: None,
    };
    let first = template.strategy_identity("build").unwrap();
    std::fs::write(&source, "second version").unwrap();
    assert_eq!(first, template.clone().strategy_identity("build").unwrap());
    assert_ne!(first.0, template.strategy_identity("other").unwrap().0);
    template.strategy_id = Some("grid".into());
    assert_eq!(template.strategy_identity("other").unwrap().0, "grid");
    template.strategy_id = Some("../invalid".into());
    assert!(template.strategy_identity("build").is_err());
    template.strategy_id = Some("a".repeat(128));
    assert_eq!(template.strategy_identity("build").unwrap().0.len(), 128);
    template.strategy_id = Some("a".repeat(129));
    assert!(template.strategy_identity("build").is_err());
    template.strategy_id = None;
    template.legacy_research_id = Some("a".repeat(119));
    let (id, _) = template.strategy_identity("build").unwrap();
    assert_eq!(id.len(), 128);
    super::checked_id(&id).unwrap();
    for length in [120, 128] {
        template.legacy_research_id = Some("a".repeat(length));
        assert!(template.strategy_identity("build").is_err());
    }
}

#[test]
fn strategy_scope_filters_before_paging_preserves_old_digests_and_separates_private_live_records() {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::open(&temp.path().join("db")).unwrap();
    let save = |id: &str, kind: &str, body| {
        store
            .save_resource(
                Resource {
                    id: id.into(),
                    kind: kind.into(),
                    node_id: "build".into(),
                    revision: String::new(),
                    frozen: kind != "parameter_preset",
                    digest: String::new(),
                    body,
                },
                None,
            )
            .unwrap()
    };
    let template = save(
        "build-template-neutral",
        "parameter_preset",
        json!({"strategy_id":"strategy-grid","strategy_name":"网格","source_digest":"source-1"}),
    );
    let old = save(
        "old-version",
        "parameter_version",
        json!({"parameter_preset_id":"neutral","source_digest":"source-1"}),
    );
    save(
        "strategy_version",
        "strategy_version",
        json!({"parameter_version_id":old.id}),
    );
    save(
        "backtest",
        "backtest",
        json!({"strategy_version_id":"strategy_version"}),
    );
    save("report", "report", json!({"backtest_id":"backtest"}));
    save(
        "deployment",
        "deployment",
        json!({"strategy_version_id":"strategy_version","environment":"live","account":"gate"}),
    );
    save(
        "run",
        "run",
        json!({"deployment_id":"deployment","environment":"live","account":"gate"}),
    );
    for n in 0..25 {
        save(
            &format!("foreign-{n}"),
            "strategy_version",
            json!({"strategy_id":"strategy-other"}),
        );
    }
    let actor = identity(vec!["gate".into()]);
    let listed = store
        .list_resources_scoped(
            &actor,
            Some("strategy_version"),
            0,
            20,
            Some("strategy-grid"),
            None,
        )
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, "strategy_version");
    assert!(
        store
            .list_resources_scoped(
                &actor,
                None,
                0,
                20,
                Some("strategy-other"),
                Some("strategy_version")
            )
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .list_resources_scoped(&actor, Some("report"), 0, 20, Some("strategy-grid"), None)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(store.resource(&old.id).unwrap().digest, old.digest);
    assert!(
        store
            .list_resources_scoped(
                &identity(vec![]),
                Some("run"),
                0,
                20,
                Some("strategy-grid"),
                None
            )
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .list_resources_scoped(&actor, Some("run"), 0, 20, Some("strategy-grid"), None)
            .unwrap()
            .len(),
        1
    );
    let mut changed = template;
    changed.body["source_digest"] = "reused-alias".into();
    store.save_resource(changed, None).unwrap();
    assert!(
        store
            .list_resources_scoped(
                &actor,
                Some("parameter_version"),
                0,
                20,
                Some("strategy-grid"),
                None
            )
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .list_resources_scoped(
                &actor,
                Some("parameter_version"),
                0,
                20,
                Some("unassigned"),
                None
            )
            .unwrap()[0]
            .id,
        old.id
    );
    assert_eq!(store.resource(&old.id).unwrap().digest, old.digest);
}
#[test]
fn research_catalog_does_not_expose_private_accounts_or_account_only_observers() {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::open(&temp.path().join("db")).unwrap();
    for (id, kind, body) in [
        (
            "parameter_preset",
            "parameter_preset",
            json!({"strategy_id":"strategy-grid","strategy_name":"网格"}),
        ),
        (
            "private",
            "run",
            json!({"strategy_id":"strategy-private","environment":"live","account":"private","mode":"trade"}),
        ),
        (
            "observer",
            "run",
            json!({"strategy_id":"strategy-observer","environment":"live","account":"gate","mode":"observe"}),
        ),
        ("dataset", "dataset", json!({"strategy_id":"strategy-grid"})),
    ] {
        store
            .save_resource(
                Resource {
                    id: id.into(),
                    kind: kind.into(),
                    node_id: "build".into(),
                    revision: String::new(),
                    frozen: false,
                    digest: String::new(),
                    body,
                },
                None,
            )
            .unwrap();
    }
    let actor = identity(vec!["gate".into()]);
    let catalog = store.list_strategies(&actor, 0, 20).unwrap();
    assert_eq!(catalog.len(), 1);
    assert_eq!(catalog[0].id, "strategy-grid");
    assert!(
        store
            .list_resources_scoped(&actor, Some("run"), 0, 20, Some("strategy-observer"), None)
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .list_resources_scoped(&actor, Some("dataset"), 0, 20, Some("strategy-grid"), None)
            .unwrap()
            .is_empty()
    );
}
#[test]
fn stale_receipt_poll_cannot_undo_proven_capacity_rejection() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).unwrap();
    let op = store
        .accept("operator", &input("race"), vec![], "hash", None)
        .unwrap();
    store.begin_dispatch_after_absence(&op.id).unwrap();
    let polled = store.operation(&op.id).unwrap();
    store.admission_rejected(&op.id, &op.input_digest).unwrap();
    let stale = Receipt {
        operation_id: op.id.clone(),
        input_digest: op.input_digest.clone(),
        state: OperationState::Unknown,
        result: None,
        error: Some("older concurrent absence poll".into()),
    };
    store
        .update_operation_if_current(&stale, &polled.revision)
        .unwrap();
    assert_eq!(
        store.operation(&op.id).unwrap().state,
        OperationState::Pending
    );
    assert!(
        store
            .begin_dispatch_after_absence(&op.id)
            .unwrap()
            .is_some()
    );
    let current = store.operation(&op.id).unwrap();
    store
        .update_operation_if_current(
            &Receipt {
                state: OperationState::Succeeded,
                ..stale
            },
            &current.revision,
        )
        .unwrap();
    assert_eq!(
        store.operation(&op.id).unwrap().state,
        OperationState::Succeeded
    );
}
#[test]
fn ordinary_capacity_cannot_consume_stop_admission_or_hide_stop_from_reconciliation() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).unwrap();
    for i in 0..256 {
        let op = store
            .accept(
                "operator",
                &input(&format!("ordinary-{i}")),
                vec![],
                "hash",
                None,
            )
            .unwrap();
        store.begin_dispatch_after_absence(&op.id).unwrap();
    }
    assert!(
        store
            .accept("operator", &input("full"), vec![], "hash", None)
            .is_err()
    );
    let request = SubmitOperation {
        idempotency_key: "stop".into(),
        node_id: "live".into(),
        action: Action::Stop {
            run_id: "run".into(),
            generation: "1".into(),
        },
    };
    let stop = store
        .accept("operator", &request, vec![], "stop", None)
        .unwrap();
    let active = store.active().unwrap();
    assert_eq!(active.len(), 257);
    assert_eq!(active[0].id, stop.id);
}
#[test]
fn cancellation_and_dispatch_claim_share_one_transaction_boundary() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).unwrap();
    let op = store
        .accept("operator", &input("before"), vec![], "hash", None)
        .unwrap();
    store.cancel(&op.id).unwrap();
    assert!(
        store
            .begin_dispatch_after_absence(&op.id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.operation(&op.id).unwrap().state,
        OperationState::Cancelled
    );
    let op = store
        .accept("operator", &input("after"), vec![], "hash", None)
        .unwrap();
    assert!(
        store
            .begin_dispatch_after_absence(&op.id)
            .unwrap()
            .is_some()
    );
    store.cancel(&op.id).unwrap();
    assert_eq!(
        store.operation(&op.id).unwrap().state,
        OperationState::Unknown
    );
    assert!(
        store
            .begin_dispatch_after_absence(&op.id)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .update_operation(&Receipt {
                operation_id: op.id.clone(),
                input_digest: op.input_digest.clone(),
                state: OperationState::Pending,
                result: None,
                error: None
            })
            .is_err()
    );
    store.admission_rejected(&op.id, &op.input_digest).unwrap();
    assert!(
        store
            .begin_dispatch_after_absence(&op.id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.operation(&op.id).unwrap().state,
        OperationState::Cancelled
    );
}
#[test]
fn concurrent_dispatchers_cannot_claim_the_same_operation_twice() {
    use std::sync::Arc;
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(&tmp.path().join("db")).unwrap());
    let op = store
        .accept("operator", &input("parallel"), vec![], "hash", None)
        .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let store = store.clone();
            let barrier = barrier.clone();
            let id = op.id.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store.begin_dispatch_after_absence(&id).unwrap().is_some()
            })
        })
        .collect();
    assert_eq!(
        workers
            .into_iter()
            .map(|w| w.join().unwrap())
            .filter(|claimed| *claimed)
            .count(),
        1
    );
}
#[test]
fn incomplete_admission_never_claims_that_an_operation_was_absent() {
    let tmp = tempfile::tempdir().unwrap();
    let configuration = format!(
        "id='build'\nlisten='127.0.0.1:0'\ntoken_env='unused'\nwork_root={}\ntitan_program='/bin/false'\nenvironments=['backtest']\n",
        serde_json::to_string(tmp.path()).unwrap()
    );
    let config: super::config::NodeConfig = toml::from_str(&configuration).unwrap();
    std::fs::create_dir_all(tmp.path().join("operations/partial")).unwrap();
    let error = super::node::read_receipt(&config, "partial").unwrap_err();
    assert_eq!(
        error.downcast_ref::<super::Fault>().unwrap().code,
        "RECEIPT_UNCERTAIN"
    );
    let error = super::node::read_receipt(&config, "never-admitted").unwrap_err();
    assert_eq!(
        error.downcast_ref::<super::Fault>().unwrap().code,
        "NOT_FOUND"
    );
}
#[tokio::test]
async fn flooded_task_output_is_drained_with_a_strict_file_limit() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tokio::io::AsyncWriteExt;
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("bounded.log");
    let (mut writer, reader) = tokio::io::duplex(64);
    let producer = tokio::spawn(async move {
        writer.write_all(&vec![42; 128 * 1024]).await.unwrap();
    });
    let overflow = Arc::new(AtomicBool::new(false));
    super::node::bounded_output(
        reader,
        std::fs::File::create(&path).unwrap(),
        1024,
        overflow.clone(),
    )
    .await
    .unwrap();
    producer.await.unwrap();
    assert_eq!(std::fs::metadata(path).unwrap().len(), 1024);
    assert!(overflow.load(Ordering::Acquire));
}
fn identity(accounts: Vec<String>) -> Identity {
    Identity {
        username: "reader".into(),
        role: Role::Reader,
        nodes: vec!["build".into()],
        accounts,
        environments: vec!["backtest".into(), "live".into()],
    }
}
#[test]
fn alert_acknowledgement_is_scoped_to_one_source_condition() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).unwrap();
    let mut alert = Resource {
        id: "alert".into(),
        kind: "alert".into(),
        node_id: "build".into(),
        revision: String::new(),
        frozen: false,
        digest: String::new(),
        body: json!({"environment":"backtest","condition_identity":"generation:one","active":true}),
    };
    let first = store.save_resource(alert.clone(), None).unwrap();
    alert.body["acknowledgement"] = json!({"actor":"operator"});
    let ack = store
        .save_resource(alert.clone(), Some(&first.revision))
        .unwrap();
    alert
        .body
        .as_object_mut()
        .unwrap()
        .remove("acknowledgement");
    assert_eq!(
        store.save_resource(alert.clone(), None).unwrap().revision,
        ack.revision
    );
    alert.body["condition_identity"] = "generation:two".into();
    assert!(
        store
            .save_resource(alert, None)
            .unwrap()
            .body
            .get("acknowledgement")
            .is_none()
    );
}
#[test]
fn idempotency_survives_restart_and_rejects_changed_input() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("manager.db");
    let store = Store::open(&path).unwrap();
    let first = store
        .accept("operator", &input("key"), vec![], "digest", None)
        .unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    let duplicate = store
        .accept("operator", &input("key"), vec![], "digest", None)
        .unwrap();
    assert_eq!(first.id, duplicate.id);
    assert!(
        store
            .accept("operator", &input("key"), vec![], "changed", None)
            .is_err()
    );
    assert_eq!(store.active().unwrap().len(), 1);
}
#[test]
fn terminal_receipt_cannot_be_overwritten_by_late_acknowledgement() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).unwrap();
    let op = store
        .accept("operator", &input("key"), vec![], "digest", None)
        .unwrap();
    let mut receipt = Receipt {
        operation_id: op.id,
        input_digest: op.input_digest,
        state: OperationState::Succeeded,
        result: Some(json!({"proof":"complete"})),
        error: None,
    };
    store.update_operation(&receipt).unwrap();
    receipt.state = OperationState::Running;
    receipt.result = None;
    let actual = store.update_operation(&receipt).unwrap();
    assert_eq!(actual.state, OperationState::Succeeded);
    assert_eq!(actual.result.unwrap()["proof"], "complete");
    receipt.input_digest = "wrong".into();
    assert!(store.update_operation(&receipt).is_err());
}
#[test]
fn frozen_resources_and_optimistic_revisions_are_enforced() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).unwrap();
    let resource = Resource {
        id: nonce(),
        kind: "parameter_version".into(),
        node_id: "build".into(),
        revision: String::new(),
        frozen: false,
        digest: String::new(),
        body: json!({"parameters":{"a":1}}),
    };
    let first = store.save_resource(resource, None).unwrap();
    let mut changed = first.clone();
    changed.body["parameters"]["a"] = 2.into();
    assert!(store.save_resource(changed.clone(), Some("0")).is_err());
    changed.frozen = true;
    let frozen = store.save_resource(changed, Some(&first.revision)).unwrap();
    let mut changed = frozen.clone();
    changed.body["parameters"]["a"] = 3.into();
    assert!(
        store
            .save_resource(changed, Some(&frozen.revision))
            .is_err()
    );
}
#[test]
fn materialized_request_does_not_drift_with_catalog_changes() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).unwrap();
    let resource = Resource {
        id: nonce(),
        kind: "parameter_preset".into(),
        node_id: "build".into(),
        revision: String::new(),
        frozen: false,
        digest: String::new(),
        body: json!({"source_digest":"original"}),
    };
    let resource = store.save_resource(resource, None).unwrap();
    let op = store
        .accept(
            "operator",
            &input("key"),
            vec![resource.clone()],
            "digest",
            None,
        )
        .unwrap();
    let mut changed = resource.clone();
    changed.body["source_digest"] = "changed".into();
    store
        .save_resource(changed, Some(&resource.revision))
        .unwrap();
    assert_eq!(
        store.request(&op.id).unwrap().resources[0].body["source_digest"],
        "original"
    );
}
#[test]
fn account_authorization_filters_both_snapshots_and_events() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).unwrap();
    let cursor = store.snapshot(&identity(vec![]), 100).unwrap().cursor;
    let resource = Resource {
        id: nonce(),
        kind: "account".into(),
        node_id: "build".into(),
        revision: String::new(),
        frozen: false,
        digest: String::new(),
        body: json!({"environment":"live","account":"secret-account","balance":"18446744073709551615.00000001"}),
    };
    store.save_resource(resource, None).unwrap();
    assert!(
        store
            .snapshot(&identity(vec![]), 100)
            .unwrap()
            .resources
            .is_empty()
    );
    assert!(
        store
            .events(&identity(vec![]), &cursor)
            .unwrap()
            .0
            .is_empty()
    );
    let actual = store
        .snapshot(&identity(vec!["secret-account".into()]), 100)
        .unwrap();
    assert_eq!(
        actual.resources[0].body["balance"],
        "18446744073709551615.00000001"
    );
}
#[test]
fn stale_epoch_and_expired_cursor_require_resynchronization() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("db");
    let store = Store::open(&path).unwrap();
    let cursor = store.snapshot(&identity(vec![]), 100).unwrap().cursor;
    drop(store);
    let store = Store::open(&path).unwrap();
    assert!(store.events(&identity(vec![]), &cursor).is_err());
    assert!(
        store
            .events(&identity(vec![]), &format!("{}:1", store.epoch))
            .is_err()
    );
}
#[test]
fn unknown_start_keeps_account_instrument_reserved() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).unwrap();
    let first = store
        .accept(
            "operator",
            &input("one"),
            vec![],
            "first",
            Some("gate:BTC_USDT"),
        )
        .unwrap();
    store
        .update_operation(&Receipt {
            operation_id: first.id,
            input_digest: first.input_digest,
            state: OperationState::Unknown,
            result: None,
            error: Some("lost ack".into()),
        })
        .unwrap();
    assert!(
        store
            .accept(
                "operator",
                &input("two"),
                vec![],
                "second",
                Some("gate:BTC_USDT")
            )
            .is_err()
    );
}

#[tokio::test]
async fn http_requires_real_session_and_does_not_fallback_api_errors_to_html() {
    use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let tmp = tempfile::tempdir().unwrap();
    let hash = Argon2::default()
        .hash_password(
            b"a-long-test-password",
            &SaltString::generate(&mut rand::rngs::OsRng),
        )
        .unwrap()
        .to_string();
    let config = super::config::ManagerConfig {
        listen: "127.0.0.1:0".into(),
        public_origin: "http://127.0.0.1:3200".into(),
        database: tmp.path().join("db"),
        web_root: tmp.path().into(),
        secure_cookie: false,
        session_seconds: 3600,
        nodes: Default::default(),
        users: vec![super::config::User {
            username: "reader".into(),
            password_hash: hash,
            role: Role::Reader,
            nodes: vec![],
            accounts: vec![],
            environments: vec!["backtest".into()],
        }],
    };
    let app = super::manager_router(config).unwrap();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/snapshot")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/does-not-exist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(response.headers()["content-type"], "application/json");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/session")
                .header("content-type", "application/json")
                .header("origin", "https://untrusted.example")
                .body(Body::from(
                    r#"{"username":"reader","password":"a-long-test-password"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/operations")
                .header("content-type", "application/json")
                .body(Body::from("{invalid-json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.headers()["content-type"], "application/json");
}

#[test]
fn occupancy_survives_unknown_and_only_matching_engine_proof_releases_it() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).unwrap();
    let start = SubmitOperation {
        idempotency_key: "first".into(),
        node_id: "live".into(),
        action: Action::Start {
            deployment_id: "deployment".into(),
        },
    };
    let op = store
        .accept(
            "operator",
            &start,
            vec![],
            "hash",
            Some("canonical-account-instrument"),
        )
        .unwrap();
    let mut competing = start.clone();
    competing.node_id = "other".into();
    competing.idempotency_key = "competing".into();
    assert!(
        store
            .accept(
                "operator",
                &competing,
                vec![],
                "other",
                Some("canonical-account-instrument")
            )
            .is_err()
    );
    store.begin_dispatch_after_absence(&op.id).unwrap();
    let mut resource = Resource {
        id: format!("live-run-{}", op.id),
        kind: "run".into(),
        node_id: "live".into(),
        revision: String::new(),
        frozen: false,
        digest: String::new(),
        body: json!({"admission_operation_id":op.id,"environment":"live","process_state":"UNKNOWN","release_proof":{"safe":false,"source":"trading_runtime"}}),
    };
    store.save_resource(resource.clone(), None).unwrap();
    assert!(
        store
            .accept(
                "operator",
                &competing,
                vec![],
                "other",
                Some("canonical-account-instrument")
            )
            .is_err()
    );
    let mut wrong = resource.clone();
    wrong.id = "other-run".into();
    wrong.body["release_proof"]["safe"] = true.into();
    store.save_resource(wrong, None).unwrap();
    assert!(
        store
            .accept(
                "operator",
                &competing,
                vec![],
                "other",
                Some("canonical-account-instrument")
            )
            .is_err()
    );
    resource.body["release_proof"]["safe"] = true.into();
    store.save_resource(resource, None).unwrap();
    assert!(
        store
            .accept(
                "operator",
                &competing,
                vec![],
                "other",
                Some("canonical-account-instrument")
            )
            .is_ok()
    );
}
#[test]
fn failed_unactivated_recovery_keeps_predecessor_retryable_without_releasing_the_account() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("db");
    let store = Store::open(&path).unwrap();
    let start = SubmitOperation {
        idempotency_key: "start".into(),
        node_id: "live".into(),
        action: Action::Start {
            deployment_id: "deployment".into(),
        },
    };
    let initial = store
        .accept("operator", &start, vec![], "start", Some("binding"))
        .unwrap();
    let predecessor = format!("live-run-{}", initial.id);
    let recovery = SubmitOperation {
        idempotency_key: "recover-one".into(),
        node_id: "live".into(),
        action: Action::Restore {
            run_id: predecessor.clone(),
            generation: "1".into(),
            checkpoint_id: "hash".into(),
        },
    };
    let failed = store
        .accept(
            "operator",
            &recovery,
            vec![],
            "recover-one",
            Some("binding"),
        )
        .unwrap();
    let candidate = Resource {
        id: format!("live-run-{}", failed.id),
        kind: "run".into(),
        node_id: "live".into(),
        revision: String::new(),
        digest: String::new(),
        frozen: false,
        body: json!({"run_generation":"unconfirmed","predecessor_run_id":predecessor,"admission_operation_id":failed.id,"observation":{"source":"runtime_owner","run_id":failed.id,"start_admission_absent":true}}),
    };
    store.save_resource(candidate.clone(), None).unwrap();
    let db = rusqlite::Connection::open(&path).unwrap();
    // Also repair a pre-fix ownership transfer even if the resource body is unchanged.
    db.execute(
        "UPDATE occupancy SET run_id=?1 WHERE operation_id=?2",
        rusqlite::params![candidate.id, failed.id],
    )
    .unwrap();
    store.save_resource(candidate.clone(), None).unwrap();
    store
        .update_operation(&Receipt {
            operation_id: failed.id.clone(),
            input_digest: failed.input_digest,
            state: OperationState::Failed,
            result: Some(json!({"start_admission_absent":true})),
            error: None,
        })
        .unwrap();
    let competing = SubmitOperation {
        idempotency_key: "other-start".into(),
        ..start
    };
    assert!(
        store
            .accept("operator", &competing, vec![], "other", Some("binding"))
            .is_err()
    );
    let retry = SubmitOperation {
        idempotency_key: "recover-two".into(),
        ..recovery
    };
    let accepted = store
        .accept("operator", &retry, vec![], "retry", Some("binding"))
        .unwrap();
    let mut confirmed = candidate.clone();
    confirmed.id = format!("live-run-{}", accepted.id);
    confirmed.body = json!({"run_generation":"3","predecessor_run_id":predecessor,"admission_operation_id":accepted.id});
    store.save_resource(confirmed.clone(), None).unwrap();
    store.save_resource(candidate, None).unwrap();
    let owner: (String, String) = db
        .query_row(
            "SELECT operation_id,run_id FROM occupancy WHERE binding='binding'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(owner, (accepted.id, confirmed.id));
    assert!(
        store
            .accept("operator", &competing, vec![], "other", Some("binding"))
            .is_err()
    );
}
#[test]
fn recovery_transfers_occupancy_without_an_old_snapshot_releasing_the_new_owner() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).unwrap();
    let start = SubmitOperation {
        idempotency_key: "start".into(),
        node_id: "live".into(),
        action: Action::Start {
            deployment_id: "deployment".into(),
        },
    };
    let old = store
        .accept("operator", &start, vec![], "hash", Some("binding"))
        .unwrap();
    let old_run = format!("live-run-{}", old.id);
    let recovery = SubmitOperation {
        idempotency_key: "restore".into(),
        node_id: "live".into(),
        action: Action::Restore {
            run_id: old_run.clone(),
            generation: "1".into(),
            checkpoint_id: "digest".into(),
        },
    };
    store
        .accept("operator", &recovery, vec![], "restore", Some("binding"))
        .unwrap();
    store
        .save_resource(
            Resource {
                id: old_run,
                kind: "run".into(),
                node_id: "live".into(),
                revision: String::new(),
                frozen: false,
                digest: String::new(),
                body: json!({"release_proof":{"safe":true,"source":"trading_runtime"}}),
            },
            None,
        )
        .unwrap();
    let next = SubmitOperation {
        idempotency_key: "next".into(),
        ..start
    };
    assert!(
        store
            .accept("operator", &next, vec![], "next", Some("binding"))
            .is_err()
    );
}
#[test]
fn managed_configuration_cannot_use_direct_core_entry() {
    assert!(
        crate::ConfigurationAdapter::adapt(
            toml::from_str("schema_version=1\nmanaged_by_console=true").unwrap(),
            std::path::Path::new(".")
        )
        .is_err()
    );
}

#[test]
fn failed_start_releases_only_with_positive_non_activation_evidence() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).unwrap();
    let start = SubmitOperation {
        idempotency_key: "failed-start".into(),
        node_id: "live".into(),
        action: Action::Start {
            deployment_id: "deployment".into(),
        },
    };
    let op = store
        .accept("operator", &start, vec![], "hash", Some("binding"))
        .unwrap();
    store
        .update_operation(&Receipt {
            operation_id: op.id,
            input_digest: op.input_digest,
            state: OperationState::Failed,
            result: Some(json!({"start_admission_absent":true})),
            error: Some("binding changed before activation".into()),
        })
        .unwrap();
    let next = SubmitOperation {
        idempotency_key: "next".into(),
        ..start
    };
    assert!(
        store
            .accept("operator", &next, vec![], "next", Some("binding"))
            .is_ok()
    );
}

#[test]
fn catalog_waits_for_generation_but_rejects_invalid_confirmed_storage() {
    use titan_strategy_runtime::{
        SqliteRecoveryConfig, SqliteRecoveryStore, StrategyRecoveryStore,
    };
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("runtime.db");
    std::fs::write(&path, []).unwrap();
    let mut raw: crate::ApplicationConfig = toml::from_str("schema_version=1").unwrap();
    raw.strategy_service.recovery_storage = Some(SqliteRecoveryConfig::new(path.clone()));
    assert!(
        super::control::catalog_recovery(&raw, "unconfirmed")
            .unwrap()
            .is_none()
    );
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    assert!(super::control::catalog_recovery(&raw, "1").is_err());
    let store = SqliteRecoveryStore::open(SqliteRecoveryConfig::new(path)).unwrap();
    let confirmed = super::control::catalog_recovery(&raw, "1")
        .unwrap()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    confirmed.close(deadline).unwrap();
    store.close(deadline).unwrap();
}

#[test]
fn late_non_activation_proof_releases_only_its_terminal_fresh_start() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).unwrap();
    let start = SubmitOperation {
        idempotency_key: "late-proof-start".into(),
        node_id: "live".into(),
        action: Action::Start {
            deployment_id: "deployment".into(),
        },
    };
    let op = store
        .accept("operator", &start, vec![], "hash", Some("binding"))
        .unwrap();
    let proof = Resource {
        id: format!("live-run-{}", op.id),
        kind: "run".into(),
        node_id: "live".into(),
        revision: String::new(),
        digest: String::new(),
        frozen: false,
        body: json!({"run_generation":"unconfirmed","admission_operation_id":op.id,"observation":{"source":"runtime_owner","run_id":op.id,"start_admission_absent":true}}),
    };
    let competing = SubmitOperation {
        idempotency_key: "competing".into(),
        ..start
    };
    // A nonterminal receipt, a missing proof, or the wrong owner cannot free it.
    store.save_resource(proof.clone(), None).unwrap();
    assert!(
        store
            .accept("operator", &competing, vec![], "next", Some("binding"))
            .is_err()
    );
    store
        .update_operation(&Receipt {
            operation_id: op.id.clone(),
            input_digest: op.input_digest,
            state: OperationState::Failed,
            result: None,
            error: Some("catalog initialization race".into()),
        })
        .unwrap();
    for field in ["source", "run_id"] {
        let mut wrong = proof.clone();
        wrong.body["observation"][field] = "wrong".into();
        store.save_resource(wrong, None).unwrap();
        assert!(
            store
                .accept("operator", &competing, vec![], "next", Some("binding"))
                .is_err()
        );
    }
    let mut wrong_node = proof.clone();
    wrong_node.node_id = "other".into();
    store.save_resource(wrong_node, None).unwrap();
    assert!(
        store
            .accept("operator", &competing, vec![], "next", Some("binding"))
            .is_err()
    );
    store.save_resource(proof.clone(), None).unwrap();
    let next = store
        .accept("operator", &competing, vec![], "next", Some("binding"))
        .unwrap();
    assert_eq!(
        store.operation(&op.id).unwrap().state,
        OperationState::Failed
    );
    store.save_resource(proof, None).unwrap();
    let third = SubmitOperation {
        idempotency_key: "third".into(),
        ..competing
    };
    assert!(
        store
            .accept("operator", &third, vec![], "third", Some("binding"))
            .is_err()
    );
    assert_eq!(
        store.operation(&next.id).unwrap().state,
        OperationState::Pending
    );
}

#[test]
fn imported_artifact_download_uses_existing_resource_authorization() {
    use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
    let tmp = tempfile::tempdir().unwrap();
    let hash = Argon2::default()
        .hash_password(
            b"long-test-password",
            &SaltString::generate(&mut rand::rngs::OsRng),
        )
        .unwrap()
        .to_string();
    let config = super::config::ManagerConfig {
        listen: "127.0.0.1:0".into(),
        public_origin: "http://127.0.0.1:3200".into(),
        database: tmp.path().join("db"),
        web_root: tmp.path().into(),
        secure_cookie: false,
        session_seconds: 3600,
        nodes: Default::default(),
        users: vec![super::config::User {
            username: "reader".into(),
            password_hash: hash,
            role: Role::Reader,
            nodes: vec!["build".into()],
            accounts: vec!["gate".into()],
            environments: vec!["backtest".into()],
        }],
    };
    let app = super::app::App::open(config).unwrap();
    app.store
        .save_resource(
            Resource {
                id: "imported".into(),
                kind: "strategy_version".into(),
                node_id: "build".into(),
                revision: String::new(),
                frozen: true,
                digest: String::new(),
                body: json!({"operation_id":"external-build","environment":"backtest"}),
            },
            None,
        )
        .unwrap();
    let reader = identity(vec!["gate".into()]);
    assert_eq!(
        app.file_node(&reader, "external-build", "strategy.titan")
            .unwrap(),
        "build"
    );
    assert!(
        app.file_node(&reader, "external-build", "receipt.json")
            .is_err()
    );
    let mut denied = reader.clone();
    denied.nodes = vec!["other".into()];
    assert_eq!(
        app.file_node(&denied, "external-build", "strategy.titan")
            .unwrap_err()
            .downcast_ref::<super::Fault>()
            .unwrap()
            .status,
        403
    );
}

#[test]
fn historical_request_hash_accepts_only_the_same_canonical_retry() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(&tmp.path().join("db")).unwrap();
    let request = input("legacy-key");
    let hash = super::legacy::request_digest(&request).unwrap();
    let first = store
        .accept("operator", &request, vec![], &hash, None)
        .unwrap();
    let canonical = super::digest(&titan_strategy_runtime::compiler_json_v13(
        &serde_json::to_value(&request).unwrap(),
    ));
    let retry = store
        .accept("operator", &request, vec![], &canonical, None)
        .unwrap();
    assert_eq!(first.id, retry.id);
    let mut changed = request;
    changed.action = Action::Describe {
        parameter_preset_id: "other".into(),
        parameters: json!({"order_qty_lots":2}),
    };
    assert!(
        store
            .accept("operator", &changed, vec![], "different", None)
            .is_err()
    );
}
