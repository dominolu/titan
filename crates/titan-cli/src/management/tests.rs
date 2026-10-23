use super::{contract::*, nonce, store::Store};
use serde_json::json;

fn input(key: &str) -> SubmitOperation {
    SubmitOperation {
        idempotency_key: key.into(),
        node_id: "build".into(),
        action: Action::Describe {
            template_id: "grid".into(),
            parameters: json!({"order_qty_lots":2}),
        },
    }
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
        kind: "version".into(),
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
        kind: "template".into(),
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
