use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use semver::Version;
use titan_event_engine::{EventClass, EventEngine, EventEngineConfig, PoolKind};
use titan_plugin_engine::{
    ApiVersion, DispatchOutcome, DynamicPluginLoader, DynamicPluginSession, EventControl,
    EventHandler, EventQos, EventView, ExecutionModel, ExecutionSpec, PluginEngine, PluginError,
    PluginIdentity, PluginSpec, ServiceKey, ServiceScope, StopReason, SubscriptionLimits,
    SubscriptionSpec, TraceContext,
};

use crate::*;

#[test]
fn fill_v2_preserves_last_and_cumulative_quantities() {
    let value = FillV2 {
        header: AccountEventHeaderV1 {
            account_id: 1,
            kind: event_kind::FILL,
            account_generation: 2,
            account_epoch: 3,
            account_version: 4,
            ..AccountEventHeaderV1::default()
        },
        asset_id: 9,
        last_fill_quantity_lots: 2,
        cumulative_filled_quantity_lots: 7,
        ..FillV2::default()
    };
    let mut encoded = vec![0; FillV2::ENCODED_LEN];
    value.encode_into(&mut encoded).unwrap();
    assert_eq!(FillV2::decode(&encoded).unwrap(), value);
    assert_eq!(
        account_event_layout_version(FILL_EVENT, FILL_EVENT_SCHEMA_VERSION),
        Some((event_kind::FILL, FillV2::ENCODED_LEN))
    );
}

struct TestSecrets;
impl SecretProvider for TestSecrets {
    fn resolve(&self, reference: &SecretRef) -> Result<SecretValue, AccountConnectorError> {
        if reference.as_str() == "secret://account/main" {
            Ok(SecretValue::new(b"very-secret".to_vec()))
        } else {
            Err(AccountConnectorError::new(
                AccountErrorKind::CredentialUnavailable,
                "credential unavailable",
            ))
        }
    }
}

#[cfg(unix)]
#[test]
fn directory_secret_provider_is_scoped_bounded_and_requires_private_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let nonce = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "titan-account-secrets-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let secret_path = root.join("okx.toml");
    std::fs::write(&secret_path, b"api_key = \"redacted\"\n").unwrap();
    std::fs::set_permissions(&secret_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let provider = DirectorySecretProvider::new(&root).unwrap();
    assert_eq!(
        provider
            .resolve(&SecretRef::new("secret://file/okx.toml"))
            .unwrap()
            .expose(),
        b"api_key = \"redacted\"\n"
    );
    assert_eq!(
        provider
            .resolve(&SecretRef::new("secret://file/../outside"))
            .unwrap_err()
            .kind,
        AccountErrorKind::CredentialUnavailable
    );
    std::fs::set_permissions(&secret_path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(
        provider
            .resolve(&SecretRef::new("secret://file/okx.toml"))
            .unwrap_err()
            .kind,
        AccountErrorKind::CredentialUnavailable
    );
    std::fs::remove_file(secret_path).unwrap();
    std::fs::remove_dir(root).unwrap();
}

struct FakeConnector {
    context: AccountConnectorContext,
    running: AtomicBool,
    reconciling: AtomicBool,
    next_id: AtomicU64,
    calls: AtomicU64,
    operation_queries: AtomicU64,
    journal: Mutex<HashMap<CommandId, (SubmitOrderCommand, AccountCommandReceipt)>>,
}

impl FakeConnector {
    fn receipt(&self, id: CommandId, client: Option<ClientOrderId>) -> AccountCommandReceipt {
        AccountCommandReceipt {
            account: self.context.account,
            command_id: id,
            client_order_id: client,
            accepted_at: 123,
        }
    }
    fn empty<T>(&self) -> AccountStateSnapshot<T> {
        AccountStateSnapshot {
            account: self.context.account,
            state: if self.reconciling.load(Ordering::Acquire) {
                AccountSnapshotState::Reconciling
            } else if self.running.load(Ordering::Acquire) {
                AccountSnapshotState::Ready
            } else {
                AccountSnapshotState::Stopped
            },
            committed_epoch: self.running.load(Ordering::Acquire).then_some(1),
            committed_version: self.running.load(Ordering::Acquire).then_some(2),
            captured_at: 10,
            items: Arc::from([]),
        }
    }

    fn publish_command_facts(
        &self,
        command: &SubmitOrderCommand,
    ) -> Result<(), AccountConnectorError> {
        let header = |kind, account_version| AccountEventHeaderV1 {
            account_id: self.context.account.account_id.0,
            kind,
            account_generation: self.context.account.generation,
            account_epoch: 1,
            account_version,
            exchange_ts: 101,
            receive_ts: 102,
            ..Default::default()
        };
        let client_order_id = command.client_order_id.unwrap_or_default();
        let venue_order_id = Id128([3; 16]);
        let publisher = &self.context.event_publisher;
        publisher
            .publish_encoded(
                &OrderChangedV1 {
                    header: header(event_kind::ORDER_CHANGED, 4),
                    asset_id: command.asset_id.0,
                    side: command.side,
                    order_type: command.order_type,
                    time_in_force: command.time_in_force,
                    status: 2,
                    price_ticks: command.price_ticks,
                    quantity_lots: command.quantity_lots,
                    filled_quantity_lots: command.quantity_lots,
                    average_price_ticks: command.price_ticks,
                    client_order_id,
                    venue_order_id,
                    command_id: command.command_id,
                },
                command.trace,
            )
            .map_err(|error| AccountConnectorError::rejected(error.to_string()))?;
        publisher
            .publish_encoded(
                &FillV2 {
                    header: header(event_kind::FILL, 5),
                    asset_id: command.asset_id.0,
                    side: command.side,
                    liquidity: 1,
                    price_ticks: command.price_ticks,
                    last_fill_quantity_lots: command.quantity_lots,
                    cumulative_filled_quantity_lots: command.quantity_lots,
                    trade_id: Id128([4; 16]),
                    venue_order_id,
                    client_order_id,
                    command_id: command.command_id,
                    ..Default::default()
                },
                command.trace,
            )
            .map_err(|error| AccountConnectorError::rejected(error.to_string()))?;
        publisher
            .publish_encoded(
                &PositionChangedV1 {
                    header: header(event_kind::POSITION_CHANGED, 6),
                    asset_id: command.asset_id.0,
                    position_side: command.side,
                    quantity_lots: command.quantity_lots,
                    entry_price_ticks: command.price_ticks,
                    margin_currency_id: self.context.currencies[0].currency_id.0,
                    ..Default::default()
                },
                command.trace,
            )
            .map_err(|error| AccountConnectorError::rejected(error.to_string()))?;
        publisher
            .publish_encoded(
                &BalanceChangedV1 {
                    header: header(event_kind::BALANCE_CHANGED, 7),
                    currency_id: self.context.currencies[0].currency_id.0,
                    wallet_units: 1_000,
                    available_units: 900,
                    margin_units: 100,
                    unrealized_pnl_units: 5,
                },
                command.trace,
            )
            .map_err(|error| AccountConnectorError::rejected(error.to_string()))
    }
}

impl AccountConnector for FakeConnector {
    fn start(&self) -> Result<(), AccountConnectorError> {
        self.running.store(true, Ordering::Release);
        if !self.context.event_publisher.is_open() {
            return Ok(());
        }
        let spoofed = OrderChangedV1 {
            header: AccountEventHeaderV1 {
                account_id: self.context.account.account_id.0.saturating_add(1),
                kind: event_kind::ORDER_CHANGED,
                account_generation: self.context.account.generation,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(
            self.context
                .event_publisher
                .publish_encoded(&spoofed, TraceContext::default())
                .is_err()
        );
        let header = AccountEventHeaderV1 {
            account_id: self.context.account.account_id.0,
            kind: event_kind::RECONCILE_STARTED,
            account_generation: self.context.account.generation,
            account_epoch: 1,
            account_version: 1,
            exchange_ts: 11,
            receive_ts: 12,
            ..Default::default()
        };
        self.context
            .event_publisher
            .publish_encoded(
                &ReconcileStartedV1(ReconcileV1 {
                    header,
                    scope: 0,
                    ..Default::default()
                }),
                TraceContext::default(),
            )
            .map_err(|e| AccountConnectorError::rejected(e.to_string()))?;
        self.context
            .event_publisher
            .publish_encoded(
                &OrderChangedV1 {
                    header: AccountEventHeaderV1 {
                        kind: event_kind::ORDER_CHANGED,
                        account_version: 2,
                        ..header
                    },
                    asset_id: self.context.instruments[0].asset_id.0,
                    quantity_lots: 1,
                    command_id: Id128([7; 16]),
                    ..Default::default()
                },
                TraceContext::default(),
            )
            .map_err(|e| AccountConnectorError::rejected(e.to_string()))?;
        self.context
            .event_publisher
            .publish_encoded(
                &ReconcileCompletedV1(ReconcileV1 {
                    header: AccountEventHeaderV1 {
                        kind: event_kind::RECONCILE_COMPLETED,
                        account_version: 3,
                        ..header
                    },
                    terminal_version: 3,
                    scope: 0,
                    success: 1,
                }),
                TraceContext::default(),
            )
            .map_err(|e| AccountConnectorError::rejected(e.to_string()))?;
        Ok(())
    }
    fn stop(&self, _: Instant) -> Result<(), AccountConnectorError> {
        self.running.store(false, Ordering::Release);
        Ok(())
    }
    fn submit(
        &self,
        c: SubmitOrderCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if c.command_id.0[0] == 255 {
            return Err(AccountConnectorError::new(
                AccountErrorKind::QueueFull,
                "queue full",
            ));
        }
        let mut j = self.journal.lock().unwrap();
        if let Some((old, r)) = j.get(&c.command_id) {
            return if old == &c {
                Ok(r.clone())
            } else {
                Err(AccountConnectorError::new(
                    AccountErrorKind::CommandConflict,
                    "command conflict",
                ))
            };
        }
        let r = self.receipt(c.command_id, c.client_order_id);
        self.publish_command_facts(&c)?;
        j.insert(c.command_id, (c, r.clone()));
        Ok(r)
    }
    fn amend(&self, c: AmendOrderCommand) -> Result<AccountCommandReceipt, AccountConnectorError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.receipt(c.command_id, c.client_order_id))
    }
    fn cancel(
        &self,
        c: CancelOrderCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.receipt(c.command_id, c.client_order_id))
    }
    fn cancel_all(
        &self,
        c: CancelAllCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.receipt(c.command_id, None))
    }
    fn cancel_all_after(
        &self,
        c: CancelAllAfterCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.receipt(c.command_id, None))
    }
    fn reconcile(&self, _: ReconcileScope) -> Result<OperationId, AccountConnectorError> {
        self.reconciling.store(true, Ordering::Release);
        Ok(OperationId(self.next_id.fetch_add(1, Ordering::Relaxed)))
    }
    fn orders(
        &self,
        _: OrderFilter,
    ) -> Result<AccountStateSnapshot<OrderSnapshot>, AccountConnectorError> {
        Ok(self.empty())
    }
    fn positions(
        &self,
        _: PositionFilter,
    ) -> Result<AccountStateSnapshot<PositionSnapshot>, AccountConnectorError> {
        Ok(self.empty())
    }
    fn balances(&self) -> Result<AccountStateSnapshot<BalanceSnapshot>, AccountConnectorError> {
        Ok(self.empty())
    }
    fn health(&self) -> AccountConnectorHealthSnapshot {
        AccountConnectorHealthSnapshot {
            state: if self.running.load(Ordering::Acquire) {
                AccountLifecycle::Ready
            } else {
                AccountLifecycle::Stopped
            },
            message: Arc::from("fake"),
            observed_at: SystemTime::now(),
        }
    }
    fn diagnostics(&self) -> AccountConnectorDiagnosticSnapshot {
        AccountConnectorDiagnosticSnapshot {
            summary: Arc::from("fake"),
            external_order_count: 0,
            command_queue_depth: 0,
            account_epoch: 1,
            account_version: 2,
        }
    }
    fn operation(&self, id: OperationId) -> AccountConnectorOperationSnapshot {
        let state = if self.operation_queries.fetch_add(1, Ordering::AcqRel) == 0 {
            OperationState::Pending
        } else {
            self.reconciling.store(false, Ordering::Release);
            OperationState::Succeeded
        };
        AccountConnectorOperationSnapshot {
            id,
            state,
            detail: Arc::from(if state == OperationState::Pending {
                "reconciling"
            } else {
                "reconciled"
            }),
        }
    }
}

struct FakeFactory;
impl AccountConnectorFactory for FakeFactory {
    fn connector_type(&self) -> &str {
        "fake"
    }
    fn create(
        &self,
        _: &AccountDefinition,
        context: AccountConnectorContext,
    ) -> Result<Arc<dyn AccountConnector>, AccountConnectorError> {
        let secret = context
            .secrets
            .resolve(&SecretRef::new("secret://account/main"))?;
        assert_eq!(secret.expose(), b"very-secret");
        assert_eq!(format!("{:?}", secret), "SecretValue(REDACTED)");
        assert!(
            context
                .secrets
                .resolve(&SecretRef::new("secret://another"))
                .is_err()
        );
        Ok(Arc::new(FakeConnector {
            context,
            running: AtomicBool::new(false),
            reconciling: AtomicBool::new(false),
            next_id: AtomicU64::new(50),
            calls: AtomicU64::new(0),
            operation_queries: AtomicU64::new(0),
            journal: Mutex::new(HashMap::new()),
        }))
    }
}

fn definition(key: &str, id: u32) -> AccountDefinition {
    AccountDefinition {
        account_key: Arc::from(key),
        account_id: AccountId(id),
        connector_type: Arc::from("fake"),
        credential_ref: SecretRef::new("secret://account/main"),
        connector_config: Arc::from([]),
        instruments: Arc::from([AccountInstrumentBinding {
            native_symbol: Arc::from("BTCUSDT"),
            asset_id: AssetId(1001),
            price_tick: "0.1".parse().unwrap(),
            quantity_lot: "0.001".parse().unwrap(),
            contract_multiplier: "1".parse().unwrap(),
        }]),
        currencies: Arc::from([AccountCurrencyBinding {
            native_currency: Arc::from("USDT"),
            currency_id: CurrencyId(10),
            amount_unit: "0.00000001".parse().unwrap(),
        }]),
        ownership: OrderOwnershipPolicy::ManagedOnly {
            client_id_prefix: Arc::from("titan-"),
        },
        shutdown_order_policy: ShutdownOrderPolicy::LeaveOpen,
        enabled: true,
        definition_version: 1,
    }
}

fn spec() -> PluginSpec {
    PluginSpec {
        instance_id: Arc::from("account"),
        plugin_type: Arc::from(ACCOUNT_PLUGIN_TYPE),
        config: Arc::new(titan_plugin_engine::ConfigSnapshot::new(
            1,
            serde_json::json!({"account_plugin":{"max_accounts":2,"max_instruments_per_account":4,"max_currencies_per_account":4}}),
        )),
        enabled: true,
        execution: ExecutionSpec {
            model: ExecutionModel::Passive,
            cpu_affinity: None,
            callback_budget: None,
        },
        subscription_limits: SubscriptionLimits {
            max_capacity: 16,
            allowed_qos: BTreeSet::from([
                EventQos::ReliableOrdered,
                EventQos::BestEffort,
                EventQos::Latest,
            ]),
        },
        service_scopes: vec![
            (
                titan_plugin_engine::ServiceId::new("titan.account", "admin"),
                ServiceScope::Global,
            ),
            (
                titan_plugin_engine::ServiceId::new("titan.account", "query"),
                ServiceScope::Global,
            ),
            (
                titan_plugin_engine::ServiceId::new("titan.account", "execution"),
                ServiceScope::Global,
            ),
        ],
        required_service_scopes: vec![],
    }
}
fn key(name: &str) -> ServiceKey {
    ServiceKey {
        id: titan_plugin_engine::ServiceId::new("titan.account", name),
        version: Version::new(1, 0, 0),
        scope: ServiceScope::Global,
    }
}
fn admin(e: &PluginEngine, r: AccountAdminRequest) -> LocalResult<AccountAdminResponse> {
    *e.services()
        .bind(&key("admin"))
        .unwrap()
        .call(Box::new(r), TraceContext::default())
        .unwrap()
        .downcast::<LocalResult<AccountAdminResponse>>()
        .unwrap()
}
fn query(e: &PluginEngine, r: AccountRequest) -> LocalResult<AccountResponse> {
    *e.services()
        .bind(&key("query"))
        .unwrap()
        .call(Box::new(r), TraceContext::default())
        .unwrap()
        .downcast::<LocalResult<AccountResponse>>()
        .unwrap()
}
fn execution(
    e: &PluginEngine,
    r: AccountExecutionRequest,
) -> LocalResult<AccountExecutionResponse> {
    *e.services()
        .bind(&key("execution"))
        .unwrap()
        .call(
            Box::new(r),
            TraceContext {
                trace_id: 9,
                causation_id: 8,
            },
        )
        .unwrap()
        .downcast::<LocalResult<AccountExecutionResponse>>()
        .unwrap()
}

fn engine_with_plugin(event_engine: &EventEngine) -> PluginEngine {
    let mut p = PluginEngine::new(Arc::new(event_engine.handle()), ApiVersion::new(1, 0)).unwrap();
    p.register(
        Arc::new(
            AccountPluginFactory::new()
                .with_factory(Arc::new(FakeFactory))
                .with_secret_provider(Arc::new(TestSecrets)),
        ),
        Version::new(1, 0, 0),
        "test",
    )
    .unwrap();
    p.apply(&[spec()]).unwrap();
    p
}

#[cfg(unix)]
fn compile_dynamic_account_fixture(
    fill_v1: &[u8],
    fill_v2: &[u8],
    schema_version: u32,
) -> std::path::PathBuf {
    fn c_bytes(value: &[u8]) -> String {
        value
            .iter()
            .map(u8::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }

    let unique = format!(
        "titan-dynamic-account-fixture-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let directory = std::env::temp_dir().join(unique);
    std::fs::create_dir(&directory).unwrap();
    let source = directory.join("fixture.c");
    let library = if cfg!(target_os = "macos") {
        directory.join("libdynamic_account_fixture.dylib")
    } else {
        directory.join("libdynamic_account_fixture.so")
    };
    let source_text = r#"
#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "titan_plugin_abi_v1.h"

typedef struct TitanAccountHostApiV1 {
  uint32_t struct_size;
  void *context;
  TitanStatus (*publish_account)(void *, const uint8_t *, size_t, const uint8_t *, size_t, uint64_t, uint64_t);
  TitanStatus (*resolve_secret)(void *, const uint8_t *, size_t, TitanBuffer *);
} TitanAccountHostApiV1;

typedef TitanStatus (*TitanAccountJsonCall)(uint64_t, const uint8_t *, size_t, TitanBuffer *);
typedef struct TitanAccountConnectorFactoryApiV1 {
  uint64_t magic;
  uint32_t struct_size;
  uint16_t abi_major;
  uint16_t abi_minor;
  const uint8_t *(*connector_type)(TitanPluginHandle, size_t *);
  TitanStatus (*create)(TitanPluginHandle, const uint8_t *, size_t, const TitanAccountHostApiV1 *, uint64_t *);
  TitanStatus (*destroy)(uint64_t);
  TitanStatus (*start)(uint64_t);
  TitanStatus (*stop)(uint64_t, uint64_t);
  TitanAccountJsonCall submit;
  TitanAccountJsonCall amend;
  TitanAccountJsonCall cancel;
  TitanAccountJsonCall cancel_all;
  TitanAccountJsonCall cancel_all_after;
  TitanAccountJsonCall reconcile;
  TitanAccountJsonCall orders;
  TitanAccountJsonCall positions;
  TitanAccountJsonCall balances;
  TitanAccountJsonCall health;
  TitanAccountJsonCall diagnostics;
  TitanAccountJsonCall operation;
  size_t (*last_error)(uint8_t *, size_t);
} TitanAccountConnectorFactoryApiV1;

static const uint8_t MANIFEST[] = "{\"plugin_type\":\"dynamic-account-fixture\",\"name\":\"Dynamic Account Fixture\",\"version\":\"1.0.0\",\"engine_api_version\":{\"major\":2,\"minor\":0},\"abi_version\":{\"major\":1,\"minor\":0},\"config_schema_version\":1,\"config_schema\":{},\"provides\":[],\"requires\":[],\"publishes\":[],\"subscribes\":[],\"supported_execution_models\":[\"Passive\"],\"reload_policy\":\"Never\"}";
static const uint8_t CONNECTOR_TYPE[] = "dynamic-account-fixture";
static const uint8_t FILL_EVENT[] = "titan.account.Fill";
static const uint8_t FILL_V1[] = { __FILL_V1__ };
static const uint8_t FILL_V2[] = { __FILL_V2__ };
static const uint32_t TARGET_SCHEMA = __TARGET_SCHEMA__;
static const TitanAccountHostApiV1 *ACCOUNT_HOST = NULL;
static char LAST_ERROR[96] = "";

static const uint8_t *manifest_json(size_t *length) { *length = sizeof(MANIFEST) - 1; return MANIFEST; }
static TitanStatus root_create(const uint8_t *config, size_t length, TitanPluginHandle *out) {
  if (!config || !length || !out) return TITAN_STATUS_INVALID_ARGUMENT; *out = 7; return TITAN_STATUS_OK;
}
static TitanStatus root_handle(TitanPluginHandle handle) { return handle == 7 ? TITAN_STATUS_OK : TITAN_STATUS_INVALID_ARGUMENT; }
static TitanStatus root_start(TitanPluginHandle handle, const TitanHostApiV1 *host) { return handle == 7 && host ? TITAN_STATUS_OK : TITAN_STATUS_INVALID_ARGUMENT; }
static TitanStatus root_quiesce(TitanPluginHandle handle, uint32_t reason) { (void)reason; return root_handle(handle); }
static size_t last_error(uint8_t *output, size_t capacity) {
  size_t length = strlen(LAST_ERROR); if (length > capacity) length = capacity;
  if (output && length) memcpy(output, LAST_ERROR, length); return length;
}
static const uint8_t *connector_type(TitanPluginHandle handle, size_t *length) {
  if (handle != 7 || !length) return NULL; *length = sizeof(CONNECTOR_TYPE) - 1; return CONNECTOR_TYPE;
}
static TitanStatus account_create(TitanPluginHandle root, const uint8_t *input, size_t length, const TitanAccountHostApiV1 *host, uint64_t *out) {
  if (root != 7 || !input || !length || !host || !host->publish_account || !out) return TITAN_STATUS_INVALID_ARGUMENT;
  ACCOUNT_HOST = host; *out = 11; return TITAN_STATUS_OK;
}
static TitanStatus account_destroy(uint64_t handle) { ACCOUNT_HOST = NULL; return handle == 11 ? TITAN_STATUS_OK : TITAN_STATUS_INVALID_ARGUMENT; }
static void *publish_account_events(void *unused) {
  (void)unused;
  usleep(10000);
  TitanStatus status = TARGET_SCHEMA == 1
    ? ACCOUNT_HOST->publish_account(ACCOUNT_HOST->context, FILL_EVENT, sizeof(FILL_EVENT) - 1, FILL_V1, sizeof(FILL_V1), 101, 11)
    : ACCOUNT_HOST->publish_account(ACCOUNT_HOST->context, FILL_EVENT, sizeof(FILL_EVENT) - 1, FILL_V2, TARGET_SCHEMA == 0 ? sizeof(FILL_V2) - 1 : sizeof(FILL_V2), 102, 12);
  snprintf(LAST_ERROR, sizeof(LAST_ERROR), "publish status schema=%u status=%d", TARGET_SCHEMA, status);
  return NULL;
}
static TitanStatus account_start(uint64_t handle) {
  if (handle != 11 || !ACCOUNT_HOST) return TITAN_STATUS_INVALID_ARGUMENT;
  pthread_t thread;
  if (pthread_create(&thread, NULL, publish_account_events, NULL) != 0) return TITAN_STATUS_HOST_ERROR;
  pthread_detach(thread);
  return TITAN_STATUS_OK;
}
static TitanStatus account_stop(uint64_t handle, uint64_t timeout_ns) { (void)timeout_ns; return handle == 11 ? TITAN_STATUS_OK : TITAN_STATUS_INVALID_ARGUMENT; }
static TitanStatus unused_json(uint64_t handle, const uint8_t *input, size_t length, TitanBuffer *output) {
  (void)handle; (void)input; (void)length; (void)output; return TITAN_STATUS_INVALID_ARGUMENT;
}
static const TitanAccountConnectorFactoryApiV1 ACCOUNT_API = {
  UINT64_C(0x544954414e414343), sizeof(TitanAccountConnectorFactoryApiV1), 1, 0,
  connector_type, account_create, account_destroy, account_start, account_stop,
  unused_json, unused_json, unused_json, unused_json, unused_json, unused_json,
  unused_json, unused_json, unused_json, unused_json, unused_json, unused_json, last_error
};
static TitanStatus query_interface(TitanPluginHandle handle, const uint8_t *name, size_t length, uint16_t major, const void **out) {
  static const uint8_t expected[] = "titan.account.connector-factory";
  if (handle != 7 || major != 1 || !out || length != sizeof(expected) - 1 || memcmp(name, expected, length)) return TITAN_STATUS_INVALID_ARGUMENT;
  *out = &ACCOUNT_API; return TITAN_STATUS_OK;
}
static const PluginApiV1 API = {
  TITAN_PLUGIN_MAGIC, sizeof(PluginApiV1), TITAN_DYNAMIC_ABI_MAJOR, TITAN_DYNAMIC_ABI_MINOR,
  TITAN_MANIFEST_SCHEMA_MAJOR, TITAN_MANIFEST_SCHEMA_MINOR, 0, 0,
  manifest_json, root_create, root_handle, last_error, root_handle, root_start,
  root_quiesce, root_handle, query_interface
};
TITAN_PLUGIN_EXPORT const PluginApiV1 *titan_plugin_entry_v1(void) { return &API; }
"#
    .replace("__FILL_V1__", &c_bytes(fill_v1))
    .replace("__FILL_V2__", &c_bytes(fill_v2))
    .replace("__TARGET_SCHEMA__", &schema_version.to_string());
    std::fs::write(&source, source_text).unwrap();
    let mut compiler = std::process::Command::new(
        std::env::var_os("CC").unwrap_or_else(|| "cc".into()),
    );
    if cfg!(target_os = "macos") {
        compiler.arg("-dynamiclib");
    } else {
        compiler.args(["-shared", "-fPIC"]);
    }
    let status = compiler
        .arg("-I")
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/../titan-plugin-engine/include"))
        .arg("-O2")
        .arg(&source)
        .arg("-o")
        .arg(&library)
        .status()
        .unwrap();
    assert!(status.success());
    library
}

#[cfg(unix)]
fn dynamic_account_fill_crosses_host_abi(schema_version: u32) -> Option<Vec<u8>> {
    let header = |kind, version| AccountEventHeaderV1 {
        account_id: 2001,
        kind,
        account_generation: 1,
        account_epoch: 1,
        account_version: version,
        exchange_ts: 10,
        receive_ts: 11,
        ..Default::default()
    };
    let fill_v1 = FillV1 {
        header: header(event_kind::FILL, 1),
        asset_id: 1001,
        quantity_lots: 2,
        ..Default::default()
    };
    let fill_v2 = FillV2 {
        header: header(event_kind::FILL, 2),
        asset_id: 1001,
        last_fill_quantity_lots: 2,
        cumulative_filled_quantity_lots: 5,
        ..Default::default()
    };
    let mut encoded_v1 = vec![0; FillV1::ENCODED_LEN];
    let mut encoded_v2 = vec![0; FillV2::ENCODED_LEN];
    fill_v1.encode_into(&mut encoded_v1).unwrap();
    fill_v2.encode_into(&mut encoded_v2).unwrap();
    if schema_version == 3 {
        encoded_v2[4..6].copy_from_slice(&event_kind::POSITION_CHANGED.to_le_bytes());
    }
    let library = compile_dynamic_account_fixture(&encoded_v1, &encoded_v2, schema_version);
    let code = DynamicPluginLoader::default().load_library(&library).unwrap();
    let session = DynamicPluginSession::start(code, &serde_json::json!({})).unwrap();
    let factory = DynamicAccountConnectorFactory::from_session(session).unwrap();

    let mut config = EventEngineConfig::default();
    config.ingress.max_sources = 5_000;
    config.subscribers.default_capacity = 16;
    config.subscribers.critical_reserve = 2;
    let event_engine = EventEngine::new(config).unwrap();
    let events = event_engine.handle();
    let registered_schema = if schema_version == 0 || schema_version == 3 {
        FILL_EVENT_SCHEMA_VERSION
    } else {
        schema_version
    };
    events
        .register_event(FILL_EVENT, registered_schema, EventClass::Critical, PoolKind::SmallEvent)
        .unwrap();
    events
        .register_event(
            STREAM_INVALIDATED_EVENT,
            ACCOUNT_EVENT_SCHEMA_VERSION,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .unwrap();
    event_engine.start().unwrap();
    let mut plugins = PluginEngine::new(Arc::new(events.clone()), ApiVersion::new(1, 0)).unwrap();
    plugins
        .register(
            Arc::new(
                AccountPluginFactory::new()
                    .with_factory(Arc::new(factory))
                    .with_secret_provider(Arc::new(TestSecrets)),
            ),
            Version::new(1, 0, 0),
            "dynamic-test",
        )
        .unwrap();
    plugins.apply(&[spec()]).unwrap();
    let mut account_definition = definition("dynamic", 2001);
    account_definition.connector_type = Arc::from("dynamic-account-fixture");
    let account = match admin(
        &plugins,
        AccountAdminRequest::Create(account_definition),
    )
    .unwrap()
    {
        AccountAdminResponse::Handle(handle) => handle,
        _ => panic!("unexpected create response"),
    };
    let transaction = events.begin_route_update(events.current_route_version()).unwrap();
    for (event_type, event_schema_version) in [
        (FILL_EVENT, registered_schema),
        (STREAM_INVALIDATED_EVENT, ACCOUNT_EVENT_SCHEMA_VERSION),
    ] {
        events
            .stage_subscription(
                transaction,
                &PluginIdentity::new(
                    "test",
                    format!("dynamic-{event_type}-{event_schema_version}"),
                ),
                &SubscriptionSpec {
                    event_type: Arc::from(event_type),
                    schema_version: event_schema_version,
                    qos: EventQos::ReliableOrdered,
                    capacity: 8,
                    routing_keys: Arc::from([2001]),
                },
            )
            .unwrap();
    }
    let (_, subscriptions) = events.commit_at_safe_point(transaction).unwrap();
    admin(&plugins, AccountAdminRequest::Start(account)).unwrap();

    let (sender, receiver) = std::sync::mpsc::channel();
    let handler = RecordingHandler(sender);
    let deadline = Instant::now()
        + if schema_version == 0 || schema_version == 3 {
            Duration::from_millis(200)
        } else {
            Duration::from_secs(3)
        };
    let mut delivered = Vec::new();
    while delivered.is_empty() && Instant::now() < deadline {
        for subscription in &subscriptions {
            let _ = subscription
                .receiver
                .dispatch_next(&handler, Duration::from_millis(10));
        }
        delivered.extend(receiver.try_iter());
    }
    if schema_version == 0 || schema_version == 3 {
        assert!(delivered.is_empty());
        let health = match query(&plugins, AccountRequest::Health(account)).unwrap() {
            AccountResponse::Health(health) => health,
            _ => panic!("unexpected health response"),
        };
        assert!(health.message.contains("payload length does not match"));
        assert!(health.message.contains("account_id=2001"));
        assert!(health.message.contains("event_type=titan.account.Fill"));
        assert!(health.message.contains("schema_version="));
        assert!(health.message.contains("payload_len="));
    } else if delivered.len() != 1 {
        let health = match query(&plugins, AccountRequest::Health(account)).unwrap() {
            AccountResponse::Health(health) => health,
            _ => panic!("unexpected health response"),
        };
        panic!(
            "expected one dynamic Fill event, received {}; connector health: {}",
            delivered.len(), health.message
        );
    }
    assert!(delivered.iter().all(|(event_type, _)| event_type == FILL_EVENT));
    let payload = delivered.pop().map(|(_, payload)| payload);
    assert!(subscriptions.iter().all(|subscription| {
        subscription
            .receiver
            .dispatch_next(&handler, Duration::from_millis(1))
            .unwrap()
            != DispatchOutcome::Delivered
    }));

    plugins.shutdown(StopReason::Shutdown).unwrap();
    event_engine.stop().unwrap();
    payload
}

#[cfg(unix)]
#[test]
fn dynamic_account_fill_v2_crosses_host_abi_with_schema_v2() {
    let v2 = dynamic_account_fill_crosses_host_abi(FILL_EVENT_SCHEMA_VERSION).unwrap();
    let v2 = FillV2::decode(&v2).unwrap();
    assert_eq!(v2.last_fill_quantity_lots, 2);
    assert_eq!(v2.cumulative_filled_quantity_lots, 5);
}

#[cfg(unix)]
#[test]
fn dynamic_account_fill_v1_remains_backward_compatible() {
    let v1 = dynamic_account_fill_crosses_host_abi(ACCOUNT_EVENT_SCHEMA_VERSION).unwrap();
    assert_eq!(FillV1::decode(&v1).unwrap().quantity_lots, 2);
}

#[cfg(unix)]
#[test]
fn dynamic_account_rejects_unknown_fill_payload_layout() {
    assert!(dynamic_account_fill_crosses_host_abi(0).is_none());
}

#[cfg(unix)]
#[test]
fn dynamic_account_rejects_fill_payload_with_mismatched_event_kind() {
    assert!(dynamic_account_fill_crosses_host_abi(3).is_none());
}

#[test]
fn decimal_units_and_abi_are_exact_and_little_endian() {
    assert_eq!(
        "0.0010".parse::<DecimalUnit>().unwrap().to_string(),
        "0.001"
    );
    assert!("0.0000000000000000001".parse::<DecimalUnit>().is_err());
    let event = OrderChangedV1 {
        header: AccountEventHeaderV1 {
            account_id: 7,
            kind: event_kind::ORDER_CHANGED,
            account_generation: 9,
            account_epoch: 3,
            account_version: 4,
            exchange_ts: -5,
            receive_ts: 6,
            ..Default::default()
        },
        asset_id: 1001,
        price_ticks: -2,
        quantity_lots: 3,
        client_order_id: Id128([1; 16]),
        ..Default::default()
    };
    let mut bytes = vec![0; OrderChangedV1::ENCODED_LEN];
    event.encode_into(&mut bytes).unwrap();
    assert_eq!(&bytes[..4], &7u32.to_le_bytes());
    assert_eq!(&bytes[56..64], &(-2i64).to_le_bytes());
    assert_eq!(OrderChangedV1::decode(&bytes).unwrap(), event);
    let pos = PositionChangedV1::default();
    let mut p = vec![0; PositionChangedV1::ENCODED_LEN];
    pos.encode_into(&mut p).unwrap();
    assert_eq!(PositionChangedV1::decode(&p).unwrap(), pos);
}

struct RecordingHandler(std::sync::mpsc::Sender<(String, Vec<u8>)>);
impl EventHandler for RecordingHandler {
    fn handle(&self, e: EventView<'_>) -> Result<(), PluginError> {
        self.0
            .send((e.event_type.to_string(), e.payload.to_vec()))
            .unwrap();
        Ok(())
    }
}

struct TraceRecordingHandler(std::sync::mpsc::Sender<(String, TraceContext)>);
impl EventHandler for TraceRecordingHandler {
    fn handle(&self, event: EventView<'_>) -> Result<(), PluginError> {
        self.0
            .send((event.event_type.to_string(), event.trace))
            .unwrap();
        Ok(())
    }
}

#[test]
fn plugin_services_direct_events_generation_and_snapshots_work() {
    let mut c = EventEngineConfig::default();
    c.ingress.max_sources = 5000;
    c.subscribers.default_capacity = 16;
    c.subscribers.critical_reserve = 2;
    let ee = EventEngine::new(c).unwrap();
    let h = ee.handle();
    for event_type in ACCOUNT_EVENT_TYPES {
        let schema_version = if event_type == FILL_EVENT {
            FILL_EVENT_SCHEMA_VERSION
        } else {
            ACCOUNT_EVENT_SCHEMA_VERSION
        };
        h.register_event(
            event_type,
            schema_version,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .unwrap();
    }
    ee.start().unwrap();
    let tx = h.begin_route_update(h.current_route_version()).unwrap();
    h.stage_subscription(
        tx,
        &PluginIdentity::new("test", "consumer"),
        &SubscriptionSpec {
            event_type: Arc::from(ORDER_CHANGED_EVENT),
            schema_version: 1,
            qos: EventQos::ReliableOrdered,
            capacity: 8,
            routing_keys: Arc::from([2001]),
        },
    )
    .unwrap();
    let (_, mut subscriptions) = h.commit_at_safe_point(tx).unwrap();
    let receiver = subscriptions.pop().unwrap().receiver;
    let (out_tx, out_rx) = std::sync::mpsc::channel();
    let consumer = std::thread::spawn(move || {
        let handler = RecordingHandler(out_tx);
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if receiver
                .dispatch_next(&handler, Duration::from_millis(10))
                .unwrap()
                == DispatchOutcome::Delivered
            {
                return;
            }
        }
        panic!("account event not delivered")
    });
    let mut pe = engine_with_plugin(&ee);
    assert!(
        matches!(admin(&pe,AccountAdminRequest::List).unwrap(),AccountAdminResponse::Accounts(v) if v.is_empty())
    );
    let handle = match admin(&pe, AccountAdminRequest::Create(definition("main", 2001))).unwrap() {
        AccountAdminResponse::Handle(h) => h,
        _ => panic!(),
    };
    assert!(
        matches!(query(&pe,AccountRequest::Resolve(Arc::from("main"))).unwrap(),AccountResponse::Handle(h) if h==handle)
    );
    admin(&pe, AccountAdminRequest::Start(handle)).unwrap();
    let (event_type, payload) = out_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(event_type, ORDER_CHANGED_EVENT);
    assert_eq!(
        OrderChangedV1::decode(&payload)
            .unwrap()
            .header
            .account_generation,
        handle.generation
    );
    consumer.join().unwrap();
    let command = SubmitOrderCommand {
        command_id: Id128([1; 16]),
        client_order_id: Some(Id128([2; 16])),
        asset_id: AssetId(1001),
        side: 1,
        order_type: 1,
        time_in_force: 1,
        price_ticks: 10,
        quantity_lots: 2,
        trace: TraceContext::default(),
    };
    let receipt = execution(
        &pe,
        AccountExecutionRequest::Submit(handle, command.clone()),
    )
    .unwrap()
    .0;
    assert_eq!(receipt.account, handle);
    assert_eq!(
        execution(
            &pe,
            AccountExecutionRequest::Submit(handle, command.clone())
        )
        .unwrap()
        .0,
        receipt
    );
    let mut conflict = command;
    conflict.price_ticks = 11;
    assert_eq!(
        execution(&pe, AccountExecutionRequest::Submit(handle, conflict))
            .unwrap_err()
            .kind,
        AccountErrorKind::CommandConflict
    );
    let op = match admin(
        &pe,
        AccountAdminRequest::Reconcile(handle, ReconcileScope::Full),
    )
    .unwrap()
    {
        AccountAdminResponse::OperationId(id) => id,
        _ => panic!(),
    };
    assert!(
        matches!(admin(&pe,AccountAdminRequest::Operation(op)).unwrap(),AccountAdminResponse::Operation(v) if v.state==OperationState::Pending)
    );
    assert!(
        matches!(query(&pe,AccountRequest::Orders(handle,OrderFilter::default())).unwrap(),AccountResponse::Orders(v) if v.state==AccountSnapshotState::Reconciling && v.committed_epoch==Some(1))
    );
    assert!(
        matches!(admin(&pe,AccountAdminRequest::Operation(op)).unwrap(),AccountAdminResponse::Operation(v) if v.state==OperationState::Succeeded)
    );
    let mut active_replacement = definition("main", 2001);
    active_replacement.definition_version = 2;
    let active_replaced = match admin(
        &pe,
        AccountAdminRequest::Replace(handle, active_replacement),
    )
    .unwrap()
    {
        AccountAdminResponse::Handle(h) => h,
        _ => panic!(),
    };
    assert!(active_replaced.generation > handle.generation);
    assert_eq!(
        query(&pe, AccountRequest::Health(handle)).unwrap_err().kind,
        AccountErrorKind::StaleHandle
    );
    admin(
        &pe,
        AccountAdminRequest::Stop(active_replaced, Instant::now() + Duration::from_secs(1)),
    )
    .unwrap();
    admin(&pe, AccountAdminRequest::Remove(active_replaced)).unwrap();
    let recreated = match admin(&pe, AccountAdminRequest::Create(definition("main", 2001))).unwrap()
    {
        AccountAdminResponse::Handle(h) => h,
        _ => panic!(),
    };
    assert!(recreated.generation > active_replaced.generation);
    assert_eq!(
        query(&pe, AccountRequest::Health(handle)).unwrap_err().kind,
        AccountErrorKind::StaleHandle
    );
    let mut replacement = definition("main", 2001);
    replacement.definition_version = 2;
    let replaced = match admin(&pe, AccountAdminRequest::Replace(recreated, replacement)).unwrap() {
        AccountAdminResponse::Handle(h) => h,
        _ => panic!(),
    };
    assert!(replaced.generation > recreated.generation);
    pe.shutdown(StopReason::Shutdown).unwrap();
    ee.stop().unwrap();
}

#[test]
fn execution_service_reaches_connector_and_returns_all_account_facts_with_trace() {
    let mut config = EventEngineConfig::default();
    config.ingress.max_sources = 5_000;
    config.subscribers.default_capacity = 16;
    config.subscribers.critical_reserve = 2;
    let event_engine = EventEngine::new(config).unwrap();
    let events = event_engine.handle();
    for event_type in ACCOUNT_EVENT_TYPES {
        let schema_version = if event_type == FILL_EVENT {
            FILL_EVENT_SCHEMA_VERSION
        } else {
            ACCOUNT_EVENT_SCHEMA_VERSION
        };
        events
            .register_event(
                event_type,
                schema_version,
                EventClass::Critical,
                PoolKind::SmallEvent,
            )
            .unwrap();
    }
    event_engine.start().unwrap();
    let mut plugins = engine_with_plugin(&event_engine);
    let account = match admin(
        &plugins,
        AccountAdminRequest::Create(definition("facts", 2001)),
    )
    .unwrap()
    {
        AccountAdminResponse::Handle(handle) => handle,
        _ => panic!("unexpected create response"),
    };
    let start = match admin(&plugins, AccountAdminRequest::Start(account)).unwrap() {
        AccountAdminResponse::OperationId(id) => id,
        _ => panic!("unexpected start response"),
    };
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match admin(&plugins, AccountAdminRequest::Operation(start)).unwrap() {
            AccountAdminResponse::Operation(snapshot)
                if snapshot.state == OperationState::Succeeded =>
            {
                break;
            }
            AccountAdminResponse::Operation(snapshot)
                if snapshot.state == OperationState::Failed =>
            {
                panic!("account start failed: {}", snapshot.detail);
            }
            AccountAdminResponse::Operation(_) => {
                assert!(Instant::now() < deadline, "account start timed out");
                std::thread::sleep(Duration::from_millis(1));
            }
            _ => panic!("unexpected operation response"),
        }
    }

    let transaction = events
        .begin_route_update(events.current_route_version())
        .unwrap();
    for (event_type, schema_version) in [
        (ORDER_CHANGED_EVENT, ACCOUNT_EVENT_SCHEMA_VERSION),
        (FILL_EVENT, FILL_EVENT_SCHEMA_VERSION),
        (POSITION_CHANGED_EVENT, ACCOUNT_EVENT_SCHEMA_VERSION),
        (BALANCE_CHANGED_EVENT, ACCOUNT_EVENT_SCHEMA_VERSION),
    ] {
        events
            .stage_subscription(
                transaction,
                &PluginIdentity::new("strategy", "account-facts"),
                &SubscriptionSpec {
                    event_type: Arc::from(event_type),
                    schema_version,
                    qos: EventQos::ReliableOrdered,
                    capacity: 8,
                    routing_keys: Arc::from([u64::from(account.account_id.0)]),
                },
            )
            .unwrap();
    }
    let (_, subscriptions) = events.commit_at_safe_point(transaction).unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let consumers = subscriptions
        .into_iter()
        .map(|subscription| {
            let sender = sender.clone();
            std::thread::spawn(move || {
                let handler = TraceRecordingHandler(sender);
                let deadline = Instant::now() + Duration::from_secs(3);
                loop {
                    if subscription
                        .receiver
                        .dispatch_next(&handler, Duration::from_millis(10))
                        .unwrap()
                        == DispatchOutcome::Delivered
                    {
                        return;
                    }
                    assert!(Instant::now() < deadline, "account fact was not delivered");
                }
            })
        })
        .collect::<Vec<_>>();
    drop(sender);

    let command = SubmitOrderCommand {
        command_id: Id128([9; 16]),
        client_order_id: Some(Id128([10; 16])),
        asset_id: AssetId(1001),
        side: 1,
        order_type: 1,
        time_in_force: 1,
        price_ticks: 20,
        quantity_lots: 3,
        trace: TraceContext::default(),
    };
    execution(&plugins, AccountExecutionRequest::Submit(account, command)).unwrap();

    let mut delivered = receiver.iter().collect::<Vec<_>>();
    for consumer in consumers {
        consumer.join().unwrap();
    }
    delivered.sort_by(|left, right| left.0.cmp(&right.0));
    assert!(delivered.iter().all(|(_, trace)| {
        *trace
            == TraceContext {
                trace_id: 9,
                causation_id: 8,
            }
    }));
    assert_eq!(
        delivered
            .iter()
            .map(|(event_type, _)| event_type.as_str())
            .collect::<BTreeSet<_>>(),
        [
            ORDER_CHANGED_EVENT,
            FILL_EVENT,
            POSITION_CHANGED_EVENT,
            BALANCE_CHANGED_EVENT,
        ]
        .into_iter()
        .collect()
    );

    plugins.shutdown(StopReason::Shutdown).unwrap();
    event_engine.stop().unwrap();
    assert_eq!(event_engine.arena().outstanding_blocks(), 0);
}

#[test]
fn validation_capacity_redaction_and_error_passthrough_work() {
    assert_eq!(
        format!("{:?}", SecretRef::new("secret://sensitive/path")),
        "SecretRef(REDACTED)"
    );
    let ee = EventEngine::new(EventEngineConfig::default()).unwrap();
    for event_type in ACCOUNT_EVENT_TYPES {
        let schema_version = if event_type == FILL_EVENT {
            FILL_EVENT_SCHEMA_VERSION
        } else {
            ACCOUNT_EVENT_SCHEMA_VERSION
        };
        ee.handle()
            .register_event(
                event_type,
                schema_version,
                EventClass::Critical,
                PoolKind::SmallEvent,
            )
            .unwrap();
    }
    ee.start().unwrap();
    let mut pe = engine_with_plugin(&ee);
    let mut invalid = definition("bad", 1);
    invalid.instruments = Arc::from([
        invalid.instruments[0].clone(),
        invalid.instruments[0].clone(),
    ]);
    assert_eq!(
        admin(&pe, AccountAdminRequest::Create(invalid))
            .unwrap_err()
            .kind,
        AccountErrorKind::InvalidDefinition
    );
    let h1 = match admin(&pe, AccountAdminRequest::Create(definition("one", 1))).unwrap() {
        AccountAdminResponse::Handle(h) => h,
        _ => panic!(),
    };
    let mut duplicate = definition("two", 1);
    duplicate.account_id = AccountId(1);
    assert_eq!(
        admin(&pe, AccountAdminRequest::Create(duplicate))
            .unwrap_err()
            .kind,
        AccountErrorKind::AlreadyExists
    );
    let h2 = match admin(&pe, AccountAdminRequest::Create(definition("two", 2))).unwrap() {
        AccountAdminResponse::Handle(h) => h,
        _ => panic!(),
    };
    assert_eq!(
        admin(&pe, AccountAdminRequest::Create(definition("three", 3)))
            .unwrap_err()
            .kind,
        AccountErrorKind::CapacityExceeded
    );
    assert_eq!(
        execution(
            &pe,
            AccountExecutionRequest::Submit(
                h1,
                SubmitOrderCommand {
                    command_id: Id128([3; 16]),
                    client_order_id: None,
                    asset_id: AssetId(1001),
                    side: 1,
                    order_type: 1,
                    time_in_force: 1,
                    price_ticks: 1,
                    quantity_lots: 1,
                    trace: TraceContext::default()
                }
            )
        )
        .unwrap_err()
        .kind,
        AccountErrorKind::NotReady
    );
    admin(&pe, AccountAdminRequest::Start(h1)).unwrap();
    admin(&pe, AccountAdminRequest::Start(h2)).unwrap();
    let reconcile_id = |account| match admin(
        &pe,
        AccountAdminRequest::Reconcile(account, ReconcileScope::Full),
    )
    .unwrap()
    {
        AccountAdminResponse::OperationId(id) => id,
        _ => panic!(),
    };
    let first_operation = reconcile_id(h1);
    let second_operation = reconcile_id(h2);
    assert_ne!(first_operation, second_operation);
    assert!(matches!(
        admin(&pe, AccountAdminRequest::Operation(first_operation)).unwrap(),
        AccountAdminResponse::Operation(value) if value.state == OperationState::Pending
    ));
    let q = SubmitOrderCommand {
        command_id: Id128([255; 16]),
        client_order_id: None,
        asset_id: AssetId(1001),
        side: 1,
        order_type: 1,
        time_in_force: 1,
        price_ticks: 1,
        quantity_lots: 1,
        trace: TraceContext::default(),
    };
    assert_eq!(
        execution(&pe, AccountExecutionRequest::Submit(h1, q))
            .unwrap_err()
            .kind,
        AccountErrorKind::QueueFull
    );
    pe.shutdown(StopReason::Shutdown).unwrap();
    ee.stop().unwrap();
}

#[test]
fn duplicate_factory_is_rejected() {
    let core = AccountPluginCore::new(AccountPluginConfig::default());
    core.register_factory(Arc::new(FakeFactory)).unwrap();
    assert_eq!(
        core.register_factory(Arc::new(FakeFactory))
            .unwrap_err()
            .kind,
        AccountErrorKind::AlreadyExists
    );
}
