use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

pub const API_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Reader,
    Operator,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct Identity {
    pub username: String,
    pub role: Role,
    pub nodes: Vec<String>,
    pub accounts: Vec<String>,
    pub environments: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct Session {
    pub identity: Identity,
    pub csrf_token: String,
    pub expires_at_ns: String,
    pub api_version: u32,
    pub build: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Unknown,
}

impl OperationState {
    pub fn terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    Describe {
        #[serde(alias = "template_id")]
        parameter_preset_id: String,
        #[ts(type = "unknown")]
        parameters: Value,
    },
    #[serde(alias = "build")]
    Compile {
        #[serde(alias = "version_id")]
        parameter_version_id: String,
    },
    Prepare {
        dataset_id: String,
    },
    Backtest {
        #[serde(alias = "artifact_id")]
        strategy_version_id: String,
        dataset_id: String,
        stress: bool,
    },
    Report {
        backtest_id: String,
    },
    Deploy {
        #[serde(alias = "artifact_id")]
        strategy_version_id: String,
        binding_id: String,
    },
    Start {
        deployment_id: String,
    },
    Pause {
        run_id: String,
        #[serde(rename = "expected_run_generation", alias = "generation")]
        generation: String,
    },
    Resume {
        run_id: String,
        #[serde(rename = "expected_run_generation", alias = "generation")]
        generation: String,
    },
    Stop {
        run_id: String,
        #[serde(rename = "expected_run_generation", alias = "generation")]
        generation: String,
    },
    #[serde(alias = "recover")]
    Restore {
        run_id: String,
        #[serde(rename = "expected_run_generation", alias = "generation")]
        generation: String,
        #[serde(rename = "recovery_point_sha256", alias = "checkpoint_id")]
        checkpoint_id: String,
    },
}

impl Action {
    pub fn environment(&self) -> &'static str {
        match self {
            Self::Describe { .. }
            | Self::Compile { .. }
            | Self::Prepare { .. }
            | Self::Backtest { .. }
            | Self::Report { .. } => "backtest",
            _ => "live",
        }
    }
    pub fn run(&self) -> Option<(&str, &str)> {
        match self {
            Self::Pause { run_id, generation }
            | Self::Resume { run_id, generation }
            | Self::Stop { run_id, generation }
            | Self::Restore {
                run_id, generation, ..
            } => Some((run_id, generation)),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct SubmitOperation {
    pub idempotency_key: String,
    pub node_id: String,
    pub action: Action,
}

/// Public v2 request. The optional strategy hint is checked against the fixed
/// action reference before using the existing authorization/admission path.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct StrategyOperationRequest {
    pub idempotency_key: String,
    pub node_id: String,
    #[ts(type = "Action & { strategy_id?: string }")]
    pub action: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct Operation {
    pub id: String,
    pub actor: String,
    pub node_id: String,
    pub input: Action,
    pub account: Option<String>,
    pub input_digest: String,
    pub state: OperationState,
    pub cancel_requested: bool,
    pub created_at_ns: String,
    pub updated_at_ns: String,
    #[ts(type = "unknown")]
    pub result: Option<Value>,
    pub error: Option<String>,
    pub revision: String,
}

#[derive(Debug, Clone, Serialize, TS)]
pub struct Resource {
    pub id: String,
    pub kind: String,
    pub node_id: String,
    pub revision: String,
    pub frozen: bool,
    pub digest: String,
    #[ts(type = "unknown")]
    pub body: Value,
}

impl<'de> Deserialize<'de> for Resource {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct StoredResource {
            id: String,
            kind: String,
            node_id: String,
            revision: String,
            frozen: bool,
            digest: String,
            body: Value,
        }
        let value = StoredResource::deserialize(deserializer)?;
        Ok(super::legacy::resource(Self {
            id: value.id,
            kind: value.kind,
            node_id: value.node_id,
            revision: value.revision,
            frozen: value.frozen,
            digest: value.digest,
            body: value.body,
        }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct StrategySummary {
    pub id: String,
    pub name: String,
    pub parameter_preset_count: usize,
    pub resource_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct SaveVersion {
    pub node_id: String,
    pub parameter_preset_id: String,
    #[ts(type = "unknown")]
    pub parameters: Value,
    pub expected_revision: Option<String>,
    pub freeze_operation_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct Snapshot {
    pub api_version: u32,
    pub cursor: String,
    pub server_time_ns: String,
    pub operations: Vec<Operation>,
    pub resources: Vec<Resource>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct Event {
    pub cursor: String,
    pub resource_id: String,
    pub kind: String,
    pub revision: String,
    #[ts(type = "unknown")]
    pub payload: Value,
}

/// Server-generated fixed job. Never accepts arbitrary command lines or paths from the browser.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeRequest {
    pub api_version: u32,
    pub operation_id: String,
    pub input_digest: String,
    pub action: Action,
    pub resources: Vec<Resource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Receipt {
    pub operation_id: String,
    pub input_digest: String,
    pub state: OperationState,
    pub result: Option<Value>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct ApiError {
    pub code: String,
    pub message: String,
    pub request_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct StrategyRunSnapshot {
    pub strategy_id: String,
    pub strategy_version_id: Option<String>,
    pub run_id: String,
    pub run_generation: Option<String>,
    pub revision: String,
    pub sampled_at_ns: Option<String>,
    pub available: bool,
    #[ts(type = "unknown")]
    pub runtime: Value,
    #[ts(type = "unknown")]
    pub account: Value,
    #[ts(type = "unknown")]
    pub financial: Value,
    #[ts(type = "unknown")]
    pub risk: Value,
    #[ts(type = "unknown")]
    pub business_observation: Value,
    #[ts(type = "unknown")]
    pub recovery: Value,
}

pub fn export_types(directory: &std::path::Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(directory)?;
    let mut source = String::from("// Generated from Rust management DTOs. Do not edit.\n");
    source.push_str(&format!(
        "export const API_VERSION = {API_VERSION};\nexport const EXPECTED_BUILD = {:?};\n",
        super::build_id()
    ));
    let config = ts_rs::Config::default();
    for definition in [
        Role::decl(&config),
        Identity::decl(&config),
        Session::decl(&config),
        OperationState::decl(&config),
        Action::decl(&config),
        SubmitOperation::decl(&config),
        StrategyOperationRequest::decl(&config),
        Operation::decl(&config),
        Resource::decl(&config),
        StrategySummary::decl(&config),
        StrategyRunSnapshot::decl(&config),
        SaveVersion::decl(&config),
        Snapshot::decl(&config),
        Event::decl(&config),
        ApiError::decl(&config),
    ] {
        source.push_str("export ");
        source.push_str(&definition);
        source.push('\n');
    }
    source.push_str("export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };\n");
    std::fs::write(directory.join("management.ts"), source)?;
    Ok(())
}
