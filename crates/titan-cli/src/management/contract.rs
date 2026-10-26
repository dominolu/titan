use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

pub const API_VERSION: u32 = 1;

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
        template_id: String,
        #[ts(type = "unknown")]
        parameters: Value,
    },
    Build {
        version_id: String,
    },
    Prepare {
        dataset_id: String,
    },
    Backtest {
        artifact_id: String,
        dataset_id: String,
        stress: bool,
    },
    Report {
        backtest_id: String,
    },
    Deploy {
        artifact_id: String,
        binding_id: String,
    },
    Start {
        deployment_id: String,
    },
    Pause {
        run_id: String,
        generation: String,
    },
    Resume {
        run_id: String,
        generation: String,
    },
    Stop {
        run_id: String,
        generation: String,
    },
    Recover {
        run_id: String,
        generation: String,
        checkpoint_id: String,
    },
}

impl Action {
    pub fn environment(&self) -> &'static str {
        match self {
            Self::Describe { .. }
            | Self::Build { .. }
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
            | Self::Recover {
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

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
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

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct ResearchWorkspace {
    pub id: String,
    pub name: String,
    pub template_count: usize,
    pub resource_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct SaveVersion {
    pub node_id: String,
    pub template_id: String,
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
        Operation::decl(&config),
        Resource::decl(&config),
        ResearchWorkspace::decl(&config),
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
