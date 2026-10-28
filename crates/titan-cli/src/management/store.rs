use super::{contract::*, digest, fault, nonce, now_ns};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};

// Only fields needed for authorization and immutable lineage. Large metadata,
// parameters, metrics and runtime observations never enter the strategy scan.
const STRATEGY_PROJECTION: &str = "json_object(
    'id',id,'kind',kind,'node_id',node_id,'revision','','frozen',json('false'),'digest','',
    'body',json_object(
        'environment',json_extract(data,'$.body.environment'),
        'account',json_extract(data,'$.body.account'),
        'mode',json_extract(data,'$.body.mode'),
        'strategy_id',COALESCE(json_extract(data,'$.body.research_id'),json_extract(data,'$.body.strategy_id')),
        'strategy_name',COALESCE(json_extract(data,'$.body.strategy_name'),json_extract(data,'$.body.research_name')),
        'source_digest',json_extract(data,'$.body.source_digest'),
        'parameter_preset_id',COALESCE(json_extract(data,'$.body.parameter_preset_id'),json_extract(data,'$.body.template_id')),
        'parameter_version_id',COALESCE(json_extract(data,'$.body.parameter_version_id'),json_extract(data,'$.body.version_id')),
        'strategy_version_id',COALESCE(json_extract(data,'$.body.strategy_version_id'),json_extract(data,'$.body.artifact_id')),
        'backtest_id',json_extract(data,'$.body.backtest_id'),
        'deployment_id',json_extract(data,'$.body.deployment_id')
    ))";

pub(crate) struct Store {
    db: Mutex<Connection>,
    path: PathBuf,
    pub epoch: String,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Connection::open(path)?;
        db.busy_timeout(Duration::from_secs(5))?;
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "synchronous", "FULL")?;
        let version: i32 = db.pragma_query_value(None, "user_version", |r| r.get(0))?;
        ensure!(
            version <= 1,
            "management database is newer than this binary"
        );
        db.execute_batch("BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
            INSERT OR IGNORE INTO meta VALUES ('revision',0);
            CREATE TABLE IF NOT EXISTS operations (
                id TEXT PRIMARY KEY, actor TEXT NOT NULL, key TEXT NOT NULL, payload_digest TEXT NOT NULL,
                node_id TEXT NOT NULL, data TEXT NOT NULL, request TEXT NOT NULL, UNIQUE(actor,key));
            CREATE TABLE IF NOT EXISTS resources (id TEXT PRIMARY KEY, kind TEXT NOT NULL, node_id TEXT NOT NULL, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events (revision INTEGER PRIMARY KEY, node_id TEXT NOT NULL, environment TEXT NOT NULL, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS audit (id INTEGER PRIMARY KEY, at_ns TEXT NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL, target TEXT NOT NULL, digest TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS sessions (token_hash TEXT PRIMARY KEY, username TEXT NOT NULL, csrf_hash TEXT NOT NULL, expires_ns TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS occupancy (binding TEXT PRIMARY KEY, operation_id TEXT NOT NULL, run_id TEXT, node_id TEXT NOT NULL);
            PRAGMA user_version=1;
            COMMIT;")?;
        Ok(Self {
            db: Mutex::new(db),
            path: path.canonicalize()?,
            epoch: nonce(),
        })
    }
    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.db
            .lock()
            .map_err(|_| anyhow::anyhow!("management store lock poisoned"))
    }
    fn strategy_connection(&self) -> Result<Connection> {
        // WAL readers use their own snapshot, never the admission/write mutex.
        let db = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        db.busy_timeout(Duration::from_secs(5))?;
        Ok(db)
    }
    pub fn audit(&self, actor: &str, action: &str, target: &str, input: &str) -> Result<()> {
        self.connection()?.execute(
            "INSERT INTO audit(at_ns,actor,action,target,digest) VALUES (?1,?2,?3,?4,?5)",
            params![now_ns(), actor, action, target, input],
        )?;
        Ok(())
    }
    pub fn session_create(
        &self,
        username: &str,
        token: &str,
        csrf: &str,
        expires: &str,
    ) -> Result<()> {
        let db = self.connection()?;
        db.execute(
            "DELETE FROM sessions WHERE CAST(expires_ns AS INTEGER)<CAST(?1 AS INTEGER)",
            [now_ns()],
        )?;
        let count: i64 = db.query_row("SELECT COUNT(*) FROM sessions", [], |r| r.get(0))?;
        ensure!(count < 4096, "session capacity exceeded");
        db.execute(
            "INSERT INTO sessions VALUES (?1,?2,?3,?4)",
            params![
                digest(token.as_bytes()),
                username,
                digest(csrf.as_bytes()),
                expires
            ],
        )?;
        Ok(())
    }
    pub fn session(&self, token: &str) -> Result<Option<(String, String, String)>> {
        Ok(self.connection()?.query_row("SELECT username,csrf_hash,expires_ns FROM sessions WHERE token_hash=?1 AND CAST(expires_ns AS INTEGER)>CAST(?2 AS INTEGER)",params![digest(token.as_bytes()),now_ns()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?)
    }
    pub fn session_remove(&self, token: &str) -> Result<()> {
        self.connection()?.execute(
            "DELETE FROM sessions WHERE token_hash=?1",
            [digest(token.as_bytes())],
        )?;
        Ok(())
    }
    pub fn by_key(&self, actor: &str, key: &str) -> Result<Option<(Operation, String)>> {
        let db = self.connection()?;
        let row: Option<(String, String)> = db
            .query_row(
                "SELECT data,payload_digest FROM operations WHERE actor=?1 AND key=?2",
                params![actor, key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        row.map(|(data, digest)| Ok((serde_json::from_str(&data)?, digest)))
            .transpose()
    }
    pub fn accept(
        &self,
        actor: &str,
        input: &SubmitOperation,
        resources: Vec<Resource>,
        payload_digest: &str,
        occupancy: Option<&str>,
    ) -> Result<Operation> {
        let mut db = self.connection()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if let Some((data, hash)) = tx
            .query_row(
                "SELECT data,payload_digest FROM operations WHERE actor=?1 AND key=?2",
                params![actor, input.idempotency_key],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
            )
            .optional()?
        {
            let old: Operation = serde_json::from_str(&data)?;
            if hash != payload_digest && hash != super::legacy::request_digest(input)? {
                return Err(fault(
                    409,
                    "IDEMPOTENCY_CONFLICT",
                    "same idempotency key has different input",
                ));
            }
            return Ok(old);
        }
        // Stop has its own bounded admission budget; ordinary/unknown work cannot consume it.
        let stopping = matches!(input.action, Action::Stop { .. });
        let active: i64 = tx.query_row("SELECT COUNT(*) FROM operations WHERE json_extract(data,'$.state') IN ('pending','running','unknown') AND (json_extract(data,'$.input.kind')='stop')=?1",[stopping],|r|r.get(0))?;
        ensure!(active < 256, "operation capacity exceeded");
        if let Some((run, generation)) = input.action.run() {
            let conflicting: i64 = tx.query_row("SELECT COUNT(*) FROM operations WHERE node_id=?1 AND json_extract(data,'$.input.run_id')=?2 AND COALESCE(json_extract(data,'$.input.expected_run_generation'),json_extract(data,'$.input.generation'))=?3 AND json_extract(data,'$.state') IN ('pending','running','unknown')",params![input.node_id,run,generation],|r|r.get(0))?;
            if conflicting != 0 && !matches!(input.action, Action::Stop { .. }) {
                return Err(fault(
                    409,
                    "RUN_BUSY",
                    "another operation is active for this run",
                ));
            }
        }
        let id = nonce();
        if let Some(binding) = occupancy {
            let mut keys = vec![binding.to_owned()];
            if let Some(resource) = resources.first()
                && let (Some(account), Some(strategy)) = (
                    resource.body["account_identity"].as_str(),
                    resource.body["runtime_strategy_no"].as_str(),
                )
            {
                keys.push(serde_json::to_string(&json!([
                    "order_namespace",
                    account,
                    strategy
                ]))?);
            }
            for binding in &keys {
                let occupied: Option<String> = tx
                    .query_row(
                        "SELECT run_id FROM occupancy WHERE binding=?1",
                        [binding],
                        |r| r.get(0),
                    )
                    .optional()?;
                let predecessor = match &input.action {
                    Action::Restore { run_id, .. } => Some(run_id),
                    _ => None,
                };
                if occupied
                    .as_ref()
                    .is_some_and(|run| Some(run) != predecessor)
                {
                    return Err(fault(
                        409,
                        "BINDING_OCCUPIED",
                        "account/instrument is reserved, including unknown or inventory-retaining runs",
                    ));
                }
                tx.execute(
                "INSERT INTO occupancy(binding,operation_id,node_id,run_id) VALUES (?1,?2,?3,?4) ON CONFLICT(binding) DO UPDATE SET operation_id=excluded.operation_id,node_id=excluded.node_id,run_id=excluded.run_id",
                params![binding, id, input.node_id, predecessor.cloned().unwrap_or_else(||format!("{}-run-{id}",input.node_id))],
            )?;
            }
        }
        let material = json!({"action":input.action,"resources":resources,"node_id":input.node_id});
        let input_digest = digest(&serde_json::to_vec(&material)?);
        let account = resources.iter().find_map(|r| {
            r.body
                .get("account")
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
        let request = NodeRequest {
            api_version: API_VERSION,
            operation_id: id.clone(),
            input_digest: input_digest.clone(),
            action: input.action.clone(),
            resources,
        };
        let time = now_ns();
        let mut operation = Operation {
            id: id.clone(),
            actor: actor.into(),
            node_id: input.node_id.clone(),
            input: input.action.clone(),
            account,
            input_digest,
            state: OperationState::Pending,
            cancel_requested: false,
            created_at_ns: time.clone(),
            updated_at_ns: time,
            result: None,
            error: None,
            revision: String::new(),
        };
        operation.revision = revision(&tx)?.to_string();
        tx.execute(
            "INSERT INTO operations VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![
                id,
                actor,
                input.idempotency_key,
                payload_digest,
                input.node_id,
                serde_json::to_string(&operation)?,
                serde_json::to_string(&request)?
            ],
        )?;
        event(
            &tx,
            &operation.revision,
            &operation.node_id,
            operation.input.environment(),
            "operation",
            &id,
            serde_json::to_value(&operation)?,
        )?;
        tx.execute(
            "INSERT INTO audit(at_ns,actor,action,target,digest) VALUES (?1,?2,'submit',?3,?4)",
            params![now_ns(), actor, id, payload_digest],
        )?;
        tx.commit()?;
        Ok(operation)
    }
    pub fn operation(&self, id: &str) -> Result<Operation> {
        let data: Option<String> = self
            .connection()?
            .query_row("SELECT data FROM operations WHERE id=?1", [id], |r| {
                r.get(0)
            })
            .optional()?;
        serde_json::from_str(&data.ok_or_else(|| fault(404, "NOT_FOUND", "operation not found"))?)
            .context("invalid stored operation")
    }
    pub fn request(&self, id: &str) -> Result<NodeRequest> {
        let data: String = self.connection()?.query_row(
            "SELECT request FROM operations WHERE id=?1",
            [id],
            |r| r.get(0),
        )?;
        Ok(serde_json::from_str(&data)?)
    }
    pub fn update_operation(&self, receipt: &Receipt) -> Result<Operation> {
        self.update_operation_at(receipt, None)
    }
    pub fn update_operation_if_current(
        &self,
        receipt: &Receipt,
        revision: &str,
    ) -> Result<Operation> {
        self.update_operation_at(receipt, Some(revision))
    }
    fn update_operation_at(&self, receipt: &Receipt, revision: Option<&str>) -> Result<Operation> {
        ensure!(
            receipt.state != OperationState::Pending,
            "node receipt cannot reset dispatch ownership"
        );
        self.mutate_operation(&receipt.operation_id, |op| {
            if revision.is_some_and(|expected| op.revision != expected) {
                return Ok(());
            }
            ensure!(
                receipt.input_digest == op.input_digest,
                "node receipt input identity mismatch"
            );
            if !op.state.terminal() {
                op.state = receipt.state.clone();
                op.result = receipt.result.clone();
                op.error = receipt.error.clone();
            }
            Ok(())
        })
    }
    pub fn cancel(&self, id: &str) -> Result<Operation> {
        self.mutate_operation(id, |op| {
            if !op.state.terminal() {
                op.cancel_requested = true;
            }
            Ok(())
        })
    }
    pub fn begin_dispatch_after_absence(&self, id: &str) -> Result<Option<NodeRequest>> {
        let mut claimed = false;
        self.mutate_operation(id, |op| {
            if op.state == OperationState::Pending {
                if op.cancel_requested {
                    op.state = OperationState::Cancelled;
                } else {
                    op.state = OperationState::Unknown;
                    op.error = Some("dispatch acknowledgement pending".into());
                    claimed = true;
                }
            }
            Ok(())
        })?;
        if claimed {
            Ok(Some(self.request(id)?))
        } else {
            if self.operation(id)?.state == OperationState::Cancelled
                && matches!(self.operation(id)?.input, Action::Start { .. })
            {
                self.connection()?
                    .execute("DELETE FROM occupancy WHERE operation_id=?1", [id])?;
            }
            Ok(None)
        }
    }
    pub fn admission_rejected(&self, id: &str, digest: &str) -> Result<Operation> {
        self.mutate_operation(id, |op| {
            ensure!(op.input_digest == digest, "admission identity mismatch");
            if op.state == OperationState::Unknown {
                op.state = OperationState::Pending;
                op.error = Some("node capacity occupied; not admitted".into());
            }
            Ok(())
        })
    }
    fn mutate_operation(
        &self,
        id: &str,
        mutate: impl FnOnce(&mut Operation) -> Result<()>,
    ) -> Result<Operation> {
        let mut db = self.connection()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let data: String = tx.query_row("SELECT data FROM operations WHERE id=?1", [id], |r| {
            r.get(0)
        })?;
        let mut op: Operation = serde_json::from_str(&data)?;
        let before = serde_json::to_string(&op)?;
        mutate(&mut op)?;
        if matches!(op.input, Action::Start { .. })
            && matches!(op.state, OperationState::Failed | OperationState::Cancelled)
            && op
                .result
                .as_ref()
                .is_some_and(|r| r["start_admission_absent"] == true)
        {
            tx.execute(
                "DELETE FROM occupancy WHERE operation_id=?1 AND node_id=?2",
                params![op.id, op.node_id],
            )?;
        }
        if before != serde_json::to_string(&op)? {
            op.updated_at_ns = now_ns();
            op.revision = revision(&tx)?.to_string();
            tx.execute(
                "UPDATE operations SET data=?2 WHERE id=?1",
                params![id, serde_json::to_string(&op)?],
            )?;
            event(
                &tx,
                &op.revision,
                &op.node_id,
                op.input.environment(),
                "operation",
                id,
                serde_json::to_value(&op)?,
            )?;
        }
        tx.commit()?;
        Ok(op)
    }
    pub fn resource(&self, id: &str) -> Result<Resource> {
        let data: Option<String> = self
            .connection()?
            .query_row("SELECT data FROM resources WHERE id=?1", [id], |r| r.get(0))
            .optional()?;
        Ok(serde_json::from_str(&data.ok_or_else(|| {
            fault(404, "NOT_FOUND", "resource not found")
        })?)?)
    }
    pub fn strategy_version_for_operation(&self, id: &str) -> Result<Resource> {
        let db = self.connection()?;
        let mut stmt = db.prepare("SELECT data FROM resources WHERE kind IN ('strategy_version','artifact') AND json_extract(data,'$.body.operation_id')=?1 LIMIT 2")?;
        let rows = stmt
            .query_map([id], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        ensure!(rows.len() <= 1, "ambiguous compiled_strategy reference");
        let data = rows
            .first()
            .ok_or_else(|| fault(404, "NOT_FOUND", "compiled_strategy not found"))?;
        Ok(serde_json::from_str(data)?)
    }
    pub fn save_resource(&self, resource: Resource, expected: Option<&str>) -> Result<Resource> {
        self.save_resource_inner(resource, expected, None)
    }
    pub fn save_resource_audited(
        &self,
        resource: Resource,
        expected: Option<&str>,
        actor: &str,
        action: &str,
    ) -> Result<Resource> {
        self.save_resource_inner(resource, expected, Some((actor, action)))
    }
    fn save_resource_inner(
        &self,
        mut resource: Resource,
        expected: Option<&str>,
        audit: Option<(&str, &str)>,
    ) -> Result<Resource> {
        let mut db = self.connection()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let never_activated = resource.kind == "run"
            && resource.body["observation"]["start_admission_absent"] == true
            && resource.body["observation"]["source"] == "runtime_owner"
            && resource.body["observation"]["run_id"] == resource.body["admission_operation_id"];
        if never_activated
            && resource.body["predecessor_run_id"].is_null()
            && let Some(admission) = resource.body["admission_operation_id"].as_str()
        {
            let data: Option<String> = tx
                .query_row(
                    "SELECT data FROM operations WHERE id=?1",
                    [admission],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(data) = data {
                let operation: Operation = serde_json::from_str(&data)?;
                if operation.node_id == resource.node_id
                    && matches!(operation.input, Action::Start { .. })
                    && matches!(
                        operation.state,
                        OperationState::Failed | OperationState::Cancelled
                    )
                {
                    // Release a terminal Start only with E's later non-activation proof.
                    // Preserve the failure and never release a recovery predecessor
                    // or an occupancy that has moved to another operation.
                    tx.execute(
                        "DELETE FROM occupancy WHERE operation_id=?1 AND node_id=?2 AND (run_id IS NULL OR run_id=?3)",
                        params![admission, resource.node_id, resource.id],
                    )?;
                }
            }
        }
        if never_activated
            && let (Some(previous), Some(admission)) = (
                resource.body["predecessor_run_id"].as_str(),
                resource.body["admission_operation_id"].as_str(),
            )
        {
            // A failed candidate never owned an activated strategy. Keep the reservation,
            // but restore its recoverable predecessor, including when repairing a cached row.
            tx.execute(
                "UPDATE occupancy SET run_id=?1 WHERE node_id=?2 AND run_id=?3 AND operation_id=?4",
                params![previous, resource.node_id, resource.id, admission],
            )?;
        }
        let old: Option<String> = tx
            .query_row(
                "SELECT data FROM resources WHERE id=?1",
                [&resource.id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(old) = old {
            let old: Resource = serde_json::from_str(&old)?;
            if resource.kind == "alert"
                && resource.body["condition_identity"] == old.body["condition_identity"]
                && resource.body.get("acknowledgement").is_none()
                && let Some(ack) = old.body.get("acknowledgement")
            {
                resource.body["acknowledgement"] = ack.clone();
            }
            if old.body == resource.body && old.frozen == resource.frozen && expected.is_none() {
                tx.commit()?;
                return Ok(old);
            }
            if old.frozen || expected.is_some_and(|r| r != old.revision) {
                return Err(fault(
                    409,
                    "VERSION_CONFLICT",
                    "resource is frozen or revision changed",
                ));
            }
        } else if expected.is_some() {
            return Err(fault(409, "VERSION_CONFLICT", "resource no longer exists"));
        }
        resource.revision = revision(&tx)?.to_string();
        if resource.kind == "run"
            && !never_activated
            && resource.body["run_generation"]
                .as_str()
                .and_then(|g| g.parse::<u64>().ok())
                .is_some_and(|g| g > 0)
            && let Some(previous) = resource.body["predecessor_run_id"].as_str()
        {
            tx.execute(
                "UPDATE occupancy SET run_id=?1 WHERE node_id=?2 AND run_id=?3 AND operation_id=?4",
                params![
                    resource.id,
                    resource.node_id,
                    previous,
                    resource.body["admission_operation_id"]
                        .as_str()
                        .unwrap_or("")
                ],
            )?;
        }
        if resource.kind == "run"
            && resource.body["release_proof"]["safe"] == true
            && resource.body["release_proof"]["source"] == "trading_runtime"
        {
            tx.execute(
                "DELETE FROM occupancy WHERE node_id=?1 AND run_id=?2 AND operation_id=?3",
                params![
                    resource.node_id,
                    resource.id,
                    resource.body["admission_operation_id"]
                        .as_str()
                        .unwrap_or("")
                ],
            )?;
        }
        resource.digest = digest(&serde_json::to_vec(&resource.body)?);
        tx.execute("INSERT INTO resources VALUES (?1,?2,?3,?4) ON CONFLICT(id) DO UPDATE SET data=excluded.data",params![resource.id,resource.kind,resource.node_id,serde_json::to_string(&resource)?])?;
        if let Some((actor, action)) = audit {
            tx.execute(
                "INSERT INTO audit(at_ns,actor,action,target,digest) VALUES (?1,?2,?3,?4,?5)",
                params![now_ns(), actor, action, resource.id, resource.digest],
            )?;
        }
        let env = resource
            .body
            .get("environment")
            .and_then(Value::as_str)
            .unwrap_or("backtest");
        event(
            &tx,
            &resource.revision,
            &resource.node_id,
            env,
            "resource",
            &resource.id,
            serde_json::to_value(&resource)?,
        )?;
        tx.commit()?;
        Ok(resource)
    }
    pub fn snapshot(&self, identity: &Identity, limit: usize) -> Result<Snapshot> {
        let db = self.connection()?;
        let seq: i64 = db.query_row("SELECT value FROM meta WHERE key='revision'", [], |r| {
            r.get(0)
        })?;
        let mut operations = Vec::new();
        let mut stmt = db.prepare("SELECT data FROM operations ORDER BY rowid DESC LIMIT 4096")?;
        for row in stmt.query_map([], |r| r.get::<_, String>(0))? {
            let op: Operation = serde_json::from_str(&row?)?;
            if allowed(identity, &op.node_id, op.input.environment())
                && account_allowed(identity, op.account.as_deref())
            {
                operations.push(op);
                if operations.len() >= limit {
                    break;
                }
            }
        }
        let mut resources = Vec::new();
        let mut stmt=db.prepare("SELECT data FROM resources ORDER BY CASE WHEN kind IN ('parameter_preset','template','dataset','binding','account','node') OR (kind='alert' AND json_extract(data,'$.body.active')=1) OR (kind='run' AND json_extract(data,'$.body.process_state') IN ('RUNNING','READY','STARTING','STOP_REQUESTED')) THEN 0 ELSE 1 END,rowid DESC LIMIT 4096")?;
        for row in stmt.query_map([], |r| r.get::<_, String>(0))? {
            let resource: Resource = serde_json::from_str(&row?)?;
            let env = resource
                .body
                .get("environment")
                .and_then(Value::as_str)
                .unwrap_or("backtest");
            let account = resource.body.get("account").and_then(Value::as_str);
            if allowed(identity, &resource.node_id, env) && account_allowed(identity, account) {
                resources.push(resource);
                if resources.len() >= limit {
                    break;
                }
            }
        }
        Ok(Snapshot {
            api_version: API_VERSION,
            cursor: format!("{}:{seq}", self.epoch),
            server_time_ns: now_ns(),
            operations,
            resources,
        })
    }
    pub fn list_strategies(
        &self,
        identity: &Identity,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<StrategySummary>> {
        let db = self.strategy_connection()?;
        let mut stmt = db.prepare(&format!("SELECT {STRATEGY_PROJECTION} FROM resources WHERE kind IN ('parameter_preset','template','parameter_version','version','strategy_version','artifact','backtest','report','deployment','run') ORDER BY CASE WHEN kind IN ('parameter_preset','template') THEN 0 ELSE 1 END,rowid DESC"))?;
        let mut memo = std::collections::HashMap::new();
        let mut groups: std::collections::BTreeMap<String, StrategySummary> = Default::default();
        for row in stmt.query_map([], |r| r.get::<_, String>(0))? {
            let item: Resource = serde_json::from_str(&row?)?;
            let env = item.body["environment"].as_str().unwrap_or("backtest");
            if !allowed(identity, &item.node_id, env)
                || !account_allowed(identity, item.body["account"].as_str())
            {
                continue;
            }
            if item.kind == "run" && item.body["mode"] == "observe" {
                continue;
            }
            let id = strategy_membership(&db, &item, 0, &mut memo)?
                .unwrap_or_else(|| "unassigned".into());
            let group = groups.entry(id.clone()).or_insert_with(|| StrategySummary {
                id: id.clone(),
                name: if id == "unassigned" {
                    "未归属的历史记录".into()
                } else {
                    item.body["strategy_name"]
                        .as_str()
                        .unwrap_or("策略研究")
                        .to_owned()
                },
                parameter_preset_count: 0,
                resource_count: 0,
            });
            if item.kind == "parameter_preset" {
                group.parameter_preset_count += 1;
            } else {
                group.resource_count += 1;
            }
        }
        Ok(groups.into_values().skip(offset).take(limit).collect())
    }

    pub fn list_resources(
        &self,
        identity: &Identity,
        kind: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Resource>> {
        self.list_resources_scoped(identity, kind, offset, limit, None, None)
    }
    pub fn list_resources_scoped(
        &self,
        identity: &Identity,
        kind: Option<&str>,
        offset: usize,
        limit: usize,
        strategy: Option<&str>,
        resource_id: Option<&str>,
    ) -> Result<Vec<Resource>> {
        let db = self.strategy_connection()?;
        let mut stmt = db.prepare(&format!(
            "SELECT {STRATEGY_PROJECTION} FROM resources WHERE (?1 IS NULL OR kind=?1 OR kind=CASE ?1 WHEN 'strategy_version' THEN 'artifact' WHEN 'parameter_version' THEN 'version' WHEN 'parameter_preset' THEN 'template' ELSE ?1 END) AND (?2 IS NULL OR id=?2) ORDER BY rowid DESC",
        ))?;
        let mut out = Vec::new();
        let mut skipped = 0;
        let mut membership = std::collections::HashMap::new();
        for row in stmt.query_map(params![kind, resource_id], |r| r.get::<_, String>(0))? {
            let item: Resource = serde_json::from_str(&row?)?;
            let env = item
                .body
                .get("environment")
                .and_then(Value::as_str)
                .unwrap_or("backtest");
            if !allowed(identity, &item.node_id, env)
                || !account_allowed(identity, item.body.get("account").and_then(Value::as_str))
            {
                continue;
            }
            if let Some(strategy) = strategy {
                if !matches!(
                    item.kind.as_str(),
                    "parameter_preset"
                        | "parameter_version"
                        | "strategy_version"
                        | "backtest"
                        | "report"
                        | "deployment"
                        | "run"
                ) {
                    continue;
                }
                if item.kind == "run" && item.body["mode"] == "observe" {
                    continue;
                }
                let resolved = strategy_membership(&db, &item, 0, &mut membership)?;
                if (strategy == "unassigned" && resolved.is_some())
                    || (strategy != "unassigned" && resolved.as_deref() != Some(strategy))
                {
                    continue;
                }
            }
            if skipped < offset {
                skipped += 1;
                continue;
            }
            // Materialize full bodies only for the authorized requested page.
            let data: String =
                db.query_row("SELECT data FROM resources WHERE id=?1", [&item.id], |r| {
                    r.get(0)
                })?;
            out.push(serde_json::from_str(&data)?);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }
    pub fn list_operations(
        &self,
        identity: &Identity,
        offset: usize,
        limit: usize,
        from: Option<u64>,
        to: Option<u64>,
        run: Option<&str>,
    ) -> Result<Vec<Operation>> {
        let db = self.connection()?;
        let mut stmt = db.prepare("SELECT data FROM operations ORDER BY rowid DESC")?;
        let mut out = Vec::new();
        let mut skipped = 0;
        for row in stmt.query_map([], |r| r.get::<_, String>(0))? {
            let item: Operation = serde_json::from_str(&row?)?;
            if !allowed(identity, &item.node_id, item.input.environment())
                || !account_allowed(identity, item.account.as_deref())
            {
                continue;
            }
            let time = item.created_at_ns.parse::<u64>()?;
            if from.is_some_and(|v| time < v)
                || to.is_some_and(|v| time > v)
                || run.is_some_and(|v| item.input.run().is_none_or(|(id, _)| id != v))
            {
                continue;
            }
            if skipped < offset {
                skipped += 1;
                continue;
            }
            out.push(item);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }
    pub fn events(&self, identity: &Identity, cursor: &str) -> Result<(Vec<Event>, String)> {
        let (epoch, seq) = cursor.split_once(':').context("invalid event cursor")?;
        let seq: i64 = seq.parse()?;
        if epoch != self.epoch {
            return Err(fault(409, "RESYNC_REQUIRED", "event stream epoch changed"));
        }
        let db = self.connection()?;
        let latest: i64 = db.query_row("SELECT value FROM meta WHERE key='revision'", [], |r| {
            r.get(0)
        })?;
        if seq < latest.saturating_sub(2000) || seq > latest {
            return Err(fault(
                409,
                "RESYNC_REQUIRED",
                "cursor is outside retained events",
            ));
        }
        let mut stmt=db.prepare("SELECT revision,node_id,environment,data FROM events WHERE revision>?1 ORDER BY revision LIMIT 256")?;
        let mut events = Vec::new();
        let mut last = seq;
        for row in stmt.query_map([seq], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
            ))
        })? {
            let (rev, node, env, data) = row?;
            last = rev;
            if allowed(identity, &node, &env) {
                let mut item: Event = serde_json::from_str(&data)?;
                let account = item
                    .payload
                    .get("account")
                    .and_then(Value::as_str)
                    .or_else(|| {
                        item.payload
                            .get("body")
                            .and_then(|b| b.get("account"))
                            .and_then(Value::as_str)
                    });
                if !account_allowed(identity, account) {
                    continue;
                }
                item.cursor = format!("{}:{rev}", self.epoch);
                events.push(item);
            }
        }
        Ok((events, format!("{}:{last}", self.epoch)))
    }
    pub fn active(&self) -> Result<Vec<Operation>> {
        let db = self.connection()?;
        let mut stmt=db.prepare("SELECT data FROM operations WHERE json_extract(data,'$.state') IN ('pending','running','unknown') ORDER BY CASE WHEN json_extract(data,'$.input.kind')='stop' THEN 0 ELSE 1 END,rowid LIMIT 512")?;
        let mut out = Vec::new();
        for row in stmt.query_map([], |r| r.get::<_, String>(0))? {
            out.push(serde_json::from_str(&row?)?);
        }
        Ok(out)
    }
}

pub(crate) fn allowed(identity: &Identity, node: &str, environment: &str) -> bool {
    identity.nodes.iter().any(|v| v == node)
        && identity.environments.iter().any(|v| v == environment)
}
pub(crate) fn account_allowed(identity: &Identity, account: Option<&str>) -> bool {
    account.is_none_or(|account| identity.accounts.iter().any(|value| value == account))
}
// Resolve immutable lineage without rewriting old frozen records or their digests.
fn strategy_membership(
    db: &Connection,
    item: &Resource,
    depth: usize,
    memo: &mut std::collections::HashMap<String, Option<String>>,
) -> Result<Option<String>> {
    if let Some(value) = memo.get(&item.id) {
        return Ok(value.clone());
    }
    if depth >= 12 {
        return Ok(None);
    }
    memo.insert(item.id.clone(), None);
    if let Some(id) = item.body["strategy_id"].as_str().filter(|v| !v.is_empty()) {
        let value = Some(id.to_owned());
        memo.insert(item.id.clone(), value.clone());
        return Ok(value);
    }
    let parent = match item.kind.as_str() {
        "parameter_version" => item.body["parameter_preset_id"]
            .as_str()
            .map(|id| format!("{}-template-{id}", item.node_id)),
        "strategy_version" => item.body["parameter_version_id"]
            .as_str()
            .map(str::to_owned),
        "backtest" | "deployment" => item.body["strategy_version_id"].as_str().map(str::to_owned),
        "report" => item.body["backtest_id"].as_str().map(str::to_owned),
        "run" => item.body["deployment_id"].as_str().map(str::to_owned),
        _ => None,
    };
    let value = if let Some(id) = parent {
        let data: Option<String> = db
            .query_row(
                &format!("SELECT {STRATEGY_PROJECTION} FROM resources WHERE id=?1"),
                [id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(data) = data {
            let parent: Resource = serde_json::from_str(&data)?;
            // A reused alias must not reassign old, unlabelled source versions.
            if item.kind == "parameter_version"
                && (item.body["source_digest"].as_str().is_none()
                    || item.body["source_digest"] != parent.body["source_digest"])
            {
                None
            } else {
                strategy_membership(db, &parent, depth + 1, memo)?
            }
        } else {
            None
        }
    } else {
        None
    };
    memo.insert(item.id.clone(), value.clone());
    Ok(value)
}

fn revision(db: &Connection) -> Result<i64> {
    Ok(db.query_row(
        "UPDATE meta SET value=value+1 WHERE key='revision' RETURNING value",
        [],
        |r| r.get(0),
    )?)
}
fn event(
    db: &Connection,
    revision: &str,
    node: &str,
    environment: &str,
    kind: &str,
    id: &str,
    payload: Value,
) -> Result<()> {
    let seq: i64 = revision.parse()?;
    let item = Event {
        cursor: String::new(),
        resource_id: id.into(),
        kind: kind.into(),
        revision: revision.into(),
        payload,
    };
    db.execute(
        "INSERT INTO events VALUES (?1,?2,?3,?4)",
        params![seq, node, environment, serde_json::to_string(&item)?],
    )?;
    db.execute(
        "DELETE FROM events WHERE revision<?1",
        [seq.saturating_sub(2000)],
    )?;
    Ok(())
}

#[cfg(test)]
mod research_read_tests {
    use super::*;
    use std::sync::{Arc, mpsc};

    #[test]
    fn research_reads_do_not_take_admission_mutex_and_return_full_authorized_bodies() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(&temp.path().join("db")).unwrap());
        let saved = store.save_resource(Resource {
            id: "strategy_version".into(), kind: "strategy_version".into(), node_id: "build".into(),
            revision: String::new(), frozen: true, digest: String::new(),
            body: json!({"strategy_id":"strategy-grid","strategy_name":"网格", "metadata":{"schema":"must-survive-projection"},"parameters":{"quantity":"9007199254740993"}}),
        }, None).unwrap();
        let actor = Identity {
            username: "reader".into(),
            role: Role::Reader,
            nodes: vec!["build".into()],
            accounts: vec![],
            environments: vec!["backtest".into()],
        };
        let lock = store.connection().unwrap();
        let reader = store.clone();
        let (tx, rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let result = reader.list_strategies(&actor, 0, 20).and_then(|catalog| {
                let rows = reader.list_resources_scoped(
                    &actor,
                    Some("strategy_version"),
                    0,
                    20,
                    Some("strategy-grid"),
                    None,
                )?;
                Ok((catalog, rows))
            });
            tx.send(result).unwrap();
        });
        let result = rx.recv_timeout(Duration::from_secs(2));
        drop(lock);
        thread.join().unwrap();
        let (catalog, rows) = result
            .expect("strategy query waited for admission mutex")
            .unwrap();
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog[0].resource_count, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            serde_json::to_value(&rows[0]).unwrap(),
            serde_json::to_value(saved).unwrap()
        );

        // A long-lived WAL reader also cannot prevent admission writes.
        let db = store.strategy_connection().unwrap();
        db.execute_batch("BEGIN; SELECT data FROM resources;")
            .unwrap();
        store
            .audit("reader", "review", "strategy_version", "probe")
            .unwrap();
        db.execute_batch("ROLLBACK").unwrap();
    }
}
