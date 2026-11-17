//! Read-only management observations. Cancelling observation never controls a remote run.
use super::client::{Client, ClientError};
use anyhow::{Context, Result, ensure};
use clap::{Args, Subcommand};
use serde_json::{Value, json};
use std::time::Duration;

#[derive(Debug, Clone, Args)]
pub struct PageArgs {
    #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u32).range(0..=1_000_000))]
    pub offset: u32,
    #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u16).range(1..=200))]
    pub limit: u16,
}
#[derive(Debug, Clone, Args)]
pub struct ResourceArgs {
    /// Exact manager resource ID, not a native account or guessed worker identifier.
    pub id: String,
}
#[derive(Debug, Subcommand)]
pub enum AccountCommand {
    Ls(PageArgs),
    Show(ResourceArgs),
    Balances(ResourceArgs),
    Positions(ResourceArgs),
    Orders(ResourceArgs),
    Risk(ResourceArgs),
    Fills(FactsArgs),
    History(FactsArgs),
    Bindings(AccountRelatedArgs),
    Runs(AccountRelatedArgs),
}
#[derive(Debug, Clone, Args)]
pub struct AccountRelatedArgs {
    /// Account or binding resource carrying a proven native account identity.
    pub id: String,
    #[command(flatten)]
    pub page: PageArgs,
}
#[derive(Debug, Subcommand)]
pub enum BindingCommand {
    Ls(PageArgs),
    Show(ResourceArgs),
}
#[derive(Debug, Subcommand)]
pub enum NodeCommand {
    Ls(PageArgs),
    Show(ResourceArgs),
}
#[derive(Debug, Clone, Args)]
pub struct StatusArgs {
    /// Inspect an exact resource instead of the bounded global snapshot.
    pub id: Option<String>,
    #[arg(long)]
    pub account: Option<String>,
    #[arg(long)]
    pub strategy: Option<String>,
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u16).range(1..=200))]
    pub limit: u16,
}
#[derive(Debug, Clone, Args)]
pub struct WatchArgs {
    #[command(flatten)]
    pub status: StatusArgs,
    /// Finite poll budget; each result is emitted immediately without accumulating history.
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u16).range(1..=1000))]
    pub samples: u16,
    #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(u32).range(250..=60_000))]
    pub interval_ms: u32,
}
#[derive(Debug, Clone, Args)]
pub struct HistoryArgs {
    pub id: String,
    #[command(flatten)]
    pub page: PageArgs,
    #[arg(long)]
    pub kind: Option<String>,
    /// Validate the current source identity; this is distinct from archive filtering.
    #[arg(long, alias = "worker-session-id")]
    pub generation: Option<String>,
    /// Select proven fact run_generation; run resources default to their current generation.
    #[arg(long)]
    pub run_generation: Option<String>,
    #[arg(long, allow_hyphen_values = true)]
    pub from_ns: Option<String>,
    #[arg(long, allow_hyphen_values = true)]
    pub to_ns: Option<String>,
}
#[derive(Debug, Clone, Args)]
pub struct FactsArgs {
    #[command(flatten)]
    pub history: HistoryArgs,
    /// Opaque server cursor, bound to the original filters and fixed upper bound.
    #[arg(long)]
    pub cursor: Option<String>,
    #[arg(long)]
    pub newest_first: bool,
    /// Export at most this many cursor pages; multiple pages require --ndjson.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u16).range(1..=100))]
    pub max_pages: u16,
}
#[derive(Debug, Clone, Args)]
pub struct LogsArgs {
    pub id: String,
    /// Byte offset in the server's fixed worker.log.
    #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u32).range(0..=8_388_608))]
    pub offset: u32,
    #[arg(long, alias = "worker-session-id")]
    pub generation: Option<String>,
    #[arg(long)]
    pub follow: bool,
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u16).range(1..=1000))]
    pub samples: u16,
    #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(u32).range(250..=60_000))]
    pub interval_ms: u32,
}
fn public_observation(value: &mut Value) {
    match value {
        Value::Array(items) => items.iter_mut().for_each(public_observation),
        Value::Object(object) => {
            for key in [
                "credential_ref",
                "notes",
                "runtime_config",
                "runtime_config_path",
                "config_path",
                "work_root",
                "endpoint",
                "token",
                "password",
                "credentials",
                "secret_ref",
            ] {
                object.remove(key);
            }
            object.values_mut().for_each(public_observation);
        }
        _ => {}
    }
}
fn print(client: &Client, kind: &str, value: &Value) -> Result<()> {
    let mut value = value.clone();
    public_observation(&mut value);
    client.print_kind(kind, &value)
}
fn resource_path(id: &str) -> Result<String> {
    super::checked_id(id)?;
    Ok(format!("/api/v2/resources/{id}"))
}
async fn resource(client: &Client, id: &str) -> Result<Value> {
    let value = client.get(&resource_path(id)?, &[]).await?;
    ensure!(
        client
            .options()
            .node
            .as_deref()
            .is_none_or(|node| value["node_id"].as_str() == Some(node)),
        ClientError::new("INVALID_INPUT", "resource does not match selected node")
    );
    ensure!(
        client
            .environment()
            .is_none_or(|environment| value["body"]["environment"].as_str() == Some(environment)),
        ClientError::new(
            "INVALID_INPUT",
            "resource does not match selected environment"
        )
    );
    Ok(value)
}
fn is_kind(value: &Value, kinds: &[&str]) -> Result<()> {
    ensure!(
        value["kind"].as_str().is_some_and(|k| kinds.contains(&k)),
        "resource kind does not support this query"
    );
    Ok(())
}
fn source_identity(value: &Value) -> Result<String> {
    value["body"]["run_generation"]
        .as_str()
        .or_else(|| value["body"]["worker_session_id"].as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .context("resource has no proven run generation or observation session")
}
fn pinned_identity(value: &Value, requested: Option<&str>) -> Result<String> {
    let actual = source_identity(value)?;
    ensure!(
        requested.is_none_or(|s| s == actual),
        ClientError::new(
            "STALE_GENERATION",
            "source changed; start a new observation"
        )
    );
    Ok(actual)
}
fn add_scope<'a>(client: &'a Client, query: &mut Vec<(&'a str, &'a str)>) {
    if let Some(node) = client.options().node.as_deref() {
        query.push(("node_id", node));
    }
    if let Some(environment) = client.environment() {
        query.push(("environment", environment));
    }
}
async fn list(
    client: &Client,
    kind: &str,
    page: &PageArgs,
    account: Option<&str>,
) -> Result<Value> {
    let offset = page.offset.to_string();
    let limit = page.limit.to_string();
    let mut query = vec![
        ("kind", kind),
        ("offset", offset.as_str()),
        ("limit", limit.as_str()),
    ];
    add_scope(client, &mut query);
    if let Some(account) = account {
        query.push(("account", account));
    }
    let items = client.get("/api/v2/resources", &query).await?;
    let count = items
        .as_array()
        .context("manager returned a non-array resource page")?
        .len();
    Ok(
        json!({"items":items,"offset":offset,"limit":limit,"possibly_more":count == usize::from(page.limit),"scope":"authorized filtered page"}),
    )
}
pub async fn execute_binding(client: &Client, command: BindingCommand) -> Result<()> {
    match command {
        BindingCommand::Ls(page) => print(
            client,
            "bindings",
            &list(client, "binding", &page, None).await?,
        ),
        BindingCommand::Show(args) => {
            let value = resource(client, &args.id).await?;
            is_kind(&value, &["binding"])?;
            print(client, "binding", &value)
        }
    }
}
pub async fn execute_node(client: &Client, command: NodeCommand) -> Result<()> {
    match command {
        NodeCommand::Ls(page) => print(client, "nodes", &list(client, "node", &page, None).await?),
        NodeCommand::Show(args) => {
            let value = resource(client, &args.id).await?;
            is_kind(&value, &["node"])?;
            print(client, "node", &value)
        }
    }
}
pub async fn execute_account(client: &Client, command: AccountCommand) -> Result<()> {
    match command {
        AccountCommand::Ls(page) => {
            let bindings = list(client, "binding", &page, None).await?;
            let profiles = list(client, "account_profile", &page, None).await?;
            let observations = list(client, "account", &page, None).await?;
            print(
                client,
                "accounts",
                &json!({"configured_bindings":bindings,"profiles":profiles,"observations":observations,"identity_note":"Profiles are inert directory records; no profile-to-runtime mapping is inferred. A configured binding without observation has no realtime evidence."}),
            )
        }
        AccountCommand::Show(args) => {
            let value = resource(client, &args.id).await?;
            is_kind(&value, &["account", "account_profile", "binding"])?;
            print(client, "account", &value)
        }
        AccountCommand::Balances(args) => account_projection(client, &args.id, "balances").await,
        AccountCommand::Positions(args) => account_projection(client, &args.id, "positions").await,
        AccountCommand::Orders(args) => account_projection(client, &args.id, "orders").await,
        AccountCommand::Risk(args) => account_projection(client, &args.id, "risk").await,
        AccountCommand::Fills(mut args) => {
            ensure!(
                args.history.kind.as_deref().is_none_or(|k| k == "fill"),
                "fills only supports kind=fill"
            );
            args.history.kind = Some("fill".into());
            execute_facts(client, args).await
        }
        AccountCommand::History(args) => execute_facts(client, args).await,
        AccountCommand::Bindings(args) => account_related(client, args, "binding").await,
        AccountCommand::Runs(args) => account_related(client, args, "run").await,
    }
}
async fn account_projection(client: &Client, id: &str, field: &str) -> Result<()> {
    let value = resource(client, id).await?;
    is_kind(&value, &["account", "run", "account_profile", "binding"])?;
    let body = &value["body"];
    let observed = &body["observation"];
    let snapshot = &observed["account_snapshot"];
    let data = snapshot.get("image").and_then(|image| image.get(field));
    let available = body["available"] == true && snapshot["available"] == true && data.is_some();
    print(
        client,
        "account_projection",
        &json!({"resource_id":value["id"],"revision":value["revision"],"node_id":value["node_id"],"account":body["account"],"instrument":body["instrument"],"run_generation":body["run_generation"],"worker_session_id":body["worker_session_id"],"sampled_at_ns":observed["captured_at_ns"],"available":available,"stale":body["observation_stale"],"field":field,"data":data,"account_snapshot":snapshot,"account_health":observed["account_health"],"financial":observed["financial"],"reason":if available {Value::Null} else {json!("no current proven account projection; missing values remain null")}}),
    )
}
async fn account_related(client: &Client, args: AccountRelatedArgs, kind: &str) -> Result<()> {
    let account = resource(client, &args.id).await?;
    is_kind(&account, &["account", "binding", "run"])?;
    let native = account["body"]["account"]
        .as_str()
        .context("resource has no proven account identity")?;
    let node = account["node_id"]
        .as_str()
        .context("account source has no node identity")?;
    let environment = account["body"]["environment"]
        .as_str()
        .context("account source has no environment")?;
    let offset = args.page.offset.to_string();
    let limit = args.page.limit.to_string();
    let items = client
        .get(
            "/api/v2/resources",
            &[
                ("kind", kind),
                ("account", native),
                ("node_id", node),
                ("environment", environment),
                ("offset", &offset),
                ("limit", &limit),
            ],
        )
        .await?;
    let count = items
        .as_array()
        .context("manager returned a non-array resource page")?
        .len();
    let page = json!({"items":items,"offset":offset,"limit":limit,"possibly_more":count == usize::from(args.page.limit),"scope":"authorized account/node/environment page"});
    print(
        client,
        "account_related",
        &json!({"account_resource_id":args.id,"page":page}),
    )
}
fn matches_filters(client: &Client, args: &StatusArgs, value: &Value) -> bool {
    let body = &value["body"];
    client
        .options()
        .node
        .as_deref()
        .is_none_or(|n| value["node_id"].as_str() == Some(n))
        && client
            .environment()
            .is_none_or(|e| body["environment"].as_str() == Some(e))
        && args
            .account
            .as_deref()
            .is_none_or(|a| body["account"].as_str() == Some(a))
        && args
            .strategy
            .as_deref()
            .is_none_or(|s| body["strategy_id"].as_str() == Some(s))
}
async fn status(client: &Client, args: &StatusArgs) -> Result<Value> {
    if let Some(id) = &args.id {
        let value = resource(client, id).await?;
        ensure!(
            matches_filters(client, args, &value),
            "resource does not match requested scope"
        );
        return Ok(value);
    }
    let limit = args.limit.to_string();
    let mut value = client.get("/api/v2/snapshot", &[("limit", &limit)]).await?;
    if let Some(items) = value["resources"].as_array_mut() {
        items.retain(|r| matches_filters(client, args, r));
    }
    if let Some(items) = value["operations"].as_array_mut() {
        items.retain(|op| {
            client
                .options()
                .node
                .as_deref()
                .is_none_or(|n| op["node_id"].as_str() == Some(n))
                && client.environment().is_none_or(|environment| {
                    serde_json::from_value::<super::contract::Action>(op["input"].clone())
                        .is_ok_and(|action| action.environment() == environment)
                })
                && args
                    .account
                    .as_deref()
                    .is_none_or(|a| op["account"].as_str() == Some(a))
        });
        if args.strategy.is_some() {
            items.clear();
        }
    }
    if args.strategy.is_some() {
        value["operations_scope"] = json!({"available":false,"reason":"manager snapshot does not prove strategy ownership for each operation; omitted, not an empty-history assertion"});
    }
    value["scope"] = json!(
        "bounded latest snapshot; filters apply within it, use paginated resource lists for discovery"
    );
    Ok(value)
}
pub async fn execute_status(client: &Client, args: StatusArgs) -> Result<()> {
    print(client, "status", &status(client, &args).await?)
}
fn revision(value: &Value) -> Option<(String, u64)> {
    if let Some(cursor) = value["cursor"].as_str() {
        let (epoch, sequence) = cursor.split_once(':')?;
        return Some((epoch.to_owned(), sequence.parse().ok()?));
    }
    Some((String::new(), value["revision"].as_str()?.parse().ok()?))
}
fn generations_changed(old: &Value, new: &Value) -> bool {
    if old.get("id").is_some() {
        return watch_identity(old) != watch_identity(new);
    }
    old["resources"].as_array().is_some_and(|items| {
        items.iter().any(|a| {
            new["resources"].as_array().is_some_and(|current| {
                current
                    .iter()
                    .any(|b| a["id"] == b["id"] && watch_identity(a) != watch_identity(b))
            })
        })
    })
}
fn evidence_stale(value: &Value) -> bool {
    value["body"]["observation_stale"] == true
        || value["body"]["available"] == false
        || value["kind"] == "node" && value["body"]["online"] == false
        || value["resources"]
            .as_array()
            .is_some_and(|items| items.iter().any(evidence_stale))
}
fn watch_identity(value: &Value) -> Value {
    json!([
        value["id"],
        value["body"]["worker_id"],
        value["body"]["run_generation"],
        value["body"]["worker_session_id"]
    ])
}
pub async fn execute_watch(client: &Client, args: WatchArgs) -> Result<()> {
    ensure!(
        !client.options().json || args.samples == 1,
        ClientError::new(
            "INVALID_INPUT",
            "watch --json requires --samples 1; use --ndjson for multiple samples"
        )
    );
    let interrupted = tokio::signal::ctrl_c();
    tokio::pin!(interrupted);
    let mut last: Option<Value> = None;
    let mut disconnected = false;
    let mut epoch = 1_u32;
    let mut high_revision: Option<(String, u64)> = None;
    for sample in 0..args.samples {
        let response = tokio::select! {
            signal = &mut interrupted => { signal?; return Ok(()); }
            response = status(client, &args.status) => response,
        };
        match response {
            Ok(value) => {
                let changed = last
                    .as_ref()
                    .is_some_and(|old| generations_changed(old, &value));
                let incoming_revision = revision(&value);
                let epoch_changed = high_revision
                    .as_ref()
                    .zip(incoming_revision.as_ref())
                    .is_some_and(|((old, _), (new, _))| old != new);
                let regressed = high_revision
                    .as_ref()
                    .zip(incoming_revision.as_ref())
                    .is_some_and(|((old_epoch, a), (new_epoch, b))| {
                        old_epoch == new_epoch && b < a
                    });
                if regressed {
                    print(
                        client,
                        "watch",
                        &json!({"sample":sample,"connection_epoch":epoch,"stale":true,"state":"old_revision_discarded","data":null}),
                    )?;
                } else {
                    if changed || disconnected || epoch_changed {
                        epoch += 1;
                    }
                    high_revision = incoming_revision.or(high_revision);
                    print(
                        client,
                        "watch",
                        &json!({"sample":sample,"connection_epoch":epoch,"stale":evidence_stale(&value),"transport_stale":false,"state":if changed {"generation_changed"} else if disconnected || epoch_changed {"reconnected_resynchronized"} else {"snapshot"},"data":value}),
                    )?;
                    last = Some(value);
                    disconnected = false;
                }
            }
            Err(error) => {
                if error.downcast_ref::<ClientError>().is_some_and(|error| {
                    error
                        .status
                        .is_some_and(|status| status < 500 && status != 408 && status != 429)
                        || error.code == "INVALID_INPUT"
                }) {
                    return Err(error);
                }
                print(
                    client,
                    "watch",
                    &json!({"sample":sample,"connection_epoch":epoch,"stale":true,"state":"reconnecting","last_evidence":last,"data":null,"reason":error.to_string()}),
                )?;
                last = None;
                disconnected = true;
            }
        }
        if sample + 1 < args.samples {
            tokio::select! {
                signal = &mut interrupted => { signal?; return Ok(()); }
                _ = tokio::time::sleep(Duration::from_millis(u64::from(args.interval_ms))) => {}
            }
        }
    }
    Ok(())
}
fn validate_history(args: &HistoryArgs, facts: bool) -> Result<()> {
    ensure!(
        facts || args.from_ns.is_none() && args.to_ns.is_none() && args.run_generation.is_none(),
        ClientError::new(
            "UNSUPPORTED_HISTORY_FILTER",
            "use facts or account history for time and run-generation archive filters"
        )
    );
    ensure!(
        args.kind
            .as_ref()
            .is_none_or(|k| !k.is_empty() && k.len() <= 64),
        "invalid history kind"
    );
    for bound in [&args.from_ns, &args.to_ns].into_iter().flatten() {
        bound
            .parse::<i64>()
            .context("time bound must be signed decimal nanoseconds")?;
    }
    if let (Some(from), Some(to)) = (&args.from_ns, &args.to_ns) {
        ensure!(
            from.parse::<i64>()? <= to.parse::<i64>()?,
            "from_ns exceeds to_ns"
        );
    }
    Ok(())
}
async fn history_page(
    client: &Client,
    args: &HistoryArgs,
    facts: Option<&FactsArgs>,
) -> Result<Value> {
    validate_history(args, facts.is_some())?;
    let record = resource(client, &args.id).await?;
    is_kind(
        &record,
        if facts.is_some() {
            &["account", "run", "backtest"]
        } else {
            &["account", "run"]
        },
    )?;
    let generation = if record["kind"] == "backtest" {
        ensure!(
            args.generation.is_none(),
            "backtest archive has no runtime generation"
        );
        None
    } else {
        Some(pinned_identity(&record, args.generation.as_deref())?)
    };
    let fact_generation = if facts.is_some() {
        args.run_generation.as_deref().or_else(|| {
            if record["kind"] == "run" {
                generation.as_deref()
            } else {
                None
            }
        })
    } else {
        None
    };
    let offset = args.page.offset.to_string();
    let limit = args.page.limit.to_string();
    let mut query = vec![("offset", offset.as_str()), ("limit", limit.as_str())];
    if let Some(generation) = &generation {
        query.push(("generation", generation));
    }
    if let Some(generation) = fact_generation {
        query.push(("run_generation", generation));
    }
    if let Some(kind) = &args.kind {
        query.push(("kind", kind));
    }
    if let Some(from) = &args.from_ns {
        query.push(("from_ns", from));
    }
    if let Some(to) = &args.to_ns {
        query.push(("to_ns", to));
    }
    if let Some(facts) = facts {
        query.push((
            "newest_first",
            if facts.newest_first { "true" } else { "false" },
        ));
        if let Some(cursor) = &facts.cursor {
            ensure!(cursor.len() <= 4096, "cursor exceeds budget");
            query.push(("cursor", cursor));
        }
    }
    let path = format!(
        "{}/{}",
        resource_path(&args.id)?,
        if facts.is_some() { "facts" } else { "history" }
    );
    let page = client.get(&path, &query).await?;
    let current = resource(client, &args.id).await?;
    ensure!(
        watch_identity(&current) == watch_identity(&record),
        ClientError::new(
            "STALE_GENERATION",
            "source changed during query; page discarded"
        )
    );
    Ok(
        json!({"resource_id":args.id,"revision":record["revision"],"source_identity":watch_identity(&record),"source_generation_guard":generation,"fact_run_generation_filter":fact_generation,"generation_scope":if fact_generation.is_some() { "only facts with the matching proven run_generation; unattributed facts excluded" } else { "account-attributed archive; no run generation is inferred" },"time_basis":if facts.is_some() { json!("received_at_ns inclusive") } else { Value::Null },"page":page}),
    )
}
pub async fn execute_history(client: &Client, args: HistoryArgs) -> Result<()> {
    print(client, "history", &history_page(client, &args, None).await?)
}
fn encoded_fact_cursor(value: &Value) -> Result<Option<String>> {
    let text = match value {
        Value::Null => return Ok(None),
        Value::String(text) => text.clone(),
        Value::Object(_) => serde_json::to_string(value)?,
        _ => {
            return Err(ClientError::new(
                "PROTOCOL_ERROR",
                "fact cursor must be an object, opaque text, or null",
            )
            .into());
        }
    };
    ensure!(
        !text.is_empty() && text.len() <= 4096,
        ClientError::new("PROTOCOL_ERROR", "fact cursor exceeds the supported budget")
    );
    Ok(Some(text))
}
pub async fn execute_facts(client: &Client, mut args: FactsArgs) -> Result<()> {
    ensure!(
        args.history.page.offset == 0,
        "facts uses opaque cursors; offset must be zero"
    );
    ensure!(
        (1..=100).contains(&args.max_pages),
        ClientError::new("INVALID_INPUT", "max-pages must be between 1 and 100")
    );
    ensure!(
        args.max_pages == 1 || client.options().ndjson,
        ClientError::new(
            "INVALID_INPUT",
            "fact exports with --max-pages greater than one require --ndjson"
        )
    );
    let interrupted = tokio::signal::ctrl_c();
    tokio::pin!(interrupted);
    let mut source: Option<Value> = None;
    let mut upper: Option<Value> = None;
    // Only bounded cursor metadata is retained, never previously emitted fact pages.
    let mut seen_cursors = std::collections::BTreeSet::new();
    for index in 1..=args.max_pages {
        if let Some(cursor) = &args.cursor {
            ensure!(
                seen_cursors.insert(cursor.clone()),
                ClientError::new(
                    "PROTOCOL_ERROR",
                    "fact export cursor repeated; refusing duplicate pages"
                )
            );
        }
        let mut result = tokio::select! {
            signal = &mut interrupted => { signal?; return Ok(()); }
            result = history_page(client, &args.history, Some(&args)) => result,
        }?;
        let identity = &result["source_identity"];
        ensure!(
            source.as_ref().is_none_or(|previous| previous == identity),
            ClientError::new(
                "STALE_GENERATION",
                "fact export source changed between pages; remaining export discarded"
            )
        );
        source = Some(identity.clone());
        let page_upper = result["page"]
            .get("upper_row")
            .filter(|value| value.is_string() || value.is_u64())
            .context("fact page has no archive upper bound")?;
        ensure!(
            upper.as_ref().is_none_or(|previous| previous == page_upper),
            ClientError::new(
                "STALE_HISTORY_CURSOR",
                "fact archive upper bound changed during export"
            )
        );
        upper = Some(page_upper.clone());
        let next_value = result["page"]
            .get("next_cursor")
            .context("fact page has no next_cursor field")?;
        let next = encoded_fact_cursor(next_value)?;
        ensure!(
            next.as_ref()
                .is_none_or(|cursor| !seen_cursors.contains(cursor)),
            ClientError::new("PROTOCOL_ERROR", "fact export cursor did not advance")
        );
        let has_more = next.is_some();
        result["export"] = json!({"page_index":index,"max_pages":args.max_pages,"last_emitted_page":!has_more || index == args.max_pages,"has_more":has_more,"stopped_at_page_limit":has_more && index == args.max_pages,"next_cursor":next,"scope":"fixed archive upper bound; exhausted pagination does not prove complete venue history"});
        // Freeze inferred defaults after the first page, alongside the server cursor.
        if args.history.generation.is_none() {
            args.history.generation = result["source_generation_guard"]
                .as_str()
                .map(str::to_owned);
        }
        if args.history.run_generation.is_none() {
            args.history.run_generation = result["fact_run_generation_filter"]
                .as_str()
                .map(str::to_owned);
        }
        print(client, "facts", &result)?;
        if !has_more {
            break;
        }
        args.cursor = next;
    }
    Ok(())
}
pub async fn execute_logs(client: &Client, args: LogsArgs) -> Result<()> {
    ensure!(
        !client.options().json || !args.follow || args.samples == 1,
        ClientError::new(
            "INVALID_INPUT",
            "logs --follow --json requires --samples 1; use --ndjson for multiple samples"
        )
    );
    let record = resource(client, &args.id).await?;
    is_kind(&record, &["account", "run"])?;
    let generation = pinned_identity(&record, args.generation.as_deref())?;
    let path = format!("{}/log", resource_path(&args.id)?);
    let interrupted = tokio::signal::ctrl_c();
    tokio::pin!(interrupted);
    let mut offset = u64::from(args.offset);
    for sample in 0..if args.follow { args.samples } else { 1 } {
        ensure!(
            offset <= 8_388_608,
            "log byte offset exceeds manager budget"
        );
        let read = async {
            let current = resource(client, &args.id).await?;
            ensure!(
                watch_identity(&current) == watch_identity(&record),
                ClientError::new(
                    "STALE_GENERATION",
                    "log source changed; start a new observation"
                )
            );
            let offset_text = offset.to_string();
            let page = client
                .get(
                    &path,
                    &[("offset", &offset_text), ("generation", &generation)],
                )
                .await?;
            let current = resource(client, &args.id).await?;
            ensure!(
                watch_identity(&current) == watch_identity(&record),
                ClientError::new(
                    "STALE_GENERATION",
                    "log source changed during read; page discarded"
                )
            );
            Ok::<Value, anyhow::Error>(page)
        };
        let page = tokio::select! { signal = &mut interrupted => { signal?; return Ok(()); } page = read => page, }?;
        let next = page["next_offset"]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| page["next_offset"].as_u64())
            .context("log response has no next byte offset")?;
        ensure!(
            next >= offset && next - offset <= 65_536,
            "log byte range exceeds page budget or moved backwards"
        );
        ensure!(
            page["text"]
                .as_str()
                .context("log response has no text")?
                .len()
                <= 196_608,
            "log page exceeds UTF8 replacement budget"
        );
        print(
            client,
            "logs",
            &json!({"resource_id":args.id,"generation":generation,"sample":sample,"page":page}),
        )?;
        offset = next;
        if args.follow && sample + 1 < args.samples {
            tokio::select! {
                signal = &mut interrupted => { signal?; return Ok(()); }
                _ = tokio::time::sleep(Duration::from_millis(u64::from(args.interval_ms))) => {}
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_projection_removes_private_fields_without_rewriting_facts() {
        let mut value = json!({"body":{"credential_ref":"secret://env/TEST","notes":"private","positions":[{"side":"LONG","quantity":"9007199254740993"},{"side":"SHORT","quantity":null}],"risk":null}});
        public_observation(&mut value);
        assert!(value["body"].get("credential_ref").is_none());
        assert!(value["body"].get("notes").is_none());
        assert_eq!(
            value["body"]["positions"][0]["quantity"],
            "9007199254740993"
        );
        assert_eq!(value["body"]["positions"][1]["side"], "SHORT");
        assert!(value["body"]["positions"][1]["quantity"].is_null());
        assert!(value["body"]["risk"].is_null());
    }

    #[test]
    fn watermark_keeps_server_epoch_and_sequence_separate() {
        assert_eq!(
            revision(&json!({"cursor":"epoch-a:42"})),
            Some(("epoch-a".into(), 42))
        );
        assert_eq!(
            revision(&json!({"revision":"43"})),
            Some((String::new(), 43))
        );
        assert_eq!(revision(&json!({"cursor":"invalid"})), None);
    }

    #[test]
    fn source_guard_rejects_wrong_run_or_observer_identity() {
        let run = json!({"body":{"run_generation":"2"}});
        assert!(pinned_identity(&run, Some("1")).is_err());
        assert_eq!(pinned_identity(&run, Some("2")).unwrap(), "2");
        assert!(source_identity(&json!({"body":{}})).is_err());
        assert_eq!(
            source_identity(&json!({"body":{"worker_session_id":"session-a"}})).unwrap(),
            "session-a"
        );
    }

    #[test]
    fn fact_cursor_object_is_encoded_as_lossless_query_json() {
        let cursor = json!({"store_id":"fixture-store","filter_sha256":"fixture-filter","upper_row":"18446744073709551615","after_row":"9007199254740993"});
        let encoded = encoded_fact_cursor(&cursor).unwrap().unwrap();
        assert_eq!(serde_json::from_str::<Value>(&encoded).unwrap(), cursor);
        assert_eq!(
            encoded_fact_cursor(&json!(encoded.clone())).unwrap(),
            Some(encoded)
        );
        assert_eq!(encoded_fact_cursor(&Value::Null).unwrap(), None);
    }

    #[test]
    fn fact_cursor_rejects_invalid_or_unbounded_metadata() {
        assert!(encoded_fact_cursor(&json!([1, 2])).is_err());
        assert!(encoded_fact_cursor(&json!("")).is_err());
        assert!(encoded_fact_cursor(&json!("x".repeat(4097))).is_err());
    }

    #[test]
    fn snapshot_generation_change_is_detected_by_resource_identity() {
        let old = json!({"resources":[{"id":"r","body":{"run_generation":"1"}}]});
        let new = json!({"resources":[{"id":"r","body":{"run_generation":"2"}}]});
        assert!(generations_changed(&old, &new));
        assert!(!generations_changed(&old, &old));
    }
}
