//! Read-only adaptation of historical management records. Never rewrites frozen
//! inputs, signed files, receipts, operation IDs or account order namespaces.
use super::contract::Resource;
use serde_json::Value;

pub(super) fn kind(value: &str) -> &str {
    match value {
        "artifact" => "strategy_version",
        "version" => "parameter_version",
        "template" => "parameter_preset",
        other => other,
    }
}
// Resource identifiers are opaque and stable across the terminology migration.
pub(super) fn id_tag(value: &str) -> &str {
    match value {
        "strategy_version" => "artifact",
        "parameter_version" => "version",
        "parameter_preset" => "template",
        other => other,
    }
}
fn rename(body: &mut serde_json::Map<String, Value>, old: &str, new: &str) {
    if let Some(value) = body.remove(old) {
        body.entry(new.to_owned()).or_insert(value);
    }
}
pub(super) fn resource(mut resource: Resource) -> Resource {
    resource.kind = kind(&resource.kind).to_owned();
    if let Some(body) = resource.body.as_object_mut() {
        if matches!(resource.kind.as_str(), "run" | "deployment")
            && (body.contains_key("research_id")
                || (!body.contains_key("runtime_strategy_no")
                    && body
                        .get("strategy_id")
                        .and_then(Value::as_str)
                        .is_some_and(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))))
        {
            rename(body, "strategy_id", "runtime_strategy_no");
        }
        let source_key = if body.get("mode").and_then(Value::as_str) == Some("observe") {
            "worker_session_id"
        } else {
            "run_generation"
        };
        rename(body, "generation", source_key);
        for (old, new) in [
            ("research_id", "strategy_id"),
            ("research_name", "strategy_name"),
            ("artifact_id", "strategy_version_id"),
            ("version_id", "parameter_version_id"),
            ("template_id", "parameter_preset_id"),
            ("artifact_digest", "compiled_strategy_digest"),
            ("blob_sha256", "strategy_file_sha256"),
            ("checkpoint_id", "recovery_point_sha256"),
        ] {
            rename(body, old, new);
        }
    }
    resource
}

// The old HTTP namespace is a read/write codec over the same application, not a
// second model. Downloaded file bytes are deliberately excluded by the adapter.
pub(super) fn response(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items {
                response(item);
            }
        }
        Value::Object(object) => {
            let resource_kind = if object.contains_key("id") && object.contains_key("body") {
                object
                    .get("kind")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            } else {
                None
            };
            for child in object.values_mut() {
                response(child);
            }
            if let Some(kind) = resource_kind {
                if let Some(body) = object.get_mut("body").and_then(Value::as_object_mut) {
                    for (new, old) in [
                        ("strategy_id", "research_id"),
                        ("strategy_name", "research_name"),
                        ("runtime_strategy_no", "strategy_id"),
                        ("strategy_version_id", "artifact_id"),
                        ("parameter_version_id", "version_id"),
                        ("parameter_preset_id", "template_id"),
                        ("compiled_strategy_digest", "artifact_digest"),
                        ("strategy_file_sha256", "blob_sha256"),
                        ("run_generation", "generation"),
                        ("worker_session_id", "generation"),
                        ("recovery_point_sha256", "checkpoint_id"),
                    ] {
                        rename(body, new, old);
                    }
                }
                object.insert("kind".into(), id_tag(&kind).into());
            } else if object.contains_key("runtime_strategy_no")
                && object.contains_key("business_observation")
            {
                rename(object, "runtime_strategy_no", "strategy_id");
                rename(object, "run_generation", "generation");
                rename(object, "compiled_strategy_digest", "artifact_digest");
                object.remove("business_observation");
                object.insert("business".into(),serde_json::json!({"available":false,"reason":"use API v2 business observation protocol"}));
            } else if object.contains_key("kind") {
                if object.get("kind").and_then(Value::as_str) == Some("compile") {
                    object.insert("kind".into(), "build".into());
                }
                if object.get("kind").and_then(Value::as_str) == Some("restore") {
                    object.insert("kind".into(), "recover".into());
                }
                for (new, old) in [
                    ("parameter_preset_id", "template_id"),
                    ("parameter_version_id", "version_id"),
                    ("strategy_version_id", "artifact_id"),
                    ("expected_run_generation", "generation"),
                    ("recovery_point_sha256", "checkpoint_id"),
                ] {
                    rename(object, new, old);
                }
            }
            if object.get("api_version").and_then(Value::as_u64) == Some(2) {
                object.insert("api_version".into(), 1.into());
            }
        }
        _ => {}
    }
}

// Retry compatibility is allowed only when the stored hash proves the exact
// historical request spelling, never merely because the parsed actions match.
pub(super) fn request_digest(input: &super::contract::SubmitOperation) -> anyhow::Result<String> {
    let mut value = serde_json::to_value(input)?;
    response(&mut value);
    Ok(super::digest(&titan_strategy_runtime::compiler_json_v13(
        &value,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn old_resource_identity_and_frozen_digest_survive_read_projection() {
        let old: Resource=serde_json::from_value(serde_json::json!({"id":"n-artifact-1","kind":"artifact","node_id":"n","revision":"1","frozen":true,"digest":"original","body":{"research_id":"research-grid","artifact_digest":"signed","version_id":"p"}})).unwrap();
        assert_eq!(old.id, "n-artifact-1");
        assert_eq!(old.digest, "original");
        assert_eq!(old.kind, "strategy_version");
        assert_eq!(old.body["strategy_id"], "research-grid");
        assert_eq!(old.body["parameter_version_id"], "p");
        assert!(old.body.get("research_id").is_none());
    }
    #[test]
    fn v1_view_keeps_ids_and_maps_back_to_the_same_strategy_record() {
        let original = serde_json::json!({"id":"node-artifact-one","kind":"strategy_version","node_id":"node","revision":"7","frozen":true,"digest":"original","body":{"strategy_id":"grid-main","strategy_name":"网格","compiled_strategy_digest":"signed","parameter_version_id":"p"}});
        let mut legacy = original.clone();
        response(&mut legacy);
        assert_eq!(legacy["kind"], "artifact");
        assert_eq!(legacy["id"], original["id"]);
        assert_eq!(legacy["body"]["research_id"], "grid-main");
        let restored: Resource = serde_json::from_value(legacy).unwrap();
        assert_eq!(serde_json::to_value(restored).unwrap(), original);
    }
}
