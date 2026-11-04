//! Bounded terminal summaries. Machine output remains the original versioned envelope.
use serde_json::Value;
use std::io::{self, Write};

fn text(value: &Value) -> String {
    let raw = match value {
        Value::Null => return "—".into(),
        Value::String(value) => value.clone(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::Array(values) => return format!("{} items", values.len()),
        Value::Object(_) => return "details (--json)".into(),
    };
    let mut chars = raw.chars();
    let mut result = chars
        .by_ref()
        .take(120)
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect::<String>();
    if chars.next().is_some() {
        result.push('…');
    }
    result
}
fn private(key: &str) -> bool {
    matches!(
        key,
        "credential_ref"
            | "password"
            | "password_hash"
            | "token"
            | "csrf_token"
            | "owner_token"
            | "notes"
            | "runtime_config"
            | "source"
            | "initial_state"
            | "recovery_state"
            | "dsn"
    )
}
fn field<'a>(value: &'a Value, names: &[&str]) -> &'a Value {
    for name in names {
        if let Some(found) = value
            .get(*name)
            .or_else(|| value.get("body").and_then(|body| body.get(*name)))
            && !found.is_null()
        {
            return found;
        }
    }
    &Value::Null
}
fn table(output: &mut impl Write, values: &[Value]) -> io::Result<()> {
    if values.is_empty() {
        return writeln!(output, "No records.");
    }
    let resources = values.iter().any(|value| value.get("id").is_some());
    if resources {
        writeln!(
            output,
            "{:<28} {:<18} {:<16} {:<18} GENERATION",
            "NAME / ID", "TYPE", "STATE", "NODE"
        )?;
        for value in values.iter().take(200) {
            writeln!(
                output,
                "{:<28} {:<18} {:<16} {:<18} {}",
                text(field(value, &["name", "strategy_key", "id"])),
                text(field(value, &["kind", "type"])),
                text(field(
                    value,
                    &["lifecycle", "state", "process_state", "connection_state"]
                )),
                text(field(value, &["node_id"])),
                text(field(value, &["run_generation", "generation"]))
            )?;
        }
    } else {
        for (index, value) in values.iter().take(200).enumerate() {
            if index != 0 {
                writeln!(output)?;
            }
            detail(output, value, 0)?;
        }
    }
    if values.len() > 200 {
        writeln!(
            output,
            "Additional records omitted; use --json for complete output."
        )?;
    }
    Ok(())
}
fn detail(output: &mut impl Write, value: &Value, depth: usize) -> io::Result<()> {
    match value {
        Value::Array(values) => table(output, values),
        Value::Object(values) => {
            for (key, value) in values.iter().filter(|(key, _)| !private(key)).take(64) {
                let label = text(&Value::String(key.clone()));
                if depth < 2 && value.is_object() {
                    writeln!(output, "{}{}:", "  ".repeat(depth), label)?;
                    detail(output, value, depth + 1)?;
                } else {
                    writeln!(
                        output,
                        "{}{:<24} {}",
                        "  ".repeat(depth),
                        label,
                        text(value)
                    )?;
                }
            }
            Ok(())
        }
        _ => writeln!(output, "{}", text(value)),
    }
}
pub(super) fn write(output: &mut impl Write, kind: &str, data: &Value) -> io::Result<()> {
    if kind == "logs"
        && let Some(value) = data
            .get("page")
            .and_then(|page| page.get("text"))
            .and_then(Value::as_str)
    {
        for c in value.chars() {
            if c == '\n' || c == '\t' || !c.is_control() {
                write!(output, "{c}")?;
            }
        }
        if !value.ends_with('\n') {
            writeln!(output)?;
        }
        return Ok(());
    }
    if kind == "account_projection" {
        writeln!(
            output,
            "{} — {}",
            text(&data["field"]),
            text(field(data, &["account", "resource_id"]))
        )?;
        writeln!(
            output,
            "Available: {}  Stale: {}",
            text(&data["available"]),
            text(&data["stale"])
        )?;
        let projection = &data["data"];
        if let Some(items) = projection.get("items").and_then(Value::as_array) {
            return table(output, items);
        }
        if projection.is_null() {
            return writeln!(output, "{}", text(&data["reason"]));
        }
        return detail(output, projection, 0);
    }
    if kind == "research" {
        writeln!(output, "Research: {}", text(&data["stage"]))?;
        writeln!(output, "State:    {}", text(&data["operation"]["state"]))?;
        if let Some(resource) = data.get("resource").filter(|value| !value.is_null()) {
            writeln!(
                output,
                "Result:   {} ({})",
                text(&resource["id"]),
                text(&resource["kind"])
            )?;
        }
        if data["complete"] != true {
            writeln!(
                output,
                "The pipeline is unfinished. Repeat the same command to follow its original task."
            )?;
        }
        return Ok(());
    }
    if kind == "operation" {
        writeln!(
            output,
            "Action:    {}",
            text(field(&data["action"], &["kind", "type", "action"]))
        )?;
        writeln!(output, "State:     {}", text(&data["state"]))?;
        writeln!(output, "Node:      {}", text(&data["node_id"]))?;
        if !data["error"].is_null() {
            writeln!(output, "Detail:    {}", text(&data["error"]))?;
        }
        if data.get("result").is_some_and(|value| !value.is_null()) {
            writeln!(output, "Result:")?;
            detail(output, &data["result"], 1)?;
        }
        return Ok(());
    }
    if let Some(values) = data.as_array() {
        return table(output, values);
    }
    for key in ["items", "resources", "strategies"] {
        if let Some(values) = data.get(key).and_then(Value::as_array) {
            table(output, values)?;
            if !data["has_more"].is_null() {
                writeln!(output, "More records: {}", text(&data["has_more"]))?;
            }
            return Ok(());
        }
    }
    detail(output, data, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn terminal_keys_and_values_are_safe_without_recomputing_facts() {
        let data = serde_json::json!({"\u{001b}[31mkey": "\u{001b}[2Jvalue", "amount":"9007199254740993.00000001", "missing":null});
        let mut bytes = Vec::new();
        write(&mut bytes, "result", &data).unwrap();
        let rendered = String::from_utf8(bytes).unwrap();
        assert!(!rendered.contains('\u{1b}'));
        assert!(rendered.contains("9007199254740993.00000001"));
        assert!(rendered.contains('—'));
    }
}
