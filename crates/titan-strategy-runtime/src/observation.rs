//! Read committed typed state using its artifact-authenticated layout; never locks a strategy lane.
use crate::*;
use serde_json::{Value, json};
use titan_domain_types::json_numbers_as_strings;

fn invalid(reason: &'static str) -> StrategyError {
    StrategyError::new(
        StrategyErrorKind::InvalidDefinition,
        "business_snapshot",
        reason,
        "typed business snapshot is unavailable",
    )
}
pub(crate) fn business_view(
    snapshot: &StrategyPrivateStateSnapshot,
    schema: &Value,
) -> LocalResult<Value> {
    let mut budget = 8192;
    let mut truncated = false;
    let data = record(
        schema
            .get("fields")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("state_fields_missing"))?,
        &snapshot.state_bytes,
        0,
        0,
        &mut budget,
        &mut truncated,
    )?;
    Ok(json_numbers_as_strings(
        json!({"available":true,"source":"committed_checkpoint","checkpoint_id":snapshot.checkpoint_id.to_string(),"generation":snapshot.generation.to_string(),"event_committed_sequence":snapshot.event_committed_sequence.to_string(),"state_schema_version":snapshot.state_schema_version,"state_schema_hash":hex(&snapshot.state_schema_hash),"artifact_digest":hex(&snapshot.artifact_digest),"binding_digest":hex(&snapshot.binding_digest),"complete":!truncated,"data":data,"orders_list":snapshot.orders_list}),
    ))
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|v| format!("{v:02x}")).collect()
}
fn record(
    fields: &[Value],
    bytes: &[u8],
    base: usize,
    depth: usize,
    budget: &mut usize,
    truncated: &mut bool,
) -> LocalResult<Value> {
    if depth > 16 {
        return Err(invalid("state_nesting_limit"));
    }
    let mut output = serde_json::Map::new();
    for field in fields {
        if *budget == 0 {
            *truncated = true;
            break;
        }
        *budget -= 1;
        let name = field["name"]
            .as_str()
            .ok_or_else(|| invalid("state_field_name"))?;
        let offset = field["offset"]
            .as_u64()
            .and_then(|v| usize::try_from(v).ok())
            .and_then(|v| base.checked_add(v))
            .ok_or_else(|| invalid("state_offset"))?;
        let size = field["itemsize"]
            .as_u64()
            .and_then(|v| usize::try_from(v).ok())
            .ok_or_else(|| invalid("state_field_size"))?;
        let end = offset
            .checked_add(size)
            .ok_or_else(|| invalid("state_offset_overflow"))?;
        if end > bytes.len() {
            return Err(invalid("state_field_outside_checkpoint"));
        }
        let shape = field["shape"]
            .as_array()
            .ok_or_else(|| invalid("state_shape"))?;
        let mut count = 1usize;
        for dim in shape {
            count = count
                .checked_mul(
                    dim.as_u64()
                        .and_then(|v| usize::try_from(v).ok())
                        .ok_or_else(|| invalid("state_shape_dimension"))?,
                )
                .ok_or_else(|| invalid("state_shape_overflow"))?;
        }
        if count == 0 || size % count != 0 {
            return Err(invalid("state_array_size"));
        }
        let element = size / count;
        let value = if shape.is_empty() {
            read_element(field, bytes, offset, element, depth, budget, truncated)?
        } else {
            let limit = count.min(128);
            let mut items = Vec::with_capacity(limit);
            for i in 0..limit {
                if *budget == 0 {
                    break;
                }
                *budget -= 1;
                items.push(read_element(
                    field,
                    bytes,
                    offset + i * element,
                    element,
                    depth,
                    budget,
                    truncated,
                )?);
            }
            let omitted = count - items.len();
            if omitted > 0 {
                *truncated = true;
            }
            json!({"shape":shape,"items":items,"omitted":omitted.to_string()})
        };
        output.insert(name.into(), value);
    }
    Ok(Value::Object(output))
}
fn read_element(
    field: &Value,
    bytes: &[u8],
    at: usize,
    element: usize,
    depth: usize,
    budget: &mut usize,
    truncated: &mut bool,
) -> LocalResult<Value> {
    if let Some(fields) = field.get("fields").and_then(Value::as_array) {
        record(
            fields,
            &bytes[at..at + element],
            0,
            depth + 1,
            budget,
            truncated,
        )
    } else {
        scalar(
            field["type"]
                .as_str()
                .ok_or_else(|| invalid("state_scalar_type"))?,
            &bytes[at..at + element],
        )
    }
}
fn scalar(kind: &str, bytes: &[u8]) -> LocalResult<Value> {
    let text = match (kind.as_bytes().first().copied(), bytes.len()) {
        (Some(b'i'), 1) => i8::from_le_bytes(bytes.try_into().unwrap()).to_string(),
        (Some(b'i'), 2) => i16::from_le_bytes(bytes.try_into().unwrap()).to_string(),
        (Some(b'i'), 4) => i32::from_le_bytes(bytes.try_into().unwrap()).to_string(),
        (Some(b'i'), 8) => i64::from_le_bytes(bytes.try_into().unwrap()).to_string(),
        (Some(b'u'), 1) => u8::from_le_bytes(bytes.try_into().unwrap()).to_string(),
        (Some(b'u'), 2) => u16::from_le_bytes(bytes.try_into().unwrap()).to_string(),
        (Some(b'u'), 4) => u32::from_le_bytes(bytes.try_into().unwrap()).to_string(),
        (Some(b'u'), 8) => u64::from_le_bytes(bytes.try_into().unwrap()).to_string(),
        (Some(b'f'), 4) => {
            let v = f32::from_le_bytes(bytes.try_into().unwrap());
            if !v.is_finite() {
                return Err(invalid("non_finite_business_state"));
            }
            v.to_string()
        }
        (Some(b'f'), 8) => {
            let v = f64::from_le_bytes(bytes.try_into().unwrap());
            if !v.is_finite() {
                return Err(invalid("non_finite_business_state"));
            }
            v.to_string()
        }
        (Some(b'b'), 1) => {
            return match bytes[0] {
                0 => Ok(false.into()),
                1 => Ok(true.into()),
                _ => Err(invalid("invalid_business_boolean")),
            };
        }
        _ => return Err(invalid("unsupported_business_scalar")),
    };
    Ok(text.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn typed_observation_preserves_u64_and_rejects_bad_offsets() {
        let fields =
            vec![json!({"name":"identity","offset":0,"itemsize":8,"type":"u8:little","shape":[]})];
        let bytes = u64::MAX.to_le_bytes();
        let mut budget = 10;
        let mut truncated = false;
        assert_eq!(
            record(&fields, &bytes, 0, 0, &mut budget, &mut truncated).unwrap()["identity"],
            u64::MAX.to_string()
        );
        let bad = vec![json!({"name":"bad","offset":4,"itemsize":8,"type":"u8:little","shape":[]})];
        assert!(record(&bad, &bytes, 0, 0, &mut budget, &mut truncated).is_err());
    }
    #[test]
    fn unsupported_or_non_finite_state_is_never_displayed_as_zero() {
        assert!(scalar("f8:little", &f64::NAN.to_le_bytes()).is_err());
        assert!(scalar("b1:not_applicable", &[2]).is_err());
    }
    #[test]
    fn array_display_is_explicitly_bounded() {
        let fields = vec![
            json!({"name":"levels","offset":0,"itemsize":256,"type":"u1:not_applicable","shape":[256]}),
        ];
        let mut budget = 1000;
        let mut truncated = false;
        let view = record(&fields, &[1; 256], 0, 0, &mut budget, &mut truncated).unwrap();
        assert!(truncated);
        assert_eq!(view["levels"]["items"].as_array().unwrap().len(), 128);
        assert_eq!(view["levels"]["omitted"], "128");
    }
}
