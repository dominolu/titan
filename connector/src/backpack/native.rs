//! Official native API surface, including spot, RFQ, lending and wallet operations.
//! These calls preserve native IDs and units and do not synthesize Titan execution facts.
include!("native_generated.rs");

pub const WEBSOCKET_STREAMS: &[&str] = &[
    "account.balanceUpdate",
    "account.fundingPayment[.<symbol>]",
    "account.orderUpdate[.<symbol>]",
    "account.positionUpdate[.<symbol>]",
    "account.rfqUpdate[.<symbol>]",
    "account.rfq[.<symbol>]",
    "bookTicker.<symbol>",
    "depth.<symbol>",
    "depth.200ms.<symbol>",
    "depth.600ms.<symbol>",
    "depth.1000ms.<symbol>",
    "kline.<interval>.<symbol>",
    "liquidation.<symbol>",
    "markPrice.<symbol>",
    "ticker.<symbol>",
    "externalTicker.<symbol>",
    "stockPrice.<ticker>",
    "stockIndicativeQuote.<ticker>",
    "openInterest.<symbol>",
    "trade.<symbol>",
];
pub struct BackpackNativeWebSocket {
    pub socket: crate::native_ws::NativeSocket,
    client: BackpackClient,
}
impl BackpackNativeWebSocket {
    pub async fn subscribe(&mut self, streams: &[String]) -> Result<(), ApiError> {
        let message = self.client.native_subscription_message(true, streams)?;
        self.socket.send_json(&message).await
    }
    pub async fn unsubscribe(&mut self, streams: &[String]) -> Result<(), ApiError> {
        let message = self.client.native_subscription_message(false, streams)?;
        self.socket.send_json(&message).await
    }
}

impl BackpackClient {
    pub async fn native_websocket(
        &self,
        include_verifying_key: bool,
    ) -> Result<BackpackNativeWebSocket, ApiError> {
        let mut url = reqwest::Url::parse(&self.config.ws_url)
            .map_err(|_| invalid(NAME, "invalid native WS URL"))?;
        if include_verifying_key {
            url.query_pairs_mut()
                .append_pair("includeVerifyingKey", "true");
        }
        Ok(BackpackNativeWebSocket {
            socket: crate::native_ws::NativeSocket::connect(NAME, url.as_str()).await?,
            client: self.clone(),
        })
    }
    pub fn native_subscription_message(
        &self,
        subscribe: bool,
        streams: &[String],
    ) -> Result<Value, ApiError> {
        if streams.is_empty()
            || streams
                .iter()
                .any(|s| s.is_empty() || s.chars().any(char::is_control))
        {
            return Err(invalid(NAME, "nonempty native streams required"));
        }
        let mut message =
            json!({"method":if subscribe{"SUBSCRIBE"}else{"UNSUBSCRIBE"},"params":streams});
        if streams.iter().any(|s| s.starts_with("account.")) {
            let timestamp = Utc::now().timestamp_millis();
            message["signature"] = json!([
                self.config.api_key,
                self.signature("subscribe", &json!({}), timestamp)?,
                timestamp.to_string(),
                self.config.window_ms.to_string()
            ]);
        }
        Ok(message)
    }
    fn native_signature(
        &self,
        instruction: &str,
        body: Option<&Value>,
        query: &[(String, String)],
        timestamp: i64,
    ) -> Result<String, ApiError> {
        let key = self
            .key
            .as_ref()
            .ok_or_else(|| invalid(NAME, "credentials required"))?;
        let mut parameter_sets = if let Some(body) = body {
            let orders: Vec<&Value> = if let Some(batch) = body.as_array() {
                if instruction != "orderExecute" || batch.is_empty() {
                    return Err(invalid(NAME, "invalid native signing batch"));
                }
                batch.iter().collect()
            } else {
                vec![body]
            };
            orders
                .into_iter()
                .map(|order| {
                    order
                        .as_object()
                        .ok_or_else(|| invalid(NAME, "native signed parameters must be object"))?
                        .iter()
                        // The official withdrawal contract explicitly excludes this object from
                        // signing. Keep it intact in the JSON body sent to the exchange.
                        .filter(|(k, _)| {
                            instruction != "withdraw" || k.as_str() != "recipientInformation"
                        })
                        .map(|(k, v)| {
                            let value = match v {
                                Value::String(s) => s.clone(),
                                Value::Number(n) => n.to_string(),
                                Value::Bool(b) => b.to_string(),
                                Value::Null => "null".into(),
                                _ => {
                                    return Err(invalid(
                                        NAME,
                                        "unsupported nested native signing parameter",
                                    ));
                                }
                            };
                            Ok((k.clone(), value))
                        })
                        .collect::<Result<Vec<_>, ApiError>>()
                })
                .collect::<Result<Vec<_>, ApiError>>()?
        } else {
            // A multimap is required: marketType arrays expand into repeated query keys.
            vec![query.to_vec()]
        };
        let mut chunks = Vec::new();
        for params in &mut parameter_sets {
            // Stable sorting retains the wire order among repeated values of the same key.
            params.sort_by(|a, b| a.0.cmp(&b.0));
            let mut text = format!("instruction={instruction}");
            for (k, value) in params {
                text.push_str(&format!("&{k}={value}"));
            }
            chunks.push(text);
        }
        let message = format!(
            "{}&timestamp={timestamp}&window={}",
            chunks.join("&"),
            self.config.window_ms
        );
        Ok(STANDARD.encode(key.sign(message.as_bytes()).to_bytes()))
    }
    pub async fn native_request(
        &self,
        operation: BackpackOperation,
        mut request: NativeRequest,
        guard: Option<SubmitGuard>,
    ) -> Result<NativeResponse, ApiError> {
        // The official withdrawal example/default permits null, but its OpenAPI allOf
        // omits nullable. Validate absence of this optional field, then restore the wire body.
        let null_recipient = operation == BackpackOperation::RequestWithdrawal
            && request
                .body
                .as_ref()
                .and_then(|b| b.get("recipientInformation"))
                == Some(&Value::Null);
        if null_recipient {
            request
                .body
                .as_mut()
                .and_then(Value::as_object_mut)
                .unwrap()
                .remove("recipientInformation");
        }
        let operation = operation.metadata();
        let mut prepared =
            crate::native_api::prepare(NAME, &self.config.rest_url, &operation, request)?;
        if null_recipient {
            prepared.body.as_mut().unwrap()["recipientInformation"] = Value::Null;
        }
        let mut request = crate::native_api::build(&self.http, &operation, &prepared, NAME)?;
        if let Some(instruction) = operation.instruction {
            let timestamp = Utc::now().timestamp_millis();
            request = request
                .header("X-API-Key", &self.config.api_key)
                .header("X-Timestamp", timestamp.to_string())
                .header("X-Window", self.config.window_ms.to_string())
                .header(
                    "X-Signature",
                    self.native_signature(
                        instruction,
                        prepared.body.as_ref(),
                        &prepared.query,
                        timestamp,
                    )?,
                );
        }
        crate::native_api::send(NAME, &operation, request, guard).await
    }
}
