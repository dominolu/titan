//! Full native HTTP API. Portable BrokerApi remains the stricter Core perpetual adapter.
include!("native_generated.rs");
pub type AuthorizationProvider = Arc<dyn Fn() -> Result<String, ApiError> + Send + Sync>;
pub const WEBSOCKET_CHANNELS: &[&str] = &[
    "order_book/{market}",
    "ticker/{market}",
    "market_stats/{market|all}",
    "bo_market_stats/{market|all}",
    "trade/{market}",
    "candle/{market}/{resolution}",
    "up_down_candle/{symbol}/{duration}/{resolution}",
    "mark_price_candle/{market}/{resolution}",
    "account_all/{account}",
    "account_market/{market}/{account}",
    "user_stats/{account}",
    "account_tx/{account}",
    "account_all_orders/{account}",
    "height",
    "pool_data/{account}",
    "pool_info/{account}",
    "notification/{account}",
    "account_orders/{market}/{account}",
    "account_all_trades/{account}",
    "account_all_positions/{account}",
    "spot_market_stats/{market|all}",
    "account_all_assets/{account}",
    "account_spot_avg_entry_prices/{account}",
    "rfq",
    "live_points/{account}",
];
pub struct LighterNativeWebSocket {
    pub socket: crate::native_ws::NativeSocket,
    authorization: Option<AuthorizationProvider>,
}
impl LighterNativeWebSocket {
    /// Extra native fields (e.g. auth, initial snapshot options) retain their exact wire types.
    pub async fn subscribe(
        &mut self,
        channel: &str,
        mut parameters: serde_json::Map<String, Value>,
    ) -> Result<(), ApiError> {
        if channel.is_empty()
            || channel.chars().any(char::is_control)
            || parameters.contains_key("type")
            || parameters.contains_key("channel")
        {
            return Err(invalid(NAME, "invalid native subscription"));
        }
        parameters.insert("type".into(), json!("subscribe"));
        parameters.insert("channel".into(), json!(channel));
        if !parameters.contains_key("auth")
            && (channel.starts_with("account_")
                || channel.starts_with("user_stats/")
                || channel.starts_with("notification/")
                || channel == "rfq"
                || channel.starts_with("live_points/"))
            && let Some(provider) = &self.authorization
        {
            parameters.insert("auth".into(), json!(provider()?));
        }
        self.socket.send_json(&Value::Object(parameters)).await
    }
    pub async fn unsubscribe(&mut self, channel: &str) -> Result<(), ApiError> {
        self.socket
            .send_json(&json!({"type":"unsubscribe","channel":channel}))
            .await
    }
    /// Submission only. The caller owns the explicit nonce and correlates the returned RPC frame.
    pub async fn send_transaction(
        &mut self,
        id: &str,
        transaction: &super::signer::NativeSignedTransaction,
        guard: Option<SubmitGuard>,
    ) -> Result<(), ApiError> {
        let info: Value = serde_json::from_str(&transaction.tx_info)
            .map_err(|_| invalid(NAME, "invalid signed transaction JSON"))?;
        let request = json!({"type":"jsonapi/sendtx","data":{"id":id,"tx_type":transaction.tx_type,"tx_info":info}});
        check_submit_guard(&guard)?;
        self.socket.send_json(&request).await
    }
    pub async fn send_transactions(
        &mut self,
        id: &str,
        transactions: &[super::signer::NativeSignedTransaction],
        guard: Option<SubmitGuard>,
    ) -> Result<(), ApiError> {
        if transactions.is_empty() {
            return Err(invalid(NAME, "empty native transaction batch"));
        }
        let types: Vec<_> = transactions.iter().map(|t| t.tx_type).collect();
        let infos: Vec<_> = transactions.iter().map(|t| t.tx_info.as_str()).collect();
        let request = json!({"type":"jsonapi/sendtxbatch","data":{"id":id,"tx_types":serde_json::to_string(&types).map_err(|_|invalid(NAME,"invalid native transaction types"))?,"tx_infos":serde_json::to_string(&infos).map_err(|_|invalid(NAME,"invalid native transaction list"))?}});
        check_submit_guard(&guard)?;
        self.socket.send_json(&request).await
    }
}
#[derive(Clone)]
pub struct LighterNativeClient {
    http: reqwest::Client,
    core_url: String,
    explorer_url: String,
    authorization: Option<AuthorizationProvider>,
}
impl LighterNativeClient {
    pub async fn websocket(
        &self,
        ws_url: &str,
        readonly: bool,
    ) -> Result<LighterNativeWebSocket, ApiError> {
        endpoint(ws_url, true, NAME)?;
        let mut url =
            reqwest::Url::parse(ws_url).map_err(|_| invalid(NAME, "invalid native WS URL"))?;
        if readonly {
            url.query_pairs_mut().append_pair("readonly", "true");
        }
        Ok(LighterNativeWebSocket {
            socket: crate::native_ws::NativeSocket::connect(NAME, url.as_str()).await?,
            authorization: self.authorization.clone(),
        })
    }
    pub fn new(core_url: &str, explorer_url: &str) -> Result<Self, ApiError> {
        endpoint(core_url, false, NAME)?;
        if !explorer_url.is_empty() {
            endpoint(explorer_url, false, NAME)?;
        }
        Ok(Self {
            http: http_client(NAME)?,
            core_url: core_url.trim_end_matches('/').into(),
            explorer_url: explorer_url.trim_end_matches('/').into(),
            authorization: None,
        })
    }
    /// Token generation is evaluated per call, so callers can supply expiring session/API tokens.
    pub fn with_authorization(mut self, provider: AuthorizationProvider) -> Self {
        self.authorization = Some(provider);
        self
    }
    pub async fn native_request(
        &self,
        operation: LighterOperation,
        request: NativeRequest,
        guard: Option<SubmitGuard>,
    ) -> Result<NativeResponse, ApiError> {
        let operation = operation.metadata();
        let explorer = operation.family == "explorer";
        let server = if explorer {
            &self.explorer_url
        } else {
            &self.core_url
        };
        if server.is_empty() {
            return Err(unsupported(
                NAME,
                "Explorer endpoint for this deployment is not configured",
            ));
        }
        let documented = operation.document["servers"]
            .as_array()
            .is_some_and(|servers| {
                servers.iter().any(|s| {
                    s["url"]
                        .as_str()
                        .is_some_and(|s| s.trim_end_matches('/') == server)
                })
            });
        let loopback = reqwest::Url::parse(server).is_ok_and(|u| {
            matches!(
                u.host_str(),
                Some("127.0.0.1" | "localhost" | "::1" | "[::1]")
            )
        });
        if !documented && !loopback {
            return Err(unsupported(
                NAME,
                "native operation on this deployment; see operation servers",
            ));
        }
        let prepared = crate::native_api::prepare(NAME, server, &operation, request)?;
        let mut request = crate::native_api::build(&self.http, &operation, &prepared, NAME)?;
        if !explorer {
            if let Some(provider) = &self.authorization {
                request = request.header("Authorization", provider()?);
            } else if operation.parameters().any(|p| {
                p["name"]
                    .as_str()
                    .is_some_and(|n| n.eq_ignore_ascii_case("Authorization"))
                    && p["required"] == true
            }) {
                return Err(invalid(NAME, "native authorization required"));
            }
        }
        crate::native_api::send(NAME, &operation, request, guard).await
    }
}
impl Default for LighterNativeClient {
    fn default() -> Self {
        Self::new(
            "https://mainnet.zklighter.elliot.ai",
            "https://explorer.elliot.ai/api",
        )
        .expect("valid official endpoints")
    }
}
impl LighterClient {
    /// The native HTTP facade shares the configured token signer, but owns no automatic nonce.
    /// Pre-signed sendTx/sendTxBatch calls require exclusive nonce ownership by their caller.
    pub fn native(&self) -> Result<LighterNativeClient, ApiError> {
        let explorer = if self.config.chain_id == 304 {
            "https://explorer.elliot.ai/api"
        } else {
            ""
        };
        let mut native = LighterNativeClient::new(&self.config.rest_url, explorer)?;
        if let Some(signer) = &self.signer {
            let signer = signer.clone();
            native = native.with_authorization(Arc::new(move || signer.auth()));
        }
        Ok(native)
    }
}
