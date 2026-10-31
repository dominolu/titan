//! ABI pinned to elliottech/lighter-go aefa968 (64-bit market indexes, all native transactions).
//! Only a locally configured, SHA256-verified library is loaded; no runtime downloads.
use super::*;
use libloading::Library;
use sha2::{Digest, Sha256};
use std::{
    ffi::{CStr, CString, c_char, c_int, c_void},
    sync::{OnceLock, Weak},
};
#[repr(C)]
struct StrOrErr {
    value: *mut c_char,
    err: *mut c_char,
}
#[repr(C)]
struct SignedResponse {
    kind: u8,
    info: *mut c_char,
    hash: *mut c_char,
    message: *mut c_char,
    err: *mut c_char,
}
type Create = unsafe extern "C" fn(*const c_char, *const c_char, c_int, c_int, i64) -> *mut c_char;
type Auth = unsafe extern "C" fn(i64, c_int, i64) -> StrOrErr;
type Free = unsafe extern "C" fn(*mut c_void);
type CreateOrder = unsafe extern "C" fn(
    i64,
    i64,
    i64,
    c_int,
    c_int,
    c_int,
    c_int,
    c_int,
    c_int,
    i64,
    i64,
    c_int,
    c_int,
    u8,
    u8,
    u8,
    i64,
    c_int,
    i64,
) -> SignedResponse;
type Cancel = unsafe extern "C" fn(i64, i64, u8, i64, c_int, i64) -> SignedResponse;
type Modify = unsafe extern "C" fn(
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    c_int,
    c_int,
    u8,
    u8,
    u8,
    i64,
    i64,
    c_int,
    i64,
) -> SignedResponse;
struct Functions {
    _library: Library,
    create: Create,
    auth: Auth,
    free: Free,
    order: CreateOrder,
    cancel: Cancel,
    modify: Modify,
}
struct Registry {
    identity: String,
    functions: Arc<Functions>,
    accounts: HashMap<(i64, u8), (String, Weak<SignerAccount>)>,
}
static REGISTRY: OnceLock<Mutex<Option<Registry>>> = OnceLock::new();
#[derive(Default)]
pub(super) struct NonceState {
    pub next: Option<i64>,
    pub uncertain: bool,
}
pub(super) struct SignerAccount {
    functions: Arc<Functions>,
    pub nonce: tokio::sync::Mutex<NonceState>,
    account: i64,
    key: u8,
}
pub(super) struct OrderParameters {
    pub market: i64,
    pub id: i64,
    pub amount: i64,
    pub price: u32,
    pub ask: bool,
    pub tif: i32,
    pub reduce: bool,
}
pub(super) struct Signed {
    pub kind: u8,
    pub info: String,
}
impl SignerAccount {
    pub fn load(c: &Config) -> Result<Arc<Self>, ApiError> {
        let path = c
            .signer_path
            .as_deref()
            .ok_or_else(|| invalid(NAME, "signer_path required"))?;
        let sha = c
            .signer_sha256
            .as_deref()
            .ok_or_else(|| invalid(NAME, "signer_sha256 required"))?;
        if !std::path::Path::new(path).is_absolute() || sha.len() != 64 {
            return Err(invalid(NAME, "absolute signer path and SHA256 required"));
        }
        let bytes = std::fs::read(path).map_err(|_| invalid(NAME, "cannot read signer library"))?;
        let digest = hex::encode(Sha256::digest(bytes));
        if digest != sha {
            return Err(invalid(NAME, "signer SHA256 mismatch"));
        }
        let identity = format!("{path}:{sha}:{}:{}", c.chain_id, c.rest_url);
        let mut registry = REGISTRY
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if registry.as_ref().is_some_and(|r| r.identity != identity) {
            return Err(invalid(
                NAME,
                "one signer build/network per process is supported",
            ));
        }
        if registry.is_none() {
            // SAFETY: the administrator supplies a pinned library and verifies its digest. Symbols below
            // use the pinned 64-bit-index C ABI; the library is kept alive by Functions.
            let functions = unsafe {
                let library =
                    Library::new(path).map_err(|_| invalid(NAME, "signer load failed"))?;
                library.get::<unsafe extern "C" fn(*const c_void,u8,i64,c_int,i64)->SignedResponse>(b"SignBinaryOptionsOperatorSettleMarket\0")
                    .map_err(|_|invalid(NAME,"signer ABI is too old; rebuild the pinned 64-bit library"))?;
                let create = *library
                    .get::<Create>(b"CreateClient\0")
                    .map_err(|_| invalid(NAME, "missing CreateClient"))?;
                let auth = *library
                    .get::<Auth>(b"CreateAuthToken\0")
                    .map_err(|_| invalid(NAME, "missing CreateAuthToken"))?;
                let free = *library
                    .get::<Free>(b"Free\0")
                    .map_err(|_| invalid(NAME, "missing Free"))?;
                let order = *library
                    .get::<CreateOrder>(b"SignCreateOrder\0")
                    .map_err(|_| invalid(NAME, "missing SignCreateOrder"))?;
                let cancel = *library
                    .get::<Cancel>(b"SignCancelOrder\0")
                    .map_err(|_| invalid(NAME, "missing SignCancelOrder"))?;
                let modify = *library
                    .get::<Modify>(b"SignModifyOrder\0")
                    .map_err(|_| invalid(NAME, "missing SignModifyOrder"))?;
                Functions {
                    _library: library,
                    create,
                    auth,
                    free,
                    order,
                    cancel,
                    modify,
                }
            };
            *registry = Some(Registry {
                identity,
                functions: Arc::new(functions),
                accounts: HashMap::new(),
            });
        }
        let r = registry.as_mut().unwrap();
        let key_digest = hex::encode(Sha256::digest(c.api_private_key.as_bytes()));
        if let Some((old, account)) = r.accounts.get(&(c.account_index, c.api_key_index)) {
            if *old != key_digest {
                return Err(invalid(NAME, "account API key changed within process"));
            }
            if let Some(account) = account.upgrade() {
                return Ok(account);
            }
        }
        let url = CString::new(c.rest_url.as_str()).map_err(|_| invalid(NAME, "invalid URL"))?;
        let key = CString::new(c.api_private_key.as_str())
            .map_err(|_| invalid(NAME, "invalid private key"))?;
        let e = unsafe {
            (r.functions.create)(
                url.as_ptr(),
                key.as_ptr(),
                c.chain_id,
                c.api_key_index as c_int,
                c.account_index,
            )
        };
        if !e.is_null() {
            unsafe { (r.functions.free)(e.cast()) };
            return Err(invalid(NAME, "official signer rejected configuration"));
        }
        let account = Arc::new(Self {
            functions: r.functions.clone(),
            nonce: tokio::sync::Mutex::new(NonceState::default()),
            account: c.account_index,
            key: c.api_key_index,
        });
        r.accounts.insert(
            (c.account_index, c.api_key_index),
            (key_digest, Arc::downgrade(&account)),
        );
        Ok(account)
    }
    unsafe fn string(&self, p: *mut c_char) -> Result<Option<String>, ApiError> {
        if p.is_null() {
            return Ok(None);
        }
        let value = unsafe { CStr::from_ptr(p) }
            .to_str()
            .map(String::from)
            .map_err(|_| error(NAME, "invalid signer UTF-8"));
        unsafe { (self.functions.free)(p.cast()) };
        value.map(Some)
    }
    fn signed(&self, r: SignedResponse) -> Result<Signed, ApiError> {
        let info = unsafe { self.string(r.info) };
        let hash = unsafe { self.string(r.hash) };
        let message = unsafe { self.string(r.message) };
        let err = unsafe { self.string(r.err) };
        // Always free every allocation, even if one field is invalid. Never return signer error text.
        let info = info?;
        hash?;
        message?;
        if err?.is_some() {
            return Err(invalid(NAME, "official signer rejected transaction"));
        }
        Ok(Signed {
            kind: r.kind,
            info: info.ok_or_else(|| error(NAME, "signer returned no transaction"))?,
        })
    }
    pub fn auth(&self) -> Result<String, ApiError> {
        let r = unsafe {
            (self.functions.auth)(
                Utc::now().timestamp() + 600,
                self.key as c_int,
                self.account,
            )
        };
        let value = unsafe { self.string(r.value) };
        let err = unsafe { self.string(r.err) };
        if err?.is_some() {
            return Err(invalid(NAME, "auth token failed"));
        }
        value?.ok_or_else(|| error(NAME, "missing auth token"))
    }
    pub fn order(&self, request: OrderParameters, nonce: i64) -> Result<Signed, ApiError> {
        let OrderParameters {
            market,
            id,
            amount: qty,
            price,
            ask,
            tif,
            reduce,
        } = request;
        self.signed(unsafe {
            (self.functions.order)(
                market,
                id,
                qty,
                price as c_int,
                ask as c_int,
                0,
                tif,
                reduce as c_int,
                0,
                if tif == 0 { 0 } else { -1 },
                0,
                0,
                0,
                0,
                0,
                0,
                nonce,
                self.key as c_int,
                self.account,
            )
        })
    }
    pub fn cancel(&self, market: i64, id: i64, nonce: i64) -> Result<Signed, ApiError> {
        self.signed(unsafe {
            (self.functions.cancel)(market, id, 0, nonce, self.key as c_int, self.account)
        })
    }
    pub fn modify(
        &self,
        market: i64,
        id: i64,
        qty: i64,
        price: u32,
        version: i64,
        nonce: i64,
    ) -> Result<Signed, ApiError> {
        self.signed(unsafe {
            (self.functions.modify)(
                market,
                id,
                qty,
                price as i64,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                nonce,
                version,
                self.key as c_int,
                self.account,
            )
        })
    }
}
include!("signer_native_generated.rs");

#[derive(Clone, Copy)]
pub struct NativeSigningOptions {
    pub nonce: i64,
    pub skip_nonce: bool,
}
pub struct NativeSignedTransaction {
    pub tx_type: u8,
    pub tx_info: String,
    pub tx_hash: Option<String>,
    /// L1 wallet signature message, when required by the native transaction flow.
    pub message_to_sign: Option<String>,
}
impl NativeSignedTransaction {
    /// Attach the external wallet's EIP-191 signature exactly as the official SDK does.
    /// This validates encoding, not wallet ownership or server acceptance.
    pub fn with_l1_signature(mut self, signature: &str) -> Result<Self, ApiError> {
        if self.message_to_sign.is_none()
            || signature.len() != 132
            || !signature.starts_with("0x")
            || !signature[2..].bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(invalid(
                NAME,
                "L1 signature must be a 65-byte 0x hex signature for the returned message",
            ));
        }
        let mut info: Value = serde_json::from_str(&self.tx_info)
            .map_err(|_| error(NAME, "invalid native transaction JSON"))?;
        let object = info
            .as_object_mut()
            .ok_or_else(|| error(NAME, "invalid native transaction object"))?;
        object.insert("L1Sig".into(), json!(signature));
        self.tx_info = info.to_string();
        Ok(self)
    }
    pub fn into_send_request(self) -> crate::native_api::NativeRequest {
        crate::native_api::NativeRequest::body(
            json!({"tx_type":self.tx_type,"tx_info":self.tx_info}),
        )
    }
}
#[derive(Clone)]
pub struct LighterNativeSigner {
    account: Arc<SignerAccount>,
}
pub struct NativeApiKeyPair {
    pub private_key: String,
    pub public_key: String,
}
impl LighterNativeSigner {
    /// Generates a new unregistered key locally. Registration is a separate L1-authorized call.
    pub fn generate_api_key(&self) -> Result<NativeApiKeyPair, ApiError> {
        #[repr(C)]
        struct Keys {
            private: *mut c_char,
            public: *mut c_char,
            err: *mut c_char,
        }
        let result = unsafe {
            let generate = self
                .account
                .functions
                ._library
                .get::<unsafe extern "C" fn() -> Keys>(b"GenerateAPIKey\0")
                .map_err(|_| invalid(NAME, "missing GenerateAPIKey"))?;
            generate()
        };
        let private = unsafe { self.account.string(result.private) };
        let public = unsafe { self.account.string(result.public) };
        let err = unsafe { self.account.string(result.err) };
        let private = private?;
        let public = public?;
        if err?.is_some() {
            return Err(invalid(NAME, "native key generation failed"));
        }
        Ok(NativeApiKeyPair {
            private_key: private.ok_or_else(|| error(NAME, "missing generated private key"))?,
            public_key: public.ok_or_else(|| error(NAME, "missing generated public key"))?,
        })
    }
    /// Reads the configured real account through the official library; run only on a live server.
    pub async fn check_client(&self) -> Result<(), ApiError> {
        let account = self.account.clone();
        tokio::task::spawn_blocking(move || {
            let result = unsafe {
                let check = account
                    .functions
                    ._library
                    .get::<unsafe extern "C" fn(c_int, i64) -> *mut c_char>(b"CheckClient\0")
                    .map_err(|_| invalid(NAME, "missing CheckClient"))?;
                check(i32::from(account.key), account.account)
            };
            if unsafe { account.string(result) }?.is_some() {
                return Err(invalid(NAME, "native client/account verification failed"));
            }
            Ok(())
        })
        .await
        .map_err(|_| ApiError::transport(NAME, "native client check worker failed"))?
    }
    pub fn new(config: &Config) -> Result<Self, ApiError> {
        endpoint(&config.rest_url, false, NAME)?;
        if config.chain_id <= 0
            || config.account_index < 0
            || config.api_private_key.is_empty()
            || config.api_key_index < 2
        {
            return Err(invalid(
                NAME,
                "explicit native network, account, API key and verified signer required",
            ));
        }
        Ok(Self {
            account: SignerAccount::load(config)?,
        })
    }
    /// Signing alone neither consumes a nonce nor proves server acceptance. The caller owns the
    /// nonce exclusively when using this method with sendTx/sendTxBatch or an external L1 wallet.
    pub fn sign_at_nonce(
        &self,
        transaction: NativeTransaction,
        options: NativeSigningOptions,
    ) -> Result<NativeSignedTransaction, ApiError> {
        if options.nonce < 0 {
            return Err(invalid(NAME, "negative native nonce"));
        }
        self.account.native_sign(transaction, options)
    }
    pub fn auth_token(&self, deadline_seconds: i64) -> Result<String, ApiError> {
        let result = unsafe {
            (self.account.functions.auth)(
                deadline_seconds,
                i32::from(self.account.key),
                self.account.account,
            )
        };
        let value = unsafe { self.account.string(result.value) };
        let err = unsafe { self.account.string(result.err) };
        if err?.is_some() {
            return Err(invalid(NAME, "native auth token rejected"));
        }
        value?.ok_or_else(|| error(NAME, "missing native auth token"))
    }
}
impl SignerAccount {
    fn native_signed_response(
        &self,
        result: SignedResponse,
    ) -> Result<NativeSignedTransaction, ApiError> {
        let info = unsafe { self.string(result.info) };
        let hash = unsafe { self.string(result.hash) };
        let message = unsafe { self.string(result.message) };
        let err = unsafe { self.string(result.err) };
        let info = info?;
        let hash = hash?;
        let message = message?;
        if err?.is_some() {
            return Err(invalid(NAME, "official native signer rejected transaction"));
        }
        Ok(NativeSignedTransaction {
            tx_type: result.kind,
            tx_info: info.ok_or_else(|| error(NAME, "missing native transaction"))?,
            tx_hash: hash,
            message_to_sign: message,
        })
    }
}
impl LighterClient {
    /// Sends a native transaction under the same nonce lock/final guard as portable order writes.
    /// Success is sendTx acceptance only; query authoritative facts to confirm the operation.
    pub async fn execute_native_transaction(
        &self,
        transaction: NativeTransaction,
        guard: Option<SubmitGuard>,
    ) -> Result<(), ApiError> {
        self.mutate(guard, move |signer, nonce| {
            let signed = signer.native_sign(
                transaction,
                NativeSigningOptions {
                    nonce,
                    skip_nonce: false,
                },
            )?;
            if signed.message_to_sign.is_some() {
                return Err(invalid(
                    NAME,
                    "native flow requires an L1 wallet signature; use explicit sign_at_nonce flow",
                ));
            }
            Ok(Signed {
                kind: signed.tx_type,
                info: signed.tx_info,
            })
        })
        .await
    }
}
#[cfg(test)]
mod official_tests {
    use super::*;
    #[test]
    #[ignore = "requires the official signer built on the compilation server; no network or account credentials"]
    fn official_signer_offline_abi_roundtrip() {
        #[repr(C)]
        struct Keys {
            private: *mut c_char,
            public: *mut c_char,
            err: *mut c_char,
        }
        type Generate = unsafe extern "C" fn() -> Keys;
        let path =
            std::env::var("TITAN_LIGHTER_SIGNER_TEST_PATH").expect("server-built signer path");
        let sha =
            std::env::var("TITAN_LIGHTER_SIGNER_TEST_SHA256").expect("verified signer digest");
        assert_eq!(
            hex::encode(Sha256::digest(std::fs::read(&path).unwrap())),
            sha
        );
        // This random key is never registered, printed, written to disk, or sent to an exchange.
        let (private, public) = unsafe {
            let lib = Library::new(&path).unwrap();
            let generate = lib.get::<Generate>(b"GenerateAPIKey\0").unwrap();
            let free = lib.get::<Free>(b"Free\0").unwrap();
            let keys = generate();
            assert!(keys.err.is_null());
            let private = CStr::from_ptr(keys.private).to_str().unwrap().to_owned();
            let public = CStr::from_ptr(keys.public).to_str().unwrap().to_owned();
            free(keys.private.cast());
            free(keys.public.cast());
            (private, public)
        };
        let c = Config {
            rest_url: "http://127.0.0.1:1".into(),
            ws_url: "ws://127.0.0.1:1".into(),
            account_index: 42,
            api_key_index: 2,
            api_private_key: private,
            signer_path: Some(path),
            signer_sha256: Some(sha),
            ..Default::default()
        };
        let signer = SignerAccount::load(&c).unwrap();
        assert!(!signer.auth().unwrap().is_empty());
        let tx = signer
            .order(
                OrderParameters {
                    market: 0,
                    id: 1234,
                    amount: 20,
                    price: 1001,
                    ask: false,
                    tif: 2,
                    reduce: true,
                },
                0,
            )
            .unwrap();
        assert_eq!(tx.kind, 14);
        let value: Value = serde_json::from_str(&tx.info).unwrap();
        assert_eq!(value["AccountIndex"], 42);
        assert_eq!(value["ClientOrderIndex"], 1234);
        assert_eq!(value["BaseAmount"], 20);
        assert_eq!(value["Price"], 1001);
        assert_eq!(value["Nonce"], 0);
        assert!(
            value
                .get("Sig")
                .or_else(|| value.get("Signature"))
                .is_some()
        );
        assert_eq!(signer.cancel(0, 281474976710657, 1).unwrap().kind, 15);
        assert_eq!(
            signer
                .modify(0, 281474976710657, 20, 1002, 0, 2)
                .unwrap()
                .kind,
            17
        );
        let now = Utc::now().timestamp_millis();
        let sl = json!({"market_index":0,"client_order_index":1234,"base_amount":20,"price":1001,"is_ask":0,"type":2,"time_in_force":0,"reduce_only":1,"trigger_price":990,"order_expiry":now+3600000});
        let tp = json!({"market_index":0,"client_order_index":1235,"base_amount":20,"price":1001,"is_ask":0,"type":4,"time_in_force":0,"reduce_only":1,"trigger_price":1010,"order_expiry":now+3600000});
        let cases = vec![
            ("change_pub_key", json!({"pub_key":public})),
            (
                "create_order",
                json!({"market_index":0,"client_order_index":1234,"base_amount":20,"price":1001,"is_ask":0,"order_type":0,"time_in_force":2,"reduce_only":1,"trigger_price":0,"order_expiry":-1,"integrator_account_index":0,"integrator_taker_fee":0,"integrator_maker_fee":0,"self_trade_behavior_mode":0,"self_trade_equality_mode":0}),
            ),
            (
                "create_grouped_orders",
                json!({"grouping_type":2,"orders":[sl,tp],"integrator_account_index":0,"integrator_taker_fee":0,"integrator_maker_fee":0,"self_trade_behavior_mode":0,"self_trade_equality_mode":0}),
            ),
            (
                "cancel_order",
                json!({"market_index":1099511627776i64,"order_index":281474976710657i64}),
            ),
            (
                "withdraw",
                json!({"asset_index":3,"route_type":0,"amount":1000000}),
            ),
            ("create_sub_account", json!({})),
            (
                "cancel_all_orders",
                json!({"time_in_force":0,"time":0,"cancel_all_market_index":255}),
            ),
            (
                "modify_order",
                json!({"market_index":0,"index":281474976710657i64,"base_amount":20,"price":1002,"trigger_price":0,"integrator_account_index":0,"integrator_taker_fee":0,"integrator_maker_fee":0,"self_trade_behavior_mode":0,"self_trade_equality_mode":0,"order_version":0}),
            ),
            (
                "transfer",
                json!({"to_account_index":43,"asset_index":3,"from_route_type":0,"to_route_type":0,"amount":1000000,"usdc_fee":0,"memo":"00000000000000000000000000000000"}),
            ),
            (
                "create_public_pool",
                json!({"operator_fee":100,"initial_total_shares":1000,"min_operator_share_rate":100}),
            ),
            (
                "update_public_pool",
                json!({"public_pool_index":43,"status":1,"operator_fee":100,"min_operator_share_rate":100}),
            ),
            (
                "mint_shares",
                json!({"public_pool_index":140737488355328i64,"share_amount":1000}),
            ),
            (
                "burn_shares",
                json!({"public_pool_index":140737488355328i64,"share_amount":1000}),
            ),
            (
                "update_leverage",
                json!({"market_index":0,"initial_margin_fraction":1000,"margin_mode":0}),
            ),
            (
                "update_margin",
                json!({"market_index":0,"usdc_amount":1000,"direction":0}),
            ),
            (
                "stake_assets",
                json!({"staking_pool_index":140737488355328i64,"share_amount":1000}),
            ),
            (
                "unstake_assets",
                json!({"staking_pool_index":140737488355328i64,"share_amount":1000}),
            ),
            (
                "approve_integrator",
                json!({"integrator_index":43,"max_perps_taker_fee":0,"max_perps_maker_fee":0,"max_spot_taker_fee":0,"max_spot_maker_fee":0,"approval_expiry":now+3600000}),
            ),
            ("update_account_config", json!({"account_trading_mode":0})),
            (
                "update_account_asset_config",
                json!({"asset_index":3,"asset_margin_mode":0}),
            ),
            (
                "binary_options_operator_create_market",
                json!({"req":{"market_slot":1000,"start_timestamp_ms":now+60000,"end_timestamp_ms":now+3600000,"settlement_cap":1000000,"settlement_type":0,"default_price":500000,"taker_fee":100,"maker_fee":0,"min_base_amount":1,"min_quote_amount":1,"order_quote_limit":1000000,"open_interest_limit":1000000,"is_frozen":0,"symbol":"ABI_TEST","size_decimals":0,"price_decimals":6}}),
            ),
            (
                "binary_options_operator_update_market",
                json!({"req":{"market_slot":1000,"market_index":65536,"start_timestamp_ms":now+60000,"end_timestamp_ms":now+3600000,"taker_fee":100,"maker_fee":0,"min_base_amount":1,"min_quote_amount":1,"order_quote_limit":1000000,"open_interest_limit":1000000,"is_frozen":0}}),
            ),
            (
                "binary_options_operator_settle_market",
                json!({"req":{"market_slot":1000,"market_index":65536,"settlement_price":500000,"is_refund":0}}),
            ),
        ];
        assert_eq!(cases.len(), NativeTransaction::OPERATIONS.len());
        let mut failures = Vec::new();
        for (index, (kind, parameters)) in cases.into_iter().enumerate() {
            let request: NativeTransaction =
                serde_json::from_value(json!({"type":kind,"parameters":parameters}))
                    .unwrap_or_else(|e| panic!("{kind}: {e}"));
            let signed = match signer.native_sign(
                request,
                NativeSigningOptions {
                    nonce: index as i64 + 10,
                    skip_nonce: false,
                },
            ) {
                Ok(s) => s,
                Err(_) => {
                    failures.push(kind);
                    continue;
                }
            };
            let body: Value = serde_json::from_str(&signed.tx_info).unwrap();
            assert_eq!(
                body.get("AccountIndex")
                    .or_else(|| body.get("FromAccountIndex"))
                    .unwrap_or(&Value::Null),
                &json!(42),
                "{kind}"
            );
            assert_eq!(body["Nonce"], index + 10, "{kind}");
            assert!(
                body.get("Sig").or_else(|| body.get("Signature")).is_some(),
                "{kind}"
            );
            assert!(signed.tx_hash.is_some(), "{kind}");
            if signed.message_to_sign.is_some() {
                let signature = format!("0x{}", "11".repeat(65));
                let request = signed
                    .with_l1_signature(&signature)
                    .unwrap()
                    .into_send_request();
                let info: Value =
                    serde_json::from_str(request.body.unwrap()["tx_info"].as_str().unwrap())
                        .unwrap();
                assert_eq!(info["L1Sig"], signature);
                assert_eq!(info["Nonce"], index + 10);
            }
        }
        assert!(
            failures.is_empty(),
            "official offline signing rejected fixture cases: {failures:?}"
        );
        let native = LighterNativeSigner::new(&c).unwrap();
        let generated = native.generate_api_key().unwrap();
        assert!(!generated.private_key.is_empty() && !generated.public_key.is_empty());
        assert!(
            !native
                .auth_token(Utc::now().timestamp() + 600)
                .unwrap()
                .is_empty()
        );
    }
}
