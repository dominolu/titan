// Generated from the server-built official C header. Do not edit.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCreateOrder {
pub market_index: i64,
pub client_order_index: i64,
pub base_amount: i64,
pub price: u32,
pub is_ask: u8,
pub r#type: u8,
pub time_in_force: u8,
pub reduce_only: u8,
pub trigger_price: u32,
pub order_expiry: i64,
}
#[repr(C)]
struct FfiCreateOrderTxReq {
market_index: i64,
client_order_index: i64,
base_amount: i64,
price: u32,
is_ask: u8,
r#type: u8,
time_in_force: u8,
reduce_only: u8,
trigger_price: u32,
order_expiry: i64,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeBinaryOptionsOperatorCreateMarket {
pub market_slot: i16,
pub start_timestamp_ms: i64,
pub end_timestamp_ms: i64,
pub settlement_cap: u32,
pub settlement_type: u8,
pub default_price: u32,
pub taker_fee: u32,
pub maker_fee: u32,
pub min_base_amount: i64,
pub min_quote_amount: i64,
pub order_quote_limit: i64,
pub open_interest_limit: i64,
pub is_frozen: u8,
pub symbol: String,
pub size_decimals: u8,
pub price_decimals: u8,
}
#[repr(C)]
struct FfiBinaryOptionsOperatorCreateMarketTxReq {
market_slot: i16,
start_timestamp_ms: i64,
end_timestamp_ms: i64,
settlement_cap: u32,
settlement_type: u8,
default_price: u32,
taker_fee: u32,
maker_fee: u32,
min_base_amount: i64,
min_quote_amount: i64,
order_quote_limit: i64,
open_interest_limit: i64,
is_frozen: u8,
symbol: *const c_char,
size_decimals: u8,
price_decimals: u8,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeBinaryOptionsOperatorUpdateMarket {
pub market_slot: i16,
pub market_index: i64,
pub start_timestamp_ms: i64,
pub end_timestamp_ms: i64,
pub taker_fee: u32,
pub maker_fee: u32,
pub min_base_amount: i64,
pub min_quote_amount: i64,
pub order_quote_limit: i64,
pub open_interest_limit: i64,
pub is_frozen: u8,
}
#[repr(C)]
struct FfiBinaryOptionsOperatorUpdateMarketTxReq {
market_slot: i16,
market_index: i64,
start_timestamp_ms: i64,
end_timestamp_ms: i64,
taker_fee: u32,
maker_fee: u32,
min_base_amount: i64,
min_quote_amount: i64,
order_quote_limit: i64,
open_interest_limit: i64,
is_frozen: u8,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeBinaryOptionsOperatorSettleMarket {
pub market_slot: i16,
pub market_index: i64,
pub settlement_price: u32,
pub is_refund: u8,
}
#[repr(C)]
struct FfiBinaryOptionsOperatorSettleMarketTxReq {
market_slot: i16,
market_index: i64,
settlement_price: u32,
is_refund: u8,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag="type",content="parameters",rename_all="snake_case",deny_unknown_fields)]
pub enum NativeTransaction {
ChangePubKey {pub_key: String},
CreateOrder {market_index: i64,client_order_index: i64,base_amount: i64,price: u32,is_ask: i32,order_type: i32,time_in_force: i32,reduce_only: i32,trigger_price: u32,order_expiry: i64,integrator_account_index: i64,integrator_taker_fee: i32,integrator_maker_fee: i32,self_trade_behavior_mode: u8,self_trade_equality_mode: u8},
CreateGroupedOrders {grouping_type: u8,orders: Vec<NativeCreateOrder>,integrator_account_index: i64,integrator_taker_fee: i32,integrator_maker_fee: i32,self_trade_behavior_mode: u8,self_trade_equality_mode: u8},
CancelOrder {market_index: i64,order_index: i64},
Withdraw {asset_index: i32,route_type: i32,amount: u64},
CreateSubAccount {},
CancelAllOrders {time_in_force: i32,time: i64,cancel_all_market_index: i64},
ModifyOrder {market_index: i64,index: i64,base_amount: i64,price: i64,trigger_price: i64,integrator_account_index: i64,integrator_taker_fee: i32,integrator_maker_fee: i32,self_trade_behavior_mode: u8,self_trade_equality_mode: u8,order_version: i64},
Transfer {to_account_index: i64,asset_index: i16,from_route_type: u8,to_route_type: u8,amount: i64,usdc_fee: i64,memo: String},
CreatePublicPool {operator_fee: i64,initial_total_shares: i32,min_operator_share_rate: i64},
UpdatePublicPool {public_pool_index: i64,status: i32,operator_fee: i64,min_operator_share_rate: i32},
MintShares {public_pool_index: i64,share_amount: i64},
BurnShares {public_pool_index: i64,share_amount: i64},
UpdateLeverage {market_index: i64,initial_margin_fraction: i32,margin_mode: i32},
UpdateMargin {market_index: i64,usdc_amount: i64,direction: i32},
StakeAssets {staking_pool_index: i64,share_amount: i64},
UnstakeAssets {staking_pool_index: i64,share_amount: i64},
ApproveIntegrator {integrator_index: i64,max_perps_taker_fee: u32,max_perps_maker_fee: u32,max_spot_taker_fee: u32,max_spot_maker_fee: u32,approval_expiry: i64},
UpdateAccountConfig {account_trading_mode: u8},
UpdateAccountAssetConfig {asset_index: i16,asset_margin_mode: u8},
BinaryOptionsOperatorCreateMarket {req: NativeBinaryOptionsOperatorCreateMarket},
BinaryOptionsOperatorUpdateMarket {req: NativeBinaryOptionsOperatorUpdateMarket},
BinaryOptionsOperatorSettleMarket {req: NativeBinaryOptionsOperatorSettleMarket},
}
impl NativeTransaction {
pub const OPERATIONS: &'static [&'static str] = &[
"SignChangePubKey",
"SignCreateOrder",
"SignCreateGroupedOrders",
"SignCancelOrder",
"SignWithdraw",
"SignCreateSubAccount",
"SignCancelAllOrders",
"SignModifyOrder",
"SignTransfer",
"SignCreatePublicPool",
"SignUpdatePublicPool",
"SignMintShares",
"SignBurnShares",
"SignUpdateLeverage",
"SignUpdateMargin",
"SignStakeAssets",
"SignUnstakeAssets",
"SignApproveIntegrator",
"SignUpdateAccountConfig",
"SignUpdateAccountAssetConfig",
"SignBinaryOptionsOperatorCreateMarket",
"SignBinaryOptionsOperatorUpdateMarket",
"SignBinaryOptionsOperatorSettleMarket",
];
}
impl SignerAccount {
fn native_sign(&self, transaction: NativeTransaction, options: NativeSigningOptions) -> Result<NativeSignedTransaction,ApiError> {
let response=match transaction {
NativeTransaction::ChangePubKey { pub_key } => {
let pub_key=CString::new(pub_key).map_err(|_|invalid(NAME,"native string contains NUL"))?;
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(*const c_char,u8,i64,i32,i64) -> SignedResponse>(b"SignChangePubKey\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(pub_key.as_ptr(),u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::CreateOrder { market_index,client_order_index,base_amount,price,is_ask,order_type,time_in_force,reduce_only,trigger_price,order_expiry,integrator_account_index,integrator_taker_fee,integrator_maker_fee,self_trade_behavior_mode,self_trade_equality_mode } => {
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(i64,i64,i64,i32,i32,i32,i32,i32,i32,i64,i64,i32,i32,u8,u8,u8,i64,i32,i64) -> SignedResponse>(b"SignCreateOrder\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(market_index,client_order_index,base_amount,price as i32,is_ask,order_type,time_in_force,reduce_only,trigger_price as i32,order_expiry,integrator_account_index,integrator_taker_fee,integrator_maker_fee,self_trade_behavior_mode,self_trade_equality_mode,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::CreateGroupedOrders { grouping_type,orders,integrator_account_index,integrator_taker_fee,integrator_maker_fee,self_trade_behavior_mode,self_trade_equality_mode } => {
if orders.is_empty() || orders.len()>i32::MAX as usize {return Err(invalid(NAME,"invalid native order group length"));}
let orders: Vec<_>=orders.into_iter().map(|value| FfiCreateOrderTxReq {market_index:value.market_index,client_order_index:value.client_order_index,base_amount:value.base_amount,price:value.price,is_ask:value.is_ask,r#type:value.r#type,time_in_force:value.time_in_force,reduce_only:value.reduce_only,trigger_price:value.trigger_price,order_expiry:value.order_expiry}).collect();
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(u8,*const FfiCreateOrderTxReq,i32,i64,i32,i32,u8,u8,u8,i64,i32,i64) -> SignedResponse>(b"SignCreateGroupedOrders\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(grouping_type,orders.as_ptr(),orders.len() as i32,integrator_account_index,integrator_taker_fee,integrator_maker_fee,self_trade_behavior_mode,self_trade_equality_mode,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::CancelOrder { market_index,order_index } => {
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(i64,i64,u8,i64,i32,i64) -> SignedResponse>(b"SignCancelOrder\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(market_index,order_index,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::Withdraw { asset_index,route_type,amount } => {
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(i32,i32,u64,u8,i64,i32,i64) -> SignedResponse>(b"SignWithdraw\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(asset_index,route_type,amount,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::CreateSubAccount {  } => {
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(u8,i64,i32,i64) -> SignedResponse>(b"SignCreateSubAccount\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::CancelAllOrders { time_in_force,time,cancel_all_market_index } => {
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(i32,i64,i64,u8,i64,i32,i64) -> SignedResponse>(b"SignCancelAllOrders\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(time_in_force,time,cancel_all_market_index,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::ModifyOrder { market_index,index,base_amount,price,trigger_price,integrator_account_index,integrator_taker_fee,integrator_maker_fee,self_trade_behavior_mode,self_trade_equality_mode,order_version } => {
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(i64,i64,i64,i64,i64,i64,i32,i32,u8,u8,u8,i64,i64,i32,i64) -> SignedResponse>(b"SignModifyOrder\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(market_index,index,base_amount,price,trigger_price,integrator_account_index,integrator_taker_fee,integrator_maker_fee,self_trade_behavior_mode,self_trade_equality_mode,u8::from(options.skip_nonce),options.nonce,order_version,i32::from(self.key),self.account)
}
},
NativeTransaction::Transfer { to_account_index,asset_index,from_route_type,to_route_type,amount,usdc_fee,memo } => {
let memo=CString::new(memo).map_err(|_|invalid(NAME,"native string contains NUL"))?;
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(i64,i16,u8,u8,i64,i64,*const c_char,u8,i64,i32,i64) -> SignedResponse>(b"SignTransfer\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(to_account_index,asset_index,from_route_type,to_route_type,amount,usdc_fee,memo.as_ptr(),u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::CreatePublicPool { operator_fee,initial_total_shares,min_operator_share_rate } => {
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(i64,i32,i64,u8,i64,i32,i64) -> SignedResponse>(b"SignCreatePublicPool\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(operator_fee,initial_total_shares,min_operator_share_rate,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::UpdatePublicPool { public_pool_index,status,operator_fee,min_operator_share_rate } => {
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(i64,i32,i64,i32,u8,i64,i32,i64) -> SignedResponse>(b"SignUpdatePublicPool\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(public_pool_index,status,operator_fee,min_operator_share_rate,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::MintShares { public_pool_index,share_amount } => {
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(i64,i64,u8,i64,i32,i64) -> SignedResponse>(b"SignMintShares\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(public_pool_index,share_amount,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::BurnShares { public_pool_index,share_amount } => {
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(i64,i64,u8,i64,i32,i64) -> SignedResponse>(b"SignBurnShares\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(public_pool_index,share_amount,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::UpdateLeverage { market_index,initial_margin_fraction,margin_mode } => {
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(i64,i32,i32,u8,i64,i32,i64) -> SignedResponse>(b"SignUpdateLeverage\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(market_index,initial_margin_fraction,margin_mode,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::UpdateMargin { market_index,usdc_amount,direction } => {
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(i64,i64,i32,u8,i64,i32,i64) -> SignedResponse>(b"SignUpdateMargin\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(market_index,usdc_amount,direction,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::StakeAssets { staking_pool_index,share_amount } => {
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(i64,i64,u8,i64,i32,i64) -> SignedResponse>(b"SignStakeAssets\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(staking_pool_index,share_amount,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::UnstakeAssets { staking_pool_index,share_amount } => {
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(i64,i64,u8,i64,i32,i64) -> SignedResponse>(b"SignUnstakeAssets\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(staking_pool_index,share_amount,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::ApproveIntegrator { integrator_index,max_perps_taker_fee,max_perps_maker_fee,max_spot_taker_fee,max_spot_maker_fee,approval_expiry } => {
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(i64,u32,u32,u32,u32,i64,u8,i64,i32,i64) -> SignedResponse>(b"SignApproveIntegrator\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(integrator_index,max_perps_taker_fee,max_perps_maker_fee,max_spot_taker_fee,max_spot_maker_fee,approval_expiry,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::UpdateAccountConfig { account_trading_mode } => {
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(u8,u8,i64,i32,i64) -> SignedResponse>(b"SignUpdateAccountConfig\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(account_trading_mode,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::UpdateAccountAssetConfig { asset_index,asset_margin_mode } => {
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(i16,u8,u8,i64,i32,i64) -> SignedResponse>(b"SignUpdateAccountAssetConfig\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(asset_index,asset_margin_mode,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::BinaryOptionsOperatorCreateMarket { req } => {
let req_symbol=CString::new(req.symbol).map_err(|_|invalid(NAME,"native string contains NUL"))?;
let req=FfiBinaryOptionsOperatorCreateMarketTxReq {market_slot:req.market_slot,start_timestamp_ms:req.start_timestamp_ms,end_timestamp_ms:req.end_timestamp_ms,settlement_cap:req.settlement_cap,settlement_type:req.settlement_type,default_price:req.default_price,taker_fee:req.taker_fee,maker_fee:req.maker_fee,min_base_amount:req.min_base_amount,min_quote_amount:req.min_quote_amount,order_quote_limit:req.order_quote_limit,open_interest_limit:req.open_interest_limit,is_frozen:req.is_frozen,symbol:req_symbol.as_ptr(),size_decimals:req.size_decimals,price_decimals:req.price_decimals};
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(*const FfiBinaryOptionsOperatorCreateMarketTxReq,u8,i64,i32,i64) -> SignedResponse>(b"SignBinaryOptionsOperatorCreateMarket\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(&req,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::BinaryOptionsOperatorUpdateMarket { req } => {
let req=FfiBinaryOptionsOperatorUpdateMarketTxReq {market_slot:req.market_slot,market_index:req.market_index,start_timestamp_ms:req.start_timestamp_ms,end_timestamp_ms:req.end_timestamp_ms,taker_fee:req.taker_fee,maker_fee:req.maker_fee,min_base_amount:req.min_base_amount,min_quote_amount:req.min_quote_amount,order_quote_limit:req.order_quote_limit,open_interest_limit:req.open_interest_limit,is_frozen:req.is_frozen};
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(*const FfiBinaryOptionsOperatorUpdateMarketTxReq,u8,i64,i32,i64) -> SignedResponse>(b"SignBinaryOptionsOperatorUpdateMarket\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(&req,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
NativeTransaction::BinaryOptionsOperatorSettleMarket { req } => {
let req=FfiBinaryOptionsOperatorSettleMarketTxReq {market_slot:req.market_slot,market_index:req.market_index,settlement_price:req.settlement_price,is_refund:req.is_refund};
// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.
unsafe {
let function=*self.functions._library.get::<unsafe extern "C" fn(*const FfiBinaryOptionsOperatorSettleMarketTxReq,u8,i64,i32,i64) -> SignedResponse>(b"SignBinaryOptionsOperatorSettleMarket\0").map_err(|_|invalid(NAME,"missing native signer operation"))?;
function(&req,u8::from(options.skip_nonce),options.nonce,i32::from(self.key),self.account)
}
},
};
self.native_signed_response(response)
}
}
