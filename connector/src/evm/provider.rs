//! EVM JSON-RPC 封装：读路径走 HTTP，事件订阅走 WS。
//!
//! 只暴露 venue 用得到的窄接口，`DynProvider` 抹平传输层类型差异，便于在
//! market 循环里替换 WS/HTTP 后端。

use std::time::Duration;

use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_provider::{DynProvider, Provider, ProviderBuilder};
use alloy_rpc_types_eth::{Filter, TransactionReceipt, TransactionRequest};
use alloy_sol_types::{SolCall, sol};
use alloy_transport_ws::WsConnect;

use crate::evm::EvmError;
use crate::evm::dex::uniswap_v2::{IUniswapV2Pair, decode_reserves};

sol! {
    interface IERC20Metadata {
        function balanceOf(address account) external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
    }
}

pub const TRANSFER_EVENT_SIGNATURE: B256 = B256::new([
    0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37, 0x8d, 0xaa,
    0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d, 0xf5, 0x23, 0xb3, 0xef,
]);

#[derive(Clone)]
pub struct EvmProvider {
    rpc: DynProvider,
    ws_url: String,
}

fn eth_call_req(to: Address, data: Vec<u8>) -> TransactionRequest {
    TransactionRequest::default()
        .to(to)
        .input(Bytes::from(data).into())
}

impl EvmProvider {
    pub fn new(rpc: DynProvider, ws_url: String) -> Self {
        Self { rpc, ws_url }
    }

    pub fn connect(rpc_url: &str, ws_url: &str) -> Result<Self, EvmError> {
        let provider = ProviderBuilder::new().connect_http(
            rpc_url
                .parse()
                .map_err(|_| EvmError::InvalidArg("rpc_url"))?,
        );
        Ok(Self::new(DynProvider::new(provider), ws_url.to_string()))
    }

    /// 建立带日志订阅能力的 WS 连接（供 market/feed 后端使用）。
    pub async fn connect_ws(&self) -> Result<DynProvider, EvmError> {
        let ws = ProviderBuilder::new()
            .connect_ws(WsConnect::new(&self.ws_url))
            .await
            .map_err(EvmError::Transport)?;
        Ok(DynProvider::new(ws))
    }

    pub async fn chain_id(&self) -> Result<u64, EvmError> {
        self.rpc.get_chain_id().await.map_err(EvmError::Transport)
    }

    pub async fn block_number(&self) -> Result<u64, EvmError> {
        self.rpc
            .get_block_number()
            .await
            .map_err(EvmError::Transport)
    }

    pub async fn eth_call(&self, to: Address, data: Vec<u8>) -> Result<Bytes, EvmError> {
        self.rpc
            .call(eth_call_req(to, data))
            .await
            .map_err(EvmError::Transport)
    }

    /// 读取 V2 pair 当前储备（常数乘积模型的状态根）。
    pub async fn get_reserves(&self, pair: Address) -> Result<(U256, U256), EvmError> {
        let data = self
            .eth_call(pair, IUniswapV2Pair::getReservesCall {}.abi_encode())
            .await?;
        decode_reserves(&data).ok_or(EvmError::Decode("getReserves"))
    }

    pub async fn token_balance(&self, token: Address, account: Address) -> Result<U256, EvmError> {
        let data = self
            .eth_call(
                token,
                IERC20Metadata::balanceOfCall { account }.abi_encode(),
            )
            .await?;
        let ret = IERC20Metadata::balanceOfCall::abi_decode_returns(&data)
            .map_err(|_| EvmError::Decode("balanceOf"))?;
        Ok(ret)
    }

    pub async fn token_allowance(
        &self,
        token: Address,
        owner: Address,
        spender: Address,
    ) -> Result<U256, EvmError> {
        let data = self
            .eth_call(
                token,
                IERC20Metadata::allowanceCall { owner, spender }.abi_encode(),
            )
            .await?;
        let ret = IERC20Metadata::allowanceCall::abi_decode_returns(&data)
            .map_err(|_| EvmError::Decode("allowance"))?;
        Ok(ret)
    }

    /// 本地 pending nonce：已广播未落块的交易数。首次调用后由 tx.rs 自行递增。
    pub async fn pending_nonce(&self, account: Address) -> Result<u64, EvmError> {
        self.rpc
            .get_transaction_count(account)
            .pending()
            .await
            .map_err(EvmError::Transport)
    }

    pub async fn gas_price(&self) -> Result<u128, EvmError> {
        self.rpc.get_gas_price().await.map_err(EvmError::Transport)
    }

    pub async fn transaction_receipt(
        &self,
        tx_hash: B256,
    ) -> Result<Option<TransactionReceipt>, EvmError> {
        self.rpc
            .get_transaction_receipt(tx_hash)
            .await
            .map_err(EvmError::Transport)
    }

    /// 订阅目标地址的指定事件（WS）。
    pub async fn subscribe_logs(
        &self,
        ws: &DynProvider,
        addresses: Vec<Address>,
        signatures: Vec<B256>,
    ) -> Result<alloy_pubsub::Subscription<alloy_rpc_types_eth::Log>, EvmError> {
        let filter = Filter::new().address(addresses).event_signature(signatures);
        ws.subscribe_logs(&filter)
            .await
            .map_err(EvmError::Transport)
    }

    pub fn rpc(&self) -> &DynProvider {
        &self.rpc
    }

    pub fn poll_receipts(&self) -> ReceiptPoller {
        ReceiptPoller {
            provider: self.clone(),
        }
    }
}

/// 收据轮询器：swap 提交后的确认循环（EVM 私有流的等价物）。
#[derive(Clone)]
pub struct ReceiptPoller {
    provider: EvmProvider,
}

impl ReceiptPoller {
    pub async fn wait(
        &self,
        tx_hash: B256,
        timeout: Duration,
        interval: Duration,
    ) -> Result<Option<TransactionReceipt>, EvmError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(receipt) = self.provider.transaction_receipt(tx_hash).await? {
                return Ok(Some(receipt));
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
            tokio::time::sleep(interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_signature_matches_keccak() {
        // keccak256("Transfer(address,address,uint256)")，锁死常量避免手写错误。
        let computed = alloy_primitives::keccak256("Transfer(address,address,uint256)");
        assert_eq!(TRANSFER_EVENT_SIGNATURE, computed);
    }
}
