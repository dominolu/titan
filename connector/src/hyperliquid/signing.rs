use k256::ecdsa::{RecoveryId, Signature, SigningKey};
use serde::{Serialize, Serializer};
use sha3::{Digest, Keccak256};

use crate::hyperliquid::HyperliquidError;

pub fn keccak256(data: &[u8]) -> [u8; 32] {
    Keccak256::digest(data).into()
}

/// An ECDSA signature over the EIP-712 digest of a phantom agent.
#[derive(Debug, Clone)]
pub struct L1Signature {
    pub r: [u8; 32],
    pub s: [u8; 32],
    pub v: u8,
}

impl Serialize for L1Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("L1Signature", 3)?;
        state.serialize_field("r", &format!("0x{}", hex::encode(self.r)))?;
        state.serialize_field("s", &format!("0x{}", hex::encode(self.s)))?;
        state.serialize_field("v", &self.v)?;
        state.end()
    }
}

/// Signs L1 actions with the phantom-agent EIP-712 scheme.
///
/// 1. msgpack-encode the action (named/map format, field order preserved).
/// 2. Append the nonce (8 bytes big-endian).
/// 3. Append the vault flag: `0x00` when absent, `0x01 || address(20)` when present.
/// 4. `connection_id = keccak256(data)`.
/// 5. Sign `keccak256(0x19 0x01 || domainSeparator || structHash(Agent))` with secp256k1.
pub fn sign_l1_action<T: Serialize>(
    action: &T,
    private_key: &[u8; 32],
    nonce: u64,
    vault_address: Option<&str>,
    is_mainnet: bool,
) -> Result<L1Signature, HyperliquidError> {
    let connection_id = agent_connection_id(action, nonce, vault_address)?;
    let signing_hash = eip712_signing_hash(&connection_id, is_mainnet);

    let signing_key = SigningKey::from_bytes(private_key.into())
        .map_err(|_| HyperliquidError::InvalidArg("invalid private key"))?;
    let (signature, recovery_id): (Signature, RecoveryId) =
        signing_key.sign_prehash_recoverable(&signing_hash)?;
    let signature_bytes = signature.to_bytes();

    let mut r = [0u8; 32];
    let mut s = [0u8; 32];
    r.copy_from_slice(&signature_bytes[..32]);
    s.copy_from_slice(&signature_bytes[32..]);

    Ok(L1Signature {
        r,
        s,
        v: 27 + recovery_id.to_byte(),
    })
}

/// Computes `connection_id = keccak256(msgpack(action) || nonce(8B BE) || vault_flag)`.
fn agent_connection_id<T: Serialize>(
    action: &T,
    nonce: u64,
    vault_address: Option<&str>,
) -> Result<[u8; 32], HyperliquidError> {
    let mut data = rmp_serde::to_vec_named(action)
        .map_err(|_| HyperliquidError::InvalidArg("msgpack action serialization failed"))?;

    data.extend_from_slice(&nonce.to_be_bytes());

    match vault_address {
        Some(addr) => {
            let addr = addr.trim_start_matches("0x");
            let addr_bytes = hex::decode(addr)
                .map_err(|_| HyperliquidError::InvalidArg("vault address must be hex"))?;
            if addr_bytes.len() != 20 {
                return Err(HyperliquidError::InvalidArg(
                    "vault address must be 20 bytes",
                ));
            }
            data.push(0x01);
            data.extend_from_slice(&addr_bytes);
        }
        None => data.push(0x00),
    }

    Ok(keccak256(&data))
}

/// Computes the EIP-712 signing hash of the phantom agent:
/// `keccak256(0x19 0x01 || domainSeparator || structHash(Agent))`.
pub fn eip712_signing_hash(connection_id: &[u8; 32], is_mainnet: bool) -> [u8; 32] {
    // EIP-712 struct hash of `Agent(string source,bytes32 connectionId)`.
    let type_hash = keccak256(b"Agent(string source,bytes32 connectionId)");
    let source_hash = keccak256(if is_mainnet { b"a" } else { b"b" });
    let mut struct_buf = Vec::with_capacity(32 * 3);
    struct_buf.extend_from_slice(&type_hash);
    struct_buf.extend_from_slice(&source_hash);
    struct_buf.extend_from_slice(connection_id);
    let struct_hash = keccak256(&struct_buf);

    // EIP-712 domain: { name: "Exchange", version: "1", chainId: 1337, verifyingContract: zero }.
    let domain_type_hash = keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let name_hash = keccak256(b"Exchange");
    let version_hash = keccak256(b"1");
    let mut chain_id_bytes = [0u8; 32];
    chain_id_bytes[24..32].copy_from_slice(&1337u64.to_be_bytes());
    let mut domain_buf = Vec::with_capacity(32 * 5);
    domain_buf.extend_from_slice(&domain_type_hash);
    domain_buf.extend_from_slice(&name_hash);
    domain_buf.extend_from_slice(&version_hash);
    domain_buf.extend_from_slice(&chain_id_bytes);
    domain_buf.extend_from_slice(&[0u8; 32]); // verifyingContract
    let domain_separator = keccak256(&domain_buf);

    let mut digest = [0u8; 66];
    digest[0] = 0x19;
    digest[1] = 0x01;
    digest[2..34].copy_from_slice(&domain_separator);
    digest[34..66].copy_from_slice(&struct_hash);
    keccak256(&digest)
}

/// Derives the Ethereum-style address from a secp256k1 private key.
pub fn derive_address(private_key: &[u8; 32]) -> Result<String, HyperliquidError> {
    let signing_key = SigningKey::from_bytes(private_key.into())
        .map_err(|_| HyperliquidError::InvalidArg("invalid private key"))?;
    let public_key = signing_key.verifying_key().to_encoded_point(false);
    let hash = keccak256(&public_key.as_bytes()[1..]);
    Ok(format!("0x{}", hex::encode(&hash[12..])))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hyperliquid::msg::{
        CancelAction, CancelWire, OrderAction, OrderTypeWire, OrderWire, Tif,
    };

    #[test]
    fn test_derive_address() {
        // Well-known test vector: the address of key 0x...01 is deterministic.
        let key = [1u8; 32];
        let addr = derive_address(&key).unwrap();
        assert!(addr.starts_with("0x"));
        assert_eq!(addr.len(), 42);
    }

    #[test]
    fn test_derive_address_known_vector() {
        // Generated with the official hyperliquid-python-sdk (eth_account.Account.from_key).
        let key = [7u8; 32];
        assert_eq!(
            derive_address(&key).unwrap(),
            "0x4a62316623ad457f02cdc5d997ded67a383ec569"
        );
    }

    #[test]
    fn test_signature_serialization() {
        let sig = L1Signature {
            r: [1u8; 32],
            s: [2u8; 32],
            v: 27,
        };
        let v = serde_json::to_value(&sig).unwrap();
        assert_eq!(v["r"], format!("0x{}", "01".repeat(32)));
        assert_eq!(v["v"], 27);
    }

    #[test]
    fn test_msgpack_field_order_order_action() {
        let action = OrderAction {
            type_: "order".to_string(),
            orders: vec![OrderWire {
                a: 0,
                b: true,
                p: "30000".to_string(),
                s: "0.1".to_string(),
                r: false,
                t: OrderTypeWire {
                    limit: Tif {
                        tif: "Gtc".to_string(),
                    },
                },
                c: None,
            }],
            grouping: "na".to_string(),
        };
        let bytes = rmp_serde::to_vec_named(&action).unwrap();
        // fixmap(3) "type" "order" "orders" array(1) order-map(6, c omitted) "grouping" "na"
        let expected: &[u8] = &[
            0x83, 0xa4, b't', b'y', b'p', b'e', 0xa5, b'o', b'r', b'd', b'e', b'r', 0xa6, b'o',
            b'r', b'd', b'e', b'r', b's', 0x91, 0x86, 0xa1, b'a', 0x00, 0xa1, b'b', 0xc3, 0xa1,
            b'p', 0xa5, b'3', b'0', b'0', b'0', b'0', 0xa1, b's', 0xa3, b'0', b'.', b'1', 0xa1,
            b'r', 0xc2, 0xa1, b't', 0x81, 0xa5, b'l', b'i', b'm', b'i', b't', 0x81, 0xa3, b't',
            b'i', b'f', 0xa3, b'G', b't', b'c', 0xa8, b'g', b'r', b'o', b'u', b'p', b'i', b'n',
            b'g', 0xa2, b'n', b'a',
        ];
        assert_eq!(&bytes, expected);
    }

    #[test]
    fn test_msgpack_field_order_order_wire() {
        let wire = OrderWire {
            a: 0,
            b: true,
            p: "30000".to_string(),
            s: "0.1".to_string(),
            r: false,
            t: OrderTypeWire {
                limit: Tif {
                    tif: "Gtc".to_string(),
                },
            },
            c: Some("abcd".to_string()),
        };
        let bytes = rmp_serde::to_vec_named(&wire).unwrap();
        // fixmap(7) "a" 0 "b" true "p" "30000" "s" "0.1" "r" false
        // "t" {"limit":{"tif":"Gtc"}} "c" "abcd"
        let expected: &[u8] = &[
            0x87, 0xa1, b'a', 0x00, 0xa1, b'b', 0xc3, 0xa1, b'p', 0xa5, b'3', b'0', b'0', b'0',
            b'0', 0xa1, b's', 0xa3, b'0', b'.', b'1', 0xa1, b'r', 0xc2, 0xa1, b't', 0x81, 0xa5,
            b'l', b'i', b'm', b'i', b't', 0x81, 0xa3, b't', b'i', b'f', 0xa3, b'G', b't', b'c',
            0xa1, b'c', 0xa4, b'a', b'b', b'c', b'd',
        ];
        assert_eq!(&bytes, expected);
    }

    #[test]
    fn test_msgpack_field_order_cancel_action() {
        let action = CancelAction {
            type_: "cancel".to_string(),
            cancels: vec![CancelWire { a: 1, o: 123456 }],
        };
        let bytes = rmp_serde::to_vec_named(&action).unwrap();
        // fixmap(2) "type" "cancel" "cancels" array(1) map(2) "a" 1 "o" uint32(123456)
        let expected: &[u8] = &[
            0x82, 0xa4, b't', b'y', b'p', b'e', 0xa6, b'c', b'a', b'n', b'c', b'e', b'l', 0xa7,
            b'c', b'a', b'n', b'c', b'e', b'l', b's', 0x91, 0x82, 0xa1, b'a', 0x01, 0xa1, b'o',
            0xce, 0x00, 0x01, 0xe2, 0x40,
        ];
        assert_eq!(&bytes, expected);
    }

    #[test]
    fn test_signature_recovers_derived_address() {
        let key = [7u8; 32];
        let action = OrderAction {
            type_: "order".to_string(),
            orders: vec![OrderWire {
                a: 0,
                b: true,
                p: "30000".to_string(),
                s: "0.1".to_string(),
                r: false,
                t: OrderTypeWire {
                    limit: Tif {
                        tif: "Gtc".to_string(),
                    },
                },
                c: None,
            }],
            grouping: "na".to_string(),
        };
        let sig = sign_l1_action(&action, &key, 1, None, false).unwrap();
        let connection_id = agent_connection_id(&action, 1, None).unwrap();
        let signing_hash = eip712_signing_hash(&connection_id, false);

        let recovery_id = RecoveryId::from_byte(sig.v - 27).unwrap();
        let mut signature_bytes = Vec::with_capacity(64);
        signature_bytes.extend_from_slice(&sig.r);
        signature_bytes.extend_from_slice(&sig.s);
        let signature = Signature::from_slice(&signature_bytes).unwrap();
        let verifying_key =
            k256::ecdsa::VerifyingKey::recover_from_prehash(&signing_hash, &signature, recovery_id)
                .unwrap();
        let public_key = verifying_key.to_encoded_point(false);
        let hash = keccak256(&public_key.as_bytes()[1..]);
        let recovered = format!("0x{}", hex::encode(&hash[12..]));
        let derived = derive_address(&key).unwrap();

        println!("derived  : {derived}");
        println!("recovered: {recovered}");
        println!(
            "r={} s={} v={}",
            hex::encode(sig.r),
            hex::encode(sig.s),
            sig.v
        );
        assert_eq!(recovered, derived);
    }

    fn sdk_action() -> OrderAction {
        OrderAction {
            type_: "order".to_string(),
            orders: vec![OrderWire {
                a: 0,
                b: true,
                p: "30000".to_string(),
                s: "0.1".to_string(),
                r: false,
                t: OrderTypeWire {
                    limit: Tif {
                        tif: "Gtc".to_string(),
                    },
                },
                c: None,
            }],
            grouping: "na".to_string(),
        }
    }

    const SDK_KEY: [u8; 32] = [7u8; 32];

    #[test]
    fn test_signature_matches_official_sdk_testnet() {
        // Test vector generated with the official hyperliquid-python-sdk:
        // sign_l1_action(Account.from_key(bytes([7]*32)), action, None, 1, None, False)
        let sig = sign_l1_action(&sdk_action(), &SDK_KEY, 1, None, false).unwrap();
        assert_eq!(
            hex::encode(sig.r),
            "2053d24cbdc1010906079209af7f2a540781f00925ca741100046ef8c0407eaf"
        );
        assert_eq!(
            hex::encode(sig.s),
            "41d99580f5f18b18bbcf0c02b3d6c59c5e55898ca505c2593c4e37072e1def82"
        );
        assert_eq!(sig.v, 28);
    }

    #[test]
    fn test_signature_matches_official_sdk_mainnet() {
        // sign_l1_action(Account.from_key(bytes([7]*32)), action, None, 1, None, True)
        let sig = sign_l1_action(&sdk_action(), &SDK_KEY, 1, None, true).unwrap();
        assert_eq!(
            hex::encode(sig.r),
            "6db36e1090a27823c1eaa63136649397ac5917fe53ce6209e2e55078555817fd"
        );
        assert_eq!(
            hex::encode(sig.s),
            "19de8cfa499fa43bfe3783114e9483433887c3b618236ca234833511c5d1b5e4"
        );
        assert_eq!(sig.v, 27);
    }

    #[test]
    fn test_connection_id_matches_official_sdk() {
        // action_hash(action, None, 1, None) from the official SDK.
        let connection_id = agent_connection_id(&sdk_action(), 1, None).unwrap();
        assert_eq!(
            hex::encode(connection_id),
            "6e043c926176b979f375717d009d5e5e130abe372ebce0d38c0a498d2b080e69"
        );
    }

    #[test]
    fn test_connection_id_with_vault_matches_official_sdk() {
        // action_hash(action, "0x1111...1111", 1, None) from the official SDK.
        let vault = "0x1111111111111111111111111111111111111111";
        let connection_id = agent_connection_id(&sdk_action(), 1, Some(vault)).unwrap();
        assert_eq!(
            hex::encode(connection_id),
            "47ba352b16d26303b62082b71a04a50ef2d1b2ab99325f3d739b727002d700ff"
        );
    }

    #[test]
    fn test_cancel_connection_id_matches_official_sdk() {
        let action = CancelAction {
            type_: "cancel".to_string(),
            cancels: vec![CancelWire { a: 1, o: 123456 }],
        };
        let connection_id = agent_connection_id(&action, 1, None).unwrap();
        assert_eq!(
            hex::encode(connection_id),
            "c2893f6ff495abc1abebe3962199d8be40ec824ec7b0782ee6323d9ec304fab4"
        );
    }
}
