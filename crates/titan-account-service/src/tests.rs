use std::time::SystemTime;

use crate::*;

#[test]
fn fill_v2_preserves_last_and_cumulative_quantities() {
    let value = FillV2 {
        header: AccountEventHeaderV1 {
            account_id: 1,
            kind: event_kind::FILL,
            account_generation: 2,
            account_epoch: 3,
            account_version: 4,
            ..AccountEventHeaderV1::default()
        },
        asset_id: 9,
        last_fill_quantity_lots: 2,
        cumulative_filled_quantity_lots: 7,
        ..FillV2::default()
    };
    let mut encoded = vec![0; FillV2::ENCODED_LEN];
    value.encode_into(&mut encoded).unwrap();
    assert_eq!(FillV2::decode(&encoded).unwrap(), value);
    assert_eq!(
        account_event_layout_version(FILL_EVENT, FILL_EVENT_SCHEMA_VERSION),
        Some((event_kind::FILL, FillV2::ENCODED_LEN))
    );
}

#[cfg(unix)]
#[test]
fn directory_secret_provider_is_scoped_bounded_and_requires_private_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let nonce = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "titan-account-secrets-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let secret_path = root.join("okx.toml");
    std::fs::write(&secret_path, b"api_key = \"redacted\"\n").unwrap();
    std::fs::set_permissions(&secret_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let provider = DirectorySecretProvider::new(&root).unwrap();
    assert_eq!(
        provider
            .resolve(&SecretRef::new("secret://file/okx.toml"))
            .unwrap()
            .expose(),
        b"api_key = \"redacted\"\n"
    );
    assert_eq!(
        provider
            .resolve(&SecretRef::new("secret://file/../outside"))
            .unwrap_err()
            .kind,
        AccountErrorKind::CredentialUnavailable
    );
    std::fs::set_permissions(&secret_path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(
        provider
            .resolve(&SecretRef::new("secret://file/okx.toml"))
            .unwrap_err()
            .kind,
        AccountErrorKind::CredentialUnavailable
    );
    std::fs::remove_file(secret_path).unwrap();
    std::fs::remove_dir(root).unwrap();
}
