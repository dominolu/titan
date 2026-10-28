use crate::*;
use aws_sdk_s3::{
    Client,
    config::{BehaviorVersion, Credentials, Region, retry::RetryConfig, timeout::TimeoutConfig},
    primitives::ByteStream,
};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    thread,
    time::Duration,
};

type Reply<T> = mpsc::SyncSender<Result<T>>;
enum Request {
    Put {
        path: PathBuf,
        reference: ObjectRef,
        reply: Reply<ObjectRef>,
    },
    Get {
        reference: ObjectRef,
        reply: Reply<File>,
    },
    Probe {
        reply: Reply<String>,
    },
    Inventory {
        reply: Reply<Vec<ObjectRef>>,
    },
    Claim {
        owner: String,
        reply: Reply<()>,
    },
}
struct S3Worker {
    sender: Option<mpsc::SyncSender<Request>>,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
    timeout: Duration,
    stage: PathBuf,
    _lease: File,
}
impl Drop for S3Worker {
    fn drop(&mut self) {
        self.sender.take();
        if let Ok(thread) = self.thread.get_mut()
            && let Some(t) = thread.take()
        {
            let _ = t.join();
        }
        let _ = fs::remove_dir_all(&self.stage);
    }
}
#[derive(Clone)]
pub struct S3ObjectStore {
    worker: Arc<S3Worker>,
    stage: PathBuf,
    budget: StorageBudget,
    pub store_id: String,
}
impl S3ObjectStore {
    /// Cold-path synchronous port. The SDK and its runtime are owned by one bounded worker thread.
    pub fn connect(
        config: S3Config,
        budget: StorageBudget,
        stage: &Path,
        expected: Option<&str>,
    ) -> Result<Self> {
        StorageProfile {
            schema_version: 1,
            postgres: None,
            s3: Some(config.clone()),
            budget: budget.clone(),
        }
        .validate()?;
        fs::create_dir_all(stage)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(stage, fs::Permissions::from_mode(0o700))?;
        }
        let session = stage.join(format!("session-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&session)?;
        let stage = session.as_path();
        let lease = crate::routing::file_lock(&stage.join(".owner.lock"), true)?;
        let (sender, receiver) = mpsc::sync_channel(4);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let worker_stage = stage.to_owned();
        let worker_budget = budget.clone();
        let worker_config = config.clone();
        let expected = expected.map(str::to_owned);
        let thread = thread::Builder::new()
            .name("s3-object-worker".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(r) => r,
                    Err(_) => {
                        let _ = ready_tx.send(Err(failure("s3_runtime_unavailable")));
                        return;
                    }
                };
                let _entered = runtime.enter();
                let access = match crate::profile::secret(&worker_config.access_key_env) {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return;
                    }
                };
                let secret = match crate::profile::secret(&worker_config.secret_key_env) {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return;
                    }
                };
                let token = match worker_config
                    .session_token_env
                    .as_ref()
                    .map(|e| crate::profile::secret(e))
                    .transpose()
                {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return;
                    }
                };
                let client = Client::from_conf(
                    aws_sdk_s3::Config::builder()
                        .behavior_version(BehaviorVersion::latest())
                        .region(Region::new(worker_config.region.clone()))
                        .endpoint_url(&worker_config.endpoint)
                        .force_path_style(true)
                        // Optional CRC trailers are not uniformly supported by compatible services.
                        // Original size and SHA256 readback remain mandatory.
                        .request_checksum_calculation(
                            aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
                        )
                        .credentials_provider(Credentials::new(
                            access,
                            secret,
                            token,
                            None,
                            "titan-secret-reference",
                        ))
                        .retry_config(RetryConfig::standard().with_max_attempts(2))
                        .timeout_config(
                            TimeoutConfig::builder()
                                .connect_timeout(Duration::from_millis(
                                    2000.min(worker_config.timeout_ms),
                                ))
                                .operation_timeout(Duration::from_millis(worker_config.timeout_ms))
                                .build(),
                        )
                        .build(),
                );
                let identity = runtime.block_on(bounded(
                    identity_and_probe(&client, &worker_config, expected.as_deref()),
                    worker_config.timeout_ms * 6,
                ));
                if ready_tx.send(identity).is_err() {
                    return;
                }
                while let Ok(request) = receiver.recv() {
                    match request {
                        Request::Put {
                            path,
                            reference,
                            reply,
                        } => {
                            let result = runtime.block_on(bounded(
                                async {
                                    let body = ByteStream::from_path(path)
                                        .await
                                        .map_err(|_| failure("s3_upload_stage_unavailable"))?;
                                    let result = client
                                        .put_object()
                                        .bucket(&worker_config.bucket)
                                        .key(object_key(&worker_config, &reference)?)
                                        .if_none_match("*")
                                        .content_length(reference.bytes as i64)
                                        .body(body)
                                        .send()
                                        .await;
                                    if let Err(error) = result {
                                        let status =
                                            error.raw_response().map(|r| r.status().as_u16());
                                        if !matches!(status, Some(409 | 412)) {
                                            let code = error
                                                .as_service_error()
                                                .and_then(|e| e.meta().code())
                                                .unwrap_or("transport");
                                            let code = match code {
                                                "BadDigest"
                                                | "InvalidRequest"
                                                | "IncompleteBody"
                                                | "XAmzContentSHA256Mismatch"
                                                | "SignatureDoesNotMatch"
                                                | "AccessDenied"
                                                | "NotImplemented"
                                                | "InvalidArgument"
                                                | "InvalidDigest"
                                                | "EntityTooLarge"
                                                | "SlowDown"
                                                | "NoSuchBucket" => code,
                                                _ => "service",
                                            };
                                            return Err(failure(&format!(
                                                "s3_put_failed:{}:{code}",
                                                status
                                                    .map_or("transport".into(), |v| v.to_string())
                                            )));
                                        }
                                    }
                                    // ETag and successful PUT are insufficient: verify the stored original bytes.
                                    download(
                                        &client,
                                        &worker_config,
                                        &worker_budget,
                                        &worker_stage,
                                        &reference,
                                    )
                                    .await?;
                                    Ok(reference)
                                },
                                worker_config.timeout_ms * 2,
                            ));
                            let _ = reply.send(result);
                        }
                        Request::Get { reference, reply } => {
                            let result = runtime.block_on(bounded(
                                download(
                                    &client,
                                    &worker_config,
                                    &worker_budget,
                                    &worker_stage,
                                    &reference,
                                ),
                                worker_config.timeout_ms * 2,
                            ));
                            let _ = reply.send(result);
                        }
                        Request::Probe { reply } => {
                            let result = runtime.block_on(bounded(
                                identity_and_probe(&client, &worker_config, None),
                                worker_config.timeout_ms * 6,
                            ));
                            let _ = reply.send(result);
                        }
                        Request::Inventory { reply } => {
                            let result = runtime.block_on(bounded(
                                inventory(&client, &worker_config),
                                worker_config.timeout_ms,
                            ));
                            let _ = reply.send(result);
                        }
                        Request::Claim { owner, reply } => {
                            let result = runtime.block_on(bounded(
                                claim_namespace(&client, &worker_config, &owner),
                                worker_config.timeout_ms * 2,
                            ));
                            let _ = reply.send(result);
                        }
                    }
                }
            })?;
        let timeout = Duration::from_millis(config.timeout_ms.saturating_mul(6) + 5000);
        let worker = Arc::new(S3Worker {
            sender: Some(sender),
            thread: Mutex::new(Some(thread)),
            timeout,
            stage: stage.to_owned(),
            _lease: lease,
        });
        let store_id = ready_rx
            .recv_timeout(timeout)
            .map_err(|_| failure("s3_capability_probe_timeout"))??;
        Ok(Self {
            worker,
            stage: stage.into(),
            budget,
            store_id,
        })
    }
    pub fn probe(&self) -> Result<String> {
        let (reply, response) = mpsc::sync_channel(1);
        self.send(Request::Probe { reply })?;
        let identity = response
            .recv_timeout(self.worker.timeout)
            .map_err(|_| failure("s3_probe_timeout"))??;
        if identity != self.store_id {
            return Err(failure("s3_target_identity_changed"));
        }
        Ok(identity)
    }
    fn send(&self, request: Request) -> Result<()> {
        self.worker
            .sender
            .as_ref()
            .ok_or_else(|| failure("s3_worker_closed"))?
            .try_send(request)
            .map_err(|_| failure("s3_worker_unavailable_or_queue_full"))
    }
    pub fn inventory(&self) -> Result<Vec<ObjectRef>> {
        let (reply, response) = mpsc::sync_channel(1);
        self.send(Request::Inventory { reply })?;
        response
            .recv_timeout(self.worker.timeout)
            .map_err(|_| failure("s3_inventory_timeout"))?
    }
    pub fn claim_namespace(&self, owner: &str) -> Result<()> {
        uuid::Uuid::parse_str(owner).map_err(|_| failure("s3_owner_invalid"))?;
        let (reply, response) = mpsc::sync_channel(1);
        self.send(Request::Claim {
            owner: owner.into(),
            reply,
        })?;
        response
            .recv_timeout(self.worker.timeout)
            .map_err(|_| failure("s3_namespace_claim_timeout"))?
    }
}
async fn claim_namespace(client: &Client, config: &S3Config, owner: &str) -> Result<()> {
    let key = format!("{}/__titan_owner_v1", config.prefix.trim_end_matches('/'));
    match client
        .put_object()
        .bucket(&config.bucket)
        .key(&key)
        .if_none_match("*")
        .body(ByteStream::from(owner.as_bytes().to_vec()))
        .send()
        .await
    {
        Ok(_) => {}
        Err(e) if e.raw_response().is_some_and(|r| r.status().as_u16() == 412) => {}
        Err(_) => return Err(failure("s3_namespace_claim_failed")),
    }
    let mut result = client
        .get_object()
        .bucket(&config.bucket)
        .key(key)
        .send()
        .await
        .map_err(|_| failure("s3_namespace_claim_failed"))?;
    if result.content_length() != Some(owner.len() as i64) {
        return Err(failure("s3_namespace_owned_by_another_control"));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = result.body.next().await {
        bytes.extend_from_slice(&chunk.map_err(|_| failure("s3_namespace_claim_failed"))?);
        if bytes.len() > 128 {
            return Err(failure("s3_namespace_owned_by_another_control"));
        }
    }
    if bytes != owner.as_bytes() {
        return Err(failure("s3_namespace_owned_by_another_control"));
    }
    Ok(())
}
async fn inventory(client: &Client, config: &S3Config) -> Result<Vec<ObjectRef>> {
    let prefix = format!("{}/objects/", config.prefix.trim_end_matches('/'));
    let mut token = None;
    let mut refs = Vec::new();
    loop {
        let page = client
            .list_objects_v2()
            .bucket(&config.bucket)
            .prefix(&prefix)
            .max_keys(1000)
            .set_continuation_token(token.clone())
            .send()
            .await
            .map_err(|_| failure("s3_inventory_unavailable"))?;
        for object in page.contents() {
            let key = object
                .key()
                .ok_or_else(|| failure("s3_inventory_invalid"))?;
            let tail = key
                .strip_prefix(&prefix)
                .ok_or_else(|| failure("s3_inventory_invalid"))?;
            let (shard, digest) = tail
                .split_once('/')
                .ok_or_else(|| failure("s3_inventory_invalid"))?;
            let size = object
                .size()
                .filter(|s| *s >= 0)
                .ok_or_else(|| failure("s3_inventory_invalid"))?;
            let reference = ObjectRef {
                sha256: digest.into(),
                bytes: size as u64,
            };
            validate_reference(&reference)?;
            if shard.len() != 2 || !digest.starts_with(shard) {
                return Err(failure("s3_inventory_invalid"));
            }
            refs.push(reference);
            if refs.len() > 100000 {
                return Err(failure("object_inventory_budget_exceeded"));
            }
        }
        if !page.is_truncated().unwrap_or(false) {
            break;
        }
        let next = page
            .next_continuation_token()
            .map(str::to_owned)
            .ok_or_else(|| failure("s3_inventory_invalid"))?;
        if token.as_ref() == Some(&next) {
            return Err(failure("s3_inventory_invalid"));
        }
        token = Some(next);
    }
    refs.sort_by(|a, b| a.sha256.cmp(&b.sha256));
    Ok(refs)
}
fn object_key(config: &S3Config, reference: &ObjectRef) -> Result<String> {
    validate_reference(reference)?;
    Ok(format!(
        "{}/objects/{}/{}",
        config.prefix.trim_end_matches('/'),
        &reference.sha256[..2],
        reference.sha256
    ))
}
pub(crate) fn validate_reference(reference: &ObjectRef) -> Result<()> {
    if reference.sha256.len() != 64
        || !reference
            .sha256
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(failure("object_reference_invalid"));
    }
    Ok(())
}
async fn identity_and_probe(
    client: &Client,
    config: &S3Config,
    expected: Option<&str>,
) -> Result<String> {
    let identity_key = format!("{}/__titan_store_v1", config.prefix.trim_end_matches('/'));
    let new_id = uuid::Uuid::new_v4().to_string();
    match client
        .put_object()
        .bucket(&config.bucket)
        .key(&identity_key)
        .if_none_match("*")
        .body(ByteStream::from(new_id.clone().into_bytes()))
        .send()
        .await
    {
        Ok(_) => {}
        Err(error)
            if error
                .raw_response()
                .is_some_and(|r| r.status().as_u16() == 412) => {}
        Err(_) => return Err(failure("s3_identity_unavailable")),
    }
    let mut response = client
        .get_object()
        .bucket(&config.bucket)
        .key(&identity_key)
        .send()
        .await
        .map_err(|_| failure("s3_identity_unavailable"))?;
    if response
        .content_length()
        .is_none_or(|n| !(0..=128).contains(&n))
    {
        return Err(failure("s3_identity_invalid"));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.body.next().await {
        let chunk = chunk.map_err(|_| failure("s3_identity_unavailable"))?;
        bytes.extend_from_slice(&chunk);
        if bytes.len() > 128 {
            return Err(failure("s3_identity_invalid"));
        }
    }
    let identity = String::from_utf8(bytes).map_err(|_| failure("s3_identity_invalid"))?;
    uuid::Uuid::parse_str(&identity).map_err(|_| failure("s3_identity_invalid"))?;
    if expected.is_some_and(|e| e != identity) {
        return Err(failure("s3_target_identity_changed"));
    }
    let key = format!(
        "{}/__capabilities/{}",
        config.prefix.trim_end_matches('/'),
        uuid::Uuid::new_v4()
    );
    client
        .put_object()
        .bucket(&config.bucket)
        .key(&key)
        .if_none_match("*")
        .body(ByteStream::from_static(b"titan-immutable-v1"))
        .send()
        .await
        .map_err(|_| failure("s3_conditional_write_probe_failed"))?;
    let second = client
        .put_object()
        .bucket(&config.bucket)
        .key(&key)
        .if_none_match("*")
        .body(ByteStream::from_static(b"must-not-overwrite"))
        .send()
        .await;
    if second
        .as_ref()
        .err()
        .and_then(|e| e.raw_response())
        .is_none_or(|r| r.status().as_u16() != 412)
    {
        return Err(failure("s3_conditional_writes_unsupported"));
    }
    // A lying implementation must also fail the read-after-precondition check.
    let mut response = client
        .get_object()
        .bucket(&config.bucket)
        .key(&key)
        .send()
        .await
        .map_err(|_| failure("s3_conditional_write_probe_failed"))?;
    if response.content_length() != Some(18) {
        return Err(failure("s3_conditional_writes_unsupported"));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.body.next().await {
        let chunk = chunk.map_err(|_| failure("s3_conditional_write_probe_failed"))?;
        if body.len() + chunk.len() > 18 {
            return Err(failure("s3_conditional_writes_unsupported"));
        }
        body.extend_from_slice(&chunk);
    }
    client
        .delete_object()
        .bucket(&config.bucket)
        .key(&key)
        .send()
        .await
        .map_err(|_| failure("s3_capability_cleanup_failed"))?;
    if body.as_slice() != b"titan-immutable-v1" {
        return Err(failure("s3_conditional_writes_unsupported"));
    }
    Ok(identity)
}
async fn download(
    client: &Client,
    config: &S3Config,
    budget: &StorageBudget,
    stage: &Path,
    reference: &ObjectRef,
) -> Result<File> {
    if reference.bytes > budget.max_object_bytes {
        return Err(failure("object_size_budget_exceeded"));
    }
    crate::profile::check_free_space(stage, budget.min_free_bytes)?;
    let mut response = client
        .get_object()
        .bucket(&config.bucket)
        .key(object_key(config, reference)?)
        .send()
        .await
        .map_err(|error| {
            failure(&format!(
                "s3_get_failed:{}",
                error
                    .raw_response()
                    .map_or("transport".into(), |r| r.status().as_u16().to_string())
            ))
        })?;
    if response.content_length() != Some(reference.bytes as i64) {
        return Err(failure("object_size_or_type_changed"));
    }
    let mut temporary = tempfile::NamedTempFile::new_in(stage)?;
    let mut hash = Sha256::new();
    let mut bytes = 0_u64;
    while let Some(chunk) = response.body.next().await {
        let chunk = chunk.map_err(|_| failure("s3_body_interrupted"))?;
        bytes = bytes
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| failure("object_size_overflow"))?;
        if bytes > reference.bytes {
            return Err(failure("object_size_or_type_changed"));
        }
        crate::profile::check_stream_space(stage, budget.min_free_bytes, bytes, chunk.len())?;
        temporary.write_all(&chunk)?;
        hash.update(&chunk);
    }
    if bytes != reference.bytes || format!("{:x}", hash.finalize()) != reference.sha256 {
        return Err(failure("object_digest_changed"));
    }
    temporary.as_file().sync_all()?;
    temporary.as_file_mut().rewind()?;
    Ok(temporary.into_file())
}
impl ObjectStore for S3ObjectStore {
    fn minimum_free_bytes(&self) -> u64 {
        self.budget.min_free_bytes
    }
    fn put(&self, source: &mut dyn Read) -> Result<ObjectRef> {
        crate::profile::check_free_space(&self.stage, self.budget.min_free_bytes)?;
        let mut temp = tempfile::NamedTempFile::new_in(&self.stage)?;
        let mut hash = Sha256::new();
        let mut count = 0_u64;
        let mut chunk = [0; 65536];
        loop {
            let n = source.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            count = count
                .checked_add(n as u64)
                .ok_or_else(|| failure("object_size_overflow"))?;
            if count > self.budget.max_object_bytes {
                return Err(failure("object_size_budget_exceeded"));
            }
            crate::profile::check_stream_space(&self.stage, self.budget.min_free_bytes, count, n)?;
            temp.write_all(&chunk[..n])?;
            hash.update(&chunk[..n]);
        }
        temp.as_file().sync_all()?;
        let reference = ObjectRef {
            sha256: format!("{:x}", hash.finalize()),
            bytes: count,
        };
        let (reply, response) = mpsc::sync_channel(1);
        self.send(Request::Put {
            path: temp.path().into(),
            reference,
            reply,
        })?;
        response
            .recv_timeout(self.worker.timeout)
            .map_err(|_| failure("s3_put_outcome_unknown"))?
    }
    fn open_verified(&self, reference: &ObjectRef) -> Result<File> {
        validate_reference(reference)?;
        let (reply, response) = mpsc::sync_channel(1);
        self.send(Request::Get {
            reference: reference.clone(),
            reply,
        })?;
        response
            .recv_timeout(self.worker.timeout)
            .map_err(|_| failure("s3_get_timeout"))?
    }
}

async fn bounded<T>(
    future: impl std::future::Future<Output = Result<T>>,
    milliseconds: u64,
) -> Result<T> {
    tokio::time::timeout(Duration::from_millis(milliseconds), future)
        .await
        .map_err(|_| failure("s3_operation_deadline_exceeded"))?
}
/// Remove only unowned per-process staging sessions. Content objects and manifests are never GC'd here.
pub fn cleanup_s3_staging(root: &Path) -> Result<usize> {
    let stage = root.join(".s3-stage");
    if !stage.exists() {
        return Ok(0);
    }
    let mut count = 0;
    for entry in fs::read_dir(&stage)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name
            .strip_prefix("session-")
            .and_then(|v| uuid::Uuid::parse_str(v).ok())
            .is_none()
        {
            continue;
        }
        if !fs::symlink_metadata(entry.path())?.is_dir() {
            return Err(failure("s3_stage_path_escape"));
        }
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(entry.path().join(".owner.lock"))?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                continue;
            }
        }
        #[cfg(not(unix))]
        {
            return Err(failure("s3_stage_cleanup_unsupported"));
        }
        fs::remove_dir_all(entry.path())?;
        count += 1;
    }
    Ok(count)
}
