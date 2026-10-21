//! Explicit build-server-only generation of a no-order native V13 measurement strategy.
use anyhow::{Result, ensure};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{fs, path::Path, process::Command};
use titan_strategy_runtime::*;
pub const CAPACITY: usize = 2048;
pub fn build(root: &Path) -> Result<()> {
    fs::create_dir_all(root)?;
    // Keep the artifact state below the platform's 64 KiB bound.
    let size = 8 + CAPACITY * 24;
    let schema = json!({"itemsize":size,"alignment":8,"endian":"little","fields":[]});
    let mut hashed = schema.clone();
    hashed["schema_version"] = json!(1);
    let meta = NumbaMetadataV13 {
        protocol_version: 1,
        abi_version: 13,
        abi_fingerprint: V13_ABI_FINGERPRINT,
        strategy_id: "l2_local_latency".into(),
        strategy_version: "1.0.0".into(),
        callbacks: vec!["on_depth".into()],
        callback_mask: 8,
        state_schema_version: 1,
        state_schema_hash: Sha256::digest(compiler_json_v13(&hashed)).into(),
        state_len: size,
        state_alignment: 8,
        initial_state: vec![0; size],
        state_schema: schema,
        parameter_schema: json!({"type":"object"}),
        subscriptions: vec![NumbaSubscriptionV13 {
            event: "depth".into(),
            handler: "on_depth".into(),
            schema_version: 1,
            qos: "reliable_ordered".into(),
        }],
        capabilities: vec!["market_data".into()],
        build: json!({"compiler":"native-c-latency-probe","no_orders":true}),
    };
    let source = format!(
        r#"{}
#include <stddef.h>
#include <time.h>
#include <string.h>
struct ContextPrefix {{
 uint32_t size,abi,event,schema,flags,reserved; int64_t now_ns; uint64_t generation,instance;
 uint8_t *state; uint64_t state_len; uint32_t alignment,state_version; uint8_t hash[32];
 void *ticks; uint64_t ticks_len; void *bars; uint64_t bars_len; void *depth; uint64_t depth_len;
}};
_Static_assert(offsetof(struct ContextPrefix,state)=={},"state ABI");
_Static_assert(offsetof(struct ContextPrefix,depth)=={},"depth ABI");
int32_t titan_numba_impl_on_depth(int32_t *result, void **exception, void *pointer) {{
 struct timespec ts; if(clock_gettime(CLOCK_REALTIME,&ts)) return -1;
 int64_t entry=(int64_t)ts.tv_sec*1000000000LL+ts.tv_nsec;
 struct ContextPrefix *ctx=pointer;
 uint64_t *state=(uint64_t *)ctx->state;
 if(!ctx->depth_len || state[0]>={}) return -1;
 int64_t receive; memcpy(&receive,(uint8_t *)ctx->depth+{},8);
 size_t offset=1+state[0]*3; state[offset]=receive; state[offset+1]=entry;
 state[offset+2]=ctx->depth_len; state[0]++;
 *result=0; return 0;
}}
"#,
        meta.descriptor_source()?,
        std::mem::offset_of!(StrategyRuntimeContextV13, state_ptr),
        std::mem::offset_of!(StrategyRuntimeContextV13, depth_ptr),
        CAPACITY,
        std::mem::offset_of!(TitanDepthView, receive_ts_ns)
    );
    fs::write(root.join("probe.c"), &source)?;
    let status = Command::new("cc")
        .args([
            "-shared",
            "-fPIC",
            "-O3",
            "-fvisibility=hidden",
            "-Wl,-z,defs",
            "-o",
        ])
        .arg(root.join("probe.so"))
        .arg(root.join("probe.c"))
        .status()?;
    ensure!(status.success(), "probe C compilation failed");
    let library = fs::read(root.join("probe.so"))?;
    write_artifact_v13(ArtifactBuildV13 {
        metadata: &meta,
        library: &library,
        source_digest: Sha256::digest(source.as_bytes()).into(),
        parameters_digest: [0; 32],
        target_triple: compiler_host_target_v13(),
        cpu_baseline: "generic",
        bundle: true,
        output: &root.join("probe.titan"),
        signing: None,
    })?;
    Ok(())
}
