#!/usr/bin/env bash
set -euo pipefail

cargo build \
  -p titan-cli --bin titan \
  -p titan-connector-binance-futures-plugin \
  -p titan-connector-okx-plugin \
  -p titan-connector-hyperliquid-plugin

case "$(uname -s)" in
  Darwin) extension="dylib" ;;
  Linux) extension="so" ;;
  *) echo "unsupported dynamic-library platform" >&2; exit 2 ;;
esac

cargo run -p connector --example dynamic_plugin_smoke --no-default-features -- \
  "target/debug/libtitan_connector_binance_futures_plugin.${extension}" binance-futures \
  "target/debug/libtitan_connector_okx_plugin.${extension}" okx \
  "target/debug/libtitan_connector_hyperliquid_plugin.${extension}" hyperliquid

cargo run -p titan-cli --example dynamic_core_smoke -- \
  "target/debug/libtitan_connector_binance_futures_plugin.${extension}" \
  "target/debug/libtitan_connector_okx_plugin.${extension}" \
  "target/debug/libtitan_connector_hyperliquid_plugin.${extension}"
