#!/usr/bin/env bash
# Run on 192.168.3.88 only. Build the reviewed ABI without account credentials.
set -euo pipefail
revision=aefa96848a105d82a6bbb55d3d05663293a71713
if [[ $# != 2 ]]; then echo 'usage: build_lighter_signer.sh BUILD_DIRECTORY OUTPUT_SO' >&2; exit 2; fi
build_directory=$1
signer_output=$2
if [[ $(uname -s) != Linux || "$build_directory" != /* || "$signer_output" != /*.so ]]; then echo 'Linux and absolute build/output.so paths required' >&2; exit 2; fi
if [[ -e "$build_directory" ]]; then echo 'build directory must be new' >&2; exit 2; fi
mkdir -p "$(dirname "$signer_output")"
# Sparse checkout excludes upstream prebuilt WASM/mobile binaries, which are not build inputs.
git clone --filter=blob:none --no-checkout --sparse https://github.com/elliottech/lighter-go.git "$build_directory"
cd "$build_directory"
git checkout --detach "$revision"
[[ $(git rev-parse HEAD) == "$revision" ]] || { echo 'upstream revision mismatch' >&2; exit 1; }
git sparse-checkout set --no-cone '/client/' '/signer/' '/types/' '/sharedlib/' '/go.mod' '/go.sum' '/LICENSE'
GOTOOLCHAIN=local CGO_ENABLED=1 go build -trimpath -buildmode=c-shared -o "$signer_output" ./sharedlib
cp LICENSE "$signer_output.LICENSE"
{
  echo "source=https://github.com/elliottech/lighter-go/tree/$revision"
  go version
  go version -m "$signer_output"
  sha256sum "$signer_output" "${signer_output%.so}.h"
} > "$signer_output.build.txt"
