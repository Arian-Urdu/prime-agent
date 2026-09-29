#!/bin/sh
# prepare.sh — prepare the cloud-image build context for the Rust resident
# image: fetch the pinned release artifacts and verify them against
# checksums.sha256. The artifacts are gitignored and never committed.
#
# Sources:
# - prime-agent-0.9.7-linux-x64.tar.gz — the v0.9.7 Rust release tarball
#   (GitHub release asset, matches the release SHA256SUMS entry).
# - frp_0.66.0_linux_amd64.tar.gz — the frp v0.66.0 release tarball.
set -eu

cd "$(dirname "$0")"

BASE_URL="https://github.com/PrimeIntellect-ai/prime-agent/releases/download/v0.9.7"
FRP_URL="https://github.com/fatedier/frp/releases/download/v0.66.0/frp_0.66.0_linux_amd64.tar.gz"

fetch() {
    url="$1"
    out="$2"
    if [ -f "$out" ]; then
        echo "prepare.sh: $out already present, skipping download"
    else
        echo "prepare.sh: fetching $url"
        curl -fsSL -o "$out" "$url"
    fi
}

fetch "$BASE_URL/prime-agent-0.9.7-linux-x64.tar.gz" prime-agent-0.9.7-linux-x64.tar.gz
fetch "$FRP_URL" frp_0.66.0_linux_amd64.tar.gz

echo "prepare.sh: verifying checksums"
sha256sum -c checksums.sha256

echo "prepare.sh: build context ready"
