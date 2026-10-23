#!/usr/bin/env bash
# Public data only. Run on the designated build server; never reads account credentials.
set -euo pipefail
destination=${1:?usage: download_grid_history.sh OUTPUT_DIRECTORY}
mkdir -p "$destination"
cd "$destination"
range_part() {
    local url=$1 file=$2 start=$3 end=$4 expected=$(( $4 - $3 + 1 )) have received attempt
    for attempt in {1..5}; do
        have=0; [[ ! -f "$file" ]] || have=$(stat -c %s "$file")
        (( have <= expected )) || { rm "$file"; have=0; }
        (( have != expected )) || return 0
        # A slow connection can resume from its verified byte offset on a fresh request.
        curl --fail --location --connect-timeout 15 --max-time 180 --speed-time 30 --speed-limit 16384 \
            --silent --show-error --range "$((start + have))-$end" "$url" -o "$file.tail" || true
        received=0; [[ ! -f "$file.tail" ]] || received=$(stat -c %s "$file.tail")
        (( received <= expected - have )) || return 1
        if (( received > 0 )); then cat "$file.tail" >> "$file"; fi
        rm -f "$file.tail"
    done
    [[ $(stat -c %s "$file") == "$expected" ]]
}
download() {
    local url=$1 name=$2 size part start end width
    size=$(curl --fail --location --head --silent --show-error --max-time 30 "$url" | tr -d '\r' | awk 'tolower($1) == "content-length:" {print $2}' | tail -n 1)
    [[ "$size" =~ ^[0-9]+$ ]] || { printf 'missing archive length: %s\n' "$url" >&2; return 1; }
    if (( size < 33554432 )); then
        curl --fail --location --retry 3 --continue-at - --connect-timeout 15 --max-time 600 --silent --show-error "$url" -o "$name.part"
    else
        width=$(( (size + 7) / 8 ))
        local pids=() files=() failed=0
        for part in {0..7}; do
            start=$((part * width)); end=$((start + width - 1)); (( end < size )) || end=$((size - 1))
            files+=("$name.range-$part")
            range_part "$url" "$name.range-$part" "$start" "$end" &
            pids+=("$!")
        done
        for part in "${pids[@]}"; do wait "$part" || failed=1; done
        (( failed == 0 )) || return 1
        cat "${files[@]}" > "$name.part"
        rm "${files[@]}"
    fi
    mv "$name.part" "$name"
}
fetch() {
    local relative=$1 name=${1##*/}
    curl --fail --location --retry 3 --connect-timeout 15 --max-time 600 --silent --show-error \
        "https://data.binance.vision/data/futures/um/$relative.CHECKSUM" -o "$name.CHECKSUM"
    if ! test -f "$name" || ! sha256sum --status -c "$name.CHECKSUM"; then
        download "https://data.binance.vision/data/futures/um/$relative" "$name"
    fi
    sha256sum -c "$name.CHECKSUM"
    unzip -p "$name" > "${name%.zip}.extract-$BASHPID.tmp"
    mv "${name%.zip}.extract-$BASHPID.tmp" "${name%.zip}.csv"
    printf '%s\n' "https://data.binance.vision/data/futures/um/$relative" > "$name.url"
}
for day in 2023-09-02 2023-10-23 2023-08-17; do
    for kind in aggTrades bookTicker; do
        fetch "daily/$kind/BTCUSDT/BTCUSDT-$kind-$day.zip"
    done
done
for month in 2023-09 2023-10 2023-08; do
    fetch "monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-$month.zip"
done
sha256sum ./*.zip ./*.csv > input-sha256.txt
