#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

# Fresh Bloom-enabled SSTs, followed by one RTC 80/20 experiment and P1/P2 plots.
set -euo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd -- "${script_dir}/../.." && pwd)
stamp=$(date +%Y%m%d-%H%M%S)-$$
db_bench=${DB_BENCH:-${repo_dir}/db_bench}
python_bin=${PYTHON:-python3}
entries=${RTC_P1_ENTRIES:-200000000}
operations=${RTC_P1_OPERATIONS:-32000000}
seed=${RTC_P1_SEED:-1000}
db_root=${RTC_P1_DB_ROOT:-/mnt/nvme/rtc_motivation_runs}
work_dir=${db_root}/rtc_p1_cache32_bloom_${stamp}
db=${work_dir}/db
result_dir=${RTC_P1_RESULT_DIR:-${repo_dir}/trace-zhao/motivation/rtc-p1-cache32-bloom/${stamp}}
raw=${result_dir}/raw/rtc
cache_size=33554432  # 32 MiB block cache, not total process memory.
bloom_bits=10       # Full Bloom filter, 10 bits/key.

case ${1:-} in
  --help|-h)
    cat <<'HELP'
Usage: bash rtc_miss_count_latency_cache32_bloom.sh [--dry-run]

Build a fresh 200M-key database (key=16 B, value=500 B, Snappy) with Bloom
filters, then run 32M randomly interleaved operations: 80% Get / 20% Put.
One foreground thread; RTC threshold=0.01; wakeup=1 second; RAC disabled.
Both loading and measurement use 32 MiB block cache and Bloom 10 bits/key.
The finished run automatically generates Miss-count/latency and interference
plots. All DBs/logs are retained; no existing template is changed.

Overrides: DB_BENCH, PYTHON, RTC_P1_ENTRIES, RTC_P1_OPERATIONS, RTC_P1_SEED,
           RTC_P1_DB_ROOT (default /mnt/nvme/rtc_motivation_runs),
           RTC_P1_RESULT_DIR (must be new).
OS page caches are not globally dropped; measurement uses a new db_bench
process after loading and draining the fresh DB. No build is performed.
HELP
    exit 0 ;;
  --dry-run|'') ;;
  *) echo "[ERROR] Unknown option: $1" >&2; exit 2 ;;
esac
(( $# <= 1 )) || { echo '[ERROR] Too many arguments' >&2; exit 2; }
for value in "$entries" "$operations"; do
  [[ $value =~ ^[1-9][0-9]*$ ]] || { echo '[ERROR] Counts must be positive integers' >&2; exit 2; }
done
[[ $seed =~ ^(0|[1-9][0-9]*)$ ]] || { echo '[ERROR] Seed must be nonnegative' >&2; exit 2; }

common=("--db=${db}" "--num=${entries}" --threads=1 --key_size=16
        --value_size=500 --compression_type=snappy --max_background_jobs=2
        --uni=0 "--seed=${seed}" "--cache_size=${cache_size}"
        "--bloom_bits=${bloom_bits}" --use_ribbon_filter=false
        --statistics --histogram --perf_level=3 --report_bg_io_stats=true
        --level0_file_num_compaction_trigger=3 --enable_rac=false)
load=("${db_bench}" "${common[@]}" --benchmarks=fillrandom,flush,waitforcompaction,stats
      "--writes=${entries}" --read_triggered_compaction_threshold=0)
measure=("${db_bench}" "${common[@]}"
         --benchmarks=readrandomwriterandom,waitforcompaction,stats
         "--reads=${operations}" "--writes=${operations}" --readwritepercent=80
         --readwrite_random_interleave=1 --use_existing_db=true
         --read_triggered_compaction_threshold=0.01
         --max_compaction_trigger_wakeup_seconds=1
         "--rtc_motivation_sst_log=${raw}/rtc_sst_observations.csv")
plot=("${python_bin}" "${repo_dir}/trace-zhao/motivation/graph/plot_rtc_p1_p2.py"
      --run-dir "${raw}" --p1-output "${result_dir}/figures/miss_count_vs_latency.png"
      --p2-output "${result_dir}/figures/compaction_interference.png")

echo "[PLAN] fresh load=${entries} keys; workload=${operations} operations; Get/Put=80/20"
echo "[PLAN] cache=32 MiB; Bloom=10 bits/key; RTC threshold=0.01 wakeup=1s"
echo "[PLAN] DB=${db}"
echo "[PLAN] results=${result_dir}"
if [[ ${1:-} == --dry-run ]]; then
  for name in load measure plot; do
    declare -n planned_command=$name
    printf '[DRY-RUN] %s: ' "$name"
    printf '%q ' "${planned_command[@]}"
    printf '\n'
    unset -n planned_command
  done
  exit 0
fi

[[ -x ${db_bench} ]] || { echo "[ERROR] Missing executable: ${db_bench}" >&2; exit 1; }
[[ ! -e ${result_dir} && ! -e ${work_dir} ]] || {
  echo '[ERROR] Result/DB directory already exists; choose a new path' >&2; exit 1;
}
command -v "${python_bin}" >/dev/null
"${python_bin}" -c 'import matplotlib, numpy'
help_text=$("${db_bench}" --help 2>&1 || true)
[[ $help_text == *rtc_motivation_sst_log* ]] || {
  echo '[ERROR] db_bench lacks RTC SST-observation logging; use the instrumented binary' >&2; exit 1;
}
ulimit -n 1048576 2>/dev/null || true
mkdir -p -- "${result_dir}/load" "${raw}/rac-telemetry" "${result_dir}/figures" "${db_root}"
mkdir -- "${work_dir}"
cp -- "${BASH_SOURCE[0]}" "${result_dir}/runner.sh"
cat > "${result_dir}/experiment.conf" <<CONF
db=${db}
entries=${entries}
operations=${operations}
seed=${seed}
threads=1
readwritepercent=80
value_size=500
key_size=16
uni=0
cache_size=${cache_size}
bloom_bits=${bloom_bits}
fresh_db_loaded_with_bloom=1
rtc_threshold=0.01
rtc_wakeup_seconds=1
page_cache=not_dropped
database_retained=1
CONF
printf '%q ' "${load[@]}" > "${result_dir}/load/command.sh"
printf '\n' >> "${result_dir}/load/command.sh"
echo '[LOAD] Creating a fresh DB with Bloom-enabled SSTs (existing DBs are untouched)'
status=0
"${load[@]}" > "${result_dir}/load/db_bench.log" 2>&1 || status=$?
[[ ! -f ${db}/LOG ]] || cp -- "${db}/LOG" "${result_dir}/load/LOG"
if ((status)); then
  tail -n 25 "${result_dir}/load/db_bench.log" >&2
  echo "[ERROR] Load failed; DB/logs retained: ${result_dir}" >&2
  exit "$status"
fi
if [[ -e ${db}/rac-telemetry || -L ${db}/rac-telemetry ]]; then
  mv -- "${db}/rac-telemetry" "${result_dir}/load/rac-telemetry"
fi
ln -s -- "${raw}/rac-telemetry" "${db}/rac-telemetry"
printf '%q ' "${measure[@]}" > "${raw}/command.sh"
printf '\n' >> "${raw}/command.sh"
echo '[RUN] RTC 80/20 workload; raw logs are saved even if no RTC repair fires'
status=0
"${measure[@]}" > "${raw}/db_bench.log" 2>&1 || status=$?
[[ ! -f ${db}/LOG ]] || cp -- "${db}/LOG" "${raw}/LOG"
printf '%s\n' "${status}" > "${result_dir}/workload_exit_status.txt"
if ((status)); then
  tail -n 25 "${raw}/db_bench.log" >&2
  echo "[ERROR] Workload failed; DB/logs retained: ${result_dir}" >&2
  exit "$status"
fi
echo '[PLOT] Drawing from actual RTC triggers; no replacement/simulated samples'
"${plot[@]}"
echo "[OK] Figures: ${result_dir}/figures; DB retained: ${db}"
