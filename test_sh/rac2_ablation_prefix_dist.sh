#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
#
# Run six RAC2 policy variants plus a RocksDB baseline on Prefix_dist.

set -euo pipefail

# Load the complete runner before executing long jobs. Bash otherwise reads
# later commands from this file after a job finishes; editing the file while
# it is running can invalidate that read position.
if [[ -n ${BASH_SOURCE[0]:-} ]]; then
  rac2_runner_path=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/$(basename -- "${BASH_SOURCE[0]}")
  rac2_runner_body=$(<"${rac2_runner_path}")
  exec bash -c "${rac2_runner_body}" "${rac2_runner_path}" "$@"
fi

ulimit -n 1048576 2>/dev/null || echo "[WARN] Could not raise open file limit; current limit: $(ulimit -n)"
echo "[INFO] open file limit: $(ulimit -n)"

script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(cd -- "${script_dir}/../.." && pwd)

usage() {
  cat <<'HELP'
Usage: rac2_ablation_prefix_dist.sh [OPTIONS] [TEMPLATE_DB]

Run seven variants sequentially per repetition:
  Full, Core, Uniform-Miss-Cost, Seed-Cost, No-Admission, No-Source-Guard,
  RocksDB
RocksDB uses the same binary with RAC, DP, and read-triggered repair disabled;
ordinary automatic compaction stays enabled.
Design 3 now disables the source-side guard by default. No-Source-Guard is
retained as a compatibility label and has the same policy as Core.

TEMPLATE_DB defaults to /mnt/nvme/fast_template_2196275. Override it with a
positional argument or RAC2_ABLATION_TEMPLATE. The template must be idle. Every
run gets a private cp --reflink=auto clone; writable DB files are never
hard-linked to the template.

Options:
  --dry-run       Print repetition order and exact db_bench commands. Do not
                  create output directories, clone DBs, or execute db_bench.
  --help          Show this help.

Principal environment overrides:
  RAC2_ABLATION_TEMPLATE             Initial DB template (positional wins;
                                    default: /mnt/nvme/fast_template_2196275)
  RAC2_ABLATION_DB_BENCH             Binary (default: <repo>/db_bench)
  RAC2_ABLATION_RESULT_ROOT          New result directory
  RAC2_ABLATION_DB_ROOT              New private-clone directory
  RAC2_ABLATION_REPEATS              Repetitions (default: 1)
  RAC2_ABLATION_SEED                 First repeat seed (default: 1000)
  RAC2_ABLATION_THREADS              Foreground threads (default: 1)
  RAC2_ABLATION_READS                Operations per thread (default: 50000000)
  RAC2_ABLATION_NUM                  Key count (default: 50000000)
  RAC2_ABLATION_KEY_SIZE             Key bytes (default: 48)
  RAC2_ABLATION_VALUE_SIZE           Value bytes (default: 1000)
  RAC2_ABLATION_CACHE_SIZE           Block cache bytes (default: 268435456)
  RAC2_ABLATION_MAX_BACKGROUND_JOBS  Background jobs (default: 2)
  RAC2_ABLATION_WINDOW               RAC window reads (default: 100000)
  RAC2_ABLATION_TOP_K                Top-K seeds (default: 6)
  RAC2_ABLATION_ETA                  Efficiency threshold (default: 0.01)
  RAC2_ABLATION_P                    Hotspot persistence (default: 0.8)
  RAC2_ABLATION_H                    Prediction horizon (default: 8)
  RAC2_ABLATION_Q                    Cost weight q > 0 (default: 0.08;
                                      smaller values relax RAC2 admission)
  RAC2_ABLATION_DP_TOP_K             DP maintenance search per level
                                      (default: 50; range: 1..50)
  RAC2_ABLATION_DP_ALLOW_TTL_PRIORITY Allow TTL-influenced size priority
                                      (default: 1; actual cleanup excluded)
  RAC2_ABLATION_BANDWIDTH_MIB_S      Initial rewrite bandwidth in MiB/s
                                      (default: 400; used until pair history exists)
  RAC2_ABLATION_EXTRA_ARGS           Additional whitespace-separated flags
  RAC2_ABLATION_KEEP_DB              Keep successful DB clones (default: 0)

All six RAC variants share the configured P and H. Set P=1,H=1 to use the
paper's one-window benefit. NUM/key/value settings must match the template.
The script never builds or replaces db_bench and never runs variants in
parallel. Failed-run DBs are retained for diagnosis.
HELP
}

dry_run=0
template_arg=
while (($#)); do
  case $1 in
    --dry-run) dry_run=1 ;;
    -h|--help) usage; exit 0 ;;
    --*) echo "[ERROR] Unknown option: $1" >&2; usage >&2; exit 2 ;;
    *)
      [[ -z ${template_arg} ]] || {
        echo "[ERROR] Only one TEMPLATE_DB positional argument is accepted" >&2
        exit 2
      }
      template_arg=$1
      ;;
  esac
  shift
done

template=${template_arg:-${RAC2_ABLATION_TEMPLATE:-/mnt/new1/fast_template_2196275}}
[[ -n ${template} ]] || {
  echo "[ERROR] Pass TEMPLATE_DB or set RAC2_ABLATION_TEMPLATE" >&2
  exit 2
}

stamp=$(date +%Y%m%d-%H%M%S)
db_bench=${RAC2_ABLATION_DB_BENCH:-${repo_dir}/db_bench}
result_root=${RAC2_ABLATION_RESULT_ROOT:-${repo_dir}/trace-zhao/graphs/rac2-ablation/${stamp}}
template_parent=$(cd -- "$(dirname -- "${template}")" && pwd)
db_root=${RAC2_ABLATION_DB_ROOT:-${template_parent}/rac2-ablation-db-${stamp}-$$}
repeats=${RAC2_ABLATION_REPEATS:-1}
base_seed=${RAC2_ABLATION_SEED:-1000}
threads=${RAC2_ABLATION_THREADS:-1}
reads=${RAC2_ABLATION_READS:-50000000}
num=${RAC2_ABLATION_NUM:-200000000}
key_size=${RAC2_ABLATION_KEY_SIZE:-48}
value_size=${RAC2_ABLATION_VALUE_SIZE:-1000}
cache_size=${RAC2_ABLATION_CACHE_SIZE:-268435456}
bloom_bits=${RAC2_ABLATION_BLOOM_BITS:-10}
max_background_jobs=${RAC2_ABLATION_MAX_BACKGROUND_JOBS:-2}
window=${RAC2_ABLATION_WINDOW:-100000}
top_k=${RAC2_ABLATION_TOP_K:-6}
eta=${RAC2_ABLATION_ETA:-0.01}
hotspot_persistence=${RAC2_ABLATION_P:-0.8}
prediction_horizon=${RAC2_ABLATION_H:-8}

bandwidth_mib_s=${RAC2_ABLATION_BANDWIDTH_MIB_S:-400}
high_miss_ratio=${RAC2_ABLATION_HIGH_MISS_RATIO:-0.95}
allow_l0_writes=${RAC2_ABLATION_ALLOW_L0_WITH_WRITES:-1}

cost_weight=${RAC2_ABLATION_Q:-0.08}
bottom_overlap=${RAC2_ABLATION_BOTTOM_OVERLAP_THRESHOLD:-32}
cooldown_windows=${RAC2_ABLATION_COOLDOWN_WINDOWS:-1}

read_benefit_slack=${RAC2_ABLATION_READ_BENEFIT_SLACK:-0.8}
emerging_score_floor=${RAC2_ABLATION_EMERGING_SCORE_FLOOR:-0.8}
dp_projection_top_k=${RAC2_ABLATION_DP_TOP_K:-10}
dp_allow_ttl_priority=${RAC2_ABLATION_DP_ALLOW_TTL_PRIORITY:-1}
num_bottom_pri_threads=${RAC2_ABLATION_NUM_BOTTOM_PRI_THREADS:-2}
keep_db=${RAC2_ABLATION_KEEP_DB:-0}
extra_args_text=${RAC2_ABLATION_EXTRA_ARGS:-}

if [[ -n ${extra_args_text} ]]; then
  read -r -a configured_extra_args <<< "${extra_args_text}"
  for extra_arg in "${configured_extra_args[@]}"; do
    case ${extra_arg} in
      --compression_type=*|--compression_ratio=*|--num_levels=*|\
      --write_buffer_size=*|--max_write_buffer_number=*|\
      --min_write_buffer_number_to_merge=*|--target_file_size_base=*|\
      --target_file_size_multiplier=*|--max_bytes_for_level_base=*|\
      --max_bytes_for_level_multiplier=*|--level0_file_num_compaction_trigger=*|\
      --level0_slowdown_writes_trigger=*|--level0_stop_writes_trigger=*|\
      --compaction_pri=*|--subcompactions=*|--bloom_bits=*|--block_size=*|\
      --open_files=*|--use_direct_reads=*|\
      --use_direct_io_for_flush_and_compaction=*|--disable_wal=*)
        ;;
      *)
        echo "[ERROR] RAC2_ABLATION_EXTRA_ARGS only accepts explicit --name=value layout flags; rejected: ${extra_arg}" >&2
        exit 2
        ;;
    esac
  done
fi

positive_values=("$repeats" "$threads" "$reads" "$num" "$key_size" "$value_size"
                 "$cache_size" "$max_background_jobs" "$window" "$top_k"
                 "$prediction_horizon" "$num_bottom_pri_threads"
                 "$dp_projection_top_k")
for value in "${positive_values[@]}"; do
  [[ ${value} =~ ^[1-9][0-9]*$ ]] || {
    echo "[ERROR] Expected a positive integer, got: ${value}" >&2
    exit 2
  }
done
[[ ${base_seed} =~ ^(0|[1-9][0-9]*)$ && ${keep_db} =~ ^[01]$ &&
   ${allow_l0_writes} =~ ^[01]$ && ${dp_allow_ttl_priority} =~ ^[01]$ ]] || {
  echo "[ERROR] Invalid seed or boolean setting" >&2
  exit 2
}
((dp_projection_top_k <= 50)) || {
  echo "[ERROR] RAC2_ABLATION_DP_TOP_K must be in [1, 50]" >&2
  exit 2
}
python3 - "$eta" "$hotspot_persistence" "$bandwidth_mib_s" "$cost_weight" <<'PY'
import math
import sys
eta, persistence, bandwidth, cost_weight = map(float, sys.argv[1:])
if not math.isfinite(eta) or eta < 0:
    raise SystemExit("RAC2_ABLATION_ETA must be finite and nonnegative")
if not math.isfinite(persistence) or not 0 <= persistence <= 1:
    raise SystemExit("RAC2_ABLATION_P must be finite and in [0, 1]")
if not math.isfinite(bandwidth) or bandwidth <= 0:
    raise SystemExit("RAC2_ABLATION_BANDWIDTH_MIB_S must be positive")
if not math.isfinite(cost_weight) or cost_weight <= 0:
    raise SystemExit("RAC2_ABLATION_Q must be finite and positive")
PY

template=$(python3 - "${template}" <<'PY'
from pathlib import Path
import sys
print(Path(sys.argv[1]).resolve())
PY
)
result_root=$(python3 - "${result_root}" <<'PY'
from pathlib import Path
import sys
print(Path(sys.argv[1]).resolve())
PY
)
db_root=$(python3 - "${db_root}" <<'PY'
from pathlib import Path
import sys
print(Path(sys.argv[1]).resolve())
PY
)

[[ -d ${template} && -f ${template}/CURRENT ]] || {
  echo "[ERROR] Template is not a RocksDB DB (CURRENT missing): ${template}" >&2
  exit 1
}
[[ ! -e ${result_root} ]] || {
  echo "[ERROR] Refusing output collision: ${result_root}" >&2
  exit 1
}
[[ ! -e ${db_root} ]] || {
  echo "[ERROR] Refusing DB-root collision: ${db_root}" >&2
  exit 1
}

python3 - "$template" "$result_root" "$db_root" <<'PY'
from pathlib import Path
import sys
template, result, db_root = map(lambda value: Path(value).resolve(), sys.argv[1:])
def nested(left, right):
    return left == right or left in right.parents or right in left.parents
if nested(template, result):
    raise SystemExit(f"Unsafe nesting between template and result root: {template} / {result}")
if nested(template, db_root):
    raise SystemExit(f"Unsafe nesting between template and DB root: {template} / {db_root}")
if nested(result, db_root):
    raise SystemExit(f"Unsafe nesting between result and DB roots: {result} / {db_root}")
PY
for csv_path in "$template" "$result_root" "$db_root"; do
  [[ ${csv_path} != *','* && ${csv_path} != *$'\n'* ]] || {
    echo "[ERROR] Template/result/DB paths cannot contain commas or newlines: ${csv_path}" >&2
    exit 2
  }
done

variant_names=(Full Core Uniform-Miss-Cost Seed-Cost No-Admission No-Source-Guard RocksDB)
variant_slugs=(full core uniform_miss_cost seed_cost no_admission no_source_guard rocksdb)
variant_ablations=(0 0 1 2 3 4 0)
variant_dp=(1 0 0 0 0 0 0)
variant_rac=(1 1 1 1 1 1 0)
variant_source_guard=(none none none none none none disabled)
variant_count=${#variant_names[@]}

write_command() {
  local output=$1
  shift
  printf '%q ' "$@" > "${output}"
  printf '\n' >> "${output}"
}

build_command() {
  local run_db=$1 jobs_csv=$2 seed=$3 ablation=$4 dp=$5 rac_enabled=$6
  command=(
    "${db_bench}"
    "--benchmarks=mixgraph,stats"
    "--db=${run_db}"
    "--use_existing_db=true"
    "--cache_size=0"
    "--bloom_bits=0"
    "--key_dist_a=0.002312"
    "--key_dist_b=0.3467"
    "--keyrange_dist_a=14.18"
    "--keyrange_dist_b=-2.917"
    "--keyrange_dist_c=0.0164"
    "--keyrange_dist_d=-0.08082"
    "--keyrange_num=30"
    "--value_k=0.2615"
    "--value_sigma=25.45"
    "--iter_k=2.517"
    "--iter_sigma=14.236"
    "--mix_get_ratio=0.9"
    "--mix_put_ratio=0.1"
    "--mix_seek_ratio=0.00"
    "--sine_mix_rate_interval_milliseconds=5000"
    "--sine_a=1000"
    "--sine_b=0.0000073"
    "--sine_d=45000"
    "--perf_level=3"
    "--threads=${threads}"
    "--reads=${reads}"
    "--num=${num}"
    "--key_size=${key_size}"
    "--value_size=${value_size}"
    "--max_background_jobs=${max_background_jobs}"
    "--statistics"
    "--histogram"
    "--report_bg_io_stats=true"
    "--seed=${seed}"
    "--enable_rac=$([[ ${rac_enabled} == 1 ]] && echo true || echo false)"
    "--read_triggered_compaction_threshold=0"
    "--rac_design_option=3"
    "--rac2_ablation=${ablation}"
    "--rac2_ablation_diagnostics=true"
    "--rac2_ablation_log=${jobs_csv}"
    "--rac2_hotspot_persistence=${hotspot_persistence}"
    "--rac2_prediction_horizon=${prediction_horizon}"
    "--rac2_cost_weight=${cost_weight}"
    "--rac2_compaction_bandwidth_mib_per_sec=${bandwidth_mib_s}"
    "--rac_window_size=${window}"
    "--rac_top_k=${top_k}"
    "--rac_benefit_efficiency_threshold=${eta}"
    "--rac_high_miss_ratio_threshold=${high_miss_ratio}"
    "--rac_option2_allow_l0_with_writes=${allow_l0_writes}"
    "--rac_bottom_overlap_threshold=${bottom_overlap}"
    "--rac_cooldown_windows=${cooldown_windows}"
    "--rac_option2_dual_purpose_selection=$([[ ${dp} == 1 ]] && echo true || echo false)"
    "--rac_option2_dp_projection_top_k=${dp_projection_top_k}"
    "--rac_option2_dp_allow_ttl_priority=$([[ ${dp_allow_ttl_priority} == 1 ]] && echo true || echo false)"
    "--rac_option2_read_benefit_slack=${read_benefit_slack}"
    "--rac_option2_emerging_score_floor=${emerging_score_floor}"
    "--num_bottom_pri_threads=${num_bottom_pri_threads}"
  )
  if [[ -n ${extra_args_text} ]]; then
    local -a extra_args
    read -r -a extra_args <<< "${extra_args_text}"
    command+=("${extra_args[@]}")
  fi
}

echo "[PLAN] Prefix_dist RAC2 ablation: repeats=${repeats}, runs=$((repeats * variant_count)), threads=${threads}"
echo "[PLAN] design=3 cost_model=input_only_v1 P=${hotspot_persistence} H=${prediction_horizon} q=${cost_weight} template=${template}"
echo "[PLAN] source_guard=none (No-Source-Guard is a compatibility label)"
echo "[PLAN] DP maintenance_top_k=${dp_projection_top_k} allow_ttl_priority=${dp_allow_ttl_priority}"
for ((repeat=1; repeat<=repeats; ++repeat)); do
  seed=$((base_seed + repeat - 1))
  offset=$(((repeat - 1) % variant_count))
  printf '[PLAN] repeat=%d seed=%d order=' "$repeat" "$seed"
  for ((position=0; position<variant_count; ++position)); do
    index=$(((position + offset) % variant_count))
    printf '%s%s' "$([[ ${position} == 0 ]] || echo ',')" "${variant_names[index]}"
  done
  printf '\n'
  if ((dry_run)); then
    for ((position=0; position<variant_count; ++position)); do
      index=$(((position + offset) % variant_count))
      run_name=$(printf 'repeat%02d-order%02d-%s' "$repeat" "$((position + 1))" "${variant_slugs[index]}")
      build_command "${db_root}/${run_name}" "${result_root}/raw/${run_name}/jobs.csv" \
        "$seed" "${variant_ablations[index]}" "${variant_dp[index]}" "${variant_rac[index]}"
      printf '[DRY-RUN] '
      printf '%q ' "${command[@]}"
      printf '\n'
    done
  fi
done
((dry_run)) && exit 0

configured_open_files=-1
if [[ -n ${extra_args_text} ]]; then
  for extra_arg in "${configured_extra_args[@]}"; do
    if [[ ${extra_arg} == --open_files=* ]]; then
      configured_open_files=${extra_arg#*=}
    fi
  done
fi
# Detect an insufficient inherited limit before copying a large template.
python3 - "$template" "$(ulimit -Sn)" "$(ulimit -Hn)" "$configured_open_files" <<'PY'
import os
import sys
template, soft, hard, configured = sys.argv[1:]
try:
    open_files = int(configured)
except ValueError:
    raise SystemExit("[ERROR] --open_files must be an integer")
if open_files < -1:
    raise SystemExit("[ERROR] --open_files must be at least -1")
with os.scandir(template) as files:
    sst_count = sum(entry.name.endswith(".sst") for entry in files)
print(f"[INFO] template SST files: {sst_count}")
cached_files = sst_count if open_files == -1 else min(sst_count, max(open_files, 20))
required = cached_files + 256
if soft != "unlimited" and int(soft) < required:
    raise SystemExit(
        f"[ERROR] Open-file limit is too low before DB open: soft={soft}, "
        f"hard={hard}, SSTs={sst_count}, --open_files={open_files}. "
        f"Need at least {required} including headroom; no DB clone was created."
    )
PY

[[ -x ${db_bench} ]] || {
  echo "[ERROR] db_bench is not executable: ${db_bench}" >&2
  exit 1
}
[[ -f ${template}/LOCK ]] || {
  echo "[ERROR] Template LOCK file is required to prove an idle clone: ${template}/LOCK" >&2
  exit 1
}

# A stale binary silently collapses all variants into the same policy. Inspect
# its own gflags help before creating any DB clone.
help_text=$("${db_bench}" --help 2>&1 || true)
for required_flag in rac2_ablation rac2_ablation_diagnostics rac2_ablation_log rac2_cost_weight \
                     rac_option2_dp_projection_top_k rac_option2_dp_allow_ttl_priority; do
  [[ ${help_text} == *"${required_flag}"* ]] || {
    echo "[ERROR] ${db_bench} lacks --${required_flag}; build the updated binary before running this experiment" >&2
    exit 1
  }
done

[[ ${help_text} == *"input_only_v1"* ]] || {
  echo "[ERROR] ${db_bench} lacks the input_only_v1 cost model; the updated source must be built before this experiment" >&2
  exit 1
}
[[ ${help_text} == *"overall_request_p99_v1"* ]] || {
  echo "[ERROR] ${db_bench} lacks overall-request P99 telemetry; the updated source must be built before this experiment" >&2
  exit 1
}
[[ ${help_text} == *"rac2_baseline_telemetry_v1"* ]] || {
  echo "[ERROR] ${db_bench} lacks RAC-disabled baseline telemetry; build the updated binary before running the seven-variant experiment" >&2
  exit 1
}
if [[ ${help_text} == *"rac3_no_source_guard_v1"* ]]; then
  source_guard_models=none_v1
  source_guard_policy_generation=guardless_v1
  no_source_guard_variant=compatibility_alias_of_core
else
  source_guard_models=count_or_none_v1
  source_guard_policy_generation=legacy_count_guard_v1
  no_source_guard_variant=distinct_legacy_ablation
  for index in "${!variant_rac[@]}"; do
    if [[ ${variant_rac[index]} == 0 ]]; then
      variant_source_guard[index]=disabled
    elif [[ ${variant_ablations[index]} == 4 ]]; then
      variant_source_guard[index]=none
    else
      variant_source_guard[index]=count
    fi
  done
  echo "[INFO] db_bench uses the historical Design 3 count guard; rebuild to enable the new guardless default"
fi

assert_template_idle() {
  python3 - "$template" <<'PY'
from pathlib import Path
import fcntl
import os
import sys
lock = Path(sys.argv[1]) / "LOCK"
if not lock.exists():
    raise SystemExit(0)
fd = os.open(lock, os.O_RDWR)
try:
    fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    fcntl.lockf(fd, fcntl.LOCK_UN)
except BlockingIOError:
    raise SystemExit(f"Template appears live (LOCK is held): {lock}")
finally:
    os.close(fd)
PY
}

assert_template_idle
mkdir -p -- "${result_root}/raw" "${db_root}"
printf '%s\n' "${BASH_EXECUTION_STRING}" > "${result_root}/runner.sh"

git -C "$repo_dir" rev-parse HEAD > "${result_root}/git-head.txt"
git -C "$repo_dir" status --short > "${result_root}/git-status.txt"
git -C "$repo_dir" diff --no-ext-diff --binary -- \
  db/column_family.cc db/db_impl/db_impl.cc db/db_impl/db_impl.h \
  db/db_impl/db_impl_compaction_flush.cc db/internal_stats.cc \
  db/internal_stats.h include/rocksdb/advanced_options.h \
  include/rocksdb/listener.h options/cf_options.cc options/cf_options.h \
  options/options_helper.cc tools/db_bench_tool.cc \
  > "${result_root}/rac2-source.diff"

cat > "${result_root}/experiment.conf" <<CONF
workload=Prefix_dist
variants=${variant_count}
rocksdb_baseline=same_binary_rac_disabled
source_guard_models=${source_guard_models}
source_guard_policy_generation=${source_guard_policy_generation}
no_source_guard_variant=${no_source_guard_variant}
cost_model=input_only_v1
template=${template}
db_bench=${db_bench}
repeats=${repeats}
base_seed=${base_seed}
threads=${threads}
reads=${reads}
num=${num}
key_size=${key_size}
value_size=${value_size}
cache_size=${cache_size}
max_background_jobs=${max_background_jobs}
rac_design_option=3
rac_window_size=${window}
rac_top_k=${top_k}
rac_benefit_efficiency_threshold=${eta}
rac2_hotspot_persistence=${hotspot_persistence}
rac2_prediction_horizon=${prediction_horizon}
rac2_cost_weight=${cost_weight}
rac2_compaction_bandwidth_mib_per_sec=${bandwidth_mib_s}
rac_option2_dp_projection_top_k=${dp_projection_top_k}
rac_option2_dp_allow_ttl_priority=${dp_allow_ttl_priority}
extra_args=${extra_args_text}
CONF
printf 'repeat,order,variant,slug,ablation,dp,seed,status,raw_dir,db_dir,cost_weight,dp_projection_top_k,dp_allow_ttl_priority,cost_model,rac_enabled,source_guard_model\n' > "${result_root}/manifest.csv"

safe_remove_run_db() {
  local path=$1
  python3 - "$db_root" "$path" <<'PY'
from pathlib import Path
import shutil
import sys
root, target = map(lambda value: Path(value).resolve(), sys.argv[1:])
if target.parent != root or not target.name.startswith("repeat") or target.is_symlink():
    raise SystemExit(f"Refusing unsafe DB removal: {target}")
shutil.rmtree(target)
PY
}

record_run_manifest() {
  local recorded_status=$1
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$repeat" "$((position + 1))" "${variant_names[index]}" \
    "${variant_slugs[index]}" "${variant_ablations[index]}" \
    "${variant_dp[index]}" "$seed" "$recorded_status" "$raw_dir" "$run_db" \
    "$cost_weight" "$dp_projection_top_k" "$dp_allow_ttl_priority" "input_only_v1" "${variant_rac[index]}" \
    "${variant_source_guard[index]}" \
    >> "${result_root}/manifest.csv"
}

validate_run_drain() {
  local jobs_csv=$1
  python3 - "$jobs_csv" <<'PY'
import csv
from collections import Counter
from pathlib import Path
import sys
path = Path(sys.argv[1])
with path.open(newline="") as source:
    rows = list(csv.DictReader(source))
drain = [row for row in rows if row.get("event") == "drain_end"]
if not drain:
    raise SystemExit(f"Missing drain_end event: {path}")
if (drain[-1].get("status") or "").strip().lower() != "ok":
    raise SystemExit(f"drain_end status is not OK in {path}: {drain[-1].get('status')}")
if int(drain[-1].get("outstanding_jobs") or 0) != 0:
    raise SystemExit(f"drain_end has outstanding jobs: {path}")
workload = [row for row in rows if row.get("event") == "workload_end"]
if len(workload) != 1 or (workload[0].get("status") or "").strip().lower() != "ok":
    raise SystemExit(f"Expected one successful workload_end event: {path}")
begun = Counter(
    (row.get("thread_id"), row.get("job_id"))
    for row in rows
    if row.get("event") == "job_begin"
)
finished = Counter(
    (row.get("thread_id"), row.get("job_id"))
    for row in rows
    if row.get("event") in ("job_completed", "job_failed")
)
if any(count != 1 for count in begun.values()):
    raise SystemExit(f"Duplicate job_begin events in {path}: {begun}")
if any(count != 1 for count in finished.values()):
    raise SystemExit(f"Duplicate terminal events in {path}: {finished}")
if begun != finished:
    raise SystemExit(f"Unmatched begin/terminal jobs in {path}: begin={begun}, terminal={finished}")
failed = [row for row in rows if row.get("event") == "job_failed"]
if failed:
    raise SystemExit(f"Compaction jobs failed in {path}; raw bytes retained: {len(failed)}")
bad_completed = [
    row for row in rows
    if row.get("event") == "job_completed"
    and (row.get("status") or "").strip().lower() != "ok"
]
if bad_completed:
    raise SystemExit(f"job_completed rows have non-OK status in {path}")
PY
}

clone_template() {
  local run_db=$1 raw_dir=$2
  # Hold RocksDB's POSIX record lock across the clone. This closes the gap
  # between an idle check and cp, so another process cannot open the template
  # while its files are being copied.
  python3 - "$template" "$run_db" <<'PY'
from pathlib import Path
import fcntl
import os
import subprocess
import sys
source, target = map(Path, sys.argv[1:])
fd = os.open(source / "LOCK", os.O_RDWR)
try:
    fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    os.close(fd)
    raise SystemExit(f"Template appears live (LOCK is held): {source / 'LOCK'}")
try:
    subprocess.run(
        ["cp", "-a", "--reflink=auto", "--", str(source), str(target)],
        check=True,
    )
finally:
    fcntl.lockf(fd, fcntl.LOCK_UN)
    os.close(fd)
PY
  python3 - "$template" "$run_db" "$raw_dir" <<'PY'
from pathlib import Path
import os
import sys
source, target, raw = map(Path, sys.argv[1:])
for path in source.rglob("*"):
    if path.is_symlink():
        if path.relative_to(source) != Path("rac-telemetry"):
            raise SystemExit(f"Unsupported template symlink: {path}")
        continue
    if not path.is_file() or path.is_symlink():
        continue
    other = target / path.relative_to(source)
    if path.stat().st_ino == other.stat().st_ino and path.stat().st_dev == other.stat().st_dev:
        raise SystemExit(f"Unsafe hard-linked writable clone file: {path}")
telemetry = target / "rac-telemetry"
if telemetry.is_symlink():
    telemetry.unlink()
elif telemetry.is_dir():
    import shutil
    shutil.rmtree(telemetry)
elif telemetry.exists():
    telemetry.unlink()
for inherited_log in target.iterdir():
    if inherited_log.name == "LOG" or inherited_log.name.startswith("LOG.old."):
        if inherited_log.is_dir() and not inherited_log.is_symlink():
            raise SystemExit(f"Inherited RocksDB log path is a directory: {inherited_log}")
        inherited_log.unlink()
(raw / "rac-telemetry").mkdir()
telemetry.symlink_to(raw / "rac-telemetry", target_is_directory=True)
PY
}

for ((repeat=1; repeat<=repeats; ++repeat)); do
  seed=$((base_seed + repeat - 1))
  offset=$(((repeat - 1) % variant_count))
  for ((position=0; position<variant_count; ++position)); do
    index=$(((position + offset) % variant_count))
    run_name=$(printf 'repeat%02d-order%02d-%s' "$repeat" "$((position + 1))" "${variant_slugs[index]}")
    raw_dir="${result_root}/raw/${run_name}"
    run_db="${db_root}/${run_name}"
    mkdir -p -- "$raw_dir"
    clone_template "$run_db" "$raw_dir"
    build_command "$run_db" "$raw_dir/jobs.csv" "$seed" \
      "${variant_ablations[index]}" "${variant_dp[index]}" "${variant_rac[index]}"
    write_command "$raw_dir/command.sh" "${command[@]}"
    cp -- "$raw_dir/command.sh" "$raw_dir/db_bench.log"
    echo "[RUN] repeat=${repeat} order=$((position + 1)) variant=${variant_names[index]} seed=${seed}"
    status=0
    "${command[@]}" >> "$raw_dir/db_bench.log" 2>&1 || status=$?
    # Preserve every log segment: decision counts must survive LOG rotation.
    python3 - "$run_db" "$raw_dir" <<'PY'
from pathlib import Path
import shutil
import sys
database, raw = map(Path, sys.argv[1:])
segments = sorted(
    (path for path in database.iterdir()
     if path.is_file() and (path.name == "LOG" or path.name.startswith("LOG.old."))),
    key=lambda path: (path.stat().st_mtime_ns, path.name),
)
(raw / "engine-logs").mkdir()
with (raw / "LOG").open("wb") as combined:
    for path in segments:
        shutil.copy2(path, raw / "engine-logs" / path.name)
        with path.open("rb") as source:
            shutil.copyfileobj(source, combined)
        combined.write(b"\n")
PY
    if ((status != 0)); then
      record_run_manifest "$status"
      echo "[ERROR] ${variant_names[index]} failed with status ${status}; DB retained: ${run_db}" >&2
      echo "[ERROR] db_bench diagnostics: ${raw_dir}/db_bench.log" >&2
      tail -n 25 "$raw_dir/db_bench.log" >&2
      exit "$status"
    fi
    [[ -s ${raw_dir}/jobs.csv ]] || {
      record_run_manifest 1
      echo "[ERROR] Missing runtime diagnostics: ${raw_dir}/jobs.csv; DB retained" >&2
      exit 1
    }
    if ! validate_run_drain "$raw_dir/jobs.csv"; then
      record_run_manifest 1
      echo "[ERROR] Incomplete drain diagnostics; DB retained: ${run_db}" >&2
      exit 1
    fi
    record_run_manifest 0
    if ((keep_db == 0)); then
      safe_remove_run_db "$run_db"
    fi
  done
done

assert_template_idle

MPLCONFIGDIR=${MPLCONFIGDIR:-/tmp/rac2-ablation-mpl} \
  python3 "${repo_dir}/trace-zhao/plot_rac2_ablation.py" "${result_root}"
echo "[OK] Completed ${repeats} repeats (${repeats}x${variant_count} runs): ${result_root}"
