#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

# Collect Prefix_dist data for two RAC validation experiments:
#   1. decision-time compaction duration prediction vs. actual job duration;
#   2. RocksDB baseline vs. RAC read-path profiling only.

set -euo pipefail

# Load the complete runner before starting long jobs. This keeps an edit to the
# on-disk script from changing an experiment that is already in progress.
if [[ -n ${BASH_SOURCE[0]:-} ]]; then
  validation_runner_path=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/$(basename -- "${BASH_SOURCE[0]}")
  validation_runner_body=$(<"${validation_runner_path}")
  exec bash -c "${validation_runner_body}" "${validation_runner_path}" "$@"
fi

script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(cd -- "${script_dir}/../.." && pwd)

usage() {
  cat <<'HELP'
Usage: rac_validation_prefix_dist.sh [OPTIONS] [TEMPLATE_DB]

Modes:
  --mode=all             Run both validations (default).
  --mode=execution-time  Run Full RAC3 only, for prediction validation.
  --mode=profiling       Run Baseline and Profiling-only in alternating order.

Options:
  --dry-run              Print the plan and exact db_bench commands only.
  --smoke                Use 1 repeat and 200000 operations per thread.
  --help                 Show this help.

TEMPLATE_DB defaults to /mnt/new1/fast_template_2196275; override it with a
positional argument or RAC_VALIDATION_TEMPLATE. It must be an idle
RocksDB database whose layout matches NUM/key/value settings. Each run uses an
independent private cp --reflink=auto copy. The template is never modified.

Current defaults use read-only mixgraph, 50M operations per
thread, num=50M, 3 repeats, cache_size=0,
bloom_bits=0, K=6, W=100000, p=0.8, H=8, q=0.08, and 400 MiB/s fallback.
For the formal validation use at least 3 repeats and a NUM matching the template.

Environment overrides:
  RAC_VALIDATION_DB_BENCH             db_bench binary (prefer the isolated
                                      build/rac-validation-release/db_bench
                                      when executable; otherwise repo db_bench)
  RAC_VALIDATION_RESULT_ROOT          new result directory
  RAC_VALIDATION_DB_ROOT              new private-clone directory (default:
                                      rac-validation-db-TIMESTAMP-PID beside
                                      the template DB, on the same filesystem)
  RAC_VALIDATION_REPEATS              repetitions (default: 3)
  RAC_VALIDATION_SEED                 first repeat seed (default: 1000)
  RAC_VALIDATION_THREADS              foreground threads (default: 1)
  RAC_VALIDATION_READS                operations per thread (default: 50000000)
  RAC_VALIDATION_NUM                  key count (default: 50000000)
  RAC_VALIDATION_KEEP_DB              retain successful clones (default: 1)
  RAC_VALIDATION_EXTRA_ARGS           explicit layout flags accepted by the
                                      current ablation runner

The runner does not build db_bench, drop host caches, modify the template, or
delete successful clones by default. Profiling-only observes read statistics
and Miss timing but does not evaluate candidates or submit read repair.
resources.csv and resources.json record process CPU and RSS every 0.5 seconds.
Resource scope includes DB open, workload, drain and close; excludes DB cloning.
HELP
}

mode=all
dry_run=0
smoke=0
template_arg=
while (($#)); do
  case $1 in
    --mode=all|--mode=execution-time|--mode=profiling) mode=${1#*=} ;;
    --dry-run) dry_run=1 ;;
    --smoke) smoke=1 ;;
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

template=${template_arg:-${RAC_VALIDATION_TEMPLATE:-/mnt/new1/fast_template_2196275}}

stamp=$(date +%Y%m%d-%H%M%S)
default_db_bench=${repo_dir}/db_bench
if [[ -x ${repo_dir}/build/rac-validation-release/db_bench ]]; then
  default_db_bench=${repo_dir}/build/rac-validation-release/db_bench
fi
db_bench=${RAC_VALIDATION_DB_BENCH:-${default_db_bench}}
result_root=${RAC_VALIDATION_RESULT_ROOT:-${repo_dir}/trace-zhao/graphs/rac-validation/${stamp}}
template_parent=$(dirname -- "$(realpath -m -- "${template}")")
db_root=${RAC_VALIDATION_DB_ROOT:-${template_parent}/rac-validation-db-${stamp}-$$}
repeats=${RAC_VALIDATION_REPEATS:-3}
base_seed=${RAC_VALIDATION_SEED:-1000}
threads=${RAC_VALIDATION_THREADS:-1}
reads=${RAC_VALIDATION_READS:-50000000}
num=${RAC_VALIDATION_NUM:-50000000}
key_size=${RAC_VALIDATION_KEY_SIZE:-48}
value_size=${RAC_VALIDATION_VALUE_SIZE:-1000}
max_background_jobs=${RAC_VALIDATION_MAX_BACKGROUND_JOBS:-2}
window=${RAC_VALIDATION_WINDOW:-100000}
top_k=${RAC_VALIDATION_TOP_K:-6}
eta=${RAC_VALIDATION_ETA:-0.01}
hotspot_persistence=${RAC_VALIDATION_P:-0.8}
prediction_horizon=${RAC_VALIDATION_H:-8}
cost_weight=${RAC_VALIDATION_Q:-0.08}
bandwidth_mib_s=${RAC_VALIDATION_BANDWIDTH_MIB_S:-400}
high_miss_ratio=${RAC_VALIDATION_HIGH_MISS_RATIO:-0.95}
allow_l0_writes=${RAC_VALIDATION_ALLOW_L0_WITH_WRITES:-1}
bottom_overlap=${RAC_VALIDATION_BOTTOM_OVERLAP_THRESHOLD:-32}
cooldown_windows=${RAC_VALIDATION_COOLDOWN_WINDOWS:-1}
read_benefit_slack=${RAC_VALIDATION_READ_BENEFIT_SLACK:-0.8}
emerging_score_floor=${RAC_VALIDATION_EMERGING_SCORE_FLOOR:-0.8}
dp_projection_top_k=${RAC_VALIDATION_DP_TOP_K:-10}
dp_allow_ttl_priority=${RAC_VALIDATION_DP_ALLOW_TTL_PRIORITY:-1}
num_bottom_pri_threads=${RAC_VALIDATION_NUM_BOTTOM_PRI_THREADS:-2}
keep_db=${RAC_VALIDATION_KEEP_DB:-1}
extra_args_text=${RAC_VALIDATION_EXTRA_ARGS:-}

if ((smoke)); then
  repeats=1
  # 100K mixed operations contain fewer than one 100K-Get observation window.
  reads=${RAC_VALIDATION_SMOKE_READS:-200000}
fi

positive_values=("$repeats" "$threads" "$reads" "$num" "$key_size" "$value_size"
                 "$max_background_jobs" "$window" "$top_k" "$prediction_horizon"
                 "$num_bottom_pri_threads" "$dp_projection_top_k")
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
  echo "[ERROR] RAC_VALIDATION_DP_TOP_K must be in [1, 50]" >&2
  exit 2
}
python3 - "$eta" "$hotspot_persistence" "$cost_weight" "$bandwidth_mib_s" <<'PY'
import math
import sys
eta, persistence, cost_weight, bandwidth = map(float, sys.argv[1:])
if not math.isfinite(eta) or eta < 0:
    raise SystemExit("RAC_VALIDATION_ETA must be finite and nonnegative")
if not math.isfinite(persistence) or not 0 <= persistence <= 1:
    raise SystemExit("RAC_VALIDATION_P must be finite and in [0, 1]")
if not math.isfinite(cost_weight) or cost_weight <= 0:
    raise SystemExit("RAC_VALIDATION_Q must be finite and positive")
if not math.isfinite(bandwidth) or bandwidth <= 0:
    raise SystemExit("RAC_VALIDATION_BANDWIDTH_MIB_S must be finite and positive")
PY

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
      --compaction_pri=*|--subcompactions=*|--block_size=*|--open_files=*|\
      --use_direct_reads=*|--use_direct_io_for_flush_and_compaction=*|--disable_wal=*)
        ;;
      *)
        echo "[ERROR] RAC_VALIDATION_EXTRA_ARGS rejected layout flag: ${extra_arg}" >&2
        exit 2
        ;;
    esac
  done
fi

variant_names=()
variant_slugs=()
variant_rac=()
variant_dp=()
variant_profiling=()
case ${mode} in
  all)
    variant_names=(Execution-Time Baseline Profiling-only)
    variant_slugs=(execution_time baseline profiling_only)
    variant_rac=(1 0 1)
    variant_dp=(1 0 0)
    variant_profiling=(0 0 1)
    ;;
  execution-time)
    variant_names=(Execution-Time)
    variant_slugs=(execution_time)
    variant_rac=(1)
    variant_dp=(1)
    variant_profiling=(0)
    ;;
  profiling)
    variant_names=(Baseline Profiling-only)
    variant_slugs=(baseline profiling_only)
    variant_rac=(0 1)
    variant_dp=(0 0)
    variant_profiling=(0 1)
    ;;
esac
variant_count=${#variant_names[@]}

build_command() {
  local run_db=$1 jobs_csv=$2 seed=$3 rac_enabled=$4 dp_enabled=$5 profiling_only=$6
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
    "--mix_get_ratio=1"
    "--mix_put_ratio=0.0"
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
    "--rac2_ablation=0"
    "--rac2_ablation_diagnostics=true"
    "--rac2_ablation_log=${jobs_csv}"
    "--rac2_profiling_only=$([[ ${profiling_only} == 1 ]] && echo true || echo false)"
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
    "--rac_option2_dual_purpose_selection=$([[ ${dp_enabled} == 1 ]] && echo true || echo false)"
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

variant_index_for_position() {
  local repeat_number=$1 position_number=$2
  if [[ ${mode} == all ]]; then
    # Rotating three variants also yields the required profiling-pair order:
    # repeat 1 B/P, repeat 2 P/B, repeat 3 B/P.
    printf '%d\n' "$(((position_number + (repeat_number - 1) * 2) % variant_count))"
  elif [[ ${mode} == profiling ]]; then
    printf '%d\n' "$(((position_number + repeat_number - 1) % variant_count))"
  else
    printf '0\n'
  fi
}

echo "[PLAN] Prefix_dist RAC validation: mode=${mode} repeats=${repeats} runs=$((repeats * variant_count)) threads=${threads}"
echo "[PLAN] reads=${reads} num=${num} seed=${base_seed} template=${template} keep_db=${keep_db}"
echo "[PLAN] db_bench=${db_bench}"
echo "[PLAN] db_root=${db_root}"
echo "[PLAN] resources=CPU-time,CPU-percent,average-RSS,peak-RSS interval=0.5s scope=whole-db_bench-process"
for ((repeat=1; repeat<=repeats; ++repeat)); do
  seed=$((base_seed + repeat - 1))
  printf '[PLAN] repeat=%d seed=%d order=' "$repeat" "$seed"
  for ((position=0; position<variant_count; ++position)); do
    index=$(variant_index_for_position "$repeat" "$position")
    printf '%s%s' "$([[ ${position} == 0 ]] || echo ',')" "${variant_names[index]}"
  done
  printf '\n'
  if ((dry_run)); then
    for ((position=0; position<variant_count; ++position)); do
      index=$(variant_index_for_position "$repeat" "$position")
      run_id=$(printf '%s-r%02d-o%02d-%s' "${stamp}" "$repeat" "$((position + 1))" "${variant_slugs[index]}")
      build_command "${db_root}/${run_id}" "${result_root}/raw/${run_id}/jobs.csv" "$seed" \
        "${variant_rac[index]}" "${variant_dp[index]}" "${variant_profiling[index]}"
      printf '[DRY-RUN] '
      printf '%q ' "${command[@]}"
      printf '\n'
    done
  fi
done
((dry_run)) && exit 0

ulimit -n 1048576 2>/dev/null || true
template=$(realpath -- "${template}")
result_root=$(realpath -m -- "${result_root}")
db_root=$(realpath -m -- "${db_root}")
for csv_path in "$template" "$result_root" "$db_root"; do
  [[ ${csv_path} != *','* && ${csv_path} != *$'\n'* ]] || {
    echo "[ERROR] Template/result/DB paths cannot contain commas or newlines: ${csv_path}" >&2
    exit 2
  }
done
[[ -d ${template} && -f ${template}/CURRENT && -f ${template}/LOCK ]] || {
  echo "[ERROR] Template is not a RocksDB DB with CURRENT and LOCK: ${template}" >&2
  exit 1
}
[[ -x ${db_bench} ]] || {
  echo "[ERROR] db_bench is not executable: ${db_bench}" >&2
  exit 1
}
[[ ! -e ${result_root} && ! -e ${db_root} ]] || {
  echo "[ERROR] Result or DB-root already exists: ${result_root} / ${db_root}" >&2
  exit 1
}

python3 - "$template" "$result_root" "$db_root" <<'PY'
from pathlib import Path
import sys
template, result, db_root = (Path(value).resolve() for value in sys.argv[1:])
def nested(left, right):
    return left == right or left in right.parents or right in left.parents
if nested(template, result) or nested(template, db_root) or nested(result, db_root):
    raise SystemExit(f"[ERROR] Unsafe path nesting: {template}, {result}, {db_root}")
PY

help_text=$("${db_bench}" --help 2>&1 || true)
for marker in rac2_ablation_log rac3_ratio_score_v1 rac3_no_source_guard_v1 \
              overall_request_p99_v1 rac2_profiling_only_v1; do
  [[ ${help_text} == *"${marker}"* ]] || {
    echo "[ERROR] ${db_bench} lacks required marker/flag ${marker}; build current source first" >&2
    exit 1
  }
done
if [[ ${mode} != profiling ]]; then
  [[ ${help_text} == *"rac2_execution_validation_v1"* ]] || {
    echo "[ERROR] ${db_bench} lacks execution-time validation telemetry; build current source first" >&2
    exit 1
  }
fi

assert_template_idle() {
  # A shared read lock is sufficient to conflict with RocksDB's exclusive DB
  # lock and works when the source template is intentionally read-only.
  python3 - "$template" <<'PY'
from pathlib import Path
import fcntl
import os
import sys
lock = Path(sys.argv[1]) / "LOCK"
fd = os.open(lock, os.O_RDONLY)
try:
    fcntl.lockf(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
    fcntl.lockf(fd, fcntl.LOCK_UN)
except BlockingIOError:
    raise SystemExit(f"[ERROR] Template appears live (LOCK is held): {lock}")
finally:
    os.close(fd)
PY
}

clone_template() {
  local run_db=$1 raw_dir=$2
  python3 - "$template" "$run_db" <<'PY'
from pathlib import Path
import fcntl
import os
import subprocess
import sys
source, target = map(Path, sys.argv[1:])
fd = os.open(source / "LOCK", os.O_RDONLY)
try:
    fcntl.lockf(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
except BlockingIOError:
    os.close(fd)
    raise SystemExit(f"[ERROR] Template appears live: {source / 'LOCK'}")
try:
    subprocess.run(["cp", "-a", "--reflink=auto", "--", str(source), str(target)], check=True)
finally:
    fcntl.lockf(fd, fcntl.LOCK_UN)
    os.close(fd)
PY
  python3 - "$run_db" "$raw_dir" <<'PY'
from pathlib import Path
import shutil
import sys
database, raw = map(Path, sys.argv[1:])
telemetry = database / "rac-telemetry"
if telemetry.is_symlink() or telemetry.is_file():
    telemetry.unlink()
elif telemetry.is_dir():
    shutil.rmtree(telemetry)
for path in database.iterdir():
    if path.name == "LOG" or path.name.startswith("LOG.old."):
        if path.is_dir() and not path.is_symlink():
            raise SystemExit(f"[ERROR] Inherited RocksDB log path is a directory: {path}")
        path.unlink()
(raw / "rac-telemetry").mkdir()
telemetry.symlink_to(raw / "rac-telemetry", target_is_directory=True)
PY
}

copy_engine_logs() {
  local run_db=$1 raw_dir=$2
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
(raw / "engine-logs").mkdir(exist_ok=True)
with (raw / "LOG").open("wb") as combined:
    for path in segments:
        shutil.copy2(path, raw / "engine-logs" / path.name)
        with path.open("rb") as source:
            shutil.copyfileobj(source, combined)
        combined.write(b"\n")
PY
}

validate_run() {
  local variant=$1 jobs_csv=$2 db_bench_log=$3 engine_log=$4 expected_ops=$5
  python3 - "$variant" "$jobs_csv" "$db_bench_log" "$engine_log" "$expected_ops" <<'PY'
import csv
from pathlib import Path
import re
import sys
variant, jobs_name, output_name, engine_name, expected_ops = sys.argv[1:]
jobs = list(csv.DictReader(Path(jobs_name).open(newline="")))
workload = [row for row in jobs if row.get("event") == "workload_end"]
drain = [row for row in jobs if row.get("event") == "drain_end"]
if len(workload) != 1 or (workload[0].get("status") or "").lower() != "ok":
    raise SystemExit("missing successful workload_end")
if int(workload[0].get("formal_ops") or 0) != int(expected_ops):
    raise SystemExit("workload operation count does not match the requested run")
if len(drain) != 1 or (drain[0].get("status") or "").lower() != "ok":
    raise SystemExit("missing successful drain_end")
if int(drain[0].get("outstanding_jobs") or 0) != 0:
    raise SystemExit("drain_end has outstanding jobs")
output = Path(output_name).read_text(errors="replace")
def counter(label):
    match = re.search(rf"^{re.escape(label)}:\s*(\d+)\s*$", output, re.MULTILINE)
    return None if match is None else int(match.group(1))
if variant in ("Baseline", "Profiling-only"):
    for label in (
        "Read-triggered compactions",
        "RAC compactions",
        "RAC evaluated windows",
        "RAC triggered windows",
        "RAC non-triggered windows",
    ):
        value = counter(label)
        if value != 0:
            raise SystemExit(f"{variant} must report {label}=0, got {value}")
if variant == "Profiling-only":
    observed = counter("RAC profiling-only observed windows")
    if observed is None or observed <= 0:
        raise SystemExit(f"Profiling-only observed no sealed windows: {observed}")
    point_lookups = counter("RAC profiling-only point lookups")
    if point_lookups is None or point_lookups <= 0:
        raise SystemExit(f"Profiling-only observed no point lookups: {point_lookups}")
engine = Path(engine_name).read_text(errors="replace")
if variant == "Profiling-only" and "RAC2 profiling_window_v1" not in engine:
    raise SystemExit("Profiling-only run has no profiling window lifecycle records")
PY
}

safe_remove_run_db() {
  local run_db=$1
  local expected_name=$2
  python3 - "$db_root" "$run_db" "$expected_name" <<'PY'
from pathlib import Path
import shutil
import sys
root, target = (Path(value).resolve() for value in sys.argv[1:3])
expected_name = sys.argv[3]
if target.parent != root or target.name != expected_name or target.is_symlink():
    raise SystemExit(f"[ERROR] Refusing unsafe DB removal: {target}")
shutil.rmtree(target)
PY
}

assert_template_idle
mkdir -p -- "${result_root}/raw" "${db_root}"
printf '%s\n' "${BASH_EXECUTION_STRING}" > "${result_root}/runner.sh"
git -C "${repo_dir}" rev-parse HEAD > "${result_root}/git-head.txt"
git -C "${repo_dir}" status --short > "${result_root}/git-status.txt"
db_bench_source=${db_bench}
cp -- "${db_bench_source}" "${result_root}/db_bench.snapshot"
cp -- "${repo_dir}/trace-zhao/collect_rac_resources.py" "${result_root}/collect_rac_resources.py"
db_bench=${result_root}/db_bench.snapshot
printf '%s\n' "${help_text}" > "${result_root}/db_bench-help.txt"
formal_configuration=0
if ((smoke == 0 && reads == 50000000 && num == 200000000 &&
     threads == 1 && repeats >= 3)); then
  formal_configuration=1
fi

cat > "${result_root}/experiment.conf" <<CONF
workload=Prefix_dist
mode=${mode}
formal=${formal_configuration}
smoke_override=${smoke}
repeats=${repeats}
template=${template}
db_bench=${db_bench}
db_bench_source=${db_bench_source}
threads=${threads}
reads=${reads}
num=${num}
key_size=${key_size}
value_size=${value_size}
cache_size=0
bloom_bits=0
mix_get_ratio=1
mix_put_ratio=0.0
rac_design_option=3
rac_window_size=${window}
rac_top_k=${top_k}
rac2_hotspot_persistence=${hotspot_persistence}
rac2_prediction_horizon=${prediction_horizon}
rac2_cost_weight=${cost_weight}
rac2_compaction_bandwidth_mib_per_sec=${bandwidth_mib_s}
profiling_pair_order=repeat1_baseline_first_repeat2_profiling_first_alternating
host_page_cache=not_dropped
resource_scope=whole_process_open_workload_drain_close
resource_sample_interval_seconds=0.5
successful_db_clones_retained=${keep_db}
extra_args=${extra_args_text}
CONF

printf 'validation,repeat,order,variant,slug,run_id,seed,db_bench_status,runner_validation_status,raw_dir,db_dir,formal_ops,profiling_only,rac_enabled,dp_enabled,source_guard_model\n' > "${result_root}/manifest.csv"

for ((repeat=1; repeat<=repeats; ++repeat)); do
  seed=$((base_seed + repeat - 1))
  for ((position=0; position<variant_count; ++position)); do
    index=$(variant_index_for_position "$repeat" "$position")
    run_id=$(printf '%s-r%02d-o%02d-%s' "${stamp}" "$repeat" "$((position + 1))" "${variant_slugs[index]}")
    raw_dir="${result_root}/raw/${run_id}"
    run_db="${db_root}/${run_id}"
    mkdir -p -- "${raw_dir}"
    clone_template "${run_db}" "${raw_dir}"
    build_command "${run_db}" "${raw_dir}/jobs.csv" "$seed" \
      "${variant_rac[index]}" "${variant_dp[index]}" "${variant_profiling[index]}"
    printf '%q ' "${command[@]}" > "${raw_dir}/command.sh"
    printf '\n' >> "${raw_dir}/command.sh"
    cp -- "${raw_dir}/command.sh" "${raw_dir}/db_bench.log"
    echo "[RUN] repeat=${repeat} order=$((position + 1)) variant=${variant_names[index]} seed=${seed} run_id=${run_id}"
    status=0
    python3 "${result_root}/collect_rac_resources.py" --output-dir "${raw_dir}" \
      -- "${command[@]}" >> "${raw_dir}/db_bench.log" 2>&1 || status=$?
    copy_engine_logs "${run_db}" "${raw_dir}"
    validation=$([[ ${variant_names[index]} == Execution-Time ]] && echo execution_time || echo profiling_overhead)
    source_guard=$([[ ${variant_rac[index]} == 1 ]] && echo none || echo disabled)
    runner_validation_status=not_run
    if ((status == 0)); then
      if [[ ! -s ${raw_dir}/jobs.csv ]]; then
        runner_validation_status=missing_jobs_csv
      elif validate_run "${variant_names[index]}" "${raw_dir}/jobs.csv" "${raw_dir}/db_bench.log" "${raw_dir}/LOG" "$((reads * threads))"; then
        runner_validation_status=ok
      else
        runner_validation_status=failed
      fi
    fi
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$validation" "$repeat" "$((position + 1))" "${variant_names[index]}" "${variant_slugs[index]}" \
      "$run_id" "$seed" "$status" "$runner_validation_status" "$raw_dir" "$run_db" "$((reads * threads))" \
      "${variant_profiling[index]}" "${variant_rac[index]}" "${variant_dp[index]}" "$source_guard" \
      >> "${result_root}/manifest.csv"
    if ((status != 0)); then
      echo "[ERROR] ${variant_names[index]} failed with status ${status}; DB retained: ${run_db}" >&2
      tail -n 30 "${raw_dir}/db_bench.log" >&2
      exit "$status"
    fi
    if [[ ${runner_validation_status} != ok ]]; then
      echo "[ERROR] ${variant_names[index]} runner validation=${runner_validation_status}; DB retained: ${run_db}" >&2
      exit 1
    fi
    if ((keep_db == 0)); then
      safe_remove_run_db "${run_db}" "${run_id}"
    fi
  done
done

assert_template_idle
echo "[OK] Raw validation data: ${result_root}"
python3 "${repo_dir}/trace-zhao/analyze_rac_validation.py" "${result_root}"
echo "[OK] CPU/memory CSVs and profiling_cpu_memory.{png,pdf,svg}: ${result_root}/analysis"
