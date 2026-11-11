#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
#
# Validate RAC's predicted read benefit and rewrite volume using a stable,
# read-only workload.
#
# Experiment modes:
#
#   collection:
#     eta_min=0 by default. Executes more RAC compactions to collect
#     prediction-vs-measurement samples. This validates the estimators,
#     not the production admission threshold.
#
#   policy:
#     Uses the production eta_min. This validates RAC admission decisions.
#
# Examples:
#
#   ./run_rac_model_accuracy.sh all
#
#   RAC_MODEL_EXPERIMENT_MODE=policy \
#   RAC_MODEL_ETA_MIN=10 \
#   ./run_rac_model_accuracy.sh all
#
#   RAC_MODEL_SEEDS="1000,2000,3000" \
#   RAC_MODEL_UNIFORM_VALUES="0,1" \
#   ./run_rac_model_accuracy.sh all
#
#   RAC_MODEL_RESULT_DIR=/path/to/existing/result \
#   ./run_rac_model_accuracy.sh plot

set -Eeuo pipefail
IFS=$'\n\t'

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd -- "${script_dir}/../.." && pwd)

python_bin=${PYTHON:-python3}
action=${1:-all}

experiment_mode=${RAC_MODEL_EXPERIMENT_MODE:-collection}
benefit_window_seconds=${RAC_MODEL_BENEFIT_WINDOW_SECONDS:-3}
fail_on_bypass=${RAC_MODEL_FAIL_ON_BYPASS:-1}

result_root=${RAC_MODEL_RESULT_DIR:-}
if [[ -z ${result_root} ]]; then
  result_root="${repo_dir}/trace-zhao/motivation/$(date +%Y%m%d-%H%M%S)-rac-model-accuracy"
fi

runner="${repo_dir}/scripts/run_rtc_motivation.sh"
plotter="${repo_dir}/trace-zhao/motivation/graph/plot_rac_model_accuracy.py"

info() {
  printf '[INFO] %s\n' "$*"
}

warn() {
  printf '[WARN] %s\n' "$*" >&2
}

die() {
  printf '[ERROR] %s\n' "$*" >&2
  exit 1
}

on_error() {
  local exit_code=$?
  local line_number=${1:-unknown}

  printf '[ERROR] RAC model experiment failed at line %s (exit=%s)\n' \
    "${line_number}" "${exit_code}" >&2

  exit "${exit_code}"
}

trap 'on_error "${LINENO}"' ERR

require_command() {
  local command_name=$1

  if ! command -v "${command_name}" >/dev/null 2>&1; then
    die "Required command was not found: ${command_name}"
  fi
}

validate_configuration() {
  case ${experiment_mode} in
    collection | policy)
      ;;
    *)
      die "RAC_MODEL_EXPERIMENT_MODE must be collection or policy"
      ;;
  esac

  [[ ${benefit_window_seconds} =~ ^[0-9]+([.][0-9]+)?$ ]] ||
    die "RAC_MODEL_BENEFIT_WINDOW_SECONDS must be numeric"

  [[ -x ${runner} ]] ||
    die "Experiment runner is missing or not executable: ${runner}"

  [[ -f ${plotter} ]] ||
    die "Plotting script does not exist: ${plotter}"

  require_command "${python_bin}"
  require_command grep
  require_command git
  require_command uname
  require_command df

  local run_db_root
  run_db_root=${RTC_MOTIVATION_RUN_DB_ROOT:-/mnt/nvme/rtc_motivation_runs}

  if [[ ! -d ${run_db_root} ]]; then
    warn "Run DB root does not exist yet: ${run_db_root}"
  elif [[ ! -w ${run_db_root} ]]; then
    warn "Run DB root may not be writable: ${run_db_root}"
  fi

  if [[ ${EUID} -ne 0 && ! -w /proc/sys/vm/drop_caches ]]; then
    warn "The current user cannot directly drop the OS page cache."
    warn "Confirm that run_rtc_motivation.sh handles this through sudo."
  fi
}

resolve_eta_min() {
  if [[ -n ${RAC_MODEL_ETA_MIN:-} ]]; then
    printf '%s\n' "${RAC_MODEL_ETA_MIN}"
    return
  fi

  case ${experiment_mode} in
    collection)
      # Collect a wider range of executed RAC compactions.
      printf '0.0\n'
      ;;
    policy)
      # Change this default if the final RAC configuration uses another value.
      printf '10.0\n'
      ;;
  esac
}

write_manifest() {
  local run_dir=$1
  local seed=$2
  local uniform=$3
  local eta_min=$4
  local extra_args=$5

  local git_sha
  local git_branch
  local disk_info

  git_sha=$(git -C "${repo_dir}" rev-parse HEAD 2>/dev/null || printf 'unknown')
  git_branch=$(git -C "${repo_dir}" branch --show-current 2>/dev/null ||
    printf 'unknown')
  disk_info=$(df -Pk "${RTC_MOTIVATION_RUN_DB_ROOT}" 2>/dev/null |
    tail -n 1 || true)

  {
    printf 'experiment=RAC model accuracy\n'
    printf 'experiment_mode=%s\n' "${experiment_mode}"
    printf 'timestamp_utc=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf 'git_sha=%s\n' "${git_sha}"
    printf 'git_branch=%s\n' "${git_branch}"
    printf 'hostname=%s\n' "$(hostname 2>/dev/null || printf 'unknown')"
    printf 'kernel=%s\n' "$(uname -srvmo)"
    printf 'disk_info=%s\n' "${disk_info}"

    printf '\n[workload]\n'
    printf 'entries=%s\n' "${RTC_MOTIVATION_ENTRIES}"
    printf 'operations=%s\n' "${RTC_MOTIVATION_OPERATIONS}"
    printf 'key_size=%s\n' "${RTC_MOTIVATION_KEY_SIZE}"
    printf 'value_size=%s\n' "${RTC_MOTIVATION_VALUE_SIZE}"
    printf 'read_ratio=%s\n' "${RTC_MOTIVATION_READ_RATIOS}"
    printf 'uniform=%s\n' "${uniform}"
    printf 'seed=%s\n' "${seed}"
    printf 'threads=%s\n' "${RTC_MOTIVATION_THREADS}"
    printf 'compression=%s\n' "${RTC_MOTIVATION_COMPRESSION}"

    printf '\n[RAC]\n'
    printf 'top_k=%s\n' "${RTC_MOTIVATION_RAC_TOP_K}"
    printf 'window_size=%s\n' "${RTC_MOTIVATION_RAC_WINDOW_SIZE}"
    printf 'eta_min=%s\n' "${eta_min}"
    printf 'high_miss_ratio_threshold=%s\n' \
      "${RTC_MOTIVATION_RAC_HIGH_MISS_RATIO_THRESHOLD}"
    printf 'dual_purpose_selection=%s\n' \
      "${RTC_MOTIVATION_RAC_DUAL_PURPOSE_SELECTION}"
    printf 'bottom_overlap_threshold=%s\n' \
      "${RTC_MOTIVATION_RAC_BOTTOM_OVERLAP_THRESHOLD}"
    printf 'cooldown_windows=%s\n' \
      "${RTC_MOTIVATION_RAC_COOLDOWN_WINDOWS}"
    printf 'bottom_priority_threads=%s\n' \
      "${RTC_MOTIVATION_RAC_NUM_BOTTOM_PRI_THREADS}"

    printf '\n[system]\n'
    printf 'max_background_jobs=%s\n' \
      "${RTC_MOTIVATION_MAX_BACKGROUND_JOBS}"
    printf 'drop_caches=%s\n' "${RTC_MOTIVATION_DROP_CACHES}"
    printf 'cleanup_run_db=%s\n' "${RTC_MOTIVATION_CLEANUP_RUN_DB}"
    printf 'run_db_root=%s\n' "${RTC_MOTIVATION_RUN_DB_ROOT}"
    printf 'reuse_rtc_db=%s\n' "${RTC_MOTIVATION_REUSE_RTC_DB}"
    printf 'extra_db_bench_args=%s\n' "${extra_args}"

    printf '\n[measurement]\n'
    printf 'benefit_window_seconds=%s\n' "${benefit_window_seconds}"
    printf 'fail_on_high_miss_bypass=%s\n' "${fail_on_bypass}"
  } >"${run_dir}/experiment_manifest.txt"
}

verify_run() {
  local run_dir=$1
  local verification_failed=0

  [[ -d ${run_dir} ]] ||
    die "Result directory does not exist: ${run_dir}"

  info "Verifying experiment: ${run_dir}"

  # high_miss_triggered=1 means the candidate bypassed the normal
  # cost-benefit gate.
  if grep -RIsEq \
    'high_miss_triggered[=:][[:space:]]*(1|true)' \
    "${run_dir}"; then
    warn "At least one RAC compaction used the high-miss bypass."

    if [[ ${fail_on_bypass} == 1 ]]; then
      verification_failed=1
    fi
  elif grep -RIsEq \
    'high_miss_triggered[=:][[:space:]]*(0|false)' \
    "${run_dir}"; then
    info "Verified: high_miss_triggered=0"
  else
    warn "No high_miss_triggered field was found in the result logs."
    warn "Confirm that RAC emitted at least one admission decision."
  fi

  # Check whether dual-purpose selection was unexpectedly enabled.
  if grep -RIsEq \
    'dual[_ -]?purpose[^[:cntrl:]]*(triggered|selected|selection)[=:][[:space:]]*(1|true)' \
    "${run_dir}"; then
    warn "Dual-purpose RAC selection appears to have been enabled."
    verification_failed=1
  else
    info "No enabled dual-purpose selection was detected."
  fi

  # These checks require run_rtc_motivation.sh to print the final db_bench
  # command. Missing text therefore produces a warning rather than failure.
  local required_flag
  for required_flag in \
    '--cache_size=0' \
    '--use_direct_reads=true' \
    '--use_direct_io_for_flush_and_compaction=true'; do
    if grep -RIsFq -- "${required_flag}" "${run_dir}"; then
      info "Observed db_bench flag: ${required_flag}"
    else
      warn "Could not verify final db_bench flag: ${required_flag}"
    fi
  done

  # Confirm that some RAC decision information exists.
  if ! grep -RIsEq \
    '(RAC|rac).*(candidate|admission|compaction|trigger)' \
    "${run_dir}"; then
    warn "No RAC decision/compaction records were detected."
  fi

  if [[ ${verification_failed} -ne 0 ]]; then
    die "Experiment isolation checks failed: ${run_dir}"
  fi

  info "Verification completed: ${run_dir}"
}

run_one_experiment() {
  local seed=$1
  local uniform=$2
  local eta_min=$3

  local workload_name
  case ${uniform} in
    1)
      workload_name=uniform
      ;;
    0)
      workload_name=nonuniform
      ;;
    *)
      workload_name="uniform-${uniform}"
      ;;
  esac

  local run_name
  run_name="${experiment_mode}-${workload_name}-seed${seed}"

  local run_dir
  run_dir="${result_root}/${run_name}"

  if [[ -e ${run_dir} ]]; then
    die "Run directory already exists: ${run_dir}"
  fi

  mkdir -p "${run_dir}"

  export RTC_MOTIVATION_RESULT_DIR="${run_dir}"
  export RTC_MOTIVATION_RESULTS_ROOT="${result_root}"

  export RTC_MOTIVATION_RUN_DB_ROOT
  RTC_MOTIVATION_RUN_DB_ROOT=${
    RTC_MOTIVATION_RUN_DB_ROOT:-/mnt/nvme/rtc_motivation_runs
  }

  export RTC_MOTIVATION_ENTRIES=${RAC_MODEL_ENTRIES:-200000000}
  export RTC_MOTIVATION_OPERATIONS=${RAC_MODEL_OPERATIONS:-64000000}
  export RTC_MOTIVATION_KEY_SIZE=${RAC_MODEL_KEY_SIZE:-16}
  export RTC_MOTIVATION_VALUE_SIZE=${RAC_MODEL_VALUE_SIZE:-500}

  # Single foreground thread reduces concurrency noise.
  export RTC_MOTIVATION_THREADS=1

  # Keep the existing RocksDB background configuration. The experiment
  # parser should reject overlapping RAC compactions when calculating
  # per-job benefit.
  export RTC_MOTIVATION_MAX_BACKGROUND_JOBS=${
    RAC_MODEL_MAX_BACKGROUND_JOBS:-2
  }

  export RTC_MOTIVATION_COMPRESSION=${RAC_MODEL_COMPRESSION:-snappy}
  export RTC_MOTIVATION_UNIFORM="${uniform}"
  export RTC_MOTIVATION_SEED="${seed}"

  # Isolate read-triggered behavior.
  export RTC_MOTIVATION_READ_RATIOS=100
  export RTC_MOTIVATION_MODES=rac

  export RTC_MOTIVATION_RAC_TOP_K=${RAC_MODEL_TOP_K:-8}
  export RTC_MOTIVATION_RAC_DUAL_PURPOSE_SELECTION=0
  export RTC_MOTIVATION_RAC_WINDOW_SIZE=${RAC_MODEL_WINDOW_SIZE:-100000}
  export RTC_MOTIVATION_RAC_BENEFIT_EFFICIENCY_THRESHOLD="${eta_min}"

  # This disables the high-miss bypass only if the implementation uses:
  #
  #   miss_ratio > high_miss_threshold
  #
  # The post-run verification detects any actual bypass.
  export RTC_MOTIVATION_RAC_HIGH_MISS_RATIO_THRESHOLD=1

  export RTC_MOTIVATION_RAC_BOTTOM_OVERLAP_THRESHOLD=${
    RAC_MODEL_BOTTOM_OVERLAP:-8
  }

  export RTC_MOTIVATION_RAC_COOLDOWN_WINDOWS=${
    RAC_MODEL_COOLDOWN_WINDOWS:-4
  }

  export RTC_MOTIVATION_RAC_NUM_BOTTOM_PRI_THREADS=1

  # Disable block-cache and OS page-cache effects.
  export RTC_MOTIVATION_DROP_CACHES=1

  # Cleanup defaults to enabled because a 200M-entry database consumes
  # substantial storage. Set RAC_MODEL_CLEANUP_RUN_DB=0 for debugging.
  export RTC_MOTIVATION_CLEANUP_RUN_DB=${
    RAC_MODEL_CLEANUP_RUN_DB:-1
  }

  # A prepared stable DB may be supplied explicitly for repeated runs.
  export RTC_MOTIVATION_REUSE_RTC_DB=${
    RAC_MODEL_REUSE_RTC_DB:-
  }

  # Put mandatory isolation flags last so optional user arguments cannot
  # accidentally override them.
  local mandatory_args
  mandatory_args='--cache_size=0 --use_direct_reads=true --use_direct_io_for_flush_and_compaction=true'

  local user_extra_args
  user_extra_args=${RAC_MODEL_EXTRA_ARGS:-}

  local final_extra_args
  if [[ -n ${user_extra_args} ]]; then
    final_extra_args="${user_extra_args} ${mandatory_args}"
  else
    final_extra_args="${mandatory_args}"
  fi

  export RTC_MOTIVATION_EXTRA_DB_BENCH_ARGS="${final_extra_args}"

  write_manifest \
    "${run_dir}" \
    "${seed}" \
    "${uniform}" \
    "${eta_min}" \
    "${final_extra_args}"

  info "Starting RAC model-accuracy experiment"
  info "Mode:      ${experiment_mode}"
  info "Seed:      ${seed}"
  info "Uniform:   ${uniform}"
  info "eta_min:   ${eta_min}"
  info "Result:    ${run_dir}"

  # Preserve both stdout and stderr.
  "${runner}" A 2>&1 | tee "${run_dir}/launcher.log"

  verify_run "${run_dir}"

  printf '%s\n' "${run_dir}" >>"${result_root}/runs.list"

  info "Experiment completed: ${run_dir}"
}

run_experiments() {
  mkdir -p "${result_root}"

  # Prevent plot/verify from reading stale entries from an earlier invocation.
  : >"${result_root}/runs.list"

  local eta_min
  eta_min=$(resolve_eta_min)

  local seed_spec
  local uniform_spec

  seed_spec=${RAC_MODEL_SEEDS:-${RAC_MODEL_SEED:-1000}}
  uniform_spec=${RAC_MODEL_UNIFORM_VALUES:-${RAC_MODEL_UNIFORM:-0}}

  local -a seeds
  local -a uniform_values

  IFS=' ,' read -r -a seeds <<<"${seed_spec}"
  IFS=' ,' read -r -a uniform_values <<<"${uniform_spec}"

  [[ ${#seeds[@]} -gt 0 ]] ||
    die "No experiment seeds were provided"

  [[ ${#uniform_values[@]} -gt 0 ]] ||
    die "No uniform settings were provided"

  local seed
  local uniform

  for uniform in "${uniform_values[@]}"; do
    for seed in "${seeds[@]}"; do
      run_one_experiment "${seed}" "${uniform}" "${eta_min}"
    done
  done
}

for_each_result() {
  local callback=$1

  if [[ -s ${result_root}/runs.list ]]; then
    local run_dir

    while IFS= read -r run_dir; do
      [[ -n ${run_dir} ]] || continue
      "${callback}" "${run_dir}"
    done <"${result_root}/runs.list"

    return
  fi

  # Compatibility with an old single-run result directory.
  "${callback}" "${result_root}"
}

plot_one_result() {
  local run_dir=$1

  [[ -d ${run_dir} ]] ||
    die "Result directory does not exist: ${run_dir}"

  info "Plotting RAC model accuracy: ${run_dir}"

  "${python_bin}" \
    "${plotter}" \
    --result-dir "${run_dir}" \
    --benefit-window-seconds "${benefit_window_seconds}"
}

plot_results() {
  for_each_result plot_one_result
}

verify_results() {
  for_each_result verify_run
}

usage() {
  cat >&2 <<EOF
Usage:
  $0 run
  $0 verify
  $0 plot
  $0 all

Important environment variables:

  RAC_MODEL_EXPERIMENT_MODE=collection|policy
  RAC_MODEL_RESULT_DIR=/path/to/result
  RAC_MODEL_SEEDS=1000,2000,3000
  RAC_MODEL_UNIFORM_VALUES=0,1
  RAC_MODEL_ETA_MIN=10
  RAC_MODEL_ENTRIES=200000000
  RAC_MODEL_OPERATIONS=64000000
  RAC_MODEL_WINDOW_SIZE=100000
  RAC_MODEL_BENEFIT_WINDOW_SECONDS=3
  RAC_MODEL_COOLDOWN_WINDOWS=4
  RAC_MODEL_CLEANUP_RUN_DB=0|1
  RAC_MODEL_REUSE_RTC_DB=/path/to/stable/db
  RAC_MODEL_FAIL_ON_BYPASS=0|1
EOF
}

validate_configuration

case ${action} in
  run)
    run_experiments
    ;;
  verify)
    verify_results
    ;;
  plot)
    plot_results
    ;;
  all)
    run_experiments
    plot_results
    ;;
  *)
    usage
    exit 2
    ;;
esac