#!/usr/bin/env bash

# RAC sensitivity on All_dist: sweep request-count window size while holding
# every other setting at rac2_ablation_all_dist.sh defaults. Each case runs
# Full only (DP enabled).

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
trace_dir=$(cd -- "${script_dir}/.." && pwd)
base_script="${script_dir}/rac2_ablation_all_dist.sh"

window_values=${RAC_SENSITIVITY_WINDOW_VALUES:-"10000 50000 100000 200000"}
timestamp=$(date +%Y%m%d-%H%M%S)
result_root=${RAC_SENSITIVITY_RESULT_ROOT:-"${trace_dir}/rac-sensitivity/${timestamp}-all-dist-window"}
dry_run=0

usage() {
  echo "Usage: $0 [--dry-run]"
  echo "Run the Full RAC policy on All_dist while sweeping RAC window size."
  echo "Override cases with RAC_SENSITIVITY_WINDOW_VALUES=\"10000 50000 100000 200000\"."
  echo "Override the suite output with RAC_SENSITIVITY_RESULT_ROOT=/path."
}

while (($#)); do
  case $1 in
    --dry-run) dry_run=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "[ERROR] Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -f ${base_script} ]] || { echo "[ERROR] Missing base script: ${base_script}" >&2; exit 1; }

read -r -a cases <<< "${window_values}"
((${#cases[@]} > 0)) || { echo "[ERROR] RAC_SENSITIVITY_WINDOW_VALUES is empty" >&2; exit 2; }

declare -A seen_cases=()
for window_size in "${cases[@]}"; do
  [[ ${window_size} =~ ^[1-9][0-9]*$ ]] || {
    echo "[ERROR] window size must be a positive integer: ${window_size}" >&2
    exit 2
  }
  [[ -z ${seen_cases[${window_size}]+x} ]] || {
    echo "[ERROR] duplicate window-size case would reuse an output directory: ${window_size}" >&2
    exit 2
  }
  seen_cases[${window_size}]=1
done

echo "[INFO] suite=RAC-All_dist-window-size policy=Full values=${window_values} result_root=${result_root}"
for window_size in "${cases[@]}"; do
  cmd=(env
    "RAC2_ABLATION_FULL_ONLY=1"
    "RAC2_ABLATION_RESULT_ROOT=${result_root}/window${window_size}"
    "RAC2_ABLATION_WINDOW=${window_size}")
  if [[ -n ${RAC2_ABLATION_DB_ROOT:-} ]]; then
    cmd+=("RAC2_ABLATION_DB_ROOT=${RAC2_ABLATION_DB_ROOT}/window${window_size}")
  fi
  cmd+=(bash "${base_script}")
  ((dry_run)) && cmd+=(--dry-run)

  printf '[PLAN] window_size=%s ' "${window_size}"
  printf '%q ' "${cmd[@]}"
  printf '\n'
  "${cmd[@]}"
done
