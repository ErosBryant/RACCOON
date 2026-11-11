#!/usr/bin/env bash

# RAC sensitivity: compare dual-purpose (DP) selection disabled/enabled.
# All non-DP settings are inherited from rtc_motivation_ratio.sh.

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
trace_dir=$(cd -- "${script_dir}/.." && pwd)
base_script="${script_dir}/rtc_motivation_ratio.sh"

dp_values=${RAC_SENSITIVITY_DP_VALUES:-"0 1"}
timestamp=$(date +%Y%m%d-%H%M%S)
result_root=${RAC_SENSITIVITY_RESULT_ROOT:-"${trace_dir}/rac-sensitivity/${timestamp}-dp"}
dry_run=0

usage() {
  echo "Usage: $0 [--dry-run]"
  echo "Override cases with RAC_SENSITIVITY_DP_VALUES=\"0 1\"."
}

while (($#)); do
  case $1 in
    --dry-run) dry_run=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "[ERROR] Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -x ${base_script} ]] || { echo "[ERROR] Missing executable: ${base_script}" >&2; exit 1; }

read -r -a cases <<< "${dp_values}"
((${#cases[@]} > 0)) || { echo "[ERROR] RAC_SENSITIVITY_DP_VALUES is empty" >&2; exit 2; }

echo "[INFO] suite=RAC-DP values=${dp_values} result_root=${result_root}"
for dp in "${cases[@]}"; do
  [[ ${dp} == 0 || ${dp} == 1 ]] || {
    echo "[ERROR] DP value must be 0 or 1: ${dp}" >&2
    exit 2
  }

  cmd=(env
    "RTC_MOTIVATION_RESULT_DIR=${result_root}/dp${dp}"
    "RTC_MOTIVATION_MODES=rac"
    "RTC_MOTIVATION_RAC_DUAL_PURPOSE_SELECTION=${dp}"
    "${base_script}" A)

  printf '[PLAN] dp=%s ' "${dp}"
  printf '%q ' "${cmd[@]}"
  printf '\n'
  ((dry_run)) || "${cmd[@]}"
done
