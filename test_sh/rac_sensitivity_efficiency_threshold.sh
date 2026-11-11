#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

# RAC sensitivity: sweep the benefit-efficiency admission threshold.
# All other settings are inherited from rtc_motivation_ratio.sh, including
# the read ratio and high-miss bypass. Read-only windows can still use the
# effective high-miss threshold of 0.95 even when configured as 1.

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
trace_dir=$(cd -- "${script_dir}/.." && pwd)
base_script="${script_dir}/rtc_motivation_ratio.sh"

threshold_values=${RAC_SENSITIVITY_EFFICIENCY_THRESHOLD_VALUES-"0.1 0.3 1 3 10 30 100"}
timestamp=$(date +%Y%m%d-%H%M%S)
result_root=${RAC_SENSITIVITY_RESULT_ROOT:-"${trace_dir}/rac-sensitivity/${timestamp}-efficiency-threshold"}
dry_run=0

usage() {
  echo "Usage: $0 [--dry-run]"
  echo 'Override cases with RAC_SENSITIVITY_EFFICIENCY_THRESHOLD_VALUES="0.1 0.3 1 3 10 30 100".'
  echo 'Other settings follow rtc_motivation_ratio.sh; use RTC_MOTIVATION_READ_RATIOS=95 for a 95% read mix.'
}

while (($#)); do
  case $1 in
    --dry-run) dry_run=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "[ERROR] Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -x ${base_script} ]] || { echo "[ERROR] Missing executable: ${base_script}" >&2; exit 1; }

read -r -a cases <<< "${threshold_values}"
((${#cases[@]} > 0)) || { echo "[ERROR] RAC_SENSITIVITY_EFFICIENCY_THRESHOLD_VALUES is empty" >&2; exit 2; }

# Validate the entire sweep before starting any expensive experiment.
for threshold in "${cases[@]}"; do
  [[ ${threshold} =~ ^[0-9]+([.][0-9]+)?$ &&
     ! ${threshold} =~ ^0+([.]0+)?$ ]] || {
    echo "[ERROR] Efficiency threshold must be a positive decimal: ${threshold}" >&2
    exit 2
  }
done

echo "[INFO] suite=RAC-efficiency-threshold values=${threshold_values} result_root=${result_root}"
for threshold in "${cases[@]}"; do
  threshold_tag=${threshold//./p}
  cmd=(env
    "RTC_MOTIVATION_RESULT_DIR=${result_root}/eta${threshold_tag}"
    "RTC_MOTIVATION_MODES=rac"
    "RTC_MOTIVATION_RAC_BENEFIT_EFFICIENCY_THRESHOLD=${threshold}"
    "${base_script}" A)

  printf '[PLAN] efficiency_threshold=%s ' "${threshold}"
  printf '%q ' "${cmd[@]}"
  printf '\n'
  ((dry_run)) || "${cmd[@]}"
done
