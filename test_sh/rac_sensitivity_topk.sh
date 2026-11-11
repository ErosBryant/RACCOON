#!/usr/bin/env bash

# RAC sensitivity on All_dist: sweep top-k while holding every other setting
# at rac2_ablation_all_dist.sh defaults. Each case runs Full only (DP enabled).

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
trace_dir=$(cd -- "${script_dir}/.." && pwd)
base_script="${script_dir}/rac2_ablation_all_dist.sh"

topk_values=${RAC_SENSITIVITY_TOPK_VALUES:-"2 4 6 8"}
timestamp=$(date +%Y%m%d-%H%M%S)
result_root=${RAC_SENSITIVITY_RESULT_ROOT:-"${trace_dir}/rac-sensitivity/${timestamp}-all-dist-topk"}
dry_run=0

usage() {
  echo "Usage: $0 [--dry-run]"
  echo "Run the Full RAC policy on All_dist while sweeping RAC top-k."
  echo "Override cases with RAC_SENSITIVITY_TOPK_VALUES=\"2 4 6 8\"."
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

read -r -a cases <<< "${topk_values}"
((${#cases[@]} > 0)) || { echo "[ERROR] RAC_SENSITIVITY_TOPK_VALUES is empty" >&2; exit 2; }

declare -A seen_cases=()
for topk in "${cases[@]}"; do
  [[ ${topk} =~ ^[1-9][0-9]*$ ]] || {
    echo "[ERROR] top-k must be a positive integer: ${topk}" >&2
    exit 2
  }
  [[ -z ${seen_cases[${topk}]+x} ]] || {
    echo "[ERROR] duplicate top-k case would reuse an output directory: ${topk}" >&2
    exit 2
  }
  seen_cases[${topk}]=1
done

echo "[INFO] suite=RAC-All_dist-top-k policy=Full values=${topk_values} result_root=${result_root}"
for topk in "${cases[@]}"; do
  cmd=(env
    "RAC2_ABLATION_FULL_ONLY=1"
    "RAC2_ABLATION_RESULT_ROOT=${result_root}/topk${topk}"
    "RAC2_ABLATION_TOP_K=${topk}")
  if [[ -n ${RAC2_ABLATION_DB_ROOT:-} ]]; then
    cmd+=("RAC2_ABLATION_DB_ROOT=${RAC2_ABLATION_DB_ROOT}/topk${topk}")
  fi
  cmd+=(bash "${base_script}")
  ((dry_run)) && cmd+=(--dry-run)

  printf '[PLAN] top_k=%s ' "${topk}"
  printf '%q ' "${cmd[@]}"
  printf '\n'
  "${cmd[@]}"
done
