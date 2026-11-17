#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

set -euo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd -- "${script_dir}/.." && pwd)

if [[ -n ${OUT_DIR:-} ]]; then
  # Refuse reuse so the summary never mixes separate experiments.
  mkdir -p -- "$(dirname -- "${OUT_DIR}")"
  mkdir -- "${OUT_DIR}"
else
  mkdir -p -- "${repo_dir}/benchmark-results"
  OUT_DIR=$(mktemp -d "${repo_dir}/benchmark-results/run.XXXXXX")
fi
DB_ROOT=${DB_ROOT:-${OUT_DIR}/db}
export OUT_DIR DB_ROOT
printf 'Results: %s\nDatabases: %s\n' "${OUT_DIR}" "${DB_ROOT}"

for mode in baseline rtc rac; do
  printf '\nRunning %s\n' "${mode}"
  MODE=${mode} bash "${script_dir}/fillrandom.sh"
  MODE=${mode} bash "${script_dir}/readrandom.sh"
done

{
  for mode in baseline rtc rac; do
    printf '%s\n' "${mode}"
    grep -hE '^(fillrandom|readrandom)[[:space:]]*:' \
      "${OUT_DIR}/${mode}_"*.log
  done
} | tee "${OUT_DIR}/summary.txt"
printf '\nCompleted. Results: %s\n' "${OUT_DIR}"
