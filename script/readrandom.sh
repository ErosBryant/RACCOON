#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

set -euo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

if [[ ! -f ${db_path}/CURRENT ]]; then
  echo "Error: database not found: ${db_path}" >&2
  echo 'Run fillrandom.sh first with the same MODE and DB_ROOT.' >&2
  exit 1
fi

run_benchmark readrandom \
  --benchmarks=readrandom,waitforcompaction,stats --use_existing_db=true
