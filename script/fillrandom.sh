#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

set -euo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

if [[ -e ${db_path} || -L ${db_path} ]]; then
  echo "Error: database path already exists: ${db_path}" >&2
  echo 'Choose a new DB_ROOT to run fillrandom again.' >&2
  exit 1
fi
mkdir -p -- "${DB_ROOT}"

run_benchmark fillrandom \
  --benchmarks=fillrandom,flush,waitforcompaction,stats --use_existing_db=false
