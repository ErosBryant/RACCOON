#!/usr/bin/env bash

# One foreground user thread executes both reads and writes. Every operation
# independently chooses read/write according to
# RTC_MOTIVATION_MIX_GET_RATIOS and its complementary Put ratio,
# using RocksDB mixgraph's randomized per-operation query selection.

set -euo pipefail
ulimit -n 1048576 2>/dev/null || echo "[WARN] Could not raise open file limit; current limit: $(ulimit -n)"


script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd -- "${script_dir}/../.." && pwd)

# -------------------- Paths --------------------
export DB_BENCH=${DB_BENCH:-"${repo_dir}/db_bench"}
export RTC_MOTIVATION_RUN_DB_ROOT=${RTC_MOTIVATION_RUN_DB_ROOT:-"/mnt/nvme/rtc_motivation_runs"}
export RTC_MOTIVATION_RESULTS_ROOT=${RTC_MOTIVATION_RESULTS_ROOT:-"${repo_dir}/trace-zhao/motivation/mixgraph/"}

# -------------------- Database/workload size --------------------
export RTC_MOTIVATION_ENTRIES=${RTC_MOTIVATION_ENTRIES:-250000000}
export RTC_MOTIVATION_OPERATIONS=${RTC_MOTIVATION_OPERATIONS:-"32000000"}
export RTC_MOTIVATION_KEY_SIZE=${RTC_MOTIVATION_KEY_SIZE:-16}
export RTC_MOTIVATION_VALUE_SIZE=${RTC_MOTIVATION_VALUE_SIZE:-"500"}

# One foreground user thread. Background compaction threads are separate.
export RTC_MOTIVATION_THREADS=${RTC_MOTIVATION_THREADS:-1}
export RTC_MOTIVATION_MAX_BACKGROUND_JOBS=${RTC_MOTIVATION_MAX_BACKGROUND_JOBS:-2}

# -------------------- RocksDB options --------------------
export RTC_MOTIVATION_COMPRESSION=${RTC_MOTIVATION_COMPRESSION:-"snappy"}
export RTC_MOTIVATION_UNIFORM=${RTC_MOTIVATION_UNIFORM:-"0 1"}
export RTC_MOTIVATION_SEED=${RTC_MOTIVATION_SEED:-1000}

# -------------------- Ratio sweep / modes --------------------
export RTC_MOTIVATION_MIX_GET_RATIOS=${RTC_MOTIVATION_MIX_GET_RATIOS:-"0.8 0.9 1.0"}
# 10 90 20 80 30 70 40 60
export RTC_MOTIVATION_MODES=${RTC_MOTIVATION_MODES:-"rac rtc baseline"}
# rtc rac baseline

# -------------------- RTC options --------------------
export RTC_MOTIVATION_RTC_THRESHOLD=${RTC_MOTIVATION_RTC_THRESHOLD:-0.01}
export RTC_MOTIVATION_RTC_WAKEUP_SECONDS=${RTC_MOTIVATION_RTC_WAKEUP_SECONDS:-1}
export RTC_MOTIVATION_RTC_READ100_WAKEUP_SECONDS=${RTC_MOTIVATION_RTC_READ100_WAKEUP_SECONDS:-600}

# -------------------- RAC Option 2 options --------------------
export RTC_MOTIVATION_RAC_TOP_K=${RTC_MOTIVATION_RAC_TOP_K:-6}
export RTC_MOTIVATION_RAC_DUAL_PURPOSE_SELECTION=${RTC_MOTIVATION_RAC_DUAL_PURPOSE_SELECTION:-"1"}
export RTC_MOTIVATION_RAC_WINDOW_SIZE=${RTC_MOTIVATION_RAC_WINDOW_SIZE:-"50000"}
export RTC_MOTIVATION_RAC_BENEFIT_EFFICIENCY_THRESHOLD=${RTC_MOTIVATION_RAC_BENEFIT_EFFICIENCY_THRESHOLD:-"7"}
export RTC_MOTIVATION_RAC_HIGH_MISS_RATIO_THRESHOLD=${RTC_MOTIVATION_RAC_HIGH_MISS_RATIO_THRESHOLD:-"1"}
export RTC_MOTIVATION_RAC_BOTTOM_OVERLAP_THRESHOLD=${RTC_MOTIVATION_RAC_BOTTOM_OVERLAP_THRESHOLD:-4}
export RTC_MOTIVATION_RAC_COOLDOWN_WINDOWS=${RTC_MOTIVATION_RAC_COOLDOWN_WINDOWS:-4}
export RTC_MOTIVATION_RAC_READ_BENEFIT_SLACK=${RTC_MOTIVATION_RAC_READ_BENEFIT_SLACK:-0.6}
export RTC_MOTIVATION_RAC_EMERGING_SCORE_FLOOR=${RTC_MOTIVATION_RAC_EMERGING_SCORE_FLOOR:-0.80}
export RTC_MOTIVATION_RAC_NUM_BOTTOM_PRI_THREADS=${RTC_MOTIVATION_RAC_NUM_BOTTOM_PRI_THREADS:-2}

# -------------------- Experiment behavior --------------------
# Each value-size/distribution/mode combination loads one fresh DB. All ratios
# for that combination run sequentially on the same DB; no base-DB copy is
# performed between ratios.
export RTC_MOTIVATION_DROP_CACHES=${RTC_MOTIVATION_DROP_CACHES:-1}
export RTC_MOTIVATION_CLEANUP_RUN_DB=${RTC_MOTIVATION_CLEANUP_RUN_DB:-1}
# This entry point always builds one fresh shared template. Do not inherit an
# external template path left in the invoking shell by an earlier experiment.
export RTC_MOTIVATION_REUSE_RTC_DB=""

# Optional extra db_bench flags, for example:
export RTC_MOTIVATION_EXTRA_DB_BENCH_ARGS="--cache_size=0"
export RTC_MOTIVATION_EXTRA_DB_BENCH_ARGS=${RTC_MOTIVATION_EXTRA_DB_BENCH_ARGS:-}

exec "${repo_dir}/scripts/run_rtc_mix.sh" "${1:-A}"
