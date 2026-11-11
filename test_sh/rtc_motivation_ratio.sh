#!/usr/bin/env bash

# One foreground user thread executes both reads and writes. Every operation
# independently chooses read/write according to RTC_MOTIVATION_READ_RATIOS,
# matching YCSB A-style random interleaving rather than fixed read/write batches.

set -euo pipefail
ulimit -n 1048576 2>/dev/null || echo "[WARN] Could not raise open file limit; current limit: $(ulimit -n)"


script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd -- "${script_dir}/../.." && pwd)

# -------------------- Paths --------------------
export DB_BENCH=${DB_BENCH:-"${repo_dir}/db_bench"}
export RTC_MOTIVATION_RUN_DB_ROOT=${RTC_MOTIVATION_RUN_DB_ROOT:-"/mnt/nvme/rtc_motivation_runs"}
export RTC_MOTIVATION_RESULTS_ROOT=${RTC_MOTIVATION_RESULTS_ROOT:-"${repo_dir}/trace-zhao/motivation/"}

# -------------------- Database/workload size --------------------
export RTC_MOTIVATION_ENTRIES=${RTC_MOTIVATION_ENTRIES:-200000000}
export RTC_MOTIVATION_OPERATIONS=${RTC_MOTIVATION_OPERATIONS:-"32000000"}
export RTC_MOTIVATION_KEY_SIZE=${RTC_MOTIVATION_KEY_SIZE:-16}
export RTC_MOTIVATION_VALUE_SIZE=${RTC_MOTIVATION_VALUE_SIZE:-"500"}

# One foreground user thread. Background compaction threads are separate.
export RTC_MOTIVATION_THREADS=${RTC_MOTIVATION_THREADS:-1}
export RTC_MOTIVATION_MAX_BACKGROUND_JOBS=${RTC_MOTIVATION_MAX_BACKGROUND_JOBS:-2}

# -------------------- RocksDB options --------------------
export RTC_MOTIVATION_COMPRESSION=${RTC_MOTIVATION_COMPRESSION:-"snappy"}
export RTC_MOTIVATION_UNIFORM=${RTC_MOTIVATION_UNIFORM:-"0"}
export RTC_MOTIVATION_SEED=${RTC_MOTIVATION_SEED:-1000}

# -------------------- Ratio sweep / modes --------------------
export RTC_MOTIVATION_READ_RATIOS=${RTC_MOTIVATION_READ_RATIOS:-"100"}
# 10 90 20 80 30 70 40 60
export RTC_MOTIVATION_MODES=${RTC_MOTIVATION_MODES:-"rac"}
# rtc rac rac2 baseline

# -------------------- RTC options --------------------
export RTC_MOTIVATION_RTC_THRESHOLD=${RTC_MOTIVATION_RTC_THRESHOLD:-0.01}
export RTC_MOTIVATION_RTC_WAKEUP_SECONDS=${RTC_MOTIVATION_RTC_WAKEUP_SECONDS:-1}
export RTC_MOTIVATION_RTC_READ100_WAKEUP_SECONDS=${RTC_MOTIVATION_RTC_READ100_WAKEUP_SECONDS:-1}

# -------------------- RAC Option 2 options --------------------
export RTC_MOTIVATION_RAC_TOP_K=${RTC_MOTIVATION_RAC_TOP_K:-6}
export RTC_MOTIVATION_RAC_DUAL_PURPOSE_SELECTION=${RTC_MOTIVATION_RAC_DUAL_PURPOSE_SELECTION:-"1"}
export RTC_MOTIVATION_RAC_WINDOW_SIZE=${RTC_MOTIVATION_RAC_WINDOW_SIZE:-"50000"}
export RTC_MOTIVATION_RAC_BENEFIT_EFFICIENCY_THRESHOLD=${RTC_MOTIVATION_RAC_BENEFIT_EFFICIENCY_THRESHOLD:-"0.01"}
# 0.01
export RTC_MOTIVATION_RAC_HIGH_MISS_RATIO_THRESHOLD=${RTC_MOTIVATION_RAC_HIGH_MISS_RATIO_THRESHOLD:-"1"}
export RTC_MOTIVATION_RAC_ALLOW_L0_WITH_WRITES=${RTC_MOTIVATION_RAC_ALLOW_L0_WITH_WRITES:-1}
export RTC_MOTIVATION_RAC_L0_EFFICIENCY_THRESHOLD=${RTC_MOTIVATION_RAC_L0_EFFICIENCY_THRESHOLD:-10}
export RTC_MOTIVATION_RAC_L0_COOLDOWN_WINDOWS=${RTC_MOTIVATION_RAC_L0_COOLDOWN_WINDOWS:-4}
export RTC_MOTIVATION_RAC_BOTTOM_OVERLAP_THRESHOLD=${RTC_MOTIVATION_RAC_BOTTOM_OVERLAP_THRESHOLD:-4}
export RTC_MOTIVATION_RAC_COOLDOWN_WINDOWS=${RTC_MOTIVATION_RAC_COOLDOWN_WINDOWS:-4}
export RTC_MOTIVATION_RAC_READ_BENEFIT_SLACK=${RTC_MOTIVATION_RAC_READ_BENEFIT_SLACK:-0.6}
export RTC_MOTIVATION_RAC_EMERGING_SCORE_FLOOR=${RTC_MOTIVATION_RAC_EMERGING_SCORE_FLOOR:-0.80}
export RTC_MOTIVATION_RAC_NUM_BOTTOM_PRI_THREADS=${RTC_MOTIVATION_RAC_NUM_BOTTOM_PRI_THREADS:-1}

# -------------------- RAC2 time-model options --------------------
# RAC2 keeps the existing RAC candidate discovery, 10 MiB seed floor, L0
# controls, and DP stage. Only the benefit/cost admission model changes.
export RTC_MOTIVATION_RAC2_HOTSPOT_PERSISTENCE=${RTC_MOTIVATION_RAC2_HOTSPOT_PERSISTENCE:-0.8}
export RTC_MOTIVATION_RAC2_PREDICTION_HORIZON=${RTC_MOTIVATION_RAC2_PREDICTION_HORIZON:-4}
export RTC_MOTIVATION_RAC2_COMPACTION_BANDWIDTH_MIB_PER_SEC=${RTC_MOTIVATION_RAC2_COMPACTION_BANDWIDTH_MIB_PER_SEC:-200}

# -------------------- Experiment behavior --------------------
# Leave empty to load a fresh template with fillrandom. Set this to an existing
# RocksDB directory to skip load, for example:
# /mnt/nvme/rtc_motivation_runs/rtc_motivation_template_ops32000000_v100_uni0_2132498
# Preserve an explicitly empty value: callers that change value size must load
# a matching template instead of accidentally reusing the default v500 DB.
if [[ ! -v RTC_MOTIVATION_REUSE_RTC_DB ]]; then
  export RTC_MOTIVATION_REUSE_RTC_DB="/mnt/nvme/rtc_motivation_runs/ycsb_template_e200000000_v500_uni0_3352618"
else
  export RTC_MOTIVATION_REUSE_RTC_DB
fi
export RTC_MOTIVATION_CLONE_PER_RATIO=${RTC_MOTIVATION_CLONE_PER_RATIO:-1}
export RTC_MOTIVATION_CLEANUP_RUN_DB=${RTC_MOTIVATION_CLEANUP_RUN_DB:-0}
export RTC_MOTIVATION_DROP_CACHES=${RTC_MOTIVATION_DROP_CACHES:-1}

if [[ -n ${RTC_MOTIVATION_REUSE_RTC_DB} && ! -f ${RTC_MOTIVATION_REUSE_RTC_DB}/CURRENT ]]; then
  echo "[ERROR] Reused template DB is missing CURRENT: ${RTC_MOTIVATION_REUSE_RTC_DB}" >&2
  echo "[ERROR] Clear RTC_MOTIVATION_REUSE_RTC_DB to load a fresh template." >&2
  exit 1
fi

# Optional extra db_bench flags, for example:
export RTC_MOTIVATION_EXTRA_DB_BENCH_ARGS="--cache_size=0"
export RTC_MOTIVATION_EXTRA_DB_BENCH_ARGS=${RTC_MOTIVATION_EXTRA_DB_BENCH_ARGS:-}

exec "${repo_dir}/scripts/run_rtc_motivation.sh" "${1:-A}"
