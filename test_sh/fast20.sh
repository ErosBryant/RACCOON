#!/bin/bash
# ============================================================
# Script: fast20.sh
# Based on FAST'20 Appendix (mixgraph) with acceleration
# - Runs ZippyDB / UDB / UP2X workloads
# - Compares normal RocksDB, conventional RTC, RAC, and RAC2
# - Creates a separate DB and log set for every experiment configuration
# ============================================================

set -euo pipefail

# RocksDB db_bench binary path
DB_BENCH="/home/eros/forRTC/rocksdb-rtc/db_bench"
# Monitoring helpers
source "/home/eros/forRTC/rocksdb-rtc/trace-zhao/test_sh/monitor.sh"

ulimit -n 1048576 2>/dev/null || echo "[WARN] Could not raise open file limit; current limit: $(ulimit -n)"
echo "[INFO] open file limit: $(ulimit -n)"


# OUTDIR="${OUTDIR:-./0428/fb_workload_results}"
OUTDIR="/home/eros/forRTC/rocksdb-rtc/trace-zhao/fast-result/$(date +%m%d_%H%M)"
HIT_MISS_LOG_DIR="${OUTDIR}/rac-telemetry"
mkdir -p "$OUTDIR" "$HIT_MISS_LOG_DIR"

# -------------------- Common knobs --------------------------
# Appendix base knobs (FAST'20)
CACHE_SIZE=268435456
KEY_SIZE=48
VAL_SIZE_FILL=1000
NUM_FILL=200000000

# Acceleration (24h sine → 10x faster)
SINE_INT_MS=5000
SINE_A=1000            # base intensity (can tune to 1500 if you want stronger)
SINE_B=0.0000073       # 0.000073 / 10
SINE_D=45000           # 4500 * 10

# Perf + threading
PERF_LEVEL=3
THREADS=1              # mixgraph更稳定

# 0: normal RocksDB, 1: RocksDB RTC, 2: our RAC, 3: RAC2 time model.
test_OPTIONS=(3 2)

# Keep FAST20 sweep overrides first, then use the YCSB/shared settings.
rac_top_k_values=${RAC_TOP_K_VALUES:-${YCSB_RAC_TOP_K:-${RTC_MOTIVATION_RAC_TOP_K:-4}}}
rac_option2_dual_values=${RAC_OPTION2_DUAL_PURPOSE_SELECTION_VALUES:-${YCSB_RAC_DUAL_PURPOSE_SELECTION:-${RTC_MOTIVATION_RAC_DUAL_PURPOSE_SELECTION:-1}}}
rac_window_size_values=${RAC_WINDOW_SIZE_VALUES:-${YCSB_RAC_WINDOW_SIZE:-${RTC_MOTIVATION_RAC_WINDOW_SIZE:-100000}}}
rac_benefit_efficiency_threshold_values=${RAC_BENEFIT_EFFICIENCY_THRESHOLD_VALUES:-${YCSB_RAC_BENEFIT_EFFICIENCY_THRESHOLD:-${RTC_MOTIVATION_RAC_BENEFIT_EFFICIENCY_THRESHOLD:-1}}}
# The current RAC2 time model ignores this legacy efficiency setting.
rac2_benefit_efficiency_threshold_values=${RAC2_BENEFIT_EFFICIENCY_THRESHOLD_VALUES:-${YCSB_RAC2_BENEFIT_EFFICIENCY_THRESHOLD:-0.05}}
rac_high_miss_ratio_threshold_values=${RAC_HIGH_MISS_RATIO_THRESHOLD_VALUES:-${YCSB_RAC_HIGH_MISS_RATIO_THRESHOLD:-${RTC_MOTIVATION_RAC_HIGH_MISS_RATIO_THRESHOLD:-0.95}}}
rac_option2_allow_l0_with_writes=${RAC_OPTION2_ALLOW_L0_WITH_WRITES:-${YCSB_RAC_ALLOW_L0_WITH_WRITES:-${RTC_MOTIVATION_RAC_ALLOW_L0_WITH_WRITES:-1}}}
rac_option2_l0_efficiency_threshold=${RAC_OPTION2_L0_EFFICIENCY_THRESHOLD:-${YCSB_RAC_L0_EFFICIENCY_THRESHOLD:-${RTC_MOTIVATION_RAC_L0_EFFICIENCY_THRESHOLD:-4}}}
rac_option2_l0_cooldown_windows=${RAC_OPTION2_L0_COOLDOWN_WINDOWS:-${YCSB_RAC_L0_COOLDOWN_WINDOWS:-${RTC_MOTIVATION_RAC_L0_COOLDOWN_WINDOWS:-4}}}
rac_bottom_overlap_threshold_values=${RAC_BOTTOM_OVERLAP_THRESHOLD_VALUES:-${YCSB_RAC_BOTTOM_OVERLAP_THRESHOLD:-${RTC_MOTIVATION_RAC_BOTTOM_OVERLAP_THRESHOLD:-4}}}

rac2_hotspot_persistence=${RAC2_HOTSPOT_PERSISTENCE:-0.8}
rac2_prediction_horizon=${RAC2_PREDICTION_HORIZON:-4}
rac2_compaction_bandwidth_mib_per_sec=${RAC2_COMPACTION_BANDWIDTH_MIB_PER_SEC:-400}

rac_cooldown_windows_values=${RAC_COOLDOWN_WINDOWS_VALUES:-${YCSB_RAC_COOLDOWN_WINDOWS:-${RTC_MOTIVATION_RAC_COOLDOWN_WINDOWS:-4}}}
rac_option2_read_benefit_slack_values=${RAC_OPTION2_READ_BENEFIT_SLACK_VALUES:-${YCSB_RAC_READ_BENEFIT_SLACK:-${RTC_MOTIVATION_RAC_READ_BENEFIT_SLACK:-0.6}}}
rac_option2_emerging_score_floor_values=${RAC_OPTION2_EMERGING_SCORE_FLOOR_VALUES:-${YCSB_RAC_EMERGING_SCORE_FLOOR:-${RTC_MOTIVATION_RAC_EMERGING_SCORE_FLOOR:-0.6}}}

read_triggered_threshold_values=${READ_TRIGGERED_COMPACTION_THRESHOLDS:-"0.01"}
trigger_check_seconds_values=${TRIGGER_CHECK_SECONDS_VALUES:-"600"}
num_bottom_pri_threads=${NUM_BOTTOM_PRI_THREADS:-${YCSB_RAC_NUM_BOTTOM_PRI_THREADS:-${RTC_MOTIVATION_RAC_NUM_BOTTOM_PRI_THREADS:-2}}}

cleanup_db_after_run=1

test_option_args=()
test_args_for_option() {
  local option=$1
  local selected_top_k=$2
  local selected_dual=$3
  local selected_threshold=$4
  local selected_trigger_check_seconds=$5

  case "${option}" in
    0)
      test_option_args=(
        "--enable_rac=false"
        "--read_triggered_compaction_threshold=0"
      )
      ;;
    1)
      test_option_args=(
        "--enable_rac=false"
        "--read_triggered_compaction_threshold=${selected_threshold}"
        "--max_compaction_trigger_wakeup_seconds=${selected_trigger_check_seconds}"
      )
      ;;
    2)
      test_option_args=(
        "--enable_rac=true"
        "--read_triggered_compaction_threshold=0"
        "--rac_design_option=2"
        "--rac_window_size=${rac_window_size}"
        "--rac_top_k=${selected_top_k}"
        "--rac_benefit_efficiency_threshold=${rac_benefit_efficiency_threshold}"
        "--rac_high_miss_ratio_threshold=${rac_high_miss_ratio_threshold}"
        "--rac_option2_allow_l0_with_writes=${rac_option2_allow_l0_with_writes}"
        "--rac_option2_l0_efficiency_threshold=${rac_option2_l0_efficiency_threshold}"
        "--rac_option2_l0_cooldown_windows=${rac_option2_l0_cooldown_windows}"
        "--rac_bottom_overlap_threshold=${rac_bottom_overlap_threshold}"
        "--rac_cooldown_windows=${rac_cooldown_windows}"
        "--rac_option2_dual_purpose_selection=${selected_dual}"
        "--rac_option2_read_benefit_slack=${rac_option2_read_benefit_slack}"
        "--rac_option2_emerging_score_floor=${rac_option2_emerging_score_floor}"
        # "--compaction_pri=3"
        "--num_bottom_pri_threads=${num_bottom_pri_threads}"
      )
      ;;
    3)
      test_option_args=(
        "--enable_rac=true"
        "--read_triggered_compaction_threshold=0"
        "--rac_design_option=3"
        "--rac2_hotspot_persistence=${rac2_hotspot_persistence}"
        "--rac2_prediction_horizon=${rac2_prediction_horizon}"
        "--rac2_compaction_bandwidth_mib_per_sec=${rac2_compaction_bandwidth_mib_per_sec}"
        "--rac_window_size=${rac_window_size}"
        "--rac_top_k=${selected_top_k}"
        "--rac_benefit_efficiency_threshold=${rac_benefit_efficiency_threshold}"
        "--rac_high_miss_ratio_threshold=${rac_high_miss_ratio_threshold}"
        "--rac_option2_allow_l0_with_writes=${rac_option2_allow_l0_with_writes}"
        "--rac_option2_l0_efficiency_threshold=${rac_option2_l0_efficiency_threshold}"
        "--rac_option2_l0_cooldown_windows=${rac_option2_l0_cooldown_windows}"
        "--rac_bottom_overlap_threshold=${rac_bottom_overlap_threshold}"
        "--rac_cooldown_windows=${rac_cooldown_windows}"
        "--rac_option2_dual_purpose_selection=${selected_dual}"
        "--rac_option2_read_benefit_slack=${rac_option2_read_benefit_slack}"
        "--rac_option2_emerging_score_floor=${rac_option2_emerging_score_floor}"
        "--num_bottom_pri_threads=${num_bottom_pri_threads}"
      )
      ;;
    *)
      echo "Error: TEST_OPTIONS only accepts 0, 1, 2, or 3; got: ${option}" >&2
      exit 2
      ;;
  esac
}

read -r -a rac_top_k <<< "${rac_top_k_values}"
read -r -a rac_option2_dual_purpose_selection <<< "${rac_option2_dual_values}"
read -r -a rac_window_sizes <<< "${rac_window_size_values}"
read -r -a rac_benefit_efficiency_thresholds \
  <<< "${rac_benefit_efficiency_threshold_values}"
read -r -a rac2_benefit_efficiency_thresholds \
  <<< "${rac2_benefit_efficiency_threshold_values}"
read -r -a rac_high_miss_ratio_thresholds \
  <<< "${rac_high_miss_ratio_threshold_values}"
read -r -a rac_bottom_overlap_thresholds \
  <<< "${rac_bottom_overlap_threshold_values}"
read -r -a rac_cooldown_windows_list <<< "${rac_cooldown_windows_values}"
read -r -a rac_option2_read_benefit_slacks \
  <<< "${rac_option2_read_benefit_slack_values}"
read -r -a rac_option2_emerging_score_floors \
  <<< "${rac_option2_emerging_score_floor_values}"
read -r -a read_triggered_thresholds <<< "${read_triggered_threshold_values}"
read -r -a trigger_check_seconds_list <<< "${trigger_check_seconds_values}"

experiment_configs=()
for test_option in "${test_OPTIONS[@]}"; do
  case "${test_option}" in
    0)
      experiment_configs+=("0|NA|0|NA|NA|NA|NA|NA|NA|NA|0|NA")
      ;;
    1)
      for selected_threshold in "${read_triggered_thresholds[@]}"; do
        for selected_trigger_check_seconds in \
          "${trigger_check_seconds_list[@]}"; do
          experiment_configs+=(
            "1|NA|0|NA|NA|NA|NA|NA|NA|NA|${selected_threshold}|${selected_trigger_check_seconds}"
          )
        done
      done
      ;;
    2|3)
      if [[ ${test_option} == 3 ]]; then
        selected_efficiency_thresholds=(
          "${rac2_benefit_efficiency_thresholds[@]}"
        )
      else
        selected_efficiency_thresholds=(
          "${rac_benefit_efficiency_thresholds[@]}"
        )
      fi
      for selected_top_k in "${rac_top_k[@]}"; do
        for selected_dual in "${rac_option2_dual_purpose_selection[@]}"; do
          for rac_window_size in "${rac_window_sizes[@]}"; do
            for rac_benefit_efficiency_threshold in \
              "${selected_efficiency_thresholds[@]}"; do
              for rac_high_miss_ratio_threshold in \
                "${rac_high_miss_ratio_thresholds[@]}"; do
                for rac_bottom_overlap_threshold in \
                  "${rac_bottom_overlap_thresholds[@]}"; do
                  for rac_cooldown_windows in \
                    "${rac_cooldown_windows_list[@]}"; do
                    for rac_option2_read_benefit_slack in \
                      "${rac_option2_read_benefit_slacks[@]}"; do
                      for rac_option2_emerging_score_floor in \
                        "${rac_option2_emerging_score_floors[@]}"; do
                        experiment_configs+=(
                          "${test_option}|${selected_top_k}|${selected_dual}|${rac_window_size}|${rac_benefit_efficiency_threshold}|${rac_high_miss_ratio_threshold}|${rac_bottom_overlap_threshold}|${rac_cooldown_windows}|${rac_option2_read_benefit_slack}|${rac_option2_emerging_score_floor}|0|NA"
                        )
                      done
                    done
                  done
                done
              done
            done
          done
        done
      done
      ;;
    *)
      echo "Error: TEST_OPTIONS only accepts 0, 1, 2, or 3; got: ${test_option}" >&2
      exit 2
      ;;
  esac
done

if ((${#experiment_configs[@]} == 0)); then
  echo "Error: no experiment configuration was selected" >&2
  exit 2
fi
if [[ ${rac_option2_allow_l0_with_writes} != 0 &&
      ${rac_option2_allow_l0_with_writes} != 1 ]]; then
  echo "Error: RAC_OPTION2_ALLOW_L0_WITH_WRITES must be 0 or 1; got: ${rac_option2_allow_l0_with_writes}" >&2
  exit 2
fi

prepare_rac_telemetry_dir() {
  local db_path="$1"
  local telemetry_link="${db_path}/rac-telemetry"

  if [[ -e "${telemetry_link}" && ! -L "${telemetry_link}" ]]; then
    echo "Error: telemetry path already exists and is not a symlink: ${telemetry_link}" >&2
    return 1
  fi
  ln -sfn -- "${HIT_MISS_LOG_DIR}" "${telemetry_link}"
}

rename_hit_miss_log() {
  local run_log="$1"
  local workload_tag="$2"
  local config_tag="$3"

  local source_log
  source_log=$(sed -n 's/.*Logging GET_HIT_L\*\/GET_RMISS_L\* per-second stats to: \[\(.*\)\]/\1/p' "$run_log" | tail -n1)

  if [ -z "$source_log" ]; then
    echo "[WARN] No hit/miss per-second log found in ${run_log}"
    return
  fi

  if [ ! -f "$source_log" ]; then
    echo "[WARN] Parsed hit/miss log does not exist: ${source_log}"
    return
  fi

  local source_name
  source_name=$(basename "$source_log")
  local source_suffix
  source_suffix=${source_name#hit_miss_per_sec_}
  source_suffix=${source_suffix#*_hit_miss_per_sec_}

  local target_log
  target_log="${HIT_MISS_LOG_DIR}/hit_miss_per_sec_${workload_tag}_${config_tag}_${source_suffix}"

  mv -f "$source_log" "$target_log"
  echo "[INFO] Renamed hit/miss log: ${target_log}"
}

rename_compaction_log() {
  local run_log="$1"
  local workload_tag="$2"
  local config_tag="$3"

  local source_log
  source_log=$(sed -n 's/.*Logging completed compactions per-second stats to: \[\(.*\)\]/\1/p' "$run_log" | tail -n1)

  if [ -z "$source_log" ]; then
    echo "[WARN] No compaction per-second log found in ${run_log}"
    return
  fi

  if [ ! -f "$source_log" ]; then
    echo "[WARN] Parsed compaction log does not exist: ${source_log}"
    return
  fi

  local source_name
  source_name=$(basename "$source_log")
  local source_suffix
  source_suffix=${source_name#*_compaction_per_sec_}

  local target_log
  target_log="${HIT_MISS_LOG_DIR}/compaction_per_sec_${workload_tag}_${config_tag}_${source_suffix}"

  mv -f "$source_log" "$target_log"
  echo "[INFO] Renamed compaction log: ${target_log}"
}

run_with_monitor() {
  local log_file="$1"
  local db_path="$2"
  local tag="$3"
  shift 3
  local cmd=( "$@" )

  echo "${cmd[*]}" >> "$log_file"

  # Launch db_bench in background to capture PID for monitoring
  "${cmd[@]}" >> "$log_file" 2>&1 &
  local bench_pid=$!
  local monitor_prefix="${OUTDIR}/monitor_${tag}"

  start_monitors "$bench_pid" "$monitor_prefix" "$db_path"

  local exit_code=0
  wait "$bench_pid" || exit_code=$?

  stop_monitors "$monitor_prefix"

  local archive_exit_code=0
  local archive_tag="${tag#zippydb_}"
  archive_db_log "$db_path" "$archive_tag" || archive_exit_code=$?

  if ((exit_code != 0)); then
    return "$exit_code"
  fi
  return "$archive_exit_code"
}

archive_db_log() {
  local db_path="$1"
  local archive_tag="${2:-$(basename -- "${db_path}")}"
  local source_log="${db_path}/LOG"
  local archived_log="${HIT_MISS_LOG_DIR}/rocksdb_LOG_${archive_tag}.log"

  if [[ ! -f "${source_log}" ]]; then
    echo "[WARN] RocksDB LOG does not exist; nothing to archive: ${source_log}"
    return 1
  fi

  if ! cp -f -- "${source_log}" "${archived_log}"; then
    echo "[ERROR] Failed to archive RocksDB LOG; preserving DB: ${db_path}" >&2
    return 1
  fi
  echo "[INFO] Archived RocksDB LOG: ${archived_log}"
}

cleanup_db_dir() {
  local db_path="$1"

  if [ "$cleanup_db_after_run" != "1" ]; then
    return
  fi

  case "$db_path" in
    /mnt/*/fast_*)
      echo "[INFO] Removing DB directory after completed workloads: ${db_path}"
      rm -rf -- "$db_path"
      ;;
    *)
      echo "[WARN] Skip DB cleanup for suspicious path: ${db_path}"
      ;;
  esac
}

remove_fast_db_dir() {
  local db_path="$1"

  case "$db_path" in
    /mnt/*/fast_*)
      rm -rf -- "$db_path"
      ;;
    *)
      echo "Error: refusing to remove suspicious DB path: ${db_path}" >&2
      return 1
      ;;
  esac
}

clone_template_db() {
  local template_db="$1"
  local run_db="$2"

  if [[ ! -d "${template_db}" || ! -f "${template_db}/CURRENT" ]]; then
    echo "Error: template DB is missing or invalid: ${template_db}" >&2
    return 1
  fi
  if [[ -e "${run_db}" ]]; then
    echo "[INFO] Removing existing run DB before clone: ${run_db}"
    remove_fast_db_dir "${run_db}"
  fi

  cp -a --reflink=auto -- "${template_db}" "${run_db}"
  if [[ ! -f "${run_db}/CURRENT" ]]; then
    echo "Error: cloned DB is missing CURRENT: ${run_db}" >&2
    return 1
  fi
  echo "[INFO] Cloned template DB: ${template_db} -> ${run_db}"
}

    # -use_direct_io_for_flush_and_compaction=true \
    # -use_direct_reads=true \
# -------------------- Functions -----------------------------
fill_random_db () {
  local dbdir="$1"
  local rac_option="$2"
  local config_tag="$3"
  echo "[FILL] Creating DB at ${dbdir} (RAC option ${rac_option}, ${config_tag})"
  mkdir -p "${dbdir}"
  local log_file="${OUTDIR}/fillrandom_${config_tag}.log"
  run_with_monitor "$log_file" "$dbdir" "fillrandom_${config_tag}" \
    "${DB_BENCH}" --benchmarks=fillrandom,waitforcompaction,stats \
      --perf_level=2 \
      -cache_size=${CACHE_SIZE} \
      -key_size=${KEY_SIZE} \
      -value_size=${VAL_SIZE_FILL} \
      -num=${NUM_FILL} \
      -max_background_jobs=18 \
      -db="${dbdir}" \
      "${test_option_args[@]}"

}

    # -use_direct_io_for_flush_and_compaction=true \
    # -use_direct_reads=true \
run_zippydb_Prefix_random () {
  local dbdir="$1"
  local rac_option="$2"
  local config_tag="$3"
  echo "[RUN] ZippyDB Prefix_random (RAC option ${rac_option}, ${config_tag})"
  local log_file="${OUTDIR}/zippydb_Prefix_random_${config_tag}.log"
  run_with_monitor "$log_file" "$dbdir" "zippydb_Prefix_random_${config_tag}" \
    "${DB_BENCH}" --benchmarks="mixgraph,stats" \
      -cache_size=${CACHE_SIZE} \
      -keyrange_dist_a=14.18 -keyrange_dist_b=-2.917 \
      -keyrange_dist_c=0.0164 -keyrange_dist_d=-0.08082 \
      -keyrange_num=30 \
      -value_k=0.2615 -value_sigma=25.45 \
      -iter_k=2.517 -iter_sigma=14.236 \
      -mix_get_ratio=0.83 -mix_put_ratio=0.14 -mix_seek_ratio=0.03 \
      -sine_mix_rate_interval_milliseconds=${SINE_INT_MS} \
      -sine_a=${SINE_A} -sine_b=${SINE_B} -sine_d=${SINE_D} \
      --perf_level=${PERF_LEVEL} --threads=${THREADS} \
      -reads=420000000 -num=50000000 -key_size=${KEY_SIZE} -value_size=${VAL_SIZE_FILL} \
      -db="${dbdir}" -use_existing_db=true \
      "${test_option_args[@]}" -statistics
  rename_hit_miss_log "$log_file" "Prefix_random" "$config_tag"
  rename_compaction_log "$log_file" "Prefix_random" "$config_tag"
}
    # -use_direct_io_for_flush_and_compaction=true \
    # -use_direct_reads=true \

run_zippydb_All_random () {
  local dbdir="$1"
  local rac_option="$2"
  local config_tag="$3"
  echo "[RUN] ZippyDB All_random (RAC option ${rac_option}, ${config_tag})"
  local log_file="${OUTDIR}/zippydb_All_random_${config_tag}.log"
  run_with_monitor "$log_file" "$dbdir" "zippydb_All_random_${config_tag}" \
    "${DB_BENCH}" --benchmarks="mixgraph,stats" \
      -cache_size=${CACHE_SIZE} \
      -keyrange_num=1 -value_k=0.2615 -value_sigma=25.45 \
      -iter_k=2.517 -iter_sigma=14.236 -mix_get_ratio=0.83 \
      -mix_put_ratio=0.14 -mix_seek_ratio=0.03 \
      -sine_mix_rate_interval_milliseconds=${SINE_INT_MS} \
      -sine_a=${SINE_A} -sine_b=${SINE_B} -sine_d=${SINE_D} --perf_level=2 \
      -reads=420000000 -num=50000000 -key_size=${KEY_SIZE} -value_size=${VAL_SIZE_FILL} \
      -db="${dbdir}" -use_existing_db=true \
      "${test_option_args[@]}" -statistics
  rename_hit_miss_log "$log_file" "All_random" "$config_tag"
  rename_compaction_log "$log_file" "All_random" "$config_tag"
}



    # -use_direct_io_for_flush_and_compaction=true \
    # -use_direct_reads=true \
run_zippydb_All_dist () {
  local dbdir="$1"
  local rac_option="$2"
  local config_tag="$3"
  echo "[RUN] ZippyDB All_dist (RAC option ${rac_option}, ${config_tag})"
  local log_file="${OUTDIR}/zippydb_All_dist_${config_tag}.log"
  run_with_monitor "$log_file" "$dbdir" "zippydb_All_dist_${config_tag}" \
    "${DB_BENCH}" --benchmarks="mixgraph,stats" \
      -cache_size=${CACHE_SIZE} \
      -key_dist_a=0.002312 -key_dist_b=0.3467 \
      -keyrange_num=1 -value_k=0.2615 -value_sigma=25.45 \
      -iter_k=2.517 -iter_sigma=14.236 -mix_get_ratio=0.83 \
      -mix_put_ratio=0.14 -mix_seek_ratio=0.03 \
      -sine_mix_rate_interval_milliseconds=${SINE_INT_MS} \
      -sine_a=${SINE_A} -sine_b=${SINE_B} -sine_d=${SINE_D} --perf_level=2 \
      -reads=420000000 -num=50000000 -key_size=${KEY_SIZE} -value_size=${VAL_SIZE_FILL} \
      -db="${dbdir}" -use_existing_db=true \
      "${test_option_args[@]}" -statistics
  rename_hit_miss_log "$log_file" "All_dist" "$config_tag"
  rename_compaction_log "$log_file" "All_dist" "$config_tag"
}

run_zippydb_Prefix_dist () {
  local dbdir="$1"
  local rac_option="$2"
  local config_tag="$3"
  echo "[RUN] ZippyDB Prefix_dist (RAC option ${rac_option}, ${config_tag})"
  local log_file="${OUTDIR}/zippydb_Prefix_dist_${config_tag}.log"
  run_with_monitor "$log_file" "$dbdir" "zippydb_Prefix_dist_${config_tag}" \
    "${DB_BENCH}" --benchmarks="mixgraph,stats" \
      -cache_size=${CACHE_SIZE} \
      -key_dist_a=0.002312 -key_dist_b=0.3467 \
      -keyrange_dist_a=14.18 -keyrange_dist_b=-2.917 \
      -keyrange_dist_c=0.0164 -keyrange_dist_d=-0.08082 \
      -keyrange_num=30 -value_k=0.2615 -value_sigma=25.45 \
      -iter_k=2.517 -iter_sigma=14.236 -mix_get_ratio=0.83 \
      -mix_put_ratio=0.14 -mix_seek_ratio=0.03 \
      -sine_mix_rate_interval_milliseconds=${SINE_INT_MS} \
      -sine_a=${SINE_A} -sine_b=${SINE_B} -sine_d=${SINE_D} --perf_level=2 \
      -reads=420000000 -num=50000000 -key_size=${KEY_SIZE} -value_size=${VAL_SIZE_FILL} \
      -db="${dbdir}" -use_existing_db=true \
      "${test_option_args[@]}" -statistics
  rename_hit_miss_log "$log_file" "Prefix_dist" "$config_tag"
  rename_compaction_log "$log_file" "Prefix_dist" "$config_tag"
}



    # -use_direct_io_for_flush_and_compaction=true \
    # -use_direct_reads=true \
run_udb () {
  local dbdir="$1"
  local rac_option="$2"
  local config_tag="$3"
  echo "[RUN] UDB (RAC option ${rac_option}, ${config_tag})"
  local log_file="${OUTDIR}/udb_${config_tag}.log"
  run_with_monitor "$log_file" "$dbdir" "udb_${config_tag}" \
    "${DB_BENCH}" --benchmarks="mixgraph,stats" \
      -cache_size=${CACHE_SIZE} \
      -keyrange_dist_a=12.0 -keyrange_dist_b=-2.5 \
      -keyrange_dist_c=0.015 -keyrange_dist_d=-0.07 \
      -keyrange_num=30 \
      -value_k=0.2615 -value_sigma=25.45 \
      -iter_k=2.517 -iter_sigma=14.236 \
      -mix_get_ratio=0.806 -mix_put_ratio=0.159 -mix_seek_ratio=0.035 \
      -sine_mix_rate_interval_milliseconds=${SINE_INT_MS} \
      -sine_a=${SINE_A} -sine_b=${SINE_B} -sine_d=${SINE_D} \
      --perf_level=${PERF_LEVEL} --threads=${THREADS} \
      -reads=420000000 -num=${NUM_FILL} -key_size=${KEY_SIZE} \
      -db="${dbdir}" -use_existing_db=true \
      "${test_option_args[@]}" -statistics
  rename_hit_miss_log "$log_file" "UDB" "$config_tag"
  rename_compaction_log "$log_file" "UDB" "$config_tag"
}

  # 说明：mixgraph 不产 Merge。这里用 Put≈0.925 近似，
  # 请在打开 RocksDB 时注册你的 MergeOperator，使写入具有合并语义。
      # -use_direct_io_for_flush_and_compaction=true \
    # -use_direct_reads=true \
run_up2x_approx () {
  local dbdir="$1"
  local rac_option="$2"
  local config_tag="$3"
  echo "[RUN] UP2X (approx, Put≈Merge, RAC option ${rac_option}, ${config_tag})"
  local log_file="${OUTDIR}/up2x_${config_tag}.log"
  run_with_monitor "$log_file" "$dbdir" "up2x_${config_tag}" \
    "${DB_BENCH}" --benchmarks="mixgraph,stats" \
      -cache_size=${CACHE_SIZE} \
      -keyrange_dist_a=10 -keyrange_dist_b=-2.2 \
      -keyrange_dist_c=0.012 -keyrange_dist_d=-0.06 \
      -keyrange_num=20 \
      -value_k=0.25 -value_sigma=16 \
      -iter_k=2 -iter_sigma=10 \
      -mix_get_ratio=0.075 -mix_put_ratio=0.925 -mix_seek_ratio=0 \
      -sine_mix_rate_interval_milliseconds=${SINE_INT_MS} \
      -sine_a=${SINE_A} -sine_b=${SINE_B} -sine_d=${SINE_D} \
      --perf_level=${PERF_LEVEL} --threads=${THREADS} \
      -reads=111000000 -num=50000000 -key_size=${KEY_SIZE} -value_size=${VAL_SIZE_FILL} \
      -db="${dbdir}" -use_existing_db=true \
      "${test_option_args[@]}" -statistics
  rename_hit_miss_log "$log_file" "UP2X" "$config_tag"
  rename_compaction_log "$log_file" "UP2X" "$config_tag"
}

# -------------------- Main -----------------------------
# Load the initial data only once. Each option gets an independent clone of
# this clean template so every comparison starts from the same DB state.

# TEMPLATE_DB="/mnt/nvme/fast_template_$$"
# test_args_for_option 0 "NA" 0 0 "NA"
# fill_random_db "${TEMPLATE_DB}" 0 "shared_template"

TEMPLATE_DB="/mnt/nvme/fast_template_2196275"
test_args_for_option 0 "NA" 0 0 "NA"
# fill_random_db "${TEMPLATE_DB}" 0 "shared_template"

if [[ ! -f "${TEMPLATE_DB}/CURRENT" ]]; then
  echo "Error: loaded template DB is missing CURRENT: ${TEMPLATE_DB}" >&2
  exit 1
fi

for experiment_config in "${experiment_configs[@]}"; do
  IFS='|' read -r test_option selected_top_k selected_dual \
    rac_window_size rac_benefit_efficiency_threshold \
    rac_high_miss_ratio_threshold \
    rac_bottom_overlap_threshold rac_cooldown_windows \
    rac_option2_read_benefit_slack rac_option2_emerging_score_floor \
    selected_threshold selected_trigger_check_seconds \
    <<< "${experiment_config}"

  case "${test_option}" in
    0)
      mode_tag="normal"
      ;;
    1)
      mode_tag="rocksdb_rtc_th${selected_threshold}_wake${selected_trigger_check_seconds}"
      ;;
    2)
      mode_tag="rac_topk${selected_top_k}_dual${selected_dual}"
      mode_tag+="_w${rac_window_size}_eta${rac_benefit_efficiency_threshold}"
      mode_tag+="_miss${rac_high_miss_ratio_threshold}"
      mode_tag+="_l0w${rac_option2_allow_l0_with_writes}"
      mode_tag+="_l0eta${rac_option2_l0_efficiency_threshold}_l0cool${rac_option2_l0_cooldown_windows}"
      mode_tag+="_bottom${rac_bottom_overlap_threshold}_cool${rac_cooldown_windows}"
      mode_tag+="_alpha${rac_option2_read_benefit_slack}"
      mode_tag+="_beta${rac_option2_emerging_score_floor}"
      mode_tag+="_bthreads${num_bottom_pri_threads}"
      ;;
    3)
      mode_tag="rac2_topk${selected_top_k}_dual${selected_dual}"
      mode_tag+="_w${rac_window_size}_eta${rac_benefit_efficiency_threshold}"
      mode_tag+="_miss${rac_high_miss_ratio_threshold}"
      mode_tag+="_l0w${rac_option2_allow_l0_with_writes}"
      mode_tag+="_l0eta${rac_option2_l0_efficiency_threshold}_l0cool${rac_option2_l0_cooldown_windows}"
      mode_tag+="_bottom${rac_bottom_overlap_threshold}_cool${rac_cooldown_windows}"
      mode_tag+="_alpha${rac_option2_read_benefit_slack}"
      mode_tag+="_beta${rac_option2_emerging_score_floor}"
      mode_tag+="_bthreads${num_bottom_pri_threads}"
      mode_tag+="_p${rac2_hotspot_persistence}_h${rac2_prediction_horizon}"
      mode_tag+="_bw${rac2_compaction_bandwidth_mib_per_sec}"
      ;;
  esac

  test_args_for_option "${test_option}" "${selected_top_k}" \
    "${selected_dual}" "${selected_threshold}" \
    "${selected_trigger_check_seconds}"

  DBBASE="/mnt/nvme/fast_${mode_tag}"
  clone_template_db "${TEMPLATE_DB}" "${DBBASE}"
  prepare_rac_telemetry_dir "${DBBASE}"

  # run_udb "${DBBASE}" "${test_option}" "${mode_tag}"
  # run_zippydb_All_random "${DBBASE}" "${test_option}" "${mode_tag}"
  run_zippydb_All_dist "${DBBASE}" "${test_option}" "${mode_tag}"
  run_zippydb_Prefix_random "${DBBASE}" "${test_option}" "${mode_tag}"
  
  # run_zippydb_Prefix_dist "${DBBASE}" "${test_option}" "${mode_tag}"
  
  # run_up2x_approx "${DBBASE}" "${test_option}" "${mode_tag}"
  cleanup_db_dir "${DBBASE}"
done

# if [[ "${cleanup_db_after_run}" == "1" ]]; then
#   echo "[INFO] Removing shared template DB: ${TEMPLATE_DB}"
#   remove_fast_db_dir "${TEMPLATE_DB}"
# fi

echo "[DONE] Logs saved to ${OUTDIR}"
