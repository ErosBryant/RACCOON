#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
# Continuous A -> B -> A hotspot migration; change only RAC window size.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
trace_dir=$(cd -- "${script_dir}/.." && pwd)
repo_dir=$(cd -- "${trace_dir}/.." && pwd)
dry_run=0
quick=0
while (($#)); do
  case "$1" in
    --dry-run) dry_run=1 ;;
    --quick) quick=1 ;;
    -h|--help)
      cat <<'HELP'
Usage: rac_sensitivity_workload_shift.sh [--quick] [--dry-run]
One process per configuration, continuously reading hotspot A, B, then A.
Writes remain uniform over the entire existing keyspace (Read80/Write20).
--quick: one exploratory run, 60 s per phase and 10 s warm-up.
Default: three repetitions, 180 s per phase and 30 s warm-up.
Overrides: RAC_SHIFT_WINDOW_VALUES, RAC_SHIFT_PHASE_SECONDS,
RAC_SHIFT_WARMUP_SECONDS, RAC_SHIFT_REPEATS, RAC_SHIFT_RATE,
RAC_SHIFT_TEMPLATE, RAC_SHIFT_RECORDS, RAC_SHIFT_RESULT_ROOT,
RAC_SHIFT_DB_ROOT, RAC_SHIFT_KEEP_DB (0 or 1), RAC_SHIFT_HOT_FRACTION.
The script builds an independent driver using the current librocksdb.so,
clones immutable SSTs with hard links, copies mutable metadata, and plots
actual telemetry after completion. The source template is never opened for writes.
HELP
      exit 0 ;;
    *) echo "[ERROR] Unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

stamp=$(date +%Y%m%d-%H%M%S)
window_values=${RAC_SHIFT_WINDOW_VALUES:-"10000 50000 100000 200000"}
phase_seconds=${RAC_SHIFT_PHASE_SECONDS:-$((quick ? 60 : 180))}
warmup_seconds=${RAC_SHIFT_WARMUP_SECONDS:-$((quick ? 10 : 30))}
repeats=${RAC_SHIFT_REPEATS:-$((quick ? 1 : 3))}
rate=${RAC_SHIFT_RATE:-10000}
records=${RAC_SHIFT_RECORDS:-200000000}
hot_fraction=${RAC_SHIFT_HOT_FRACTION:-0.001}
keep_db=${RAC_SHIFT_KEEP_DB:-0}
template=${RAC_SHIFT_TEMPLATE:-/mnt/nvme/rtc_motivation_runs/ycsb_template_e200000000_v500_uni0_3352618}
result_root=${RAC_SHIFT_RESULT_ROOT:-${trace_dir}/graphs/rac-sensitivity/${stamp}-workload-shift}
db_parent=${RAC_SHIFT_DB_ROOT:-/mnt/nvme/rtc_motivation_runs}
python_bin=${RAC_SHIFT_PYTHON:-python3}
for value in "${phase_seconds}" "${repeats}" "${rate}" "${records}"; do
  [[ $value =~ ^[1-9][0-9]*$ ]] || { echo "[ERROR] Invalid positive integer: $value" >&2; exit 2; }
done
[[ $warmup_seconds =~ ^[0-9]+$ && $keep_db =~ ^[01]$ ]] || exit 2
read -r -a windows <<< "$window_values"
((${#windows[@]})) || { echo "[ERROR] Empty window list" >&2; exit 2; }
for window in "${windows[@]}"; do
  [[ $window =~ ^[1-9][0-9]*$ ]] || { echo "[ERROR] Invalid window: $window" >&2; exit 2; }
done
[[ -f $template/CURRENT && -f $repo_dir/librocksdb.so ]] || {
  echo "[ERROR] Missing template or compiled RocksDB library" >&2; exit 1;
}
echo "[PLAN] windows=$window_values repetitions=$repeats phase=${phase_seconds}s warmup=${warmup_seconds}s rate=${rate}ops/s"
echo "[PLAN] template=$template output=$result_root"
((dry_run)) && exit 0

ulimit -n 1048576 2>/dev/null || true
mkdir -p -- "$result_root"
[[ ! -e $result_root/manifest.csv ]] || { echo "[ERROR] Result directory already contains an experiment" >&2; exit 1; }
build_dir=$(mktemp -d /tmp/rac-shift-build.XXXXXX)
driver=$build_dir/workload_shift_driver
g++ -std=c++20 -O2 -Wall -Wextra -Werror -DNDEBUG -fno-rtti -DROCKSDB_PLATFORM_POSIX \
  -DROCKSDB_LIB_IO_POSIX -DOS_LINUX -I"$repo_dir" -I"$repo_dir/include" \
  "$trace_dir/workload_shift_driver.cc" -L"$repo_dir" -Wl,-rpath,"$repo_dir" \
  -lrocksdb -lpthread -ldl -lrt -lsnappy -lz -lbz2 -llz4 -lzstd -o "$driver"
db_root=$(mktemp -d "$db_parent/rac_shift_${stamp}.XXXXXX")
printf 'repeat,window,status,raw_dir,db_path\n' > "$result_root/manifest.csv"
cat > "$result_root/experiment.conf" <<CONF
template=$template
records=$records
windows=$window_values
phase_seconds=$phase_seconds
warmup_seconds=$warmup_seconds
repeats=$repeats
rate_ops_sec=$rate
read_percent=80
hot_fraction=$hot_fraction
hot_distribution=Zipfian_theta_0.8_within_current_range
write_distribution=uniform_full_keyspace
workload_timing=paced_single_client_no_catch_up
driver=$driver
db_root=$db_root
library=$repo_dir/librocksdb.so
CONF
cp -- "$trace_dir/workload_shift_driver.cc" "$result_root/workload_shift_driver.cc"
sha256sum "$repo_dir/librocksdb.so" "$driver" > "$result_root/binary_checksums.txt"
"$python_bin" - "$template" "$result_root/template_metadata.json" <<'PY'
import hashlib
import json
from pathlib import Path
import sys
source, output = map(Path, sys.argv[1:])
files = [p for p in source.iterdir() if p.is_file() and
         (p.name in ('CURRENT', 'IDENTITY') or p.name.startswith(('MANIFEST-', 'OPTIONS-')))]
output.write_text(json.dumps({p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in files}, indent=2))
PY
for ((repeat=1; repeat<=repeats; repeat++)); do
  # Rotate run order across repetitions to reduce systematic cache/order bias.
  for ((index=0; index<${#windows[@]}; index++)); do
    window=${windows[$(((index + repeat - 1) % ${#windows[@]}))]}
    raw=$result_root/raw/repeat${repeat}_window${window}
    db=$db_root/repeat${repeat}_window${window}
    mkdir -p -- "$raw"
    "$python_bin" - "$template" "$db" <<'PY'
from pathlib import Path
import os
import shutil
import sys
source, target = map(Path, sys.argv[1:])
target.mkdir(exist_ok=False)
for item in source.iterdir():
    if item.is_symlink() or item.is_dir():
        raise RuntimeError(f"Unsupported template entry: {item}")
    if item.name == 'LOCK':
        continue
    destination = target / item.name
    if item.suffix in ('.sst', '.blob'):
        os.link(item, destination)
    else:
        shutil.copy2(item, destination)
PY
    cmd=("$driver" --db "$db" --output-dir "$raw" --records "$records"
         --window "$window" --phase-seconds "$phase_seconds" --rate "$rate"
         --read-percent 80 --hot-fraction "$hot_fraction" --seed "$((1000 + repeat - 1))"
         --warmup-seconds "$warmup_seconds")
    printf '%q ' "${cmd[@]}" > "$raw/command.sh"
    printf '\n' >> "$raw/command.sh"
    echo "[RUN] repeat=$repeat window=$window (A -> B -> A)"
    status=0
    "${cmd[@]}" > "$raw/driver.log" 2>&1 || status=$?
    printf '%s,%s,%s,%s,%s\n' "$repeat" "$window" "$status" "$raw" "$db" >> "$result_root/manifest.csv"
    if ((status)); then
      tail -30 "$raw/driver.log" >&2
      echo "[ERROR] Experiment failed; private clone retained at $db" >&2
      exit "$status"
    fi
    if ((keep_db == 0)); then
      "$python_bin" - "$db_root" "$db" <<'PY'
from pathlib import Path
import shutil
import sys
root, target = map(Path, sys.argv[1:])
if target.is_symlink() or target.parent.resolve() != root.resolve() or not target.name.startswith('repeat'):
    raise RuntimeError('Refusing to remove an unexpected clone path')
shutil.rmtree(target)
PY
    fi
    echo "[OK] repeat=$repeat window=$window"
  done
done
"$python_bin" - "$template" "$result_root/template_metadata.json" <<'PY'
import hashlib
import json
from pathlib import Path
import sys
source, saved = map(Path, sys.argv[1:])
for name, expected in json.loads(saved.read_text()).items():
    if hashlib.sha256((source / name).read_bytes()).hexdigest() != expected:
        raise RuntimeError(f'Template metadata changed: {name}')
print('[OK] Source template metadata unchanged')
PY
MPLCONFIGDIR=${MPLCONFIGDIR:-/tmp/rtc-mpl} "$python_bin" \
  "$trace_dir/plot_rac_workload_shift.py" --run-dir "$result_root"
echo "[OK] Experiment and figures: $result_root"
