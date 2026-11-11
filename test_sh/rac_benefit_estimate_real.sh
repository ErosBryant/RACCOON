#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
# Multi-compaction predicted vs measured read benefit using paired checkpoints.
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
Usage: rac_benefit_estimate_real.sh [--quick] [--dry-run]

Run one main read-only trace and capture every RAC compaction candidate in its
pre-execution callback, together with its estimate and exact physical input
closure. Then measure each captured compaction offline: Control keeps the
layout, while Treatment performs exactly that compaction. Both replay the same
read-only warm-up and measurement traces in alternating order.

Read-only calibration first: mean read latency difference in ns/lookup is the
primary measured benefit. P99 and actual compaction I/O are auxiliary metrics.
No negative measured benefits are discarded and no fake results are generated.

Defaults: existing 200M-key v500 template, TopK4, Window100k, eta0.01,
DP enabled, one foreground thread, no block cache, and one main experiment.
The main trace performs 50M reads and may capture multiple compactions. Each
captured compaction is then measured for two rounds, alternating which arm runs
first; each arm performs 100k warm-up reads and 1M measured reads per round.
Thus each captured compaction adds 4M measured reads plus warm-up by default.
After the 50M reads, queued RAC work is drained without issuing more reads so
the final eligible compactions are also captured.
--quick: 200k main reads, 10k warm-up, and 100k measured reads per arm/round.

Overrides: RAC_BENEFIT_TEMPLATE, RAC_BENEFIT_RECORDS, RAC_BENEFIT_REPEATS,
RAC_BENEFIT_MAIN_READS, RAC_BENEFIT_WARMUP_READS, RAC_BENEFIT_MEASURE_READS,
RAC_BENEFIT_MAX_COMPACTIONS, RAC_BENEFIT_MEASUREMENT_ROUNDS,
RAC_BENEFIT_WINDOW, RAC_BENEFIT_TOP_K, RAC_BENEFIT_ETA, RAC_BENEFIT_DP,
RAC_BENEFIT_UNIFORM, RAC_BENEFIT_SEED, RAC_BENEFIT_DIRECT_READS, RAC_BENEFIT_DB_ROOT,
RAC_BENEFIT_RESULT_ROOT, RAC_BENEFIT_KEEP_DB (default 0).
Matching RTC_MOTIVATION_* variables are accepted as fallbacks for shared settings.
RAC_BENEFIT_LEARN_READS remains accepted as a compatibility fallback for
RAC_BENEFIT_MAIN_READS. RAC_BENEFIT_MAX_COMPACTIONS=0 captures without a cap.

The source template must be idle. Immutable SSTs are hard-linked; mutable
metadata is copied. All experiment writes occur in newly created private DBs.
OS caches are not dropped globally; arm order alternates across pairs.
Direct SST reads default to 0; SST reads use the OS page cache while the
RocksDB block cache is disabled. Set RAC_BENEFIT_DIRECT_READS=1 for direct reads.
HELP
      exit 0 ;;
    *) echo "[ERROR] Unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

stamp=$(date +%Y%m%d-%H%M%S)
template=${RAC_BENEFIT_TEMPLATE:-${RTC_MOTIVATION_REUSE_RTC_DB:-/mnt/nvme/rtc_motivation_runs/ycsb_template_e200000000_v500_uni0_3352618}}
records=${RAC_BENEFIT_RECORDS:-${RTC_MOTIVATION_ENTRIES:-200000000}}
window=${RAC_BENEFIT_WINDOW:-${RTC_MOTIVATION_RAC_WINDOW_SIZE:-100000}}
top_k=${RAC_BENEFIT_TOP_K:-${RTC_MOTIVATION_RAC_TOP_K:-4}}
eta=${RAC_BENEFIT_ETA:-${RTC_MOTIVATION_RAC_BENEFIT_EFFICIENCY_THRESHOLD:-0.01}}
dp=${RAC_BENEFIT_DP:-${RTC_MOTIVATION_RAC_DUAL_PURPOSE_SELECTION:-1}}
uniform=${RAC_BENEFIT_UNIFORM:-${RTC_MOTIVATION_UNIFORM:-0}}
direct_reads=${RAC_BENEFIT_DIRECT_READS:-0}
seed=${RAC_BENEFIT_SEED:-${RTC_MOTIVATION_SEED:-1000}}
repeats=${RAC_BENEFIT_REPEATS:-1}
main_reads=${RAC_BENEFIT_MAIN_READS:-${RAC_BENEFIT_LEARN_READS:-$((quick ? 200000 : 50000000))}}
warmup_reads=${RAC_BENEFIT_WARMUP_READS:-$((quick ? 10000 : 100000))}
measure_reads=${RAC_BENEFIT_MEASURE_READS:-$((quick ? 100000 : 1000000))}
max_compactions=${RAC_BENEFIT_MAX_COMPACTIONS:-0}
measurement_rounds=${RAC_BENEFIT_MEASUREMENT_ROUNDS:-2}
db_parent=${RAC_BENEFIT_DB_ROOT:-${RTC_MOTIVATION_RUN_DB_ROOT:-/mnt/nvme/rtc_motivation_runs}}
result_root=${RAC_BENEFIT_RESULT_ROOT:-${trace_dir}/graphs/rac-benefit/${stamp}-estimate-real}
keep_db=${RAC_BENEFIT_KEEP_DB:-0}
python_bin=${RAC_BENEFIT_PYTHON:-python3}
for value in "$records" "$window" "$top_k" "$repeats" "$main_reads" "$measure_reads"; do
  [[ $value =~ ^[1-9][0-9]*$ ]] || { echo "[ERROR] Expected positive integer: $value" >&2; exit 2; }
done
[[ $warmup_reads =~ ^(0|[1-9][0-9]*)$ && $max_compactions =~ ^(0|[1-9][0-9]*)$ && $measurement_rounds =~ ^[1-9][0-9]*$ && $seed =~ ^(0|[1-9][0-9]*)$ && $dp =~ ^[01]$ && $uniform =~ ^[01]$ && $direct_reads =~ ^[01]$ && $keep_db =~ ^[01]$ ]] || {
  echo "[ERROR] Invalid nonnegative count or boolean setting" >&2
  exit 2
}
((main_reads >= window)) || {
  echo "[ERROR] RAC_BENEFIT_MAIN_READS must be at least RAC_BENEFIT_WINDOW" >&2
  exit 2
}
((measurement_rounds >= 2)) || {
  echo "[ERROR] RAC_BENEFIT_MEASUREMENT_ROUNDS must be at least 2 so both arm orders are measured" >&2
  exit 2
}
((measurement_rounds % 2 == 0)) || {
  echo "[ERROR] RAC_BENEFIT_MEASUREMENT_ROUNDS must be even so both arm orders have equal weight" >&2
  exit 2
}
"$python_bin" - "$eta" "$records" "$window" "$top_k" "$repeats" "$main_reads" \
  "$warmup_reads" "$measure_reads" "$measurement_rounds" "$max_compactions" "$seed" <<'PY'
import math,sys
value=float(sys.argv[1])
if not math.isfinite(value) or value < 0:
    raise SystemExit('RAC_BENEFIT_ETA must be finite and nonnegative')
counts=list(map(int,sys.argv[2:]))
if any(value > 2**63-1 for value in counts):
    raise SystemExit('Counts and seed must fit signed 64-bit shell arithmetic')
records,window,top_k,repeats,main_reads,warmup,measure,rounds,cap,seed=counts
if top_k > 2**32-1 or measure*rounds > 2**63-1 or seed+repeats-1 > 2**63-1:
    raise SystemExit('Top K, total measurement reads, or repeated seed is too large')
PY
[[ -f $template/CURRENT && -f $repo_dir/librocksdb.so ]] || {
  echo "[ERROR] Missing existing DB template or librocksdb.so" >&2; exit 1;
}
echo "[PLAN] Multi-compaction read-only calibration: W=$window K=$top_k eta=$eta DP=$dp repeats=$repeats"
echo "[PLAN] main_reads=$main_reads max_compactions=$max_compactions warmup=$warmup_reads measure_per_arm_round=$measure_reads rounds=$measurement_rounds"
echo "[PLAN] seed=$seed uniform=$uniform direct_reads=$direct_reads block_cache=disabled"
echo "[PLAN] template=$template output=$result_root"
((dry_run)) && exit 0

ulimit -n 1048576 2>/dev/null || true
mkdir -p -- "$result_root"
[[ ! -e $result_root/manifest.csv ]] || { echo "[ERROR] Results already exist" >&2; exit 1; }
build_dir=$(mktemp -d /tmp/rac-benefit-build.XXXXXX)
driver=$build_dir/benefit_estimate_real_driver
g++ -std=c++20 -O2 -Wall -Wextra -Werror -DNDEBUG -fno-rtti \
  -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX \
  -I"$repo_dir" -I"$repo_dir/include" "$trace_dir/benefit_estimate_real_driver.cc" \
  -L"$repo_dir" -Wl,-rpath,"$repo_dir" -lrocksdb -lpthread -ldl -lrt \
  -lsnappy -lz -lbz2 -llz4 -lzstd -o "$driver"
db_root=$(mktemp -d "$db_parent/rac_benefit_${stamp}.XXXXXX")
cp -- "$trace_dir/benefit_estimate_real_driver.cc" "$result_root/benefit_estimate_real_driver.cc"
sha256sum "$repo_dir/librocksdb.so" "$driver" > "$result_root/binary_checksums.txt"
printf 'pair,seed,status,raw_dir,db_root\n' > "$result_root/manifest.csv"
cat > "$result_root/experiment.conf" <<CONF
template=$template
records=$records
window=$window
top_k=$top_k
eta=$eta
dual_purpose=$dp
uniform=$uniform
direct_reads=$direct_reads
seed=$seed
repeats=$repeats
main_reads=$main_reads
warmup_reads=$warmup_reads
measure_reads=$measure_reads
measure_reads_per_arm_round=$measure_reads
measure_reads_per_arm_total=$((measure_reads * measurement_rounds))
max_compactions=$max_compactions
measurement_rounds=$measurement_rounds
workload=read_only_stationary
db_root=$db_root
CONF
"$python_bin" - "$template" "$result_root/template_metadata.json" <<'PY'
import hashlib,json
from pathlib import Path
import sys
source,output=map(Path,sys.argv[1:])
files=[p for p in source.iterdir() if p.is_file() and
       (p.name in ('CURRENT','IDENTITY') or p.name.startswith(('MANIFEST-','OPTIONS-')))]
output.write_text(json.dumps({p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in files},indent=2))
PY
for ((pair=1; pair<=repeats; pair++)); do
  raw=$result_root/raw/pair${pair}
  private_root=$db_root/pair${pair}
  learning=$private_root/learning
  mkdir -p -- "$raw" "$private_root"
  "$python_bin" - "$template" "$learning" <<'PY'
from pathlib import Path
import os,shutil,sys
source,target=map(Path,sys.argv[1:])
target.mkdir(exist_ok=False)
for item in source.iterdir():
    if item.is_symlink() or not item.is_file():
        raise RuntimeError(f'Unsupported template entry: {item}')
    if item.name=='LOCK':
        continue
    if item.suffix in ('.sst','.blob'):
        os.link(item,target/item.name)
    else:
        shutil.copy2(item,target/item.name)
PY
  pair_seed=$((seed + pair - 1))
  order=control-first
  treatment_first=0
  if ((pair % 2 == 0)); then
    order=treatment-first
    treatment_first=1
  fi
  cmd=("$driver" --db "$learning" --output-dir "$raw" --checkpoint-root "$private_root"
       --records "$records" --window "$window" --top-k "$top_k" --eta "$eta"
       --dual-purpose "$dp" --seed "$pair_seed" --uniform "$uniform"
       --direct-reads "$direct_reads"
       --main-reads "$main_reads" --warmup-reads "$warmup_reads"
       --measure-reads "$measure_reads" --max-compactions "$max_compactions"
       --measurement-rounds "$measurement_rounds"
       --treatment-first "$treatment_first")
  printf '%q ' "${cmd[@]}" > "$raw/command.sh"
  printf '\n' >> "$raw/command.sh"
  echo "[RUN] pair=$pair seed=$pair_seed order=$order"
  status=0
  "${cmd[@]}" > "$raw/driver.log" 2>&1 || status=$?
  printf '%s,%s,%s,%s,%s\n' "$pair" "$pair_seed" "$status" "$raw" "$private_root" >> "$result_root/manifest.csv"
  if ((status)); then
    if [[ -f $learning/LOG ]]; then
      cp -- "$learning/LOG" "$raw/learning.LOG"
    fi
    tail -30 "$raw/driver.log" >&2
    echo "[ERROR] Pair failed; private DBs and logs retained. No synthetic result substituted." >&2
    exit "$status"
  fi
  if ((keep_db == 0)); then
    "$python_bin" - "$db_root" "$private_root" <<'PY'
from pathlib import Path
import shutil,sys
root,target=map(Path,sys.argv[1:])
if target.is_symlink() or target.parent.resolve()!=root.resolve() or not target.name.startswith('pair'):
    raise RuntimeError('Unexpected private DB path')
shutil.rmtree(target)
PY
  fi
  echo "[OK] pair=$pair"
done
"$python_bin" - "$template" "$result_root/template_metadata.json" <<'PY'
import hashlib,json
from pathlib import Path
import sys
source,saved=map(Path,sys.argv[1:])
for name,expected in json.loads(saved.read_text()).items():
    if hashlib.sha256((source/name).read_bytes()).hexdigest()!=expected:
        raise RuntimeError(f'Source template metadata changed: {name}')
print('[OK] Source template metadata unchanged')
PY
MPLCONFIGDIR=${MPLCONFIGDIR:-/tmp/rtc-mpl} "$python_bin" \
  "$trace_dir/plot_rac_benefit_estimate_real.py" --run-dir "$result_root"
echo "[OK] Paired benefit experiment: $result_root"
