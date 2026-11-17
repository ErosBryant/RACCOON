# RACCOON

**Read-driven compaction for LSM-tree key-value stores**

[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue)](INSTALL.md)
[![Benchmark: db_bench](https://img.shields.io/badge/benchmark-db__bench-green)](#quick-start)
[![License](https://img.shields.io/badge/license-Apache%202.0%20%2F%20GPLv2-blue)](#license)

This repository contains the RocksDB-based implementation accompanying our
**IEEE ICDE 2027 submission (Paper ID: 2123)**:

> **Revisiting Read-Driven Compaction in LSM-Trees**

[Overview](#overview) · [Build](#build) · [Quick start](#quick-start) ·
[Benchmarks](#benchmarks) · [Configuration](#configuration) · [Results](#results)

## Overview

RACCOON uses read-path observations to decide when compaction is worthwhile and
which SSTables should participate. It combines three mechanisms:

| Mechanism | Purpose |
| --- | --- |
| Miss-latency profiling | Track per-SSTable hits, misses, and accumulated miss latency. |
| Efficiency-based admission | Compare the expected read benefit of a complete compaction candidate against its modeled rewrite cost. |
| Write-aligned selection (WAS) | Prefer candidates that also serve projected write-side maintenance. |

The scripts in [`script/`](script/) provide a small, self-contained evaluation
with random writes and random reads. They compare ordinary RocksDB compaction,
read-triggered compaction (RTC), and RACCOON using the same benchmark binary and
workload settings. No external datasets are required.

## Build

The benchmark instructions below target **Linux** with Bash, GNU Make, Git,
a C++20 compiler (GCC 11 or newer), and gflags 2.2 or newer.
On Ubuntu, install the build tools and optional compression libraries:

```bash
sudo apt-get update
sudo apt-get install -y build-essential git libgflags-dev \
  libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev

git clone https://github.com/ErosBryant/RACCOON.git
cd RACCOON
AUTO_CLEAN=1 make -j"$(nproc)" DEBUG_LEVEL=0 db_bench
./db_bench --version
```

`DEBUG_LEVEL=0` builds the release benchmark. If compiler memory usage is too
high, reduce the number of parallel jobs. See [INSTALL.md](INSTALL.md) for
additional build options and supported platforms.

## Quick start

Run all three configurations, each with a separate, fresh database:

```bash
bash script/run_all.sh
```

By default, each configuration performs **1,000,000 random writes** followed by
**1,000,000 random reads**, with one foreground thread, 16-byte keys, 500-byte
values, a 32 MiB block cache, and compression disabled. Allow several GiB of
free disk space for the three databases and their temporary compaction files.

For a short installation check:

```bash
NUM=10000 READS=10000 bash script/run_all.sh
```

The runner prints the result directory and saves a `summary.txt` containing
throughput and average latency for each configuration. It creates a new result
directory on every invocation and retains the databases for inspection.

## Benchmarks

The individual scripts default to RACCOON. Run them in this order:

```bash
# Write random keys, flush the memtable, and wait for background compaction.
bash script/fillrandom.sh

# Read the database created above, then wait for background work to finish.
bash script/readrandom.sh
```

Use `MODE=baseline`, `MODE=rtc`, or `MODE=rac` to choose the configuration.
For example, to run only the RocksDB baseline:

```bash
MODE=baseline bash script/fillrandom.sh
MODE=baseline bash script/readrandom.sh
```

| Mode | Configuration |
| --- | --- |
| `baseline` | RACCOON and RTC disabled; ordinary automatic compaction enabled. |
| `rtc` | RACCOON disabled; `read_triggered_compaction_threshold=0.01`. |
| `rac` | RACCOON enabled with `rac_design_option=3` and write-aligned selection enabled. |

The `rac` mode selects the current time-based policy, using observed rewrite
bandwidth to model compaction input cost. `rac_design_option=2` selects the
older policy and is not used by these scripts. The modeled cost is a proxy,
rather than a prediction of total compaction job time.

`fillrandom` refuses to use an existing database path. Choose a new `DB_ROOT`
for another load. For `readrandom`, keep `MODE`, `DB_ROOT`, `NUM`, `KEY_SIZE`,
and `VALUE_SIZE` consistent with the load.

## Configuration

Override settings with environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `NUM` | `1000000` | Key space and random writes per foreground thread. |
| `READS` | Same as `NUM` | Random reads per foreground thread. |
| `THREADS` | `1` | Foreground benchmark threads. |
| `KEY_SIZE` | `16` | Key size in bytes. |
| `VALUE_SIZE` | `500` | Value size in bytes. |
| `CACHE_SIZE` | `33554432` | Block cache capacity in bytes. |
| `SEED` | `1000` | Fixed workload seed for repeatable key generation. |
| `DB_BENCH` | `<repository>/db_bench` | Benchmark executable. |
| `DB_ROOT` | See below | Parent directory for the databases. |
| `OUT_DIR` | See below | Directory for benchmark logs. |
| `MODE` | `rac` | Individual-script configuration; `run_all.sh` runs all modes. |

For individual scripts, the default database path is
`benchmark-data/<MODE>` and logs go to `benchmark-results/manual/`.
`run_all.sh` creates `benchmark-results/run.XXXXXX/` and places its databases
under that directory's `db/` subdirectory. An explicit `OUT_DIR` for
`run_all.sh` must be a new directory. Set `DB_ROOT` to put databases on the
storage device being evaluated:

```bash
NUM=32000000 READS=50000000 VALUE_SIZE=500 \
  DB_ROOT=/path/to/ssd/raccoon-run-01 \
  OUT_DIR=benchmark-results/large-run-01 \
  bash script/run_all.sh
```

These are simple uniform-random microbenchmarks for checking the artifact and
exploring performance. Paper-scale results depend on the workload, database
size, hardware, and cache state; a small run may not trigger read-driven
compaction. The scripts leave the operating system page cache enabled.

## Results

Each log records the source revision, benchmark executable, exact command,
benchmark output, latency histograms, and RocksDB statistics. Look for:

| Output | Meaning |
| --- | --- |
| `fillrandom : ... micros/op ... ops/sec` | Random-write average latency and throughput. |
| `readrandom : ... micros/op ... ops/sec` | Random-read average latency and throughput. |
| Histogram and statistics sections | Latency distribution and database counters. |
| `<database>/LOG` | RocksDB's internal log and compaction details. |

`fillrandom` samples keys with replacement, so the number of distinct stored
keys can be smaller than `NUM`; random reads can include missing keys. Flush
and post-workload compaction waits are outside the reported foreground
throughput. Repeat larger experiments on the same device and with the same
settings when comparing configurations.

## Repository layout

| Path | Contents |
| --- | --- |
| [`script/`](script/) | Simple performance and artifact-check scripts. |
| [`db/`](db/) | Database implementation and compaction policies. |
| [`include/rocksdb/`](include/rocksdb/) | Public APIs and configuration options. |
| [`tools/db_bench_tool.cc`](tools/db_bench_tool.cc) | Benchmark workloads and command-line flags. |
| [`INSTALL.md`](INSTALL.md) | Detailed RocksDB build instructions. |

## License

This repository follows RocksDB's dual-license terms: [Apache 2.0](LICENSE.Apache)
or [GPLv2](COPYING). See the license files for details.
