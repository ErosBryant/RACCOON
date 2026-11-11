# RACCOON

RACCOON is a read-driven compaction mechanism built on RocksDB. This repository accompanies our **IEEE ICDE 2027 submission (Paper ID: 2123)**:

**Revisiting Read-Driven Compaction in LSM-Trees**

RACCOON combines three mechanisms:

- **Miss-latency profiling:** records per-SSTable hits, misses, and accumulated miss latency.
- **Efficiency-based admission:** evaluates complete compaction candidates using observed miss latency and estimated execution time.
- **Write-aligned selection (WAS):** prefers candidates that also serve projected write-side maintenance.

For build instructions, see [INSTALL.md](INSTALL.md).