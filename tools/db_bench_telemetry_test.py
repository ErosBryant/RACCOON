# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

"""Integration tests: python3 tools/db_bench_telemetry_test.py [db_bench]."""

import csv
import importlib.util
import math
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


BINARY = Path(sys.argv.pop(1) if len(sys.argv) > 1 else "./db_bench").resolve()


def load_ablation_plot_module():
    path = Path(__file__).resolve().parents[1] / "trace-zhao/plot_rac2_ablation.py"
    spec = importlib.util.spec_from_file_location("plot_rac2_ablation", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RAC2AblationParserTest(unittest.TestCase):
    def test_api_higher_quantiles_are_optional_and_must_be_ordered(self):
        module = load_ablation_plot_module()
        with tempfile.TemporaryDirectory(prefix="rac2-api-quantiles-") as tmp:
            path = Path(tmp) / "jobs.csv"
            self.write_jobs(path, 10)
            legacy, _ = module.parse_jobs(path)
            self.assertTrue(math.isnan(legacy["all_request_p999_us"]))
            self.assertTrue(math.isnan(legacy["all_request_p9999_us"]))
            with path.open(newline="") as source:
                reader = csv.DictReader(source)
                columns = list(reader.fieldnames) + [
                    "all_request_p999_us", "all_request_p9999_us",
                ]
                rows = list(reader)
            for row in rows:
                row.update(all_request_p999_us="12", all_request_p9999_us="13")
            with path.open("w", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=columns)
                writer.writeheader()
                writer.writerows(rows)
            metrics, _ = module.parse_jobs(path)
            self.assertEqual(metrics["all_request_p999_us"], 12)
            self.assertEqual(metrics["all_request_p9999_us"], 13)
            rows[0]["all_request_p9999_us"] = "10"
            with path.open("w", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=columns)
                writer.writeheader()
                writer.writerows(rows)
            with self.assertRaisesRegex(ValueError, "higher percentiles"):
                module.parse_jobs(path)

    def test_benchmark_histograms_merge_buckets_before_percentiles(self):
        module = load_ablation_plot_module()
        text = "\n".join((
            "Microseconds per read:",
            "Count: 9 Average: 5.0000  StdDev: 1.00",
            "Min: 0  Median: 5.0000  Max: 10",
            "Percentiles: P50: 5 P75: 7.5 P99: 9.9 P99.9: 9.99 "
            "P99.99: 9.999",
            "------------------------------------------------------",
            "(       0,      10 ]        9 100.000% 100.000%",
            "",
            "Microseconds per write:",
            "Count: 1 Average: 150.0000  StdDev: 0.00",
            "Min: 100  Median: 150.0000  Max: 200",
            "Percentiles: P50: 150 P75: 175 P99: 199 P99.9: 199.9 "
            "P99.99: 199.99",
            "------------------------------------------------------",
            "(     100,     200 ]        1 100.000% 100.000%",
        ))
        metrics = module.parse_benchmark_latency_histograms(text, 10)
        self.assertEqual(metrics["benchmark_all_count"], 10)
        self.assertAlmostEqual(metrics["benchmark_all_mean_us"], 19.5)
        self.assertEqual(metrics["benchmark_all_max_us"], 200)
        self.assertAlmostEqual(metrics["benchmark_all_p99_us"], 190)
        self.assertAlmostEqual(metrics["benchmark_all_p999_us"], 199)
        self.assertAlmostEqual(metrics["benchmark_all_p9999_us"], 199.9)
        self.assertAlmostEqual(metrics["benchmark_read_p99_us"], 9.9)
        self.assertAlmostEqual(metrics["benchmark_write_p99_us"], 199)
        self.assertEqual(metrics["benchmark_seek_count"], 0)
        self.assertTrue(math.isnan(metrics["benchmark_seek_p99_us"]))

    def test_benchmark_histograms_reject_count_mismatch_or_truncation(self):
        module = load_ablation_plot_module()
        count_mismatch = "\n".join((
            "Microseconds per read:",
            "Count: 2 Average: 5.0000  StdDev: 0.00",
            "Min: 0  Median: 5.0000  Max: 10",
            "Percentiles: P50: 5 P75: 7.5 P99: 9.9 P99.9: 9.99 "
            "P99.99: 9.999",
            "------------------------------------------------------",
            "(       0,      10 ]        1  50.000%  50.000%",
        ))
        with self.assertRaisesRegex(ValueError, "bucket count mismatch"):
            module.parse_benchmark_latency_histograms(count_mismatch, 2)

        valid_read = count_mismatch.replace("Count: 2", "Count: 1")
        with self.assertRaisesRegex(ValueError, "differs from formal_ops"):
            module.parse_benchmark_latency_histograms(valid_read, 2)

    def test_missing_benchmark_histograms_return_nan_metrics(self):
        module = load_ablation_plot_module()
        metrics = module.parse_benchmark_latency_histograms(
            "mixgraph : 10 micros/op 100000 ops/sec", 10
        )
        for prefix in ("all", "read", "write", "seek"):
            self.assertEqual(metrics[f"benchmark_{prefix}_count"], 0)
            for suffix in (
                "mean_us", "max_us", "p99_us", "p999_us", "p9999_us",
            ):
                self.assertTrue(
                    math.isnan(metrics[f"benchmark_{prefix}_{suffix}"])
                )

    def test_amplification_counter_scopes_and_missing_data(self):
        module = load_ablation_plot_module()
        text = "\n".join((
            "total_level_hits COUNT : 20",
            "total_level_misses COUNT : 80",
            "rocksdb.bytes.written COUNT : 100",
            "rocksdb.compact.write.bytes COUNT : 200",
            "rocksdb.flush.write.bytes COUNT : 50",
            "rocksdb.wal.bytes COUNT : 100",
            "total_sst_files_size_bytes COUNT : 100000",
            "estimated_live_data_size_bytes COUNT : 50000",
        ))
        metrics = module.parse_amplification_metrics(text)
        self.assertEqual(metrics["sst_read_amplification"], 5)
        self.assertEqual(metrics["sst_write_amplification"], 2.5)
        self.assertEqual(metrics["write_amplification_with_wal"], 3.5)
        self.assertEqual(metrics["space_amplification"], 2)
        # Missing compaction traffic is unknown, not zero; the template's
        # existing SST bytes are never used to dilute session write cost.
        missing = module.parse_amplification_metrics(
            text.replace("rocksdb.compact.write.bytes COUNT : 200", "")
        )
        self.assertTrue(math.isnan(missing["sst_write_amplification"]))
        empty = module.parse_amplification_metrics("")
        self.assertTrue(math.isnan(empty["sst_read_amplification"]))
        self.assertTrue(math.isnan(empty["space_amplification"]))

    def test_rocksdb_reference_preserves_core_ablation_comparisons(self):
        module = load_ablation_plot_module()
        rows = []
        for variant, throughput, p99, output in (
            ("RocksDB", 100, 20, 1000),
            ("Core", 120, 18, 1300),
            ("Full", 126, 17, 1200),
        ):
            rows.append({
                "repeat": 1, "variant": variant,
                "throughput_ops_sec": throughput, "get_p99_us": p99,
                "all_request_p99_us": p99, "read_write_p99_sum_us": p99,
                "total_output_bytes": output,
            })
        vs_core = {row["variant"]: row for row in module.paired_rows(rows, "Core")}
        vs_rocksdb = {
            row["variant"]: row for row in module.paired_rows(rows, "RocksDB")
        }
        self.assertAlmostEqual(vs_core["Full"]["throughput_pct_vs_core"], 5)
        self.assertAlmostEqual(vs_rocksdb["Full"]["throughput_pct_vs_rocksdb"], 26)
        self.assertAlmostEqual(vs_rocksdb["Core"]["output_bytes_pct_vs_rocksdb"], 30)
        legacy = [row for row in rows if row["variant"] != "RocksDB"]
        self.assertEqual(module.paired_rows(legacy, "RocksDB"), [])
        summary = module.summarize_pairs(module.paired_rows(legacy, "Core"))
        self.assertEqual({row["variant"] for row in summary}, {"Core", "Full"})

    def test_plot_accepts_legacy_and_time_guard_variant_sets(self):
        module = load_ablation_plot_module()
        legacy_six = [{"variant": name} for name in module.LEGACY_ABLATION_VARIANTS]
        legacy_seven = [{"variant": name} for name in module.LEGACY_VARIANTS]
        current_eight = [{"variant": name} for name in module.VARIANTS]
        self.assertEqual(
            module.expected_variants_for_manifest(legacy_six),
            module.LEGACY_ABLATION_VARIANTS,
        )
        self.assertEqual(
            module.expected_variants_for_manifest(legacy_seven),
            module.LEGACY_VARIANTS,
        )
        self.assertEqual(
            module.expected_variants_for_manifest(current_eight), module.VARIANTS
        )
        self.assertEqual(module.ABLATIONS["Time-Guard"], 5)
        self.assertEqual(module.SOURCE_GUARDS["Time-Guard"], "time")
        self.assertEqual(module.SOURCE_GUARDS["Full"], "none")
        self.assertEqual(module.LEGACY_SOURCE_GUARDS["Full"], "count")

    def test_source_guard_metadata_preserves_old_results_and_rejects_mixtures(self):
        module = load_ablation_plot_module()
        self.assertEqual(
            module.source_guard_model_for_entry({"variant": "Core"}), "count"
        )
        self.assertEqual(
            module.source_guard_model_for_entry(
                {"variant": "Core", "source_guard_model": "none"}
            ),
            "none",
        )
        self.assertEqual(
            module.source_guard_model_for_entry({"variant": "No-Source-Guard"}),
            "none",
        )

        with tempfile.TemporaryDirectory(prefix="rac2-mixed-guards-") as tmp:
            root = Path(tmp)
            entries = []
            for order, variant in enumerate(module.LEGACY_ABLATION_VARIANTS, 1):
                entries.append({
                    "repeat": "1", "order": str(order), "variant": variant,
                    "ablation": str(module.ABLATIONS[variant]),
                    "dp": "1" if variant == "Full" else "0",
                    "seed": "1000", "status": "0", "raw_dir": "unused",
                    "rac_enabled": "1", "cost_weight": "0.08",
                    "dp_projection_top_k": "50",
                    "dp_allow_ttl_priority": "1",
                    "source_guard_model": (
                        "count" if variant == "Full" else "none"
                    ),
                })
            with (root / "manifest.csv").open("w", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=list(entries[0]))
                writer.writeheader()
                writer.writerows(entries)
            with mock.patch.object(sys, "argv", ["plot", str(root), "--no-plot"]):
                with self.assertRaisesRegex(ValueError, "cannot mix count"):
                    module.main()

    def test_guardless_metadata_rejects_source_guard_decisions(self):
        module = load_ablation_plot_module()

        def run_case(root, model):
            raw = root / "raw"
            raw.mkdir(parents=True)
            self.write_jobs(raw / "jobs.csv", 10)
            (raw / "db_bench.log").write_text("")
            (raw / "LOG").write_text(
                "RAC2 ablation_decision cf=default epoch=1 "
                "reason=source_guard\n"
            )
            entry = {
                "repeat": "1", "order": "1", "variant": "Full",
                "ablation": "0", "dp": "1", "seed": "1000", "status": "0",
                "raw_dir": str(raw), "rac_enabled": "1",
                "source_guard_model": model,
            }
            with (root / "manifest.csv").open("w", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=list(entry))
                writer.writeheader()
                writer.writerow(entry)
            argv = ["plot", str(root), "--no-plot", "--full-only"]
            with mock.patch.object(sys, "argv", argv):
                return module.main()

        with tempfile.TemporaryDirectory(prefix="rac2-count-guard-") as tmp:
            self.assertEqual(run_case(Path(tmp), "count"), 0)
        with tempfile.TemporaryDirectory(prefix="rac2-no-guard-") as tmp:
            with self.assertRaisesRegex(ValueError, "Guardless policy"):
                run_case(Path(tmp), "none")

    def write_jobs(self, path, all_request_count=None):
        workload_end = {
            "event": "workload_end", "status": "ok", "formal_ops": "10",
            "formal_seconds": "1", "throughput_ops_sec": "10",
            "get_count": "8", "get_mean_us": "5", "get_p99_us": "9",
            "outstanding_jobs": "0", "event_time_us": "200",
            "experiment_start_us": "100",
        }
        if all_request_count is not None:
            workload_end.update(
                {
                    "all_request_count": str(all_request_count),
                    "all_request_mean_us": "6", "all_request_p99_us": "11",
                }
            )
        rows = [
            workload_end,
            {
                "event": "drain_end", "status": "ok", "formal_ops": "10",
                "outstanding_jobs": "0", "event_time_us": "200", "drain_us": "0",
            },
        ]
        fields = sorted({key for row in rows for key in row})
        with path.open("w", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    def test_full_only_summary_requires_explicit_mode_and_only_full(self):
        module = load_ablation_plot_module()
        with tempfile.TemporaryDirectory(prefix="rac2-full-only-summary-") as tmp:
            root = Path(tmp)
            raw = root / "raw"
            raw.mkdir()
            self.write_jobs(raw / "jobs.csv", 10)
            (raw / "db_bench.log").write_text("")
            (raw / "LOG").write_text("")
            entry = {
                "repeat": "1", "order": "1", "variant": "Full",
                "ablation": "0", "dp": "1", "seed": "1000", "status": "0",
                "raw_dir": str(raw), "rac_enabled": "1",
            }
            manifest = root / "manifest.csv"
            with manifest.open("w", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=list(entry))
                writer.writeheader()
                writer.writerow(entry)
            with mock.patch.object(sys, "argv", ["plot", str(root), "--no-plot"]):
                with self.assertRaisesRegex(ValueError, "each expected variant once"):
                    module.main()
            argv = ["plot", str(root), "--no-plot", "--full-only"]
            with mock.patch.object(sys, "argv", argv):
                self.assertEqual(module.main(), 0)
            with (root / "run_metrics.csv").open() as source:
                rows = list(csv.DictReader(source))
            self.assertEqual([row["variant"] for row in rows], ["Full"])
            self.assertFalse((root / "paired_vs_core.csv").exists())
            entry["variant"] = "Core"
            entry["dp"] = "0"
            with manifest.open("w", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=list(entry))
                writer.writeheader()
                writer.writerow(entry)
            with mock.patch.object(sys, "argv", argv):
                with self.assertRaisesRegex(ValueError, "each expected variant once"):
                    module.main()

    def test_combined_foreground_percentile_is_read_from_workload_boundary(self):
        module = load_ablation_plot_module()
        with tempfile.TemporaryDirectory(prefix="rac2-ablation-parser-") as tmp:
            jobs = Path(tmp) / "jobs.csv"
            self.write_jobs(jobs, 10)
            metrics, _ = module.parse_jobs(jobs)
            self.assertEqual(metrics["all_request_count"], 10)
            self.assertEqual(metrics["all_request_mean_us"], 6)
            self.assertEqual(metrics["all_request_p99_us"], 11)

            self.write_jobs(jobs, 9)
            with self.assertRaisesRegex(ValueError, "all-request latency"):
                module.parse_jobs(jobs)

            self.write_jobs(jobs)
            legacy_metrics, _ = module.parse_jobs(jobs)
            self.assertTrue(math.isnan(legacy_metrics["all_request_p99_us"]))
            with self.assertRaisesRegex(ValueError, "cannot be combined"):
                module.make_plot([legacy_metrics], Path(tmp) / "legacy.pdf", "all-requests")


class RAC2RunnerSnapshotTest(unittest.TestCase):
    def test_standard_runners_default_to_guardless_design3(self):
        scripts = Path(__file__).resolve().parents[1] / "trace-zhao/test_sh"
        runners = (
            "rac2_ablation_all_dist.sh",
            "rac2_ablation_all_random.sh",
            "rac2_ablation_prefix_dist.sh",
            "rac2_ablation_prefix_random.sh",
        )
        with tempfile.TemporaryDirectory(prefix="rac2-guardless-runners-") as tmp:
            root = Path(tmp)
            template = root / "template"
            template.mkdir()
            (template / "CURRENT").write_text("MANIFEST-000001\n")
            for filename in runners:
                runner = scripts / filename
                result = subprocess.run(
                    ["bash", str(runner), "--dry-run"],
                    env=dict(
                        os.environ,
                        RAC2_ABLATION_TEMPLATE=str(template),
                        RAC2_ABLATION_DB_BENCH="/bin/false",
                        RAC2_ABLATION_REPEATS="1",
                        RAC2_ABLATION_RESULT_ROOT=str(root / filename),
                        RAC2_ABLATION_DB_ROOT=str(root / f"{filename}.db"),
                    ),
                    capture_output=True, text=True, timeout=60,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn("source_guard=none", result.stdout)
                self.assertIn("No-Source-Guard", result.stdout)
                self.assertEqual(result.stdout.count("[DRY-RUN]"), 7)
                source = runner.read_text()
                self.assertIn(
                    "variant_source_guard=(none none none none none none disabled)",
                    source,
                )
                self.assertIn('source_guard_models=${source_guard_models}', source)
                self.assertIn('rac3_no_source_guard_v1', source)

    def test_all_dist_sweeps_only_full_and_changes_one_parameter(self):
        scripts = Path(__file__).resolve().parents[1] / "trace-zhao/test_sh"
        with tempfile.TemporaryDirectory(prefix="rac2-all-dist-sensitivity-") as tmp:
            root = Path(tmp)
            template = root / "template"
            template.mkdir()
            (template / "CURRENT").write_text("MANIFEST-000001\n")
            env = dict(
                os.environ, RAC2_ABLATION_TEMPLATE=str(template),
                RAC2_ABLATION_DB_BENCH="/bin/false", RAC2_ABLATION_REPEATS="1",
                RAC2_ABLATION_FULL_ONLY="0", RAC2_ABLATION_EXTRA_ARGS="",
                RAC2_ABLATION_DB_ROOT=str(root / "clones"),
                RAC_SENSITIVITY_RESULT_ROOT=str(root / "results"),
                RAC_SENSITIVITY_TOPK_VALUES="2 4 6 8",
                RAC_SENSITIVITY_WINDOW_VALUES="10000 50000 100000 200000",
            )
            base = subprocess.run(
                ["bash", str(scripts / "rac2_ablation_all_dist.sh"), "--dry-run"],
                env=dict(env, RAC2_ABLATION_RESULT_ROOT=str(root / "base")),
                capture_output=True, text=True, timeout=60,
            )
            self.assertEqual(base.returncode, 0, base.stdout + base.stderr)
            base_commands = [line for line in base.stdout.splitlines()
                             if line.startswith("[DRY-RUN] ")]
            self.assertEqual(len(base_commands), 7)

            def flags(line):
                return dict(arg[2:].split("=", 1)
                            for arg in shlex.split(line[len("[DRY-RUN] "):])[1:]
                            if arg.startswith("--") and "=" in arg)

            reference = flags(base_commands[0])
            for filename, parameter, values in (
                ("rac_sensitivity_topk.sh", "rac_top_k", ["2", "4", "6", "8"]),
                ("rac_sensitivity_window_size.sh", "rac_window_size",
                 ["10000", "50000", "100000", "200000"]),
            ):
                result = subprocess.run(
                    ["bash", str(scripts / filename), "--dry-run"], env=env,
                    capture_output=True, text=True, timeout=60,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                commands = [flags(line) for line in result.stdout.splitlines()
                            if line.startswith("[DRY-RUN] ")]
                self.assertEqual([command[parameter] for command in commands], values)
                self.assertEqual(len({command["db"] for command in commands}), 4)
                self.assertEqual(len({command["rac2_ablation_log"] for command in commands}), 4)
                for command in commands:
                    for key in reference.keys() - {"db", "rac2_ablation_log", parameter}:
                        self.assertEqual(command[key], reference[key], (filename, key))
                    self.assertEqual(command["enable_rac"], "true")
                    self.assertEqual(command["rac_option2_dual_purpose_selection"], "true")
                    self.assertEqual(command["rac2_ablation"], "0")
            self.assertEqual(list(root.iterdir()), [template])

    def test_sensitivity_rejects_invalid_or_duplicate_cases_before_running(self):
        scripts = Path(__file__).resolve().parents[1] / "trace-zhao/test_sh"
        for filename, variable in (
            ("rac_sensitivity_topk.sh", "RAC_SENSITIVITY_TOPK_VALUES"),
            ("rac_sensitivity_window_size.sh", "RAC_SENSITIVITY_WINDOW_VALUES"),
        ):
            for values in ("2 invalid", "2 2"):
                result = subprocess.run(
                    ["bash", str(scripts / filename), "--dry-run"],
                    env=dict(os.environ, **{variable: values}),
                    capture_output=True, text=True, timeout=60,
                )
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertNotIn("[PLAN]", result.stdout)

    def test_runner_records_detected_source_guard_generation(self):
        runner = (
            Path(__file__).resolve().parents[1]
            / "trace-zhao/test_sh/rac2_ablation_all_dist.sh"
        )
        with tempfile.TemporaryDirectory(prefix="rac2-guard-detection-") as tmp:
            root = Path(tmp)
            template = root / "template"
            template.mkdir()
            (template / "CURRENT").write_text("MANIFEST-000001\n")
            (template / "LOCK").touch()
            binary = root / "fake-db-bench"
            binary.write_text(
                "#!/usr/bin/env bash\n"
                "set -eu\n"
                "if [[ ${1:-} == --help ]]; then\n"
                "  echo 'rac2_ablation rac2_ablation_diagnostics "
                "rac2_ablation_log rac2_cost_weight "
                "rac_option2_dp_projection_top_k "
                "rac_option2_dp_allow_ttl_priority input_only_v1 "
                "overall_request_p99_v1 rac2_baseline_telemetry_v1'\n"
                "  [[ ${RAC2_FAKE_GUARDLESS:-0} == 0 ]] || "
                "echo rac3_no_source_guard_v1\n"
                "  exit 0\n"
                "fi\n"
                "run_db= jobs=\n"
                "for arg in \"$@\"; do\n"
                "  case $arg in\n"
                "    --db=*) run_db=${arg#*=} ;;\n"
                "    --rac2_ablation_log=*) jobs=${arg#*=} ;;\n"
                "  esac\n"
                "done\n"
                "mkdir -p -- \"$run_db\"\n"
                ": > \"$run_db/LOG\"\n"
                "printf '%s\\n' "
                "'event,status,formal_ops,formal_seconds,throughput_ops_sec," 
                "get_count,get_mean_us,get_p99_us,outstanding_jobs,event_time_us," 
                "experiment_start_us,drain_us,all_request_count," 
                "all_request_mean_us,all_request_p99_us' "
                "'workload_end,ok,10,1,10,8,5,9,0,200,100,,10,6,11' "
                "'drain_end,ok,10,,,,,,0,200,,0,,,' > \"$jobs\"\n"
            )
            binary.chmod(0o755)
            for marker, expected_model, expected_generation in (
                ("0", "count", "legacy_count_guard_v1"),
                ("1", "none", "guardless_v1"),
            ):
                result_root = root / f"results-{marker}"
                result = subprocess.run(
                    ["bash", str(runner)],
                    env=dict(
                        os.environ,
                        MPLBACKEND="Agg",
                        RAC2_FAKE_GUARDLESS=marker,
                        RAC2_ABLATION_TEMPLATE=str(template),
                        RAC2_ABLATION_DB_BENCH=str(binary),
                        RAC2_ABLATION_REPEATS="1",
                        RAC2_ABLATION_FULL_ONLY="1",
                        RAC2_ABLATION_RESULT_ROOT=str(result_root),
                        RAC2_ABLATION_DB_ROOT=str(root / f"clones-{marker}"),
                    ),
                    capture_output=True, text=True, timeout=60,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                with (result_root / "manifest.csv").open() as source:
                    rows = list(csv.DictReader(source))
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["source_guard_model"], expected_model)
                config = (result_root / "experiment.conf").read_text()
                self.assertIn(
                    f"source_guard_policy_generation={expected_generation}",
                    config,
                )
                self.assertEqual(
                    "historical Design 3 count guard" in result.stdout,
                    marker == "0",
                )

    def test_editing_runner_during_execution_does_not_change_loaded_commands(self):
        runner = (
            Path(__file__).resolve().parents[1]
            / "trace-zhao/test_sh/rac2_ablation_prefix_dist.sh"
        )
        with tempfile.TemporaryDirectory(prefix="rac2-runner-snapshot-") as tmp:
            root = Path(tmp)
            entry = root / "repo/trace-zhao/test_sh/runner with spaces.sh"
            entry.parent.mkdir(parents=True)
            entry.write_text(runner.read_text())
            template = root / "template"
            template.mkdir()
            (template / "CURRENT").write_text("MANIFEST-000001\n")
            shim_dir = root / "bin"
            shim_dir.mkdir()
            shim = shim_dir / "python3"
            # Mutate only the temporary runner when its first Python helper
            # starts. This reproduces edits between Bash command reads.
            shim.write_text(
                f"#!{sys.executable}\n"
                "import os, sys\n"
                "from pathlib import Path\n"
                "Path(os.environ['RAC2_SNAPSHOT_TEST_ENTRY']).write_text(')\\n' * 20000)\n"
                f"os.execv({sys.executable!r}, [{sys.executable!r}, *sys.argv[1:]])\n"
            )
            shim.chmod(0o755)
            env = dict(
                os.environ,
                PATH=str(shim_dir) + os.pathsep + os.environ.get("PATH", ""),
                RAC2_SNAPSHOT_TEST_ENTRY=str(entry),
                RAC2_ABLATION_TEMPLATE=str(template),
                RAC2_ABLATION_RESULT_ROOT=str(root / "results"),
                RAC2_ABLATION_DB_ROOT=str(root / "clones"),
                RAC2_ABLATION_DB_BENCH="/bin/false",
                RAC2_ABLATION_REPEATS="1",
            )
            result = subprocess.run(
                ["bash", str(entry), "--dry-run"], env=env,
                capture_output=True, text=True, timeout=60,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertTrue(entry.read_text().startswith(")\n"))
            self.assertIn("[DRY-RUN]", result.stdout)
            self.assertIn("source_guard=none", result.stdout)
            self.assertIn("No-Source-Guard", result.stdout)
            self.assertNotIn("Time-Guard", result.stdout)
            self.assertEqual(result.stdout.count("[DRY-RUN]"), 7)
            self.assertNotIn("--rac2_ablation=5", result.stdout)
            self.assertFalse((root / "results").exists())
            self.assertFalse((root / "clones").exists())


class OverwriteTelemetryTest(unittest.TestCase):
    def run_benchmark(self, root, benchmarks, **options):
        flags = {
            "db": root / "db",
            "benchmarks": benchmarks,
            "num": 1000,
            "writes": 120,
            "reads": 100,
            "threads": 1,
            "statistics": "true",
            "histogram": "false",
            "compression_type": "none",
            "value_size": 500,
            "progress_reports": "false",
            "ycsb_latency_log": root / "latency.log",
        }
        flags.update(options)
        result = subprocess.run(
            [str(BINARY)] + [f"--{key}={value}" for key, value in flags.items()],
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def load_samples(self, path):
        rows = []
        for line in path.read_text().splitlines():
            rows.append(dict(field.split("=", 1) for field in line.split()[1:]))
        return rows

    def check_samples(self, rows, operation, count):
        self.assertTrue(rows)
        self.assertEqual(sum(int(row[f"{operation}_ops"]) for row in rows), count)
        for row in rows:
            ops = int(row[f"{operation}_ops"])
            self.assertEqual(int(row["latency_ops"]), ops)
            other = "read" if operation == "write" else "write"
            self.assertEqual(int(row[f"{other}_ops"]), 0)
            interval = float(row["interval_seconds"])
            self.assertGreater(interval, 0)
            rate = float(row[f"{operation}_ops_per_sec"])
            self.assertTrue(math.isclose(rate, ops / interval, rel_tol=2e-5))
            self.assertEqual(rate, float(row["ops_per_sec"]))
            if ops:
                self.assertGreater(float(row[f"{operation}_latency_avg_micros"]), 0)
                self.assertGreaterEqual(float(row[f"{operation}_latency_p99_micros"]), 0)

    def test_short_batched_multithreaded_and_repeated_phases(self):
        with tempfile.TemporaryDirectory(prefix="overwrite-telemetry-") as tmp:
            root = Path(tmp)
            self.run_benchmark(
                root, "fillseq,overwrite,readrandom,overwrite",
                threads=2, batch_size=4,
            )
            paths = sorted((root / "db/rac-telemetry").glob("overwrite_hit_miss*.log"))
            self.assertEqual(len(paths), 2)
            for path in paths:
                rows = self.load_samples(path)
                self.check_samples(rows, "write", 240)
                self.assertEqual(len(rows), 1)  # Final sub-second interval.
            reads = list((root / "db/rac-telemetry").glob("readrandom_hit_miss*.log"))
            self.assertEqual(len(reads), 1)
            self.check_samples(self.load_samples(reads[0]), "read", 200)
            shared = self.load_samples(root / "latency.log")
            self.assertEqual(sum(int(row["write_ops"]) for row in shared), 480)
            self.assertEqual(sum(int(row["read_ops"]) for row in shared), 200)

    def test_periodic_and_final_intervals_on_existing_db(self):
        with tempfile.TemporaryDirectory(prefix="overwrite-telemetry-") as tmp:
            root = Path(tmp)
            self.run_benchmark(root, "fillseq")
            self.run_benchmark(
                root, "overwrite", use_existing_db="true", writes=6000,
                benchmark_write_rate_limit=1000000,
            )
            paths = list((root / "db/rac-telemetry").glob("overwrite_hit_miss*.log"))
            self.assertEqual(len(paths), 1)
            rows = self.load_samples(paths[0])
            self.check_samples(rows, "write", 6000)
            self.assertGreaterEqual(len(rows), 3)
            for row in rows[:-1]:
                self.assertAlmostEqual(float(row["interval_seconds"]), 1, delta=0.2)
            shared = self.load_samples(root / "latency.log")
            self.assertEqual(len(rows), len(shared))
            for telemetry, latency in zip(rows, shared):
                self.assertEqual(latency["workload"], "overwrite")
                for key, value in latency.items():
                    if key not in {"db", "workload"}:
                        self.assertEqual(telemetry[key], value)

    def test_rac2_ablation_reports_combined_foreground_percentiles(self):
        help_result = subprocess.run(
            [str(BINARY), "--help"], capture_output=True, text=True, timeout=60
        )
        self.assertEqual(
            help_result.returncode, 0, help_result.stdout + help_result.stderr
        )
        help_text = help_result.stdout + help_result.stderr
        self.assertIn("overall_request_p99_v1", help_text)
        self.assertIn("overall_request_tails_v1", help_text)
        self.assertIn("cost_model=input_only_v1", help_text)
        self.assertIn("rac2_baseline_telemetry_v1", help_text)

        with tempfile.TemporaryDirectory(prefix="rac2-overall-p99-") as tmp:
            root = Path(tmp)
            jobs_path = root / "jobs.csv"
            self.run_benchmark(
                root, "fillseq,mixgraph", threads=2, reads=1000,
                enable_rac="true", rac_design_option=3,
                rac2_ablation_log=jobs_path, mix_get_ratio=0.4,
                mix_put_ratio=0.4, mix_seek_ratio=0.2,
            )
            with jobs_path.open(newline="") as jobs_file:
                rows = list(csv.DictReader(jobs_file))
            workload_end = [row for row in rows if row["event"] == "workload_end"]
            drain_end = [row for row in rows if row["event"] == "drain_end"]
            self.assertEqual(len(workload_end), 1)
            self.assertEqual(len(drain_end), 1)
            row = workload_end[0]
            self.assertEqual(row["status"], "ok")
            self.assertEqual(int(row["all_request_count"]), int(row["formal_ops"]))
            self.assertGreater(int(row["all_request_count"]), 0)
            self.assertTrue(math.isfinite(float(row["all_request_mean_us"])))
            p99 = float(row["all_request_p99_us"])
            p999 = float(row["all_request_p999_us"])
            p9999 = float(row["all_request_p9999_us"])
            self.assertTrue(math.isfinite(p99))
            self.assertTrue(math.isfinite(p999))
            self.assertTrue(math.isfinite(p9999))
            self.assertLessEqual(p99, p999)
            self.assertLessEqual(p999, p9999)
            for field in (
                "all_request_count", "all_request_mean_us",
                "all_request_p99_us", "all_request_p999_us",
                "all_request_p9999_us",
            ):
                self.assertEqual(drain_end[0][field], row[field])

    def test_rac_disabled_baseline_records_foreground_and_drain(self):
        with tempfile.TemporaryDirectory(prefix="rocksdb-baseline-telemetry-") as tmp:
            root = Path(tmp)
            jobs_path = root / "jobs.csv"
            self.run_benchmark(
                root, "fillseq,mixgraph", threads=2, reads=1000,
                enable_rac="false", rac_design_option=3, rac2_ablation=0,
                rac_option2_dual_purpose_selection="false",
                read_triggered_compaction_threshold=0,
                rac2_ablation_log=jobs_path, mix_get_ratio=0.5,
                mix_put_ratio=0.5, mix_seek_ratio=0,
            )
            with jobs_path.open(newline="") as jobs_file:
                rows = list(csv.DictReader(jobs_file))
            ends = [row for row in rows if row["event"] == "workload_end"]
            drains = [row for row in rows if row["event"] == "drain_end"]
            self.assertEqual(len(ends), 1)
            self.assertEqual(len(drains), 1)
            self.assertEqual(ends[0]["status"], "ok")
            self.assertEqual(drains[0]["status"], "OK")
            self.assertEqual(int(ends[0]["formal_ops"]), 2000)
            self.assertEqual(int(ends[0]["all_request_count"]), 2000)
            p99 = float(ends[0]["all_request_p99_us"])
            p999 = float(ends[0]["all_request_p999_us"])
            p9999 = float(ends[0]["all_request_p9999_us"])
            self.assertTrue(math.isfinite(p99))
            self.assertTrue(math.isfinite(p999))
            self.assertTrue(math.isfinite(p9999))
            self.assertLessEqual(p99, p999)
            self.assertLessEqual(p999, p9999)
            for field in (
                "all_request_count", "all_request_mean_us",
                "all_request_p99_us", "all_request_p999_us",
                "all_request_p9999_us",
            ):
                self.assertEqual(drains[0][field], ends[0][field])
            self.assertTrue(all(int(row["is_rac"]) == 0 for row in rows))
            self.assertEqual(int(drains[0]["outstanding_jobs"]), 0)
            self.assertEqual(int(drains[0]["foreground_rac_output_bytes"]), 0)
            self.assertEqual(int(drains[0]["drain_rac_output_bytes"]), 0)


if __name__ == "__main__":
    unittest.main()
