#!/usr/bin/env python3
"""Derive RTC Motivation CSVs and a human-readable summary."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path


READ_RATIO_ORDER = [100, 90, 80, 70, 60, 50, 40, 30, 20, 10, 1, 0]
RTC_REASON = "ReadTriggered"


def warn(message: str) -> None:
    print(f"[WARN] {message}", file=sys.stderr)


def number(value, default=0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def integer(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    position = (len(ordered) - 1) * pct / 100.0
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def csv_list(values) -> str:
    return ";".join(str(value) for value in values)


def read_manifest(result_dir: Path) -> list[dict[str, str]]:
    path = result_dir / "manifest.csv"
    if not path.exists():
        raise SystemExit(f"Missing manifest: {path}")
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def run_uniform(run: dict) -> int:
    value = run.get("uniform", "")
    if value not in (None, ""):
        return integer(value, -1)

    # Backward compatibility for manifests created before the uniform column
    # was added. The first line of db_bench.log is the recorded command.
    benchmark_path = Path(run.get("benchmark_log", ""))
    if benchmark_path.is_file():
        with benchmark_path.open(errors="replace") as handle:
            command = handle.readline()
        match = re.search(r"--uni=([01])(?:\s|$)", command)
        if match:
            return int(match.group(1))
    return -1


def distribution_name(uniform: int) -> str:
    return {0: "Zipfian", 1: "Uniform"}.get(uniform, "Unknown")


def load_event_objects(path: Path) -> list[dict]:
    events = []
    if not path.exists():
        return events
    with path.open(errors="replace") as handle:
        for line in handle:
            brace = line.find("{")
            if brace < 0:
                continue
            try:
                event = json.loads(line[brace:])
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict) and "event" in event:
                events.append(event)
    return events


def event_time_micros(event: dict) -> int:
    for key in ("time_micros", "timestamp_micros", "time"):
        value = event.get(key)
        if value is not None:
            return integer(value)
    return 0


def parse_compactions(path: Path) -> list[dict]:
    jobs: dict[int, dict] = {}
    for event in load_event_objects(path):
        job_id = integer(event.get("job"), -1)
        if job_id < 0:
            continue
        event_type = event.get("event")
        if event_type == "compaction_started":
            inputs_by_level = {}
            sizes_by_level = {}
            for key, value in event.items():
                match = re.fullmatch(r"files_L(-?\d+)", key)
                if match and isinstance(value, list):
                    inputs_by_level[int(match.group(1))] = [integer(v) for v in value]
                match = re.fullmatch(r"file_sizes_L(-?\d+)", key)
                if match and isinstance(value, list):
                    sizes_by_level[int(match.group(1))] = [integer(v) for v in value]
            jobs[job_id] = {
                "job_id": job_id,
                "timestamp_micros": event_time_micros(event),
                "reason": str(event.get("compaction_reason", "unknown")),
                "inputs_by_level": inputs_by_level,
                "sizes_by_level": sizes_by_level,
                "input_bytes": integer(event.get("input_data_size")),
                "seed_file_number": integer(event.get("rtc_seed_file_number")),
                "seed_file_size": integer(event.get("rtc_seed_file_size")),
                "rtc_read_score": number(event.get("rtc_read_score"), math.nan),
                "outputs": [],
                "output_sizes": [],
            }
        elif event_type == "table_file_creation" and job_id in jobs:
            jobs[job_id]["outputs"].append(integer(event.get("file_number")))
            jobs[job_id]["output_sizes"].append(integer(event.get("file_size")))
        elif event_type == "compaction_finished" and job_id in jobs:
            job = jobs[job_id]
            job["finish_timestamp_micros"] = event_time_micros(event)
            job["duration_micros"] = integer(event.get("compaction_time_micros"))
            job["output_level"] = integer(event.get("output_level"), -1)
            job["output_bytes"] = integer(event.get("total_output_size"))
    complete = []
    for job in jobs.values():
        if "output_level" not in job:
            continue
        levels = list(job["inputs_by_level"])
        output_level = job["output_level"]
        source_candidates = [level for level in levels if level != output_level]
        job["source_level"] = source_candidates[0] if source_candidates else (levels[0] if levels else -1)
        job["all_inputs"] = [
            file_number
            for level in levels
            for file_number in job["inputs_by_level"].get(level, [])
        ]
        if job["input_bytes"] == 0:
            job["input_bytes"] = sum(
                size for sizes in job["sizes_by_level"].values() for size in sizes
            )
        complete.append(job)
    return sorted(complete, key=lambda job: (job["timestamp_micros"], job["job_id"]))


def parse_benchmark(path: Path, read_ratio: int, operations: int, value_size: int, key_size: int) -> dict:
    text = path.read_text(errors="replace") if path.exists() else ""
    throughput = math.nan
    benchmark_pattern = re.compile(
        r"^(?:readrandomwriterandom|mixgraph)\s*:\s*.*?"
        r"([0-9]+(?:\.[0-9]+)?)\s+ops/sec",
        re.MULTILINE,
    )
    matches = benchmark_pattern.findall(text)
    if matches:
        throughput = float(matches[-1])

    p99_read_micros = math.nan
    read_histogram = text.find("Microseconds per read:")
    if read_histogram >= 0:
        histogram_window = text[read_histogram : read_histogram + 1600]
        p99_match = re.search(r"P99(?:\.0+)?\s*[:=]\s*([0-9.eE+-]+)", histogram_window)
        if p99_match:
            p99_read_micros = float(p99_match.group(1))
    if math.isnan(p99_read_micros) and read_ratio > 0:
        p99_matches = re.findall(r"P99(?:\.0+)?\s*[:=]\s*([0-9.eE+-]+)", text)
        if p99_matches:
            p99_read_micros = float(p99_matches[-1])

    ra_matches = re.findall(r"^RA\s*:\s*([0-9.eE+-]+)", text, re.MULTILINE)
    ra_valid = not re.search(r"^RA_VALID\s*:\s*false", text, re.MULTILINE)
    ra = float(ra_matches[-1]) if ra_matches and ra_valid else math.nan
    delays = re.findall(r"total-delays:\s*([0-9]+)", text)
    stops = re.findall(r"total-stops:\s*([0-9]+)", text)
    write_stall_delays = integer(delays[-1]) if delays else 0
    write_stall_stops = integer(stops[-1]) if stops else 0
    operation_summaries = re.findall(
        r"\(\s*reads:([0-9]+)\s+writes:([0-9]+)\s+total:([0-9]+)", text
    )
    if operation_summaries:
        write_ops = integer(operation_summaries[-1][1])
    else:
        write_ops = round(operations * (100 - read_ratio) / 100.0)
    logical_write_bytes = write_ops * (key_size + value_size)
    return {
        "throughput_ops_sec": throughput,
        "p99_read_latency_micros": p99_read_micros,
        "read_amplification": ra,
        "logical_write_bytes": logical_write_bytes,
        "write_stall_delays": write_stall_delays,
        "write_stall_stops": write_stall_stops,
    }


def parse_device_util(path: Path, device: str) -> float:
    if not path.exists() or not path.stat().st_size:
        return math.nan
    values = []
    with path.open(errors="replace") as handle:
        for line in handle:
            fields = line.split()
            if len(fields) < 2:
                continue
            if device and fields[0] != device:
                continue
            if not device and not re.match(r"^(sd|nvme|vd|xvd|dm-)", fields[0]):
                continue
            try:
                values.append(float(fields[-1]))
            except ValueError:
                pass
    return statistics.mean(values) if values else math.nan


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def derive_a(
    result_dir: Path, manifest: list[dict]
) -> tuple[list[dict], dict[int, list[dict]]]:
    rows = []
    compactions_by_run = {}
    for run_index, run in enumerate(manifest):
        read_ratio = integer(run["read_ratio"])
        mode = run["mode"]
        base_mode = run.get("base_mode") or mode
        operations = integer(run["operations"])
        uniform = run_uniform(run)
        value_size = integer(run.get("value_size"), 1000)
        benchmark = parse_benchmark(
            Path(run["benchmark_log"]), read_ratio, operations,
            value_size=value_size,
            key_size=integer(run.get("key_size"), 16),
        )
        jobs = parse_compactions(Path(run["rocksdb_log"]))
        for job in jobs:
            job["read_ratio"] = read_ratio
            job["write_ratio"] = integer(run["write_ratio"])
            job["mode"] = mode
            job["base_mode"] = base_mode
            job["operations"] = operations
            job["uniform"] = uniform
            job["value_size"] = value_size
            job["db_path"] = run["db_path"]
            job["run_index"] = run_index
        compactions_by_run[run_index] = jobs
        rtc_jobs = [job for job in jobs if job["reason"] == RTC_REASON]
        normal_jobs = [job for job in jobs if job["reason"] != RTC_REASON]
        compaction_input_bytes = sum(job["input_bytes"] for job in jobs)
        compaction_output_bytes = sum(job["output_bytes"] for job in jobs)
        logical_write_bytes = benchmark["logical_write_bytes"]
        write_amplification = (
            (logical_write_bytes + compaction_output_bytes) / logical_write_bytes
            if logical_write_bytes
            else math.nan
        )
        rows.append(
            {
                "read_ratio": read_ratio,
                "write_ratio": integer(run["write_ratio"]),
                "mode": mode,
                "base_mode": base_mode,
                "operations": operations,
                "uniform": uniform,
                "value_size": value_size,
                "rac_dual_purpose_selection": run.get(
                    "rac_dual_purpose_selection", ""
                ),
                "rac_window_size": run.get("rac_window_size", ""),
                "rac_high_miss_ratio_threshold": run.get(
                    "rac_high_miss_ratio_threshold", ""
                ),
                "throughput_ops_sec": benchmark["throughput_ops_sec"],
                "p99_read_latency_micros": benchmark["p99_read_latency_micros"],
                "read_amplification": benchmark["read_amplification"],
                "compaction_input_bytes": compaction_input_bytes,
                "compaction_output_bytes": compaction_output_bytes,
                "rtc_compaction_count": len(rtc_jobs),
                "normal_compaction_count": len(normal_jobs),
                "logical_write_bytes": logical_write_bytes,
                "write_amplification": write_amplification,
                "write_stall_delays": benchmark["write_stall_delays"],
                "write_stall_stops": benchmark["write_stall_stops"],
                "device_util_percent": parse_device_util(Path(run["iostat_log"]), run.get("device", "")),
                "status": integer(run["status"]),
            }
        )
    rows.sort(key=lambda row: (
        row["value_size"],
        row["uniform"],
        row["operations"],
        READ_RATIO_ORDER.index(row["read_ratio"])
        if row["read_ratio"] in READ_RATIO_ORDER else 999,
        row["mode"],
    ))
    return rows, compactions_by_run


def derive_b(result_dir: Path, jobs: list[dict]) -> list[dict]:
    rows = []
    for job in jobs:
        if job["reason"] != RTC_REASON:
            continue
        source_level = job["source_level"]
        output_level = job["output_level"]
        source_files = job["inputs_by_level"].get(source_level, [])
        source_sizes = job["sizes_by_level"].get(source_level, [])
        target_files = job["inputs_by_level"].get(output_level, []) if output_level != source_level else []
        target_sizes = job["sizes_by_level"].get(output_level, []) if output_level != source_level else []
        seed_size = job["seed_file_size"]
        rows.append(
            {
                "job_id": job["job_id"],
                "uniform": job.get("uniform", -1),
                "value_size": job.get("value_size", 0),
                "operations": job.get("operations", 0),
                "read_ratio": job.get("read_ratio", -1),
                "write_ratio": job.get("write_ratio", -1),
                "timestamp_micros": job["timestamp_micros"],
                "seed_file_number": job["seed_file_number"],
                "seed_file_size": seed_size,
                "rtc_read_score": job["rtc_read_score"],
                "source_level": source_level,
                "output_level": output_level,
                "source_input_file_ids": csv_list(source_files),
                "target_input_file_ids": csv_list(target_files),
                "all_input_file_ids": csv_list(job["all_inputs"]),
                "source_input_bytes": sum(source_sizes),
                "target_input_bytes": sum(target_sizes),
                "total_physical_input_bytes": job["input_bytes"],
                "output_file_ids": csv_list(job["outputs"]),
                "output_bytes": job["output_bytes"],
                "duration_micros": job["duration_micros"],
                "expansion_factor": job["input_bytes"] / seed_size if seed_size else math.nan,
                "level_pair": f"L{source_level}->L{output_level}",
            }
        )
    return rows


def summarize_values(values: list[float], prefix: str) -> dict:
    return {
        f"{prefix}_count": len(values),
        f"{prefix}_min": min(values) if values else math.nan,
        f"{prefix}_median": percentile(values, 50),
        f"{prefix}_p90": percentile(values, 90),
        f"{prefix}_max": max(values) if values else math.nan,
    }


def derive_c(
    rtc_runs: list[dict],
) -> tuple[dict, dict[tuple[int, int, int, int], dict]]:
    all_values = []
    summaries_by_ratio = {}
    for run in rtc_runs:
        read_ratio = integer(run["read_ratio"])
        operations = integer(run["operations"])
        uniform = run_uniform(run)
        value_size = integer(run.get("value_size"), 1000)
        source_text = run.get("rtc_sst_log", "")
        source_path = Path(source_text) if source_text else None
        final_by_sst = {}
        if source_path and source_path.exists() and source_path.stat().st_size:
            with source_path.open(newline="") as handle:
                for observation in csv.DictReader(handle):
                    key = (observation["column_family_id"], observation["file_number"])
                    interval_id = integer(observation["interval_id"])
                    previous = final_by_sst.get(key)
                    if previous is None or interval_id >= previous[0]:
                        final_by_sst[key] = (
                            interval_id,
                            integer(observation["miss_count"]),
                            integer(observation["accumulated_miss_latency_nanos"]),
                        )
        values = [
            latency_nanos / (1000.0 * miss_count)
            for _, miss_count, latency_nanos in final_by_sst.values()
            if miss_count > 0
        ]
        summaries_by_ratio[
            (value_size, uniform, operations, read_ratio)
        ] = summarize_values(values, "miss_latency_us")
        all_values.extend(values)
    return summarize_values(all_values, "miss_latency_us"), summaries_by_ratio


def derive_d(result_dir: Path, jobs: list[dict]) -> tuple[list[dict], list[dict]]:
    rtc_jobs = [
        job for job in jobs
        if job.get("base_mode", job.get("mode")) == "rtc"
        and job["reason"] == RTC_REASON
    ]
    conventional = [
        job for job in jobs
        if job.get("base_mode", job.get("mode")) == "rtc"
        and job["reason"] != RTC_REASON
    ]
    overlap_rows = []
    job_rows = []
    for rtc in rtc_jobs:
        first_rewrite_by_output = {}
        for output_id, output_size in zip(rtc["outputs"], rtc["output_sizes"]):
            for normal in conventional:
                if normal.get("db_path") != rtc.get("db_path"):
                    continue
                if normal["timestamp_micros"] <= rtc["timestamp_micros"]:
                    continue
                if output_id in normal["all_inputs"]:
                    first_rewrite_by_output[output_id] = normal
                    overlap_rows.append(
                        {
                            "uniform": rtc.get("uniform", -1),
                            "value_size": rtc.get("value_size", 0),
                            "read_ratio": rtc.get("read_ratio", -1),
                            "operations": rtc.get("operations", 0),
                            "write_ratio": rtc.get("write_ratio", -1),
                            "rtc_job_id": rtc["job_id"],
                            "rtc_timestamp_micros": rtc["timestamp_micros"],
                            "rtc_level_pair": f"L{rtc['source_level']}->L{rtc['output_level']}",
                            "rtc_output_file_id": output_id,
                            "rtc_output_file_bytes": output_size,
                            "normal_job_id": normal["job_id"],
                            "normal_read_ratio": normal.get("read_ratio", -1),
                            "normal_timestamp_micros": normal["timestamp_micros"],
                            "normal_reason": normal["reason"],
                            "normal_level_pair": f"L{normal['source_level']}->L{normal['output_level']}",
                            "time_to_rewrite_seconds": (normal["timestamp_micros"] - rtc["timestamp_micros"]) / 1_000_000.0,
                        }
                    )
                    break
        rewritten_bytes = sum(
            size for output_id, size in zip(rtc["outputs"], rtc["output_sizes"]) if output_id in first_rewrite_by_output
        )
        times = [
            (job["timestamp_micros"] - rtc["timestamp_micros"]) / 1_000_000.0
            for job in first_rewrite_by_output.values()
        ]
        job_rows.append(
            {
                "uniform": rtc.get("uniform", -1),
                "value_size": rtc.get("value_size", 0),
                "read_ratio": rtc.get("read_ratio", -1),
                "operations": rtc.get("operations", 0),
                "write_ratio": rtc.get("write_ratio", -1),
                "rtc_job_id": rtc["job_id"],
                "rtc_timestamp_micros": rtc["timestamp_micros"],
                "rtc_level_pair": f"L{rtc['source_level']}->L{rtc['output_level']}",
                "rtc_output_file_ids": csv_list(rtc["outputs"]),
                "rtc_output_bytes": rtc["output_bytes"],
                "rewritten_output_file_ids": csv_list(first_rewrite_by_output),
                "rewritten_output_file_count": len(first_rewrite_by_output),
                "rewritten_output_bytes": rewritten_bytes,
                "output_rewritten": 1 if first_rewrite_by_output else 0,
                "rewritten_byte_fraction": rewritten_bytes / rtc["output_bytes"] if rtc["output_bytes"] else 0,
                "first_time_to_rewrite_seconds": min(times) if times else math.nan,
            }
        )
    return job_rows, overlap_rows


def human_bytes(value: int) -> str:
    if value == 0:
        return "0 B"
    for unit, divisor in (("GiB", 1024 ** 3), ("MiB", 1024 ** 2), ("KiB", 1024)):
        if value >= divisor:
            return f"{value / divisor:.2f} {unit}"
    return f"{value} B"


def format_micros(value: float) -> str:
    return "没有数据" if math.isnan(value) else f"{value:.3f} µs/miss"


def rounded(value: float, digits: int = 3):
    return "" if math.isnan(value) else round(value, digits)


def build_workload_rows(
    manifest: list[dict],
    a_rows: list[dict],
    b_rows: list[dict],
    c_by_ratio: dict[tuple[int, int, int, int], dict],
    d_jobs: list[dict],
) -> list[dict]:
    operation_values = sorted({integer(run["operations"]) for run in manifest})
    value_size_values = sorted({integer(run.get("value_size"), 1000) for run in manifest})
    uniform_values = sorted({run_uniform(run) for run in manifest})
    a_by_run = {
        (
            integer(row["value_size"]), integer(row["uniform"]),
            integer(row["operations"]),
            integer(row["read_ratio"]), row["base_mode"]
        ): row
        for row in a_rows
        if row["base_mode"] != "rac"
    }
    rows = []
    for value_size in value_size_values:
        for uniform in uniform_values:
            for operations in operation_values:
                ratios = {
                    integer(run["read_ratio"])
                    for run in manifest
                    if integer(run.get("value_size"), 1000) == value_size
                    and run_uniform(run) == uniform
                    and integer(run["operations"]) == operations
                }
                ordered_ratios = [ratio for ratio in READ_RATIO_ORDER if ratio in ratios]
                ordered_ratios.extend(sorted(ratios - set(ordered_ratios), reverse=True))
                for ratio in ordered_ratios:
                    baseline = a_by_run.get(
                        (value_size, uniform, operations, ratio, "baseline"), {}
                    )
                    rtc = a_by_run.get(
                        (value_size, uniform, operations, ratio, "rtc"), {}
                    )
                    rac_candidates = [
                        row for row in a_rows
                        if integer(row["value_size"]) == value_size
                        and integer(row["uniform"]) == uniform
                        and integer(row["operations"]) == operations
                        and integer(row["read_ratio"]) == ratio
                        and row["base_mode"] == "rac"
                    ]
                    rac = rac_candidates[0] if len(rac_candidates) == 1 else {}
                    rtc_jobs = [
                        row for row in b_rows
                        if integer(row["value_size"]) == value_size
                        and integer(row["uniform"]) == uniform
                        and integer(row["operations"]) == operations
                        and integer(row["read_ratio"]) == ratio
                    ]
                    rewrite_jobs = [
                        row for row in d_jobs
                        if integer(row["value_size"]) == value_size
                        and integer(row["uniform"]) == uniform
                        and integer(row["operations"]) == operations
                        and integer(row["read_ratio"]) == ratio
                    ]
                    expansions = [
                        number(row["expansion_factor"], math.nan)
                        for row in rtc_jobs
                        if not math.isnan(number(row["expansion_factor"], math.nan))
                    ]
                    expansion = summarize_values(expansions, "expansion")
                    miss = c_by_ratio.get(
                        (value_size, uniform, operations, ratio),
                        summarize_values([], "miss_latency_us"),
                    )
                    level_counts = {}
                    for job in rtc_jobs:
                        level_pair = job["level_pair"]
                        level_counts[level_pair] = level_counts.get(level_pair, 0) + 1
                    rows.append({
                        "value_size": value_size,
                        "uniform": uniform,
                        "distribution": distribution_name(uniform),
                        "operations": operations,
                        "read_ratio": ratio,
                        "write_ratio": 100 - ratio,
                        "baseline_ops_sec": rounded(number(baseline.get("throughput_ops_sec"), math.nan), 2),
                        "rtc_ops_sec": rounded(number(rtc.get("throughput_ops_sec"), math.nan), 2),
                        "rac_ops_sec": rounded(number(rac.get("throughput_ops_sec"), math.nan), 2),
                        "baseline_p99_read_us": rounded(number(baseline.get("p99_read_latency_micros"), math.nan)),
                        "rtc_p99_read_us": rounded(number(rtc.get("p99_read_latency_micros"), math.nan)),
                        "rac_p99_read_us": rounded(number(rac.get("p99_read_latency_micros"), math.nan)),
                        "baseline_read_amp": rounded(number(baseline.get("read_amplification"), math.nan)),
                        "rtc_read_amp": rounded(number(rtc.get("read_amplification"), math.nan)),
                        "rac_read_amp": rounded(number(rac.get("read_amplification"), math.nan)),
                        "rtc_compactions": len(rtc_jobs),
                        "rtc_physical_input_gib": rounded(
                            sum(integer(job["total_physical_input_bytes"]) for job in rtc_jobs) / (1024 ** 3)
                        ),
                        "rtc_level_pairs": ";".join(
                            f"{pair}:{count}" for pair, count in sorted(level_counts.items())
                        ),
                        "expansion_median": rounded(expansion["expansion_median"]),
                        "expansion_p90": rounded(expansion["expansion_p90"]),
                        "expansion_max": rounded(expansion["expansion_max"]),
                        "sst_with_misses": integer(miss["miss_latency_us_count"]),
                        "miss_latency_min_us": rounded(miss["miss_latency_us_min"]),
                        "miss_latency_median_us": rounded(miss["miss_latency_us_median"]),
                        "miss_latency_max_us": rounded(miss["miss_latency_us_max"]),
                        "rewritten_rtc_jobs": sum(integer(job["output_rewritten"]) for job in rewrite_jobs),
                        "rewritten_sst_files": sum(
                            integer(job["rewritten_output_file_count"]) for job in rewrite_jobs
                        ),
                        "rewritten_gib": rounded(
                            sum(integer(job["rewritten_output_bytes"]) for job in rewrite_jobs) / (1024 ** 3)
                        ),
                    })
    return rows


def write_workload_csv(result_dir: Path, rows: list[dict]) -> Path:
    path = result_dir / "motivation_workload_summary.csv"
    fields = list(rows[0]) if rows else ["read_ratio", "write_ratio"]
    write_csv(path, fields, rows)
    return path


def write_rac_variants_csv(result_dir: Path, a_rows: list[dict]) -> tuple[Path, list[dict]]:
    rows = [
        row
        for row in a_rows
        if row.get("base_mode", row["mode"]) == "rac"
    ]
    fields = [
        "value_size",
        "uniform",
        "read_ratio",
        "write_ratio",
        "operations",
        "mode",
        "rac_dual_purpose_selection",
        "rac_window_size",
        "rac_high_miss_ratio_threshold",
        "throughput_ops_sec",
        "p99_read_latency_micros",
        "read_amplification",
        "compaction_input_bytes",
        "compaction_output_bytes",
        "write_amplification",
        "status",
    ]
    path = result_dir / "motivation_rac_variants.csv"
    write_csv(path, fields, rows)
    return path, rows


def display_number(value, digits: int = 2) -> str:
    if value == "" or value is None:
        return "—"
    return f"{number(value):.{digits}f}"


def write_markdown_summary(
    result_dir: Path, rows: list[dict], c_summary: dict, rac_rows: list[dict]
) -> Path:
    report_path = result_dir / "motivation_summary.md"
    with report_path.open("w") as handle:
        handle.write("# RTC Motivation 精简汇总\n\n")
        handle.write("详细原始证据保留在 `raw/`；日常分析只需查看本文件和 "
                     "`motivation_workload_summary.csv`。\n\n")
        handle.write("## Baseline / RTC / RAC 性能\n\n")
        handle.write(
            "| Value bytes | Distribution | Operations | Workload | Baseline ops/s | RTC ops/s | RAC ops/s | "
            "Baseline P99 read µs | RTC P99 read µs | RAC P99 read µs |\n"
        )
        handle.write("|---:|---|---:|---|---:|---:|---:|---:|---:|---:|\n")
        for row in rows:
            handle.write(
                f"| {row['value_size']} | "
                f"{row['distribution']} (`uni={row['uniform']}`) | "
                f"{row['operations']} | "
                f"Read{row['read_ratio']}/Write{row['write_ratio']} | "
                f"{display_number(row['baseline_ops_sec'])} | "
                f"{display_number(row['rtc_ops_sec'])} | "
                f"{display_number(row['rac_ops_sec'])} | "
                f"{display_number(row['baseline_p99_read_us'])} | "
                f"{display_number(row['rtc_p99_read_us'])} | "
                f"{display_number(row['rac_p99_read_us'])} |\n"
            )

        if rac_rows:
            handle.write("\n## RAC parameter variants\n\n")
            handle.write(
                "| Value bytes | Distribution | Operations | Workload | RAC config | Dual-purpose | Window | High-miss threshold | "
                "Ops/s | P99 read µs | Read amp | Compaction input GiB | "
                "Compaction output GiB |\n"
            )
            handle.write("|---:|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
            for row in rac_rows:
                handle.write(
                    f"| {row['value_size']} | "
                    f"{distribution_name(integer(row['uniform'], -1))} (`uni={row['uniform']}`) | "
                    f"{row['operations']} | Read{row['read_ratio']}/Write{row['write_ratio']} | "
                    f"{row['mode']} | {row['rac_dual_purpose_selection']} | "
                    f"{row['rac_window_size']} | "
                    f"{row['rac_high_miss_ratio_threshold']} | "
                    f"{display_number(row['throughput_ops_sec'])} | "
                    f"{display_number(row['p99_read_latency_micros'])} | "
                    f"{display_number(row['read_amplification'], 3)} | "
                    f"{display_number(number(row['compaction_input_bytes']) / (1024 ** 3))} | "
                    f"{display_number(number(row['compaction_output_bytes']) / (1024 ** 3))} |\n"
                )

        handle.write("\n## 各 workload 的 RTC physical amplification\n\n")
        for row in rows:
            ratio = integer(row["read_ratio"])
            jobs = integer(row["rtc_compactions"])
            prefix = (
                f"- Value {row['value_size']} bytes, "
                f"{row['distribution']} (`uni={row['uniform']}`), "
                f"Operations {row['operations']}, "
                f"Read{ratio}/Write{100 - ratio}"
            )
            if jobs == 0:
                handle.write(f"{prefix}：没有 RTC compaction。\n")
                continue
            handle.write(
                f"{prefix}：{jobs} 次 RTC，处理约 "
                f"{display_number(row['rtc_physical_input_gib'])} GiB；"
                f"expansion 中位数 {display_number(row['expansion_median'])}×，"
                f"P90 {display_number(row['expansion_p90'])}×，"
                f"最高 {display_number(row['expansion_max'])}×。\n"
            )

        handle.write("\n## 每个 SST 的 miss 平均延迟\n\n")
        handle.write(
            "口径：每个 SST 在整个 workload 内累计 "
            "`accumulated_miss_latency_nanos / miss_count`。\n\n"
        )
        handle.write("| Value bytes | Distribution | Operations | Workload | SST 数 | 最低 µs/miss | 中位 µs/miss | 最高 µs/miss |\n")
        handle.write("|---:|---|---:|---|---:|---:|---:|---:|\n")
        for row in rows:
            handle.write(
                f"| {row['value_size']} | "
                f"{row['distribution']} (`uni={row['uniform']}`) | "
                f"{row['operations']} | "
                f"Read{row['read_ratio']}/Write{row['write_ratio']} | "
                f"{row['sst_with_misses']} | {display_number(row['miss_latency_min_us'], 3)} | "
                f"{display_number(row['miss_latency_median_us'], 3)} | "
                f"{display_number(row['miss_latency_max_us'], 3)} |\n"
            )
        handle.write(
            f"| 全部 | 全部 | — | 全部 | {integer(c_summary['miss_latency_us_count'])} | "
            f"{display_number(rounded(c_summary['miss_latency_us_min']), 3)} | "
            f"{display_number(rounded(c_summary['miss_latency_us_median']), 3)} | "
            f"{display_number(rounded(c_summary['miss_latency_us_max']), 3)} |\n"
        )

        handle.write("\n## RTC 输出被后续普通 compaction 重写\n\n")
        handle.write("| Value bytes | Distribution | Operations | Workload | RTC 次数 | 被重写 RTC job | 被重写 SST | 重写 GiB |\n")
        handle.write("|---:|---|---:|---|---:|---:|---:|---:|\n")
        for row in rows:
            handle.write(
                f"| {row['value_size']} | "
                f"{row['distribution']} (`uni={row['uniform']}`) | "
                f"{row['operations']} | "
                f"Read{row['read_ratio']}/Write{row['write_ratio']} | "
                f"{row['rtc_compactions']} | {row['rewritten_rtc_jobs']} | "
                f"{row['rewritten_sst_files']} | {display_number(row['rewritten_gib'])} |\n"
            )
        handle.write(
            "\n普通 compaction 指 `compaction_reason != ReadTriggered`；"
            "重写按 RTC 输出 SST 文件号与后续 compaction 输入文件号匹配。\n"
        )
    return report_path


def remove_legacy_derived_files(result_dir: Path) -> None:
    for name in (
        "motivation_A.csv",
        "motivation_B_jobs.csv",
        "motivation_C_sst.csv",
        "motivation_C_summary.csv",
        "motivation_D_jobs.csv",
        "motivation_D_overlap.csv",
        "analysis_notes.txt",
    ):
        (result_dir / name).unlink(missing_ok=True)


def import_plotting():
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise SystemExit("matplotlib is required for plot mode") from exc
    return plt


def save_plot(plt, output_base: Path) -> None:
    plt.tight_layout()
    plt.savefig(output_base.with_suffix(".png"), dpi=180)
    plt.savefig(output_base.with_suffix(".pdf"))
    plt.close()


def grouped_series(rows: list[dict], field: str):
    grouped = defaultdict(dict)
    for row in rows:
        grouped[row["mode"]][row["read_ratio"]] = row[field]
    return grouped


def plot_a(plt, plot_dir: Path, rows: list[dict]) -> None:
    specs = [
        ("A1_throughput", "throughput_ops_sec", "Throughput (ops/s)"),
        ("A2_p99_read_latency", "p99_read_latency_micros", "P99 read latency (us)"),
        ("A3_read_amplification", "read_amplification", "Read amplification"),
    ]
    for filename, field, ylabel in specs:
        plt.figure(figsize=(7, 4.2))
        for mode, values in grouped_series(rows, field).items():
            xs = [ratio for ratio in READ_RATIO_ORDER if ratio in values and not math.isnan(number(values[ratio], math.nan))]
            ys = [values[ratio] for ratio in xs]
            if xs:
                plt.plot(xs, ys, marker="o", label=mode)
        plt.gca().invert_xaxis()
        plt.xlabel("Read ratio (%)")
        plt.ylabel(ylabel)
        plt.grid(alpha=0.3)
        plt.legend()
        save_plot(plt, plot_dir / filename)

    plt.figure(figsize=(7, 4.2))
    for mode, values in grouped_series(rows, "compaction_input_bytes").items():
        xs = [ratio for ratio in READ_RATIO_ORDER if ratio in values]
        plt.plot(xs, [values[x] / (1024 ** 3) for x in xs], marker="o", label=f"{mode} input")
    for mode, values in grouped_series(rows, "compaction_output_bytes").items():
        xs = [ratio for ratio in READ_RATIO_ORDER if ratio in values]
        plt.plot(xs, [values[x] / (1024 ** 3) for x in xs], marker="x", linestyle="--", label=f"{mode} output")
    plt.gca().invert_xaxis()
    plt.xlabel("Read ratio (%)")
    plt.ylabel("Compaction I/O (GiB)")
    plt.grid(alpha=0.3)
    plt.legend(ncol=2, fontsize=8)
    save_plot(plt, plot_dir / "A4_compaction_io")


def ecdf(values: list[float]):
    ordered = sorted(values)
    return ordered, [(index + 1) / len(ordered) for index in range(len(ordered))]


def plot_b(plt, plot_dir: Path, rows: list[dict]) -> None:
    if not rows:
        warn("No RTC compactions in the 50/50 run; skipping Figure B plots")
        return
    values = [number(row["expansion_factor"]) for row in rows]
    xs, ys = ecdf(values)
    plt.figure(figsize=(6.5, 4.2)); plt.plot(xs, ys); plt.xlabel("Expansion factor X(C)/seed size"); plt.ylabel("CDF"); plt.grid(alpha=0.3)
    save_plot(plt, plot_dir / "B1_expansion_factor_cdf")

    pairs = sorted({row["level_pair"] for row in rows})
    data = [[number(row["expansion_factor"]) for row in rows if row["level_pair"] == pair] for pair in pairs]
    plt.figure(figsize=(max(6.5, len(pairs) * 1.2), 4.2)); plt.boxplot(data, labels=pairs, showfliers=True); plt.ylabel("Expansion factor"); plt.xlabel("Level pair"); plt.grid(axis="y", alpha=0.3)
    save_plot(plt, plot_dir / "B2_expansion_by_level_pair")

    physical = [[number(row["total_physical_input_bytes"]) / (1024 ** 2) for row in rows if row["level_pair"] == pair] for pair in pairs]
    plt.figure(figsize=(max(6.5, len(pairs) * 1.2), 4.2)); plt.boxplot(physical, labels=pairs, showfliers=True); plt.ylabel("Physical input (MiB)"); plt.xlabel("Level pair"); plt.grid(axis="y", alpha=0.3)
    save_plot(plt, plot_dir / "B3_physical_input_by_level_pair")


def plot_c(plt, plot_dir: Path, rows: list[dict]) -> None:
    active = [row for row in rows if row["miss_count"] > 0]
    if not active:
        warn("No sampled SST miss-cost observations; skipping Figure C plots")
        return
    plt.figure(figsize=(6.5, 4.2))
    plt.scatter([row["rtc_read_score"] for row in active], [row["T_s_micros"] for row in active], s=9, alpha=0.55)
    plt.axvline(0.01, color="red", linestyle="--", label="RTC threshold 0.01")
    plt.xlabel("RTC read score"); plt.ylabel("T_s: unsuccessful-probe latency (us/interval)"); plt.grid(alpha=0.3); plt.legend()
    save_plot(plt, plot_dir / "C1_read_score_vs_miss_cost")

    eligible = [row["T_s_micros"] for row in active if row["rtc_eligible"]]
    if eligible:
        xs, ys = ecdf(eligible)
        plt.figure(figsize=(6.5, 4.2)); plt.plot(xs, ys); plt.xlabel("T_s among RTC-eligible SST observations (us)"); plt.ylabel("ECDF"); plt.grid(alpha=0.3)
        save_plot(plt, plot_dir / "C2_eligible_miss_cost_ecdf")


def plot_d(plt, plot_dir: Path, jobs: list[dict], overlaps: list[dict]) -> None:
    if not jobs:
        warn("No RTC output lineage in the 50/50 run; skipping Figure D plots")
        return
    representative = next((row for row in jobs if row["output_rewritten"]), jobs[0])
    representative_overlaps = [row for row in overlaps if row["rtc_job_id"] == representative["rtc_job_id"]]
    plt.figure(figsize=(7, 2.6)); plt.scatter([0], [0], marker="D", s=70, label=f"RTC job {representative['rtc_job_id']}")
    for row in representative_overlaps:
        x = number(row["time_to_rewrite_seconds"]); plt.plot([0, x], [0, 0], color="gray", alpha=0.5); plt.scatter([x], [0], marker="o", label=f"normal job {row['normal_job_id']}")
    plt.yticks([]); plt.xlabel("Seconds after RTC compaction"); plt.legend(fontsize=7, ncol=2); plt.grid(axis="x", alpha=0.3)
    save_plot(plt, plot_dir / "D1_representative_timeline")

    rewritten_jobs = sum(integer(row["output_rewritten"]) for row in jobs)
    plt.figure(figsize=(5, 4)); plt.bar(["rewritten", "not rewritten"], [rewritten_jobs, len(jobs) - rewritten_jobs]); plt.ylabel("RTC jobs")
    save_plot(plt, plot_dir / "D2_rewritten_job_fraction")

    total_bytes = sum(integer(row["rtc_output_bytes"]) for row in jobs)
    rewritten_bytes = sum(integer(row["rewritten_output_bytes"]) for row in jobs)
    plt.figure(figsize=(5, 4)); plt.bar(["rewritten", "not rewritten"], [rewritten_bytes, max(0, total_bytes - rewritten_bytes)]); plt.ylabel("RTC output bytes")
    save_plot(plt, plot_dir / "D3_rewritten_byte_fraction")

    times = [number(row["time_to_rewrite_seconds"]) for row in overlaps]
    if times:
        xs, ys = ecdf(times); plt.figure(figsize=(6.5, 4.2)); plt.plot(xs, ys); plt.xlabel("Time to conventional rewrite (s)"); plt.ylabel("CDF"); plt.grid(alpha=0.3)
        save_plot(plt, plot_dir / "D4_time_to_rewrite_cdf")


def write_notes(result_dir: Path, a_rows: list[dict], b_rows: list[dict], c_summary: dict, d_jobs: list[dict]) -> None:
    rewritten_jobs = sum(integer(row["output_rewritten"]) for row in d_jobs)
    rewritten_bytes = sum(integer(row["rewritten_output_bytes"]) for row in d_jobs)
    total_rtc_bytes = sum(integer(row["rtc_output_bytes"]) for row in d_jobs)
    with (result_dir / "analysis_notes.txt").open("w") as handle:
        handle.write("RTC Motivation metric definitions\n")
        handle.write("=================================\n")
        handle.write("RA is the existing db_bench value: 1 + total level misses / total level hits.\n")
        handle.write("Write amplification is (logical user write bytes + compaction output bytes) / logical user write bytes.\n")
        handle.write("T_s is sampled accumulated latency of unsuccessful probes at that SST during an observation interval; it is not lower-level lookup latency.\n")
        handle.write("Repeated maintenance uses exact RTC output-file ID -> later conventional input-file ID lineage.\n\n")
        handle.write(f"Dataset A run count: {len(a_rows)}\n")
        handle.write(f"Dataset B RTC job count: {len(b_rows)}\n")
        for key, value in c_summary.items():
            handle.write(f"Dataset C {key}: {value}\n")
        handle.write(f"Dataset D rewritten RTC jobs: {rewritten_jobs}/{len(d_jobs)}\n")
        handle.write(f"Dataset D rewritten RTC output bytes: {rewritten_bytes}/{total_rtc_bytes}\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-dir", required=True, type=Path)
    parser.add_argument("--derive-only", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    result_dir = args.result_dir.resolve()
    manifest = read_manifest(result_dir)
    a_rows, jobs_by_run = derive_a(result_dir, manifest)
    all_jobs = [job for jobs in jobs_by_run.values() for job in jobs]
    rtc_jobs = [job for job in all_jobs if job.get("base_mode", job.get("mode")) == "rtc"]
    b_rows = derive_b(result_dir, rtc_jobs)
    rtc_runs = [
        run for run in manifest
        if (run.get("base_mode") or run["mode"]) == "rtc"
    ]
    c_summary, c_by_ratio = derive_c(rtc_runs)
    d_jobs, _d_overlaps = derive_d(result_dir, all_jobs)
    workload_rows = build_workload_rows(
        manifest, a_rows, b_rows, c_by_ratio, d_jobs
    )
    remove_legacy_derived_files(result_dir)
    csv_path = write_workload_csv(result_dir, workload_rows)
    rac_csv_path, rac_rows = write_rac_variants_csv(result_dir, a_rows)
    report_path = write_markdown_summary(
        result_dir, workload_rows, c_summary, rac_rows
    )
    print(report_path.read_text())
    print(f"[INFO] RTC Motivation CSV: {csv_path}")
    print(f"[INFO] RAC variants CSV: {rac_csv_path}")
    print(f"[INFO] RTC Motivation summary: {report_path}")


if __name__ == "__main__":
    main()
