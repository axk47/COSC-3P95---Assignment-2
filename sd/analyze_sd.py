import csv
from pathlib import Path
from collections import defaultdict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SD_CSV = PROJECT_ROOT / "sd" / "sd_data.csv"


def load_sd_data():
    if not SD_CSV.exists():
        raise FileNotFoundError(f"SD data file not found: {SD_CSV}")

    rows = []
    with SD_CSV.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert numeric fields
            row["original_size"] = int(row["original_size"])
            row["compressed_size"] = int(row["compressed_size"])
            row["compression_ratio"] = float(row["compression_ratio"])
            row["latency_ms"] = float(row["latency_ms"])
            row["is_large_file"] = int(row["is_large_file"])
            row["bug_triggered"] = int(row["bug_triggered"])
            row["checksum_ok"] = int(row["checksum_ok"])
            row["failed"] = int(row["failed"])
            rows.append(row)
    return rows


def compute_baseline(rows):
    total_runs = len(rows)
    total_failed = sum(r["failed"] for r in rows)
    total_passed = total_runs - total_failed

    baseline_failure_rate = (
        total_failed / total_runs if total_runs > 0 else 0.0
    )

    return {
        "total_runs": total_runs,
        "total_failed": total_failed,
        "total_passed": total_passed,
        "baseline_failure_rate": baseline_failure_rate,
    }


def define_predicates(row):
    """
    Define predicates P1..Pn as booleans based on a row.
    You can expand this list later to hit 8–12 predicates.
    """
    preds = {}

    # P1: is_large_file (from server threshold)
    preds["P1_is_large_file"] = bool(row["is_large_file"])

    # P2: bug_triggered (server-side random corruption)
    preds["P2_bug_triggered"] = bool(row["bug_triggered"])

    # P3: checksum_failed
    preds["P3_checksum_failed"] = not bool(row["checksum_ok"])

    # P4: high_latency (arbitrary 200ms threshold for now)
    preds["P4_high_latency"] = row["latency_ms"] > 200.0

    # P5: high_compression_ratio (> 0.8)
    preds["P5_high_compression_ratio"] = row["compression_ratio"] > 0.8

    # P6: very_large_file (> 50 MB)
    preds["P6_very_large_file"] = row["original_size"] > 50 * 1024 * 1024

    # P7: small_file (< 100 KB)
    preds["P7_small_file"] = row["original_size"] < 100 * 1024

    # P8: medium_file (between 100 KB and 10 MB)
    preds["P8_medium_file"] = (
        100 * 1024 <= row["original_size"] <= 10 * 1024 * 1024
    )

    return preds


def analyze_predicates(rows, baseline):
    total_failed = baseline["total_failed"]
    total_passed = baseline["total_passed"]
    total_runs = baseline["total_runs"]
    baseline_failure_rate = baseline["baseline_failure_rate"]

    if total_runs == 0:
        print("No SD data rows. Run the client first.")
        return

    # Stats per predicate
    stats = defaultdict(lambda: {
        "failed_P": 0,
        "passed_P": 0,
        "support": 0,
        "failure_P": 0.0,
        "increase": 0.0,
    })

    for row in rows:
        failed = bool(row["failed"])
        preds = define_predicates(row)

        for name, value in preds.items():
            if not value:
                continue

            s = stats[name]
            s["support"] += 1
            if failed:
                s["failed_P"] += 1
            else:
                s["passed_P"] += 1

    # Compute Failure(P) and Increase(P) for each predicate
    result = []
    for name, s in stats.items():
        failed_P = s["failed_P"]
        passed_P = s["passed_P"]
        support = s["support"]

        if total_failed > 0:
            failure_P = failed_P / total_failed
        else:
            failure_P = 0.0

        if support > 0:
            failure_rate_when_P = failed_P / support
        else:
            failure_rate_when_P = 0.0

        increase = failure_rate_when_P - baseline_failure_rate

        s["failure_P"] = failure_P
        s["increase"] = increase

        result.append({
            "name": name,
            "failed_P": failed_P,
            "passed_P": passed_P,
            "support": support,
            "failure_P": failure_P,
            "increase": increase,
        })

    # Sort predicates: first by decrease/increase in failure_rate, then by failure_P
    result.sort(key=lambda r: (r["increase"], r["failure_P"]), reverse=True)

    return result


def main():
    rows = load_sd_data()
    baseline = compute_baseline(rows)

    print("=== Baseline ===")
    print(f"Total runs:      {baseline['total_runs']}")
    print(f"Total failed:    {baseline['total_failed']}")
    print(f"Total passed:    {baseline['total_passed']}")
    print(f"Failure rate:    {baseline['baseline_failure_rate']:.3f}")
    print()

    preds = analyze_predicates(rows, baseline)
    if not preds:
        print("No predicates had support (never true).")
        return

    print("=== Predicates ranked by Increase(P) (and Failure(P)) ===")
    print(f"{'Predicate':30} {'support':>8} {'failed_P':>9} {'passed_P':>9} {'Failure(P)':>11} {'Increase':>10}")
    for r in preds:
        print(
            f"{r['name']:30} "
            f"{r['support']:8d} "
            f"{r['failed_P']:9d} "
            f"{r['passed_P']:9d} "
            f"{r['failure_P']:11.3f} "
            f"{r['increase']:10.3f}"
        )


if __name__ == "__main__":
    main()
