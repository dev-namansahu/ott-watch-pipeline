#!/usr/bin/env python3
"""Compare what the generator says a CORRECT pipeline must produce with what the PySpark pipeline produced."""
import json
import sys

inp = sys.argv[1] if len(sys.argv) > 1 else "data/input"
out = sys.argv[2] if len(sys.argv) > 2 else "data/lake"
t = json.load(open(f"{inp}/ground_truth.json"))
s = json.load(open(f"{out}/gold_summary.json"))

checks = []
def add(name, exp, act):
    checks.append((name, exp, act, exp == act))

for k in ["input_rows", "duplicates_removed", "silver_rows", "total_watch_minutes", "completed_sessions",
          "sessions_with_known_runtime", "over_100_sessions", "runtime_missing_sessions", "rating_count", "rating_sum",
          "catalog_unique_shows", "users"]:
    add(k, t[k], s[k])
for k in t["rejects"]:
    add(f"rejected:{k}", t["rejects"][k], s["rejects"].get(k, 0))
for k in t["by_device_minutes"]:
    add(f"minutes:device:{k}", t["by_device_minutes"][k], s["by_device_minutes"].get(k))
for k in t["by_genre_minutes"]:
    add(f"minutes:genre:{k}", t["by_genre_minutes"][k], s["by_genre_minutes"].get(k))
add("genres_are_canonical", True, set(s["by_genre_minutes"]) <= {"Drama", "Comedy", "Action", "Thriller", "Sci-Fi", "Romance",
                                                                 "Documentary", "Horror", "Animation", "Unknown"})
add("catalog_duplicate_rows_removed", t["catalog_raw_rows"] - t["catalog_unique_shows"], s["catalog_raw_rows"] - s["catalog_unique_shows"])

w = max(len(c[0]) for c in checks) + 2
print(f"{'check':<{w}}{'expected':>14}{'actual':>14}  result")
for n, e, a, ok in checks:
    print(f"{n:<{w}}{str(e):>14}{str(a):>14}  {'PASS' if ok else 'FAIL'}")
good = all(c[3] for c in checks)
print(f"\n{sum(c[3] for c in checks)}/{len(checks)} checks passed" + ("  -> ALL CHECKS PASSED" if good else "  -> MISMATCH FOUND"))
sys.exit(0 if good else 1)
