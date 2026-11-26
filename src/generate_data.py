#!/usr/bin/env python3
"""
Synthetic OTT data simulator: catalog, users and daily watch-event files with intentional data-quality problems.

Writes under --out:
  raw/catalog/catalog.csv                 messy casing, nulls, duplicate show rows with a missing runtime
  raw/users/users.csv                     messy country / plan casing, nulls
  raw/events/dt=YYYY-MM-DD/events.csv     one file per day (daily incremental landing), re-delivered duplicates
  ground_truth.json                       what a CORRECT pipeline must produce (used by validate_results.py)

Injected problems in events: duplicate session_ids (same day and re-delivered next day), unknown show_id,
null / negative watch_minutes, out-of-range ratings, null ratings, device-type spelling variants, null device,
watch_minutes above runtime (re-watch).
"""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

GENRES = ["Drama", "Comedy", "Action", "Thriller", "Sci-Fi", "Romance", "Documentary", "Horror", "Animation"]
DEVICE_RAW = {"MOBILE": ["Mobile", "mobile", "MOBILE", "Phone", "android", "iOS"],
              "TV": ["TV", "tv", "Smart TV", "smart tv", "Television"],
              "WEB": ["Web", "web", "WEB", "Browser", "desktop"],
              "TABLET": ["Tablet", "tablet", "TABLET"]}
COUNTRIES = {"India": ["India", "india", "INDIA", "IN"], "United States": ["United States", "USA", "us", "US"],
             "United Kingdom": ["United Kingdom", "UK", "uk"], "Canada": ["Canada", "CANADA", "canada"],
             "Germany": ["Germany", "germany"], "Australia": ["Australia", "AUSTRALIA"]}
ADJ = ["Silent", "Crimson", "Midnight", "Broken", "Golden", "Hidden", "Last", "Electric", "Wild", "Frozen"]
NOUN = ["Harbor", "Empire", "Garden", "Signal", "Horizon", "Letters", "Kingdom", "Rivals", "Echo", "Journey"]

GENRE_COMPLETE = {"Drama": 0.45, "Comedy": 0.50, "Action": 0.42, "Thriller": 0.55, "Sci-Fi": 0.40, "Romance": 0.47,
                  "Documentary": 0.28, "Horror": 0.35, "Animation": 0.58}   # completion propensity by genre
P_ORPHAN, P_NULL_MIN, P_NEG_MIN, P_DUP = 0.003, 0.007, 0.004, 0.02


def genre_variants(g):
    v = [g, g.lower(), g.upper(), f" {g} ", g.lower().replace("-", " ")]
    return v + ["Science Fiction"] if g == "Sci-Fi" else v


def messy_title(t, style):
    return [t, t.upper(), t.lower(), "  " + t.replace(" ", "   ") + " "][style]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/input")
    ap.add_argument("--events", type=int, default=300_000)
    ap.add_argument("--users", type=int, default=5_000)
    ap.add_argument("--shows", type=int, default=300)
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--start-date", default="2026-11-01")
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    rng = np.random.default_rng(a.seed)
    out = Path(a.out)
    for d in ["raw/catalog", "raw/users", "raw/events"]:
        (out / d).mkdir(parents=True, exist_ok=True)
    base = pd.Timestamp(a.start_date)
    ns, nu, n, days = a.shows, a.users, a.events, a.days

    # ---------------- catalog ----------------
    sid = np.array([f"S{i:04d}" for i in range(1, ns + 1)])
    ctype = rng.choice(["Movie", "Series"], ns, p=[0.35, 0.65])
    rt_true = np.where(ctype == "Movie", rng.integers(80, 171, ns), rng.integers(22, 61, ns))
    genre_true = rng.choice(GENRES, ns)
    title_true = np.array([f"{rng.choice(ADJ)} {rng.choice(NOUN)}" for _ in range(ns)])
    null_rt, null_genre, null_year = rng.random(ns) < 0.05, rng.random(ns) < 0.02, rng.random(ns) < 0.03
    ct_raw = {"Movie": ["Movie", "movie", "MOVIE", "Film"], "Series": ["Series", "series", "TV Series", "tv series"]}
    cat = pd.DataFrame({
        "show_id": sid,
        "title": [messy_title(t, rng.integers(0, 4)) for t in title_true],
        "genre": [None if nl else rng.choice(genre_variants(g)) for g, nl in zip(genre_true, null_genre)],
        "release_year": [None if ny else int(y) for y, ny in zip(rng.integers(1995, 2027, ns), null_year)],
        "runtime_minutes": [None if nr else int(r) for r, nr in zip(rt_true, null_rt)],
        "content_type": [rng.choice(ct_raw[c]) for c in ctype]})
    ok = np.where(~null_rt & ~null_genre)[0]
    dup_idx = rng.choice(ok, 10, replace=False)                       # duplicate show rows, runtime missing
    dup_rows = cat.iloc[dup_idx].copy()
    dup_rows["runtime_minutes"] = None
    dup_rows["genre"] = [rng.choice(genre_variants(g)).lower() for g in genre_true[dup_idx]]
    cat = pd.concat([cat, dup_rows]).sample(frac=1, random_state=a.seed).reset_index(drop=True)
    cat["release_year"] = cat["release_year"].astype("Int64")
    cat["runtime_minutes"] = cat["runtime_minutes"].astype("Int64")
    cat.to_csv(out / "raw/catalog/catalog.csv", index=False)

    # ---------------- users ----------------
    ctry = rng.choice(list(COUNTRIES), nu, p=[0.45, 0.2, 0.1, 0.1, 0.1, 0.05])
    plan = rng.choice(["BASIC", "STANDARD", "PREMIUM"], nu, p=[0.4, 0.4, 0.2])
    users = pd.DataFrame({
        "user_id": [f"U{i:06d}" for i in range(1, nu + 1)],
        "country": [None if rng.random() < 0.02 else rng.choice(COUNTRIES[c]) for c in ctry],
        "plan": [rng.choice([p, p.lower(), p.title(), f" {p} "]) for p in plan],
        "age_group": [None if rng.random() < 0.03 else g for g in rng.choice(["18-24", "25-34", "35-44", "45+"], nu)],
        "signup_date": (pd.Timestamp("2022-01-01") + pd.to_timedelta(rng.integers(0, 1400, nu), "D")).strftime("%Y-%m-%d")})
    users.to_csv(out / "raw/users/users.csv", index=False)

    # ---------------- events ----------------
    pop = rng.pareto(1.5, ns) + 1
    pop /= pop.sum()
    show_idx = rng.choice(ns, n, p=pop)
    dow_w = np.array([1.4 if (base + pd.Timedelta(days=d)).weekday() >= 5 else 1.0 for d in range(days)])
    day_idx = rng.choice(days, n, p=dow_w / dow_w.sum())
    hw = np.array([1, 1, 1, 1, 1, 1, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 5, 6, 8, 10, 11, 10, 6, 3], float)
    hour = rng.choice(24, n, p=hw / hw.sum())
    ts = base + pd.to_timedelta(day_idx, "D") + pd.to_timedelta(hour * 3600 + rng.integers(0, 3600, n), "s")
    weekend = (day_idx % 7 + base.weekday()) % 7 >= 5                       # TV skews to weekends, mobile to weekdays
    dev_canon = np.where(weekend, rng.choice(list(DEVICE_RAW), n, p=[0.35, 0.42, 0.18, 0.05]),
                         rng.choice(list(DEVICE_RAW), n, p=[0.50, 0.25, 0.20, 0.05]))
    dev_null = rng.random(n) < 0.01
    dev_raw = np.array([rng.choice(DEVICE_RAW[d]) for d in dev_canon], dtype=object)
    dev_raw[dev_null] = None
    dev_final = np.where(dev_null, "UNKNOWN", dev_canon)

    u2 = rng.random(n)
    p_done = pd.Series(genre_true).map(GENRE_COMPLETE).to_numpy()[show_idx] + np.where(dev_canon == "TV", 0.08, 0.0)
    frac = np.where(u2 < p_done, rng.uniform(0.92, 1.0, n),
                    np.where(u2 < p_done + 0.03, rng.uniform(1.02, 1.5, n), rng.beta(2, 3, n)))
    watch = np.maximum(1, np.round(frac * rt_true[show_idx])).astype(int)

    rated = rng.random(n) >= 0.40
    rating = rng.choice([1, 2, 3, 4, 5], n, p=[0.05, 0.08, 0.20, 0.35, 0.32]).astype(float)
    oor = rated & (rng.random(n) < 0.01)
    rating[oor] = rng.choice([0, 6, 10, -1], oor.sum())
    rating[~rated] = np.nan
    rating_valid = np.where((rating >= 1) & (rating <= 5), rating, np.nan)

    u = rng.random(n)
    cat_ev = np.full(n, "NORMAL", dtype=object)
    cat_ev[u < P_ORPHAN] = "ORPHAN"
    cat_ev[(u >= P_ORPHAN) & (u < P_ORPHAN + P_NULL_MIN)] = "NULL_MIN"
    cat_ev[(u >= P_ORPHAN + P_NULL_MIN) & (u < P_ORPHAN + P_NULL_MIN + P_NEG_MIN)] = "NEG_MIN"

    show_col = sid[show_idx].astype(object)
    orph = cat_ev == "ORPHAN"
    show_col[orph] = [f"S{9000 + int(i)}" for i in rng.integers(0, 500, orph.sum())]
    watch_col = watch.astype(object)
    watch_col[cat_ev == "NULL_MIN"] = None
    watch_col[cat_ev == "NEG_MIN"] = -rng.integers(1, 31, (cat_ev == "NEG_MIN").sum())
    rating_raw = np.array([None if np.isnan(r) else int(r) for r in rating], dtype=object)

    ev = pd.DataFrame({
        "session_id": [f"SES{i:09d}" for i in range(n)], "user_id": [f"U{i:06d}" for i in rng.integers(1, nu + 1, n)],
        "show_id": show_col, "device_type": dev_raw, "watch_minutes": watch_col, "rating": rating_raw,
        "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S")})
    ev["dt"] = ts.strftime("%Y-%m-%d")

    # re-delivered duplicates: exact copies, same day (60%) or next day's file (40%)
    normal_pos = np.where(cat_ev == "NORMAL")[0]
    dup_pos = normal_pos[rng.random(len(normal_pos)) < P_DUP]
    dup = ev.iloc[dup_pos].copy()
    nxt = rng.random(len(dup)) < 0.40
    day_of = day_idx[dup_pos]
    nxt &= day_of < days - 1
    dup["dt"] = (base + pd.to_timedelta(day_of + nxt.astype(int), "D")).strftime("%Y-%m-%d")
    allev = pd.concat([ev, dup], ignore_index=True).sample(frac=1, random_state=a.seed)
    for dt, g in allev.groupby("dt"):
        p = out / f"raw/events/dt={dt}"
        p.mkdir(parents=True, exist_ok=True)
        g.drop(columns="dt").to_csv(p / "events.csv", index=False)

    # ---------------- ground truth: what a correct pipeline must produce ----------------
    N = cat_ev == "NORMAL"
    known = ~null_rt[show_idx]
    genre_final = np.where(null_genre, "Unknown", genre_true)[show_idx]
    rt = rt_true[show_idx]
    truth = {
        "input_rows": int(len(allev)), "duplicates_removed": int(len(dup)),
        "rejects": {"INVALID_SHOW": int((cat_ev == "ORPHAN").sum()), "NULL_MINUTES": int((cat_ev == "NULL_MIN").sum()),
                    "NEGATIVE_MINUTES": int((cat_ev == "NEG_MIN").sum())},
        "silver_rows": int(N.sum()), "total_watch_minutes": int(watch[N].sum()),
        "by_device_minutes": {k: int(v) for k, v in pd.Series(watch[N]).groupby(dev_final[N]).sum().items()},
        "by_genre_minutes": {k: int(v) for k, v in pd.Series(watch[N]).groupby(genre_final[N]).sum().items()},
        "completed_sessions": int((N & known & (watch * 100 >= 90 * rt)).sum()),
        "sessions_with_known_runtime": int((N & known).sum()),
        "over_100_sessions": int((N & known & (watch > rt)).sum()),
        "runtime_missing_sessions": int((N & ~known).sum()),
        "rating_count": int((N & ~np.isnan(rating_valid)).sum()),
        "rating_sum": int(np.nansum(np.where(N, rating_valid, np.nan))),
        "catalog_raw_rows": int(len(cat)), "catalog_unique_shows": int(ns), "users": int(nu)}
    json.dump(truth, open(out / "ground_truth.json", "w"), indent=2)
    print(f"generated {len(allev):,} event rows in {allev.dt.nunique()} daily files, {len(cat)} catalog rows, {nu:,} users")
    print(json.dumps(truth))


if __name__ == "__main__":
    main()
