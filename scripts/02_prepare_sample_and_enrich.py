#!/usr/bin/env python3
"""
STEP 2 (run on your LAPTOP, not in the lab - saves lab time and disk)

Reads the raw PaySim CSV from Kaggle, keeps ALL fraud rows + a random sample of normal rows,
enriches with synthetic fields and writes a small nested-JSON file:  data/fraud_tx.jsonl.gz

Usage:
  python 02_prepare_sample_and_enrich.py --input ../data/PS_20174392719_1491204439457_log.csv
  python 02_prepare_sample_and_enrich.py --input ../data/PS_...csv --rows 500000
  python 02_prepare_sample_and_enrich.py --synthetic            # no Kaggle download available
"""
import argparse
import os
import time

import pandas as pd

from data_utils import enrich_df, make_synthetic_paysim, write_jsonl_gz

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

ap = argparse.ArgumentParser()
ap.add_argument("--input", help="path to PaySim CSV from Kaggle")
ap.add_argument("--synthetic", action="store_true", help="generate PaySim-like data instead of reading Kaggle CSV")
ap.add_argument("--rows", type=int, default=300000, help="total rows to keep (all frauds are always kept)")
ap.add_argument("--output", default=os.path.join(ROOT, "data", "fraud_tx.jsonl.gz"))
ap.add_argument("--seed", type=int, default=42)
a = ap.parse_args()

t0 = time.time()
if a.synthetic:
    print(f"[INFO] Generating synthetic PaySim-like data ({a.rows} rows) ...")
    df = make_synthetic_paysim(a.rows, a.seed)
else:
    if not a.input:
        ap.error("give --input <csv> or use --synthetic")
    print(f"[INFO] Reading {a.input} ...")
    df = pd.read_csv(a.input)
    print(f"[INFO] Raw rows: {len(df):,} | frauds: {int(df.isFraud.sum()):,}")
    fr = df[df.isFraud == 1]
    nm = df[df.isFraud == 0].sample(n=max(a.rows - len(fr), 1), random_state=a.seed)
    df = pd.concat([fr, nm], ignore_index=True)

print(f"[INFO] Rows kept: {len(df):,} | frauds: {int(df.isFraud.sum()):,} "
      f"({100 * df.isFraud.mean():.2f}% - higher than the raw 0.13% because we keep every fraud)")
df = enrich_df(df, a.seed)
os.makedirs(os.path.dirname(a.output), exist_ok=True)
n = write_jsonl_gz(df, a.output)
size_mb = os.path.getsize(a.output) / 1e6
print(f"[DONE] Wrote {n:,} documents -> {a.output} ({size_mb:.1f} MB) in {time.time() - t0:.0f}s")
print("[NEXT] Upload this .jsonl.gz (and the whole project folder) to the lab machine.")
