#!/usr/bin/env python3
"""
Unified Parquet Validator PRO+ (aligned with unified candle schema)

Validates parquet candle files produced by:

    • CandleManager PRO+
    • CandleSync PRO+
    • ParquetStorage PRO+
    • REST Client (open-time normalization)
    • WS Client PRO (auto-aligned open_ts)

Schema rules enforced:
----------------------
Required columns (new unified schema):
    - symbol
    - timeframe
    - timestamp   (canonical open timestamp)
    - open, high, low, close
    - volume
    - received_at

Optional columns:
    - open_ts     (must match timestamp)
    - close_ts    (= open_ts + interval)
    - datetime    (optional convenience)

Checks performed:
-----------------
✓ File existence + readability
✓ Required column presence
✓ Canonical timestamp resolution (open_ts or timestamp)
✓ Timestamp ordering (strict ascending)
✓ Duplicate timestamp detection
✓ Open-time alignment to timeframe interval
✓ Sequential continuity (gap detection)
✓ Future timestamp detection
✓ OHLCV integrity
✓ open_ts != timestamp mismatches
✓ close_ts sanity check (exact = ts + interval)
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from tabulate import tabulate


class ParquetValidator:
    """Validator for unified PRO+ parquet candle schema."""

    # Timeframe → seconds
    TF = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "1h": 3600,
    }

    REQUIRED = [
        "symbol",
        "timeframe",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "received_at",
    ]

    OPTIONAL = ["open_ts", "close_ts", "datetime"]

    def __init__(self, data_dir="data/live"):
        self.dir = Path(data_dir)
        self.results = []
        self.errors = []
        self.warnings = []

    # ----------------------------------------------------------------------
    # File finder
    # ----------------------------------------------------------------------
    def find_files(self, symbol=None, date=None) -> List[Path]:
        pattern = "*"
        if symbol:
            pattern = f"{symbol}_*"
        if date:
            pattern = f"{pattern}{date}.parquet"
        else:
            pattern = f"{pattern}*.parquet"
        return sorted(self.dir.glob(pattern))

    # ----------------------------------------------------------------------
    # Read + schema validation
    # ----------------------------------------------------------------------
    def load_and_validate_structure(self, fp: Path):
        meta = {
            "file": fp.name,
            "exists": fp.exists(),
            "readable": False,
            "size_mb": 0,
            "rows": 0,
            "columns_ok": False,
            "missing": [],
        }

        if not fp.exists():
            self.errors.append(f"{fp.name}: File does not exist")
            return None, meta

        try:
            df = pd.read_parquet(fp)
        except Exception as e:
            self.errors.append(f"{fp.name}: Parquet read error → {e}")
            return None, meta

        meta["readable"] = True
        meta["size_mb"] = round(fp.stat().st_size / (1024 * 1024), 2)
        meta["rows"] = len(df)

        missing = set(self.REQUIRED) - set(df.columns)
        meta["missing"] = sorted(list(missing))
        meta["columns_ok"] = (len(missing) == 0)

        if missing:
            self.errors.append(f"{fp.name}: Missing required columns → {missing}")

        return df, meta

    # ----------------------------------------------------------------------
    # Timestamp resolution (open_ts preferred)
    # ----------------------------------------------------------------------
    def resolve_ts(self, df: pd.DataFrame) -> pd.Series:
        if "open_ts" in df.columns:
            return df["open_ts"].astype(int)
        return df["timestamp"].astype(int)

    # ----------------------------------------------------------------------
    # Duplicate timestamps
    # ----------------------------------------------------------------------
    def check_duplicates(self, ts: pd.Series, file: str):
        dup = ts.duplicated(keep=False)
        count = int(dup.sum())
        if count > 0:
            vals = ts[dup].unique()[:5].tolist()
            self.errors.append(f"{file}: {count} duplicate timestamps → {vals}")
        return count

    # ----------------------------------------------------------------------
    # Ordering
    # ----------------------------------------------------------------------
    def check_sorted(self, ts: pd.Series, file: str):
        if not ts.is_monotonic_increasing:
            self.errors.append(f"{file}: Timestamp ordering incorrect (not ascending)")
            return False
        return True

    # ----------------------------------------------------------------------
    # TF alignment (open_ts % interval == 0)
    # ----------------------------------------------------------------------
    def check_alignment(self, ts: pd.Series, tf: str, file: str):
        if tf not in self.TF:
            return 0, []

        step = self.TF[tf]
        misaligned = ts[ts % step != 0]

        if len(misaligned) > 0:
            sample = misaligned.head(5).tolist()
            self.errors.append(
                f"{file}: {len(misaligned)} misaligned timestamps → {sample}"
            )

        return len(misaligned), misaligned.head(5).tolist()

    # ----------------------------------------------------------------------
    # Gap detection
    # ----------------------------------------------------------------------
    def check_gaps(self, ts: pd.Series, tf: str, file: str):
        if tf not in self.TF:
            return 0, []

        step = self.TF[tf]
        ts_sorted = ts.sort_values().reset_index(drop=True)
        diffs = ts_sorted.diff().iloc[1:]

        gap_idx = diffs[diffs != step].index
        details = []

        for idx in gap_idx:
            prev_ts = ts_sorted.iloc[idx - 1]
            curr_ts = ts_sorted.iloc[idx]
            missing = (curr_ts - prev_ts) // step - 1

            details.append(
                {
                    "prev": datetime.fromtimestamp(prev_ts),
                    "next": datetime.fromtimestamp(curr_ts),
                    "gap_seconds": curr_ts - prev_ts,
                    "missing": missing,
                }
            )

        if len(details) > 0:
            self.errors.append(f"{file}: {len(details)} timestamp gaps detected")

        return len(details), details

    # ----------------------------------------------------------------------
    # Future timestamp detection
    # ----------------------------------------------------------------------
    def check_future(self, ts: pd.Series, tf: str, file: str):
        if tf not in self.TF:
            return 0, []

        now = int(datetime.now().timestamp())
        step = self.TF[tf]

        future = ts[ts > (now + step)]
        if len(future) > 0:
            sample = future.head(5).tolist()
            self.errors.append(
                f"{file}: {len(future)} future timestamps → {sample}"
            )

        return len(future), future.head(5).tolist()

    # ----------------------------------------------------------------------
    # OHLCV integrity
    # ----------------------------------------------------------------------
    def check_ohlcv(self, df: pd.DataFrame, file: str):
        issues = {
            "high<low": int((df["high"] < df["low"]).sum()),
            "open_out": int(
                ((df["open"] < df["low"]) | (df["open"] > df["high"])).sum()
            ),
            "close_out": int(
                ((df["close"] < df["low"]) | (df["close"] > df["high"])).sum()
            ),
            "neg_vol": int((df["volume"] < 0).sum()),
        }

        for name, n in issues.items():
            if n > 0:
                self.errors.append(f"{file}: {n} OHLCV violations ({name})")

        return issues

    # ----------------------------------------------------------------------
    # open_ts vs timestamp mismatch
    # ----------------------------------------------------------------------
    def check_field_mismatch(self, df: pd.DataFrame, file: str):
        if "open_ts" in df.columns:
            mismatch = df[df["open_ts"] != df["timestamp"]]
            if len(mismatch) > 0:
                vals = mismatch["timestamp"].head(5).tolist()
                self.errors.append(
                    f"{file}: {len(mismatch)} rows where open_ts != timestamp → {vals}"
                )
                return len(mismatch)
        return 0

    # ----------------------------------------------------------------------
    # close_ts check: close_ts == open_ts + interval - 1
    # ----------------------------------------------------------------------
    def check_close_ts(self, df: pd.DataFrame, tf: str, file: str):
        """
        Validate close_ts values.
        
        Binance close_time is the LAST millisecond of the candle (inclusive).
        For a 5m candle: close_time = open_time + 299999ms
        When converted to seconds: close_ts = open_ts + 299
        
        So the correct formula is: close_ts = open_ts + (interval - 1)
        """
        if "close_ts" not in df.columns:
            return 0

        if tf not in self.TF:
            return 0

        step = self.TF[tf]

        # Binance uses inclusive close time (last millisecond of candle)
        # close_time = open_time + (interval_ms - 1)
        # When divided by 1000: close_ts = open_ts + (interval_seconds - 1)
        bad = df[df["close_ts"] != (df["timestamp"] + step - 1)]
        if len(bad) > 0:
            sample = bad["timestamp"].head(5).tolist()
            self.errors.append(
                f"{file}: {len(bad)} bad close_ts rows → {sample}"
            )
        return len(bad)

    # ----------------------------------------------------------------------
    # Single file validator
    # ----------------------------------------------------------------------
    def validate_file(self, fp: Path):
        print("\n" + "=" * 80)
        print(f"Validating: {fp.name}")
        print("=" * 80)

        # filename parts
        parts = fp.stem.split("_")
        if len(parts) < 3:
            self.errors.append(f"{fp.name}: Invalid filename format")
            return

        symbol, tf, date = parts[0], parts[1], parts[2]

        df, meta = self.load_and_validate_structure(fp)
        if df is None:
            return

        print(f"\n📄 Structure: {meta['size_mb']} MB, {meta['rows']} rows")

        if not meta["columns_ok"]:
            print(f"❌ Missing required columns → {meta['missing']}")
            return

        ts = self.resolve_ts(df)

        # Sorted order
        print("\n🔢 Ordering:")
        ordered = self.check_sorted(ts, fp.name)
        print(f"   Ordered → {'YES' if ordered else 'NO'}")

        # Duplicates
        print("\n🔍 Duplicates:")
        dup = self.check_duplicates(ts, fp.name)
        print(f"   Duplicates → {dup}")

        # Gaps
        print("\n⛓️  Gaps:")
        gaps, gap_info = self.check_gaps(ts, tf, fp.name)
        print(f"   Gaps → {gaps}")

        # Alignment
        print("\n📏 Alignment:")
        misaligned, mis_ex = self.check_alignment(ts, tf, fp.name)
        print(f"   Misaligned → {misaligned}")

        # Future check
        print("\n⏩ Future:")
        fut, fut_ex = self.check_future(ts, tf, fp.name)
        print(f"   Future → {fut}")

        # OHLCV
        print("\n💹 OHLCV:")
        ohlcv = self.check_ohlcv(df, fp.name)
        print(f"   Issues → {ohlcv}")

        # Field mismatch
        print("\n⚠️  open_ts mismatch:")
        mismatch = self.check_field_mismatch(df, fp.name)
        print(f"   Mismatch rows → {mismatch}")

        # close_ts checks
        print("\n⛔ close_ts check:")
        bad_close = self.check_close_ts(df, tf, fp.name)
        print(f"   Bad close_ts rows → {bad_close}")

        # Summary row for master table
        self.results.append(
            {
                "file": fp.name,
                "symbol": symbol,
                "tf": tf,
                "rows": len(df),
                "gaps": gaps,
                "dups": dup,
                "misaligned": misaligned,
                "future": fut,
                "ohlcv_ok": all(v == 0 for v in ohlcv.values()),
                "ordered": ordered,
                "mismatch": mismatch,
                "close_bad": bad_close,
            }
        )

    # ----------------------------------------------------------------------
    # Master runner
    # ----------------------------------------------------------------------
    def run(self, symbol=None, date=None):
        files = self.find_files(symbol, date)

        if not files:
            print(f"No parquet files found in {self.dir}")
            return

        print("=" * 80)
        print("PARQUET VALIDATION REPORT (Unified Schema PRO+)")
        print("=" * 80)

        for fp in files:
            self.validate_file(fp)

        # summary
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)

        if not self.results:
            print("No successful validations.")
            return

        rows = []
        for r in self.results:
            ok = (
                r["gaps"] == 0
                and r["dups"] == 0
                and r["misaligned"] == 0
                and r["future"] == 0
                and r["ohlcv_ok"]
                and r["ordered"]
                and r["mismatch"] == 0
                and r["close_bad"] == 0
            )

            rows.append(
                [
                    "✓" if ok else "✗",
                    r["file"],
                    r["symbol"],
                    r["tf"],
                    r["rows"],
                    r["gaps"],
                    r["dups"],
                    r["misaligned"],
                    r["future"],
                    "YES" if r["ohlcv_ok"] else "NO",
                    "YES" if r["ordered"] else "NO",
                    r["mismatch"],
                    r["close_bad"],
                ]
            )

        headers = [
            "OK",
            "File",
            "Symbol",
            "TF",
            "Rows",
            "Gaps",
            "Dups",
            "Misaligned",
            "Future",
            "OHLCV",
            "Ordered",
            "Mismatch",
            "close_ts",
        ]

        print(tabulate(rows, headers=headers, tablefmt="grid"))

        if self.errors:
            print("\nERRORS:")
            for e in self.errors:
                print(" •", e)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dir", default="data/live")
    p.add_argument("--symbol")
    p.add_argument("--date")
    args = p.parse_args()

    validator = ParquetValidator(args.dir)
    validator.run(args.symbol, args.date)

    sys.exit(1 if validator.errors else 0)


if __name__ == "__main__":
    main()
