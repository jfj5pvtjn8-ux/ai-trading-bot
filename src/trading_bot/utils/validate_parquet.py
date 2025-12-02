#!/usr/bin/env python3
"""
Unified Parquet Validator (aligned with new unified candle schema)

Validates parquet candles with structure:

Required (new schema):
- symbol
- timeframe
- timestamp   (alias for open_ts)
- open_ts
- close_ts
- datetime
- open, high, low, close
- volume
- received_at

Also supports older files where only 'timestamp' exists
and open_ts was not present.

Validation includes:
- File existence + readability
- Required columns present
- Sequential timestamps (open_ts)
- Gap detection based on timeframe
- Duplicate timestamp detection
- Misalignment to TF interval
- Future timestamp check
- OHLCV integrity
- Mismatched fields (open_ts != timestamp)
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import pandas as pd
from tabulate import tabulate


class ParquetValidator:
    """Validates unified parquet candle files."""

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

    OPTIONAL = [
        "open_ts",
        "close_ts",
        "datetime",
    ]

    def __init__(self, data_dir="data/live"):
        self.dir = Path(data_dir)
        self.results = []
        self.errors = []
        self.warnings = []

    # ----------------------------------------------------------------------
    # File discovery
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
    # Basic file validation
    # ----------------------------------------------------------------------

    def validate_structure(self, fp: Path) -> Tuple[Optional[pd.DataFrame], dict]:
        result = {
            "file": fp.name,
            "exists": fp.exists(),
            "readable": False,
            "size_mb": 0,
            "row_count": 0,
            "columns_ok": False,
            "missing_cols": [],
        }

        if not fp.exists():
            self.errors.append(f"{fp.name}: File does not exist")
            return None, result

        try:
            df = pd.read_parquet(fp)
            result["readable"] = True
            result["size_mb"] = round(fp.stat().st_size / (1024 * 1024), 2)
            result["row_count"] = len(df)

            missing = set(self.REQUIRED) - set(df.columns)
            result["missing_cols"] = list(missing)
            result["columns_ok"] = len(missing) == 0

            if missing:
                self.errors.append(f"{fp.name}: Missing columns → {missing}")

            return df, result

        except Exception as e:
            self.errors.append(f"{fp.name}: Cannot read parquet → {e}")
            return None, result

    # ----------------------------------------------------------------------
    # Timestamp resolution
    # ----------------------------------------------------------------------

    def resolve_timestamp(self, df: pd.DataFrame) -> pd.Series:
        """
        Choose canonical timestamp series.
        Priority:
        1. open_ts (new schema)
        2. timestamp (old + new alias)
        """
        if "open_ts" in df.columns:
            return df["open_ts"].astype(int)

        return df["timestamp"].astype(int)

    # ----------------------------------------------------------------------
    # Check duplicates
    # ----------------------------------------------------------------------

    def check_duplicates(self, ts: pd.Series, file: str):
        dup = ts.duplicated()
        count = int(dup.sum())
        if count > 0:
            vals = ts[dup].unique()[:5]
            self.errors.append(f"{file}: Duplicate timestamps → {vals.tolist()}")
        return count

    # ----------------------------------------------------------------------
    # Check ordering
    # ----------------------------------------------------------------------

    def check_order(self, ts: pd.Series, file: str):
        if not ts.is_monotonic_increasing:
            self.errors.append(f"{file}: Timestamps not sorted ascending")
            return False
        return True

    # ----------------------------------------------------------------------
    # Check gaps (open_ts)
    # ----------------------------------------------------------------------

    def check_gaps(self, ts: pd.Series, tf: str, file: str):
        if tf not in self.TF:
            return 0, []

        interval = self.TF[tf]
        ts_sorted = ts.sort_values().reset_index(drop=True)
        diffs = ts_sorted.diff().iloc[1:]

        gap_idx = diffs[diffs != interval].index
        details = []

        for idx in gap_idx:
            prev_ts = ts_sorted.iloc[idx - 1]
            curr_ts = ts_sorted.iloc[idx]
            missing = (curr_ts - prev_ts) // interval - 1

            details.append(
                {
                    "prev": datetime.fromtimestamp(prev_ts),
                    "next": datetime.fromtimestamp(curr_ts),
                    "missing": int(missing),
                    "seconds_gap": int(curr_ts - prev_ts),
                }
            )

        if details:
            self.errors.append(
                f"{file}: {len(details)} timestamp gaps (open_ts-based)"
            )

        return len(details), details

    # ----------------------------------------------------------------------
    # Alignment
    # ----------------------------------------------------------------------

    def check_alignment(self, ts: pd.Series, tf: str, file: str):
        if tf not in self.TF:
            return 0, []

        interval = self.TF[tf]
        misaligned = ts[ts % interval != 0]

        if len(misaligned) > 0:
            head = misaligned.head(5).tolist()
            self.errors.append(
                f"{file}: {len(misaligned)} misaligned timestamps → {head}"
            )

        return len(misaligned), misaligned.head(5).tolist()

    # ----------------------------------------------------------------------
    # Future timestamp check
    # ----------------------------------------------------------------------

    def check_future(self, ts: pd.Series, tf: str, file: str):
        if tf not in self.TF:
            return 0, []

        interval = self.TF[tf]
        now = int(datetime.now().timestamp())

        future = ts[ts > now + interval]

        if len(future) > 0:
            head = future.head(5).tolist()
            self.errors.append(
                f"{file}: {len(future)} timestamps appear to be in the future → {head}"
            )

        return len(future), future.head(5).tolist()

    # ----------------------------------------------------------------------
    # OHLCV integrity
    # ----------------------------------------------------------------------

    def check_ohlcv(self, df: pd.DataFrame, file: str):
        issues = {
            "bad_high_low": int((df["high"] < df["low"]).sum()),
            "open_outside_range": int(
                ((df["open"] < df["low"]) | (df["open"] > df["high"])).sum()
            ),
            "close_outside_range": int(
                ((df["close"] < df["low"]) | (df["close"] > df["high"])).sum()
            ),
            "negative_volume": int((df["volume"] < 0).sum()),
        }

        for name, count in issues.items():
            if count > 0:
                self.errors.append(f"{file}: {count} {name} violations")

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
    # Main file validator
    # ----------------------------------------------------------------------

    def validate_file(self, fp: Path):
        print("\n" + "=" * 80)
        print(f"Validating: {fp.name}")
        print("=" * 80)

        # Extract metadata
        parts = fp.stem.split("_")
        if len(parts) < 3:
            self.errors.append(f"{fp.name}: Invalid filename format")
            return

        symbol, tf, date = parts[0], parts[1], parts[2]

        df, meta = self.validate_structure(fp)
        if df is None:
            return

        print(f"\n📄 Structure: {meta['size_mb']} MB, {meta['row_count']} rows")
        if meta["missing_cols"]:
            print(f"❌ Missing columns → {meta['missing_cols']}")
            return

        # Canonical timestamp
        ts = self.resolve_timestamp(df)

        # Ordering
        print("\n🔢 Ordering:")
        ordered = self.check_order(ts, fp.name)
        print(f"   Sorted → {'YES' if ordered else 'NO'}")

        # Duplicates
        print("\n🔍 Duplicates:")
        dup = self.check_duplicates(ts, fp.name)
        print(f"   Duplicates → {dup}")

        # Gaps
        print("\n⛓️  Gaps:")
        gaps, gap_details = self.check_gaps(ts, tf, fp.name)
        print(f"   Gaps → {gaps}")

        # Alignment
        print("\n📏 Alignment:")
        misaligned, examples = self.check_alignment(ts, tf, fp.name)
        print(f"   Misaligned → {misaligned}")

        # Future timestamps
        print("\n⏩ Future:")
        fut, fut_ex = self.check_future(ts, tf, fp.name)
        print(f"   Future → {fut}")

        # OHLCV
        print("\n💹 OHLCV:")
        ohlcv = self.check_ohlcv(df, fp.name)
        print(f"   Issues → {ohlcv}")

        # open_ts vs timestamp mismatch
        print("\n⚠️  Field Mismatch:")
        mismatch_count = self.check_field_mismatch(df, fp.name)
        print(f"   open_ts != timestamp → {mismatch_count}")

        # Summary
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
                "mismatch": mismatch_count,
            }
        )

    # ----------------------------------------------------------------------
    # Validate all files
    # ----------------------------------------------------------------------

    def run(self, symbol=None, date=None):
        files = self.find_files(symbol, date)

        if not files:
            print(f"No files found in {self.dir}")
            return

        print("=" * 80)
        print("PARQUET VALIDATION REPORT")
        print("=" * 80)

        for fp in files:
            self.validate_file(fp)

        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)

        if not self.results:
            print("Nothing validated.")
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
        ]

        print(tabulate(rows, headers=headers, tablefmt="grid"))

        # Error dump
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

    v = ParquetValidator(args.dir)
    v.run(args.symbol, args.date)

    sys.exit(1 if v.errors else 0)


if __name__ == "__main__":
    main()
