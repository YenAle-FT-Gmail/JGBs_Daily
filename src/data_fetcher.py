#!/usr/bin/env python3
"""
JGBsDaily Data Fetcher
Fetches JGB yield data from the Japanese Ministry of Finance,
calculates historical deltas, and outputs structured JSON.
"""

import io
import json
import logging
import os
import smtplib
import sys
from datetime import datetime
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SIMPLE_CURRENT_URL = "https://www.mof.go.jp/jgbs/reference/interest_rate/jgbcm.csv"
SIMPLE_HISTORICAL_URL = "https://www.mof.go.jp/jgbs/reference/interest_rate/data/jgbcm_all.csv"
COMPOUND_CURRENT_URL = "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/jgbcme.csv"
COMPOUND_HISTORICAL_URL = "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/historical/jgbcme_all.csv"

OUTPUT_PATH = Path(__file__).resolve().parent.parent / "public" / "data" / "yields.json"

# Tenors we care about – the MOF CSV columns (Japanese simple) use yen labels,
# the English compound CSV uses English labels.  We normalise everything to these keys.
TENORS = ["1Y", "2Y", "3Y", "4Y", "5Y", "6Y", "7Y", "8Y", "9Y", "10Y",
          "15Y", "20Y", "25Y", "30Y", "40Y"]

# Delta look-back definitions: label → offset from end for the comparison row.
# "DoD" means last row vs second-to-last, etc.  Offsets assume business-day rows.
DELTA_DEFS = {
    "DoD": (-1, -2),
    "2D":  (-1, -3),
    "3D":  (-1, -4),
    "1W":  (-1, -6),
    "2W":  (-1, -11),
    "1M":  (-1, -22),
    "3M":  (-1, -64),
    "6M":  (-1, -127),
    "1Y":  (-1, -253),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def send_alert_email(subject: str, body: str) -> None:
    """Send an alert email using SMTP credentials from environment variables."""
    sender = os.environ.get("EMAIL_SENDER")
    password = os.environ.get("EMAIL_PASSWORD")
    receiver = os.environ.get("EMAIL_RECEIVER")
    if not all([sender, password, receiver]):
        logger.warning("Email credentials not configured – skipping alert email.")
        return
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = receiver
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, [receiver], msg.as_string())
        logger.info("Alert email sent successfully.")
    except Exception as e:
        logger.error("Failed to send alert email: %s", e)


def fetch_csv(url: str, encoding: str = "shift_jis") -> pd.DataFrame:
    """Download a CSV from *url* and return a cleaned DataFrame."""
    logger.info("Fetching %s", url)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    raw = resp.content.decode(encoding, errors="replace")

    # The MOF CSVs typically have 1-2 header/comment rows before the actual
    # column names.  We peek at the first few lines to find the header row.
    lines = raw.splitlines()
    header_idx = 0
    for i, line in enumerate(lines[:5]):
        # The header row usually contains "基準日" (date) for Japanese CSVs
        # or "Date" for English CSVs.
        if "基準日" in line or "Date" in line:
            header_idx = i
            break

    df = pd.read_csv(io.StringIO(raw), header=header_idx, encoding="utf-8")

    # Drop fully-empty rows and columns
    df = df.dropna(how="all").dropna(axis=1, how="all")
    return df


# Japanese Imperial Era offsets (era start year in Gregorian)
_ERA_OFFSETS = {
    "S": 1925,   # Showa
    "H": 1988,   # Heisei
    "R": 2018,   # Reiwa
}


def _imperial_to_iso(val: str) -> str:
    """Convert a Japanese imperial date like 'R8.3.2' → '2026/03/02'.
    Returns the original string unchanged if it doesn't match."""
    val = str(val).strip()
    if not val or val[0] not in _ERA_OFFSETS:
        return val
    try:
        era_char = val[0]
        parts = val[1:].split(".")
        if len(parts) != 3:
            return val
        year = _ERA_OFFSETS[era_char] + int(parts[0])
        return f"{year}/{int(parts[1]):02d}/{int(parts[2]):02d}"
    except (ValueError, IndexError):
        return val


def _normalise_date(val: str) -> str:
    """Normalise a date like '2026/3/2' → '2026/03/02' for proper sorting."""
    val = str(val).strip()
    try:
        parts = val.split("/")
        if len(parts) == 3:
            return f"{int(parts[0])}/{int(parts[1]):02d}/{int(parts[2]):02d}"
    except (ValueError, IndexError):
        pass
    return val


def normalise_simple(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise the Japanese-language simple-yield CSV into a standard form."""
    # First column is the date column
    date_col = df.columns[0]
    df = df.rename(columns={date_col: "Date"})

    # Map Japanese tenor headers to our standard tenor keys.
    jp_tenor_map = {}
    for col in df.columns:
        col_str = str(col).strip()
        for t in TENORS:
            years = t.replace("Y", "")
            if col_str == f"{years}年":
                jp_tenor_map[col] = t
                break
    df = df.rename(columns=jp_tenor_map)

    # Filter out footer/comment rows (e.g. rows starting with ※ or empty dates)
    df = df[df["Date"].astype(str).str.match(r'^[A-ZSHR]\d', na=False)].copy()

    # Convert imperial era dates to ISO-ish format for consistency
    df["Date"] = df["Date"].apply(_imperial_to_iso)
    return df


def normalise_compound(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise the English-language compound-yield CSV into a standard form."""
    date_col = df.columns[0]
    df = df.rename(columns={date_col: "Date"})

    # English CSV columns look like: "1Y", "2Y", etc.
    rename_map = {}
    for col in df.columns:
        col_str = str(col).strip()
        if col_str in TENORS:
            rename_map[col] = col_str
    df = df.rename(columns=rename_map)

    # Filter out footer/comment rows – keep only rows where Date looks like a date
    df = df[df["Date"].astype(str).str.match(r'^\d{4}/', na=False)].copy()

    # Normalise dates to zero-padded YYYY/MM/DD for consistent sorting
    df["Date"] = df["Date"].apply(_normalise_date)
    return df


def merge_historical_current(hist_df: pd.DataFrame, cur_df: pd.DataFrame) -> pd.DataFrame:
    """Concat historical + current, drop duplicates by date, sort."""
    combined = pd.concat([hist_df, cur_df], ignore_index=True)
    combined["Date"] = combined["Date"].astype(str).str.strip()
    combined = combined.drop_duplicates(subset="Date", keep="last")
    combined = combined.sort_values("Date").reset_index(drop=True)
    return combined


def compute_deltas(df: pd.DataFrame) -> dict:
    """Compute basis-point deltas for each tenor.

    Returns a dict like:
    {
        "DoD": {"1Y": 0.5, "2Y": -1.0, …},
        "2D":  {…}, "3D": {…}, "1W": {…}
    }
    """
    result = {}
    n = len(df)
    for label, (idx_a, idx_b) in DELTA_DEFS.items():
        deltas = {}
        actual_a = n + idx_a  # e.g. n - 1
        actual_b = n + idx_b  # e.g. n - 2
        if actual_a < 0 or actual_b < 0:
            # Not enough data
            for t in TENORS:
                deltas[t] = None
        else:
            row_a = df.iloc[actual_a]
            row_b = df.iloc[actual_b]
            for t in TENORS:
                try:
                    val_a = float(row_a[t])
                    val_b = float(row_b[t])
                    deltas[t] = round((val_a - val_b) * 100, 1)  # bps
                except (KeyError, ValueError, TypeError):
                    deltas[t] = None
        result[label] = deltas
    return result


def build_current_yields(df: pd.DataFrame) -> dict:
    """Extract the latest yield for each tenor."""
    if df.empty:
        return {t: None for t in TENORS}
    last = df.iloc[-1]
    yields = {}
    for t in TENORS:
        try:
            yields[t] = round(float(last[t]), 3)
        except (KeyError, ValueError, TypeError):
            yields[t] = None
    return yields


def build_historical_curves(df: pd.DataFrame) -> dict:
    """Extract historical yield curves for each delta period.

    Returns a dict like:
    {
        "1W": {"date": "2026/03/05", "yields": {"1Y": 1.023, …}},
        …
    }
    """
    n = len(df)
    result = {}
    for label, (_idx_a, idx_b) in DELTA_DEFS.items():
        actual_b = n + idx_b
        if actual_b < 0:
            result[label] = {"date": None, "yields": {t: None for t in TENORS}}
        else:
            row = df.iloc[actual_b]
            yields = {}
            for t in TENORS:
                try:
                    yields[t] = round(float(row[t]), 3)
                except (KeyError, ValueError, TypeError):
                    yields[t] = None
            result[label] = {
                "date": str(row["Date"]).strip(),
                "yields": yields,
            }
    return result


def get_latest_date(df: pd.DataFrame) -> str:
    if df.empty:
        return ""
    return str(df.iloc[-1]["Date"]).strip()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    try:
        # ------ Simple yields ------
        simple_hist = normalise_simple(fetch_csv(SIMPLE_HISTORICAL_URL, encoding="shift_jis"))
        simple_cur = normalise_simple(fetch_csv(SIMPLE_CURRENT_URL, encoding="shift_jis"))
        simple = merge_historical_current(simple_hist, simple_cur)

        # ------ Compound yields ------
        compound_hist = normalise_compound(fetch_csv(COMPOUND_HISTORICAL_URL, encoding="utf-8"))
        compound_cur = normalise_compound(fetch_csv(COMPOUND_CURRENT_URL, encoding="utf-8"))
        compound = merge_historical_current(compound_hist, compound_cur)

        # ------ Build output ------
        output = {
            "updated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tenors": TENORS,
            "delta_keys": list(DELTA_DEFS.keys()),
            "simple": {
                "date": get_latest_date(simple),
                "yields": build_current_yields(simple),
                "deltas": compute_deltas(simple),
                "curves": build_historical_curves(simple),
            },
            "compound": {
                "date": get_latest_date(compound),
                "yields": build_current_yields(compound),
                "deltas": compute_deltas(compound),
                "curves": build_historical_curves(compound),
            },
        }

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_PATH.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info("Data successfully updated → %s", OUTPUT_PATH)

    except Exception as exc:
        logger.error("Data fetch failed: %s", exc, exc_info=True)
        send_alert_email(
            subject="[JGBsDaily] Data Update Failed",
            body=f"The JGBsDaily data pipeline failed at {datetime.utcnow().isoformat()}.\n\nError:\n{exc}",
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
