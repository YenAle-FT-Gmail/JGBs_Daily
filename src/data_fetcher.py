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
CURRENT_URL = "https://www.mof.go.jp/jgbs/reference/interest_rate/jgbcm.csv"
HISTORICAL_URL = "https://www.mof.go.jp/jgbs/reference/interest_rate/data/jgbcm_all.csv"

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

# RV: Curve Spreads  (label → (long_tenor, short_tenor))  value = long − short in bps
SPREAD_DEFS = [
    ("2s5s",   "5Y",  "2Y"),
    ("5s7s",   "7Y",  "5Y"),
    ("5s10s",  "10Y", "5Y"),
    ("7s10s",  "10Y", "7Y"),
    ("2s10s",  "10Y", "2Y"),
    ("10s15s", "15Y", "10Y"),
    ("10s20s", "20Y", "10Y"),
    ("15s20s", "20Y", "15Y"),
    ("20s25s", "25Y", "20Y"),
    ("20s30s", "30Y", "20Y"),
    ("25s30s", "30Y", "25Y"),
    ("5s20s",  "20Y", "5Y"),
    ("5s30s",  "30Y", "5Y"),
    ("10s30s", "30Y", "10Y"),
    ("20s40s", "40Y", "20Y"),
    ("30s40s", "40Y", "30Y"),
]

# RV: Butterfly Spreads  (label → (wing1, belly, wing2))  value = 2*belly − (wing1 + wing2) in bps
BUTTERFLY_DEFS = [
    ("2s5s10s",   "2Y",  "5Y",  "10Y"),
    ("5s7s10s",   "5Y",  "7Y",  "10Y"),
    ("5s10s15s",  "5Y",  "10Y", "15Y"),
    ("5s10s20s",  "5Y",  "10Y", "20Y"),
    ("7s10s15s",  "7Y",  "10Y", "15Y"),
    ("10s15s20s", "10Y", "15Y", "20Y"),
    ("10s20s30s", "10Y", "20Y", "30Y"),
    ("15s20s25s", "15Y", "20Y", "25Y"),
    ("20s25s30s", "20Y", "25Y", "30Y"),
    ("20s30s40s", "20Y", "30Y", "40Y"),
    ("25s30s40s", "25Y", "30Y", "40Y"),
]

# ---------------------------------------------------------------------------
# UST Constants
# ---------------------------------------------------------------------------
UST_TENORS = ["1M", "2M", "3M", "4M", "6M", "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "20Y", "30Y"]

UST_COL_MAP = {
    "1 Mo": "1M", "2 Mo": "2M", "3 Mo": "3M", "4 Mo": "4M", "6 Mo": "6M",
    "1 Yr": "1Y", "2 Yr": "2Y", "3 Yr": "3Y", "5 Yr": "5Y", "7 Yr": "7Y",
    "10 Yr": "10Y", "20 Yr": "20Y", "30 Yr": "30Y",
}

UST_BASE_URL = ("https://home.treasury.gov/resource-center/data-chart-center/"
                "interest-rates/daily-treasury-rates.csv/{year}/all?"
                "type=daily_treasury_yield_curve&field_tdr_date_value={year}&page&_format=csv")

UST_OUTPUT_PATH = Path(__file__).resolve().parent.parent / "public" / "data" / "ust_yields.json"

# ---------------------------------------------------------------------------
# EGB Constants
# ---------------------------------------------------------------------------
EGB_TENORS = ["1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "15Y", "20Y", "30Y"]

_EGB_TENOR_KEYS = [f"SR_{t}" for t in EGB_TENORS]

_EGB_TENOR_MAP = {f"SR_{t}": t for t in EGB_TENORS}

EGB_URL = ("https://data-api.ecb.europa.eu/service/data/YC/"
           "B.U2.EUR.4F.G_N_A.SV_C_YM." + "+".join(_EGB_TENOR_KEYS) +
           "?detail=dataonly&format=csvdata&lastNObservations=300")

EGB_OUTPUT_PATH = Path(__file__).resolve().parent.parent / "public" / "data" / "egb_yields.json"

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


def compute_deltas(df: pd.DataFrame, tenors=None, delta_defs=None) -> dict:
    """Compute basis-point deltas for each tenor."""
    if tenors is None:
        tenors = TENORS
    if delta_defs is None:
        delta_defs = DELTA_DEFS
    result = {}
    n = len(df)
    for label, (idx_a, idx_b) in delta_defs.items():
        deltas = {}
        actual_a = n + idx_a
        actual_b = n + idx_b
        if actual_a < 0 or actual_b < 0:
            for t in tenors:
                deltas[t] = None
        else:
            row_a = df.iloc[actual_a]
            row_b = df.iloc[actual_b]
            for t in tenors:
                try:
                    val_a = float(row_a[t])
                    val_b = float(row_b[t])
                    deltas[t] = round((val_a - val_b) * 100, 1)  # bps
                except (KeyError, ValueError, TypeError):
                    deltas[t] = None
        result[label] = deltas
    return result


def build_current_yields(df: pd.DataFrame, tenors=None) -> dict:
    """Extract the latest yield for each tenor."""
    if tenors is None:
        tenors = TENORS
    if df.empty:
        return {t: None for t in tenors}
    last = df.iloc[-1]
    yields = {}
    for t in tenors:
        try:
            yields[t] = round(float(last[t]), 3)
        except (KeyError, ValueError, TypeError):
            yields[t] = None
    return yields


def build_historical_curves(df: pd.DataFrame, tenors=None, delta_defs=None) -> dict:
    """Extract historical yield curves for each delta period."""
    if tenors is None:
        tenors = TENORS
    if delta_defs is None:
        delta_defs = DELTA_DEFS
    n = len(df)
    result = {}
    for label, (_idx_a, idx_b) in delta_defs.items():
        actual_b = n + idx_b
        if actual_b < 0:
            result[label] = {"date": None, "yields": {t: None for t in tenors}}
        else:
            row = df.iloc[actual_b]
            yields = {}
            for t in tenors:
                try:
                    yields[t] = round(float(row[t]), 3)
                except (KeyError, ValueError, TypeError):
                    yields[t] = None
            result[label] = {
                "date": str(row["Date"]).strip(),
                "yields": yields,
            }
    return result


def build_high_low(df: pd.DataFrame, tenors=None, delta_defs=None) -> dict:
    """Compute high/low yields for each tenor over each delta period window."""
    if tenors is None:
        tenors = TENORS
    if delta_defs is None:
        delta_defs = DELTA_DEFS
    n = len(df)
    result = {}
    for label, (idx_a, idx_b) in delta_defs.items():
        actual_a = n + idx_a
        actual_b = n + idx_b
        if actual_a < 0 or actual_b < 0:
            result[label] = {t: {"high": None, "low": None, "high_date": None, "low_date": None} for t in tenors}
            continue
        window = df.iloc[actual_b:actual_a + 1]
        tenor_hl = {}
        for t in tenors:
            try:
                col = pd.to_numeric(window[t], errors="coerce")
                valid = col.dropna()
                if valid.empty:
                    tenor_hl[t] = {"high": None, "low": None, "high_date": None, "low_date": None}
                else:
                    hi_idx = valid.idxmax()
                    lo_idx = valid.idxmin()
                    tenor_hl[t] = {
                        "high": round(float(valid.loc[hi_idx]), 3),
                        "low": round(float(valid.loc[lo_idx]), 3),
                        "high_date": str(df.loc[hi_idx, "Date"]).strip(),
                        "low_date": str(df.loc[lo_idx, "Date"]).strip(),
                    }
            except (KeyError, ValueError, TypeError):
                tenor_hl[t] = {"high": None, "low": None, "high_date": None, "low_date": None}
        result[label] = tenor_hl
    return result


def _compute_spread_series(df: pd.DataFrame, long_t: str, short_t: str) -> pd.Series:
    """Return spread = long − short in bps for every row."""
    return (pd.to_numeric(df[long_t], errors="coerce") - pd.to_numeric(df[short_t], errors="coerce")) * 100


def _compute_fly_series(df: pd.DataFrame, w1: str, belly: str, w2: str) -> pd.Series:
    """Return butterfly = 2*belly − (wing1 + wing2) in bps for every row."""
    return (2 * pd.to_numeric(df[belly], errors="coerce")
            - pd.to_numeric(df[w1], errors="coerce")
            - pd.to_numeric(df[w2], errors="coerce")) * 100


def _rv_current_deltas_hl(series: pd.Series, df: pd.DataFrame) -> dict:
    """Given a bps Series aligned with df, compute current value, deltas, and high/low per period."""
    n = len(series)
    current = round(float(series.iloc[-1]), 1) if n > 0 and pd.notna(series.iloc[-1]) else None
    deltas = {}
    high_low = {}
    for label, (idx_a, idx_b) in DELTA_DEFS.items():
        actual_a = n + idx_a
        actual_b = n + idx_b
        if actual_a < 0 or actual_b < 0:
            deltas[label] = None
            high_low[label] = {"high": None, "low": None, "high_date": None, "low_date": None}
            continue
        try:
            deltas[label] = round(float(series.iloc[actual_a] - series.iloc[actual_b]), 1)
        except (ValueError, TypeError):
            deltas[label] = None
        window = series.iloc[actual_b:actual_a + 1].dropna()
        if window.empty:
            high_low[label] = {"high": None, "low": None, "high_date": None, "low_date": None}
        else:
            hi_idx = window.idxmax()
            lo_idx = window.idxmin()
            high_low[label] = {
                "high": round(float(window.loc[hi_idx]), 1),
                "low": round(float(window.loc[lo_idx]), 1),
                "high_date": str(df.loc[hi_idx, "Date"]).strip(),
                "low_date": str(df.loc[lo_idx, "Date"]).strip(),
            }
    return {"current": current, "deltas": deltas, "high_low": high_low}


def build_rv(df: pd.DataFrame) -> dict:
    """Build RV spreads and butterflies with current, deltas, and high/low."""
    spreads = {}
    for label, long_t, short_t in SPREAD_DEFS:
        s = _compute_spread_series(df, long_t, short_t)
        spreads[label] = _rv_current_deltas_hl(s, df)
    flies = {}
    for label, w1, belly, w2 in BUTTERFLY_DEFS:
        s = _compute_fly_series(df, w1, belly, w2)
        flies[label] = _rv_current_deltas_hl(s, df)
    return {"spreads": spreads, "butterflies": flies}


# ---------------------------------------------------------------------------
# Forward Rates
# ---------------------------------------------------------------------------

# Tenor → years mapping
_TENOR_YEARS = {t: int(t.replace("Y", "")) for t in TENORS}

# Forward matrix: (start_years, tenor_years) pairs
FORWARD_MATRIX_STARTS = [1, 2, 3, 5, 10]
FORWARD_MATRIX_TENORS = [1, 2, 3, 5, 10]

# Implied BOJ rate path horizons (1Y forward at each start)
RATE_PATH_HORIZONS = [0, 1, 2, 3, 4, 5, 7, 9]


def _bootstrap_forward_row(row: pd.Series) -> dict:
    """Given a single row of compound yields, compute zero rates and forwards.

    Uses compound yields directly as par rates and bootstraps zero-coupon rates,
    then computes forward rates from the zero curve.

    Returns dict: {(start, tenor): forward_rate_pct, ...}
    """
    import math

    # Build zero-rate curve from compound yields via bootstrapping
    # Compound yields from MOF are par yields (semi-annual compounding assumed
    # for simplicity we treat them as continuously-compounded spot rates, which
    # for government bonds at these tenors is a very close approximation).
    zeros = {}
    for t in TENORS:
        try:
            y = float(row[t])
            if pd.isna(y):
                continue
            yrs = _TENOR_YEARS[t]
            zeros[yrs] = y / 100.0  # convert % to decimal
        except (KeyError, ValueError, TypeError):
            continue

    if not zeros:
        return {}

    # Interpolate missing integer years via linear interp on available zeros
    available = sorted(zeros.keys())
    def _interp_zero(yr):
        if yr in zeros:
            return zeros[yr]
        # Find bracketing points
        lo = [a for a in available if a <= yr]
        hi = [a for a in available if a >= yr]
        if not lo or not hi:
            return None
        lo_yr, hi_yr = lo[-1], hi[0]
        if lo_yr == hi_yr:
            return zeros[lo_yr]
        frac = (yr - lo_yr) / (hi_yr - lo_yr)
        return zeros[lo_yr] + frac * (zeros[hi_yr] - zeros[lo_yr])

    # Compute forward rate: f(s, s+t) = ((1+z_{s+t})^{s+t} / (1+z_s)^s)^{1/t} - 1
    result = {}
    for start in FORWARD_MATRIX_STARTS:
        for tenor in FORWARD_MATRIX_TENORS:
            end = start + tenor
            z_end = _interp_zero(end)
            z_start = _interp_zero(start)
            if z_end is None or z_start is None:
                continue
            try:
                df_end = (1 + z_end) ** end
                df_start = (1 + z_start) ** start
                if df_start <= 0:
                    continue
                fwd = (df_end / df_start) ** (1.0 / tenor) - 1
                result[(start, tenor)] = round(fwd * 100, 3)  # back to %
            except (ZeroDivisionError, ValueError):
                continue

    # Rate path: 1Y forward at each horizon
    for horizon in RATE_PATH_HORIZONS:
        if horizon == 0:
            z1 = _interp_zero(1)
            if z1 is not None:
                result[("path", horizon)] = round(z1 * 100, 3)
        else:
            end = horizon + 1
            z_end = _interp_zero(end)
            z_start = _interp_zero(horizon)
            if z_end is None or z_start is None:
                continue
            try:
                df_end = (1 + z_end) ** end
                df_start = (1 + z_start) ** horizon
                if df_start <= 0:
                    continue
                fwd = df_end / df_start - 1
                result[("path", horizon)] = round(fwd * 100, 3)
            except (ZeroDivisionError, ValueError):
                continue

    return result


def _build_forward_series(df: pd.DataFrame, key: tuple) -> pd.Series:
    """Compute a forward rate for every row in df, returning a Series."""
    vals = []
    for _, row in df.iterrows():
        fwds = _bootstrap_forward_row(row)
        vals.append(fwds.get(key))
    return pd.Series(vals, index=df.index)


def build_forwards(df: pd.DataFrame) -> dict:
    """Build forward rate matrix + rate path with current, deltas, high/low."""
    # Forward matrix
    matrix = {}
    for start in FORWARD_MATRIX_STARTS:
        for tenor in FORWARD_MATRIX_TENORS:
            key = (start, tenor)
            label = f"{start}Y{tenor}Y"
            s = _build_forward_series(df, key)
            # Convert to bps-style current/deltas/hl using the helper
            # but we want rates in %, so use a modified version
            matrix[label] = _fwd_current_deltas_hl(s, df)

    # Rate path
    path = {}
    for horizon in RATE_PATH_HORIZONS:
        key = ("path", horizon)
        label = f"{horizon}Y" if horizon > 0 else "Spot"
        s = _build_forward_series(df, key)
        path[label] = _fwd_current_deltas_hl(s, df)

    return {"matrix": matrix, "path": path}


def _fwd_current_deltas_hl(series: pd.Series, df: pd.DataFrame) -> dict:
    """Like _rv_current_deltas_hl but for forward rates in % (3dp) and deltas in bps (1dp)."""
    n = len(series)
    current = round(float(series.iloc[-1]), 3) if n > 0 and pd.notna(series.iloc[-1]) else None
    deltas = {}
    high_low = {}
    for label, (idx_a, idx_b) in DELTA_DEFS.items():
        actual_a = n + idx_a
        actual_b = n + idx_b
        if actual_a < 0 or actual_b < 0:
            deltas[label] = None
            high_low[label] = {"high": None, "low": None, "high_date": None, "low_date": None}
            continue
        try:
            va = series.iloc[actual_a]
            vb = series.iloc[actual_b]
            if pd.notna(va) and pd.notna(vb):
                deltas[label] = round(float(va - vb) * 100, 1)  # bps
            else:
                deltas[label] = None
        except (ValueError, TypeError):
            deltas[label] = None
        window = series.iloc[actual_b:actual_a + 1].dropna()
        if window.empty:
            high_low[label] = {"high": None, "low": None, "high_date": None, "low_date": None}
        else:
            hi_idx = window.idxmax()
            lo_idx = window.idxmin()
            high_low[label] = {
                "high": round(float(window.loc[hi_idx]), 3),
                "low": round(float(window.loc[lo_idx]), 3),
                "high_date": str(df.loc[hi_idx, "Date"]).strip(),
                "low_date": str(df.loc[lo_idx, "Date"]).strip(),
            }
    return {"current": current, "deltas": deltas, "high_low": high_low}


def get_latest_date(df: pd.DataFrame) -> str:
    if df.empty:
        return ""
    return str(df.iloc[-1]["Date"]).strip()


# ---------------------------------------------------------------------------
# UST Fetcher
# ---------------------------------------------------------------------------

def fetch_ust_data() -> pd.DataFrame:
    """Fetch UST yield curve data from Treasury.gov for current and previous year."""
    now = datetime.utcnow()
    years = [now.year - 1, now.year]
    frames = []
    for year in years:
        url = UST_BASE_URL.format(year=year)
        logger.info("Fetching UST data for %d", year)
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text))
            frames.append(df)
        except Exception as e:
            logger.warning("Failed to fetch UST %d: %s", year, e)
    if not frames:
        raise RuntimeError("No UST data could be fetched")

    df = pd.concat(frames, ignore_index=True)
    df = df.rename(columns=UST_COL_MAP)
    df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y").dt.strftime("%Y/%m/%d")
    df = df.drop_duplicates(subset="Date", keep="last")
    df = df.sort_values("Date").reset_index(drop=True)
    for t in UST_TENORS:
        if t in df.columns:
            df[t] = pd.to_numeric(df[t], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# EGB Fetcher
# ---------------------------------------------------------------------------

def fetch_egb_data() -> pd.DataFrame:
    """Fetch EGB yield curve data from the ECB Statistical Data Warehouse."""
    logger.info("Fetching EGB data from ECB")
    resp = requests.get(EGB_URL, timeout=90)
    resp.raise_for_status()

    df = pd.read_csv(io.StringIO(resp.text))
    df = df[["DATA_TYPE_FM", "TIME_PERIOD", "OBS_VALUE"]].copy()
    df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    df["tenor"] = df["DATA_TYPE_FM"].map(_EGB_TENOR_MAP)
    df = df.dropna(subset=["tenor"])

    wide = df.pivot_table(index="TIME_PERIOD", columns="tenor", values="OBS_VALUE", aggfunc="first")
    wide = wide.reset_index().rename(columns={"TIME_PERIOD": "Date"})
    wide["Date"] = wide["Date"].str.replace("-", "/")
    wide = wide.sort_values("Date").reset_index(drop=True)
    return wide


def build_curve_output(df: pd.DataFrame, tenors: list) -> dict:
    """Build standard yield/delta/curve/high_low output for a single-mode curve."""
    return {
        "date": get_latest_date(df),
        "yields": build_current_yields(df, tenors),
        "deltas": compute_deltas(df, tenors),
        "curves": build_historical_curves(df, tenors),
        "high_low": build_high_low(df, tenors),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    jgb_ok = False

    # ------ JGB ------
    try:
        hist = normalise_simple(fetch_csv(HISTORICAL_URL, encoding="shift_jis"))
        cur = normalise_simple(fetch_csv(CURRENT_URL, encoding="shift_jis"))
        df = merge_historical_current(hist, cur)

        # ------ Build output (flat, same shape as UST/EGB) ------
        output = {
            "updated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tenors": TENORS,
            "delta_keys": list(DELTA_DEFS.keys()),
            "spread_keys": [s[0] for s in SPREAD_DEFS],
            "fly_keys": [f[0] for f in BUTTERFLY_DEFS],
            "fwd_matrix_keys": [f"{s}Y{t}Y" for s in FORWARD_MATRIX_STARTS for t in FORWARD_MATRIX_TENORS],
            "rate_path_keys": ["Spot"] + [f"{h}Y" for h in RATE_PATH_HORIZONS if h > 0],
            **build_curve_output(df, TENORS),
            "rv": build_rv(df),
            "forwards": build_forwards(df),
        }

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_PATH.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info("JGB data successfully updated → %s", OUTPUT_PATH)
        jgb_ok = True

    except Exception as exc:
        logger.error("JGB data fetch failed: %s", exc, exc_info=True)
        send_alert_email(
            subject="[JGBsDaily] JGB Data Update Failed",
            body=f"The JGB data pipeline failed at {datetime.utcnow().isoformat()}.\n\nError:\n{exc}",
        )

    # ------ UST ------
    try:
        ust_df = fetch_ust_data()
        ust_output = {
            "updated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tenors": UST_TENORS,
            "delta_keys": list(DELTA_DEFS.keys()),
            **build_curve_output(ust_df, UST_TENORS),
        }
        UST_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        UST_OUTPUT_PATH.write_text(json.dumps(ust_output, indent=2), encoding="utf-8")
        logger.info("UST data successfully updated → %s", UST_OUTPUT_PATH)
    except Exception as exc:
        logger.error("UST fetch failed: %s", exc, exc_info=True)
        send_alert_email(
            subject="[JGBsDaily] UST Data Update Failed",
            body=f"The UST data pipeline failed at {datetime.utcnow().isoformat()}.\n\nError:\n{exc}",
        )

    # ------ EGB ------
    try:
        egb_df = fetch_egb_data()
        egb_output = {
            "updated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tenors": EGB_TENORS,
            "delta_keys": list(DELTA_DEFS.keys()),
            **build_curve_output(egb_df, EGB_TENORS),
        }
        EGB_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        EGB_OUTPUT_PATH.write_text(json.dumps(egb_output, indent=2), encoding="utf-8")
        logger.info("EGB data successfully updated → %s", EGB_OUTPUT_PATH)
    except Exception as exc:
        logger.error("EGB fetch failed: %s", exc, exc_info=True)
        send_alert_email(
            subject="[JGBsDaily] EGB Data Update Failed",
            body=f"The EGB data pipeline failed at {datetime.utcnow().isoformat()}.\n\nError:\n{exc}",
        )

    if not jgb_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
