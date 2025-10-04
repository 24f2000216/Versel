# app.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
from pathlib import Path
import pandas as pd
import numpy as np

app = FastAPI(title="Telemetry Latency Checker")

# Allow POST from any origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST"],
    allow_headers=["*"],
)

# Path to the CSV you will include in repo
DATA_PATH = Path(__file__).parent / "data" / "telemetry.csv"

# Try to load at startup (if missing, we still start but the endpoint will report a clear error)
try:
    _df = pd.read_csv(DATA_PATH)
except Exception as e:
    _df = None
    _load_error = str(e)
else:
    _load_error = None

class Payload(BaseModel):
    regions: List[str]
    threshold_ms: float

def _find_column(df: pd.DataFrame, candidates: List[str]) -> str | None:
    lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in lower:
            return lower[cand]
    # also try substring match
    for col in df.columns:
        for cand in candidates:
            if cand in col.lower():
                return col
    return None

@app.post("/api/latency")
def latency_check(payload: Payload) -> Dict[str, Any]:
    if _df is None:
        raise HTTPException(status_code=500, detail=f"Telemetry CSV not loaded: {_load_error}")

    df = _df.copy()

    # Try to auto-detect expected columns:
    latency_col = _find_column(df, ["latency_ms", "latency", "rtt", "ping_ms", "ping"])
    region_col = _find_column(df, ["region", "region_name", "area", "location"])
    uptime_col = _find_column(df, ["uptime", "availability", "up", "availability_pct"])

    if latency_col is None:
        raise HTTPException(status_code=400, detail="Could not detect latency column (expected names: latency_ms, latency, rtt, ping_ms)")
    if region_col is None:
        raise HTTPException(status_code=400, detail="Could not detect region column (expected names: region, region_name, area)")

    # Ensure numeric latency
    df[latency_col] = pd.to_numeric(df[latency_col], errors="coerce")
    if uptime_col is not None:
        df[uptime_col] = pd.to_numeric(df[uptime_col], errors="coerce")

    # Normalize region comparisons (support both string and id)
    # Convert region column to string for case-insensitive compare if it's object dtype
    if df[region_col].dtype == object:
        df["_region_norm"] = df[region_col].astype(str).str.strip().str.lower()
    else:
        # numeric region IDs: keep as-is but cast to string when matching payload values
        df["_region_norm"] = df[region_col].astype(str)

    out: Dict[str, Any] = {}
    for region in payload.regions:
        region_key = str(region).strip().lower()
        region_df = df[df["_region_norm"] == region_key]

        if region_df.empty:
            out[region] = {
                "avg_latency": None,
                "p95_latency": None,
                "avg_uptime": None,
                "breaches": 0,
            }
            continue

        # compute metrics
        avg_latency = float(region_df[latency_col].mean())
        p95_latency = float(region_df[latency_col].quantile(0.95))

        # uptime normalization: if values look like 0..1 we keep them, if >1 (0..100), we convert to 0..1
        avg_uptime = None
        if uptime_col is not None:
            avg_uptime_raw = float(region_df[uptime_col].mean())
            if avg_uptime_raw > 1.5:  # likely 0..100 percent
                avg_uptime = avg_uptime_raw / 100.0
            else:
                avg_uptime = avg_uptime_raw

        breaches = int((region_df[latency_col] > payload.threshold_ms).sum())

        out[region] = {
            "avg_latency": round(avg_latency, 3),
            "p95_latency": round(p95_latency, 3),
            "avg_uptime": round(avg_uptime, 6) if avg_uptime is not None else None,
            "breaches": breaches,
        }

    return out
