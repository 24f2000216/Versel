from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import numpy as np
import pandas as pd
import json

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load sample telemetry once (replace with your CSV or JSON)
telemetry = pd.read_csv("telemetry.csv")

@app.post("/")
async def check_latency(request: Request):
    body = await request.json()
    regions = body.get("regions", [])
    threshold = body.get("threshold_ms", 180)

    result = {}
    for region in regions:
        df = telemetry[telemetry["region"] == region]
        if df.empty:
            continue

        avg_latency = df["latency_ms"].mean()
        p95_latency = np.percentile(df["latency_ms"], 95)
        avg_uptime = df["uptime"].mean()
        breaches = (df["latency_ms"] > threshold).sum()

        result[region] = {
            "avg_latency": round(avg_latency, 2),
            "p95_latency": round(p95_latency, 2),
            "avg_uptime": round(avg_uptime, 3),
            "breaches": int(breaches)
        }

    return result
