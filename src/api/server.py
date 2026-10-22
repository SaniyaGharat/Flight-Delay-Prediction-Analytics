#!/usr/bin/env python3
"""
src/api/server.py

FastAPI Backend Server for Flight Delay Analytics:
- Historical delay-rate lookup endpoint (/api/predict) computed directly from CSV data:
  delay rate = delayed_arr_15 / flown_flights for the selected airline, origin airport, and departure hour.
- Labeled: "Historical delay rate (not a model forecast)"
- Returns "no data" when flown_flights is below 100.
- High-level KPI aggregations (/api/kpi/summary)
- Carrier and airport directory lookups (/api/airlines, /api/airports)
- Machine Learning models metrics summary (/api/models)
- Static dataset serving (/data/*) with full CORS support
"""

import os
import json
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import pandas as pd

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "frontend" / "public" / "data"
if not DATA_DIR.exists():
    DATA_DIR = PROJECT_ROOT / "output" / "powerbi"

app = FastAPI(
    title="Flight Delay Analytics Backend API",
    description="REST API service providing flight delay analytics, metrics, and historical delay rate lookups.",
    version="1.0.0"
)

# Enable CORS for frontend development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory caches for lookups
_AIRLINES_CACHE: Optional[List[Dict[str, Any]]] = None
_AIRPORTS_CACHE: Optional[List[Dict[str, Any]]] = None
_AIRLINE_ORIGIN_HOUR_CACHE: Dict[Tuple[str, str, int], Dict[str, Any]] = {}

def get_data_file(filename: str) -> Path:
    fpath = DATA_DIR / filename
    if not fpath.is_file():
        raise HTTPException(status_code=404, detail=f"Data file '{filename}' not found. Run sync_frontend.py first.")
    return fpath

def load_lookups():
    global _AIRLINES_CACHE, _AIRPORTS_CACHE, _AIRLINE_ORIGIN_HOUR_CACHE
    if _AIRLINES_CACHE is not None and _AIRLINE_ORIGIN_HOUR_CACHE:
        return
        
    # 1. Load airlines
    try:
        airline_file = get_data_file("dim_airline.csv")
        df_air = pd.read_csv(airline_file).fillna("")
        _AIRLINES_CACHE = df_air.to_dict(orient="records")
    except Exception:
        _AIRLINES_CACHE = []

    # 2. Load airports
    try:
        airport_file = get_data_file("airport_origin_totals.csv")
        df_port = pd.read_csv(airport_file).fillna("")
        _AIRPORTS_CACHE = df_port.to_dict(orient="records")
    except Exception:
        _AIRPORTS_CACHE = []

    # 3. Load historical airline x origin x hour table
    try:
        table_path = get_data_file("fact_airline_origin_hour.csv")
        df_aoh = pd.read_csv(table_path).fillna(0)
        cache: Dict[Tuple[str, str, int], Dict[str, Any]] = {}
        for _, row in df_aoh.iterrows():
            carrier = str(row["AIRLINE"]).strip().upper()
            origin = str(row["ORIGIN_AIRPORT"]).strip().upper()
            hour = int(row["SCHED_DEP_HOUR"])
            cache[(carrier, origin, hour)] = {
                "total_flights": int(row.get("total_flights", 0)),
                "flown_flights": int(row.get("flown_flights", 0)),
                "delayed_arr_15": int(float(row.get("delayed_arr_15", 0)))
            }
        _AIRLINE_ORIGIN_HOUR_CACHE = cache
        print(f"[INFO] Loaded {len(_AIRLINE_ORIGIN_HOUR_CACHE):,} historical airline x origin x hour segments.")
    except Exception as e:
        print(f"[WARNING] Could not load fact_airline_origin_hour.csv: {e}")
        _AIRLINE_ORIGIN_HOUR_CACHE = {}

# ---------------------------------------------------------------------------
# Request / Response Models
# ---------------------------------------------------------------------------

class HistoricalLookupRequest(BaseModel):
    airline: str = Field(..., description="2-letter carrier code (e.g. 'AA', 'DL', 'UA', 'WN')", example="AA")
    origin: str = Field(..., description="3-letter origin IATA code (e.g. 'ORD', 'ATL', 'JFK')", example="ORD")
    dest: Optional[str] = Field(None, description="Optional 3-letter destination IATA code", example="LAX")
    sched_dep_hour: int = Field(..., ge=0, le=23, description="Scheduled departure hour (0-23)", example=17)

class HistoricalLookupResponse(BaseModel):
    label: str = "Historical delay rate (not a model forecast)"
    airline: str
    origin: str
    sched_dep_hour: int
    flown_flights: int
    delayed_arr_15: int
    total_flights: int
    delay_rate: Optional[float] = None
    has_sufficient_data: bool
    status: str  # "available" or "no data"
    description: str

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.on_event("startup")
def startup_event():
    load_lookups()

@app.get("/api/health")
def health_check():
    return {
        "status": "online",
        "service": "Flight Delay Analytics Backend",
        "dataset_flown_rows": 5714008,
        "dataset_total_rows": 5819079,
        "source_connected": DATA_DIR.exists()
    }

@app.get("/api/kpi/summary")
def get_kpi_summary():
    return {
        "total_flights": 5819079,
        "flown_flights": 5714008,
        "cancelled_flights": 89884,
        "diverted_flights": 15187,
        "delayed_arr_15": 1063439,
        "delayed_dep_15": 1050422,
        "cancel_rate": 0.0154,
        "delay_rate": 0.1861,
        "on_time_rate": 0.8139,
        "avg_arr_delay_min": 4.41,
        "data_year": 2015,
        "total_cause_minutes": 62651867
    }

@app.get("/api/models")
def get_models():
    fpath = get_data_file("ml_summary.json")
    with open(fpath, "r", encoding="utf-8") as f:
        return json.load(f)

@app.get("/api/airlines")
def get_airlines():
    load_lookups()
    return _AIRLINES_CACHE or []

@app.get("/api/airports")
def get_airports(limit: int = Query(500, ge=1, le=1000)):
    load_lookups()
    return (_AIRPORTS_CACHE or [])[:limit]

@app.post("/api/predict", response_model=HistoricalLookupResponse)
def get_historical_delay_rate(req: HistoricalLookupRequest):
    load_lookups()
    carrier = req.airline.strip().upper()
    origin = req.origin.strip().upper()
    hour = int(req.sched_dep_hour)

    key = (carrier, origin, hour)
    data = _AIRLINE_ORIGIN_HOUR_CACHE.get(key)

    if not data:
        return HistoricalLookupResponse(
            label="Historical delay rate (not a model forecast)",
            airline=carrier,
            origin=origin,
            sched_dep_hour=hour,
            flown_flights=0,
            delayed_arr_15=0,
            total_flights=0,
            delay_rate=None,
            has_sufficient_data=False,
            status="no data",
            description="no data (0 flights recorded for this carrier, origin airport, and hour in 2015)"
        )

    flown = data["flown_flights"]
    delayed = data["delayed_arr_15"]
    total = data["total_flights"]

    # Threshold: show "no data" when flown_flights is below 100
    if flown < 100:
        return HistoricalLookupResponse(
            label="Historical delay rate (not a model forecast)",
            airline=carrier,
            origin=origin,
            sched_dep_hour=hour,
            flown_flights=flown,
            delayed_arr_15=delayed,
            total_flights=total,
            delay_rate=None,
            has_sufficient_data=False,
            status="no data",
            description=f"no data (only {flown} flown flights recorded; threshold is 100 flights)"
        )

    rate = round(delayed / flown, 4)
    pct_str = f"{rate * 100:.1f}%"
    return HistoricalLookupResponse(
        label="Historical delay rate (not a model forecast)",
        airline=carrier,
        origin=origin,
        sched_dep_hour=hour,
        flown_flights=flown,
        delayed_arr_15=delayed,
        total_flights=total,
        delay_rate=rate,
        has_sufficient_data=True,
        status="available",
        description=f"Historical delay rate: {pct_str} ({delayed:,} delayed / {flown:,} flown flights)"
    )

# Mount static data directory so frontend can fetch directly if needed
if DATA_DIR.exists():
    app.mount("/data", StaticFiles(directory=str(DATA_DIR)), name="data")

def main():
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "0.0.0.0")
    print(f"[START] Starting Flight Delay Analytics Backend API at http://{host}:{port}")
    uvicorn.run("src.api.server:app", host=host, port=port, reload=True)

if __name__ == "__main__":
    main()
