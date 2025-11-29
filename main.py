from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import math
import os

# Catalyst Engine
from catalyst_impact import (
    CatalystInstance,
    compute_catalyst_score_for_parcel,
)

# Supabase Client
from supabase import create_client

app = FastAPI()

# ---------------- SUPABASE CONNECTION ----------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Allow frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or list specific frontend domains
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)



# ---------------- CATALYST LOADER ----------------
def load_catalysts_from_supabase():
    response = supabase.table("catalysts").select("*").execute()
    rows = response.data
    catalysts = []

    for row in rows:
        radius = float(row.get("radius_miles") or 10)

        r_peak = radius
        r_max = radius * 2.5
        decay_k = max(1.0, radius / 3)

        capex = row.get("capex_usd") or 0
        jobs = row.get("jobs_estimated") or 0

        if capex > 0:
            base_strength = max(0.5, math.log10(capex))
        elif jobs > 0:
            base_strength = max(0.5, jobs / 500)
        else:
            base_strength = 1.0

        catalysts.append(CatalystInstance(
            id=row["id"],
            type_id=row["type"],
            name=row["name"],
            lat=row["lat"],
            lng=row["lng"],
            r_peak_miles=r_peak,
            r_max_miles=r_max,
            decay_k_miles=decay_k,
            base_strength=base_strength,
        ))

    return catalysts


# Load once at startup
CATALYSTS = load_catalysts_from_supabase()


# ---------------- INPUT SCHEMA ----------------
class PropertyInput(BaseModel):
    lat: float
    lng: float

    price_anomaly: float
    replacement_delta: float
    historical_delta: float
    dom_score: float

    distance_score: float
    capex_score: float
    jobs_score: float
    sector_rel: float
    cluster: float
    media_tone: float
    recency_multiplier: float

    zoning_flex: float
    utilities: float
    topo_index: float

    job_growth: float
    permits: float
    population: float
    traffic: float
    macro_cycle: float
    inst_cluster: float

    oz: float
    hub: float
    tif: float

    crime: float
    flood: float
    wildfire: float
    epa: float


# ---------------- SCORING ENGINE ----------------
@app.post("/score")
def score_property(p: PropertyInput):

    catalyst_decay_impact = compute_catalyst_score_for_parcel(
        parcel_lat=p.lat,
        parcel_lng=p.lng,
        catalysts=CATALYSTS,
    )

    value_anomaly = (
        0.50 * p.price_anomaly +
        0.30 * p.replacement_delta +
        0.15 * p.historical_delta +
        0.05 * p.dom_score
    )

    catalyst_base = (
        0.40 * p.distance_score +
        0.25 * p.capex_score +
        0.20 * p.jobs_score +
        0.10 * p.sector_rel +
        0.03 * p.cluster +
        0.02 * p.media_tone
    )

    catalyst_adj = catalyst_base * (1 + p.recency_multiplier)

    asset_upside = (
        0.45 * p.zoning_flex +
        0.35 * p.utilities +
        0.20 * p.topo_index
    )

    market_momentum = (
        0.30 * p.job_growth +
        0.25 * p.permits +
        0.20 * p.population +
        0.15 * p.traffic +
        0.05 * p.macro_cycle +
        0.05 * p.inst_cluster
    )

    incentive_score = (
        0.45 * p.oz +
        0.30 * p.hub +
        0.25 * p.tif
    )

    risk_penalty = (
        0.40 * p.crime +
        0.35 * p.flood +
        0.15 * p.wildfire +
        0.10 * p.epa
    )

    poi_raw = (
        0.25 * value_anomaly +
        0.20 * catalyst_adj +
        0.15 * asset_upside +
        0.15 * market_momentum +
        0.10 * incentive_score -
        0.15 * risk_penalty
    )

    poi = round(100 * poi_raw)

    if poi >= 75:
        tier = "Gold"
    elif poi >= 50:
        tier = "Silver"
    else:
        tier = "Bronze"

    return {
        "poi": poi,
        "tier": tier,
        "value_anomaly": value_anomaly,
        "catalyst_adj": catalyst_adj,
        "catalyst_decay_impact": catalyst_decay_impact,
        "asset_upside": asset_upside,
        "market_momentum": market_momentum,
        "incentive_score": incentive_score,
        "risk_penalty": risk_penalty,
    }
