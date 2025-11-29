from fastapi import FastAPI
from pydantic import BaseModel
import math
from fastapi.middleware.cors import CORSMiddleware

# Catalyst Engine (NEW IMPORT)
from catalyst_impact import (
    CatalystInstance,
    compute_catalyst_score_for_parcel,
)

app = FastAPI()

# Allow frontend (unchanged behavior)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------- Catalyst Instances (NEW) ----------------
CATALYSTS = [
    CatalystInstance(
        id="gm_ultium",
        type_id="ev_gigafactory",
        name="GM Ultium Lansing",
        lat=42.708,
        lng=-84.668,
    ),
    CatalystInstance(
        id="honda_ev_oh",
        type_id="auto_tier1",
        name="Honda EV Ohio",
        lat=40.236,
        lng=-83.367,
    ),
    CatalystInstance(
        id="panasonic_ks",
        type_id="ev_gigafactory",
        name="Panasonic Kansas",
        lat=38.964,
        lng=-94.97,
    ),
]

# ---------------- INPUT SCHEMA (YOUR ORIGINAL MODEL + lat/lng added) ----------------
class PropertyInput(BaseModel):
    # NEW fields
    lat: float
    lng: float

    # ORIGINAL fields preserved exactly
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


# ---------------- SCORING ENGINE (YOUR ORIGINAL MATH + catalyst decay added) ----------------
@app.post("/score")
def score_property(p: PropertyInput):

    # NEW: Catalyst distance-decay impact
    catalyst_decay_impact = compute_catalyst_score_for_parcel(
        parcel_lat=p.lat,
        parcel_lng=p.lng,
        catalysts=CATALYSTS,
    )

    # ---------------- ORIGINAL MODEL (UNCHANGED) ----------------

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

    # ---------------- ORIGINAL FINAL POI FORMULA (UNCHANGED) ----------------
    poi_raw = (
        0.25 * value_anomaly +
        0.20 * catalyst_adj +   # ← unchanged
        0.15 * asset_upside +
        0.15 * market_momentum +
        0.10 * incentive_score -
        0.15 * risk_penalty
    )

    poi = round(100 * poi_raw)

    # ORIGINAL tier logic
    if poi >= 75:
        tier = "Gold"
    elif poi >= 50:
        tier = "Silver"
    else:
        tier = "Bronze"

    # ---------------- RETURN (All original fields + NEW catalyst_decay_impact) ----------------
    return {
        "poi": poi,
        "tier": tier,
        "value_anomaly": value_anomaly,
        "catalyst_adj": catalyst_adj,             # ← original score
        "catalyst_decay_impact": catalyst_decay_impact,  # ← added, does NOT affect POI
        "asset_upside": asset_upside,
        "market_momentum": market_momentum,
        "incentive_score": incentive_score,
        "risk_penalty": risk_penalty
    }
