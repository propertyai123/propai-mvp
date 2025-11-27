from fastapi import FastAPI
from pydantic import BaseModel
import math

app = FastAPI()

# ---- INPUT SCHEMA ----
class PropertyInput(BaseModel):
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


# ---- ROUTE ----
@app.post("/score")
def score_property(p: PropertyInput):

    # Value Anomaly
    value_anomaly = (
        0.50 * p.price_anomaly +
        0.30 * p.replacement_delta +
        0.15 * p.historical_delta +
        0.05 * p.dom_score
    )

    # Catalyst Base
    catalyst_base = (
        0.40 * p.distance_score +
        0.25 * p.capex_score +
        0.20 * p.jobs_score +
        0.10 * p.sector_rel +
        0.03 * p.cluster +
        0.02 * p.media_tone
    )

    catalyst_adj = catalyst_base * (1 + p.recency_multiplier)

    # Asset Upside
    asset_upside = (
        0.45 * p.zoning_flex +
        0.35 * p.utilities +
        0.20 * p.topo_index
    )

    # Market Momentum
    market_momentum = (
        0.30 * p.job_growth +
        0.25 * p.permits +
        0.20 * p.population +
        0.15 * p.traffic +
        0.05 * p.macro_cycle +
        0.05 * p.inst_cluster
    )

    # Incentives
    incentive_score = (
        0.45 * p.oz +
        0.30 * p.hub +
        0.25 * p.tif
    )

    # Risk
    risk_penalty = (
        0.40 * p.crime +
        0.35 * p.flood +
        0.15 * p.wildfire +
        0.10 * p.epa
    )

    # Final POI
    poi_raw = (
        0.25 * value_anomaly +
        0.20 * catalyst_adj +
        0.15 * asset_upside +
        0.15 * market_momentum +
        0.10 * incentive_score -
        0.15 * risk_penalty
    )

    poi = round(100 * poi_raw)

    # TIER
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
        "asset_upside": asset_upside,
        "market_momentum": market_momentum,
        "incentive_score": incentive_score,
        "risk_penalty": risk_penalty
    }

