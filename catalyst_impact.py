# backend/catalyst_impact.py

import math
from dataclasses import dataclass
from typing import List, Dict


# ---------------------------------------------
# Catalyst Instance (from Supabase)
# ---------------------------------------------
@dataclass
class CatalystInstance:
    id: str
    type_id: str
    name: str
    lat: float
    lng: float

    # Dynamic spatial impact parameters
    r_peak_miles: float
    r_max_miles: float
    decay_k_miles: float
    base_strength: float


# ---------------------------------------------
# OPTIONAL: Legacy Type Profiles (still useful)
# ---------------------------------------------
@dataclass
class CatalystTypeProfile:
    type_id: str
    label: str
    r_peak_miles: float
    r_max_miles: float
    decay_k_miles: float
    base_strength: float


# Your old type catalog — unchanged (kept for future if needed)
CATALYST_TYPES: Dict[str, CatalystTypeProfile] = {
    "ev_gigafactory": CatalystTypeProfile(
        type_id="ev_gigafactory",
        label="EV / Battery Gigafactory",
        r_peak_miles=10.0,
        r_max_miles=60.0,
        decay_k_miles=8.0,
        base_strength=1.0,
    ),
    "auto_tier1": CatalystTypeProfile(
        type_id="auto_tier1",
        label="Automotive Tier-1 Supplier",
        r_peak_miles=5.0,
        r_max_miles=20.0,
        decay_k_miles=4.0,
        base_strength=0.8,
    ),
    "semiconductor_fab": CatalystTypeProfile(
        type_id="semiconductor_fab",
        label="Semiconductor Fab",
        r_peak_miles=15.0,
        r_max_miles=70.0,
        decay_k_miles=10.0,
        base_strength=1.1,
    ),
    "logistics_hub": CatalystTypeProfile(
        type_id="logistics_hub",
        label="Logistics Hub",
        r_peak_miles=4.0,
        r_max_miles=15.0,
        decay_k_miles=3.0,
        base_strength=0.9,
    ),
    # ... (rest unchanged)
}


# ---------------------------------------------
# Haversine Distance
# ---------------------------------------------
def haversine_miles(lat1, lon1, lat2, lon2):
    R_km = 6371.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (
        math.sin(d_lat / 2)**2 +
        math.cos(math.radians(lat1)) *
        math.cos(math.radians(lat2)) *
        math.sin(d_lon / 2)**2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    km = R_km * c
    return km * 0.621371  # convert to miles


# ---------------------------------------------
# Impact Decay Function
# ---------------------------------------------
def impact_weight(distance_miles, r_peak, r_max, k):
    if distance_miles <= r_peak:
        return 1.0
    if distance_miles >= r_max:
        return 0.0
    return math.exp(-(distance_miles - r_peak) / max(k, 1e-6))


# ---------------------------------------------
# Scoring Function Using Dynamic Catalysts
# ---------------------------------------------
def compute_catalyst_score_for_parcel(
    parcel_lat: float,
    parcel_lng: float,
    catalysts: List[CatalystInstance],
) -> float:

    if not catalysts:
        return 0.0

    total = 0.0
    strength_sum = 0.0

    for c in catalysts:
        # Use dynamic parameters from CatalystInstance
        d = haversine_miles(parcel_lat, parcel_lng, c.lat, c.lng)
        w = impact_weight(d, c.r_peak_miles, c.r_max_miles, c.decay_k_miles)

        if w <= 0:
            continue

        total += w * c.base_strength
        strength_sum += c.base_strength

    if strength_sum == 0:
        return 0.0

    score = total / strength_sum
    return max(0.0, min(score, 1.5))
