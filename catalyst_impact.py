# backend/catalyst_impact.py

import math
from dataclasses import dataclass
from typing import List, Dict


@dataclass
class CatalystTypeProfile:
    type_id: str
    label: str
    r_peak_miles: float      # full impact until this radius
    r_max_miles: float       # zero impact beyond this
    decay_k_miles: float     # controls how fast impact decays
    base_strength: float     # relative importance vs other types


@dataclass
class CatalystInstance:
    id: str
    type_id: str
    name: str
    lat: float
    lng: float


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Great-circle distance between two points, in miles.
    """
    R_km = 6371.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(d_lon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    km = R_km * c
    miles = km * 0.621371
    return miles


def impact_weight(distance_miles: float, r_peak: float, r_max: float, k: float) -> float:
    """
    Generic radial impact function:
    - Full impact inside r_peak
    - Exponential decay between r_peak and r_max
    - Zero impact beyond r_max
    """
    if distance_miles <= r_peak:
        return 1.0
    if distance_miles >= r_max:
        return 0.0
    # smooth exponential tail
    return math.exp(-(distance_miles - r_peak) / max(k, 1e-6))


# ---- TYPE CATALOG: this is your proprietary sauce ----

CATALYST_TYPES: Dict[str, CatalystTypeProfile] = {
    # EV / Battery
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

    # Semiconductors
    "semiconductor_fab": CatalystTypeProfile(
        type_id="semiconductor_fab",
        label="Semiconductor Fab",
        r_peak_miles=15.0,
        r_max_miles=70.0,
        decay_k_miles=10.0,
        base_strength=1.1,
    ),

    # Logistics & distribution
    "logistics_hub": CatalystTypeProfile(
        type_id="logistics_hub",
        label="Logistics / Intermodal Hub",
        r_peak_miles=4.0,
        r_max_miles=15.0,
        decay_k_miles=3.0,
        base_strength=0.9,
    ),
    "fulfillment_center": CatalystTypeProfile(
        type_id="fulfillment_center",
        label="E-commerce Fulfillment Center",
        r_peak_miles=5.0,
        r_max_miles=20.0,
        decay_k_miles=4.0,
        base_strength=0.7,
    ),

    # Ports / rail / airports
    "port": CatalystTypeProfile(
        type_id="port",
        label="Port / Marine Terminal",
        r_peak_miles=10.0,
        r_max_miles=50.0,
        decay_k_miles=8.0,
        base_strength=1.0,
    ),
    "rail_terminal": CatalystTypeProfile(
        type_id="rail_terminal",
        label="Rail / Intermodal Terminal",
        r_peak_miles=6.0,
        r_max_miles=25.0,
        decay_k_miles=5.0,
        base_strength=0.8,
    ),
    "airport": CatalystTypeProfile(
        type_id="airport",
        label="Commercial Airport",
        r_peak_miles=8.0,
        r_max_miles=35.0,
        decay_k_miles=6.0,
        base_strength=0.9,
    ),

    # Energy / utilities
    "power_plant": CatalystTypeProfile(
        type_id="power_plant",
        label="Power Plant / Major Substation",
        r_peak_miles=5.0,
        r_max_miles=25.0,
        decay_k_miles=4.0,
        base_strength=0.9,
    ),

    # Civic / gov / institutional
    "government_complex": CatalystTypeProfile(
        type_id="government_complex",
        label="Government / Civic Complex",
        r_peak_miles=3.0,
        r_max_miles=15.0,
        decay_k_miles=3.0,
        base_strength=0.7,
    ),
    "university_cluster": CatalystTypeProfile(
        type_id="university_cluster",
        label="University / Research Cluster",
        r_peak_miles=5.0,
        r_max_miles=25.0,
        decay_k_miles=5.0,
        base_strength=0.85,
    ),

    # Tech / data
    "data_center_cluster": CatalystTypeProfile(
        type_id="data_center_cluster",
        label="Data Center Cluster",
        r_peak_miles=3.0,
        r_max_miles=12.0,
        decay_k_miles=2.5,
        base_strength=0.8,
    ),
}


def compute_catalyst_score_for_parcel(
    parcel_lat: float,
    parcel_lng: float,
    catalysts: List[CatalystInstance],
) -> float:
    """
    Aggregate catalyst impact for a single parcel.
    Returns a normalized score between 0 and 1 (approx).
    """
    if not catalysts:
        return 0.0

    total = 0.0
    strength_sum = 0.0

    for c in catalysts:
        profile = CATALYST_TYPES.get(c.type_id)
        if not profile:
            continue

        d = haversine_miles(parcel_lat, parcel_lng, c.lat, c.lng)
        w = impact_weight(d, profile.r_peak_miles, profile.r_max_miles, profile.decay_k_miles)
        if w <= 0:
            continue

        total += w * profile.base_strength
        strength_sum += profile.base_strength

    if strength_sum == 0:
        return 0.0

    # normalize roughly into 0–1 range
    score = total / strength_sum
    return max(0.0, min(score, 1.5))  # allow >1 slightly; can cap at 1 if you prefer
