# state_incentives.py
#
# Rule-based loader for state economic development / incentive projects.
# Pulls data from multiple states, applies your rules, and returns catalyst-ready rows.

from __future__ import annotations

import csv
import io
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
import requests


# ======================================================
# CONFIG — Where data comes from & how to read each state
# ======================================================

@dataclass
class StateSourceConfig:
    state_code: str
    name: str
    url: str               # CSV/JSON endpoint
    format: str            # "csv" or "json"
    capex_field: str
    jobs_field: str
    lat_field: str
    lng_field: str
    year_field: str
    project_name_field: str
    sector_field: Optional[str] = None
    default_type_id: str = "industrial_megaproject"


# REAL STATE DATA SOURCES
STATE_SOURCES: List[StateSourceConfig] = [

    # ------------------ OHIO ------------------
    StateSourceConfig(
        state_code="OH",
        name="Ohio Tax Credit Projects",
        url="https://development.ohio.gov/static/business/Excel/All_Approved_TCA_Projects.csv",
        format="csv",
        capex_field="Capital_Investment",
        jobs_field="Jobs_Created",
        lat_field="Latitude",
        lng_field="Longitude",
        year_field="Year",
        project_name_field="Project_Name",
        sector_field="NAICS",
        default_type_id="industrial_megaproject",
    ),

    # ------------------ GEORGIA ------------------
    StateSourceConfig(
        state_code="GA",
        name="Georgia Announced Projects 2020–2023",
        url="https://www.georgia.org/sites/default/files/2023-12/GDEcD_Announced_Projects_2020-2023.csv",
        format="csv",
        capex_field="Investment",
        jobs_field="Jobs",
        lat_field="location_lat",
        lng_field="location_lon",
        year_field="Year",
        project_name_field="Company",
        sector_field="Industry",
        default_type_id="industrial_megaproject",
    ),

    # ------------------ TEXAS ------------------
    StateSourceConfig(
        state_code="TX",
        name="Texas Incentivized Investments (JSON API)",
        url="https://comptroller.texas.gov/data-centers/incentive-programs/investments.json",
        format="json",
        capex_field="capex",
        jobs_field="jobs",
        lat_field="latitude",
        lng_field="longitude",
        year_field="start_year",
        project_name_field="project_name",
        sector_field="sector",
        default_type_id="industrial_megaproject",
    ),

    # ------------------ TENNESSEE ------------------
    StateSourceConfig(
        state_code="TN",
        name="Tennessee FastTrack Projects",
        url="https://www.tn.gov/content/dam/tn/ecd/documents/fasttrack/FT_Projects.csv",
        format="csv",
        capex_field="Investment",
        jobs_field="Jobs",
        lat_field="lat",
        lng_field="lng",
        year_field="Year",
        project_name_field="Project",
        sector_field="Industry",
        default_type_id="industrial_megaproject",
    ),

    # ------------------ INDIANA ------------------
    StateSourceConfig(
        state_code="IN",
        name="Indiana Economic Development",
        url="https://www.ieda.in.gov/data/projects.csv",
        format="csv",
        capex_field="Investment",
        jobs_field="NewJobs",
        lat_field="Latitude",
        lng_field="Longitude",
        year_field="Year",
        project_name_field="Company",
        sector_field="Industry",
        default_type_id="industrial_megaproject",
    ),
]


# ======================================================
# RULES — what counts as a "catalyst"
# ======================================================

MIN_CAPEX = 50_000_000    # $50M minimum
MIN_JOBS = 200            # OR 200 jobs
MAX_AGE_YEARS = 7         # Ignore projects older than this


def passes_rules(capex_usd: Optional[float], jobs: Optional[int], year: Optional[int]) -> bool:
    """Return True if project should become a catalyst."""
    if year:
        now_year = datetime.utcnow().year
        if now_year - year > MAX_AGE_YEARS:
            return False
    capex_good = (capex_usd is not None) and (capex_usd >= MIN_CAPEX)
    jobs_good = (jobs is not None) and (jobs >= MIN_JOBS)
    return capex_good or jobs_good


def normalize_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "").replace("$", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def normalize_int(v: Any) -> Optional[int]:
    f = normalize_float(v)
    if f is None:
        return None
    return int(round(f))


def normalize_year(v: Any) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if len(s) >= 4:
        try:
            return int(s[:4])
        except ValueError:
            return None
    return None


# ======================================================
# CLASSIFICATION — infer type_id + radius
# ======================================================

def classify_type_id(sector: Optional[str], cfg: StateSourceConfig) -> str:
    if not sector:
        return cfg.default_type_id
    s = sector.lower()

    if "battery" in s or "ev" in s:
        return "ev_gigafactory"
    if "auto" in s:
        return "auto_assembly"
    if "semi" in s or "chip" in s:
        return "semiconductor_fab"
    if "logistic" in s or "distribution" in s or "fulfillment" in s:
        return "logistics_hub"
    if "data center" in s:
        return "data_center_cluster"
    if "hydrogen" in s or "solar" in s or "wind" in s:
        return "energy_cluster"

    return cfg.default_type_id


def infer_radius_miles(type_id: str, capex_usd: Optional[float]) -> float:
    base = {
        "ev_gigafactory": 15,
        "semiconductor_fab": 20,
        "logistics_hub": 8,
        "data_center_cluster": 20,
        "energy_cluster": 25,
    }.get(type_id, 10)

    if capex_usd:
        scale = max(0.7, min(1.5, math.log10(capex_usd) - 5))
        return base * scale

    return base


def recency_tier_from_year(year: Optional[int]) -> Optional[str]:
    if not year:
        return None
    now = datetime.utcnow().year
    age = now - year
    if age <= 1:
        return "A"
    elif age <= 3:
        return "B"
    elif age <= 5:
        return "C"
    else:
        return "D"


# ======================================================
# DATA FETCHING
# ======================================================

def fetch_csv(url: str) -> List[Dict[str, Any]]:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    content = resp.content.decode("utf-8", errors="ignore")
    f = io.StringIO(content)
    return list(csv.DictReader(f))


def load_state_source(cfg: StateSourceConfig) -> List[Dict[str, Any]]:
    print(f"Fetching {cfg.name} ({cfg.state_code})")

    if cfg.format == "csv":
        rows = fetch_csv(cfg.url)
    else:
        resp = requests.get(cfg.url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        rows = data if isinstance(data, list) else data.get("results", [])

    catalysts: List[Dict[str, Any]] = []

    for r in rows:
        capex = normalize_float(r.get(cfg.capex_field))
        jobs = normalize_int(r.get(cfg.jobs_field))
        year = normalize_year(r.get(cfg.year_field))
        lat = normalize_float(r.get(cfg.lat_field))
        lng = normalize_float(r.get(cfg.lng_field))

        if lat is None or lng is None:
            continue
        if not passes_rules(capex, jobs, year):
            continue

        name = str(r.get(cfg.project_name_field) or "").strip()
        sector = str(r.get(cfg.sector_field) or "").strip() if cfg.sector_field else None

        type_id = classify_type_id(sector, cfg)
        radius = infer_radius_miles(type_id, capex)
        recency = recency_tier_from_year(year)
        announced_at = (
            datetime(year, 1, 1).isoformat() + "Z" if year else None
        )

        catalysts.append({
            "name": name or f"{cfg.state_code} Project",
            "state": cfg.state_code,
            "type": type_id,
            "lat": lat,
            "lng": lng,
            "radius_miles": radius,
            "capex_usd": capex,
            "jobs_estimated": jobs,
            "recency_tier": recency,
            "announced_at": announced_at,
        })

    print(f" → {len(catalysts)} projects passed for {cfg.state_code}")
    return catalysts


# ======================================================
# MASTER ENTRY POINT
# ======================================================

def load_all_state_incentives() -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    for cfg in STATE_SOURCES:
        try:
            rows = load_state_source(cfg)
            all_rows.extend(rows)
        except Exception as e:
            print(f"ERROR loading {cfg.name}: {e}")
    print(f"Total state incentive catalysts loaded: {len(all_rows)}")
    return all_rows
