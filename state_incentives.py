# state_incentives.py
#
# Rule-based loader for state economic development / incentive projects.
# This is the skeleton for your fully automated catalyst ingestion:
# - Fetches CSV/JSON from multiple states
# - Maps them into a unified schema
# - Applies your rules (capex, jobs, recency, etc.)
# - Returns rows ready to insert into `catalysts`.

from __future__ import annotations

import csv
import io
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
import requests


# ---- CONFIG: where data comes from & how to read it ----

@dataclass
class StateSourceConfig:
    state_code: str
    name: str
    url: str               # CSV/JSON endpoint. You will plug real URLs here.
    format: str            # "csv" or "json"
    capex_field: str       # column name for capex
    jobs_field: str        # column name for jobs
    lat_field: str         # column name for latitude
    lng_field: str         # column name for longitude
    year_field: str        # column name for announcement / start year
    project_name_field: str
    sector_field: Optional[str] = None    # optional sector/industry column
    default_type_id: str = "industrial_megaproject"


# NOTE: These URLs are placeholders. Once you identify real CSV/API links
# from state websites, you’ll just replace the url="..." in each config.
STATE_SOURCES: List[StateSourceConfig] = [
    StateSourceConfig(
        state_code="OH",
        name="Ohio Development Projects",
        url="https://example.com/ohio_projects.csv",  # TODO: replace with real CSV endpoint
        format="csv",
        capex_field="capex_usd",
        jobs_field="jobs",
        lat_field="lat",
        lng_field="lng",
        year_field="announcement_year",
        project_name_field="project_name",
        sector_field="sector",
        default_type_id="industrial_megaproject",
    ),
    StateSourceConfig(
        state_code="GA",
        name="Georgia Major Projects",
        url="https://example.com/georgia_projects.csv",  # TODO: replace
        format="csv",
        capex_field="investment_usd",
        jobs_field="jobs_created",
        lat_field="latitude",
        lng_field="longitude",
        year_field="year",
        project_name_field="project",
        sector_field="industry",
        default_type_id="industrial_megaproject",
    ),
    StateSourceConfig(
        state_code="TX",
        name="Texas Incentivized Projects",
        url="https://example.com/texas_projects.csv",   # TODO: replace
        format="csv",
        capex_field="capex",
        jobs_field="jobs",
        lat_field="lat",
        lng_field="lon",
        year_field="announcement_year",
        project_name_field="project_name",
        sector_field="naics_sector",
        default_type_id="industrial_megaproject",
    ),
    # You can add MI, TN, NC, IN, etc. as more configs using the same pattern.
]


# ---- RULES: what counts as a "catalyst"? ----

MIN_CAPEX = 50_000_000       # $50M
MIN_JOBS = 200               # OR 200 jobs
MAX_AGE_YEARS = 7            # ignore projects older than this


def passes_rules(capex_usd: Optional[float], jobs: Optional[int], year: Optional[int]) -> bool:
    """Apply your rules to decide if a project is a catalyst."""
    if year:
        now_year = datetime.utcnow().year
        if now_year - year > MAX_AGE_YEARS:
            return False

    capex_good = (capex_usd is not None) and (capex_usd >= MIN_CAPEX)
    jobs_good = (jobs is not None) and (jobs >= MIN_JOBS)

    # require at least one strong signal
    if not (capex_good or jobs_good):
        return False

    return True


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
    if not s:
        return None
    try:
        # Allow full dates like "2023-05-01"
        if len(s) >= 4:
            return int(s[:4])
        return int(s)
    except ValueError:
        return None


def classify_type_id(sector: Optional[str], cfg: StateSourceConfig) -> str:
    """Map sector/industry text into a catalyst type_id."""
    if not sector:
        return cfg.default_type_id

    s = sector.lower()
    # simple heuristic mapping; can be improved later
    if "battery" in s or "ev" in s or "electric vehicle" in s:
        return "ev_gigafactory"
    if "auto" in s or "automotive" in s:
        return "auto_assembly"
    if "semi" in s or "chip" in s or "wafer" in s:
        return "semiconductor_fab"
    if "logistic" in s or "warehous" in s or "distribution" in s or "fulfillment" in s:
        return "logistics_hub"
    if "data center" in s or "cloud" in s or "hyperscale" in s:
        return "data_center_cluster"
    if "hydrogen" in s or "renewable" in s or "solar" in s or "wind" in s:
        return "energy_cluster"
    return cfg.default_type_id


def infer_radius_miles(type_id: str, capex_usd: Optional[float]) -> float:
    """Set radius based on type & scale. This feeds your distance-decay model."""
    base = 10.0

    if type_id in ("ev_gigafactory", "semiconductor_fab"):
        base = 15.0
    elif type_id in ("logistics_hub", "distribution_center", "fulfillment_center"):
        base = 8.0
    elif type_id in ("data_center_cluster", "energy_cluster"):
        base = 20.0
    else:
        base = 10.0

    if capex_usd:
        # Mild scaling with capex: log10 so that $1B > $100M, but not crazy
        scale = max(0.7, min(1.5, math.log10(capex_usd) - 5))
        return base * scale

    return base


# ---- FETCHERS ----

def fetch_csv(url: str) -> List[Dict[str, Any]]:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    content = resp.content.decode("utf-8", errors="ignore")
    f = io.StringIO(content)
    reader = csv.DictReader(f)
    return list(reader)


def load_state_source(cfg: StateSourceConfig) -> List[Dict[str, Any]]:
    """Load one state's dataset, apply rules & mapping, return catalyst-ready dicts."""
    print(f"Fetching state source: {cfg.name} ({cfg.state_code})")
    if cfg.format == "csv":
        rows = fetch_csv(cfg.url)
    else:
        # JSON path (for later, if needed)
        resp = requests.get(cfg.url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        # Assume top-level list for now
        if isinstance(data, list):
            rows = data
        else:
            # If it's an object wrapper, user will adapt this
            rows = data.get("results", [])

    catalysts: List[Dict[str, Any]] = []

    for row in rows:
        capex_usd = normalize_float(row.get(cfg.capex_field))
        jobs = normalize_int(row.get(cfg.jobs_field))
        year = normalize_year(row.get(cfg.year_field))
        lat = normalize_float(row.get(cfg.lat_field))
        lng = normalize_float(row.get(cfg.lng_field))

        if lat is None or lng is None:
            continue  # skip rows we can't geocode (for now)

        if not passes_rules(capex_usd, jobs, year):
            continue

        name = str(row.get(cfg.project_name_field) or "").strip()
        if not name:
            name = f"{cfg.state_code} Project"

        sector = str(row.get(cfg.sector_field) or "").strip() if cfg.sector_field else None
        type_id = classify_type_id(sector, cfg)

        radius_miles = infer_radius_miles(type_id, capex_usd)

        catalysts.append(
            {
                "name": name,
                "state": cfg.state_code,
                "type": type_id,
                "lat": lat,
                "lng": lng,
                "radius_miles": radius_miles,
                "capex_usd": capex_usd,
                "jobs_estimated": jobs,
                "recency_tier": recency_tier_from_year(year),
                "announced_at": (
                    datetime(year, 1, 1).isoformat() + "Z" if year else None
                ),
            }
        )

    print(f"  → {len(catalysts)} projects passed rules for {cfg.state_code}")
    return catalysts


def recency_tier_from_year(year: int | None) -> Optional[str]:
    if not year:
        return None
    now_year = datetime.utcnow().year
    age = now_year - year
    if age <= 1:
        return "A"
    elif age <= 3:
        return "B"
    elif age <= 5:
        return "C"
    else:
        return "D"


def load_all_state_incentives() -> List[Dict[str, Any]]:
    """Master entry point: pull from all configured states."""
    all_rows: List[Dict[str, Any]] = []
    for cfg in STATE_SOURCES:
        try:
            state_rows = load_state_source(cfg)
            all_rows.extend(state_rows)
        except Exception as e:
            print(f"Error loading {cfg.name}: {e}")
    print(f"Total state incentive catalysts loaded: {len(all_rows)}")
    return all_rows
