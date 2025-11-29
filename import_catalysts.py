# import_catalysts.py
#
# Master catalyst ingestion pipeline for PropAI.
# This version:
# - Connects to Supabase
# - Loads seed catalysts (EV, CHIPS, logistics, airports, energy)
# - Loads rule-based dynamic catalysts from state incentive datasets
# - Upserts everything into the `catalysts` table
#
# You can gradually remove seed items as live sources expand.

import os
import math
from datetime import datetime

from supabase import create_client

# --- NEW: rule-driven state incentive loader ---
from state_incentives import load_all_state_incentives


# ------------------------- SUPABASE SETUP -------------------------

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ------------------------- HELPERS -------------------------

def recency_tier_from_year(year: int) -> str:
    """Simple recency tier logic."""
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


def make_catalyst(
    name: str,
    state: str,
    type_id: str,
    lat: float,
    lng: float,
    radius_miles: float,
    capex_usd: float | None,
    jobs_estimated: int | None,
    announced_year: int | None,
):
    recency_tier = recency_tier_from_year(announced_year) if announced_year else None
    announced_at = (
        datetime(announced_year, 1, 1).isoformat() + "Z" if announced_year else None
    )

    return {
        "name": name,
        "state": state,
        "type": type_id,
        "lat": lat,
        "lng": lng,
        "radius_miles": radius_miles,
        "capex_usd": capex_usd,
        "jobs_estimated": jobs_estimated,
        "recency_tier": recency_tier,
        "announced_at": announced_at,
    }


# ------------------------- SEED CATALYSTS -------------------------

def seed_catalysts() -> list[dict]:
    """
    Temporary curated catalysts for demos.
    Later replaced by real API-driven ingestion.
    """

    data: list[dict] = []

    # --- EV / Battery ---
    data.append(
        make_catalyst(
            name="GM Ultium Lansing",
            state="MI",
            type_id="ev_gigafactory",
            lat=42.708, lng=-84.668,
            radius_miles=10,
            capex_usd=2100000000,
            jobs_estimated=1700,
            announced_year=2022,
        )
    )
    data.append(
        make_catalyst(
            name="Honda LG EV Battery Ohio",
            state="OH",
            type_id="ev_gigafactory",
            lat=40.236, lng=-83.367,
            radius_miles=12,
            capex_usd=4400000000,
            jobs_estimated=2200,
            announced_year=2022,
        )
    )
    data.append(
        make_catalyst(
            name="Panasonic EV Battery Kansas",
            state="KS",
            type_id="ev_gigafactory",
            lat=38.964, lng=-94.97,
            radius_miles=15,
            capex_usd=4000000000,
            jobs_estimated=4000,
            announced_year=2022,
        )
    )

    # --- Semiconductor ---
    data.append(
        make_catalyst(
            name="Intel New Albany Fab",
            state="OH",
            type_id="semiconductor_fab",
            lat=40.083, lng=-82.808,
            radius_miles=20,
            capex_usd=20000000000,
            jobs_estimated=3000,
            announced_year=2022,
        )
    )

    # --- Logistics / Fulfillment ---
    data.append(
        make_catalyst(
            name="Amazon Fulfillment Center – Columbus",
            state="OH",
            type_id="fulfillment_center",
            lat=39.99, lng=-82.88,
            radius_miles=8,
            capex_usd=300000000,
            jobs_estimated=1500,
            announced_year=2020,
        )
    )

    # --- Rail Terminal ---
    data.append(
        make_catalyst(
            name="Kansas City Intermodal Facility",
            state="KS",
            type_id="rail_terminal",
            lat=38.82, lng=-94.97,
            radius_miles=15,
            capex_usd=500000000,
            jobs_estimated=800,
            announced_year=2018,
        )
    )

    # --- Airport ---
    data.append(
        make_catalyst(
            name="Chicago O'Hare Cargo Cluster",
            state="IL",
            type_id="airport",
            lat=41.98, lng=-87.9,
            radius_miles=20,
            capex_usd=1000000000,
            jobs_estimated=2000,
            announced_year=2019,
        )
    )

    # --- Energy / Hydrogen ---
    data.append(
        make_catalyst(
            name="Midwest Clean Hydrogen Hub – Example Node",
            state="IL",
            type_id="power_plant",
            lat=41.88, lng=-87.63,
            radius_miles=30,
            capex_usd=3000000000,
            jobs_estimated=1200,
            announced_year=2023,
        )
    )

    return data


# ------------------------- UPSERT LOGIC -------------------------

def upsert_catalysts(rows: list[dict]):
    """Insert or update catalysts using (name, state, type) uniqueness."""

    if not rows:
        print("No catalysts to upsert.")
        return

    for row in rows:
        name = row["name"]
        state = row["state"]
        type_id = row["type"]

        existing = (
            supabase.table("catalysts")
            .select("id")
            .eq("name", name)
            .eq("state", state)
            .eq("type", type_id)
            .execute()
        )

        if existing.data:
            catalyst_id = existing.data[0]["id"]
            print(f"Updating catalyst: {name} ({state}, {type_id})")
            supabase.table("catalysts").update(row).eq("id", catalyst_id).execute()
        else:
            print(f"Inserting catalyst: {name} ({state}, {type_id})")
            supabase.table("catalysts").insert(row).execute()


# ------------------------- MASTER PIPELINE -------------------------

def main():
    print("Loading seed catalysts...")
    catalysts = seed_catalysts()

    print("Loading dynamic state incentive catalysts...")
    state_rows = load_all_state_incentives()
    catalysts.extend(state_rows)

    print(f"Prepared {len(catalysts)} total catalysts to upsert.")
    upsert_catalysts(catalysts)
    print("Done.")


# ------------------------- ENTRY POINT -------------------------

if __name__ == "__main__":
    main()
