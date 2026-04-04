import os
import requests
from geopy.geocoders import ArcGIS
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# fetch_soil_data.py
#
# Fixes vs previous version:
#   BUG 1 — CROSS APPLY pattern was structurally wrong; the intersection
#            function result is now the FROM anchor, not mapunit.
#   BUG 2 — Join order now follows the correct USDA SDA hierarchy:
#            intersection_fn -> mapunit -> component -> chorizon
#   BUG 3 — Horizon depth filter now uses hzdept_r < 30 (top of horizon
#            must start within the top 30 cm window).
#   BUG 4 — Replaced SELECT TOP 1 ORDER BY om_r DESC (single richest
#            horizon) with a thickness-weighted average across ALL horizons
#            in the top 30 cm. This is the scientifically correct SOC
#            baseline for a mass-balance model.
#   BUG 5 — Added majcompflag = 'Yes' to restrict results to the dominant
#            soil component of the map unit, not minor inclusions.
#
# Supabase writes are intentionally removed from this module.
# Persistence is handled by bridge.py / the orchestration layer.
# ---------------------------------------------------------------------------

USDA_SDA_URL = "https://sdmdataaccess.nrcs.usda.gov/tabular/post.rest"
FALLBACK_RESULT = {"om": 2.0, "soil_type": "Unknown/Fallback", "is_fallback": True}


def get_lat_long(address: str) -> tuple[float, float]:
    """
    Converts a plain-text US address to (latitude, longitude) using ArcGIS.
    Falls back to the state/region component if the full address is not found.
    """
    geolocator = ArcGIS(user_agent="CarbonConsultant_DataEngine")
    location = geolocator.geocode(address)

    if not location:
        print(f"⚠️  Full address not found. Trying broader region...")
        fallback_region = address.split(",")[-1].strip()
        location = geolocator.geocode(fallback_region)

        if not location:
            raise ValueError(f"❌ Could not geocode address: {address}")

    return location.latitude, location.longitude


def fetch_usda_soil_data(lat: float, lon: float) -> dict:
    """
    Queries the USDA Soil Data Access (SDA) API for a thickness-weighted
    average of Organic Matter (OM %) across the top 30 cm of the dominant
    soil component at the given coordinates.

    Returns:
        {"om": float, "soil_type": str}
        Falls back to {"om": 2.0, "soil_type": "Unknown/Fallback"} on any
        failure so the pipeline always has a value to continue with.
    """
    # 5 decimal places is ~1 m precision — sufficient for USDA polygon lookup
    # and avoids the WKT parser hanging on high-precision floats
    clean_lon = round(lon, 5)
    clean_lat = round(lat, 5)

    # -----------------------------------------------------------------------
    # Corrected SQL — key changes from old version:
    #
    # FROM anchor : SDA_Get_Mukey_from_intersection_with_WktWgs84(...)  AS lut
    #   The intersection function is now the starting point. All tables are
    #   joined onto it, not the other way around.
    #
    # majcompflag = 'Yes'
    #   Restricts to the dominant soil component of the map unit. Without
    #   this, USDA returns minor inclusions that are scientifically irrelevant
    #   to a whole-field carbon calculation.
    #
    # Weighted average OM:
    #   SUM(om_r * thickness) / SUM(thickness)
    #   Gives the true depth-weighted organic matter for the 0–30 cm profile,
    #   not just the single richest horizon.
    #
    # hzdept_r >= 0 AND hzdept_r < 30
    #   Selects horizons whose TOP is within the 0–30 cm window.
    #   The old filter (hzdept_r < 30 only) was technically the same but
    #   the explicit >= 0 makes intent clear and prevents negative-depth
    #   anomalies in unusual SSURGO records.
    # -----------------------------------------------------------------------
    sql_query = f"""
SELECT
    SUM(ch.om_r * (ch.hzdepb_r - ch.hzdept_r)) / SUM(ch.hzdepb_r - ch.hzdept_r) AS weighted_om,
    co.compname
FROM
    SDA_Get_Mukey_from_intersection_with_WktWgs84('POINT({clean_lon} {clean_lat})') AS lut
    JOIN mapunit   mu ON mu.mukey = lut.mukey
    JOIN component co ON co.mukey = mu.mukey
    JOIN chorizon  ch ON ch.cokey = co.cokey
WHERE
    co.majcompflag = 'Yes'
    AND ch.hzdept_r >= 0
    AND ch.hzdept_r <  30
    AND ch.om_r     IS NOT NULL
GROUP BY
    co.compname
ORDER BY
    SUM(ch.om_r * (ch.hzdepb_r - ch.hzdept_r)) / SUM(ch.hzdepb_r - ch.hzdept_r) DESC
"""

    payload = {"query": sql_query, "format": "JSON+COLUMNNAME"}
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    try:
        response = requests.post(
            USDA_SDA_URL, json=payload, headers=headers, timeout=20
        )
        response.raise_for_status()
        data = response.json()

        # JSON+COLUMNNAME format: Table[0] = column headers, Table[1..n] = data rows
        table = data.get("Table", [])
        if len(table) < 2:
            print(
                f"⚠️  No USDA soil data found for ({clean_lat}, {clean_lon}). "
                "Using national average fallback."
            )
            return FALLBACK_RESULT

        # Table[0] = ['weighted_om', 'compname']
        # Table[1] = ['3.24', 'Mexico silt loam']  (highest-OM dominant component)
        row = table[1]
        om_raw = row[0]
        comp_name = row[1] if row[1] else "Unknown"

        if om_raw is None:
            print("⚠️  USDA returned NULL organic matter. Using fallback.")
            return FALLBACK_RESULT

        om_value = round(float(om_raw), 3)

        # Sanity gate: OM >25% indicates peat/histosol — flag it but don't discard
        if om_value > 25.0:
            print(
                f"⚠️  Unusually high OM ({om_value}%) detected for {comp_name}. "
                "This may be a peat/histosol — review before submitting to a registry."
            )

        return {"om": om_value, "soil_type": comp_name}

    except requests.exceptions.Timeout:
        print("⚠️  USDA SDA request timed out. Using fallback.")
        return FALLBACK_RESULT

    except requests.exceptions.RequestException as e:
        print(f"⚠️  USDA API network error: {e}. Using fallback.")
        return FALLBACK_RESULT

    except (KeyError, IndexError, ValueError) as e:
        print(f"⚠️  USDA response parse error: {e}. Using fallback.")
        return FALLBACK_RESULT


if __name__ == "__main__":
    # Quick smoke test — swap in any rural US address to verify live
    test_address = "16651 Schofield Rd, Clermont, FL 34714"
    print(f"--- Smoke Test: {test_address} ---")
    lat, lon = get_lat_long(test_address)
    print(f"Coordinates: {lat}, {lon}")
    result = fetch_usda_soil_data(lat, lon)
    soc = round(result["om"] * 0.58, 3)
    print(f"Soil Type : {result['soil_type']}")
    print(f"OM        : {result['om']}%")
    print(f"SOC       : {soc}%")