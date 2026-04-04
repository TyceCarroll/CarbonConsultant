from __future__ import annotations
import dataclasses
from core.ai_parser import robust_farm_parser
from core.program_ranker import ProgramRanker
from core.carbon_math import calculate_sequestration
from core.fetch_soil_data import get_lat_long, fetch_usda_soil_data
from core.market_pricing import AgPriceOracle

# ---------------------------------------------------------------------------
# bridge.py — CarbonConsultant Orchestrator
#
# Pipes all sub-modules into a single auditable result dict.
# Returns a structured dict on success so callers (tests, API, UI) can
# inspect every value without parsing printed output.
#
# Return schemas:
#
# Success:
# {
#   "status": "success",
#   "inputs":         {address, acres, bulk_density, lat, lon},
#   "soil":           {om, soil_type, soc_baseline, is_fallback, is_regional_proxy},
#   "market":         {prices{floor,standard,premium}, metadata{source,is_fallback,...}},
#   "ai_data":        {stir_class, biomass_input, amendments, residue_retained,
#                      legacy_years, missing_data_query},
#   "sequestration":  {tonnes_per_acre, total_tonnes_co2e, estimated_annual_value_usd,
#                      saturation_warning, flag_for_review},
#   "program_rankings": [list of program dicts sorted by rank_score desc]
# }
#
# Incomplete narrative (Gemini needs more info):
# {
#   "status": "incomplete",
#   "stage":  "ai_parsing",
#   "missing_data_query": str,   <- surface this to the user as a follow-up question
#   "partial_ai_data": dict
# }
#
# Error:
# {"status": "error", "stage": str, "reason": str}
# ---------------------------------------------------------------------------

BULK_DENSITY_MIN = 0.8   # g/cm3 - below this is peat/organic horizon
BULK_DENSITY_MAX = 2.0   # g/cm3 - above this is gravel/bedrock
ACRES_MIN        = 0.1   # minimum auditable parcel size


def _log(msg: str, silent: bool) -> None:
    """Print only when not in silent/API mode."""
    if not silent:
        print(msg)


def run_carbon_audit(
    address: str,
    acres: float,
    narrative: str,
    bulk_density: float = 1.35,
    silent: bool = False,
) -> dict:
    """
    Runs a full Tier 2 carbon sequestration audit for a US farm parcel.

    Args:
        address:      Plain-text US address or city/state for the farm.
        acres:        Total auditable acreage (must be >= 0.1).
        narrative:    Free-text description of farm management practices.
        bulk_density: Soil bulk density in g/cm3 (default 1.35 for row crops).
        silent:       Suppress all stdout output. Set True in API/test mode.

    Returns:
        dict with status='success' | 'incomplete' | 'error'.
    """
    _log("\n" + "=" * 60, silent)
    _log("CARBON CONSULTANT: LIVE PRODUCTION AUDIT", silent)
    _log("=" * 60, silent)

    # -- Input Validation -----------------------------------------------------
    if acres < ACRES_MIN:
        reason = f"acres must be >= {ACRES_MIN} (got {acres})"
        _log(f"Input Error: {reason}", silent)
        return {"status": "error", "stage": "validation", "reason": reason}

    if not (BULK_DENSITY_MIN <= bulk_density <= BULK_DENSITY_MAX):
        reason = (
            f"bulk_density must be {BULK_DENSITY_MIN}-{BULK_DENSITY_MAX} g/cm3 "
            f"(got {bulk_density})"
        )
        _log(f"Input Error: {reason}", silent)
        return {"status": "error", "stage": "validation", "reason": reason}

    if not narrative or not narrative.strip():
        reason = "narrative cannot be empty"
        _log(f"Input Error: {reason}", silent)
        return {"status": "error", "stage": "validation", "reason": reason}

    # -- Step 1: Geospatial + USDA Soil Data ----------------------------------
    _log("Locating farm and fetching USDA soil baselines...", silent)
    try:
        lat, lon = get_lat_long(address)
        soil_stats = fetch_usda_soil_data(lat, lon)
        soc_baseline = round(soil_stats["om"] * 0.58, 3)
        _log(f"Location: {lat}, {lon}", silent)
        _log(
            f"Soil: {soil_stats['soil_type']} | OM: {soil_stats['om']}% "
            f"| SOC Baseline: {soc_baseline}%",
            silent,
        )
        if soil_stats.get("is_fallback"):
            _log(
                "SOIL DATA ALERT: No USDA cropland data found within 50km. "
                "OM=2.0% (national average) used. Revenue estimates may be inaccurate. "
                "Re-run with a precise rural field address.",
                silent,
            )
        elif soil_stats.get("is_regional_proxy"):
            _log(
                "Soil data sourced from nearest available cropland (~5-50km). "
                "Consider re-running with exact field coordinates.",
                silent,
            )
    except Exception as e:
        reason = str(e)
        _log(f"Geographic Data Error: {reason}", silent)
        return {"status": "error", "stage": "geospatial", "reason": reason}

    # -- Step 2: Live Market Pricing ------------------------------------------
    _log("Fetching real-time market pricing from Carbonmark...", silent)
    market_engine = AgPriceOracle()
    market_data = market_engine.get_consensual_price()
    live_price = market_data["prices"]["standard_registry"]
    _log(
        f"Market Rate: ${live_price}/tonne ({market_data['metadata']['source']})",
        silent,
    )
    if market_data["metadata"].get("is_fallback"):
        _log(
            "Price source: Research-backed benchmark (MSCI/Ecosystem Marketplace 2025). "
            "No live US ag market price API exists.",
            silent,
        )
        ceiling = market_data["metadata"].get("carbonmark_ceiling")
        if ceiling:
            n = market_data["metadata"].get("n_listings", 0)
            _log(
                f"Carbonmark premium ceiling: ${ceiling}/tonne ({n} active listing(s)).",
                silent,
            )

    # -- Step 3: AI Narrative Parsing -----------------------------------------
    _log("Analyzing Farm Management Narrative...", silent)
    ai_data = robust_farm_parser(narrative)

    if ai_data is None:
        reason = "Gemini parsing returned None - check GEMINI_API_KEY and quota"
        _log(f"Critical Error: {reason}", silent)
        return {"status": "error", "stage": "ai_parsing", "reason": reason}

    if ai_data.get("missing_data_query"):
        # Return incomplete (not error) so the frontend can surface the
        # follow-up question to the user and re-submit with more detail.
        _log(
            "NARRATIVE INCOMPLETE - returning partial result for follow-up. "
            f"Follow-up needed: {ai_data['missing_data_query']}",
            silent,
        )
        return {
            "status": "incomplete",
            "stage": "ai_parsing",
            "missing_data_query": ai_data["missing_data_query"],
            "partial_ai_data": ai_data,
        }

    _log("Narrative Parsed Successfully.", silent)

    # -- Step 4: Sequestration Math -------------------------------------------
    _log("Calculating Tier 2 Sequestration (Bulk Density Mass Model)...", silent)
    results = calculate_sequestration(
        ai_data, soc_baseline, bulk_density, acres, live_price
    )

    # -- Step 5: Final Report -------------------------------------------------
    _log("\n" + "=" * 60, silent)
    _log("FINAL AUDIT REPORT", silent)
    _log("=" * 60, silent)
    _log(f"Farm Size:      {acres} Acres", silent)
    _log(f"Soil Type:      {soil_stats['soil_type']}", silent)
    _log(f"SOC Baseline:   {soc_baseline}%", silent)

    # Defensive formatting: Gemini can return None for any field if the
    # narrative was ambiguous. .title() on None raises AttributeError.
    _stir       = (ai_data.get("stir_class")    or "Unknown").title()
    _biomass    = (ai_data.get("biomass_input") or "Unknown").title()
    _amendments = (ai_data.get("amendments")    or "Unknown").title()
    _residue    = ai_data.get("residue_retained")
    _residue_s  = (
        "Retained" if _residue is True
        else "Removed" if _residue is False
        else "Unknown"
    )
    _log(f"Tillage:        {_stir}", silent)
    _log(f"Cover Crops:    {_biomass} Biomass", silent)
    _log(f"Residue:        {_residue_s}", silent)
    _log(f"Amendments:     {_amendments}", silent)
    _log(f"Legacy Years:   {ai_data['legacy_years']} yrs", silent)
    _log("-" * 60, silent)
    _log(f"Annual Yield:   {results['total_tonnes_co2e']} tonnes CO2e", silent)
    _log(f"Gross Revenue:  ${results['estimated_annual_value_usd']:,} / yr", silent)
    _log(f"  Floor:        ${market_data['prices']['conservative_floor']}/tonne", silent)
    _log(f"  Standard:     ${live_price}/tonne", silent)
    _log(f"  Premium:      ${market_data['prices']['premium_market']}/tonne", silent)

    if results["saturation_warning"]:
        _log(
            "\nSATURATION WARNING: >15 legacy years detected. "
            "Sequestration rate is likely near its natural ceiling. "
            "Diminishing returns apply - consider independent re-baselining.",
            silent,
        )

    if results["flag_for_review"]:
        _log(
            "\nREGISTRY CAP: Yield exceeds 2.2 t CO2e/acre. "
            "Value capped for conservatism. Flag for verifier review.",
            silent,
        )

    # Data confidence summary
    soil_conf = (
        "ESTIMATED (national avg)"  if soil_stats.get("is_fallback")
        else "REGIONAL PROXY (~5-50km)" if soil_stats.get("is_regional_proxy")
        else "REAL USDA DATA"
    )
    price_conf = (
        "BENCHMARK (no live API)"
        if market_data["metadata"].get("is_fallback")
        else "LIVE MARKET DATA"
    )
    _log("\n-- DATA CONFIDENCE --", silent)
    _log(f"Soil Data:     {soil_conf}", silent)
    _log(f"Market Price:  {price_conf}", silent)
    ceiling = market_data["metadata"].get("carbonmark_ceiling")
    if ceiling:
        n = market_data["metadata"].get("n_listings", 0)
        _log(f"Mkt Ceiling:   ${ceiling}/tonne (Carbonmark, {n} listing(s))", silent)
    _log("=" * 60 + "\n", silent)

    # -- Step 6: Program Ranking ----------------------------------------------
    # Build the full result dict first, then pass to the ranker.
    # dataclasses.asdict() converts ProgramResult objects to plain dicts
    # so the entire return value is JSON-serializable without extra work.
    final_result = {
        "status": "success",
        "inputs": {
            "address":           address,
            "acres":             acres,
            "bulk_density":      bulk_density,
            "lat":               lat,
            "lon":               lon,
        },
        "soil": {
            "om":                soil_stats["om"],
            "soil_type":         soil_stats["soil_type"],
            "soc_baseline":      soc_baseline,
            "is_fallback":       soil_stats.get("is_fallback", False),
            "is_regional_proxy": soil_stats.get("is_regional_proxy", False),
        },
        "market":        market_data,
        "ai_data":       ai_data,
        "sequestration": results,
    }
    ranker = ProgramRanker()
    program_results = ranker.rank(final_result)
    final_result["program_rankings"] = [
        dataclasses.asdict(r) for r in program_results
    ]
    _log(ranker.format_report(program_results, final_result), silent)

    return final_result


if __name__ == "__main__":
    farm_address   = "North Kansas City, MO"
    farm_size      = 500
    farm_narrative = (
        "We manage 500 acres of corn. We've used no-till methods for the last 6 years. "
        "We plant a cereal rye cover crop every fall after harvest and we never bale "
        "the stalks - everything stays on the dirt. No manure or biochar used."
    )
    run_carbon_audit(farm_address, farm_size, farm_narrative)