"""
api.py — CarbonConsultant REST API
====================================
FastAPI wrapper around bridge.run_carbon_audit().
Designed for integration with Lovable (or any frontend).

Endpoints:
  POST /api/audit         — full carbon audit
  GET  /api/health        — liveness check
  GET  /api/schema        — returns the full response schema as JSON

Run locally:
  uvicorn api:app --reload --port 8000

Environment variables required (see .env.example):
  GEMINI_API_KEY
  SUPABASE_URL           (optional — only if persistence enabled)
  SUPABASE_SECRET_KEY    (optional)

CORS is configured to allow all origins by default so Lovable's
preview domain works without configuration. Lock this down to
your production domain before going live.
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
import traceback

from core.bridge import run_carbon_audit

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI(
    title="CarbonConsultant API",
    description="Tier 2 agricultural carbon credit audit engine",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # ⚠️ lock to your domain in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------
class AuditRequest(BaseModel):
    address: str = Field(
        ...,
        description="Plain-text US farm address or city/state",
        examples=["16651 Schofield Rd, Clermont, FL 34714"],
        min_length=3,
    )
    acres: float = Field(
        ...,
        description="Total auditable acreage (>= 0.1)",
        examples=[500.0],
        gt=0,
    )
    narrative: str = Field(
        ...,
        description=(
            "Free-text description of farm management practices. "
            "Include tillage method, cover crops, residue management, "
            "amendments (biochar/compost/manure), and years of practice."
        ),
        examples=[
            "We grow corn and soybeans on 500 acres in Iowa. We've used "
            "no-till for 6 years. We plant cereal rye every fall and leave "
            "all residue on the field. No manure or biochar."
        ],
        min_length=20,
    )
    bulk_density: float = Field(
        default=1.35,
        description="Soil bulk density in g/cm³ (default 1.35 for row crops)",
        examples=[1.35],
        ge=0.8,
        le=2.0,
    )

    @field_validator("narrative")
    @classmethod
    def narrative_not_whitespace(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("narrative cannot be blank")
        return v


class AuditResponse(BaseModel):
    """
    Successful audit response.
    All monetary values are USD. All weights are metric tonnes CO₂e.
    """
    status: str               # "success"
    inputs: dict
    soil: dict                # om, soil_type, soc_baseline, is_fallback, is_regional_proxy
    market: dict              # prices{floor,standard,premium}, metadata{source,is_fallback,...}
    ai_data: dict             # stir_class, biomass_input, amendments, residue_retained,
                              # legacy_years, missing_data_query
    sequestration: dict       # tonnes_per_acre, total_tonnes_co2e,
                              # estimated_annual_value_usd, saturation_warning, flag_for_review
    program_rankings: list    # list of program dicts, sorted by rank_score desc


class IncompleteResponse(BaseModel):
    """
    Returned when the narrative lacks enough detail for a full audit.
    The frontend should surface missing_data_query to the user as a
    follow-up question and re-submit with an enriched narrative.
    """
    status: str               # "incomplete"
    stage: str                # "ai_parsing"
    missing_data_query: str   # The specific question Gemini needs answered
    partial_ai_data: dict     # Whatever was parsed — useful for prefilling UI


class ErrorResponse(BaseModel):
    status: str               # "error"
    stage: str                # "validation" | "geospatial" | "ai_parsing"
    reason: str


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/api/health")
def health():
    """Liveness check — returns 200 if the API is up."""
    return {"status": "ok", "service": "CarbonConsultant API"}


@app.get("/api/schema")
def schema():
    """Returns the full request/response schema for frontend integration."""
    return {
        "request": AuditRequest.model_json_schema(),
        "response_success": AuditResponse.model_json_schema(),
        "response_incomplete": IncompleteResponse.model_json_schema(),
        "response_error": ErrorResponse.model_json_schema(),
    }


@app.post(
    "/api/audit",
    response_model=AuditResponse,
    responses={
        200: {"description": "Successful audit"},
        206: {"model": IncompleteResponse, "description": "Narrative incomplete — follow-up required"},
        400: {"model": ErrorResponse, "description": "Validation or geospatial error"},
        422: {"description": "Request body failed schema validation"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
    summary="Run a full carbon audit for a US farm parcel",
)
def audit(body: AuditRequest):
    """
    Runs a full Tier 2 carbon sequestration audit:

    1. Geocodes the address and fetches USDA soil data (with 50km expansion)
    2. Fetches live Carbonmark pricing (falls back to research benchmark)
    3. Parses the farm narrative with Gemini AI
    4. Calculates annual CO₂e sequestration using the Bulk Density Mass Model
    5. Evaluates and ranks all 7 major US carbon programs by estimated payout

    **Narrative tips for best results:**
    - State the tillage method (no-till, strip-till, conventional)
    - Mention cover crops and species if applicable
    - Say whether crop residue is left on the field or removed
    - Include any soil amendments (biochar, compost, manure)
    - State how many years the practices have been in use
    """
    try:
        result = run_carbon_audit(
            address=body.address,
            acres=body.acres,
            narrative=body.narrative,
            bulk_density=body.bulk_density,
            silent=True,  # suppress stdout in API mode
        )
    except Exception:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "stage": "internal",
                "reason": traceback.format_exc(),
            },
        )

    status = result.get("status")

    if status == "success":
        return result

    if status == "incomplete":
        # 206 Partial Content — frontend should ask the follow-up question
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=206, content=result)

    if status == "error":
        stage = result.get("stage", "unknown")
        reason = result.get("reason", "Unknown error")

        if stage == "validation":
            raise HTTPException(status_code=400, detail=result)
        elif stage == "geospatial":
            raise HTTPException(status_code=400, detail=result)
        elif stage == "ai_parsing":
            raise HTTPException(status_code=400, detail=result)
        else:
            raise HTTPException(status_code=500, detail=result)

    # Fallback — should never reach here
    raise HTTPException(status_code=500, detail={"status": "error",
                                                  "stage": "unknown",
                                                  "reason": str(result)})