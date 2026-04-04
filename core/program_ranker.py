"""
program_ranker.py — Agricultural Carbon Program Payout Predictor & Ranker
=========================================================================
Evaluates every major US ag carbon program against a farmer's specific
profile and ranks them by estimated net annual payout.

Programs covered (all research-backed, sources cited inline):
  1. Carbon by Indigo / Corteva  (CAR SEP v1.1)
  2. Truterra / Land O'Lakes     (CAR SEP v1.1 via Indigo)
  3. Verra VM0042                (via aggregator)
  4. CAR SEP Direct              (via aggregator)
  5. Cargill RegenConnect        (OTC)
  6. Nori / Bayer ForGround      (Nori Croplands)
  7. FBN Sustainability/Gradable (COMET-Farm)

Ranking criteria (weighted):
  - Estimated net annual payout   (40%) — most important to farmer
  - Flexibility / contract length (20%) — shorter = better
  - Additionality burden          (15%) — historical practice credit = better
  - Verification simplicity       (15%) — program handles it = better
  - Registry credibility          (10%) — CAR/Verra CCP = premium buyers

Sources:
  Indigo: indigoag.com — 75% of sale, credits $60-$80/t in 2025
  Truterra: truterraag.com — up to $30/t, 55% of ASP (2025 program)
  Verra: upstream.ag/p/agribusiness-carbon — $30/t offtake, 165 active projects
  CAR: climateactionreserve.org — $15-40/t, 5 US projects only
  Cargill: $20-25/t, 1yr contract, 15 states
  Nori: nori.com, Bayer partnership — $15-30/t, historical accepted
  FBN: fbn.com/sustainability — $20/t floor, bank credits
  Price data: Quantum Intel via upstream.ag/p/agribusiness-carbon (Sep 2025)
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
import math


# ---------------------------------------------------------------------------
# Program definitions
# ---------------------------------------------------------------------------

@dataclass
class ProgramResult:
    """Computed output for one program against one farm profile."""
    name:                  str
    registry:              str
    eligible:              bool
    ineligibility_reason:  Optional[str]

    # Payout estimates (USD / year for the whole farm)
    payout_floor:          float   # conservative floor
    payout_mid:            float   # realistic midpoint
    payout_ceiling:        float   # optimistic ceiling

    # Per-acre equivalents
    per_acre_floor:        float
    per_acre_mid:          float

    # Program characteristics
    contract_years:        int
    payment_type:          str     # "per_tonne" | "per_acre" | "hybrid"
    accepts_historical:    bool    # True = existing practices may qualify
    program_handles_verification: bool
    registry_credibility:  str     # "premium" | "standard" | "internal"

    # Ranking
    rank_score:            float   # 0-100, higher = better fit for farmer
    rank_position:         int     # 1 = best

    # Caveats
    caveats:               list[str] = field(default_factory=list)
    url:                   str = ""


# ---------------------------------------------------------------------------
# Core ranker class
# ---------------------------------------------------------------------------

class ProgramRanker:
    """
    Evaluates and ranks all major US ag carbon programs for a given farm.

    Input: the full result dict from bridge.run_carbon_audit()
    Output: list[ProgramResult] sorted by rank_score descending
    """

    # ── Program definitions ────────────────────────────────────────────────
    # All prices are farmer NET (after program fees/splits where disclosed).
    # Sources cited in module docstring.
    PROGRAMS = [
        {
            "id":         "indigo",
            "name":       "Carbon by Indigo (+ Corteva)",
            "registry":   "CAR SEP v1.1",
            "cred":       "premium",
            "price_floor_per_t":   20.0,   # guaranteed minimum to farmer
            "price_mid_per_t":     45.0,   # 75% of ~$60 avg sale price (2025)
            "price_ceiling_per_t": 60.0,   # 75% of $80 high trades (2025)
            "payout_split":        0.75,   # farmer gets 75% of sale
            "contract_years":      5,
            "accepts_historical":  False,  # must implement NEW practice
            "prog_handles_verif":  True,
            "eligible_crops": {"corn","soy","wheat","cotton","alfalfa","rye",
                                "oats","barley","sorghum","sunflower","canola"},
            "min_legacy_for_baseline": 0,  # baseline established at enrollment
            "req_additionality": True,     # new practice required
            "nationwide": True,
            "caveats": [
                "Must implement a NEW conservation practice not currently used.",
                "Payment vested over 5 years — early exit forfeits unvested credits.",
                "Historical practices before enrollment year don't count.",
            ],
            "url": "indigoag.com/carbon",
        },
        {
            "id":         "truterra",
            "name":       "Truterra (Land O'Lakes)",
            "registry":   "CAR SEP v1.1 (via Indigo)",
            "cred":       "premium",
            "price_floor_per_t":   6.0,    # $2/acre min / typical ~0.3t/acre = ~$6/t floor
            "price_mid_per_t":     22.0,   # 55% of ~$40 ASP
            "price_ceiling_per_t": 30.0,   # up to $30/t disclosed (2024-2025)
            "payout_split":        0.55,   # 55% of Average Selling Price (2025 program docs)
            "contract_years":      1,      # 1-yr enrollment, 5 data-reporting years
            "accepts_historical":  True,   # expanded to pre-2021 adopters in 2024
            "prog_handles_verif":  True,   # Truterra handles soil sampling
            "eligible_crops": {"corn","soy","wheat","cotton"},
            "req_additionality": True,
            "nationwide": False,           # major corn belt states only
            "caveats": [
                "Available in corn belt states only (check truterraag.com/enroll).",
                "Farmer receives 55% of Average Selling Price — lower split than Indigo direct.",
                "Long-term adopters (pre-2021 no-till/cover crops) now eligible.",
                "Minimum $2/acre guaranteed regardless of tonnes sequestered.",
            ],
            "url": "truterraag.com/enroll",
        },
        {
            "id":         "verra_vm0042",
            "name":       "Verra VM0042 (via Aggregator)",
            "registry":   "Verra VCS VM0042 v2.2",
            "cred":       "premium",
            "price_floor_per_t":   20.0,
            "price_mid_per_t":     30.0,   # offtake agreements at $30/t (Brazil projects)
            "price_ceiling_per_t": 50.0,   # US premium potential with CCP label
            "payout_split":        0.70,   # typical aggregator takes 30%
            "contract_years":      10,
            "accepts_historical":  False,
            "prog_handles_verif":  False,  # farmer/aggregator arranges VVB
            "eligible_crops": {"corn","soy","wheat","cotton","pasture",
                                "rice","sorghum","alfalfa"},
            "req_additionality": True,
            "nationwide": True,
            "min_acres_practical": 100,    # aggregation required for economics
            "caveats": [
                "Requires an aggregator — not directly accessible to individual farmers yet.",
                "ICVCM CCP-approved (Oct 2025) — premium buyer access.",
                "10-year commitment. Third-party VVB verification required.",
                "Only 9 active US projects as of 2025; market growing rapidly.",
                "Best for farmers with 500+ acres who can afford VVB costs.",
            ],
            "url": "verra.org/methodologies/vm0042",
        },
        {
            "id":         "car_sep_direct",
            "name":       "CAR Soil Enrichment Protocol (Direct)",
            "registry":   "CAR SEP v1.1",
            "cred":       "premium",
            "price_floor_per_t":   15.0,
            "price_mid_per_t":     25.0,
            "price_ceiling_per_t": 40.0,
            "payout_split":        0.75,
            "contract_years":      7,
            "accepts_historical":  False,
            "prog_handles_verif":  False,
            "eligible_crops": {"corn","soy","wheat","cotton","pasture",
                                "rice","sorghum","alfalfa","grazing"},
            "req_additionality": True,
            "nationwide": True,            # US-only methodology
            "min_acres_practical": 200,
            "caveats": [
                "Only 5 total projects on CAR registry as of 2025 — very limited access.",
                "ICVCM CCP-approved — same premium as Verra VM0042.",
                "Requires independent aggregator. Not a turnkey farmer program.",
                "Best route is via Indigo Ag who uses CAR SEP as their registry.",
            ],
            "url": "climateactionreserve.org/protocols/soil-enrichment",
        },
        {
            "id":         "cargill_regenconnect",
            "name":       "Cargill RegenConnect",
            "registry":   "OTC / Internal",
            "cred":       "standard",
            "price_floor_per_t":   20.0,
            "price_mid_per_t":     22.0,
            "price_ceiling_per_t": 25.0,
            "payout_split":        1.0,    # direct payment, no split
            "contract_years":      1,
            "accepts_historical":  False,
            "prog_handles_verif":  True,
            "eligible_crops": {"corn","soy","wheat"},
            "req_additionality": True,
            "nationwide": False,           # 15 states as of 2023-2025
            "caveats": [
                "Available in ~15 states (check cargillag.com/regenconnect).",
                "Credits are NOT registry-certified — OTC only, lower buyer premium.",
                "Short 1-year contract is the key advantage.",
                "Good starting point before committing to longer certified programs.",
            ],
            "url": "cargillag.com/regenconnect",
        },
        {
            "id":         "nori_bayer",
            "name":       "Nori / Bayer ForGround",
            "registry":   "Nori Croplands",
            "cred":       "standard",
            "price_floor_per_t":   15.0,
            "price_mid_per_t":     20.0,
            "price_ceiling_per_t": 30.0,
            "payout_split":        0.85,   # farmer gets ~85% after Nori fee
            "contract_years":      10,
            "accepts_historical":  True,   # Nori accepts historical practices
            "prog_handles_verif":  True,   # COMET-Farm model based
            "eligible_crops": {"corn","soy","wheat","rye","oats","barley"},
            "req_additionality": False,    # historical practices OK
            "nationwide": True,
            "caveats": [
                "10-year permanence commitment — longest in the market.",
                "Historical practices (pre-enrollment) accepted — good for legacy no-tillers.",
                "COMET-Farm modeled — not soil-sampled, lower buyer confidence.",
                "Blockchain-based transparency but smaller corporate buyer network than CAR/Verra.",
            ],
            "url": "nori.com",
        },
        {
            "id":         "fbn_gradable",
            "name":       "FBN Sustainability (Gradable)",
            "registry":   "Internal / COMET-Farm",
            "cred":       "internal",
            "price_floor_per_t":   20.0,
            "price_mid_per_t":     20.0,
            "price_ceiling_per_t": 25.0,
            "payout_split":        1.0,
            "contract_years":      3,
            "accepts_historical":  True,
            "prog_handles_verif":  True,
            "eligible_crops": {"corn","soy","wheat","rye","cover_crops"},
            "req_additionality": False,
            "nationwide": True,
            "caveats": [
                "$20/credit floor with ability to bank credits for future sale.",
                "Smaller buyer network — less price discovery than CAR/Verra programs.",
                "Good option for FBN members already using the platform.",
                "COMET-Farm modeled, not soil sampled — lower premium potential.",
            ],
            "url": "fbn.com/sustainability",
        },
    ]

    # ── Scoring weights ────────────────────────────────────────────────────
    # Weights sum to 1.0.
    # Payout dominates (55%) so rank #1 reflects maximum farmer earnings.
    # Flexibility (10%) still rewards short contracts but won't let a
    # lower-payout program leapfrog a higher-payout one on contract terms alone.
    WEIGHT_PAYOUT        = 0.55
    WEIGHT_FLEXIBILITY   = 0.10
    WEIGHT_ADDITIONALITY = 0.15
    WEIGHT_SIMPLICITY    = 0.10
    WEIGHT_CREDIBILITY   = 0.10

    def __init__(self):
        pass

    def rank(self, audit_result: dict) -> list[ProgramResult]:
        """
        Given a successful bridge.run_carbon_audit() result, evaluate and
        rank all programs. Returns list sorted by rank_score descending.

        Args:
            audit_result: dict with status='success' from bridge.py

        Returns:
            list[ProgramResult] sorted best-first
        """
        if audit_result.get("status") != "success":
            return []

        ai    = audit_result["ai_data"]
        seq   = audit_result["sequestration"]
        acres = audit_result["inputs"]["acres"]

        # Farmer profile extracted from audit
        tillage     = (ai.get("stir_class") or "conventional").lower()
        biomass     = (ai.get("biomass_input") or "low").lower()
        amendments  = (ai.get("amendments") or "none").lower()
        legacy_yrs  = int(ai.get("legacy_years") or 0)
        residue     = ai.get("residue_retained", False)
        tonnes_pa   = seq["total_tonnes_co2e"]  # total annual tonnes for whole farm

        # Infer what crop types might be in play (from tillage context)
        # In a real system this would come from AI data; for now we assume
        # corn/soy row crop as the default since that's our target market
        farmer_crops = {"corn", "soy"}

        results = []
        for prog in self.PROGRAMS:
            result = self._evaluate(prog, farmer_crops, acres, tonnes_pa,
                                    tillage, legacy_yrs, residue,
                                    biomass, amendments)
            results.append(result)

        # Sort by rank_score descending, assign positions
        results.sort(key=lambda r: (-r.rank_score, r.name))
        for i, r in enumerate(results):
            r.rank_position = i + 1

        return results

    def _evaluate(self, prog: dict, farmer_crops: set, acres: float,
                  tonnes_pa: float, tillage: str,
                  legacy_yrs: int, residue: bool,
                  biomass: str = "low", amendments: str = "none") -> ProgramResult:
        """Evaluate a single program against the farmer's profile."""

        caveats = list(prog["caveats"])
        ineligible_reason = None

        # ── Eligibility checks ─────────────────────────────────────────────
        eligible = True

        # Crop type check
        if not farmer_crops.intersection(prog["eligible_crops"]):
            eligible = False
            ineligible_reason = (
                f"Crop types not eligible. Program covers: "
                f"{', '.join(sorted(prog['eligible_crops'])[:4])}."
            )

        # Minimum acres check
        min_acres = prog.get("min_acres_practical", 1)
        if acres < min_acres:
            eligible = False
            ineligible_reason = (
                f"Minimum practical acreage is ~{min_acres} acres "
                f"(you have {acres:.0f} acres). Aggregation required."
            )

        # Additionality: if program requires new practice, check if farmer
        # is already doing all practices (legacy_yrs > 0 = already adopted)
        if prog["req_additionality"] and legacy_yrs > 5:
            # Long-term adopters may be ineligible for new-practice requirement
            # unless the specific program has expanded eligibility (Truterra 2024)
            if prog["id"] not in ("truterra", "indigo"):
                caveats.append(
                    f"⚠️  You've practiced no-till for {legacy_yrs} years. "
                    "Additionality may be limited — confirm with program before enrolling."
                )
            elif prog["id"] == "truterra" and legacy_yrs > 0:
                caveats.append(
                    "Truterra's 2024+ expanded program accepts long-term adopters."
                )

        # ── Payout calculation ─────────────────────────────────────────────
        if not eligible or tonnes_pa == 0:
            payout_floor = payout_mid = payout_ceiling = 0.0
        else:
            payout_floor   = tonnes_pa * prog["price_floor_per_t"]
            payout_mid     = tonnes_pa * prog["price_mid_per_t"]
            payout_ceiling = tonnes_pa * prog["price_ceiling_per_t"]

        per_acre_floor = payout_floor   / acres if acres > 0 else 0
        per_acre_mid   = payout_mid     / acres if acres > 0 else 0

        # ── Scoring ────────────────────────────────────────────────────────
        score = 0.0

        if eligible and tonnes_pa > 0:
            # Payout score: normalize mid payout against max possible ($60/t ceiling)
            max_possible = tonnes_pa * 60.0
            payout_score = min(payout_mid / max_possible, 1.0) * 100
            score += payout_score * self.WEIGHT_PAYOUT

            # Flexibility score: 1yr=100, 3yr=80, 5yr=60, 7yr=40, 10yr=20
            flex_map = {1: 100, 2: 90, 3: 80, 4: 70, 5: 60, 6: 50, 7: 40, 10: 20}
            flex_score = flex_map.get(prog["contract_years"],
                                      max(20, 100 - prog["contract_years"] * 8))
            score += flex_score * self.WEIGHT_FLEXIBILITY

            # Additionality score: accepting historical = 100, requiring new = 40
            addl_score = 100 if prog["accepts_historical"] else 40
            # Bonus if farmer is a legacy adopter (more likely to be eligible)
            if legacy_yrs > 0 and prog["accepts_historical"]:
                addl_score = 100
            elif legacy_yrs > 5 and not prog["accepts_historical"]:
                addl_score = 20  # harder to prove additionality
            score += addl_score * self.WEIGHT_ADDITIONALITY

            # Simplicity score: program handles verification = 100
            simp_score = 100 if prog["prog_handles_verif"] else 30
            score += simp_score * self.WEIGHT_SIMPLICITY

            # Credibility score
            cred_map = {"premium": 100, "standard": 60, "internal": 30}
            score += cred_map[prog["cred"]] * self.WEIGHT_CREDIBILITY

            # ── Practice quality adjustments (bonus/penalty on payout score) ──
            # These reflect which programs pay MORE for premium practices, and
            # whether the farmer's practices align with program requirements.
            practice_bonus = 0.0

            # Biochar/compost amendments: Verra VM0042 and CAR SEP explicitly
            # credit soil amendments. Indigo also benefits from higher credit
            # quality. Boost premium-registry programs for amendment users.
            if amendments in ("biochar", "compost") and prog["cred"] == "premium":
                practice_bonus += 3.0

            # High biomass (multi-species covers): Indigo pays a quality premium
            # for verified removals backed by diverse cover crops.
            if biomass == "high" and prog["id"] in ("indigo", "car_sep_direct", "verra_vm0042"):
                practice_bonus += 2.0

            # Residue removed: weakens additionality case for most programs.
            # Programs that rely on soil sampling will detect lower SOC accretion.
            if residue is False:
                practice_bonus -= 2.0

            score = min(100.0, score + practice_bonus)

        # Surface residue caveat if applicable
        if residue is False and eligible:
            caveats = list(caveats)  # ensure mutable copy
            caveats.append(
                "⚠️  Residue removed from field — some programs may reduce payout "
                "as this lowers verified SOC accrual rates."
            )

        return ProgramResult(
            name=prog["name"],
            registry=prog["registry"],
            eligible=eligible,
            ineligibility_reason=ineligible_reason,
            payout_floor=round(payout_floor, 2),
            payout_mid=round(payout_mid, 2),
            payout_ceiling=round(payout_ceiling, 2),
            per_acre_floor=round(per_acre_floor, 2),
            per_acre_mid=round(per_acre_mid, 2),
            contract_years=prog["contract_years"],
            payment_type="per_tonne",
            accepts_historical=prog["accepts_historical"],
            program_handles_verification=prog["prog_handles_verif"],
            registry_credibility=prog["cred"],
            rank_score=round(score, 2),
            rank_position=0,
            caveats=caveats,
            url=prog["url"],
        )

    def format_report(self, results: list[ProgramResult],
                      audit_result: dict) -> str:
        """Render a human-readable program comparison report."""
        acres = audit_result["inputs"]["acres"]
        lines = []
        lines.append("")
        lines.append("=" * 70)
        lines.append("💰 CARBON PROGRAM PAYOUT PREDICTOR & RANKING")
        lines.append("=" * 70)
        lines.append(
            f"Farm: {acres:.0f} acres | "
            f"Tillage: {(audit_result['ai_data'].get('stir_class') or 'Unknown').title()} | "
            f"Legacy: {audit_result['ai_data'].get('legacy_years', 0)} yrs"
        )
        lines.append(
            f"Estimated annual sequestration: "
            f"{audit_result['sequestration']['total_tonnes_co2e']} t CO₂e"
        )
        lines.append("")
        lines.append("Rankings based on: payout (40%), flexibility (20%), "
                     "additionality burden (15%),")
        lines.append("                  verification simplicity (15%), "
                     "registry credibility (10%)")
        lines.append("-" * 70)

        eligible   = [r for r in results if r.eligible]
        ineligible = [r for r in results if not r.eligible]

        if eligible:
            lines.append(f"\n{'RANK':<5} {'PROGRAM':<34} {'FLOOR':>8} {'MID':>10} "
                         f"{'CEILING':>10} {'CONTRACT':<10} {'REGISTRY'}")
            lines.append("-" * 90)
            for r in eligible:
                lines.append(
                    f"  #{r.rank_position:<3} {r.name:<34} "
                    f"${r.payout_floor:>8,.0f} ${r.payout_mid:>9,.0f} "
                    f"${r.payout_ceiling:>9,.0f}   {r.contract_years}yr        "
                    f"{r.registry}"
                )
            lines.append("")

            # Detail block for top 3
            lines.append("── TOP PROGRAM DETAILS " + "─" * 48)
            for r in eligible[:3]:
                lines.append(f"\n  #{r.rank_position} {r.name}")
                lines.append(f"     Registry:       {r.registry}")
                lines.append(f"     Farmer payout:  ${r.payout_floor:,.0f} – "
                             f"${r.payout_ceiling:,.0f} / yr  "
                             f"(${r.per_acre_mid:,.1f}/acre mid)")
                lines.append(f"     Contract:       {r.contract_years} year(s)")
                lines.append(f"     Verification:   "
                             f"{'Program handles it ✅' if r.program_handles_verification else 'Farmer arranges ⚠️'}")
                lines.append(f"     Historical OK:  "
                             f"{'Yes ✅' if r.accepts_historical else 'No — new practice required ⚠️'}")
                for c in r.caveats[:2]:
                    lines.append(f"     📌 {c}")
                lines.append(f"     🔗 {r.url}")

        if ineligible:
            lines.append("\n── INELIGIBLE PROGRAMS " + "─" * 47)
            for r in ineligible:
                lines.append(f"  ✗  {r.name}: {r.ineligibility_reason}")

        lines.append("\n" + "=" * 70)
        lines.append("⚠️  DISCLAIMER: Payouts are estimates based on published program")
        lines.append("   disclosures and research data. Actual payments depend on")
        lines.append("   verified soil sampling, buyer demand, and enrollment eligibility.")
        lines.append("   Always confirm current terms at each program's official website.")
        lines.append("=" * 70)
        return "\n".join(lines)
