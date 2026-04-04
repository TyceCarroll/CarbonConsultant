import requests
import statistics
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# AgPriceOracle — CarbonConsultant Market Pricing Engine
#
# Source: Carbonmark public REST API v18 (no auth required for GET)
#   Base URL : https://v18.api.carbonmark.com
#   Endpoint : GET /carbonProjects
#   Response : {"items": [...], "itemsCount": N, "offset": N}  (paginated)
#   Price    : item["price"]  (top-level string, can be 0 or numeric string)
#
# Valid v18 categories (confirmed via /categories endpoint):
#   Agriculture, Biochar, Blue Carbon, Energy Efficiency, Forestry,
#   Industrial Processing, Other, Renewable Energy, Waste Disposal
#   ("Other Nature Based" does NOT exist in v18)
#
# Architecture: TIERED BENCHMARK
#   Tier 1 — Carbonmark live spot price (Agriculture -> Biochar -> global,
#             all paginated). Used only when MIN_SAMPLE_SIZE listings found.
#             Carbonmark is a premium registry marketplace — prices are higher
#             than typical US ag program payouts. When n < MIN_SAMPLE_SIZE,
#             the Carbonmark ceiling is stored in metadata for display only.
#   Tier 2 — Research-backed benchmark ($20.00/tonne, updated annually).
#             Sources: MSCI 2025 (high-rated NBS avg $14.80/t),
#             Ecosystem Marketplace 2025 (NBS offtake avg >$20/t),
#             US programs 2025: Truterra/Indigo/Corteva $15-$28/t.
#             Midpoint of reputable range = $20.00. Conservative by design.
# ---------------------------------------------------------------------------

CARBONMARK_BASE    = "https://v18.api.carbonmark.com"
INTERNAL_BENCHMARK = 20.00   # 2025-2026 US row-crop conservative midpoint
SANITY_MIN         = 1.0     # below = pool/USDC pricing artefact, not USD market
SANITY_MAX         = 200.0   # above = clearly erroneous
PREMIUM_MULTIPLIER = 1.15    # No-Till + Cover Crop verified-removal bonus
FLOOR_MULTIPLIER   = 0.90    # 10% conservative safety margin
MIN_SAMPLE_SIZE    = 3       # minimum listings for reliable IQR median
MAX_PAGES          = 5       # max pages to paginate per query (avoids runaway)
PAGE_SIZE          = 50      # items per page request


class AgPriceOracle:
    """
    Fetches voluntary carbon credit prices from the Carbonmark v18 API
    and formats a conservative price payload for the CarbonConsultant pipeline.

    Output schema (get_consensual_price):
    {
        "prices": {
            "conservative_floor": float,
            "standard_registry":  float,
            "premium_market":     float
        },
        "metadata": {
            "source":             str,
            "timestamp":          str (ISO-8601 UTC),
            "mode":               str,   "Carbonmark-Live" | "Benchmark"
            "status":             str,   "Live" | "Fallback"
            "n_listings":         int,
            "carbonmark_ceiling": float|None
        }
    }
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept":     "application/json",
            "User-Agent": "CarbonConsultant-PriceOracle/2.0",
        })

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def get_consensual_price(self) -> dict:
        """
        Returns a conservative price payload for the pipeline.
        Tries Carbonmark with pagination across three category strategies.
        Falls back to benchmark when n < MIN_SAMPLE_SIZE.
        Always stores the best Carbonmark spot in metadata.
        """
        all_prices = []
        best_source = None

        # Strategy 1: Agriculture — exact match for US soil/row-crop credits
        prices, source = self._fetch_all_pages(category="Agriculture")
        if len(prices) > len(all_prices):
            all_prices, best_source = prices, source

        # Strategy 2: Biochar — directly adjacent verified removal category
        if len(all_prices) < MIN_SAMPLE_SIZE:
            prices, source = self._fetch_all_pages(category="Biochar")
            if len(prices) > len(all_prices):
                all_prices, best_source = prices, source

        # Strategy 3: Global unfiltered — paginate entire marketplace
        if len(all_prices) < MIN_SAMPLE_SIZE:
            prices, source = self._fetch_all_pages()
            if len(prices) > len(all_prices):
                all_prices, best_source = prices, source

        # Store the best Carbonmark spot for metadata even if below threshold
        carbonmark_ceiling = (
            round(self._iqr_filtered_median(all_prices), 2) if all_prices else None
        )
        carbonmark_n = len(all_prices)

        # Use live price only when we have enough data for a reliable median
        if carbonmark_n >= MIN_SAMPLE_SIZE:
            live_price = self._iqr_filtered_median(all_prices)
            return self._format_payload(
                price=live_price,
                source=best_source,
                n=carbonmark_n,
                carbonmark_ceiling=carbonmark_ceiling,
                live=True,
            )

        # Benchmark fallback
        return self._format_payload(
            price=INTERNAL_BENCHMARK,
            source="2025-2026 US Ag Benchmark (MSCI/Ecosystem Marketplace)",
            n=carbonmark_n,
            carbonmark_ceiling=carbonmark_ceiling,
            live=False,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch_all_pages(self, category: str = None) -> tuple:
        """
        Paginates /carbonProjects up to MAX_PAGES * PAGE_SIZE items.
        Returns (prices: list[float], source_label: str).
        """
        label = (f"Carbonmark API ({category})" if category
                 else "Carbonmark API (Global)")
        all_prices = []
        offset = 0

        for _ in range(MAX_PAGES):
            params = {"limit": PAGE_SIZE, "offset": offset}
            if category:
                params["category"] = category

            try:
                resp = self.session.get(
                    f"{CARBONMARK_BASE}/carbonProjects",
                    params=params,
                    timeout=12,
                )
                resp.raise_for_status()
                data = resp.json()

                if isinstance(data, dict):
                    items = data.get("items", [])
                    total = data.get("itemsCount", 0)
                elif isinstance(data, list):
                    items = data          # graceful fallback for older API versions
                    total = len(data)
                else:
                    break

                all_prices.extend(self._extract_prices(items))

                offset += PAGE_SIZE
                if offset >= total:
                    break

            except Exception as e:
                print(f"Warning: Carbonmark API error ({label}, offset={offset}): {e}")
                break

        return all_prices, label

    @staticmethod
    def _extract_prices(items: list) -> list:
        """Extracts valid USD prices, applying hasSupply and sanity filters."""
        prices = []
        for project in items:
            if not project.get("hasSupply", False):
                continue
            raw = project.get("price")
            if raw is None:
                continue
            try:
                val = float(raw)
                if SANITY_MIN <= val <= SANITY_MAX:
                    prices.append(val)
            except (ValueError, TypeError):
                continue
        return prices

    @staticmethod
    def _iqr_filtered_median(prices: list) -> float:
        """IQR outlier removal then median. Plain median for n < 4."""
        if len(prices) < 4:
            return statistics.median(prices)
        sorted_p = sorted(prices)
        n = len(sorted_p)
        q1 = statistics.median(sorted_p[: n // 2])
        q3 = statistics.median(sorted_p[n // 2 + (n % 2):])
        iqr = q3 - q1
        lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        filtered = [p for p in sorted_p if lower <= p <= upper]
        return statistics.median(filtered) if filtered else statistics.median(sorted_p)

    @staticmethod
    def _format_payload(price: float, source: str, n: int,
                        carbonmark_ceiling, live: bool) -> dict:
        """Builds the standard output dict consumed by bridge.py."""
        return {
            "prices": {
                "conservative_floor": round(price * FLOOR_MULTIPLIER, 2),
                "standard_registry":  round(price, 2),
                "premium_market":     round(price * PREMIUM_MULTIPLIER, 2),
            },
            "metadata": {
                "source":             source,
                "timestamp":          datetime.now(timezone.utc).isoformat(),
                "mode":               "Carbonmark-Live" if live else "Benchmark",
                "status":             "Live" if live else "Fallback",
                "n_listings":         n,
                "carbonmark_ceiling": carbonmark_ceiling,
                # is_fallback=True means revenue was calculated using the
                # research-backed benchmark ($20/t), NOT a live market price.
                # The farmer should be informed and the report must note this.
                "is_fallback":        not live,
            },
        }


if __name__ == "__main__":
    import json
    oracle = AgPriceOracle()
    print(json.dumps(oracle.get_consensual_price(), indent=2))