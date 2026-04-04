import os
import json
import time
from datetime import datetime
from google import genai
from google.genai import types
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize the Gemini Client
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# --- RIGOROUS SCIENTIFIC RUBRIC ---
# This ensures <10% wiggle room by forcing specific data extraction
AG_RUBRIC = """
STRICT SCIENTIFIC CLASSIFICATION:
- stir_class: 'No-Till' (STIR <20), 'Strip-Till' (STIR 20-40), 'Reduced' (STIR 40-80), 'Conventional' (STIR >80).
- residue_retained: Boolean (True if stalks/straw left on field, False if baled/removed).
- biomass_input: 'High' (Multi-species covers), 'Medium' (Single-species), 'Low' (No covers).
- legacy_years: Number of years since practicing these methods.
- amendments: 'None', 'Manure', 'Compost', 'Biochar' (Priority: Biochar > Compost > Manure).
"""

def robust_farm_parser(narrative_text):
    """
    Parses natural language farm narratives into high-precision soil science data.
    Uses Gemini 3.1 Flash Lite Preview for cost-efficient, high-volume extraction.
    """
    current_year = datetime.now().year

    try:
        response = client.models.generate_content(
            model="gemini-3.1-flash-lite-preview",
            contents=narrative_text,
            config=types.GenerateContentConfig(
                system_instruction=(
                    f"You are a Senior Soil Scientist. Analyze this farm narrative based on: {AG_RUBRIC}\n"
                    f"Assume the current year is {current_year}. Output valid JSON with strictly LOWERCASE keys. "
                    "DO NOT wrap the output in a list/array. "
                    "CRITICAL: Be extremely skeptical. DO NOT assume residue is retained or amendments are used "
                    "unless explicitly stated. If an attribute is not CLEARLY mentioned, set it to null "
                    "and include a specific question in 'missing_data_query'."
                ),
                response_mime_type="application/json"
            )
        )
        
        # 1. Strip potential Markdown artifacts
        clean_text = response.text.replace("```json", "").replace("```", "").strip()
        result = json.loads(clean_text)

        # 2. Defensive Parsing: Handle unexpected list-wrapping
        if isinstance(result, list):
            result = result[0] if len(result) > 0 else {}

        # 3. Schema Normalization: Ensure 'missing_data_query' always exists
        if "missing_data_query" not in result:
            result["missing_data_query"] = None

        return result

    except Exception as e:
        print(f"❌ Extraction Error: {e}")
        return None