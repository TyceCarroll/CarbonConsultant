import math

def calculate_sequestration(ai_data, soc_baseline_percent, bulk_density, acres, market_price):
    """
    Calculates the estimated annual metric tonnes of CO2e sequestered 
    using exact soil mass calculations via Bulk Density.
    """
    # 1. Base Multipliers
    tillage_weights = {"no-till": 1.20, "strip-till": 1.12, "reduced": 1.05, "conventional": 1.00}
    input_weights = {"high": 1.15, "medium": 1.07, "low": 1.00}
    amendment_weights = {"biochar": 1.25, "compost": 1.18, "manure": 1.12, "none": 1.00}

    # Safely extract variables
    tillage = str(ai_data.get('stir_class', 'conventional')).lower()
    biomass = str(ai_data.get('biomass_input', 'low')).lower()
    amendment = str(ai_data.get('amendments', 'none')).lower()
    legacy_years = int(ai_data.get('legacy_years', 1))
    residue_retained = ai_data.get('residue_retained', False)

    f_tillage = tillage_weights.get(tillage, 1.00)
    f_input = input_weights.get(biomass, 1.00)
    f_amend = amendment_weights.get(amendment, 1.00)

    # 2. Modifiers
    residue_multiplier = 1.0 if residue_retained else 0.4
    k = 0.05
    time_factor = math.exp(-k * legacy_years)

    # 3. EXACT MASS CALCULATION (The Wiggle-Room Killer)
    # 1 acre = 4046.86 sq meters.
    # Depth = 0.3m (30cm) matches USDA SDA SQL query (hzdept_r < 30).
    # Using 20cm while SOC baseline comes from 30cm profile understates carbon stock by ~33%.
    MEASUREMENT_DEPTH_M = 0.3
    volume_m3_per_acre = 4046.86 * MEASUREMENT_DEPTH_M
    # Bulk density is typically g/cm3, which equals metric tonnes per cubic meter
    soil_mass_tonnes_per_acre = volume_m3_per_acre * bulk_density 
    
    # Calculate actual carbon mass in the soil
    current_carbon_tonnes_per_acre = soil_mass_tonnes_per_acre * (soc_baseline_percent / 100)

    # 4. Apply Practice Deltas
    # How much MORE carbon can this soil hold based on these practices?
    max_potential_carbon_added = current_carbon_tonnes_per_acre * (f_tillage * f_input * f_amend - 1)
    
    # Distribute over 20 years, modified by residue loss and saturation decay
    annual_carbon_delta = (max_potential_carbon_added / 20) * residue_multiplier * time_factor

    # 5. Convert to CO2e and scale by acreage
    tonnes_co2_per_acre = annual_carbon_delta * 3.67
    total_tonnes = tonnes_co2_per_acre * acres
    
    # --- NEW: OPTIMIZATION TIP (The Sanity Cap) ---
    REGISTRY_CAP = 2.2  # Max realistic tonnes for row crops
    flag_for_review = False
    
    if tonnes_co2_per_acre > REGISTRY_CAP:
        flag_for_review = True
        # We cap the reported value for the economic projection to remain conservative 
        tonnes_co2_per_acre = REGISTRY_CAP 

    total_tonnes = tonnes_co2_per_acre * acres
    
    # 6. Economic Projection
    estimated_value = total_tonnes * market_price

    return {
        "tonnes_per_acre": round(tonnes_co2_per_acre, 3),
        "total_tonnes_co2e": round(total_tonnes, 2),
        "estimated_annual_value_usd": round(estimated_value, 2),
        "saturation_warning": legacy_years > 15,
        "flag_for_review": flag_for_review
    }