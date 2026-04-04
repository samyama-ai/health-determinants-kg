// Health Determinants Knowledge Graph — Schema
// 7 Node Labels, 6 Edge Types
// Sources: World Bank WDI, WHO Air Quality, FAO AQUASTAT, UNDP HDI
// Focus: "Why are populations vulnerable?"

// --- Indexes (create BEFORE loading data) ---
CREATE INDEX ON :Country(iso_code);
CREATE INDEX ON :Country(name);
CREATE INDEX ON :Region(code);
CREATE INDEX ON :Region(name);
CREATE INDEX ON :SocioeconomicIndicator(id);
CREATE INDEX ON :EnvironmentalFactor(id);
CREATE INDEX ON :NutritionIndicator(id);
CREATE INDEX ON :DemographicProfile(id);
CREATE INDEX ON :WaterResource(id);

// --- Node Labels ---
// Country:                  iso_code (ISO 3166-1 alpha-3), name, income_level, region_wb
// Region:                   code (World Bank region code), name
// SocioeconomicIndicator:   id (SE-{iso}-{code}-{year}), indicator_code, indicator_name, year, value, category
// EnvironmentalFactor:      id (EF-{iso}-{code}-{year}), indicator_code, indicator_name, year, value, category
// NutritionIndicator:       id (NI-{iso}-{code}-{year}), indicator_code, indicator_name, year, value, category
// DemographicProfile:       id (DP-{iso}-{code}-{year}), indicator_code, indicator_name, year, value, category
// WaterResource:            id (WR-{iso}-{code}-{year}), indicator_code, indicator_name, year, value, category

// --- Edge Types ---
// HAS_INDICATOR:      Country -> SocioeconomicIndicator
// ENVIRONMENT_OF:     Country -> EnvironmentalFactor
// NUTRITION_STATUS:   Country -> NutritionIndicator
// DEMOGRAPHIC_OF:     Country -> DemographicProfile
// WATER_RESOURCE_OF:  Country -> WaterResource
// IN_REGION:          Country -> Region

// --- Cross-KG Bridge Properties ---
// Country.iso_code   -> surveillance-kg Country.iso_code
// Country.iso_code   -> health-systems-kg Country.iso_code
// Country.iso_code   -> clinicaltrials-kg (via ICTRP country mapping)
// Region.code        -> surveillance-kg Region.who_code (with mapping)
