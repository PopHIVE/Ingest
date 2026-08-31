# Shared setup for Youth_Wellbeing_Draft_Dashboard.qmd
# Loads packages, reads every parquet file used by the dashboard once, and
# provides small helpers for emitting compact JSON <script> blocks that the
# dashboard's vanilla-JS/Plotly components read from `window.*`.

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(tidyr)
  library(jsonlite)
})

# ---------------------------------------------------------------------------
# FIPS / state-abbreviation crosswalk (per repo convention)
# ---------------------------------------------------------------------------
all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

state_abbr_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(state_fips = geography, geography_name, state_abbr = state) %>%
  filter(state_abbr != "US")

add_state_abbr <- function(df, name_col = "geography") {
  df %>% left_join(state_abbr_lookup, by = setNames("geography_name", name_col))
}

fips2abbr <- setNames(state_abbr_lookup$state_abbr, state_abbr_lookup$state_fips)
fips2name <- setNames(state_abbr_lookup$geography_name, state_abbr_lookup$state_fips)

# ---------------------------------------------------------------------------
# Canonical geography lists (one fixed loc/label array per level, for the
# WHOLE dashboard). Every choropleth measure and every scatter-tool measure
# aligns its values to these instead of carrying its own locs/labels -- that
# was previously the single biggest source of HTML bloat: the same ~51 state
# or ~3,140 county id+name pairs were being repeated verbatim inside every
# one of dozens of measure entries. See `build_measure_entry()` below.
# ---------------------------------------------------------------------------
geo_state_canon <- state_abbr_lookup %>%
  transmute(loc = state_abbr, loc_label = geography_name) %>%
  distinct(loc, .keep_all = TRUE) %>%
  arrange(loc)

geo_county_canon <- all_fips %>%
  filter(nchar(geography) == 5) %>%
  transmute(loc = geography, loc_label = paste0(geography_name, ", ", state)) %>%
  distinct(loc, .keep_all = TRUE) %>%
  arrange(loc)

# ---------------------------------------------------------------------------
# Load every source file the dashboard uses (read once)
# ---------------------------------------------------------------------------
chr_county   <- read_parquet("dist/chr_county.parquet")
chr_state    <- read_parquet("dist/chr_state.parquet")
epic_chronic_county <- read_parquet("dist/epic_chronic_county_age.parquet")
epic_chronic_state  <- read_parquet("dist/epic_chronic_state_age.parquet")
epic_injury_year  <- read_parquet("dist/epic_injury_state_age_year.parquet")
epic_injury_month <- read_parquet("dist/epic_injury_state_age_month.parquet")
medicaid     <- read_parquet("dist/medicaid_state_payer.parquet")
neiss_diag   <- read_parquet("dist/neiss_diagnosis_age_sex_year.parquet")
neiss_prod   <- read_parquet("dist/neiss_product_age_sex_year.parquet")
nhtsa_state  <- read_parquet("dist/nhtsa_state_age_sex.parquet")
nhtsa_county <- read_parquet("dist/nhtsa_county_age_sex.parquet")
noaa_county  <- read_parquet("dist/noaa_heat_risk_county.parquet")
noaa_state   <- read_parquet("dist/noaa_heat_risk_state.parquet")
wisqars      <- read_parquet("dist/wisqars_state_age_demographics.parquet")
yrbss        <- read_parquet("dist/yrbss_state_age_demographics.parquet")

# Epic Cosmos concussion ED-visit data. This lives in a separate
# epic_preprocessing pipeline that hasn't yet been formally ingested into
# PopHIVE/Ingest as its own data/{source} folder, so for now it's read
# directly from its absolute path on this machine rather than from dist/.
epic_concussion_path <- "C:/Users/as5325/Desktop/epic_preprocessing/data/cosmos_concussions/standard/data.csv.gz"
epic_concussion <- vroom::vroom(epic_concussion_path, show_col_types = FALSE) %>%
  mutate(geography_name = if_else(geography == "00", "United States", fips2name[geography])) %>%
  filter(!is.na(geography_name))

# Epic Cosmos mental-health ED length-of-stay data (Suicidal behavior
# diagnosis only -- the Mood diagnosis bucket in the same file is out of
# scope for now). Same stopgap as epic_concussion above: this lives on the
# epic_preprocessing repo's `ingest-mh` branch and hasn't yet been merged /
# formally ingested into PopHIVE/Ingest as its own data/{source} folder.
# Median/Q1/Q3 ED length-of-stay cells are suppressed-to-NA at source (no
# imputation, unlike count-based Epic measures), so no suppressed_flag
# handling is needed downstream for those three. `pct_share` is a
# compositional share of THIS diagnosis's own encounters across state/age --
# not a rate or a visit count -- see the chart's "About this chart" caveat.
epic_mh_path <- "C:/Users/as5325/Desktop/epic_preprocessing/data/cosmos_mental_health/standard/data.csv.gz"
epic_mh_suicidal <- vroom::vroom(epic_mh_path, show_col_types = FALSE) %>%
  transmute(
    geography, time, age,
    geography_name = if_else(geography == "00", "United States", fips2name[geography]),
    median_los = epic_median_ed_los_suicidal_behavior,
    q1_los = epic_q1_ed_los_suicidal_behavior,
    q3_los = epic_q3_ed_los_suicidal_behavior,
    pct_share = epic_pct_sliced_population_suicidal_behavior,
    pct_share_suppressed = epic_pct_sliced_population_suicidal_behavior_suppressed_flag
  ) %>%
  filter(!is.na(geography_name))

# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------

# Round a numeric vector for compact JSON without materially changing display.
rnd <- function(x, digits = 2) {
  ifelse(is.na(x), NA, round(as.numeric(x), digits))
}

# jsonlite's auto_unbox (needed so scalar fields like `title`/`height` don't
# serialize as 1-element arrays) has a sharp edge: any *vector* field that
# happens to have exactly one element (a state with only one substance
# recorded, a map measure with only one available year, ...) gets collapsed
# to a bare JSON scalar too, which breaks every renderer's `.map()`/
# `.forEach()`/`.length` calls in JS. Recursively force known always-array
# keys to stay arrays regardless of length, so this can't regress silently
# as new charts/data combinations are added.
force_arrays <- function(x, keys = c("options", "seriesOrder", "defaultOn", "times", "locs", "labels", "x", "y", "note", "values", "errUp", "errDown")) {
  if (is.list(x)) {
    nms <- names(x)
    for (i in seq_along(x)) {
      key <- if (!is.null(nms)) nms[i] else ""
      val <- x[[i]]
      if (nzchar(key) && key %in% keys && is.atomic(val) && !is.null(val) && !inherits(val, "AsIs")) {
        x[[i]] <- I(val)
      } else if (is.list(val)) {
        x[[i]] <- force_arrays(val, keys)
      }
    }
  }
  x
}

# Emit `window.<name> = <json>;` inside a <script> tag. Call from a chunk with
# `#| results: asis`. `auto_unbox` keeps scalars as scalars (not length-1
# arrays); NA -> null.
emit_json <- function(name, obj) {
  obj <- force_arrays(obj)
  json <- jsonlite::toJSON(obj, auto_unbox = TRUE, na = "null", digits = NA, null = "null")
  cat(sprintf('<script>\nwindow.%s = %s;\n</script>\n', name, json))
}

# Natural sort key for NEISS's mixed "NN months" / "N-N years" age labels so
# dropdowns/legends read in age order rather than alphabetically.
neiss_age_order <- c(sprintf("%02d months", 0:23), "2-4 years", "5-9 years", "10-14 years", "15-19 years")
neiss_age_label <- function(a) {
  ifelse(grepl("months$", a),
         paste0(as.integer(sub(" months", "", a)), " month", ifelse(as.integer(sub(" months", "", a)) == 1, "", "s")),
         a)
}

yrbss_age_order <- c("14 years", "15 years", "16 years", "17 years", "Overall")

# ---------------------------------------------------------------------------
# Generic reshapers used by (almost) every chart in the dashboard
# ---------------------------------------------------------------------------

# Build one JS "measure" entry for renderChoropleth from a tidy df with
# columns: loc (join key: 2-letter state abbr or 5-digit county FIPS), t
# (time value: year int or ISO date string), value (numeric), and optionally
# note (string suffix shown in the hover, e.g. " (suppressed - imputed)").
# `tags` is an optional named list used by renderChoropleth's extra dropdown
# filters to narrow the measure list (e.g. list(age = "0-14 years")).
#
# Values align to the shared `geo_state_canon`/`geo_county_canon` id order
# (window.PH_GEO_STATE/PH_GEO_COUNTY on the JS side) rather than carrying
# their own locs/labels -- see the comment above those two data frames.
build_measure_entry <- function(df, id, label, level = c("state", "county"), unit = "",
                                 decimals = 1, sub = "", reverse = FALSE, tags = NULL) {
  level <- match.arg(level)
  canon <- if (level == "state") geo_state_canon else geo_county_canon
  has_note <- "note" %in% names(df)
  if (has_note) df$note[is.na(df$note)] <- ""
  df <- df %>% filter(!is.na(loc), !is.na(t))
  times <- sort(unique(df$t))
  z <- lapply(times, function(tt) {
    sub_df <- df[df$t == tt, ]
    rnd(sub_df$value[match(canon$loc, sub_df$loc)], decimals)
  })
  entry <- list(
    id = id, label = label, level = level, unit = unit, decimals = decimals,
    sub = sub, reverse = reverse, times = I(times), z = z
  )
  if (has_note && any(nzchar(df$note))) {
    entry$extra <- lapply(times, function(tt) {
      sub_df <- df[df$t == tt, ]
      nt <- sub_df$note[match(canon$loc, sub_df$loc)]
      ifelse(is.na(nt), "", nt)
    })
  }
  if (!is.null(tags)) entry$tags <- tags
  entry
}

# Build the `compareBy` config block for renderLineChart's "Compare by"
# dropdown, used on every YRBSS time series: toggle the legend between lines
# for each age (sex held at "All") or lines for each sex (age held at
# "Overall"). `ages` should already be filtered to the levels present in the
# specific chart's data.
#
# NOTE: a combined "age x sex" cross-tab (e.g. "14 years, Female") is NOT
# offered here because the data doesn't support it -- CDC's YRBSS chart-data
# export (this bundle's only source) stratifies by Total, Sex, Race, or
# Grade one at a time, never Sex-by-Grade jointly (see data/yrbss/ingest.R's
# StratType handling). Every age-specific row has sex == "All" and every
# sex-specific row has age == "Overall"; there is no row with both a
# specific age AND a specific sex. Confirmed by cross-tabulating
# dist/yrbss_state_age_demographics.parquet: 0 rows exist at the
# intersection. A joint compareBy group was tried and shipped an empty
# chart for exactly this reason -- don't re-add it without a new upstream
# source that publishes the joint breakdown.
yrbss_compare_by <- function(ages = yrbss_age_order, default = "age") {
  list(
    label = "Compare by", default = default,
    groups = list(
      age = list(label = "Age", fixed = list(sex = "All"), seriesOrder = ages,
                 seriesMeta = setNames(lapply(ages, function(a) list(label = a)), ages)),
      sex = list(label = "Sex", fixed = list(age = "Overall"), seriesOrder = c("All", "Female", "Male"),
                 seriesMeta = list(All = list(label = "Overall"), Female = list(label = "Female"), Male = list(label = "Male")))
    )
  )
}

# Build the JS "lines" array for renderLineChart from a tidy df. `dim_cols`
# are the dimensions driven by dropdown filters (must match `filters[].key`
# in the chart config); `series_col` identifies which column distinguishes
# separate lines/legend entries (e.g. a measure name or an age group).
#
# `lower_col`/`upper_col` are optional companion columns (e.g. Q1/Q3) that,
# when both given, emit `errUp`/`errDown` -- offsets from `y_col`, as
# Plotly's asymmetric `error_y` wants, not the raw bounds. A row whose
# companion is NA (present in one bound but not the other) gets an offset of
# 0 rather than NA, so the point still renders with no visible whisker
# instead of breaking the chart's error_y array.
build_lines <- function(df, dim_cols, series_col, x_col, y_col, note_col = NULL,
                         lower_col = NULL, upper_col = NULL) {
  d <- df
  d$.x <- d[[x_col]]
  d$.y <- rnd(d[[y_col]], 3)
  has_note <- !is.null(note_col)
  if (has_note) { n <- d[[note_col]]; d$.note <- ifelse(is.na(n), "", n) }
  has_range <- !is.null(lower_col) && !is.null(upper_col)
  if (has_range) {
    err_down <- d[[y_col]] - d[[lower_col]]
    err_up   <- d[[upper_col]] - d[[y_col]]
    d$.errDown <- ifelse(is.na(err_down), 0, rnd(err_down, 3))
    d$.errUp   <- ifelse(is.na(err_up), 0, rnd(err_up, 3))
  }
  d$.series <- as.character(d[[series_col]])
  key_cols <- c(dim_cols, ".series")
  d <- d %>% arrange(across(all_of(key_cols)), .x)
  grp <- do.call(paste, c(d[key_cols], sep = ""))
  split_idx <- split(seq_len(nrow(d)), grp, drop = TRUE)
  lapply(split_idx, function(idx) {
    sub <- d[idx, , drop = FALSE]
    dims <- as.list(sub[1, dim_cols, drop = FALSE])
    out <- list(dims = dims, series = sub$.series[1], x = I(sub$.x), y = I(sub$.y))
    if (has_note && any(nzchar(sub$.note))) out$note <- I(sub$.note)
    if (has_range) { out$errUp <- I(sub$.errUp); out$errDown <- I(sub$.errDown) }
    out
  }) %>% unname()
}

# ---------------------------------------------------------------------------
# County Health Rankings measure metadata. measure_info.json only defines
# short_name/long_name/unit for a handful of these measures; the rest were
# inferred from each measure's actual value range (see chr_state %>%
# group_by(measure) %>% summarise(min,median,max)) against County Health
# Rankings & Roadmaps' published methodology. `scale` converts the raw value
# to the display unit (x100 for 0-1 proportions, x100000 for tiny per-capita
# ratios reported as rates per 100,000).
# ---------------------------------------------------------------------------
chr_measure_info <- tibble::tribble(
  ~measure, ~label, ~unit, ~scale, ~decimals,
  "chr_infant_mortality", "Infant mortality", "per 1,000 live births", 1, 1,
  "chr_child_mortality", "Child mortality (ages 1-19)", "per 100,000 population", 1, 1,
  "chr_access_to_exercise_opportunities", "Access to exercise opportunities", "%", 100, 1,
  "chr_access_to_parks", "Access to parks (within half-mile in urban / 1 mile in rural areas)", "%", 100, 1,
  "chr_access_to_recreational_facilities", "Recreational facilities", "per 100,000", 1, 1,
  "chr_adverse_climate_events", "Adverse climate events (FEMA-declared, cumulative)", "events", 1, 0,
  "chr_air_pollution_ozone_days", "Ozone days (exceeding standard)", "days/year", 1, 1,
  "chr_air_pollution_particulate_matter", "Particulate matter (PM2.5)", "µg/m³", 1, 1,
  "chr_air_pollution_particulate_matter_days", "Particulate matter days (exceeding standard)", "days/year", 1, 1,
  "chr_child_care_centers", "Child care centers", "per 1,000 children under 5", 1, 1,
  "chr_children_in_poverty", "Children in poverty", "%", 100, 1,
  "chr_disconnected_youth", "Disconnected youth (ages 16-19, not in school and not working)", "%", 100, 1,
  "chr_firearm_fatalities", "Firearm fatalities", "per 100,000", 1, 1,
  "chr_food_environment_index", "Food environment index", "index (0-10, best)", 1, 1,
  "chr_food_insecurity", "Food insecurity", "%", 100, 1,
  "chr_high_housing_costs", "High housing costs", "%", 100, 1,
  "chr_high_school_completion", "High school completion", "%", 100, 1,
  "chr_high_school_graduation", "High school graduation (4-year cohort)", "%", 100, 1,
  "chr_inadequate_social_support", "Inadequate social support", "%", 100, 1,
  "chr_income_inequality", "Income inequality (80th:20th percentile ratio)", "ratio", 1, 2,
  "chr_juvenile_arrests", "Juvenile arrests", "per 1,000 juveniles", 1, 1,
  "chr_limited_access_to_healthy_foods", "Limited access to healthy foods", "%", 100, 1,
  "chr_math_scores", "Math test scores (avg. standardized)", "score", 1, 2,
  "chr_other_primary_care_providers", "Other primary care providers", "per 100,000", 100000, 1,
  "chr_primary_care_physicians", "Primary care physicians", "per 100,000", 100000, 1,
  "chr_reading_scores", "Reading test scores (avg. standardized)", "score", 1, 2,
  "chr_residential_segregation_black_white", "Residential segregation (Black/white)", "index (0-100)", 1, 1,
  "chr_school_funding_adequacy", "School funding adequacy (gap vs. cost to reach avg. outcomes)", "$/pupil", 1, 0,
  "chr_school_segregation", "School segregation (economic)", "index", 1, 2,
  "chr_severe_housing_cost_burden", "Severe housing cost burden (>50% income on housing)", "%", 100, 1,
  "chr_severe_housing_problems", "Severe housing problems", "%", 100, 1,
  "chr_single_parent_households", "Single-parent households", "%", 100, 1,
  "chr_teen_births", "Teen births", "per 1,000 females 15-19", 1, 1,
  "chr_uninsured_children", "Uninsured children", "%", 100, 1,
  "chr_violent_crime", "Violent crime", "per 100,000", 1, 0
)
chr_label <- setNames(chr_measure_info$label, chr_measure_info$measure)
chr_unit  <- setNames(chr_measure_info$unit, chr_measure_info$measure)
chr_scale <- setNames(chr_measure_info$scale, chr_measure_info$measure)
chr_dec   <- setNames(chr_measure_info$decimals, chr_measure_info$measure)

# Build a renderChoropleth "measure" entry for one CHR measure at state or
# county level, applying the scale/unit/decimals above.
chr_choropleth_measure <- function(measure_id, level = c("state", "county"), tags = NULL) {
  level <- match.arg(level)
  src <- if (level == "state") chr_state else chr_county
  d <- src %>%
    filter(measure == measure_id) %>%
    mutate(loc = if (level == "state") fips2abbr[fips] else fips, t = year, value = value * chr_scale[[measure_id]]) %>%
    select(loc, t, value)
  build_measure_entry(d, id = measure_id, label = chr_label[[measure_id]], level = level,
                       unit = chr_unit[[measure_id]], decimals = chr_dec[[measure_id]], tags = tags)
}

cat("Dashboard data loaded:", nrow(chr_county), "chr_county rows,", nrow(yrbss), "yrbss rows\n")
