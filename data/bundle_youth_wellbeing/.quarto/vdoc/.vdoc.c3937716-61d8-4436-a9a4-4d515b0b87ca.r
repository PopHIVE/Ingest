#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| include: false
source("_dashboard_utils.R")
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| include: false
# Referenced below via <script src>, not injected as R text output --
# routing a multi-MB minified bundle through cat()/knitr's text pipeline on
# Windows corrupts it (likely CRLF translation on an embedded literal
# newline inside the bundle). A <script src> tag lets Quarto's own
# embed-resources machinery inline the file byte-safely instead.
#
# plotly-geo (cartesian + choropleth/geo traces only, no 3D/gl/mapbox) is
# ~1.2MB vs. ~4.5MB for the full bundle -- this dashboard never uses 3D,
# WebGL, or mapbox traces, so the full bundle was pure dead weight.
invisible(file.exists("plotly-geo.min.js"))
#
#
#
#| include: false
# Same rationale as above: write a byte-exact .js companion file and load
# it via <script src> rather than cat()-ing the text into the page.
geo_con <- gzfile("us_counties_fips.geojson.gz", "rb")
geo_raw <- readBin(geo_con, "raw", n = 50 * 1024 * 1024)
close(geo_con)
out_con <- file("counties_geojson.js", "wb")
writeBin(charToRaw("window.PH_COUNTIES_GEOJSON = "), out_con)
writeBin(geo_raw, out_con)
writeBin(charToRaw(";\n"), out_con)
close(out_con)
#
#
#
#| results: asis
# Emitted ONCE for the whole page. Every choropleth measure and every
# scatter-tool measure aligns its values to these two id/label arrays
# instead of carrying its own -- see the comment on geo_state_canon /
# geo_county_canon in _dashboard_utils.R.
emit_json("PH_GEO_STATE", list(locs = I(geo_state_canon$loc), labels = I(geo_state_canon$loc_label)))
emit_json("PH_GEO_COUNTY", list(locs = I(geo_county_canon$loc), labels = I(geo_county_canon$loc_label)))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
neiss_prep <- function(df, cause_col) {
  df %>%
    # rank <= 5: the grid only ever shows the top 5, so ranks 6-10 would be
    # dead weight in the embedded JSON.
    filter(sex %in% c("All", "Female", "Male"), age %in% neiss_age_order, rank <= 5) %>%
    transmute(
      age = age, sex = sex, year = year, rank = rank,
      label = .data[[cause_col]], value = value,
      pct_of_all = pct_of_all, n_sampled = n_sampled,
      unstable = unstable_flag == 1
    )
}

prod_rows <- neiss_prep(neiss_prod, "product")
diag_rows <- neiss_prep(neiss_diag, "diagnosis")

age_present <- neiss_age_order[neiss_age_order %in% union(prod_rows$age, diag_rows$age)]
age_opts <- lapply(age_present, function(a) list(value = a, label = neiss_age_label(a)))
year_opts <- sort(unique(c(prod_rows$year, diag_rows$year)), decreasing = TRUE)

ov_injury_cfg <- list(
  topN = 5, valueLabel = "ED visits", height = 380,
  filters = list(
    list(key = "age", label = "Age", options = age_opts, default = "5-9 years"),
    list(key = "sex", label = "Sex", options = c("All", "Female", "Male"), default = "All"),
    list(key = "year", label = "Year", options = year_opts, default = year_opts[1])
  ),
  panels = list(
    list(id = "product", title = "Top 5 consumer products involved", rows = prod_rows),
    list(id = "diagnosis", title = "Top 5 diagnoses", rows = diag_rows)
  )
)
emit_json("OV_INJURY_GRID", ov_injury_cfg)
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
emit_json("OV_MORTALITY_MAP", list(
  measureLabel = "Measure", height = 520, defaultMeasure = "chr_infant_mortality",
  measures = list(
    chr_choropleth_measure("chr_infant_mortality", "county"),
    chr_choropleth_measure("chr_child_mortality", "county")
  )
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| include: false
subst_measures <- c("pct_ever_cigarette", "pct_ever_vape", "pct_current_smokeless_tobacco", "pct_current_alcohol",
  "pct_binge_drinking", "pct_ever_marijuana", "pct_early_marijuana", "pct_current_marijuana",
  "pct_ever_rx_opioid_misuse", "pct_current_rx_opioid_misuse", "pct_ever_cocaine", "pct_ever_inhalants",
  "pct_ever_heroin", "pct_ever_methamphetamines", "pct_ever_ecstasy", "pct_ever_hallucinogens",
  "pct_ever_inject_drug", "pct_ever_illicit_drug")

subst_labels <- c(
  pct_ever_cigarette = "Ever smoked a cigarette", pct_ever_vape = "Ever used an e-cigarette/vape",
  pct_current_smokeless_tobacco = "Currently uses smokeless tobacco", pct_current_alcohol = "Currently drinks alcohol",
  pct_binge_drinking = "Binge drinking (past 30 days)", pct_ever_marijuana = "Ever used marijuana",
  pct_early_marijuana = "First used marijuana before age 13", pct_current_marijuana = "Currently uses marijuana",
  pct_ever_rx_opioid_misuse = "Ever misused prescription opioids", pct_current_rx_opioid_misuse = "Currently misuses prescription opioids",
  pct_ever_cocaine = "Ever used cocaine", pct_ever_inhalants = "Ever used inhalants",
  pct_ever_heroin = "Ever used heroin", pct_ever_methamphetamines = "Ever used methamphetamines",
  pct_ever_ecstasy = "Ever used ecstasy", pct_ever_hallucinogens = "Ever used hallucinogens",
  pct_ever_inject_drug = "Ever injected an illegal drug", pct_ever_illicit_drug = "Ever used any illicit drug"
)
#
#
#
#
#
#| results: asis
subst_map_data <- yrbss %>%
  filter(measure %in% subst_measures, age == "Overall", sex == "All", race == "All", ethnicity == "All",
         not_asked == 0, geography != "United States") %>%
  mutate(loc = fips2abbr[fips], t = year,
         note = if_else(suppressed == 1, " (suppressed - estimate imputed)", ""))

subst_map_measures <- lapply(subst_measures, function(mid) {
  d <- subst_map_data %>% filter(measure == mid) %>% select(loc, t, value, note)
  build_measure_entry(d, id = mid, label = subst_labels[[mid]], level = "state", unit = "%", decimals = 1,
                       sub = "Overall estimate, grades 9-12 (CDC YRBSS)")
})

emit_json("SA_MAP", list(measureLabel = "Substance", height = 480,
                          defaultMeasure = "pct_ever_cigarette", measures = subst_map_measures))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
subst_ts_data <- yrbss %>%
  filter(measure %in% subst_measures, race == "All", ethnicity == "All", not_asked == 0, age %in% yrbss_age_order) %>%
  mutate(note = if_else(suppressed == 1, " (suppressed - imputed)", ""))

subst_ts_lines <- build_lines(subst_ts_data, dim_cols = c("geography", "age", "sex", "measure"),
                               series_col = "age", x_col = "year", y_col = "value", note_col = "note")
state_opts_sa <- c("United States", sort(setdiff(unique(subst_ts_data$geography), "United States")))
age_opts_sa <- yrbss_age_order[yrbss_age_order %in% unique(subst_ts_data$age)]

emit_json("SA_TS", list(
  title = "Substance use over time, grades 9-12", height = 480,
  xTitle = "Year", yTitle = "Percent of students (%)", yUnit = "%", decimals = 1,
  filters = list(
    list(key = "geography", label = "State", options = state_opts_sa, default = "United States"),
    list(key = "measure", label = "Substance", options = lapply(subst_measures, function(m) list(value = m, label = subst_labels[[m]])), default = "pct_ever_cigarette")
  ),
  compareBy = yrbss_compare_by(age_opts_sa),
  lines = subst_ts_lines
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
# Annual only for both sources -- monthly x-axis labels were too dense to
# read, and going annual-only means WISQARS (which has no monthly data) and
# Epic Cosmos can share one axis without a resolution toggle. The two
# sources still use different age bands (WISQARS: 0-14/15-24; Epic:
# 0-14/15-25), so those are harmonized into one merged age filter.
age_map_wisqars <- c("0-14 years" = "0-14", "15-24 years" = "15-24")
age_map_epic <- c("0-14 years" = "0-14", "15-25 years" = "15-24")

od_wisqars <- wisqars %>%
  filter(measure == "wisqars_rate_drug_poisoning", sex == "All", race == "All", ethnicity == "All") %>%
  mutate(age_grp = age_map_wisqars[age]) %>%
  filter(!is.na(age_grp)) %>%
  transmute(geography, age_grp, source = "wisqars_drug_poisoning", series = "wisqars_drug_poisoning", period = as.character(year), value)

od_epic <- epic_injury_year %>%
  filter(measure == "epic_rate_ed_opioid") %>%
  mutate(age_grp = age_map_epic[age]) %>%
  filter(!is.na(age_grp)) %>%
  transmute(geography, age_grp, source = "epic_opioid_ed", series = "epic_opioid_ed", period = as.character(year), value)

od_all <- bind_rows(od_wisqars, od_epic)
od_lines <- build_lines(od_all, dim_cols = c("geography", "age_grp", "source"),
                         series_col = "series", x_col = "period", y_col = "value")
state_opts_od <- c("United States", sort(setdiff(unique(od_all$geography), "United States")))

emit_json("SA_OD", list(
  title = "Drug overdose / opioid ED visit trends", height = 460, toggle = FALSE,
  xTitle = "Year", yTitle = "Rate (per 100,000 population)", yUnit = " per 100k", decimals = 1,
  filters = list(
    list(key = "geography", label = "State", options = state_opts_od, default = "United States"),
    list(key = "age_grp", label = "Age", options = list(list(value = "0-14", label = "Ages 0-14"), list(value = "15-24", label = "Ages 15-24")), default = "15-24"),
    list(key = "source", label = "Data source", options = list(
      list(value = "wisqars_drug_poisoning", label = "CDC/WISQARS drug poisoning deaths"),
      list(value = "epic_opioid_ed", label = "Epic Cosmos opioid-related ED visits")
    ), default = "wisqars_drug_poisoning")
  ),
  seriesOrder = c("wisqars_drug_poisoning", "epic_opioid_ed"),
  seriesMeta = list(
    wisqars_drug_poisoning = list(label = "CDC/WISQARS drug poisoning deaths"),
    epic_opioid_ed = list(label = "Epic Cosmos opioid-related ED visits")
  ),
  lines = od_lines
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
# Medicaid/CHIP payers are pooled into one "Medicaid" label (averaged on the
# rare state-year where both are reported) -- same treatment as every other
# Medicaid Child Core Set chart in this dashboard -- but the 7-day and
# 30-day follow-up windows are kept separate as a measure toggle rather than
# pooled, since they answer different questions.
fua_labels <- c(medicaid_fua_ch_7d_rate = "Follow-up within 7 days", medicaid_fua_ch_30d_rate = "Follow-up within 30 days")
fua_data <- medicaid %>% filter(measure %in% names(fua_labels)) %>%
  mutate(loc = fips2abbr[fips], t = year) %>%
  group_by(measure, loc, t) %>% summarise(value = mean(value, na.rm = TRUE), .groups = "drop")
fua_entries <- lapply(names(fua_labels), function(mid) {
  d <- fua_data %>% filter(measure == mid) %>% select(loc, t, value)
  build_measure_entry(d, id = mid, label = fua_labels[[mid]], level = "state", unit = "%", decimals = 1)
})
emit_json("SA_FUA_MAP", list(
  measureLabel = "Follow-up window", height = 460,
  defaultMeasure = "medicaid_fua_ch_7d_rate", measures = fua_entries
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
mv_age_opts <- c("0-14 years", "15-24 years")
mv_sex_opts <- c("All", "Female", "Male")

mv_wisqars_labels <- c(
  wisqars_rate_pedestrian_mv_traffic = "Pedestrian deaths in MV crashes (rate per 100k)",
  wisqars_rate_motor_vehicle_traffic = "All motor-vehicle traffic deaths (rate per 100k)"
)

mv_base <- wisqars %>%
  filter(measure %in% names(mv_wisqars_labels), race == "All", ethnicity == "All", geography != "United States",
         age %in% mv_age_opts, sex %in% mv_sex_opts) %>%
  mutate(loc = fips2abbr[fips], t = year)

mv_entries <- list()
for (mid in names(mv_wisqars_labels)) for (ag in mv_age_opts) for (sx in mv_sex_opts) {
  d <- mv_base %>% filter(measure == mid, age == ag, sex == sx) %>% select(loc, t, value)
  if (nrow(d) == 0) next
  eid <- paste("wisqars", mid, ag, sx, sep = "|")
  mv_entries[[eid]] <- build_measure_entry(
    d, id = eid, label = paste0(mv_wisqars_labels[[mid]], " — ", ag, ", ", sx),
    level = "state", unit = if (grepl("_rate_", mid)) "per 100k" else "deaths",
    decimals = if (grepl("_rate_", mid)) 2 else 0, tags = list(source = "WISQARS", age = ag, sex = sx)
  )
}
for (ag in mv_age_opts) for (sx in mv_sex_opts) {
  d <- nhtsa_state %>% filter(age == ag, sex == sx, geography != "United States") %>%
    mutate(loc = fips2abbr[fips], t = year) %>% select(loc, t, value)
  if (nrow(d) == 0) next
  eid <- paste("nhtsa_state", ag, sx, sep = "|")
  mv_entries[[eid]] <- build_measure_entry(
    d, id = eid, label = paste0("NHTSA traffic fatalities, state (count) — ", ag, ", ", sx),
    level = "state", unit = "deaths", decimals = 0, tags = list(source = "NHTSA (state)", age = ag, sex = sx)
  )
}
for (ag in mv_age_opts) for (sx in mv_sex_opts) {
  d <- nhtsa_county %>% filter(age == ag, sex == sx) %>%
    mutate(loc = fips, t = year) %>% select(loc, t, value)
  if (nrow(d) == 0) next
  eid <- paste("nhtsa_county", ag, sx, sep = "|")
  mv_entries[[eid]] <- build_measure_entry(
    d, id = eid, label = paste0("NHTSA traffic fatalities, county (count) — ", ag, ", ", sx),
    level = "county", unit = "deaths", decimals = 0, tags = list(source = "NHTSA (county)", age = ag, sex = sx)
  )
}
mv_all <- unname(mv_entries)
mv_sources <- unique(vapply(mv_all, function(m) m$tags$source, character(1)))

emit_json("IV_MV_MAP", list(
  measureLabel = "Source / measure", height = 500,
  filters = list(
    list(key = "source", label = "Source", options = mv_sources, default = "NHTSA (state)"),
    list(key = "age", label = "Age", options = mv_age_opts, default = "15-24 years"),
    list(key = "sex", label = "Sex", options = mv_sex_opts, default = "All")
  ),
  defaultMeasure = "nhtsa_state|15-24 years|All",
  measures = mv_all
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
driving_measures <- c("pct_no_seatbelt", "pct_rode_drinking_driver", "pct_drove_drinking", "pct_text_while_driving")
driving_labels <- c(
  pct_no_seatbelt = "Rarely/never wears a seatbelt",
  pct_rode_drinking_driver = "Rode with a driver who had been drinking",
  pct_drove_drinking = "Drove after drinking alcohol",
  pct_text_while_driving = "Texted/emailed while driving"
)
driving_data <- yrbss %>%
  filter(measure %in% driving_measures, race == "All", ethnicity == "All", not_asked == 0, age %in% yrbss_age_order) %>%
  mutate(note = if_else(suppressed == 1, " (suppressed - imputed)", ""))
driving_lines <- build_lines(driving_data, dim_cols = c("geography", "age", "sex", "measure"),
                              series_col = "age", x_col = "year", y_col = "value", note_col = "note")
state_opts_drv <- c("United States", sort(setdiff(unique(driving_data$geography), "United States")))
age_opts_drv <- yrbss_age_order[yrbss_age_order %in% unique(driving_data$age)]

emit_json("IV_DRIVING", list(
  title = "Dangerous driving behaviors, by state and age", height = 460,
  xTitle = "Year", yTitle = "Percent of students (%)", yUnit = "%", decimals = 1,
  filters = list(
    list(key = "geography", label = "State", options = state_opts_drv, default = "United States"),
    list(key = "measure", label = "Behavior", options = lapply(driving_measures, function(m) list(value = m, label = driving_labels[[m]])), default = "pct_no_seatbelt")
  ),
  compareBy = yrbss_compare_by(age_opts_drv),
  lines = driving_lines
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
conc_yrbss <- yrbss %>%
  filter(measure == "pct_sports_concussion", race == "All", ethnicity == "All", not_asked == 0, age %in% yrbss_age_order) %>%
  mutate(source = "yrbss", note = if_else(suppressed == 1, " (suppressed - imputed)", "")) %>%
  transmute(source, geography, age, sex, period = as.character(year), value, note)

# Epic Cosmos's sex column uses "Overall" for the total where YRBSS (and
# yrbss_compare_by's "age" group, which fixes sex at "All") uses "All" --
# recode so both sources share one sex vocabulary for the shared compareBy
# fixed-value matching.
#
# Suppressed/imputed Epic rows are dropped entirely rather than plotted with
# a note (unlike every other chart's suppressed-but-shown convention) --
# most Epic state/age/sex strata are small-count and imputed, and imputed
# values would otherwise dominate a boys-vs-girls comparison at a single
# age band like 10-13 years.
conc_epic <- epic_concussion %>%
  filter(epic_n_concussion_suppressed_flag == 0) %>%
  transmute(
    source = "epic", geography = geography_name,
    age, sex = if_else(sex == "Overall", "All", sex),
    period = as.character(time), value = epic_pct_concussion
  )

conc_all <- bind_rows(conc_yrbss, conc_epic)
conc_lines <- build_lines(conc_all, dim_cols = c("source", "geography", "age", "sex"),
                           series_col = "age", x_col = "period", y_col = "value", note_col = "note")

state_opts_conc <- c("United States", sort(setdiff(unique(conc_all$geography), "United States")))
age_opts_conc_yrbss <- yrbss_age_order[yrbss_age_order %in% unique(conc_yrbss$age)]
age_opts_conc_epic <- c("<1 Years", "1-4 Years", "5-9 Years", "10-13 Years", "14-17 Years",
                         "18-29 Years", "30-44 Years", "45-64 Years", "65+ Years", "Overall")
age_opts_conc_epic <- age_opts_conc_epic[age_opts_conc_epic %in% unique(conc_epic$age)]

conc_sex_group_yrbss <- list(fixed = list(age = "Overall"), seriesOrder = c("All", "Female", "Male"),
                              seriesMeta = list(All = list(label = "Overall"), Female = list(label = "Female"), Male = list(label = "Male")))
# Unlike YRBSS (which only ever reports sex OR age, never both together --
# see yrbss_compare_by's note), Epic Cosmos has a genuine age x sex cross-tab,
# so its sex-comparison line can hold any single age band fixed instead of
# always "Overall". Which age is held fixed comes from the "concAge" filter
# below (default "10-13 Years") rather than a hardcoded value.
conc_sex_group_epic <- list(fixed = list(age = list(fromFilter = "concAge")), seriesOrder = c("All", "Female", "Male"),
                             seriesMeta = list(All = list(label = "Overall"), Female = list(label = "Female"), Male = list(label = "Male")))

emit_json("IV_CONCUSSION", list(
  title = "Sports/activity-related concussion", height = 460,
  xTitle = "Year", yTitle = "Percent of students (%)", yUnit = "%", decimals = 1,
  unitFilterKey = "source",
  xTitleMap = list(yrbss = "Year", epic = "Month"),
  yTitleMap = list(yrbss = "Percent of students (%)", epic = "Percent of ED encounters (%)"),
  yUnitMap = list(yrbss = "%", epic = "%"),
  decimalsMap = list(yrbss = 1, epic = 3),
  filters = list(
    list(key = "source", label = "Data source", options = list(
      list(value = "epic", label = "Epic Cosmos (ED visits, all ages)"),
      list(value = "yrbss", label = "CDC YRBSS (self-report, high school)")
    ), default = "epic"),
    list(key = "geography", label = "State", options = state_opts_conc, default = "United States"),
    # Only shown while "Compare by Sex" is active on the Epic source -- picks
    # which age band the boys-vs-girls comparison holds fixed (see
    # conc_sex_group_epic's fromFilter). Defaults to 10-13 years.
    list(key = "concAge", label = "Age (boys vs. girls)", options = age_opts_conc_epic, default = "10-13 Years",
         showWhen = list(cmpBy = "sex", otherFilterEq = list(source = "epic")))
  ),
  compareBy = list(
    label = "Compare by", default = "age", sourceKey = "source",
    groups = list(
      age = list(label = "Age", bySource = list(
        yrbss = list(fixed = list(sex = "All"), seriesOrder = age_opts_conc_yrbss,
                     seriesMeta = setNames(lapply(age_opts_conc_yrbss, function(a) list(label = a)), age_opts_conc_yrbss)),
        epic  = list(fixed = list(sex = "All"), seriesOrder = age_opts_conc_epic,
                     seriesMeta = setNames(lapply(age_opts_conc_epic, function(a) list(label = a)), age_opts_conc_epic))
      )),
      sex = list(label = "Sex", bySource = list(yrbss = conc_sex_group_yrbss, epic = conc_sex_group_epic))
    )
  ),
  lines = conc_lines
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
fa_age_opts <- c("0-14 years", "15-24 years", "15-25 years")
fa_age_labels <- c(
  "0-14 years" = "0-14 years",
  "15-24 years" = "15-24 years (WISQARS)",
  "15-25 years" = "15-25 years (Epic Cosmos)"
)

fa_wisqars_labels <- c(
  wisqars_rate_firearm_accident = "Unintentional firearm deaths (rate per 100k)",
  wisqars_rate_firearm_homicide = "Firearm homicide deaths (rate per 100k)",
  wisqars_rate_firearm_suicide = "Firearm suicide deaths (rate per 100k)",
  wisqars_rate_firearm_intentional = "All intentional firearm deaths (rate per 100k)"
)
fa_base <- wisqars %>%
  filter(measure %in% names(fa_wisqars_labels), race == "All", age %in% fa_age_opts,
         sex == "All", ethnicity == "All") %>%
  mutate(note = "")
fa_wisqars_lines <- build_lines(fa_base, dim_cols = c("geography", "age"),
                                 series_col = "measure", x_col = "year", y_col = "value", note_col = "note")

fa_epic <- epic_injury_year %>%
  filter(measure == "epic_rate_ed_firearm", age %in% fa_age_opts) %>%
  mutate(note = "")
fa_epic_lines <- build_lines(fa_epic, dim_cols = c("geography", "age"),
                              series_col = "measure", x_col = "year", y_col = "value", note_col = "note")

fa_lines <- c(fa_wisqars_lines, fa_epic_lines)
fa_series_order <- c(names(fa_wisqars_labels), "epic_rate_ed_firearm")
fa_series_meta <- c(fa_wisqars_labels,
  epic_rate_ed_firearm = "Epic Cosmos firearm-related ED visits (rate per 100k)")
state_opts_fa <- c("United States", sort(setdiff(unique(fa_base$geography), "United States")))

emit_json("IV_FIREARM_INJURY", list(
  title = "Firearm deaths and ED visits", height = 460, toggle = TRUE,
  xTitle = "Year", yTitle = "Rate per 100,000 population", decimals = 2,
  filters = list(
    list(key = "geography", label = "State", options = state_opts_fa, default = "United States"),
    list(key = "age", label = "Age", options = lapply(fa_age_opts, function(a) list(value = a, label = fa_age_labels[[a]])), default = "15-24 years")
  ),
  seriesOrder = fa_series_order,
  seriesMeta = setNames(lapply(fa_series_order, function(m) list(label = fa_series_meta[[m]])), fa_series_order),
  defaultOn = c("wisqars_rate_firearm_accident"),
  lines = fa_lines
))
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
fb_measures <- c("pct_carried_weapon_school", "pct_carried_gun", "pct_threatened_weapon_school")
fb_labels <- c(
  pct_carried_weapon_school = "Carried a weapon on school property",
  pct_carried_gun = "Carried a gun (not for hunting/sport)",
  pct_threatened_weapon_school = "Threatened/injured with a weapon at school"
)
fb_data <- yrbss %>%
  filter(measure %in% fb_measures, race == "All", ethnicity == "All", not_asked == 0, age %in% yrbss_age_order) %>%
  mutate(note = if_else(suppressed == 1, " (suppressed - imputed)", ""))
fb_lines <- build_lines(fb_data, dim_cols = c("geography", "age", "sex", "measure"), series_col = "age",
                         x_col = "year", y_col = "value", note_col = "note")
state_opts_fb <- c("United States", sort(setdiff(unique(fb_data$geography), "United States")))
age_opts_fb <- yrbss_age_order[yrbss_age_order %in% unique(fb_data$age)]

emit_json("IV_FIREARM_BEHAVIOR", list(
  title = "Weapon-carrying and threats at school", height = 420,
  xTitle = "Year", yTitle = "Percent of students (%)", yUnit = "%", decimals = 1,
  filters = list(
    list(key = "geography", label = "State", options = state_opts_fb, default = "United States"),
    list(key = "measure", label = "Measure", options = lapply(fb_measures, function(m) list(value = m, label = fb_labels[[m]])), default = "pct_carried_weapon_school")
  ),
  compareBy = yrbss_compare_by(age_opts_fb),
  lines = fb_lines
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| include: false
mh_filter_lists <- function(df) {
  list(
    state = c("United States", sort(setdiff(unique(df$geography), "United States"))),
    age = yrbss_age_order[yrbss_age_order %in% unique(df$age)],
    race = c("All", sort(setdiff(unique(df$race), "All")))
  )
}
#
#
#
#
#
#| results: asis
# Every YRBSS time series uses the same "Compare by" pattern: the legend
# toggles between one line per age (sex fixed at "All") or one line per sex
# (age fixed at "Overall"). Race and Hispanic ethnicity are dropped entirely
# (always filtered to "All") to keep this comparison focused.
mh_suicide_measures <- c("pct_considered_suicide", "pct_planned_suicide", "pct_attempted_suicide", "pct_injurious_suicide_attempt")
mh_suicide_labels <- c(
  pct_considered_suicide = "Seriously considered suicide",
  pct_planned_suicide = "Made a suicide plan",
  pct_attempted_suicide = "Attempted suicide",
  pct_injurious_suicide_attempt = "Suicide attempt requiring medical treatment"
)
mh_suicide_data <- yrbss %>%
  filter(measure %in% mh_suicide_measures, race == "All", ethnicity == "All", not_asked == 0, age %in% yrbss_age_order) %>%
  mutate(note = if_else(suppressed == 1, " (suppressed - imputed)", ""))
mh_suicide_lines <- build_lines(mh_suicide_data, dim_cols = c("geography", "age", "sex", "measure"),
                                 series_col = "age", x_col = "year", y_col = "value", note_col = "note")
state_opts_suicide <- c("United States", sort(setdiff(unique(mh_suicide_data$geography), "United States")))
age_opts_suicide <- yrbss_age_order[yrbss_age_order %in% unique(mh_suicide_data$age)]

emit_json("MH_SUICIDE", list(
  title = "Suicidal ideation and behavior, past 12 months", height = 480,
  xTitle = "Year", yTitle = "Percent of students (%)", yUnit = "%", decimals = 1,
  filters = list(
    list(key = "geography", label = "State", options = state_opts_suicide, default = "United States"),
    list(key = "measure", label = "Measure", options = lapply(mh_suicide_measures, function(m) list(value = m, label = mh_suicide_labels[[m]])), default = "pct_considered_suicide")
  ),
  compareBy = yrbss_compare_by(age_opts_suicide),
  lines = mh_suicide_lines
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
mh_ed_age_order <- c("<5 Years", "5-9 Years", "10-14 Years", "15-19 Years", "20-24 Years",
                      "25-44 Years", "45-64 Years", "65-84 Years", "85+ Years", "Overall")
mh_ed_diagnoses <- c("Suicidal behavior", "Mood")

# ---- Map: median ED length of stay vs. share of ED visits for the selected
# diagnosis, by state, age (dropdown), diagnosis (dropdown), and month
# (slider) ----
mh_ed_map_base <- epic_mh %>%
  filter(geography != "00") %>%
  mutate(loc = fips2abbr[geography], t = time)

mh_ed_metrics <- list(
  median = list(col = "median_los", label = "Median ED length of stay (minutes)",
                unit = "min", decimals = 0,
                sub = "Epic Cosmos ED visits for the selected diagnosis"),
  pct = list(col = "pct_share", label = "Share of ED visits (%)",
             unit = "%", decimals = 2,
             sub = "Compositional share of this diagnosis's own visits across state/age -- not a rate or a visit count")
)

mh_ed_measures <- list()
for (dx in mh_ed_diagnoses) {
  for (ag in mh_ed_age_order) {
    for (mkey in names(mh_ed_metrics)) {
      m <- mh_ed_metrics[[mkey]]
      d <- mh_ed_map_base %>%
        filter(age == ag, diagnosis == dx) %>%
        transmute(loc, t, value = .data[[m$col]])
      mh_ed_measures[[length(mh_ed_measures) + 1]] <- build_measure_entry(
        d, id = paste(mkey, ag, dx, sep = "__"), label = m$label, level = "state",
        unit = m$unit, decimals = m$decimals, sub = m$sub, tags = list(age = ag, diagnosis = dx)
      )
    }
  }
}

emit_json("MH_ED_MAP", list(
  measureLabel = "Metric", height = 520, defaultMeasure = "median__Overall__Suicidal behavior",
  filters = list(
    list(key = "age", label = "Age", options = mh_ed_age_order, default = "Overall"),
    list(key = "diagnosis", label = "Diagnosis", options = mh_ed_diagnoses, default = "Suicidal behavior")
  ),
  measures = mh_ed_measures
))

# ---- Time series: median ED length of stay, state + diagnosis dropdowns,
# age legend toggle, optional Q1-Q3 error bars ----
mh_ed_ts_data <- epic_mh %>%
  filter(!is.na(median_los)) %>%
  transmute(geography = geography_name, age, diagnosis, period = as.character(time),
            median_los, q1_los, q3_los)

mh_ed_ts_lines <- build_lines(mh_ed_ts_data, dim_cols = c("geography", "diagnosis"), series_col = "age",
                               x_col = "period", y_col = "median_los",
                               lower_col = "q1_los", upper_col = "q3_los")
state_opts_mh_ed <- c("United States", sort(setdiff(unique(mh_ed_ts_data$geography), "United States")))
age_opts_mh_ed <- mh_ed_age_order[mh_ed_age_order %in% unique(mh_ed_ts_data$age)]

emit_json("MH_ED_TS", list(
  title = "Emergency department length of stay by diagnosis", height = 460, toggle = TRUE,
  xTitle = "Month", yTitle = "Median ED length of stay (minutes)", yUnit = " min", decimals = 0,
  filters = list(
    list(key = "geography", label = "State", options = state_opts_mh_ed, default = "United States"),
    list(key = "diagnosis", label = "Diagnosis", options = mh_ed_diagnoses, default = "Suicidal behavior")
  ),
  seriesOrder = age_opts_mh_ed,
  seriesMeta = setNames(lapply(age_opts_mh_ed, function(a) list(label = a)), age_opts_mh_ed),
  defaultOn = c("Overall"),
  errorBand = list(key = "q1q3", label = "Show interquartile range (Q1-Q3)", default = FALSE),
  lines = mh_ed_ts_lines
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
ctl_category_labels <- c(
  crisistrends_suicide                = "Suicide",
  crisistrends_self_harm              = "Self-Harm",
  crisistrends_eating_body_image      = "Eating / Body Image (eating disorders)",
  crisistrends_depression_sadness     = "Depression / Sadness",
  crisistrends_anxiety_stress         = "Anxiety / Stress",
  crisistrends_isolation_loneliness   = "Isolation / Loneliness",
  crisistrends_grief                  = "Grief",
  crisistrends_bullying               = "Bullying",
  crisistrends_abuse                  = "Abuse",
  crisistrends_substance_use          = "Substance Use",
  crisistrends_relationships          = "Relationships",
  crisistrends_gender_sexual_identity = "Gender / Sexual Identity"
)
ctl_age_order <- c("13 or younger", "14-17", "18-24", "25-34", "35-44", "45-54", "55-64", "65+", "Overall")

# Years outside the Census population vintage (2020-2025) are clamped to the
# nearest one available, same convention as the source dashboard
# (crisis-text-line/crisis_trends_dashboard.qmd).
ctl_pop_year_range <- range(ctl_population$year)
ctl_map_base <- ctl_annual %>%
  filter(geography != "00", age %in% ctl_age_order) %>%
  mutate(
    loc = fips2abbr[geography],
    census_year = pmin(pmax(year, ctl_pop_year_range[1]), ctl_pop_year_range[2])
  ) %>%
  left_join(
    ctl_population %>% select(geography, year, age_group, population),
    by = c("geography" = "geography", "census_year" = "year", "age" = "age_group")
  )

ctl_map_measures <- list()
for (ctl_cat in names(ctl_category_labels)) {
  for (ctl_ag in ctl_age_order) {
    d <- ctl_map_base %>%
      filter(age == ctl_ag) %>%
      transmute(loc, t = year, value = .data[[ctl_cat]] / population * 1e5)
    ctl_map_measures[[length(ctl_map_measures) + 1]] <- build_measure_entry(
      d, id = paste(ctl_cat, ctl_ag, sep = "__"), label = ctl_category_labels[[ctl_cat]],
      level = "state", unit = "per 100k", decimals = 1,
      sub = "Crisis Text Line conversations per 100,000 population, by year",
      tags = list(age = ctl_ag)
    )
  }
}

emit_json("CTL_MAP", list(
  measureLabel = "Topic", height = 520, defaultMeasure = "crisistrends_suicide__Overall",
  filters = list(list(key = "age", label = "Age group", options = ctl_age_order, default = "Overall")),
  measures = ctl_map_measures
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
# Top 5 topics kept per state/year/age before emitting -- same "only ship
# what's shown" optimization as the NEISS grid in the Overview tab.
ctl_bar_age_order <- setdiff(ctl_age_order, "Overall")
ctl_geo_name <- function(g) {
  case_when(g == "00" ~ "United States", g == "72" ~ "Puerto Rico", TRUE ~ unname(fips2name[g]))
}

ctl_bar_long <- ctl_annual %>%
  filter(age %in% ctl_bar_age_order) %>%
  mutate(geography_name = ctl_geo_name(geography)) %>%
  filter(!is.na(geography_name)) %>%
  select(geography_name, year, age, crisistrends_n_tagged, all_of(names(ctl_category_labels))) %>%
  pivot_longer(all_of(names(ctl_category_labels)), names_to = "tag", values_to = "count") %>%
  mutate(
    label = unname(ctl_category_labels[tag]),
    suppressed = is.na(count) | is.na(crisistrends_n_tagged),
    pct = if_else(suppressed | crisistrends_n_tagged == 0, NA_real_, 100 * count / crisistrends_n_tagged)
  ) %>%
  group_by(geography_name, year, age) %>%
  mutate(rank = rank(-replace_na(pct, -1), ties.method = "first")) %>%
  ungroup() %>%
  filter(rank <= 5) %>%
  transmute(state = geography_name, year, age, rank, label, value = count,
            pct_of_all = round(pct, 1), n_sampled = crisistrends_n_tagged, unstable = suppressed)

ctl_bar_states <- c("United States", sort(setdiff(unique(ctl_bar_long$state), "United States")))
ctl_bar_years  <- sort(unique(ctl_bar_long$year), decreasing = TRUE)

emit_json("CTL_TOPIC_AGE", list(
  topN = 5, valueLabel = "conversations", height = 320,
  filters = list(
    list(key = "state", label = "State", options = ctl_bar_states, default = "United States"),
    list(key = "year", label = "Year", options = ctl_bar_years, default = ctl_bar_years[1])
  ),
  panels = lapply(ctl_bar_age_order, function(ag) {
    list(id = gsub("[^A-Za-z0-9]", "_", ag), title = paste0("Age ", ag),
         rows = ctl_bar_long %>% filter(age == ag) %>% select(-age))
  })
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
mh_bully_measures <- c("pct_bullied_at_school", "pct_bullied_electronic")
mh_bully_labels <- c(pct_bullied_at_school = "Bullied at school", pct_bullied_electronic = "Bullied electronically")
mh_bully_data <- yrbss %>%
  filter(measure %in% mh_bully_measures, race == "All", ethnicity == "All", not_asked == 0, age %in% yrbss_age_order) %>%
  mutate(note = if_else(suppressed == 1, " (suppressed - imputed)", ""))
mh_bully_lines <- build_lines(mh_bully_data, dim_cols = c("geography", "age", "sex", "measure"),
                               series_col = "age", x_col = "year", y_col = "value", note_col = "note")
state_opts_bully <- c("United States", sort(setdiff(unique(mh_bully_data$geography), "United States")))
age_opts_bully <- yrbss_age_order[yrbss_age_order %in% unique(mh_bully_data$age)]

emit_json("MH_BULLYING", list(
  title = "Bullying, past 12 months", height = 440,
  xTitle = "Year", yTitle = "Percent of students (%)", yUnit = "%", decimals = 1,
  filters = list(
    list(key = "geography", label = "State", options = state_opts_bully, default = "United States"),
    list(key = "measure", label = "Measure", options = lapply(mh_bully_measures, function(m) list(value = m, label = mh_bully_labels[[m]])), default = "pct_bullied_at_school")
  ),
  compareBy = yrbss_compare_by(age_opts_bully),
  lines = mh_bully_lines
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
mh_other_measures <- c("pct_social_media_daily", "pct_poor_mental_health", "pct_insufficient_sleep", "pct_not_close_at_school")
mh_other_labels <- c(
  pct_social_media_daily = "Uses social media several times a day",
  pct_poor_mental_health = "Frequent poor mental health (past 30 days)",
  pct_insufficient_sleep = "Insufficient sleep on school nights",
  pct_not_close_at_school = "Does not feel close to people at school"
)
mh_other_data <- yrbss %>%
  filter(measure %in% mh_other_measures, race == "All", ethnicity == "All", not_asked == 0, age %in% yrbss_age_order) %>%
  mutate(note = if_else(suppressed == 1, " (suppressed - imputed)", ""))
mh_other_lines <- build_lines(mh_other_data, dim_cols = c("geography", "age", "sex", "measure"),
                               series_col = "age", x_col = "year", y_col = "value", note_col = "note")
state_opts_other <- c("United States", sort(setdiff(unique(mh_other_data$geography), "United States")))
age_opts_other <- yrbss_age_order[yrbss_age_order %in% unique(mh_other_data$age)]

emit_json("MH_OTHER", list(
  title = "Other mental health and wellbeing indicators", height = 460,
  xTitle = "Year", yTitle = "Percent of students (%)", yUnit = "%", decimals = 1,
  filters = list(
    list(key = "geography", label = "State", options = state_opts_other, default = "United States"),
    list(key = "measure", label = "Measure", options = lapply(mh_other_measures, function(m) list(value = m, label = mh_other_labels[[m]])), default = "pct_insufficient_sleep")
  ),
  compareBy = yrbss_compare_by(age_opts_other),
  lines = mh_other_lines
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
emit_json("MH_DISCONNECTED_MAP", list(height = 520, measures = list(chr_choropleth_measure("chr_disconnected_youth", "county"))))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| include: false
epic_chronic_measure <- function(measure_id, label, level = c("state", "county")) {
  level <- match.arg(level)
  src <- if (level == "state") epic_chronic_state else epic_chronic_county
  cov <- src %>% filter(measure == "n_patients_chronic") %>%
    transmute(loc = if (level == "state") fips2abbr[fips] else fips, t = year, n_patients = value)
  d <- src %>% filter(measure == measure_id) %>%
    mutate(loc = if (level == "state") fips2abbr[fips] else fips, t = year) %>%
    left_join(cov, by = c("loc", "t")) %>%
    # Epic Cosmos reports a denominator of exactly 5 as a small-cell
    # suppression placeholder (the true count, 1-9, is masked). Treat those
    # cells as missing rather than plotting a rate computed on a masked
    # denominator.
    mutate(
      suppressed = !is.na(n_patients) & round(n_patients) == 5,
      value = if_else(suppressed, NA_real_, value),
      note = if_else(suppressed, " (suppressed: denominator masked at n=5)", paste0(" (n=", format(round(n_patients), big.mark = ","), ")"))
    ) %>%
    select(loc, t, value, note)
  build_measure_entry(d, id = measure_id, label = label, level = level, unit = "%", decimals = 2)
}
#
#
#
#
#
#| results: asis
emit_json("CD_OBESITY_MAP", list(measureLabel = "Measure", height = 500, defaultMeasure = "obesity_bmi", measures = list(
  epic_chronic_measure("obesity_bmi", "Obesity (BMI ≥95th percentile, ages 0-17)", "county"),
  epic_chronic_measure("obesity_dx_ccw", "Obesity diagnosis (CCW chronic condition flag, ages 0-17)", "county")
)))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
emit_json("CD_DIABETES_MAP", list(measureLabel = "Measure", height = 500, defaultMeasure = "diabetes_dx_ccw", measures = list(
  epic_chronic_measure("diabetes_a1c_6_5", "Diabetes (A1c ≥6.5%, ages 0-17)", "county"),
  epic_chronic_measure("diabetes_dx_ccw", "Diabetes diagnosis (CCW chronic condition flag, ages 0-17)", "county")
)))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
asthma_labels <- c(medicaid_mma_ch_rate = "Medication management for asthma (MMA)", medicaid_amr_ch_rate = "Asthma medication ratio (AMR)")
asthma_map_data <- medicaid %>% filter(measure %in% names(asthma_labels), payer == "Medicaid") %>%
  mutate(loc = fips2abbr[fips], t = year)
asthma_map_entries <- lapply(names(asthma_labels), function(mid) {
  d <- asthma_map_data %>% filter(measure == mid) %>% select(loc, t, value)
  build_measure_entry(d, id = mid, label = asthma_labels[[mid]], level = "state", unit = "%", decimals = 1)
})
emit_json("CD_ASTHMA_MAP", list(
  measureLabel = "Measure", height = 480,
  defaultMeasure = "medicaid_mma_ch_rate", measures = asthma_map_entries
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
lead_data <- medicaid %>% filter(measure == "medicaid_lsc_ch_rate") %>%
  mutate(loc = fips2abbr[fips], t = year) %>%
  # CHIP has very few reporting state-years (3, vs 44 for Medicaid) for this
  # measure; pool both payers under one "Medicaid" label rather than a payer
  # toggle. Average on the rare state-year where both happen to be reported.
  group_by(loc, t) %>%
  summarise(value = mean(value, na.rm = TRUE), .groups = "drop")
lead_entries <- list(build_measure_entry(lead_data, id = "medicaid_lsc_ch_rate", label = "Lead screening in children (LSC) — Medicaid",
  level = "state", unit = "%", decimals = 1))
emit_json("CD_LEAD_MAP", list(height = 460, measures = lead_entries))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
heat_year <- epic_injury_year %>% filter(measure %in% c("epic_rate_ed_heat", "epic_n_ed_heat")) %>%
  mutate(unit = if_else(grepl("^epic_rate", measure), "Rate", "Count"), resolution = "Annual", period = as.character(year)) %>%
  transmute(geography, unit, resolution, series = age, period, value)
heat_month <- epic_injury_month %>% filter(measure %in% c("epic_rate_ed_heat", "epic_n_ed_heat")) %>%
  mutate(unit = if_else(grepl("^epic_rate", measure), "Rate", "Count"), resolution = "Monthly", period = format(time, "%Y-%m")) %>%
  transmute(geography, unit, resolution, series = age, period, value)
heat_all <- bind_rows(heat_year, heat_month)
heat_lines <- build_lines(heat_all, dim_cols = c("geography", "unit", "resolution"), series_col = "series", x_col = "period", y_col = "value")
state_opts_heat <- c("United States", sort(setdiff(unique(heat_all$geography), "United States")))
heat_age_order <- sort(unique(heat_all$series))

emit_json("CD_HEAT_TS", list(
  title = "Heat-related ED visits, by age", height = 440, toggle = FALSE, xTitle = "Year",
  unitFilterKey = "unit", yTitleMap = list(Rate = "Rate (per 100,000 population)", Count = "Number of ED visits"),
  yUnitMap = list(Rate = " per 100k", Count = ""), decimals = 1,
  filters = list(
    list(key = "geography", label = "State", options = state_opts_heat, default = "United States"),
    list(key = "unit", label = "Metric", options = c("Rate", "Count"), default = "Rate"),
    list(key = "resolution", label = "Time resolution", options = c("Annual", "Monthly"), default = "Annual")
  ),
  seriesOrder = heat_age_order,
  seriesMeta = setNames(lapply(heat_age_order, function(a) list(label = a)), heat_age_order),
  lines = heat_lines
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
noaa_latest_date <- noaa_county %>% filter(forecast_day == 0) %>% summarise(m = max(time)) %>% pull(m)
noaa_snapshot_data <- noaa_county %>% filter(forecast_day == 0, time == noaa_latest_date) %>%
  mutate(loc = fips, t = year,
         note = if_else(!is.na(low_coverage_flag) & low_coverage_flag == 1, " (low forecast coverage for this county)", "")) %>%
  select(loc, t, value, note)
noaa_entry <- build_measure_entry(noaa_snapshot_data, id = "heat_risk", label = "NOAA/NWS HeatRisk index (most recent day available)",
                                   level = "county", unit = "index (0-4)", decimals = 0)

env_measures <- list(
  chr_choropleth_measure("chr_air_pollution_particulate_matter", "county"),
  chr_choropleth_measure("chr_air_pollution_ozone_days", "county"),
  chr_choropleth_measure("chr_air_pollution_particulate_matter_days", "county"),
  chr_choropleth_measure("chr_adverse_climate_events", "county"),
  noaa_entry
)
emit_json("CD_ENV_MAP", list(measureLabel = "Environmental measure", height = 520,
                              defaultMeasure = "chr_air_pollution_particulate_matter", measures = env_measures))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
emit_json("PH_TEEN_BIRTHS_MAP", list(height = 520, measures = list(chr_choropleth_measure("chr_teen_births", "county"))))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
contra_labels <- c(medicaid_fpc_ch_rate = "Family planning services (FPC)", medicaid_ppc_ch_rate = "Postpartum care (PPC)")
# Pool Medicaid + CHIP under one "Medicaid" label (as with lead testing):
# average the rare state-year where both payers are reported.
contra_map_data <- medicaid %>% filter(measure %in% names(contra_labels)) %>%
  mutate(loc = fips2abbr[fips], t = year) %>%
  group_by(measure, loc, t) %>% summarise(value = mean(value, na.rm = TRUE), .groups = "drop")
contra_map_entries <- lapply(names(contra_labels), function(mid) {
  d <- contra_map_data %>% filter(measure == mid) %>% select(loc, t, value)
  build_measure_entry(d, id = mid, label = contra_labels[[mid]], level = "state", unit = "%", decimals = 1)
})
emit_json("PH_CONTRA_MAP", list(
  measureLabel = "Measure", height = 480,
  defaultMeasure = "medicaid_fpc_ch_rate", measures = contra_map_entries
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
prev_measures <- c("medicaid_ima_ch_rate", "medicaid_w34_ch_rate", "medicaid_awc_ch_rate", "medicaid_w15_ch_rate",
                    "medicaid_w30_ch_rate", "medicaid_oev_ch_rate", "medicaid_cap_ch_rate", "medicaid_dev_ch_rate")
prev_labels <- c(
  medicaid_ima_ch_rate = "Childhood immunization status (IMA)",
  medicaid_w34_ch_rate = "Well-child visits, ages 3-6 (W34)",
  medicaid_awc_ch_rate = "Adolescent well-care visits (AWC)",
  medicaid_w15_ch_rate = "Well-child visits, first 15 months (W15)",
  medicaid_w30_ch_rate = "Well-child visits, 15-30 months (W30)",
  medicaid_oev_ch_rate = "Oral evaluation, dental services (OEV)",
  medicaid_cap_ch_rate = "Children's access to primary care (CAP)",
  medicaid_dev_ch_rate = "Developmental screening in first 3 years (DEV)"
)
prev_data <- medicaid %>% filter(measure %in% prev_measures) %>%
  group_by(measure, geography, year) %>% summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
  transmute(geography, series = measure, year, value)
prev_lines <- build_lines(prev_data, dim_cols = "geography", series_col = "series", x_col = "year", y_col = "value")
state_opts_prev <- sort(unique(prev_data$geography))
emit_json("PH_PREVENTIVE_TS", list(
  title = "Preventive care over time", height = 480, toggle = TRUE,
  xTitle = "Year", yTitle = "Percent (%)", yUnit = "%", decimals = 1,
  filters = list(
    list(key = "geography", label = "State", options = state_opts_prev, default = state_opts_prev[1])
  ),
  seriesOrder = prev_measures,
  seriesMeta = setNames(lapply(prev_measures, function(m) list(label = prev_labels[[m]])), prev_measures),
  defaultOn = c("medicaid_ima_ch_rate", "medicaid_cap_ch_rate"),
  lines = prev_lines
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
act_measures <- c("pct_inactive_60min_5days", "pct_no_pe_classes", "pct_no_sports_team", "pct_no_daily_pe", "pct_inactive_all_days")
act_labels <- c(
  pct_inactive_60min_5days = "Not active ≥60 min on ≥5 days",
  pct_no_pe_classes = "Does not attend any PE classes",
  pct_no_sports_team = "Did not play on a sports team",
  pct_no_daily_pe = "Not in daily PE class",
  pct_inactive_all_days = "No physical activity on any day"
)
act_data <- yrbss %>%
  filter(measure %in% act_measures, race == "All", ethnicity == "All", not_asked == 0, age %in% yrbss_age_order) %>%
  mutate(note = if_else(suppressed == 1, " (suppressed - imputed)", ""))
act_lines <- build_lines(act_data, dim_cols = c("geography", "age", "sex", "measure"),
                          series_col = "age", x_col = "year", y_col = "value", note_col = "note")
state_opts_act <- c("United States", sort(setdiff(unique(act_data$geography), "United States")))
age_opts_act <- yrbss_age_order[yrbss_age_order %in% unique(act_data$age)]

emit_json("PH_ACTIVITY_TS", list(
  title = "Physical activity, past 7 days", height = 480,
  xTitle = "Year", yTitle = "Percent of students (%)", yUnit = "%", decimals = 1,
  filters = list(
    list(key = "geography", label = "State", options = state_opts_act, default = "United States"),
    list(key = "measure", label = "Measure", options = lapply(act_measures, function(m) list(value = m, label = act_labels[[m]])), default = "pct_inactive_all_days")
  ),
  compareBy = yrbss_compare_by(age_opts_act),
  lines = act_lines
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
nutr_yrbss_labels <- c(pct_no_breakfast = "Did not eat breakfast (yesterday)", pct_no_breakfast_7days = "Ate no breakfast, any of past 7 days",
                        pct_no_fruit = "Ate no fruit (past 7 days)", pct_no_vegetables = "Ate no vegetables (past 7 days)")
nutr_yrbss_data <- yrbss %>%
  filter(measure %in% names(nutr_yrbss_labels), age == "Overall", sex == "All", race == "All", ethnicity == "All",
         not_asked == 0, geography != "United States") %>%
  mutate(loc = fips2abbr[fips], t = year, note = if_else(suppressed == 1, " (suppressed - imputed)", ""))
nutr_yrbss_entries <- lapply(names(nutr_yrbss_labels), function(mid) {
  d <- nutr_yrbss_data %>% filter(measure == mid) %>% select(loc, t, value, note)
  build_measure_entry(d, id = mid, label = paste0(nutr_yrbss_labels[[mid]], " (state, YRBSS)"), level = "state", unit = "%", decimals = 1)
})

nutr_medicaid_data <- medicaid %>% filter(measure == "medicaid_wcc_ch_rate") %>% mutate(loc = fips2abbr[fips], t = year)
nutr_medicaid_entries <- lapply(c("Medicaid", "CHIP"), function(py) {
  d <- nutr_medicaid_data %>% filter(payer == py) %>% select(loc, t, value)
  build_measure_entry(d, id = paste0("medicaid_wcc_ch_rate|", py),
    label = paste0("Weight assessment & nutrition counseling (WCC) — ", py, " (state, Medicaid)"), level = "state", unit = "%", decimals = 1)
})

nutr_chr_entries <- lapply(c("chr_limited_access_to_healthy_foods", "chr_food_environment_index"), function(mid) {
  e <- chr_choropleth_measure(mid, "county")
  e$label <- paste0(e$label, " (county, CHR)")
  e
})

nutr_all <- c(nutr_yrbss_entries, nutr_medicaid_entries, nutr_chr_entries)
emit_json("PH_NUTRITION_MAP", list(measureLabel = "Measure", height = 520,
                                    defaultMeasure = "chr_limited_access_to_healthy_foods", measures = nutr_all))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
sdoh_categories <- list(
  "Schools" = c("chr_high_school_completion", "chr_school_segregation", "chr_school_funding_adequacy",
                "chr_child_care_centers", "chr_high_school_graduation", "chr_math_scores", "chr_reading_scores"),
  "Access to care" = c("chr_uninsured_children", "chr_primary_care_physicians", "chr_inadequate_social_support", "chr_other_primary_care_providers"),
  "Poverty measures" = c("chr_children_in_poverty", "chr_income_inequality", "chr_food_insecurity"),
  "Housing" = c("chr_single_parent_households", "chr_high_housing_costs", "chr_severe_housing_problems", "chr_severe_housing_cost_burden"),
  "Surrounding community" = c("chr_access_to_recreational_facilities", "chr_limited_access_to_healthy_foods", "chr_access_to_parks",
                               "chr_access_to_exercise_opportunities", "chr_food_environment_index", "chr_residential_segregation_black_white"),
  "Crime/violence" = c("chr_violent_crime", "chr_juvenile_arrests", "chr_firearm_fatalities")
)
sdoh_measure_to_cat <- unlist(lapply(names(sdoh_categories), function(cat) setNames(rep(cat, length(sdoh_categories[[cat]])), sdoh_categories[[cat]])))

sdoh_entries <- list()
for (mid in names(sdoh_measure_to_cat)) {
  cat_lbl <- sdoh_measure_to_cat[[mid]]
  for (lvl in c("state", "county")) {
    eid <- paste(mid, lvl, sep = "|")
    sdoh_entries[[eid]] <- chr_choropleth_measure(mid, lvl, tags = list(category = cat_lbl, geo_level = lvl))
  }
}

emit_json("SDOH_MAP", list(
  measureLabel = "Measure", height = 560,
  filters = list(
    list(key = "geo_level", label = "Geography", options = list(list(value = "county", label = "County"), list(value = "state", label = "State")), default = "county"),
    list(key = "category", label = "Category", options = names(sdoh_categories), default = "Schools")
  ),
  defaultMeasure = "chr_high_school_completion|county",
  measures = unname(sdoh_entries)
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| results: asis
# Values align to the shared geo_state_canon/geo_county_canon id order (like
# build_measure_entry) -- only `values` travels per measure, not locs/labels.
build_scatter_measures <- function(level) {
  src <- if (level == "state") chr_state else chr_county
  canon <- if (level == "state") geo_state_canon else geo_county_canon
  measure_ids <- intersect(chr_measure_info$measure, unique(src$measure))
  lapply(measure_ids, function(mid) {
    dd <- src %>% filter(measure == mid)
    latest_yr <- max(dd$year)
    dd <- dd %>% filter(year == latest_yr) %>%
      mutate(loc = if (level == "state") fips2abbr[fips] else fips, value = value * chr_scale[[mid]])
    vals <- dd$value[match(canon$loc, dd$loc)]
    list(id = mid, label = chr_label[[mid]], values = I(rnd(vals, 3)))
  })
}

emit_json("SDOH_SCATTER", list(
  height = 500, defaultLevel = "county", levelLabels = list(state = "State", county = "County"),
  defaultX = "chr_children_in_poverty", defaultY = "chr_teen_births",
  measuresByLevel = list(state = build_scatter_measures("state"), county = build_scatter_measures("county"))
))
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
