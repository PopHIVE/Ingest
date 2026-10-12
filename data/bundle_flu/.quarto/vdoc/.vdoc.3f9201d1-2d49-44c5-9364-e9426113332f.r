#
#
#
#
#
#
#
#
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
source("_flu_dashboard_utils.R")
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
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
#
# The file is slimmed on the way through: only the feature `id` is used
# (featureidkey = "id"), so the per-county properties block is dropped,
# whitespace is removed, and coordinates are cut to 3 decimals (~100 m,
# invisible at national-map scale).
geo_con <- gzfile("us_counties_fips.geojson.gz", "rb")
geo_raw <- readBin(geo_con, "raw", n = 50 * 1024 * 1024)
close(geo_con)
geo_txt <- rawToChar(geo_raw)
geo_txt <- gsub('"properties": \\{[^}]*\\}, ', "", geo_txt)
geo_txt <- gsub('(-?[0-9]+\\.[0-9]{3})[0-9]+', "\\1", geo_txt)
geo_txt <- gsub('(?<=[,:]) ', "", geo_txt, perl = TRUE)
out_con <- file("counties_geojson.js", "wb")
writeBin(charToRaw("window.PH_COUNTIES_GEOJSON = "), out_con)
writeBin(charToRaw(geo_txt), out_con)
writeBin(charToRaw(";\n"), out_con)
close(out_con)
#
#
#
#| results: asis
# Emitted ONCE for the whole page. Every choropleth measure and every
# scatter-tool measure aligns its values to these two id/label arrays
# instead of carrying its own -- see the comment on geo_state_canon /
# geo_county_canon in _flu_dashboard_utils.R.
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
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
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
nat <- function(df, src) df %>% filter(geography_fips == "00", source == src) %>% select(date, value)
nat_hosp <- function(m) flu_hosp %>% filter(geography_fips == "00", measure == m) %>% select(date, value)

cards <- list(
  ov_card("ED visits for flu (CDC NSSP)", nat(flu_overall, "CDC NSSP"), "% of ED visits", 2),
  ov_card("Hospital admissions for flu (CDC NHSN)", nat_hosp("nhsn_adm_flu"), "admissions", 0),
  ov_card("Patients in hospital with flu (CDC NHSN)", nat_hosp("nhsn_hosp_pats_flu"), "patients", 0),
  ov_card("Patients in ICU with flu (CDC NHSN)", nat_hosp("nhsn_icu_pats_flu"), "patients", 0),
  ov_card("Outpatient flu-like illness (CDC ILINet)", nat(flu_overall, "CDC ILINet"), "% of visits", 2),
  ov_card("Flu in wastewater (CDC NWSS)", nat(flu_overall, "CDC NWSS"), "viral activity", 2),
  ov_card("Flu hospitalization rate (CDC RESP-NET)", nat(flu_overall, "CDC RespNET"), "per 100,000", 2),
  ov_card("ED visits for flu (Epic Cosmos)", nat(flu_overall, "Epic Cosmos, ED"), "% of ED visits", 2,
          foot = "Epic data arrive in batches, so this lags the others.", date_label = "Refreshed through"),
  ov_card("Illness signal (Kinsa thermometers)", nat(flu_overall, "Kinsa"), "illness signal", 2)
)
cards <- Filter(Negate(is.null), cards)
dirs <- vapply(cards, `[[`, character(1), "dir")
pct_peak <- median(vapply(cards, `[[`, numeric(1), "pct_peak"), na.rm = TRUE)

# Rt: latest estimate per state (CDC publishes state estimates only).
rt_state <- flu_rtm %>% filter(measure == "cdc_rt_flu", geography_fips != "00") %>%
  group_by(geography_fips) %>% filter(date == max(date)) %>% ungroup()
rt_date <- max(rt_state$date)
rt_state <- rt_state %>% filter(date >= rt_date - 14)
n_grow <- sum(rt_state$value > 1)

week_txt <- ov_date(max(flu_overall$date[flu_overall$source == "CDC NSSP"]))
cat(sprintf(
  '<p class="ov-headline">Latest national data (most sources through the week ending <b>%s</b>): of <b>%d</b> indicators, <b>%d</b> are rising, <b>%d</b> falling and <b>%d</b> steady over the past two weeks. Typical indicator is at <b>%.0f%%</b> of its past-year peak. CDC&rsquo;s state models estimate flu is growing (Rt above 1) in <b>%d of %d</b> states as of %s.</p>\n',
  week_txt, length(dirs), sum(dirs == "rising"), sum(dirs == "falling"), sum(dirs == "steady"),
  pct_peak, n_grow, nrow(rt_state), ov_date(rt_date)))

cat('<div class="ov-list">\n', paste(vapply(cards, `[[`, character(1), "html"), collapse = "\n"), '\n</div>\n')
#
#
#
#
#
#| results: asis
lat <- function(df, src) max(df$date[df$source == src])
fresh <- tibble::tribble(
  ~name, ~date, ~lag,
  "CDC NSSP emergency department visits", lat(flu_overall, "CDC NSSP"), source_caveats$nssp$lag,
  "CDC NHSN hospital admissions", max(flu_hosp$date), source_caveats$nhsn$lag,
  "CDC ILINet outpatient visits", lat(flu_overall, "CDC ILINet"), source_caveats$ilinet$lag,
  "CDC NWSS wastewater", lat(flu_overall, "CDC NWSS"), source_caveats$nwss$lag,
  "CDC RESP-NET hospitalizations", lat(flu_overall, "CDC RespNET"), source_caveats$respnet$lag,
  "Epic Cosmos ED visits", lat(flu_overall, "Epic Cosmos, ED"), "Usually 3 to 5 weeks",
  "Kinsa illness signal", lat(flu_overall, "Kinsa"), source_caveats$kinsa$lag,
  "CDC Rt, states", rt_date, source_caveats$rt$lag
)
cat('<details class="chart-info"><summary>Data freshness</summary>\n<table class="ov-table"><tr><th>Source</th><th>Latest data</th><th>Typical lag</th></tr>\n')
for (i in seq_len(nrow(fresh))) cat(sprintf('<tr><td>%s</td><td>%s</td><td>%s</td></tr>\n',
  fresh$name[i], ov_date(fresh$date[i]), sub("\\. .*$", "", fresh$lag[i])))
cat('</table></details>\n')
#
#
#
#
#
#
#
#| results: asis
src_units <- tribble(
  ~source,            ~label,                         ~unit,                 ~dec, ~sub,
  "CDC ILINet",       "CDC ILINet",                   "% of visits",         1, "Weighted % of outpatient visits for influenza-like illness",
  "CDC NSSP",         "CDC NSSP (ED visits)",         "% of ED visits",      1, "% of emergency department visits with a flu diagnosis",
  "Epic Cosmos, ED",  "Epic Cosmos (ED visits)",      "% of ED visits",      1, "% of emergency department encounters with flu, all ages",
  "CDC NWSS",         "CDC NWSS (wastewater)",        "viral activity",      1, "Influenza A wastewater viral activity",
  "CDC NHSN",         "CDC NHSN (hospital admissions)", "admissions",        0, "Weekly flu hospital admissions (count)",
  "CDC RespNET",      "CDC RESP-NET (hospitalizations)", "per 100,000",      1, "Flu-associated hospitalization rate, participating states only"
)

act_state <- flu_overall %>%
  filter(geography_fips != "00", source %in% src_units$source) %>%
  mutate(loc = fips2abbr[geography_fips], t = chr_date(date),
         note = if_else(suppressed_flag == 1, " (suppressed - imputed)", ""))

# "Overall" first for every source, then that source's age groups (only NHSN and
# Epic Cosmos publish age-specific state data); the dropdowns are Source -> Age group.
act_map_measures <- lapply(seq_len(nrow(src_units)), function(i) {
  u <- src_units[i, ]
  d <- act_state %>% filter(source == u$source) %>% select(loc, t, value, note)
  e <- build_measure_entry(d, id = paste(u$source, "Overall", sep = "|"), label = "Overall", level = "state",
                           unit = u$unit, decimals = u$dec, sub = u$sub, tags = list(source = u$source))
  e$title <- paste0(u$label, ", all ages")
  e
})

age_state <- flu_age %>%
  filter(geography_fips != "00", age != "Total") %>%
  mutate(loc = fips2abbr[geography_fips], t = chr_date(date),
         note = if_else(suppressed_flag == 1, " (suppressed - imputed)", ""))
age_units <- c("CDC NHSN" = "admissions per 100,000", "Epic Cosmos, ED" = "% of ED visits")
age_combos <- age_state %>% distinct(source, age) %>% arrange(source, match(age, age_sort(age)))
age_map_measures <- lapply(seq_len(nrow(age_combos)), function(i) {
  s <- age_combos$source[i]; a <- age_combos$age[i]
  d <- age_state %>% filter(source == s, age == a) %>% select(loc, t, value, note)
  e <- build_measure_entry(d, id = paste(s, a, sep = "|"), label = a, level = "state",
                           unit = age_units[[s]], decimals = 1,
                           sub = paste0(s, ", age ", a), tags = list(source = s))
  e$title <- paste0(s, ", age ", a)
  e
})
act_map_measures <- c(act_map_measures, age_map_measures)

cty <- flu_county %>%
  transmute(loc = geography_fips, t = chr_date(date), value = value,
            note = if_else(is_state_estimate == 1, " (state-level estimate)", ""))
act_map_measures <- c(act_map_measures, list(
  build_measure_entry(cty, id = "CDC NSSP|county", label = "CDC NSSP (ED visits)", level = "county",
                      unit = "% of ED visits", decimals = 1,
                      sub = "% of emergency department visits with a flu diagnosis, CDC NSSP, by county")))

emit_chart("act-map", "Choropleth", list(
  measureLabel = "Age group", height = 500, defaultMeasure = "CDC NSSP|Overall", levelToggle = TRUE,
  stateOptions = lapply(sort(unique(substr(cty$loc, 1, 2))), function(f) list(value = f, label = unname(fips2name[f]))),
  filters = list(list(key = "source", label = "Source",
                      options = lapply(seq_len(nrow(src_units)), function(i) list(value = src_units$source[i], label = src_units$label[i])),
                      default = "CDC NSSP")),
  measures = act_map_measures),
  "Weekly influenza activity from each surveillance system, overall or by age group. Choose a <b>source</b>, then an <b>age group</b> (Overall is the default; only CDC NHSN and Epic Cosmos publish age-specific state data) and drag the slider to change the week. Switch <b>Map | Beeswarm</b> to see every state as one dot on a single value axis, with the median of states marked. Grey areas indicate the source does not provide data for that geography. Use the <b>State | County</b> toggle to switch geography; county estimates are available for CDC NSSP ED visits only, and with Beeswarm selected you can pick a state to see each of its counties as one dot. Sources report in different units (NHSN is admissions per 100,000 residents in the age group; Epic Cosmos is the share of ED encounters with a flu diagnosis), so compare patterns within a source rather than colors across sources. Data: <a href=\"https://github.com/PopHIVE/Ingest/tree/main/data/bundle_flu\">bundle_flu</a> (<code>flu_overall_trends</code>, <code>flu_trends_by_age</code>).")
#
#
#
#
#
#| results: asis
act_ts <- flu_overall %>% filter(source %in% src_units$source | source == "Kinsa") %>% mutate(age = "Overall")
act_src_order <- c(src_units$source, "Kinsa")
act_src_order <- act_src_order[act_src_order %in% unique(act_ts$source)]
act_src_label <- c(setNames(src_units$label, src_units$source), "Kinsa" = "Kinsa")

age_ts <- flu_age
age_srcs <- sort(unique(age_ts$source))
age_by_src <- setNames(lapply(age_srcs, function(s) list(
  fixed = list(source = s), seriesOrder = I(as.character(age_sort(age_ts$age[age_ts$source == s]))), seriesMeta = empty_obj())), age_srcs)

act_cmp <- list(
  label = "Show", default = "source", sourceKey = "src_pick",
  groups = list(
    source = list(label = "Overall (compare sources)", fixed = list(age = "Overall"), seriesOrder = I(act_src_order),
                  seriesMeta = lapply(act_src_label[act_src_order], function(l) list(label = l))),
    age = list(label = "By age group", bySource = age_by_src)))

emit_chart("act-ts", "LineChart", list(
  title = "Flu activity over time", height = 480, xTitle = "Week ending", decimals = 1, views = TRUE,
  unitFilterKey = "display",
  yTitleMap = list(scaled = "Scaled 0-100 within each series", smooth = "Native units (differ by source; hover for unit)",
                   raw = "Native units (differ by source; hover for unit)"),
  yUnitMap = list(scaled = "", smooth = "", raw = ""),
  seriesUnits = as.list(c(setNames(src_units$unit, src_units$source), "Kinsa" = "illness signal")),
  filters = list(
    list(key = "geography", label = "Location", options = geo_options(c(act_ts$geography, age_ts$geography)), default = "United States"),
    list(key = "display", label = "Units", options = display_options, default = "scaled"),
    list(key = "src_pick", label = "Source", showWhen = list(cmpBy = "age"),
         options = lapply(age_srcs, function(s) list(value = s, label = unname(act_src_label[s]))), default = "CDC NHSN")
  ),
  compareBy = act_cmp,
  lines = c(build_display_lines(act_ts, c("geography", "source", "age"), "source"),
            build_display_lines(age_ts %>% mutate(age = as.character(age)), c("geography", "source", "age"), "age"))
), "Weekly flu activity, overall or by age group. With <b>Show: Overall</b> each line is a surveillance system; with <b>Show: By age group</b> pick a source and each line is an age group. Sources use different units, so the default view rescales every series to 0-100 over its own history; switch <b>Units</b> to see native units. Click legend entries to hide or show a line. <b>Faceted</b> gives each line its own panel on a shared time axis, so peaks can be compared by timing; <b>Season overlay</b> draws one line per flu season (starting October) for a single series, so you can see whether this season is early, late or large. Kinsa is national only and RESP-NET covers only participating states. Age labels are each source's own and do not line up. Epic Cosmos points flagged as suppressed small cells (<10) are imputed as 5. Data: <a href=\"https://github.com/PopHIVE/Ingest/tree/main/data/bundle_flu\">bundle_flu</a>.")
#
#
#
#
#
#| results: asis
rt_state_all <- flu_rtm %>%
  filter(geography_fips != "00", measure %in% c("cdc_rt_flu", "cdc_rt_flu_p_growing")) %>%
  mutate(loc = fips2abbr[geography_fips], t = chr_date(date))
rt_map_entry <- function(m, label, unit, dec, sub, scale, rng) {
  d <- rt_state_all %>% filter(measure == m) %>% mutate(value = value * scale) %>% select(loc, t, value)
  e <- build_measure_entry(d, id = m, label = label, level = "state", unit = unit, decimals = dec, sub = sub)
  e$diverging <- list(min = rng[1], max = rng[2])
  e
}
emit_chart("rt-map", "Choropleth", list(
  measureLabel = "Measure", height = 500, defaultMeasure = "cdc_rt_flu",
  measures = list(
    rt_map_entry("cdc_rt_flu", "Rt (above 1 = growing)", "Rt", 2, "CDC CFA estimate of the flu reproduction number", 1, c(0.6, 1.4)),
    rt_map_entry("cdc_rt_flu_p_growing", "Probability flu is growing", "%", 0, "CDC CFA probability that Rt is above 1", 100, c(0, 100))
  )
), "Estimated growth of flu infections in each state, from the CDC Center for Forecasting and Outbreak Analytics. <b>Orange</b> states have Rt above 1 (flu growing); <b>blue</b> states have Rt below 1 (shrinking); pale means near 1. Drag the slider to see how each state changed day by day, or switch the measure to the probability that flu is growing. Grey means no estimate. CDC publishes state estimates only; there is no current national estimate. PopHIVE keeps only each state&rsquo;s latest model run, so the history is short. Data: <a href=\"https://github.com/PopHIVE/Ingest/tree/main/data/cdc_cfa_rt\">CDC CFA Rt</a>.")
#
#
#
#
#
#| results: asis
rt <- flu_rtm %>%
  filter(geography != "United States", measure %in% c("cdc_rt_flu", "cdc_rt_flu_lower", "cdc_rt_flu_upper")) %>%
  select(geography, date, measure, value) %>%
  pivot_wider(names_from = measure, values_from = value) %>%
  mutate(xx = chr_date(date), series = "Rt")
emit_chart("rt-ts", "LineChart", list(
  title = "Flu effective reproduction number (Rt)", height = 440, xTitle = "Date", decimals = 2,
  yTitle = "Rt (above 1 = growing)",
  errorBand = list(label = "Show interval", default = TRUE),
  seriesOrder = I("Rt"),
  filters = list(list(key = "geography", label = "State", options = geo_options(rt$geography), default = "California")),
  lines = build_lines(rt, "geography", "series", "xx", "cdc_rt_flu", lower_col = "cdc_rt_flu_lower", upper_col = "cdc_rt_flu_upper", digits = 3)
), "Estimated growth of flu infections for the selected state, from the CDC Center for Forecasting and Outbreak Analytics. Rt above 1 means infections are increasing; below 1, decreasing. Whiskers show the lower and upper bounds. CDC publishes state estimates only. PopHIVE keeps only each state&rsquo;s latest model run. Data: <a href=\"https://github.com/PopHIVE/Ingest/tree/main/data/cdc_cfa_rt\">CDC CFA Rt</a>.")
#
#
#
#
#
#
#
#| results: asis
hosp_state <- flu_hosp %>% filter(geography_level == "State") %>%
  mutate(loc = fips2abbr[geography_fips], t = chr_date(date))
hosp_map_ids <- c("nhsn_adm_rate_flu", "nhsn_adm_flu", "nhsn_hosp_pats_flu", "nhsn_icu_pats_flu",
                  "nhsn_pct_inpt_beds_flu", "nhsn_pct_icu_beds_flu")
hosp_map_measures <- lapply(hosp_map_ids, function(m) {
  d <- hosp_state %>% filter(measure == m) %>% select(loc, t, value)
  build_measure_entry(d, id = m, label = hosp_label[[m]], level = "state", unit = hosp_unit[[m]],
                      decimals = if (hosp_unit[[m]] %in% c("%", "per 100,000")) 1 else 0,
                      sub = "CDC NHSN hospital reporting")
})
death_ids <- c("rate_influenza_and_pneumonia", "influenza_associated_pediatric_mortality_weekly",
               "influenza_associated_pediatric_mortality_cumulative")
death_state <- flu_rtm %>% filter(measure %in% death_ids) %>%
  mutate(loc = fips2abbr[geography_fips], t = chr_date(date))
death_map_measures <- lapply(death_ids, function(m) {
  d <- death_state %>% filter(measure == m) %>% select(loc, t, value)
  build_measure_entry(d, id = m, label = rtm_label[[m]], level = "state", unit = rtm_unit[[m]],
                      decimals = if (m == "rate_influenza_and_pneumonia") 1 else 0,
                      sub = if (m == "rate_influenza_and_pneumonia") "NCHS quarterly rate; influenza and pneumonia combined" else "CDC NNDSS")
})
emit_chart("hd-map", "Choropleth", list(
  measureLabel = "Measure", height = 500, defaultMeasure = "nhsn_adm_rate_flu",
  measures = c(hosp_map_measures, death_map_measures)
), "Flu hospital metrics (CDC NHSN) and flu-related deaths by state. Choose a measure and drag the slider to change the week or period; switch <b>Map | Beeswarm</b> to see every state as one dot. Hospital counts depend on how many hospitals report; use the per-100,000 admission rate to compare states of different size. The NCHS death rate is for <b>influenza and pneumonia combined</b>, not flu alone. Pediatric death counts (CDC NNDSS) are weekly or year-to-date totals; grey means none reported or no data. Data: <a href=\"https://github.com/PopHIVE/Ingest/tree/main/data/nhsn_hospital_capacity\">CDC NHSN</a> and <a href=\"https://github.com/PopHIVE/Ingest/tree/main/data/bundle_flu\">NCHS and CDC NNDSS</a>.")
#
#
#
#
#
#| results: asis
hosp_ts <- flu_hosp %>% filter(!is.na(value))
hosp_geo <- c("United States",
              sort(unique(hosp_ts$geography[hosp_ts$geography_level == "HHS region"])),
              sort(unique(hosp_ts$geography[hosp_ts$geography_level == "State"])),
              sort(unique(hosp_ts$geography[!hosp_ts$geography_level %in% c("National", "HHS region", "State")])))
hosp_geo <- setdiff(unique(hosp_geo), non_us_territories)
hosp_ids <- names(hosp_label)
death_ts <- flu_rtm %>% filter(measure %in% death_ids, !is.na(value))
death_geo <- geo_options(death_ts$geography)

hd_ts <- bind_rows(hosp_ts %>% select(geography, date, measure, value),
                   death_ts %>% select(geography, date, measure, value)) %>%
  mutate(xx = chr_date(date), series = "Value")
hd_ids <- c(hosp_ids, death_ids)
hd_label <- c(unlist(hosp_label), unlist(rtm_label[death_ids]))
hd_unit <- c(unlist(hosp_unit), unlist(rtm_unit[death_ids]))
hd_label <- setNames(hd_label, hd_ids); hd_unit <- setNames(hd_unit, hd_ids)

emit_chart("hd-ts", "LineChart", list(
  title = "Flu hospital and death measures over time", height = 460, xTitle = "Date", decimals = 1, views = TRUE,
  unitFilterKey = "measure",
  yTitleMap = as.list(setNames(paste0(hd_label, " (", hd_unit, ")"), hd_ids)),
  yUnitMap = as.list(setNames(ifelse(hd_unit == "%", "%", paste0(" ", hd_unit)), hd_ids)),
  seriesOrder = I("Value"),
  filters = list(
    list(key = "measure", label = "Measure",
         options = lapply(hd_ids, function(m) list(value = m, label = hd_label[[m]])), default = "nhsn_adm_flu"),
    list(key = "geography", label = "Location", options = hosp_geo, default = "United States",
         optionsBy = list(key = "measure", map = c(
           setNames(lapply(hosp_ids, function(m) I(hosp_geo)), hosp_ids),
           setNames(lapply(death_ids, function(m) I(death_geo)), death_ids))))
  ),
  lines = build_lines(hd_ts, c("geography", "measure"), "series", "xx", "value", digits = 1)
), "Weekly NHSN flu hospital measures for the nation, HHS regions, states and territories, plus pediatric flu deaths and the quarterly NCHS death rate for influenza and pneumonia combined (nation and states). Choose a measure and a location; locations without data for the measure are not listed. Percent-of-hospitals-reporting measures show how complete the reporting is. <b>Season overlay</b> draws one line per flu season (starting October) so the current season can be compared with earlier ones. A few weekly NNDSS values are negative where a count was revised downward; they are shown as reported. Data: <a href=\"https://github.com/PopHIVE/Ingest/tree/main/data/nhsn_hospital_capacity\">CDC NHSN</a>, <a href=\"https://github.com/PopHIVE/Ingest/tree/main/data/bundle_flu\">bundle_flu</a> (<code>flu_rt_and_mortality</code>).")
#
#
#
#
#
#
#
#| results: asis
# One end-of-season value per state x season x age group: the latest date the
# source reports inside that season.
vax_overall <- flu_vax %>%
  filter(stratum_type == "Overall", measure == "coverage", population == "Total",
         geography_fips != "00", !is.na(value), !is.na(season)) %>%
  group_by(source, geography_fips, season, age) %>% slice_max(date, n = 1, with_ties = FALSE) %>% ungroup() %>%
  mutate(loc = fips2abbr[geography_fips], t = season)
vax_map_sources <- c("CDC FluVaxView", "CDC NIS-Flu", "IIS", "CMS Medicare")
vax_combos <- vax_overall %>% filter(source %in% vax_map_sources) %>% distinct(source, age) %>%
  arrange(factor(source, vax_map_sources), match(age, age_sort(age)))
vax_map_measures <- lapply(seq_len(nrow(vax_combos)), function(i) {
  s <- vax_combos$source[i]; a <- vax_combos$age[i]
  d <- vax_overall %>% filter(source == s, age == a) %>% select(loc, t, value)
  build_measure_entry(d, id = paste(s, a, sep = "|"), label = a, level = "state", unit = "%", decimals = 1,
                      sub = paste0("Flu vaccination coverage, ", s, ", ", a), tags = list(source = s))
})
vax_cty <- flu_vax_sub %>%
  filter(geography_level == "County", measure == "coverage", !is.na(value)) %>%
  transmute(loc = geography_fips, t = format(date, "%Y"), value = value)
vax_map_measures <- c(vax_map_measures, list(
  build_measure_entry(vax_cty, id = "vax_county", label = "Adult flu vaccination coverage", level = "county",
                      unit = "%", decimals = 1, sub = "Adults 18 and older, CDC FluVaxView modeled county estimates, by calendar year")))
emit_chart("vax-map", "Choropleth", list(
  measureLabel = "Age group", height = 500, levelToggle = TRUE,
  stateOptions = lapply(sort(unique(substr(vax_cty$loc, 1, 2))), function(f) list(value = f, label = unname(fips2name[f]))),
  filters = list(list(key = "source", label = "Source", options = I(vax_map_sources), default = "CDC FluVaxView")),
  defaultMeasure = "CDC FluVaxView|>=6 Months", measures = vax_map_measures
), "Percent vaccinated against flu, by state and flu season (drag the slider). Each value is the last estimate the source reports in that season. Sources use different age bands and survey or administrative methods, so choose a source first. The <b>County</b> toggle shows modeled CDC FluVaxView estimates of adult (18 and older) coverage by calendar year; with <b>Map | Beeswarm</b> set to Beeswarm, pick a state to see each of its counties as one dot. Switch to Beeswarm on the state view to compare states instead. Data: <a href=\"https://github.com/PopHIVE/Ingest/tree/main/data/bundle_flu\">bundle_flu</a> (<code>flu_vax_coverage</code>, <code>flu_vax_substate</code>).")
#
#
#
#
#
#| results: asis
vax_ts <- flu_vax %>%
  filter(stratum_type == "Overall", measure == "coverage", population == "Total", !is.na(value), !is.na(season),
         source %in% c("CDC FluVaxView", "CDC NIS-Flu", "IIS", "CMS Medicare")) %>%
  group_by(source, geography, season, age) %>% slice_max(date, n = 1, with_ties = FALSE) %>% ungroup() %>%
  mutate(xx = season, series = age)

# IQVIA weekly doses administered (national) live in the same chart as one more
# "source": its lines carry measure/setting dims, which the matching filters
# below only show (and apply) while that source is selected.
dos_src <- "IQVIA doses"
dos_adm <- flu_doses %>% filter(source == "IQVIA", !is.na(value)) %>%
  mutate(source = dos_src, xx = chr_date(date), series = age)
dos_meas <- c(doses_administered = "Weekly doses administered", doses_administered_cumulative = "Cumulative doses administered")

vax_srcs <- c(sort(unique(vax_ts$source)), dos_src)
vax_src_opts <- lapply(vax_srcs, function(s) list(value = s, label = if (s == dos_src) "IQVIA (doses administered)" else s))
is_dos <- vax_srcs == dos_src
vax_geo_all <- geo_options(c(vax_ts$geography, "United States"))
vax_geo_by_src <- c(
  lapply(split(vax_ts$geography, vax_ts$source), function(g) I(geo_options(g))),
  setNames(list(I("United States")), dos_src))
vax_cmp_df <-bind_rows(vax_ts %>% select(source, age), dos_adm %>% select(source, age))

emit_chart("vax-ts", "LineChart", list(
  title = "Flu vaccination over time", height = 480, zeroAsGap = TRUE, xTitle = "Flu season", yTitle = "Percent vaccinated",
  yUnit = "%", decimals = 1,
  unitFilterKey = "source",
  xTitleMap = as.list(setNames(ifelse(is_dos, "Week ending", "Flu season"), vax_srcs)),
  yTitleMap = as.list(setNames(ifelse(is_dos, "Doses administered", "Percent vaccinated"), vax_srcs)),
  yUnitMap = as.list(setNames(ifelse(is_dos, "", "%"), vax_srcs)),
  decimalsMap = as.list(setNames(ifelse(is_dos, 0, 1), vax_srcs)),
  errorBand = list(label = "Show confidence interval", default = FALSE),
  filters = list(
    list(key = "source", label = "Source", options = vax_src_opts, default = "CDC FluVaxView"),
    # Location only appears for sources that publish state-level estimates
    list(key = "geography", label = "Location", options = vax_geo_all, default = "United States", hideIfSingle = TRUE,
         optionsBy = list(key = "source", map = vax_geo_by_src)),
    list(key = "measure", label = "Measure", showWhen = list(otherFilterEq = list(source = dos_src)),
         options = lapply(names(dos_meas), function(m) list(value = m, label = dos_meas[[m]])), default = "doses_administered"),
    list(key = "setting", label = "Setting", showWhen = list(otherFilterEq = list(source = dos_src)),
         options = I(sort(unique(dos_adm$setting))), default = "Combined")
  ),
  # FluVaxView and NIS-Flu report many nested age bands; start with a
  # non-overlapping set and leave the rest to the legend toggles.
  compareBy = by_source_compare(vax_cmp_df, "source", "age", "Age group", age_sort, default_on = list(
    "CDC FluVaxView" = c("6 Months - 17 Years", "18-64 Years", ">=65 Years", ">=6 Months"),
    "CDC NIS-Flu" = c("6 months-17 years", "18-49 years", "50-64 years", "65+ years"))),
  lines = c(
    build_lines(vax_ts, c("geography", "source", "age"), "series", "xx", "value",
                lower_col = "value_lcl", upper_col = "value_ucl"),
    build_lines(dos_adm %>% mutate(geography = "United States"),
                c("geography", "source", "measure", "setting", "age"), "series", "xx", "value"))
), "Flu vaccination over time. For the coverage sources, lines are the last estimate reported in each season, by age group; choose a location and source, and click legend entries to hide or show age groups (a representative non-overlapping set is shown first because the sources publish many nested age bands). Confidence intervals are shown where the source publishes them. Choose <b>IQVIA (doses administered)</b> to see national weekly or cumulative flu vaccine doses from pharmacy and physician-office data (Combined = both), by age group; IQVIA is national only. Data: <a href=\"https://github.com/PopHIVE/Ingest/tree/main/data/bundle_flu\">bundle_flu</a>, <a href=\"https://github.com/PopHIVE/Ingest/tree/main/data/iqvia_vax_administered\">IQVIA</a>.")
#
#
#
#
#
#| results: asis
# End-of-season coverage: the latest estimate each source reports inside the season.
db_main <- flu_vax %>%
  filter(stratum_type == "Overall", measure == "coverage", population == "Total", !is.na(value), !is.na(season),
         source %in% vax_map_sources) %>%
  group_by(source, geography, season, age) %>% slice_max(date, n = 1, with_ties = FALSE) %>% ungroup() %>%
  transmute(view = "main", source, loc = geography, age, group = "", season, v = value)
db_race <- flu_vax %>%
  filter(source == "CDC FluVaxView", stratum_type == "Race and Ethnicity", measure == "coverage",
         age == "Total", !is.na(value), !is.na(season)) %>%
  group_by(geography, season, stratum) %>% slice_max(date, n = 1, with_ties = FALSE) %>% ungroup() %>%
  transmute(view = "race", source, loc = geography, age, group = stratum, season, v = value)
db <- bind_rows(db_main, db_race) %>% filter(!loc %in% non_us_territories)
db_dict <- lapply(db[c("view", "source", "loc", "age", "group", "season")], function(x) sort(unique(x)))
db_dict$age <- as.character(age_sort(db_dict$age))
db_cols <- c(lapply(names(db_dict), function(n) I(match(db[[n]], db_dict[[n]]) - 1L)) %>% setNames(names(db_dict)),
             list(v = I(rnd(db$v, 1))))
db_dict <- lapply(db_dict, I)
db_cols$age <- I(match(db$age, db_dict$age) - 1L)
emit_chart("vax-dumbbell", "Dumbbell", list(
  title = "Flu vaccination coverage: one season against another",
  dict = db_dict, cols = db_cols,
  views = list(
    list(id = "state", label = "States", rowLabel = "State", rowCol = "loc", splitBy = "age", data = "main"),
    list(id = "age", label = "Age groups", rowLabel = "Age group", rowCol = "age", splitBy = "loc", data = "main"),
    list(id = "race", label = "Race and ethnicity", rowLabel = "Race and ethnicity", rowCol = "group", splitBy = "loc", data = "race"))
), "Each row is a state, an age group or a race and ethnicity group; the open dot is coverage in the earlier season and the filled dot is the later season, so the length of the line is the change. Rows are sorted by the later season. <b>States</b> compares every state for one age group; <b>Age groups</b> and <b>Race and ethnicity</b> compare groups within one location (race and ethnicity is published by CDC FluVaxView only). Values are each source's last estimate in the season, so seasons still in progress reflect the data collected so far. Data: <a href=\"https://github.com/PopHIVE/Ingest/tree/main/data/bundle_flu\">bundle_flu</a> (<code>flu_vax_coverage</code>).")
#
#
#
#
#
#| results: asis
season_of <- function(d) {
  d <- as.Date(d); y <- as.integer(format(d, "%Y")); m <- as.integer(format(d, "%m"))
  s <- ifelse(m >= 10, y, y - 1)
  sprintf("%d-%02d", s, (s + 1) %% 100)
}
# Severity: season total of NHSN weekly admissions per 100,000 (complete seasons only)
sc_hosp <- flu_hosp %>%
  filter(geography_level == "State", measure == "nhsn_adm_rate_flu", !is.na(value)) %>%
  mutate(loc = fips2abbr[geography_fips], season = season_of(date)) %>%
  group_by(season, loc) %>% summarise(v = sum(value), n = n(), .groups = "drop") %>% filter(n >= 26)
sc_vax <- flu_vax %>%
  filter(source == "CDC FluVaxView", stratum_type == "Overall", measure == "coverage", population == "Total",
         age == ">=6 Months", geography_fips != "00", !is.na(value), !is.na(season)) %>%
  group_by(season, geography_fips) %>% slice_max(date, n = 1, with_ties = FALSE) %>% ungroup() %>%
  transmute(season, loc = fips2abbr[geography_fips], v = value)
sc_seasons <- sort(intersect(unique(sc_hosp$season), unique(sc_vax$season)))
align_state <- function(df) rnd(df$v[match(geo_state_canon$loc, df$loc)], 1)
sc_measures <- c(
  lapply(sc_seasons, function(s) list(id = paste0("vax_", s), label = paste0("Flu vaccination coverage, ages 6 months and older, ", s, " (%)"),
                                      values = I(align_state(sc_vax[sc_vax$season == s, ])))),
  lapply(sc_seasons, function(s) list(id = paste0("hosp_", s), label = paste0("Flu hospital admissions per 100,000, ", s, " season total"),
                                      values = I(align_state(sc_hosp[sc_hosp$season == s, ])))))
sc_latest <- sc_seasons[length(sc_seasons)]
emit_chart("vax-scatter", "Scatter", list(
  height = 500, defaultLevel = "state", levelLabels = list(state = "State"), explainStats = TRUE,
  defaultX = paste0("vax_", sc_latest), defaultY = paste0("hosp_", sc_latest),
  measuresByLevel = list(state = sc_measures)
), "Each dot is a state. By default the x axis is end-of-season flu vaccination coverage (CDC FluVaxView, everyone 6 months and older) and the y axis is total flu hospital admissions per 100,000 residents over the same season (CDC NHSN weekly admissions, summed from October to September). Change either axis to compare other seasons, for example last season's coverage against this season's hospitalizations. The correlation is across states and is <b>not</b> evidence that vaccination caused the difference: states also differ in age, population density, testing, hospital reporting and which flu strain circulated. Seasons shown have a complete NHSN reporting year. Data: <a href=\"https://github.com/PopHIVE/Ingest/tree/main/data/bundle_flu\">bundle_flu</a>.")
#
#
#
#
#
#| results: asis
vax_race <- flu_vax %>%
  filter(source == "CDC FluVaxView", stratum_type == "Race and Ethnicity", measure == "coverage",
         !is.na(value), !is.na(season)) %>%
  group_by(geography, season, age, stratum) %>% slice_max(date, n = 1, with_ties = FALSE) %>% ungroup() %>%
  mutate(xx = season, series = stratum)
race_order <- sort(unique(vax_race$stratum))
vax_race <- vax_race %>% filter(age == "Total")  # the only age group published; no age dropdown needed
emit_chart("vax-race", "LineChart", list(
  title = "Flu vaccination coverage by race and ethnicity", height = 460, xTitle = "Flu season",
  yTitle = "Percent vaccinated", yUnit = "%", decimals = 1,
  toggle = TRUE, seriesOrder = I(race_order), defaultOn = I(race_order),
  errorBand = list(label = "Show confidence interval", default = FALSE),
  filters = list(
    list(key = "geography", label = "Location", options = geo_options(vax_race$geography), default = "United States")
  ),
  lines = build_lines(vax_race, "geography", "series", "xx", "value", lower_col = "value_lcl", upper_col = "value_ucl")
), "Flu vaccination coverage by race and ethnicity from CDC FluVaxView, by season. Choose a location; click legend entries to hide or show groups. Not every group is published for every state and season. Data: <a href=\"https://github.com/PopHIVE/Ingest/tree/main/data/fluvaxview\">CDC FluVaxView</a>.")
#
#
#
#
#
#| results: asis
vax_nis <- flu_vax %>%
  filter(source == "CDC NIS-Flu", measure == "coverage", stratum_type != "Overall", !is.na(value), !is.na(season)) %>%
  group_by(geography, season, age, stratum_type, stratum) %>% slice_max(date, n = 1, with_ties = FALSE) %>% ungroup() %>%
  mutate(xx = season, series = stratum)
nis_types <- sort(unique(vax_nis$stratum_type))
nis_ages <- age_sort(vax_nis$age)
# Characteristics published for each age group (drives the dependent dropdown)
nis_types_by_age <- setNames(lapply(nis_ages, function(a) I(sort(unique(vax_nis$stratum_type[vax_nis$age == a])))), nis_ages)
nis_cmp <- list(label = "Compare by", default = "stratum", sourceKey = "stratum_type", hidden = TRUE, groups = list(stratum = list(
  label = "Group", bySource = setNames(lapply(nis_types, function(s)
    list(fixed = empty_obj(), seriesOrder = I(sort(unique(vax_nis$stratum[vax_nis$stratum_type == s]))), seriesMeta = empty_obj())), nis_types))))
emit_chart("vax-nis", "LineChart", list(
  title = "Flu vaccination coverage by demographic group", height = 460, xTitle = "Flu season",
  yTitle = "Percent vaccinated", yUnit = "%", decimals = 1,
  errorBand = list(label = "Show confidence interval", default = FALSE),
  filters = list(
    list(key = "geography", label = "Location", options = geo_options(vax_nis$geography), default = "United States"),
    list(key = "age", label = "Age group", options = I(nis_ages), default = nis_ages[1]),
    list(key = "stratum_type", label = "Characteristic", options = I(nis_types),
         default = if ("Sex" %in% nis_types) "Sex" else nis_types[1],
         optionsBy = list(key = "age", map = nis_types_by_age))
  ),
  compareBy = nis_cmp,
  lines = build_lines(vax_nis, c("geography", "stratum_type", "age", "stratum"), "series", "xx", "value",
                      lower_col = "value_lcl", upper_col = "value_ucl")
), "Flu vaccination coverage from the CDC National Immunization Survey by demographic characteristic (sex, poverty status, insurance, urbanicity and others). Choose a location and age group, then a characteristic (only characteristics published for that age group are listed); click legend entries to hide or show groups. State-level survey estimates for small groups can be noisy; turn on confidence intervals to see the uncertainty. Data: <a href=\"https://github.com/PopHIVE/Ingest/tree/main/data/nis_flu_rsv\">CDC NIS-Flu</a>.")
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
