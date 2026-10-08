# Shared setup for Flu_Dashboard.qmd: packages, parquet loading, canonical geographies,
# and helpers that emit compact JSON for the Plotly-based chart components.

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
force_arrays <- function(x, keys = c("q", "qModes", "noteTbl", "options", "seriesOrder", "defaultOn", "times", "locs", "labels", "x", "xs", "y", "note", "values", "errUp", "errDown", "i", "v")) {
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
  # One split per measure instead of re-filtering df for every time point
  by_t <- split(df, factor(df$t, levels = times))
  # A time slice is emitted dense (array aligned to the canonical locs) unless
  # fewer than half the locations have a value (typical for county maps and
  # RESP-NET), in which case it is {i: positions, v: values}; the JS side
  # expands either form (see denseZ in PH).
  z <- lapply(by_t, function(sub_df) {
    vals <- rnd(sub_df$value[match(canon$loc, sub_df$loc)], decimals)
    nn <- which(!is.na(vals))
    if (length(nn) < 0.5 * length(vals)) list(i = I(nn - 1L), v = I(vals[nn])) else vals
  }) %>% unname()
  entry <- list(
    id = id, label = label, level = level, unit = unit, decimals = decimals,
    sub = sub, reverse = reverse, times = I(times), z = z
  )
  if (has_note && any(nzchar(df$note))) {
    # Notes are a small text table (entry$noteTbl) plus, per time slice, integer
    # codes into it (0 = no note), dense or sparse like z. Repeating the text
    # itself per location made county maps (state-level-estimate flags) huge.
    tbl <- sort(unique(df$note[nzchar(df$note)]))
    entry$noteTbl <- I(tbl)
    entry$extra <- lapply(by_t, function(sub_df) {
      codes <- match(sub_df$note[match(canon$loc, sub_df$loc)], tbl)
      codes[is.na(codes)] <- 0L
      nn <- which(codes > 0L)
      if (length(nn) < 0.5 * length(codes)) list(i = I(nn - 1L), v = I(codes[nn])) else codes
    }) %>% unname()
  }
  if (!is.null(tags)) entry$tags <- tags
  entry
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
#
# x values are not repeated per line: each line carries integer indices into a
# per-chart table (cfg.xs) that `emit_chart()` flushes from `.xreg`. Notes are
# sparse ({index: text}) since almost every point has none.
.xreg <- new.env()
.xreg$vals <- character(0)

build_lines <- function(df, dim_cols, series_col, x_col, y_col, note_col = NULL,
                         lower_col = NULL, upper_col = NULL, digits = 2) {
  d <- df
  d$.x <- as.character(d[[x_col]])
  d$.y <- rnd(d[[y_col]], digits)
  has_note <- !is.null(note_col)
  if (has_note) { n <- d[[note_col]]; d$.note <- ifelse(is.na(n), "", n) }
  has_range <- !is.null(lower_col) && !is.null(upper_col)
  if (has_range) {
    err_down <- d[[y_col]] - d[[lower_col]]
    err_up   <- d[[upper_col]] - d[[y_col]]
    d$.errDown <- ifelse(is.na(err_down), 0, rnd(err_down, digits))
    d$.errUp   <- ifelse(is.na(err_up), 0, rnd(err_up, digits))
  }
  d$.series <- as.character(d[[series_col]])
  .xreg$vals <- c(.xreg$vals, setdiff(unique(d$.x), .xreg$vals))
  d$.xi <- match(d$.x, .xreg$vals) - 1L
  key_cols <- c(dim_cols, ".series")
  d <- d %>% arrange(across(all_of(key_cols)), .x)
  grp <- do.call(paste, c(d[key_cols], sep = ""))
  split_idx <- split(seq_len(nrow(d)), grp, drop = TRUE)
  lapply(split_idx, function(idx) {
    sub <- d[idx, , drop = FALSE]
    dims <- as.list(sub[1, dim_cols, drop = FALSE])
    out <- list(dims = dims, series = sub$.series[1], x = I(sub$.xi), y = I(sub$.y))
    if (has_note) {
      w <- which(nzchar(sub$.note))
      if (length(w)) out$note <- as.list(setNames(sub$.note[w], w - 1L))
    }
    if (has_range) { out$errUp <- I(sub$.errUp); out$errDown <- I(sub$.errDown) }
    out
  }) %>% unname()
}



# ---------------------------------------------------------------------------
# Flu bundle data (read once)
# ---------------------------------------------------------------------------
flu_overall <- read_parquet("dist/flu_overall_trends.parquet")
flu_age     <- read_parquet("dist/flu_trends_by_age.parquet")
flu_county  <- read_parquet("dist/flu_ed_visits_by_county.parquet")
flu_hosp    <- read_parquet("dist/flu_hospital_capacity.parquet")
flu_rtm     <- read_parquet("dist/flu_rt_and_mortality.parquet")
flu_vax     <- read_parquet("dist/flu_vax_coverage.parquet")
flu_vax_sub <- read_parquet("dist/flu_vax_substate.parquet")
flu_doses   <- read_parquet("dist/flu_vax_doses.parquet")

# Bundle measure labels / units (from measure_info.json, one entry per level)
mi <- jsonlite::read_json("measure_info.json")
level_info <- function(key, field) {
  lv <- mi[[key]]$levels
  vapply(lv, function(x) { v <- x[[field]]; if (is.null(v)) "" else v }, character(1))
}
hosp_label <- level_info("bundle_flu/dist/flu_hospital_capacity.parquet|measure", "short_name")
hosp_unit  <- level_info("bundle_flu/dist/flu_hospital_capacity.parquet|measure", "unit")
rtm_label  <- level_info("bundle_flu/dist/flu_rt_and_mortality.parquet|measure", "short_name")
rtm_unit   <- level_info("bundle_flu/dist/flu_rt_and_mortality.parquet|measure", "unit")

# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
chr_date <- function(x) as.character(as.Date(x))

# Age labels in natural order, "Total" first
age_sort <- function(v) {
  v <- unique(v)
  num <- suppressWarnings(as.numeric(sub("^[^0-9]*([0-9]+).*", "\\1", v)))
  num[v == "Total"] <- -1
  v[order(num, v, na.last = TRUE)]
}

# National first, then alphabetical (works for state names and HHS regions)
#
# US territories other than Puerto Rico are dropped from every location
# dropdown (50 states + DC + Puerto Rico are kept).
non_us_territories <- c("American Samoa", "Guam", "Northern Mariana Islands",
                        "U.S. Virgin Islands", "Virgin Islands", "United States Virgin Islands")
geo_options <- function(g, first = "United States") {
  g <- setdiff(unique(g), non_us_territories)
  c(intersect(first, g), sort(setdiff(g, first)))
}

empty_obj <- function() setNames(list(), character(0))

# "Compare by" block that only picks the legend entries for the selected
# source: renderLineChart reads l.dims[series_col] when compareBy is set, and
# bySource[<source filter value>] supplies the series that exist for it.
#
# The dropdown is always hidden (the series is fixed); `default_on` is an
# optional named list {source: series to show initially} -- everything else
# stays available through the legend toggles.
by_source_compare <- function(df, source_col, series_col, label = series_col, sorter = sort, default_on = list()) {
  srcs <- sort(unique(df[[source_col]]))
  by_src <- setNames(lapply(srcs, function(s) {
    ord <- as.character(sorter(df[[series_col]][df[[source_col]] == s]))
    g <- list(fixed = empty_obj(), seriesOrder = I(ord), seriesMeta = empty_obj())
    if (!is.null(default_on[[s]])) g$defaultOn <- I(intersect(default_on[[s]], ord))
    g
  }), srcs)
  groups <- setNames(list(list(label = label, bySource = by_src)), series_col)
  list(label = "Compare by", default = series_col, sourceKey = source_col, groups = groups, hidden = TRUE)
}

# The same dims/series lines, repeated for raw / smoothed / scaled values and
# tagged with a `display` dimension so one dropdown switches between them.
display_variants <- c(raw = "value", smooth = "value_smooth", scaled = "value_smooth_scale")
display_options <- list(
  list(value = "scaled", label = "Scaled 0-100 (compare sources)"),
  list(value = "smooth", label = "3-week average, native units"),
  list(value = "raw", label = "Weekly value, native units")
)
build_display_lines <- function(df, dim_cols, series_col, variants = names(display_variants), digits = 1) {
  out <- lapply(intersect(variants, "raw"), function(v) {
    d <- df
    d$display <- v
    d$yy <- d[[display_variants[[v]]]]
    d <- d %>% filter(!is.na(yy))
    d$xx <- chr_date(d$date)
    d$note <- ifelse(!is.na(d$suppressed_flag) & d$suppressed_flag == 1, " (suppressed - imputed)", "")
    build_lines(d, c(dim_cols, "display"), series_col, "xx", "yy", note_col = "note", digits = digits)
  })
  # "smooth" and "scaled" share one line object (see build_q_lines)
  if (length(intersect(variants, c("smooth", "scaled")))) {
    out <- c(out, list(build_q_lines(df, dim_cols, series_col)))
  }
  unlist(out, recursive = FALSE)
}

# One line object serves BOTH the "smooth" (native units) and "scaled" (0-100)
# displays, instead of storing two near-identical arrays. value_smooth_scale is
# min-max of value_smooth within the series (see add_trend_measures() in
# build.R), so each point is stored once as an integer q = (v - lo) / step and
# the browser derives:
#   smooth = lo + q * step          scaled = q * step / rng * 100
# step = min(0.05, rng / 10000): native values are exact to +/-0.025 (finer
# than the 1-decimal hover) and scaled values to well under 0.01. A constant
# series (rng = 0) has no scaled version in the source, so it only advertises
# the "smooth" mode.
build_q_lines <- function(df, dim_cols, series_col) {
  d <- df %>% filter(!is.na(value_smooth))
  d$.x <- chr_date(d$date)
  d$.note <- ifelse(!is.na(d$suppressed_flag) & d$suppressed_flag == 1, " (suppressed - imputed)", "")
  d$.series <- as.character(d[[series_col]])
  .xreg$vals <- c(.xreg$vals, setdiff(unique(d$.x), .xreg$vals))
  d$.xi <- match(d$.x, .xreg$vals) - 1L
  key_cols <- c(dim_cols, ".series")
  d <- d %>% arrange(across(all_of(key_cols)), .x)
  split_idx <- split(seq_len(nrow(d)), as.list(d[key_cols]), drop = TRUE, sep = "\r")
  lapply(split_idx, function(idx) {
    sub <- d[idx, , drop = FALSE]
    v <- sub$value_smooth
    lo <- min(v); rng <- max(v) - lo
    step <- if (rng > 0) min(0.05, rng / 10000) else 0.05
    out <- list(dims = as.list(sub[1, dim_cols, drop = FALSE]), series = sub$.series[1],
                x = I(sub$.xi), q = I(as.integer(round((v - lo) / step))),
                lo = signif(lo, 10), step = signif(step, 10), rng = signif(rng, 10),
                qModes = I(if (rng > 0) c("smooth", "scaled") else "smooth"))
    w <- which(nzchar(sub$.note))
    if (length(w)) out$note <- as.list(setNames(sub$.note[w], w - 1L))
    out
  }) %>% unname()
}

# Emit config + container + render call + "About this chart" note in one go.
# Call from a chunk with `#| results: asis`. `about` is trusted HTML.
# Plain-language reporting caveats and typical data lag, per source (from the
# flu source-summary spreadsheet). Shown in each chart's details drawer.
source_caveats <- list(
  nssp = list(name = "CDC NSSP emergency department visits", lag = "About 1 week",
    text = "Counts visits, not people, and a diagnosis code does not always mean a lab test was done. County values are for groups of counties (Health Service Areas); where a state reports none, PopHIVE repeats the state value and flags it. Data begin October 2022."),
  epic = list(name = "Epic Cosmos emergency department visits", lag = "Usually 3 to 5 weeks, because exports are loaded in batches every few weeks. Previously reported data may also change, as data are updated when new hospitals are added to the Epic system",
    text = "Not a random sample; coverage depends on which health systems in a state use Epic. Counts under 10 are suppressed and filled with an estimate (flagged). Cite as research performed with Epic Cosmos obtained through PopHIVE."),
  respnet = list(name = "CDC RESP-NET hospitalizations", lag = "About 2 weeks. Recent weeks are revised upward as late reports arrive",
    text = "Counts only patients who received a test and tested positive, so CDC says the true burden is higher. A state's rate reflects its surveillance counties, not the whole state. National flu rates begin with the 2018-19 season."),
  nhsn = list(name = "CDC NHSN hospital admissions and bed capacity", lag = "About 1 week. Hospitals can revise earlier weeks later, so previously reported numbers may vary",
    text = "Reporting was voluntary from May to October 2024, so that period is an undercount. Each row carries the share of hospitals reporting. Covers beds, not staff or staffing hours."),
  ilinet = list(name = "CDC ILINet outpatient visits", lag = "About 1 week",
    text = "Measures symptoms (fever with cough or sore throat), not confirmed flu, so it rises with any respiratory virus."),
  kinsa = list(name = "Kinsa smart-thermometer illness signal", lag = "About 1 day",
    text = "Not a diagnosis and not a representative sample. It is an early signal of fever and symptoms, not a count of flu cases."),
  nwss = list(name = "CDC NWSS wastewater (influenza A)", lag = "About 1 week. Earlier weeks can change as late reports arrive",
    text = "Shows whether virus is rising or falling, not how many people are sick. Influenza A only. Rural areas and homes on septic systems are underrepresented."),
  fluvaxview = list(name = "CDC FluVaxView end-of-season vaccination coverage", lag = "A year or more; the newest season is 2024-25. Use NIS-Flu for the current season",
    text = "Self-reported, not verified against records. Children and adults come from different surveys. County data cover adults only and end in 2022."),
  nis = list(name = "CDC NIS-Flu weekly vaccination coverage", lag = "About 1 to 2 weeks",
    text = "Preliminary in-season estimates with wide confidence intervals in small states. Self-report tends to overstate coverage."),
  iis = list(name = "CDC immunization registry (IIS) monthly coverage", lag = "About 1 month",
    text = "Registry completeness varies by state, especially for adults, so a low value can mean missing records. There is no national estimate. New York and Pennsylvania exclude New York City and Philadelphia."),
  medicare = list(name = "CMS Medicare flu vaccination claims", lag = "Several weeks, for claims to process",
    text = "Vaccines not billed to Medicare (for example, at workplaces) are missed. Says nothing about Medicare Advantage enrollees or people under 65."),
  iqvia = list(name = "IQVIA flu vaccinations administered to adults", lag = "About 1 to 2 weeks for pharmacies; physician-office counts may be revised, so previously reported numbers may vary",
    text = "Counts doses, not people, and cannot be turned into a coverage rate. Attribute to CDC FluVaxView and note the IQVIA source."),
  rt = list(name = "CDC Epidemic Trends and Rt", lag = "A few days",
    text = "Rt above 1 means flu is growing; below 1 means it is shrinking. PopHIVE keeps only the latest model run (about six months). CDC changed models on June 1, 2026."),
  nchs = list(name = "CDC NCHS flu and pneumonia death rate", lag = "About 9 to 12 months",
    text = "Flu and pneumonia are combined, and flu is widely undercounted on death certificates. Reported data are provisional and may be revised."),
  nndss = list(name = "CDC NNDSS flu-related reports", lag = "About 1 week, but counts may be revised as late reports arrive",
    text = "Reported cases only; completeness differs by state because states are not required to report to CDC. Seasonal flu cases are not notifiable, only pediatric deaths and novel influenza A strains.")
)
chart_sources <- list(
  "act-map" = c("nssp", "epic", "respnet", "nhsn", "ilinet", "nwss"),
  "act-ts" = c("nssp", "epic", "respnet", "nhsn", "ilinet", "nwss", "kinsa"),
  "rt-ts" = "rt",
  "age-map" = c("nhsn", "epic"),
  "age-ts" = c("nhsn", "epic", "respnet"),
  "hosp-map" = "nhsn", "hosp-ts" = "nhsn",
  "death-map" = c("nchs", "nndss"), "death-ts" = c("nchs", "nndss"),
  "vax-map" = c("fluvaxview", "nis", "iis", "medicare"),
  "vax-ts" = c("fluvaxview", "nis", "iis", "medicare", "iqvia"),
  "vax-race" = "fluvaxview",
  "vax-nis" = "nis"
)
caveats_html <- function(id) {
  keys <- chart_sources[[id]]
  if (is.null(keys)) return("")
  items <- vapply(keys, function(k) {
    s <- source_caveats[[k]]
    sprintf("<li><b>%s.</b> %s <em>Data lag: %s.</em></li>", s$name, s$text, s$lag)
  }, character(1))
  sprintf("<ul>%s</ul>", paste(items, collapse = ""))
}

emit_chart <- function(id, kind = c("Choropleth", "LineChart"), cfg, about) {
  kind <- match.arg(kind)
  nm <- toupper(gsub("-", "_", id))
  force(cfg)  # evaluates build_lines() calls, which fill .xreg
  if (length(.xreg$vals)) cfg$xs <- I(.xreg$vals)
  .xreg$vals <- character(0)
  emit_json(nm, cfg)
  cat(sprintf('<div class="ph-card" id="%s"></div>\n<script>PH.render%s(\'%s\', window.%s);</script>\n',
              id, kind, id, nm))
  cat(sprintf('<details class="chart-info">\n<summary>About this chart</summary>\n<p>%s</p>\n</details>\n', about))
  cav <- caveats_html(id)
  if (nzchar(cav)) cat(sprintf('<details class="chart-info chart-caveats">\n<summary>Caveats</summary>\n%s\n</details>\n', cav))
}
