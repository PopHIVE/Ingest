# =============================================================================
# Crisis Trends Monthly Aggregate Data Ingestion
# Source: CTL Data Engineering, dataset "crisistrends_monthly_aggregate",
#         delivered via the ctl_data_client Python package
#         (https://github.com/CrisisTextLine/ctl-data-client)
#
# Adapted from the standalone crisis-text-line project's ingest.R, trimmed to
# publish only standard/data.csv.gz (that project also produces tag-
# combination and annual-aggregate files and unsuppressed internal QC copies;
# this source deliberately does not). The only other structural difference
# from that project: there, pophive.config.json is `auth_type: "public"` and
# committed to the repo. In THIS repo it is instead kept out of version
# control (see .gitignore) and supplied at runtime from a GitHub Actions
# secret, written to disk as pophive.config.json before the client reads it
# (see step 1).
#
# Raw schema (one row per month/state/age/issue-tag-combination):
#   month_startTime      <date>  first of month
#   state_name           <chr>   full state name; NA = state not reported
#   age                  <chr>   age bin; NA = age not reported (~76% of rows)
#   issue_tags           <chr>   comma-separated tags for the conversation(s);
#                                 NA = no tag recorded
#   count_conversations  <int>   conversation count for that exact combination
#
# Output: standard/data.csv.gz (documented in measure_info.json), one row per
# (geography, time, age), where geography is a state FIPS code, "00" (national
# rollup, including rows with unreported state), or "Missing" (state not
# reported); age is a specific bin, "Overall" (all ages, including rows with
# unreported age), or "Missing" (age not reported). Each crisistrends_<tag>
# column counts conversations mentioning that tag anywhere in issue_tags -- a
# multi-tag conversation (e.g. "Isolation / Loneliness, School") counts
# towards every tag column it mentions, so columns are not mutually exclusive
# and won't sum to a total. The 4 CTL abuse subtype tags (Emotional, Physical,
# Sexual, Unspecified) are collapsed into a single crisistrends_abuse column.
# crisistrends_total counts every conversation (including missing issue tags
# and additional tags not included in variable names). crisistrends_n_tagged
# counts each conversation with >=1 issue_tags once, regardless of how many
# tags it carries. crisistrends_<col>_reported_total (for every column above)
# is the sum of that column's visible (post-suppression) values across the
# real states, excluding "00" and "Missing", for the same month and age
# group -- the denominator for a state's share when a map only plots real
# states.
#
# Statistical disclosure control (step 4, via the GaussSuppression package
# https://cran.r-project.org/package=GaussSuppression), applied to every
# crisistrends_* count: a fully-specific cell (a real geography and a real
# age) is suppressed (NA) if its count is <= LEAF_MAX (4), and any total --
# across state ("00"), age ("Overall"), or issue (crisistrends_n_tagged /
# crisistrends_total) -- is suppressed if its count is <= TOTAL_MAX (24). A
# cell is also suppressed if its calendar-year sum, or its all-time visible
# sum, is <= YEAR_MAX (24). True zeros are left as 0. Complementary
# suppression additionally blanks some otherwise-safe cells if a suppressed
# value could be recomputed from the state/age hierarchy.
#
# The transform runs whenever the raw release changes, an output file is
# missing, TRANSFORM_VERSION below has been bumped, or CTL_FORCE_TRANSFORM is
# set in the environment. Raw data is never written under this source
# directory: each run that needs to transform downloads the dataset's parquet
# to a temp file, reads it, and deletes it before finishing (even on error),
# so nothing raw persists on disk or in git between runs. This means every
# transform run needs pophive.config.json, materialized at runtime from a
# GitHub secret (step 1) -- there is no local archived copy to fall back to.
#
# IMPORTANT: `library(reticulate)` and all Python calls happen BEFORE
# `library(arrow)` is loaded. Loading R's `arrow` package and Python's
# `pyarrow` (pulled in by ctl_data_client) into the same process in the
# opposite order crashes on Windows with "DLL load failed while importing
# lib" (the two bundle incompatible Arrow C++ builds) -- pyarrow-then-arrow
# works fine, arrow-then-pyarrow does not. Keep this order if you edit this
# script.
# =============================================================================

library(reticulate)

# Bump whenever the transform logic below changes, so the standard files are
# regenerated on the next run even if CTL has not published a new release.
TRANSFORM_VERSION <- 3L

# Suppression thresholds (see header comment).
LEAF_MAX <- 4L    # specific geography x specific age cell: suppressed if <= LEAF_MAX
TOTAL_MAX <- 24L  # any total cell: suppressed if <= TOTAL_MAX
YEAR_MAX <- 24L   # calendar-year / all-time visible sum: suppressed if <= YEAR_MAX (unified with TOTAL_MAX)

dataset_id <- "crisistrends_monthly_aggregate"
config_path <- "pophive.config.json"
# Name of the GitHub Actions secret holding pophive.config.json's contents
# (see step 1 and README.md's Setup section).
config_secret_env_var <- "CRISISTRENDS_POPHIVE_CONFIG"
outputs <- c("standard/data.csv.gz")

# Initialize process record
process <- dcf::dcf_process_record()
raw_state <- process$raw_state

# -----------------------------------------------------------------------------
# 1. Check dataset metadata for changes
# -----------------------------------------------------------------------------
# pophive.config.json is issued per-user by CTL Data Engineering. It is not a
# secret in the sense of granting write access or billing (just which
# dataset(s) this project can read), but it is kept out of version control
# here and supplied via the CRISISTRENDS_POPHIVE_CONFIG GitHub secret instead,
# so it is written to disk from that secret before use. It also carries an
# expiration reminder date, so refresh it with CTL Data Engineering and update
# the secret before it lapses. Raw data is never archived locally (see header
# comment), so unlike a typical dcf source there is no fallback for a missing
# config here -- it's required on every run.
if (!file.exists(config_path) && nzchar(Sys.getenv(config_secret_env_var))) {
  writeLines(Sys.getenv(config_secret_env_var), config_path)
}

if (!file.exists(config_path)) {
  stop(
    "Missing ", config_path, " (and ", config_secret_env_var, " is not set). ",
    "Raw data is not archived locally, so this is required on every run. ",
    "See README.md for how to provision the GitHub secret."
  )
}

py_require("git+https://github.com/CrisisTextLine/ctl-data-client.git")
ctl_data_client <- import("ctl_data_client")
client <- ctl_data_client$DataClient$from_config(config_path)

info <- client$dataset_info(dataset_id)
raw_state <- list(release = info$release, refreshed_at = info$refreshed_at)

needs_transform <- !identical(process$raw_state, raw_state) ||
  !all(file.exists(outputs)) ||
  !isTRUE(process$transform_version == TRANSFORM_VERSION) ||
  nzchar(Sys.getenv("CTL_FORCE_TRANSFORM"))

if (needs_transform) {

  library(dplyr)
  library(tidyr)
  library(arrow)
  library(GaussSuppression)

  dir.create("standard", showWarnings = FALSE)

  # Download to a temp directory rather than under this source directory, and
  # remove it once the parquet is read (even on error), so raw data never
  # persists on disk between runs (see header comment).
  raw_tmp_dir <- tempfile("crisistrends_raw_")
  dir.create(raw_tmp_dir)
  on.exit(unlink(raw_tmp_dir, recursive = TRUE), add = TRUE)
  client$download(dataset_id, destination = raw_tmp_dir, overwrite = TRUE)
  data_raw <- arrow::read_parquet(file.path(raw_tmp_dir, paste0(dataset_id, ".parquet")))

  # ---------------------------------------------------------------------------
  # 2. Shared lookups
  # ---------------------------------------------------------------------------

  # State name -> FIPS, via the repo's shared crosswalk (preferred over a
  # hardcoded map / cdlTools::fips(), see CLAUDE.md). all_fips.csv.gz's
  # geography_name is NA for territories, so Puerto Rico (the one territory
  # CTL's data covers) is patched in separately.
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  state_name_fips <- all_fips %>%
    filter(nchar(geography) == 2, !is.na(geography_name)) %>%
    select(geography_name, geography) %>%
    tibble::deframe() %>%
    c(`Puerto Rico` = "72")

  # Crisis Text Line issue tags -> column-name slugs, restricted to the
  # topic tags crisistrends.org's public dashboard surfaces. The 4 abuse
  # subtype tags are handled separately below (`abuse_tags`) and collapsed
  # into a single crisistrends_abuse column, so they are not in this map.
  tag_slugs <- c(
    "Anxiety / Stress" = "anxiety_stress", "Bullying" = "bullying",
    "Depression / Sadness" = "depression_sadness",
    "Eating / Body Image" = "eating_body_image",
    "Gender / Sexual Identity" = "gender_sexual_identity", "Grief" = "grief",
    "Isolation / Loneliness" = "isolation_loneliness",
    "Relationships" = "relationships", "Self-Harm" = "self_harm",
    "Substance Use" = "substance_use", "Suicide" = "suicide"
  )

  # The 4 abuse tag names contain a literal comma, because CTL's abuse tags
  # are their own atomic tags rather than an "Abuse" tag plus a subtype tag
  # (confirmed against the raw data: "Abuse" and the subtypes never occur on
  # their own). That collides with ", ", the delimiter CTL uses to join
  # multiple tags on one conversation, so they are shielded with placeholders
  # before splitting issue_tags on comma.
  abuse_tags <- c(
    "Abuse, Emotional", "Abuse, Physical", "Abuse, Sexual", "Abuse, Unspecified"
  )
  abuse_placeholders <- paste0("ABUSE_TAG_", seq_along(abuse_tags))

  protect_abuse_tags <- function(x) {
    for (i in seq_along(abuse_tags)) {
      x <- gsub(abuse_tags[i], abuse_placeholders[i], x, fixed = TRUE)
    }
    x
  }
  unprotect_abuse_tag <- function(x) {
    for (i in seq_along(abuse_tags)) {
      x[x == abuse_placeholders[i]] <- abuse_tags[i]
    }
    x
  }

  data <- data_raw %>%
    mutate(geography = unname(state_name_fips[state_name]))

  # Last day of month, computed once per distinct month and joined back
  month_lookup <- tibble(month_startTime = unique(data$month_startTime)) %>%
    mutate(
      year  = as.integer(format(month_startTime, "%Y")),
      month = as.integer(format(month_startTime, "%m")),
      next_month_first = as.Date(if_else(
        month == 12L,
        paste0(year + 1L, "-01-01"),
        sprintf("%d-%02d-01", year, month + 1L)
      )),
      time = format(next_month_first - 1, "%Y-%m-%d")
    ) %>%
    select(month_startTime, time)

  data <- data %>% left_join(month_lookup, by = "month_startTime")

  # Expand geography/age to include state+national and specific+"Overall"
  # rollups, PLUS an explicit "Missing" geography/age category (rather than
  # silently dropping unreported values). Each row contributes to 4 cells:
  # (its own geography-or-"Missing", its own age-or-"Missing"), (that
  # geography, "Overall"), ("00", that age-or-"Missing"), and ("00",
  # "Overall") -- so "00" and "Overall" always include unreported rows, and
  # "Missing" gives those rows their own visible bucket too.
  expand_geo_age <- function(df) {
    df <- df %>%
      mutate(
        geo_specific = if_else(is.na(geography), "Missing", geography),
        age_specific = if_else(is.na(age), "Missing", age)
      )
    bind_rows(
      df %>% mutate(out_geography = geo_specific, out_age = age_specific),
      df %>% mutate(out_geography = geo_specific, out_age = "Overall"),
      df %>% mutate(out_geography = "00", out_age = age_specific),
      df %>% mutate(out_geography = "00", out_age = "Overall")
    )
  }

  # Every (geography, time, age) cell that should appear in the output, even
  # if none of the selected tags were mentioned there
  all_keys <- data %>%
    expand_geo_age() %>%
    distinct(geography = out_geography, time, age = out_age)

  # ---------------------------------------------------------------------------
  # 3. Transform to standard wide format
  # ---------------------------------------------------------------------------

  # Per-tag conversation counts. A conversation may carry multiple tags (e.g.
  # "Isolation / Loneliness, School" counts towards both Isolation/Loneliness
  # and School), so tag columns are not mutually exclusive.
  tagged_rows <- data %>%
    filter(!is.na(issue_tags)) %>%
    mutate(issue_tags = protect_abuse_tags(issue_tags)) %>%
    separate_rows(issue_tags, sep = ",\\s*") %>%
    mutate(issue_tags = unprotect_abuse_tag(issue_tags)) %>%
    filter(issue_tags %in% names(tag_slugs)) %>%
    mutate(tag_col = paste0("crisistrends_", unname(tag_slugs[issue_tags])))

  # The 4 abuse subtypes collapse into one crisistrends_abuse column. Checked
  # against the original (un-split) issue_tags string rather than routed
  # through separate_rows like tagged_rows above, so a conversation carrying
  # more than one abuse subtype is still counted once, not once per subtype.
  abuse_rows <- data %>%
    filter(!is.na(issue_tags)) %>%
    filter(Reduce(`|`, lapply(abuse_tags, grepl, x = issue_tags, fixed = TRUE))) %>%
    mutate(tag_col = "crisistrends_abuse")

  # Each row here is already a distinct group of conversations sharing one
  # issue_tags combo, so summing count_conversations over these un-split rows
  # counts every tagged conversation once, no matter how many tags it carries.
  n_tagged_rows <- data %>%
    filter(!is.na(issue_tags)) %>%
    mutate(tag_col = "crisistrends_n_tagged")

  # Every conversation, tagged or not -- the overall denominator.
  total_rows <- data %>%
    mutate(tag_col = "crisistrends_total")

  tags_wide <- bind_rows(tagged_rows, abuse_rows, n_tagged_rows, total_rows) %>%
    expand_geo_age() %>%
    group_by(geography = out_geography, time, age = out_age, tag_col) %>%
    summarize(value = sum(count_conversations), .groups = "drop") %>%
    pivot_wider(names_from = tag_col, values_from = value, values_fill = 0)

  data_standard <- all_keys %>%
    left_join(tags_wide, by = c("geography", "time", "age")) %>%
    mutate(across(starts_with("crisistrends_"), ~ replace_na(., 0))) %>%
    arrange(geography, time, age)

  # ---------------------------------------------------------------------------
  # 4. Statistical disclosure control: suppress small counts
  # ---------------------------------------------------------------------------
  # See the header comment for the rule. Run per crisistrends_* column
  # (they aren't mutually exclusive, so they can't share one hierarchy the way
  # a normal frequency table's categories would) and per month. Counts within a
  # year are checked later.

  tag_cols <- setdiff(names(data_standard), c("geography", "time", "age"))

  # Suppression runs on the leaf-level cells (a real geography-or-"Missing"
  # crossed with a real age-or-"Missing"); GaussSuppressionFromData derives
  # the "00"/"Overall" rollups itself from the hierarchies below, so the
  # resulting flags cover every row of data_standard, rollups included.
  leaf <- data_standard %>% filter(geography != "00", age != "Overall")

  # AutoHierarchies-style two-level hierarchies: every leaf code actually
  # present rolls up into a single total node ("00" / "Overall").
  geo_hierarchy <- data.frame(
    levels = c("@", rep("@@", length(unique(leaf$geography)))),
    codes = c("00", sort(unique(leaf$geography)))
  )
  age_hierarchy <- data.frame(
    levels = c("@", rep("@@", length(unique(leaf$age)))),
    codes = c("Overall", sort(unique(leaf$age)))
  )

  # Custom primary-suppression rule: the threshold depends on whether a cell
  # is a total. `crossTable` reports each output cell's geography/age
  # category, so geography == "00" or age == "Overall" identifies a rollup.
  make_primary_rule <- function(leaf_threshold, margin_threshold) {
    force(leaf_threshold)
    force(margin_threshold)
    function(freq, crossTable, protectZeros = TRUE, ...) {
      threshold <- ifelse(
        crossTable$geography == "00" | crossTable$age == "Overall",
        margin_threshold, leaf_threshold
      )
      primary <- freq <= threshold
      if (!protectZeros) primary[freq == 0] <- FALSE
      primary
    }
  }
  detail_rule <- make_primary_rule(leaf_threshold = LEAF_MAX, margin_threshold = TOTAL_MAX)
  # crisistrends_n_tagged/crisistrends_total are already totals across every
  # issue tag, so all of their cells use the total threshold, not just their
  # "00"/"Overall" rollups.
  total_rule <- make_primary_rule(leaf_threshold = TOTAL_MAX, margin_threshold = TOTAL_MAX)

  # Run suppression per column and month
  suppressed_flags <- function(col, primary_rule) {
    bind_rows(lapply(unique(leaf$time), function(m) {
      out <- GaussSuppressionFromData(
        data = leaf[leaf$time == m, c("geography", "age", col)],
        freqVar = col,
        dimVar = c("geography", "age"),
        hierarchies = list(geography = geo_hierarchy, age = age_hierarchy),
        primary = primary_rule,
        protectZeros = FALSE,
        printInc = FALSE
      )
      out$time <- m
      out[, c("geography", "age", "time", "suppressed")]
    })) %>%
      rename(!!paste0(col, "_suppressed") := suppressed)
  }

  flags <- lapply(tag_cols, function(col) {
    rule <- if (col %in% c("crisistrends_n_tagged", "crisistrends_total")) total_rule else detail_rule
    suppressed_flags(col, rule)
  })
  flags_wide <- Reduce(function(x, y) left_join(x, y, by = c("geography", "age", "time")), flags)

  # Primary suppression for years. Computed against the pre-suppression
  # counts, since an already-suppressed month must still count towards
  # whether its year's total is small. True zero months are left alone.
  year_flags <- data_standard %>%
    mutate(year = substr(time, 1, 4)) %>%
    group_by(geography, age, year) %>%
    mutate(across(all_of(tag_cols), ~ . > 0 & sum(.) <= YEAR_MAX, .names = "{.col}_year_suppressed")) %>%
    ungroup() %>%
    select(geography, age, time, ends_with("_year_suppressed"))

  data_standard <- data_standard %>%
    left_join(flags_wide, by = c("geography", "age", "time")) %>%
    left_join(year_flags, by = c("geography", "age", "time"))

  for (col in tag_cols) {
    is_suppressed <- data_standard[[paste0(col, "_suppressed")]] | data_standard[[paste0(col, "_year_suppressed")]]
    data_standard[[col]][is_suppressed] <- NA
  }
  data_standard <- data_standard %>% select(-ends_with("_suppressed"))

  # Primary suppression for all-time totals. A cell can pass both the month
  # rule and every single year's raw-total rule and still be dangerous once
  # years are combined: each year's raw total being large just means enough
  # OTHER months in that year got suppressed, not that the few months left
  # visible are safe to add up across the whole multi-year history. So this
  # one is computed against data_standard AFTER month+year suppression (the
  # visible counts), not against raw counts.
  alltime_flags <- data_standard %>%
    group_by(geography, age) %>%
    mutate(across(
      all_of(tag_cols),
      ~ !is.na(.) & . > 0 & sum(., na.rm = TRUE) <= YEAR_MAX,
      .names = "{.col}_alltime_suppressed"
    )) %>%
    ungroup() %>%
    select(geography, age, time, ends_with("_alltime_suppressed"))

  data_standard <- data_standard %>%
    left_join(alltime_flags, by = c("geography", "age", "time"))

  for (col in tag_cols) {
    is_suppressed <- data_standard[[paste0(col, "_alltime_suppressed")]]
    data_standard[[col]][is_suppressed] <- NA
  }
  data_standard <- data_standard %>% select(-ends_with("_alltime_suppressed"))

  # ---------------------------------------------------------------------------
  # 4b. Reported-states denominator
  # ---------------------------------------------------------------------------
  # Sum of real states' *visible* (post-suppression) values for a topic/time/
  # age, excluding "00" (the national rollup, which folds in conversations
  # with unreported state) and "Missing" itself. This is the right
  # denominator for a state's % share of a topic when a map only plots real
  # states: dividing by crisistrends_<tag> at geography == "00" directly
  # would understate every state's share by however much came from an
  # unreported state. Suppressed cells (NA) contribute 0 via na.rm, so this
  # never discloses anything beyond what's already published per-state.
  reported_state_totals <- data_standard %>%
    filter(geography != "00", geography != "Missing") %>%
    group_by(time, age) %>%
    summarize(
      across(all_of(tag_cols), ~ sum(., na.rm = TRUE), .names = "{.col}_reported_total"),
      .groups = "drop"
    )

  data_standard <- data_standard %>%
    left_join(reported_state_totals, by = c("time", "age"))

  # ---------------------------------------------------------------------------
  # 5. Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 6. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  process$transform_version <- TRANSFORM_VERSION
  dcf::dcf_process_record(updated = process)
}
