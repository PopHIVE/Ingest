# =============================================================================
# Crisis Text Line (Crisis Trends) Data Ingestion
# Source: https://github.com/PopHIVE/crisis-text-line (PRIVATE repository)
#
# The CTL extract, cell suppression (GaussSuppression), and percent calculation
# all happen in the private crisis-text-line repo, which pulls from CTL's data
# client on its own schedule. Only its published, already-suppressed
# standard/data.csv.gz is fetched here -- nothing is written to raw/ -- and it
# is reshaped to PopHIVE conventions.
#
# Access: the upstream repo is private, so the download needs a GitHub token
# with read access to it, in the environment variable CTL_READ_TOKEN (a
# repository secret in CI; put it in .Renviron to run locally). Alternatively,
# set CTL_LOCAL_REPO to the path of a local clone of crisis-text-line.
# With neither set, the script leaves the existing standard file untouched and
# exits cleanly, so dcf_build() still works for people without access.
#
# Upstream file (long): geography, time (last day of quarter), age, category,
#   percent = topic conversations / conversations with >= 1 issue tag x 100.
#   percent is NA when suppressed and -999 when no tagged conversations exist.
#
# Standard output (wide): one row per (geography, time, age), with
#   ctl_pct_<topic>             percent (0-100)
#   ctl_pct_<topic>_suppressed  1 if the upstream value was suppressed, else 0
# =============================================================================

library(dplyr)
library(tidyr)

process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Fetch the upstream standard file to a temp path (not kept in raw/)
# -----------------------------------------------------------------------------
upstream_path <- "standard/data.csv.gz"
token <- Sys.getenv("CTL_READ_TOKEN")
local_repo <- Sys.getenv("CTL_LOCAL_REPO")
tmp <- tempfile(fileext = ".csv.gz")
on.exit(unlink(tmp), add = TRUE)

fetched <- if (nzchar(token)) {
  tryCatch({
    download.file(
      paste0(
        "https://api.github.com/repos/PopHIVE/crisis-text-line/contents/",
        upstream_path, "?ref=main"
      ),
      tmp,
      mode = "wb", quiet = TRUE, method = "libcurl",
      headers = c(
        Authorization = paste("Bearer", token),
        Accept = "application/vnd.github.raw"
      )
    )
    TRUE
  }, error = function(e) {
    # Fail loudly (rather than skip) so an expired or revoked token shows up
    # as a failed source in the build instead of silently stale data.
    stop("Failed to download crisis-text-line data with CTL_READ_TOKEN ",
         "(token expired, revoked, or missing repo access?): ",
         conditionMessage(e))
  })
} else if (nzchar(local_repo)) {
  local_file <- file.path(local_repo, upstream_path)
  if (!file.copy(local_file, tmp, overwrite = TRUE)) {
    stop("CTL_LOCAL_REPO is set but ", local_file, " could not be read")
  }
  TRUE
} else {
  message(
    "CTL_READ_TOKEN / CTL_LOCAL_REPO not set; skipping crisis_text_line ",
    "download and keeping the existing standard file."
  )
  FALSE
}

raw_state <- if (isTRUE(fetched)) list(data = unname(tools::md5sum(tmp))) else NULL

# -----------------------------------------------------------------------------
# 2. Reshape only if the upstream file changed
# -----------------------------------------------------------------------------
if (!is.null(raw_state) && !identical(process$raw_state, raw_state)) {

  upstream <- vroom::vroom(
    tmp,
    col_types = vroom::cols(
      geography = "c", time = "c", age = "c", category = "c", percent = "d"
    ),
    altrep = FALSE
  )

  topic_slugs <- c(
    "Abuse" = "abuse",
    "Anxiety / Stress" = "anxiety_stress",
    "Bullying" = "bullying",
    "Depression / Sadness" = "depression_sadness",
    "Eating / Body Image" = "eating_body_image",
    "Gender / Sexual Identity" = "gender_sexual_identity",
    "Grief" = "grief",
    "Isolation / Loneliness" = "isolation_loneliness",
    "Relationships" = "relationships",
    "Self-Harm" = "self_harm",
    "Substance Use" = "substance_use",
    "Suicide" = "suicide"
  )
  new_topics <- setdiff(unique(upstream$category), names(topic_slugs))
  if (length(new_topics)) {
    stop("Unmapped crisis-text-line categories: ", paste(new_topics, collapse = ", "))
  }

  # "Missing" state / age are CTL's buckets for conversations without that
  # field. They are not valid PopHIVE geographies / age groups, so they are
  # dropped; those conversations are still counted in the "00" and "Overall"
  # rows.
  data <- upstream %>%
    filter(geography != "Missing", age != "Missing") %>%
    mutate(
      age = if_else(age == "13 or younger", "<14", age),
      measure = paste0("ctl_pct_", unname(topic_slugs[category])),
      suppressed = as.integer(is.na(percent)),
      # -999 = no tagged conversations, so the share is undefined (not suppressed)
      percent = if_else(percent %in% -999, NA_real_, percent)
    )

  # Strata with no conversations at all are absent upstream; complete the
  # geography x time x age x topic grid so every stratum has every quarter.
  # Added cells are NA and not suppressed (nothing to report).
  data <- data %>%
    select(geography, time, age, measure, percent, suppressed) %>%
    complete(geography, time, age, measure, fill = list(suppressed = 0L))

  # QC: suppressed count matches upstream (after dropping "Missing" buckets)
  n_supp_upstream <- upstream %>%
    filter(geography != "Missing", age != "Missing", is.na(percent)) %>%
    nrow()
  if (sum(data$suppressed) != n_supp_upstream) {
    stop("Suppressed cell count changed in reshaping: ",
         sum(data$suppressed), " vs ", n_supp_upstream, " upstream")
  }
  if (any(duplicated(data[c("geography", "time", "age", "measure")]))) {
    stop("Duplicate geography/time/age/topic rows in crisis-text-line data")
  }

  data_standard <- data %>%
    pivot_wider(
      id_cols = c(geography, time, age),
      names_from = measure,
      values_from = c(percent, suppressed),
      names_glue = "{measure}{ifelse(.value == 'suppressed', '_suppressed', '')}"
    ) %>%
    mutate(time = format(as.Date(time), "%Y-%m-%d")) %>%
    arrange(geography, time, age)

  # Each topic's value column followed by its flag
  value_cols <- sort(paste0("ctl_pct_", unname(topic_slugs)))
  data_standard <- data_standard %>%
    select(geography, time, age,
           all_of(as.vector(rbind(value_cols, paste0(value_cols, "_suppressed")))))

  # ---------------------------------------------------------------------------
  # 3. Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
