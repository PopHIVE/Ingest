# =============================================================================
# Area Health Resource File (AHRF) Multi-Year Data Ingestion
# Source: HRSA Area Health Resource File — https://data.hrsa.gov/data/download
#
# Editions covered:
#   1999:        AHRF1999.zip (data) + AHRF_USER_TECH_1999-2000.zip (layout.txt)
#   2000-2006:   AHRF{Y}.zip  (data) + AHRF_USER_TECH_{Y}-{Y+1}.zip (SAS format)
#   2007-2008:   AHRF{Y}.zip  (data) + AHRF_USER_TECH_{Y}-{Y+1}.zip (layout.txt)
#   2009,2011-2017: ahrf{Y}.asc (direct) + AHRF_USER_TECH_{...}.zip (SAS format)
#   2018-2022:   AHRF_{Y-1}-{Y}.zip  (data + embedded SAS)
#   2023-2025:   CSV component files in zip
#
# Notes:
#   - 2010, 2013: no data file on HRSA servers → skipped
# =============================================================================

library(dplyr)
library(vroom)
library(readr)

`%||%` <- function(a, b) if (!is.null(a)) a else b

# Initialize process record (process.json is created by dcf::dcf_add_source())
process <- dcf::dcf_process_record()

# ---------------------------------------------------------------------------
# Helper: find a file by pattern within a directory (recursive, case-insensitive)
# ---------------------------------------------------------------------------
find_file <- function(dir, pattern) {
  hits <- list.files(dir, pattern = pattern, recursive = TRUE,
                     full.names = TRUE, ignore.case = TRUE)
  if (length(hits)) hits[1] else NULL
}

# ---------------------------------------------------------------------------
# 1. Edition manifest
# ---------------------------------------------------------------------------
base_url   <- "https://data.hrsa.gov/DataDownload/AHRF"
static_url <- "https://data.hrsa.gov/DataDownload/StaticDocuments"

editions <- list(
  # --- 1999: old-style zip + tech doc with pipe-delimited layout.txt ---
  list(yr = 1999L, type = "old_zip",
       data_url   = paste0(base_url, "/AHRF1999.zip"),
       data_local = "raw/AHRF1999.zip",
       tech_url   = paste0(base_url, "/AHRF_USER_TECH_1999-2000.zip"),
       tech_local = "raw/AHRF_USER_TECH_1999-2000.zip"),

  # --- 2000-2006: old-style zip + separate tech doc zip (SAS format) ---
  list(yr = 2000L, type = "old_zip",
       data_url   = paste0(base_url, "/AHRF2000.zip"),
       data_local = "raw/AHRF2000.zip",
       tech_url   = paste0(base_url, "/AHRF_USER_TECH_2000-2001.zip"),
       tech_local = "raw/AHRF_USER_TECH_2000-2001.zip"),
  list(yr = 2001L, type = "old_zip",
       data_url   = paste0(base_url, "/AHRF2001.zip"),
       data_local = "raw/AHRF2001.zip",
       tech_url   = paste0(base_url, "/AHRF_USER_TECH_2001-2002.zip"),
       tech_local = "raw/AHRF_USER_TECH_2001-2002.zip"),
  list(yr = 2002L, type = "old_zip",
       data_url   = paste0(base_url, "/AHRF2002.zip"),
       data_local = "raw/AHRF2002.zip",
       tech_url   = paste0(base_url, "/AHRF_USER_TECH_2002-2003.zip"),
       tech_local = "raw/AHRF_USER_TECH_2002-2003.zip"),
  list(yr = 2003L, type = "old_zip",
       data_url   = paste0(base_url, "/AHRF2003.zip"),
       data_local = "raw/AHRF2003.zip",
       tech_url   = paste0(base_url, "/AHRF_USER_TECH_2003-2004.zip"),
       tech_local = "raw/AHRF_USER_TECH_2003-2004.zip"),
  list(yr = 2004L, type = "old_zip",
       data_url   = paste0(base_url, "/AHRF2004.zip"),
       data_local = "raw/AHRF2004.zip",
       tech_url   = paste0(base_url, "/AHRF_USER_TECH_2004-2005.zip"),
       tech_local = "raw/AHRF_USER_TECH_2004-2005.zip"),
  list(yr = 2005L, type = "old_zip",
       data_url   = paste0(base_url, "/AHRF2005.zip"),
       data_local = "raw/AHRF2005.zip",
       tech_url   = paste0(base_url, "/AHRF_USER_TECH_2005-2006.zip"),
       tech_local = "raw/AHRF_USER_TECH_2005-2006.zip"),
  list(yr = 2006L, type = "old_zip",
       data_url   = paste0(base_url, "/AHRF2006.zip"),
       data_local = "raw/AHRF2006.zip",
       tech_url   = paste0(base_url, "/AHRF_USER_TECH_2006-2007.zip"),
       tech_local = "raw/AHRF_USER_TECH_2006-2007.zip"),

  # --- 2007-2008: old-style zip + tech doc with layout.txt (no SAS) ---
  list(yr = 2007L, type = "old_zip",
       data_url   = paste0(base_url, "/AHRF2007.zip"),
       data_local = "raw/AHRF2007.zip",
       tech_url   = paste0(base_url, "/AHRF_USER_TECH_2007-2008.zip"),
       tech_local = "raw/AHRF_USER_TECH_2007-2008.zip"),
  list(yr = 2008L, type = "old_zip",
       data_url   = paste0(base_url, "/AHRF2008.zip"),
       data_local = "raw/AHRF2008.zip",
       tech_url   = paste0(base_url, "/AHRF_USER_TECH_2008-2009.zip"),
       tech_local = "raw/AHRF_USER_TECH_2008-2009.zip"),

  # --- Direct ASC file + separate tech doc zip ---
  # Note: 2010 (404) and 2013 (404) do not exist on HRSA servers
  list(yr = 2009L, type = "direct_asc",
       data_url   = paste0(base_url, "/ahrf2009.asc"),
       data_local = "raw/ahrf2009.asc",
       tech_url   = paste0(base_url, "/AHRF_USER_TECH_2009-2010.zip"),
       tech_local = "raw/AHRF_USER_TECH_2009-2010.zip"),
  list(yr = 2011L, type = "direct_asc",
       data_url   = paste0(base_url, "/ahrf2011.asc"),
       data_local = "raw/ahrf2011.asc",
       tech_url   = paste0(base_url, "/AHRF_USER_TECH_2011-2012.zip"),
       tech_local = "raw/AHRF_USER_TECH_2011-2012.zip"),
  list(yr = 2012L, type = "direct_asc",
       data_url   = paste0(base_url, "/ahrf2012.asc"),
       data_local = "raw/ahrf2012.asc",
       tech_url   = paste0(base_url, "/AHRF_USER_TECH_2012-2013.zip"),
       tech_local = "raw/AHRF_USER_TECH_2012-2013.zip"),
  list(yr = 2014L, type = "direct_asc",
       data_url   = paste0(base_url, "/ahrf2014.asc"),
       data_local = "raw/ahrf2014.asc",
       tech_url   = paste0(base_url, "/AHRF_USER_TECH_2013-2014.zip"),
       tech_local = "raw/AHRF_USER_TECH_2013-2014.zip"),
  list(yr = 2015L, type = "direct_asc",
       data_url   = paste0(base_url, "/ahrf2015.asc"),
       data_local = "raw/ahrf2015.asc",
       tech_url   = paste0(base_url, "/AHRF_USER_TECH_2014-2015.zip"),
       tech_local = "raw/AHRF_USER_TECH_2014-2015.zip"),
  list(yr = 2016L, type = "direct_asc",
       data_url   = paste0(base_url, "/ahrf2016.asc"),
       data_local = "raw/ahrf2016.asc",
       tech_url   = paste0(base_url, "/AHRF_USER_TECH_2015-2016.zip"),
       tech_local = "raw/AHRF_USER_TECH_2015-2016.zip"),
  list(yr = 2017L, type = "direct_asc",
       data_url   = paste0(base_url, "/ahrf2017.asc"),
       data_local = "raw/ahrf2017.asc",
       tech_url   = paste0(base_url, "/AHRF_USER_TECH_2016-2017.zip"),
       tech_local = "raw/AHRF_USER_TECH_2016-2017.zip"),

  # --- Range zip (data .asc + embedded .sas) ---
  list(yr = 2018L, type = "asc_zip",
       data_url   = paste0(base_url, "/AHRF_2017-2018.zip"),
       data_local = "raw/AHRF_2017-2018.zip"),
  list(yr = 2019L, type = "asc_zip",
       data_url   = paste0(base_url, "/AHRF_2018-2019.zip"),
       data_local = "raw/AHRF_2018-2019.zip"),
  list(yr = 2020L, type = "asc_zip",
       data_url   = paste0(base_url, "/AHRF_2019-2020.zip"),
       data_local = "raw/AHRF_2019-2020.zip"),
  list(yr = 2021L, type = "asc_zip",
       data_url   = paste0(base_url, "/AHRF_2020-2021.zip"),
       data_local = "raw/AHRF_2020-2021.zip"),
  list(yr = 2022L, type = "asc_zip",
       data_url   = paste0(base_url, "/AHRF_2021-2022.zip"),
       data_local = "raw/AHRF_2021-2022.zip"),

  # --- CSV component zip ---
  # yr = final year of range (matches filenames inside: ahrf{yr}*.csv)
  list(yr = 2023L, type = "csv_zip",
       data_url   = paste0(static_url, "/AHRF_CSV_2022-2023.zip"),
       data_local = "raw/AHRF_CSV_2022-2023.zip"),
  list(yr = 2024L, type = "csv_zip",
       data_url   = paste0(static_url, "/AHRF%202023-2024%20CSV.zip"),
       data_local = "raw/AHRF_2023-2024_CSV.zip"),
  list(yr = 2025L, type = "csv_zip",
       data_url   = paste0(base_url, "/AHRF_2024-2025_CSV.zip"),
       data_local = "raw/AHRF_2024-2025_CSV.zip")
)

# ---------------------------------------------------------------------------
# 2. Download raw files (data + tech docs where applicable)
# ---------------------------------------------------------------------------
safe_download <- function(url, dest) {
  if (file.exists(dest)) return(invisible(NULL))
  dir.create(dirname(dest), showWarnings = FALSE, recursive = TRUE)
  tryCatch(
    suppressWarnings(download.file(url, dest, mode = "wb", quiet = TRUE)),
    error = function(e) {
      message("Could not download ", basename(dest))
      if (file.exists(dest)) file.remove(dest)
    }
  )
}

for (ed in editions) {
  safe_download(ed$data_url, ed$data_local)
  if (!is.null(ed$tech_url)) safe_download(ed$tech_url, ed$tech_local)
}

raw_files    <- sort(list.files("raw", full.names = TRUE, recursive = FALSE))
current_hash <- list(hash = unname(tools::md5sum(raw_files)))

if (!identical(process$raw_state, current_hash)) {

  # -------------------------------------------------------------------------
  # 3. ASCII parser functions
  # -------------------------------------------------------------------------

  # Parse SAS INPUT fixed-width spec: @START  varname  $? WW.D?  /* desc */
  # Returns data.frame(start, varname, width, decimals, desc, year_4digit)
  parse_sas_format <- function(sas_file) {
    lines <- readLines(sas_file, warn = FALSE, encoding = "latin1")
    pat   <- "^\\s*@(\\d+)\\s+(\\S+)\\s+\\$?\\s*(\\d+)\\.(\\d?)\\s*/\\*(.*)\\*/"
    rows  <- lapply(lines, function(ln) {
      m <- regexec(pat, ln, perl = TRUE)
      r <- regmatches(ln, m)[[1]]
      if (length(r) == 0) return(NULL)
      data.frame(start       = as.integer(r[2]),
                 varname     = r[3],
                 width       = as.integer(r[4]),
                 decimals    = if (nchar(r[5]) > 0) as.integer(r[5]) else 0L,
                 desc        = trimws(r[6]),
                 year_4digit = NA_integer_,
                 stringsAsFactors = FALSE)
    })
    do.call(rbind, Filter(Negate(is.null), rows))
  }

  # Parse plain-text layout files (pipe-delimited 1999/2007 or space-delimited 2008).
  # Returns data.frame(start, varname, width, decimals, desc, year_4digit)
  #
  # All three formats share: FIELDNAME  SSSSS-EEEEE  [YYYY]  DESCRIPTION...
  # The field name is anchored at start of line; positions are five-digit pairs.
  parse_layout_txt <- function(layout_file) {
    lines <- readLines(layout_file, warn = FALSE, encoding = "latin1")
    # Match: optional leading space, field name (may have -YY year suffix),
    # optional pipe/space, then 5-digit start - 5-digit end, then rest.
    pat <- "^\\s*([A-Z][\\w-]+?)\\s*\\|?\\s*(\\d{5})-(\\d{5})(.*)"
    rows <- lapply(lines, function(ln) {
      m <- regexec(pat, ln, perl = TRUE)
      r <- regmatches(ln, m)[[1]]
      if (length(r) == 0) return(NULL)
      varname <- r[2]
      start   <- as.integer(r[3])
      end     <- as.integer(r[4])
      rest    <- r[5]

      # Normalize separators: replace pipes with semicolons, collapse whitespace
      rest <- gsub("\\|", ";", rest)
      rest <- gsub("\\s+", " ", trimws(rest))
      rest <- gsub("^[; ]+", "", rest)   # strip leading separators

      # Extract optional 4-digit year at start (e.g. "2006 " or "; 2006 ;")
      year_4digit <- NA_integer_
      ym <- regexec("^(\\d{4})\\s", rest, perl = TRUE)[[1]]
      if (ym[1] > 0) {
        yr_val <- as.integer(substr(rest, ym[2], ym[2] + 3L))
        if (!is.na(yr_val) && yr_val >= 1970L && yr_val <= 2035L) {
          year_4digit <- yr_val
          rest <- gsub("^[; ]+", "", trimws(sub("^\\d{4}\\s+", "", rest)))
        }
      }

      # Detect implied decimals from notation like (.1) (.01) etc.
      decimals <- 0L
      dm <- regexec("\\(\\.([0-9]+)\\)", rest, perl = TRUE)[[1]]
      if (dm[1] > 0) decimals <- nchar(substr(rest, dm[2], dm[2] + attr(dm, "match.length")[2] - 1L))

      data.frame(start       = start,
                 varname     = varname,
                 width       = end - start + 1L,
                 decimals    = decimals,
                 desc        = trimws(rest),
                 year_4digit = year_4digit,
                 stringsAsFactors = FALSE)
    })
    do.call(rbind, Filter(Negate(is.null), rows))
  }

  # Find the variable matching a description pattern.
  # When multiple matches exist (e.g., multi-year editions), pick the one
  # with the highest year. Uses explicit year_4digit column when available
  # (layout.txt format); otherwise extracts 2-digit year from varname (SAS format).
  find_sas_var <- function(fmt, pattern) {
    hits <- fmt[grepl(pattern, fmt$desc, ignore.case = TRUE, perl = TRUE), ]
    if (nrow(hits) == 0) return(NULL)

    if (!is.null(hits$year_4digit) && any(!is.na(hits$year_4digit))) {
      hits$sort_yr <- ifelse(!is.na(hits$year_4digit), hits$year_4digit, -1L)
    } else {
      has_yr <- nchar(hits$varname) >= 8L
      yr_num <- suppressWarnings(as.integer(
        substr(hits$varname, nchar(hits$varname) - 1L, nchar(hits$varname))
      ))
      hits$sort_yr <- ifelse(has_yr & !is.na(yr_num), yr_num, -1L)
    }
    hits[which.max(hits$sort_yr), ]
  }

  # Variable targets: each entry is list(output_column_name, description_regex)
  # Patterns cover both modern SAS naming and older layout.txt naming conventions.
  targets <- list(
    list("fips_st_cnty",             "Header.*FIPS.*St.*Cty|FIPS St.*Cty Code"),
    list("ahrf_hpsa_prim_care",      "HPSA Code.*Primary Care"),
    list("ahrf_hpsa_dental",         "HPSA Code.*Dentists"),
    list("ahrf_hpsa_mental_health",  "HPSA Code.*Mental"),
    list("ahrf_rural_urban_code",    "Rural.Urban Continuum"),
    list("ahrf_pcp",                 "Phys,Primary Care, Patient Care.*Excl"),
    list("ahrf_md_all",              "M\\.D\\..*Total Ptn Care Non|^Total Active M\\.D\\..*Non.Federal"),
    list("ahrf_psych",               "Psychiatry, Total.*Non-Fed;MD"),
    list("ahrf_dentists",            "Dentists w/NPI"),
    list("ahrf_hospitals",           "^Total Number Hospitals"),
    list("ahrf_critical_access_hosp","^# Critical Access Hospitals"),
    list("ahrf_population",          "^Population Estimate"),
    list("ahrf_good_air_pct",        "% Good Air Quality"),
    list("ahrf_pm25",                "Fine Particulate Matter"),
    list("ahrf_pop_density",         "Population Density per Sq Mile"),
    list("ahrf_medicare_per_capita", "Actual Per Capita Medicare Cost"),
    list("ahrf_ed_per_1k_medicare",  "ED Visits per 1K Medicare")
  )

  # Core parser: given an ASC data file and a pre-parsed format data.frame,
  # extract target columns and return a tidy data frame.
  parse_asc_data <- function(asc_path, fmt, yr_num) {
    if (is.null(fmt) || nrow(fmt) == 0) {
      message("  No parseable format for yr=", yr_num); return(NULL)
    }
    vars <- lapply(targets, function(t) {
      v <- find_sas_var(fmt, t[[2]])
      if (is.null(v)) return(NULL)
      v$out_name <- t[[1]]
      v
    })
    vars <- Filter(Negate(is.null), vars)
    if (length(vars) == 0) { message("  No matching vars for yr=", yr_num); return(NULL) }

    vdf <- do.call(rbind, vars)

    result <- readr::read_fwf(
      asc_path,
      readr::fwf_positions(
        start     = vdf$start,
        end       = vdf$start + vdf$width - 1L,
        col_names = vdf$out_name
      ),
      col_types      = readr::cols(.default = readr::col_character()),
      show_col_types = FALSE,
      progress       = FALSE
    )

    for (i in seq_len(nrow(vdf))) {
      nm  <- vdf$out_name[i]
      dec <- vdf$decimals[i]
      if (dec > 0L)
        result[[nm]] <- suppressWarnings(as.numeric(result[[nm]])) / (10^dec)
    }

    result %>%
      mutate(
        fips_raw  = trimws(fips_st_cnty),
        # Pad 4-digit FIPS to 5 chars when raw data omits leading zero
        geography = case_when(
          nchar(fips_raw) == 5L ~ fips_raw,
          nchar(fips_raw) == 4L ~ paste0("0", fips_raw),
          TRUE                  ~ NA_character_
        ),
        time = paste0(yr_num, "-12-31")
      ) %>%
      filter(!is.na(geography)) %>%
      mutate(across(starts_with("ahrf_"), ~ suppressWarnings(as.numeric(.)))) %>%
      select(geography, time, starts_with("ahrf_"))
  }

  # Load format from a tech doc zip: try SAS first, fall back to layout.txt.
  load_fmt_from_zip <- function(tech_zip, yr_num) {
    tmp <- tempfile(); dir.create(tmp)
    on.exit(unlink(tmp, recursive = TRUE))
    tryCatch({
      unzip(tech_zip, exdir = tmp)
      sas_f    <- find_file(tmp, "\\.sas$|\\.SAS\\.txt$|SA\\.DOC\\.txt$")
      layout_f <- if (is.null(sas_f))
        find_file(tmp, "layout\\.txt$|ARF\\d{4}\\.txt$") else NULL
      if (!is.null(sas_f)) {
        parse_sas_format(sas_f)
      } else if (!is.null(layout_f)) {
        parse_layout_txt(layout_f)
      } else {
        message("  No SAS or layout file in tech doc for yr=", yr_num); NULL
      }
    }, error = function(e) { message("  Error loading fmt yr=", yr_num, ": ", e$message); NULL })
  }

  # Wrapper: old-style data zip (.txt inside) + tech doc zip
  parse_old_zip <- function(data_zip, tech_zip, yr_num) {
    tmp1 <- tempfile(); dir.create(tmp1)
    on.exit(unlink(tmp1, recursive = TRUE))
    tryCatch({
      unzip(data_zip, exdir = tmp1)
      data_f <- find_file(tmp1, "\\.(txt|asc)$")
      if (is.null(data_f)) {
        message("  No data file in zip for yr=", yr_num); return(NULL)
      }
      fmt <- load_fmt_from_zip(tech_zip, yr_num)
      if (is.null(fmt) || nrow(fmt) == 0) return(NULL)
      parse_asc_data(data_f, fmt, yr_num)
    }, error = function(e) { message("  Error yr=", yr_num, ": ", e$message); NULL })
  }

  # Wrapper: direct .asc file + tech doc zip
  parse_direct_asc <- function(asc_path, tech_zip, yr_num) {
    fmt <- load_fmt_from_zip(tech_zip, yr_num)
    if (is.null(fmt) || nrow(fmt) == 0) return(NULL)
    tryCatch(
      parse_asc_data(asc_path, fmt, yr_num),
      error = function(e) { message("  Error yr=", yr_num, ": ", e$message); NULL }
    )
  }

  # Wrapper: range zip with embedded .asc + .sas
  parse_asc_zip <- function(zip_path, yr_num) {
    tmp <- tempfile(); dir.create(tmp)
    on.exit(unlink(tmp, recursive = TRUE))
    tryCatch({
      unzip(zip_path, exdir = tmp)
      asc_f <- find_file(tmp, "\\.(asc|txt)$")
      sas_f <- find_file(tmp, "\\.sas$")
      if (is.null(asc_f) || is.null(sas_f)) {
        message("  Missing ASC or SAS in ", basename(zip_path)); return(NULL)
      }
      fmt <- parse_sas_format(sas_f)
      parse_asc_data(asc_f, fmt, yr_num)
    }, error = function(e) { message("  Error yr=", yr_num, ": ", e$message); NULL })
  }

  # -------------------------------------------------------------------------
  # 4. CSV parser (component files with dynamic year-suffix column selection)
  # -------------------------------------------------------------------------
  read_latest_cols <- function(file, base_col_map) {
    if (is.null(file)) return(NULL)
    tryCatch({
      all_names     <- suppressMessages(
        names(vroom(file, n_max = 0, show_col_types = FALSE))
      )
      select_actual <- character(0)
      rename_to     <- character(0)
      for (out_nm in names(base_col_map)) {
        base  <- base_col_map[[out_nm]]
        pat   <- paste0("^", gsub("\\.", "\\\\.", base), "_\\d{2}$")
        cands <- all_names[grepl(pat, all_names, ignore.case = TRUE)]
        if (length(cands) == 0) next
        yrs  <- as.integer(sub(".*_", "", cands))
        best <- cands[which.max(yrs)]
        select_actual <- c(select_actual, best)
        rename_to    <- c(rename_to, out_nm)
      }
      if (length(select_actual) == 0) return(NULL)
      df <- suppressMessages(
        vroom(file, col_select = c("fips_st_cnty", all_of(select_actual)),
              col_types = list(fips_st_cnty = col_character()),
              show_col_types = FALSE)
      )
      names(df)[match(select_actual, names(df))] <- rename_to
      df
    }, error = function(e) NULL)
  }

  parse_csv_zip <- function(zip_path, yr_num) {
    tmp_dir <- tempfile(); dir.create(tmp_dir)
    on.exit(unlink(tmp_dir, recursive = TRUE))
    tryCatch({
      unzip(zip_path, exdir = tmp_dir)

      comp <- function(sfx)
        find_file(tmp_dir, paste0("ahrf", yr_num, sfx, ".*\\.csv$")) %||%
        find_file(tmp_dir, paste0("AHRF", yr_num, sfx, ".*\\.csv$"))

      geo <- read_latest_cols(comp("geo"), c(
        ahrf_hpsa_prim_care     = "hpsa_prim_care",
        ahrf_hpsa_dental        = "hpsa_dent",
        ahrf_hpsa_mental_health = "hpsa_mentl_hlth",
        ahrf_rural_urban_code   = "rural_urban_contnm"
      ))
      hp <- read_latest_cols(comp("hp"), c(
        ahrf_pcp      = "phys_nf_prim_care_pc_exc_rsdt",
        ahrf_md_all   = "md_nf_all_pc",
        ahrf_psych    = "md_nf_psych",
        ahrf_dentists = "dent_npi"
      ))
      hf <- read_latest_cols(comp("hf"), c(
        ahrf_hospitals            = "hosp",
        ahrf_critical_access_hosp = "critcl_access_hosp"
      ))
      pop <- read_latest_cols(comp("pop"), c(ahrf_population = "popn"))
      env <- read_latest_cols(comp("env"), c(
        ahrf_good_air_pct = "good_air_qulty_dys_pct",
        ahrf_pm25         = "annul_partclt_mattr_2_5_avg",
        ahrf_pop_density  = "popn_densty_per_squr_mi"
      ))
      exp <- read_latest_cols(comp("exp"), c(
        ahrf_medicare_per_capita = "actl_per_cap_ffs_cost"
      ))
      utl <- read_latest_cols(comp("utl"), c(
        ahrf_ed_per_1k_medicare = "ed_vists_per_1k_medcr_ffs"
      ))

      parts <- Filter(Negate(is.null), list(geo, hp, hf, pop, env, exp, utl))
      if (length(parts) == 0L) return(NULL)

      Reduce(function(a, b) left_join(a, b, by = "fips_st_cnty"), parts) %>%
        mutate(
          fips_raw  = fips_st_cnty,
          geography = case_when(
            nchar(fips_raw) == 5L ~ fips_raw,
            nchar(fips_raw) == 4L ~ paste0("0", fips_raw),
            TRUE                  ~ NA_character_
          ),
          time = paste0(yr_num, "-12-31")
        ) %>%
        filter(!is.na(geography)) %>%
        select(geography, time, starts_with("ahrf_"))
    }, error = function(e) {
      message("  Error CSV yr=", yr_num, ": ", e$message); NULL
    })
  }

  # -------------------------------------------------------------------------
  # 5. Process each edition
  # -------------------------------------------------------------------------
  year_dfs <- list()

  for (ed in editions) {
    message("Processing yr=", ed$yr, " (", ed$type, ")")
    result <- NULL

    if (ed$type == "old_zip") {
      if (file.exists(ed$data_local) && file.exists(ed$tech_local))
        result <- parse_old_zip(ed$data_local, ed$tech_local, ed$yr)
      else
        message("  Skipping yr=", ed$yr, ": file(s) not downloaded")

    } else if (ed$type == "direct_asc") {
      if (file.exists(ed$data_local) && file.exists(ed$tech_local))
        result <- parse_direct_asc(ed$data_local, ed$tech_local, ed$yr)
      else
        message("  Skipping yr=", ed$yr, ": file(s) not downloaded")

    } else if (ed$type == "asc_zip") {
      if (file.exists(ed$data_local))
        result <- parse_asc_zip(ed$data_local, ed$yr)
      else
        message("  Skipping yr=", ed$yr, ": zip not downloaded")

    } else if (ed$type == "csv_zip") {
      if (file.exists(ed$data_local))
        result <- parse_csv_zip(ed$data_local, ed$yr)
      else
        message("  Skipping yr=", ed$yr, ": zip not downloaded")
    }

    if (!is.null(result) && nrow(result) > 0L) {
      result <- result %>%
        mutate(across(starts_with("ahrf_"), ~ suppressWarnings(as.numeric(.))))
      message("  -> ", nrow(result), " rows")
      year_dfs[[as.character(ed$yr)]] <- result
    } else {
      message("  -> no data")
    }
  }

  # -------------------------------------------------------------------------
  # 6. Combine and write
  # -------------------------------------------------------------------------
  if (length(year_dfs) > 0L) {
    data_standard <- bind_rows(year_dfs) %>% arrange(geography, time)
    vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")
    message("Wrote ", nrow(data_standard), " rows across ", length(year_dfs), " editions")
    process$raw_state <- current_hash
    dcf::dcf_process_record(updated = process)
  } else {
    message("No editions processed — process.json not updated.")
  }
}
