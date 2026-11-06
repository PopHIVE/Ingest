# =============================================================================
# NARMS Data Ingestion
# Source 1: NARMS Now Human Data - Resistance by Agent (Power BI API)
# Source 2: NARMS Now Human Data - Resistance by Pattern (Power BI API)
# Source 3: FDA NARMS Retail Meats (xlsx)
# Source 4: FDA NARMS Animal Pathogen (xlsx)
# Source 5: FDA NARMS Food-Producing Animals (xlsx)
#
# The NARMS Now scrape (sources 1-2) is controlled by the NARMS_NOW_MODE
# environment variable, so the one ingest.R can serve both workflows:
#   standardize  (default) never queries CDC; rebuilds the standard files from
#                the raw files already in raw/. Used by the Tue/Fri build.
#   check        runs a national-only pre-pass (130 queries, ~10 minutes) and
#                compares it, year by year, with the stored national rows. The
#                national counts are sums over states, so only the years whose
#                national rows changed (or a newly appeared year) are then
#                re-scraped for every state, from the earliest changed year to
#                the current one. Earlier years are kept from the stored raw
#                files. When more than FULL_SCRAPE_YEARS years changed (CDC
#                reissues the whole WGS series about once a year) it switches
#                to a full all-years scrape, which is then the cheaper path.
#                Used by the monthly update_narms workflow.
#   full         re-scrapes every year since 1999 for every state (workflow
#                dispatch with force = true, or a local backfill).
# Query cost is server-side and grows with the year range: ~1.5 s for one year,
# ~6 s for all years, and the Power BI backend serves only a few requests at
# once (8 in flight = 3-4x speedup, measured 2026-11-04). Re-scraping one year
# for every state takes ~25 minutes, so a typical monthly update (the current
# year changes almost every time, earlier years occasionally) takes 35-60
# minutes; a full all-years run takes about 3 hours. Any query that still
# fails after retries makes the run fail; the raw files are only replaced when
# every query succeeded.
#
# Local testing without a 40-minute run:
#   NARMS_NOW_LIMIT_ORGANISMS=2 NARMS_NOW_LIMIT_SITES=3   (0 = no limit)
#   NARMS_NOW_OUT_DIR=/some/dir   writes the scraped raw files there and leaves
#                                 process.json untouched
# =============================================================================

library(dplyr)
library(vroom)
library(httr2)
library(jsonlite)
library(tidyr)

# Initialize process record (process.json is created by dcf::dcf_add_source())
process <- dcf::dcf_process_record()

# =============================================================================
# NARMS Now - Resistance by Agent & Pattern (Power BI API)
# =============================================================================

# --- Power BI API Configuration ---
POWERBI_ENDPOINT <- "https://wabi-us-gov-virginia-api.analysis.usgovcloudapi.net/public/reports/querydata?synchronous=true"
POWERBI_RESOURCE_KEY <- "fe9f06d2-5541-43a3-a2fd-ff7cdef7ca7c"
POWERBI_MODEL_ID <- 562153
POWERBI_DATASET_ID <- "79ae757b-89a7-402a-b539-769c6da4ca8e"
POWERBI_REPORT_ID <- "4d3aa8d6-8c28-485a-aa28-9defc6e356ad"
AGENT_VISUAL_ID <- "6ac0e3afaa4ddc521019"
PATTERN_VISUAL_ID <- "f400588f58b922c7b131"

# Concurrency and timeouts. The Power BI backend serves only a few queries at
# once: 8 in flight gave a 3-4x speedup over sequential and 16 gave no more
# (2026-11-04). The 300 s timeout is for the one known slow query (national
# Salmonella nontyphoidal WGS, ~80 s per year), which is also split into
# single-year requests below.
MAX_ACTIVE <- 8L
FALLBACK_MAX_ACTIVE <- 4L
REQ_TIMEOUT_SEC <- 300
# Requests are issued in chunks of this many so that memory and progress
# messages stay bounded (one chunk is about one organism's 52 sites).
CHUNK_SIZE <- 200L
# If at least this fraction of a chunk fails, treat it as an outage: wait and
# retry the whole chunk, up to OUTAGE_MAX_TRIES times (20 minutes in total).
OUTAGE_FAIL_FRACTION <- 0.5
OUTAGE_WAIT_SEC <- 60L
OUTAGE_MAX_TRIES <- 20L
# Year-by-year re-scraping costs ~25 min per year (6,120 single-year queries),
# a full all-years scrape ~3 h flat, so beyond this many changed years the
# full scrape is faster and also stays inside the workflow timeout.
FULL_SCRAPE_YEARS <- 7L

# --- Run mode (see header) ---
NARMS_NOW_MODE <- Sys.getenv("NARMS_NOW_MODE", "standardize")
if (!NARMS_NOW_MODE %in% c("standardize", "check", "full")) {
  stop("NARMS_NOW_MODE must be one of standardize, check, full; got '",
       NARMS_NOW_MODE, "'")
}
NARMS_NOW_OUT_DIR <- Sys.getenv("NARMS_NOW_OUT_DIR", "raw")
is_test_run <- NARMS_NOW_OUT_DIR != "raw"
env_int <- function(name) {
  v <- Sys.getenv(name, "0")
  n <- suppressWarnings(as.integer(v))
  if (is.na(n) || n < 0) stop(name, " must be a non-negative integer; got '", v, "'")
  n
}
limit_organisms <- env_int("NARMS_NOW_LIMIT_ORGANISMS")
limit_sites <- env_int("NARMS_NOW_LIMIT_SITES")
# A limited run only ever covers a subset, so it must not replace the real raw files.
if ((limit_organisms > 0 || limit_sites > 0) && !is_test_run) {
  stop("NARMS_NOW_LIMIT_ORGANISMS / NARMS_NOW_LIMIT_SITES require NARMS_NOW_OUT_DIR ",
       "to point somewhere other than raw/")
}

# --- Organism definitions ---
organisms <- list(
  list(genus = "Campylobacter", species = "coli"),
  list(genus = "Campylobacter", species = "jejuni"),
  list(genus = "E. coli O157", species = "Escherichia coli O157"),
  list(genus = "Non-cholera Vibrio", species = "alginolyticus"),
  list(genus = "Non-cholera Vibrio", species = "fluvialis"),
  list(genus = "Non-cholera Vibrio", species = "harveyi"),
  list(genus = "Non-cholera Vibrio", species = "mimicus"),
  list(genus = "Non-cholera Vibrio", species = "other"),
  list(genus = "Non-cholera Vibrio", species = "parahaemolyticus"),
  list(genus = "Non-cholera Vibrio", species = "vulnificus"),
  list(genus = "Salmonella", species = "All nontyphoidal"),
  list(genus = "Salmonella", species = "All typhoidal"),
  list(genus = "Salmonella", species = "Dublin"),
  list(genus = "Salmonella", species = "Enteritidis"),
  list(genus = "Salmonella", species = "Hadar"),
  list(genus = "Salmonella", species = "Heidelberg"),
  list(genus = "Salmonella", species = "I 4,[5],12:i:-"),
  list(genus = "Salmonella", species = "Infantis"),
  list(genus = "Salmonella", species = "Javiana"),
  list(genus = "Salmonella", species = "Muenchen"),
  list(genus = "Salmonella", species = "Newport"),
  list(genus = "Salmonella", species = "Paratyphi A"),
  list(genus = "Salmonella", species = "Paratyphi B var. L(+) tartrate+"),
  list(genus = "Salmonella", species = "Poona"),
  list(genus = "Salmonella", species = "Saintpaul"),
  list(genus = "Salmonella", species = "Typhi"),
  list(genus = "Salmonella", species = "Typhimurium"),
  list(genus = "Shigella", species = "flexneri"),
  list(genus = "Shigella", species = "other"),
  list(genus = "Shigella", species = "sonnei")
)

test_methods <- c("AST", "WGS")
YEAR_FROM_AST <- 1999
YEAR_FROM_WGS <- 2016
# Ask for everything up to the current year. The model lists a year as soon as
# it exists (2026 was present, with no tested isolates, in November 2026), so a
# trailing year with no tested cells is dropped before the raw files are
# written rather than hard-coding the last complete year here.
YEAR_TO <- as.integer(format(Sys.Date(), "%Y"))

# Queries that time out or return HTTP 500 when asked for their full year
# range. They are issued one year at a time from the start.
SPLIT_PER_YEAR <- list(
  list(site_name = NA_character_, genus = "Salmonella",
       species = "All nontyphoidal", test_method = "WGS", tab = "agent")
)

# --- Site definitions ---
# NARMSSiteName entity values from the Power BI model (51 states + DC)
# The national "All" figures come from queries with no site filter.
sites <- c(
  "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
  "Connecticut", "Delaware", "District Of Columbia", "Florida", "Georgia",
  "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky",
  "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
  "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire",
  "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota",
  "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island",
  "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",
  "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"
)

if (limit_organisms > 0) organisms <- head(organisms, limit_organisms)
if (limit_sites > 0) sites <- head(sites, limit_sites)

# =============================================================================
# Helper Functions
# =============================================================================

#' Join organism name parts, dropping any that are missing
#' paste() would render a missing serotype as the literal string "NA"
#' ("Enterococcus faecalis NA"), so drop absent parts instead of pasting them.
combine_organism <- function(...) {
  parts <- lapply(list(...), function(p) {
    p <- as.character(p)
    p[is.na(p) | p == "NA" | !nzchar(trimws(p))] <- ""
    p
  })
  out <- do.call(paste, c(parts, sep = " "))
  trimws(gsub(" +", " ", out))
}

#' Build a site filter Where clause for Power BI queries
#' Returns NULL if site_name is NULL (no filter = national "All")
build_site_filter <- function(site_name) {
  if (is.null(site_name)) return(NULL)
  list(Condition = list(
    In = list(
      Expressions = list(list(Column = list(
        Expression = list(SourceRef = list(Source = "site")),
        Property = "SiteName"))),
      Values = list(list(list(
        Literal = list(Value = paste0("'", site_name, "'"))
      )))
    )
  ))
}

#' Build a Power BI query for resistance by agent
#' @param site_name NULL for national "All", or a state name like "California"
build_agent_query <- function(genus, species, test_method,
                              site_name = NULL,
                              year_from, year_to) {

  # From clause: include NARMSSiteName only when filtering by site
  from_clause <- list(
    list(Name = "n", Entity = "NARMSAgent", Type = 0L),
    list(Name = "n1", Entity = "NARMSYear", Type = 0L),
    list(Name = "n2", Entity = "NARMSResultAST", Type = 0L),
    list(Name = "n11", Entity = "NARMSTest", Type = 0L),
    list(Name = "n21", Entity = "NARMSLookupGenus", Type = 0L),
    list(Name = "n111", Entity = "NARMSLookupSpecies", Type = 0L)
  )
  if (!is.null(site_name)) {
    from_clause <- c(from_clause, list(
      list(Name = "site", Entity = "NARMSSiteName", Type = 0L)
    ))
  }

  # Where clause: base filters + optional site filter
  where_clause <- list(
    # Exclude null ranks
    list(Condition = list(
      Not = list(Expression = list(
        In = list(
          Expressions = list(
            list(Column = list(
              Expression = list(SourceRef = list(Source = "n")),
              Property = "Rank"))
          ),
          Values = list(list(list(Literal = list(Value = "null"))))
        )
      ))
    )),
    # ShowVibrioAbxAgentTbl filter
    list(
      Condition = list(Comparison = list(
        ComparisonKind = 0L,
        Left = list(Measure = list(
          Expression = list(SourceRef = list(Source = "n")),
          Property = "ShowVibrioAbxAgentTbl")),
        Right = list(Literal = list(Value = "1L"))
      )),
      Target = list(
        list(Column = list(Expression = list(SourceRef = list(Source = "n")),
                           Property = "Rank")),
        list(Column = list(Expression = list(SourceRef = list(Source = "n")),
                           Property = "CLSI Antimicrobial Class")),
        list(Column = list(Expression = list(SourceRef = list(Source = "n")),
                           Property = "Antimicrobial Agent"))
      )
    ),
    # Show SquashReport filter
    list(
      Condition = list(Comparison = list(
        ComparisonKind = 0L,
        Left = list(Measure = list(
          Expression = list(SourceRef = list(Source = "n")),
          Property = "Show SquashReport")),
        Right = list(Literal = list(Value = "1L"))
      )),
      Target = list(
        list(Column = list(Expression = list(SourceRef = list(Source = "n")),
                           Property = "Rank")),
        list(Column = list(Expression = list(SourceRef = list(Source = "n")),
                           Property = "CLSI Antimicrobial Class")),
        list(Column = list(Expression = list(SourceRef = list(Source = "n")),
                           Property = "Antimicrobial Agent"))
      )
    ),
    # Test method filter
    list(Condition = list(
      In = list(
        Expressions = list(
          list(Column = list(
            Expression = list(SourceRef = list(Source = "n11")),
            Property = "TestMethod"))
        ),
        Values = list(list(list(
          Literal = list(Value = paste0("'", test_method, "'"))
        )))
      )
    )),
    # Genus + Species filter
    list(Condition = list(
      In = list(
        Expressions = list(
          list(Column = list(
            Expression = list(SourceRef = list(Source = "n21")),
            Property = "Genus")),
          list(Column = list(
            Expression = list(SourceRef = list(Source = "n111")),
            Property = "SpeciesSerotype"))
        ),
        Values = list(list(
          list(Literal = list(Value = paste0("'", genus, "'"))),
          list(Literal = list(Value = paste0("'", species, "'")))
        ))
      )
    )),
    # Year range filter
    list(Condition = list(
      And = list(
        Left = list(Comparison = list(
          ComparisonKind = 2L,
          Left = list(Column = list(
            Expression = list(SourceRef = list(Source = "n1")),
            Property = "DataYear")),
          Right = list(Literal = list(
            Value = paste0(year_from, "D")))
        )),
        Right = list(Comparison = list(
          ComparisonKind = 4L,
          Left = list(Column = list(
            Expression = list(SourceRef = list(Source = "n1")),
            Property = "DataYear")),
          Right = list(Literal = list(
            Value = paste0(year_to, "D")))
        ))
      )
    ))
  )

  # Append site filter if specified
  site_filter <- build_site_filter(site_name)
  if (!is.null(site_filter)) {
    where_clause <- c(where_clause, list(site_filter))
  }

  list(
    version = "1.0.0",
    queries = list(
      list(
        Query = list(
          Commands = list(
            list(
              SemanticQueryDataShapeCommand = list(
                Query = list(
                  Version = 2L,
                  From = from_clause,
                  Select = list(
                    list(
                      Column = list(Expression = list(SourceRef = list(Source = "n")),
                                    Property = "Rank"),
                      Name = "NARMSAgent.Rank",
                      NativeReferenceName = "Rank"
                    ),
                    list(
                      Column = list(Expression = list(SourceRef = list(Source = "n")),
                                    Property = "CLSI Antimicrobial Class"),
                      Name = "NARMSAgent.CLSI Antimicrobial Class",
                      NativeReferenceName = "CLSI Antimicrobial Class"
                    ),
                    list(
                      Column = list(Expression = list(SourceRef = list(Source = "n")),
                                    Property = "Antimicrobial Agent"),
                      Name = "NARMSAgent.Antimicrobial Agent",
                      NativeReferenceName = "Antimicrobial Agent"
                    ),
                    list(
                      Column = list(Expression = list(SourceRef = list(Source = "n1")),
                                    Property = "DataYear"),
                      Name = "NARMSYear.Year",
                      NativeReferenceName = "Year"
                    ),
                    list(
                      Measure = list(Expression = list(SourceRef = list(Source = "n2")),
                                     Property = "ResistByAgentCell"),
                      Name = "NARMSResultAST.ResistByAgentCell",
                      NativeReferenceName = "ResistByAgentCell"
                    )
                  ),
                  Where = where_clause
                ),
                Binding = list(
                  Primary = list(
                    Groupings = list(
                      list(Projections = list(0L)),
                      list(Projections = list(1L)),
                      list(Projections = list(2L))
                    )
                  ),
                  Secondary = list(
                    Groupings = list(
                      list(Projections = list(3L, 4L))
                    )
                  ),
                  DataReduction = list(
                    DataVolume = 3L,
                    Primary = list(Window = list(Count = 100L)),
                    Secondary = list(Top = list(Count = 100L))
                  ),
                  Version = 1L
                ),
                ExecutionMetricsKind = 1L
              )
            )
          )
        ),
        QueryId = "",
        ApplicationContext = list(
          DatasetId = POWERBI_DATASET_ID,
          Sources = list(list(
            ReportId = POWERBI_REPORT_ID,
            VisualId = AGENT_VISUAL_ID
          ))
        )
      )
    ),
    cancelQueries = list(),
    modelId = POWERBI_MODEL_ID
  )
}

#' Build a Power BI query for resistance by pattern
#' @param site_name NULL for national "All", or a state name like "California"
build_pattern_query <- function(genus, species, test_method,
                                site_name = NULL,
                                year_from, year_to) {

  # From clause: include NARMSSiteName only when filtering by site
  from_clause <- list(
    list(Name = "n1", Entity = "NARMSYear", Type = 0L),
    list(Name = "r", Entity = "NARMSResistancePatternTable", Type = 0L),
    list(Name = "n", Entity = "NARMSResByPatternAST", Type = 0L),
    list(Name = "n2", Entity = "NARMSTest", Type = 0L),
    list(Name = "n11", Entity = "NARMSLookupGenus", Type = 0L),
    list(Name = "n111", Entity = "NARMSLookupSpecies", Type = 0L)
  )
  if (!is.null(site_name)) {
    from_clause <- c(from_clause, list(
      list(Name = "site", Entity = "NARMSSiteName", Type = 0L)
    ))
  }

  # Where clause: base filters + optional site filter
  where_clause <- list(
    # Exclude null Display
    list(Condition = list(
      Not = list(Expression = list(
        In = list(
          Expressions = list(
            list(Column = list(
              Expression = list(SourceRef = list(Source = "r")),
              Property = "Display"))
          ),
          Values = list(list(list(Literal = list(Value = "null"))))
        )
      ))
    )),
    # ShowDisplay filter
    list(
      Condition = list(Comparison = list(
        ComparisonKind = 0L,
        Left = list(Measure = list(
          Expression = list(SourceRef = list(Source = "r")),
          Property = "ShowDisplay")),
        Right = list(Literal = list(Value = "1L"))
      )),
      Target = list(
        list(Column = list(Expression = list(SourceRef = list(Source = "r")),
                           Property = "Display"))
      )
    ),
    # Test method filter
    list(Condition = list(
      In = list(
        Expressions = list(
          list(Column = list(
            Expression = list(SourceRef = list(Source = "n2")),
            Property = "TestMethod"))
        ),
        Values = list(list(list(
          Literal = list(Value = paste0("'", test_method, "'"))
        )))
      )
    )),
    # Genus + Species filter
    list(Condition = list(
      In = list(
        Expressions = list(
          list(Column = list(
            Expression = list(SourceRef = list(Source = "n11")),
            Property = "Genus")),
          list(Column = list(
            Expression = list(SourceRef = list(Source = "n111")),
            Property = "SpeciesSerotype"))
        ),
        Values = list(list(
          list(Literal = list(Value = paste0("'", genus, "'"))),
          list(Literal = list(Value = paste0("'", species, "'")))
        ))
      )
    )),
    # Year range filter
    list(Condition = list(
      And = list(
        Left = list(Comparison = list(
          ComparisonKind = 2L,
          Left = list(Column = list(
            Expression = list(SourceRef = list(Source = "n1")),
            Property = "DataYear")),
          Right = list(Literal = list(
            Value = paste0(year_from, "D")))
        )),
        Right = list(Comparison = list(
          ComparisonKind = 4L,
          Left = list(Column = list(
            Expression = list(SourceRef = list(Source = "n1")),
            Property = "DataYear")),
          Right = list(Literal = list(
            Value = paste0(year_to, "D")))
        ))
      )
    ))
  )

  # Append site filter if specified
  site_filter <- build_site_filter(site_name)
  if (!is.null(site_filter)) {
    where_clause <- c(where_clause, list(site_filter))
  }

  list(
    version = "1.0.0",
    queries = list(
      list(
        Query = list(
          Commands = list(
            list(
              SemanticQueryDataShapeCommand = list(
                Query = list(
                  Version = 2L,
                  From = from_clause,
                  Select = list(
                    list(
                      Column = list(Expression = list(SourceRef = list(Source = "n1")),
                                    Property = "DataYear"),
                      Name = "NARMSYear.Year",
                      NativeReferenceName = "DataYear"
                    ),
                    list(
                      Column = list(Expression = list(SourceRef = list(Source = "r")),
                                    Property = "Display"),
                      Name = "ResistancePatternTable.Display",
                      NativeReferenceName = "Display"
                    ),
                    list(
                      Measure = list(Expression = list(SourceRef = list(Source = "n")),
                                     Property = "ResistancePatternCell"),
                      Name = "NARMSResByPatternAST.ResistancePatternCell",
                      NativeReferenceName = "ResistancePatternCell"
                    )
                  ),
                  Where = where_clause
                ),
                Binding = list(
                  Primary = list(
                    Groupings = list(
                      list(Projections = list(1L))
                    )
                  ),
                  Secondary = list(
                    Groupings = list(
                      list(Projections = list(0L, 2L))
                    )
                  ),
                  DataReduction = list(
                    DataVolume = 3L,
                    Primary = list(Window = list(Count = 100L)),
                    Secondary = list(Top = list(Count = 100L))
                  ),
                  Version = 1L
                ),
                ExecutionMetricsKind = 1L
              )
            )
          )
        ),
        QueryId = "",
        ApplicationContext = list(
          DatasetId = POWERBI_DATASET_ID,
          Sources = list(list(
            ReportId = POWERBI_REPORT_ID,
            VisualId = PATTERN_VISUAL_ID
          ))
        )
      )
    ),
    cancelQueries = list(),
    modelId = POWERBI_MODEL_ID
  )
}

#' Parse a cell value like "4.3%\n(19/446)" or "Not\nTested"
#' Returns a named list: pct_resistant, n_resistant, n_tested
parse_cell_value <- function(cell_text) {
  if (is.null(cell_text) || grepl("Not", cell_text, fixed = TRUE)) {
    return(list(pct_resistant = NA_real_,
                n_resistant = NA_integer_,
                n_tested = NA_integer_))
  }

  pct <- as.numeric(sub("%.*", "", cell_text))
  fraction_match <- regmatches(cell_text, regexpr("\\((\\d+)/(\\d+)\\)", cell_text))

  if (length(fraction_match) > 0 && nchar(fraction_match) > 0) {
    nums <- as.integer(strsplit(gsub("[()]", "", fraction_match), "/")[[1]])
    return(list(pct_resistant = pct,
                n_resistant = nums[1],
                n_tested = nums[2]))
  }

  list(pct_resistant = pct, n_resistant = NA_integer_, n_tested = NA_integer_)
}

#' Parse resistance by agent response into a data frame
parse_agent_response <- function(response, genus, species, test_method) {
  dsr <- response$results[[1]]$result$data$dsr

  # Check for error responses
  if (is.null(dsr$DS)) {
    warning(sprintf("No data returned for %s / %s / %s (agent)", genus, species, test_method))
    return(NULL)
  }

  ds <- dsr$DS[[1]]
  value_dicts <- ds$ValueDicts

  # Get class names (D0), agent names (D1), cell values (D2)
  class_names <- value_dicts$D0
  agent_names <- value_dicts$D1
  cell_values <- value_dicts$D2

  # Get years from secondary header
  sh_key <- names(ds$SH[[1]])[grep("^DM", names(ds$SH[[1]]))]
  years <- sapply(ds$SH[[1]][[sh_key]], function(x) {
    x[[grep("^G", names(x))[1]]]
  })

  # Parse hierarchical primary data
  rows <- list()
  ph_key <- names(ds$PH[[1]])[grep("^DM", names(ds$PH[[1]]))]
  rank_groups <- ds$PH[[1]][[ph_key]]

  # A cell's R flag means "same value as the previous cell in the response",
  # and the previous cell can be the last cell of the previous row (agent), so
  # the carried value has to survive across agents and classes. Resetting it
  # per row turned the first cells of many rows into NA ("Not Tested").
  prev_value <- NULL

  for (rank_group in rank_groups) {
    # rank_val <- rank_group$G0  # Not needed in output

    if (is.null(rank_group$M)) next
    dm1_key <- names(rank_group$M[[1]])[grep("^DM", names(rank_group$M[[1]]))]
    class_groups <- rank_group$M[[1]][[dm1_key]]

    for (class_group in class_groups) {
      class_idx <- class_group[[grep("^G", names(class_group))[1]]]
      class_name <- class_names[[class_idx + 1]]

      if (is.null(class_group$M)) next
      dm2_key <- names(class_group$M[[1]])[grep("^DM", names(class_group$M[[1]]))]
      agent_groups <- class_group$M[[1]][[dm2_key]]

      for (agent_group in agent_groups) {
        agent_idx <- agent_group[[grep("^G", names(agent_group))[1]]]
        agent_name <- agent_names[[agent_idx + 1]]

        if (is.null(agent_group$X)) next
        cells <- agent_group$X

        for (i in seq_along(cells)) {
          cell <- cells[[i]]

          if (!is.null(cell$R)) {
            # R flag means repeat previous value
            cell_text <- prev_value
          } else if (is.character(cell$M0)) {
            # M0 is inline text (not a dict index) — large responses
            cell_text <- cell$M0
            prev_value <- cell_text
          } else {
            m0_idx <- cell$M0
            cell_text <- cell_values[[m0_idx + 1]]
            prev_value <- cell_text
          }

          parsed <- parse_cell_value(cell_text)

          rows[[length(rows) + 1]] <- data.frame(
            year = years[i],
            genus = genus,
            species_serotype = species,
            antimicrobial_class = class_name,
            antimicrobial_agent = agent_name,
            test_method = test_method,
            narms_now_pct_resistant = parsed$pct_resistant,
            narms_now_n_resistant = parsed$n_resistant,
            narms_now_n_tested = parsed$n_tested,
            stringsAsFactors = FALSE
          )
        }
      }
    }
  }

  if (length(rows) == 0) return(NULL)
  do.call(rbind, rows)
}

#' Parse resistance by pattern response into a data frame
parse_pattern_response <- function(response, genus, species, test_method) {
  dsr <- response$results[[1]]$result$data$dsr

  if (is.null(dsr$DS)) {
    warning(sprintf("No data returned for %s / %s / %s (pattern)", genus, species, test_method))
    return(NULL)
  }

  ds <- dsr$DS[[1]]
  value_dicts <- ds$ValueDicts

  # Get pattern names (D0) and cell values (D1)
  pattern_names <- value_dicts$D0
  cell_values <- value_dicts$D1

  # Get years from secondary header
  sh_key <- names(ds$SH[[1]])[grep("^DM", names(ds$SH[[1]]))]
  years <- sapply(ds$SH[[1]][[sh_key]], function(x) {
    x[[grep("^G", names(x))[1]]]
  })

  # Parse primary data (simpler flat structure)
  rows <- list()
  ph_key <- names(ds$PH[[1]])[grep("^DM", names(ds$PH[[1]]))]
  pattern_groups <- ds$PH[[1]][[ph_key]]

  # R = repeat the previous cell in the response, across rows (see
  # parse_agent_response)
  prev_value <- NULL

  for (pattern_group in pattern_groups) {
    pattern_idx <- pattern_group[[grep("^G", names(pattern_group))[1]]]
    pattern_name <- pattern_names[[pattern_idx + 1]]

    if (is.null(pattern_group$X)) next
    cells <- pattern_group$X

    for (i in seq_along(cells)) {
      cell <- cells[[i]]

      if (!is.null(cell$R)) {
        cell_text <- prev_value
      } else if (is.character(cell$M0)) {
        # M0 is inline text (not a dict index) — large responses
        cell_text <- cell$M0
        prev_value <- cell_text
      } else {
        m0_idx <- cell$M0
        cell_text <- cell_values[[m0_idx + 1]]
        prev_value <- cell_text
      }

      parsed <- parse_cell_value(cell_text)

      rows[[length(rows) + 1]] <- data.frame(
        year = years[i],
        genus = genus,
        species_serotype = species,
        pattern = pattern_name,
        test_method = test_method,
        narms_now_pct_resistant = parsed$pct_resistant,
        narms_now_n_resistant = parsed$n_resistant,
        narms_now_n_tested = parsed$n_tested,
        stringsAsFactors = FALSE
      )
    }
  }

  if (length(rows) == 0) return(NULL)
  do.call(rbind, rows)
}

#' Build a corrected DSC query that groups by state using the NARMS fact table.
#' The standard agent query uses NARMSSiteName for state filtering, which causes
#' the ResistByAgentCell numerator to leak national counts for Shigella DSC.
#' This query uses the NARMS entity's SiteName for grouping, which works correctly.
build_dsc_state_query <- function(species, test_method,
                                   year_from, year_to) {
  list(
    version = "1.0.0",
    queries = list(list(
      Query = list(Commands = list(list(
        SemanticQueryDataShapeCommand = list(
          Query = list(
            Version = 2L,
            From = list(
              list(Name = "n", Entity = "NARMS", Type = 0L),
              list(Name = "n1", Entity = "NARMSTest", Type = 0L),
              list(Name = "n11", Entity = "NARMSLookupGenus", Type = 0L),
              list(Name = "n111", Entity = "NARMSLookupSpecies", Type = 0L),
              list(Name = "n2", Entity = "NARMSYear", Type = 0L),
              list(Name = "n3", Entity = "NARMSAgent", Type = 0L),
              list(Name = "r", Entity = "NARMSResultAST", Type = 0L)
            ),
            Select = list(
              list(
                Column = list(Expression = list(SourceRef = list(Source = "n")),
                              Property = "SiteName"),
                Name = "NARMS.SiteName", NativeReferenceName = "SiteName"
              ),
              list(
                Column = list(Expression = list(SourceRef = list(Source = "n2")),
                              Property = "DataYear"),
                Name = "NARMSYear.DataYear", NativeReferenceName = "Year"
              ),
              list(
                Measure = list(Expression = list(SourceRef = list(Source = "r")),
                               Property = "ResistByAgentCell"),
                Name = "NARMSResultAST.ResistByAgentCell",
                NativeReferenceName = "ResistByAgentCell"
              )
            ),
            Where = list(
              list(Condition = list(In = list(
                Expressions = list(list(Column = list(
                  Expression = list(SourceRef = list(Source = "n1")),
                  Property = "TestMethod"))),
                Values = list(list(list(Literal = list(
                  Value = paste0("'", test_method, "'")))))
              ))),
              list(Condition = list(In = list(
                Expressions = list(
                  list(Column = list(Expression = list(SourceRef = list(Source = "n11")),
                                     Property = "Genus")),
                  list(Column = list(Expression = list(SourceRef = list(Source = "n111")),
                                     Property = "SpeciesSerotype"))
                ),
                Values = list(list(
                  list(Literal = list(Value = "'Shigella'")),
                  list(Literal = list(Value = paste0("'", species, "'")))
                ))
              ))),
              list(Condition = list(And = list(
                Left = list(Comparison = list(ComparisonKind = 2L,
                  Left = list(Column = list(Expression = list(SourceRef = list(Source = "n2")),
                                             Property = "DataYear")),
                  Right = list(Literal = list(Value = paste0(year_from, "D"))))),
                Right = list(Comparison = list(ComparisonKind = 4L,
                  Left = list(Column = list(Expression = list(SourceRef = list(Source = "n2")),
                                             Property = "DataYear")),
                  Right = list(Literal = list(Value = paste0(year_to, "D")))))
              ))),
              list(Condition = list(In = list(
                Expressions = list(
                  list(Column = list(Expression = list(SourceRef = list(Source = "n3")),
                                     Property = "SearchType")),
                  list(Column = list(Expression = list(SourceRef = list(Source = "n3")),
                                     Property = "Display")),
                  list(Column = list(Expression = list(SourceRef = list(Source = "n3")),
                                     Property = "Antimicrobial Agent"))
                ),
                Values = list(list(
                  list(Literal = list(Value = "'By Agent'")),
                  list(Literal = list(Value = "'Select Agent'")),
                  list(Literal = list(Value = "'Ciprofloxacin (DSC)'"))
                ))
              )))
            )
          ),
          Binding = list(
            Primary = list(Groupings = list(list(Projections = list(0L)))),
            Secondary = list(Groupings = list(list(Projections = list(1L, 2L)))),
            DataReduction = list(DataVolume = 4L,
              Primary = list(Window = list(Count = 200L)),
              Secondary = list(Top = list(Count = 100L))),
            Version = 1L
          ),
          ExecutionMetricsKind = 1L
        )
      ))),
      QueryId = "",
      ApplicationContext = list(
        DatasetId = POWERBI_DATASET_ID,
        Sources = list(list(ReportId = POWERBI_REPORT_ID,
                            VisualId = AGENT_VISUAL_ID))
      )
    )),
    cancelQueries = list(),
    modelId = POWERBI_MODEL_ID
  )
}

#' Parse DSC state query response (state × year matrix) into a data frame
parse_dsc_state_response <- function(response, species, test_method) {
  dsr <- response$results[[1]]$result$data$dsr

  if (is.null(dsr$DS)) {
    warning(sprintf("No data returned for Shigella / %s / %s (DSC state)", species, test_method))
    return(NULL)
  }

  ds <- dsr$DS[[1]]
  cell_values <- ds$ValueDicts$D0

  # Years from secondary header
  sh_key <- names(ds$SH[[1]])[grep("^DM", names(ds$SH[[1]]))]
  years <- sapply(ds$SH[[1]][[sh_key]], function(x) x[[grep("^G", names(x))[1]]])

  # Parse primary groups (states)
  ph_key <- names(ds$PH[[1]])[grep("^DM", names(ds$PH[[1]]))]
  groups <- ds$PH[[1]][[ph_key]]

  rows <- list()
  # R = repeat the previous cell in the response, across rows (see
  # parse_agent_response)
  prev_value <- NULL
  for (g in groups) {
    if (!is.null(g[["Ø"]])) next

    state_name <- g$G0
    if (is.null(state_name) || !is.character(state_name)) next
    if (is.null(g$X)) next

    for (i in seq_along(g$X)) {
      cell <- g$X[[i]]

      if (!is.null(cell$R)) {
        cell_text <- prev_value
      } else if (!is.null(cell$M0)) {
        if (is.character(cell$M0)) {
          cell_text <- cell$M0
        } else {
          cell_text <- cell_values[[cell$M0 + 1]]
        }
        prev_value <- cell_text
      } else {
        cell_text <- NULL
      }

      if (is.null(cell_text)) next
      parsed <- parse_cell_value(cell_text)

      rows[[length(rows) + 1]] <- data.frame(
        year = years[i],
        genus = "Shigella",
        species_serotype = species,
        antimicrobial_class = "Quinolones",
        antimicrobial_agent = "Ciprofloxacin (DSC)",
        test_method = test_method,
        narms_now_pct_resistant = parsed$pct_resistant,
        narms_now_n_resistant = parsed$n_resistant,
        narms_now_n_tested = parsed$n_tested,
        site_name = state_name,
        stringsAsFactors = FALSE
      )
    }
  }

  if (length(rows) == 0) return(NULL)
  do.call(rbind, rows)
}

# =============================================================================
# Query execution: parallel requests with retries and a per-year fallback
# =============================================================================

#' Build (but do not perform) a Power BI querydata request
build_powerbi_request <- function(query_body) {
  request(POWERBI_ENDPOINT) |>
    req_headers(
      `X-PowerBI-ResourceKey` = POWERBI_RESOURCE_KEY,
      `Content-Type` = "application/json;charset=UTF-8",
      Accept = "application/json, text/plain, */*",
      Origin = "https://app.powerbigov.us",
      Referer = "https://app.powerbigov.us/"
    ) |>
    req_body_json(query_body, auto_unbox = TRUE) |>
    # retry_on_failure = TRUE also retries curl-level failures such as
    # timeouts, which the default (status-based) retry silently drops.
    req_retry(
      max_tries = 3,
      backoff = ~ 10 * 2^.x,
      retry_on_failure = TRUE,
      is_transient = ~ resp_status(.x) %in% c(429L, 500L, 502L, 503L, 504L)
    ) |>
    req_timeout(REQ_TIMEOUT_SEC)
}

#' Perform a list of requests concurrently.
#' Returns one element per request: the parsed JSON body, or an error
#' condition for a request that failed after retries.
perform_powerbi_batch <- function(reqs, max_active = MAX_ACTIVE) {
  if (length(reqs) == 0) return(list())
  resps <- req_perform_parallel(reqs, on_error = "continue",
                                progress = FALSE, max_active = max_active)
  lapply(resps, function(r) {
    if (!inherits(r, "httr2_response")) return(r)
    # Power BI returns text/plain even though the body is JSON, so parse manually
    tryCatch(jsonlite::fromJSON(resp_body_string(r), simplifyVector = FALSE),
             error = function(e) e)
  })
}

#' Execute a single Power BI querydata request (errors are raised)
execute_powerbi_query <- function(query_body) {
  out <- perform_powerbi_batch(list(build_powerbi_request(query_body)))[[1]]
  if (inherits(out, "condition")) stop(out)
  out
}

#' Message from a Power BI-level error embedded in a 200 response, if any
powerbi_error_message <- function(response) {
  shapes <- response$results[[1]]$result$data$dsr$DataShapes
  if (is.null(shapes)) return(NULL)
  shapes[[1]]$odata.error$message$value
}

#' One row per query: site_name (NA = national), genus, species, test_method,
#' tab ("agent" or "pattern"), year_from, year_to.
#' year_from_min raises the start year (used to re-scrape only changed years).
make_query_specs <- function(site_list, organisms, test_methods, year_to,
                             year_from_min = NULL) {
  site_vec <- vapply(site_list, function(s) if (is.null(s)) NA_character_ else s,
                     character(1))
  org_df <- bind_rows(lapply(organisms, function(o) {
    data.frame(genus = o$genus, species = o$species, stringsAsFactors = FALSE)
  }))
  tidyr::expand_grid(site_name = site_vec, org_df, test_method = test_methods,
                     tab = c("agent", "pattern")) %>%
    mutate(
      year_from = if_else(test_method == "WGS", YEAR_FROM_WGS, YEAR_FROM_AST),
      year_from = if (is.null(year_from_min)) year_from else pmax(year_from, year_from_min),
      year_to = year_to
    ) %>%
    filter(year_from <= year_to) %>%
    as.data.frame(stringsAsFactors = FALSE)
}

#' Expand each spec into one spec per year in its range
expand_spec_years <- function(specs) {
  if (nrow(specs) == 0) return(specs)
  bind_rows(lapply(seq_len(nrow(specs)), function(i) {
    s <- specs[i, , drop = FALSE]
    years <- seq(s$year_from, s$year_to)
    out <- s[rep(1, length(years)), , drop = FALSE]
    out$year_from <- years
    out$year_to <- years
    out
  }))
}

#' Which specs match an entry of SPLIT_PER_YEAR
is_split_spec <- function(specs) {
  hit <- rep(FALSE, nrow(specs))
  for (k in SPLIT_PER_YEAR) {
    hit <- hit | (
      (is.na(specs$site_name) & is.na(k$site_name) |
         (!is.na(specs$site_name) & !is.na(k$site_name) & specs$site_name == k$site_name)) &
        specs$genus == k$genus & specs$species == k$species &
        specs$test_method == k$test_method & specs$tab == k$tab
    )
  }
  hit
}

spec_label <- function(s) {
  sprintf("%s / %s / %s / %s (%s, %d-%d)",
          if (is.na(s$site_name)) "All (national)" else s$site_name,
          s$genus, s$species, s$test_method, s$tab, s$year_from, s$year_to)
}

#' Run a set of specs once. Returns list(agent, pattern, errors, failed_specs)
run_specs <- function(specs, max_active = MAX_ACTIVE) {
  empty <- list(agent = NULL, pattern = NULL, errors = list(),
                failed_specs = specs[0, , drop = FALSE])
  if (nrow(specs) == 0) return(empty)

  reqs <- lapply(seq_len(nrow(specs)), function(i) {
    s <- specs[i, , drop = FALSE]
    site <- if (is.na(s$site_name)) NULL else s$site_name
    q <- if (s$tab == "agent") {
      build_agent_query(s$genus, s$species, s$test_method, site_name = site,
                        year_from = s$year_from, year_to = s$year_to)
    } else {
      build_pattern_query(s$genus, s$species, s$test_method, site_name = site,
                          year_from = s$year_from, year_to = s$year_to)
    }
    build_powerbi_request(q)
  })
  responses <- perform_powerbi_batch(reqs, max_active = max_active)

  agent_parts <- list()
  pattern_parts <- list()
  errors <- list()
  failed <- integer()
  for (i in seq_len(nrow(specs))) {
    s <- specs[i, , drop = FALSE]
    resp <- responses[[i]]
    err <- NULL
    parsed <- NULL
    if (inherits(resp, "condition")) {
      err <- conditionMessage(resp)
    } else {
      pbi_err <- powerbi_error_message(resp)
      if (!is.null(pbi_err)) {
        err <- paste("Power BI error:", pbi_err)
      } else {
        parsed <- suppressWarnings(
          if (s$tab == "agent") {
            parse_agent_response(resp, s$genus, s$species, s$test_method)
          } else {
            parse_pattern_response(resp, s$genus, s$species, s$test_method)
          }
        )
        if (is.null(parsed) || nrow(parsed) == 0) {
          # A well-formed response with no data shape is the dashboard's way of
          # saying there are no rows for this combination. Log it and move on
          # rather than failing the whole run on one empty site/organism.
          warning(sprintf("  No data returned for %s", spec_label(s)))
          next
        }
      }
    }
    if (!is.null(err)) {
      failed <- c(failed, i)
      errors[[length(errors) + 1]] <- list(
        site = if (is.na(s$site_name)) "All (national)" else s$site_name,
        genus = s$genus, species = s$species, test_method = s$test_method,
        tab = s$tab, year_from = s$year_from, year_to = s$year_to, error = err
      )
      next
    }
    parsed$site_name <- s$site_name
    if (s$tab == "agent") {
      agent_parts[[length(agent_parts) + 1]] <- parsed
    } else {
      pattern_parts[[length(pattern_parts) + 1]] <- parsed
    }
  }

  list(
    agent = if (length(agent_parts)) bind_rows(agent_parts) else NULL,
    pattern = if (length(pattern_parts)) bind_rows(pattern_parts) else NULL,
    errors = errors,
    failed_specs = specs[failed, , drop = FALSE]
  )
}

#' Scrape a set of specs in chunks of CHUNK_SIZE requests, then re-issue
#' anything that failed one year at a time.
#' Returns list(agent, pattern, errors, n_queries).
scrape_specs <- function(specs) {
  # Known-slow queries go one year at a time from the start
  split <- is_split_spec(specs)
  specs <- bind_rows(specs[!split, , drop = FALSE],
                     expand_spec_years(specs[split, , drop = FALSE]))

  chunk_id <- ceiling(seq_len(nrow(specs)) / CHUNK_SIZE)
  n_chunks <- max(chunk_id)
  agent_parts <- list()
  pattern_parts <- list()
  failed <- specs[0, , drop = FALSE]
  n_queries <- 0L

  # Completed chunks are checkpointed so an interrupted run (lost network,
  # killed session) resumes where it stopped. The key includes today's date so
  # a later run never picks up another day's data; checkpoints are removed once
  # the raw files have been written, and stale ones from a failed run are
  # cleared here.
  ckpt_dir <- file.path(NARMS_NOW_OUT_DIR, "narms_now_checkpoints")
  dir.create(ckpt_dir, showWarnings = FALSE, recursive = TRUE)
  run_day <- as.character(Sys.Date())
  stale <- list.files(ckpt_dir, full.names = TRUE)
  stale <- stale[!startsWith(basename(stale), run_day)]
  if (length(stale) > 0) unlink(stale)

  for (k in seq_len(n_chunks)) {
    batch <- specs[chunk_id == k, , drop = FALSE]
    ckpt <- file.path(ckpt_dir, paste0(run_day, "_", rlang::hash(batch), ".rds"))
    if (file.exists(ckpt)) {
      res <- readRDS(ckpt)
      message(sprintf("  [chunk %d/%d] loaded from checkpoint", k, n_chunks))
      agent_parts[[k]] <- res$agent
      pattern_parts[[k]] <- res$pattern
      next
    }
    t0 <- Sys.time()
    res <- run_specs(batch)
    n_queries <- n_queries + nrow(batch)
    # A chunk in which (nearly) everything failed means the network or the
    # service is down, not that the queries are bad. Wait and retry the chunk
    # rather than cascading failures into the per-year fallback.
    outage_tries <- 0L
    while (nrow(res$failed_specs) >= OUTAGE_FAIL_FRACTION * nrow(batch) &&
           outage_tries < OUTAGE_MAX_TRIES) {
      outage_tries <- outage_tries + 1L
      message(sprintf("  chunk %d: %d of %d queries failed (%s); waiting %d s and retrying (%d/%d)",
                      k, nrow(res$failed_specs), nrow(batch),
                      substr(res$errors[[1]]$error, 1, 80),
                      OUTAGE_WAIT_SEC, outage_tries, OUTAGE_MAX_TRIES))
      Sys.sleep(OUTAGE_WAIT_SEC)
      res <- run_specs(batch)
      n_queries <- n_queries + nrow(batch)
    }
    message(sprintf("  [chunk %d/%d] %s .. %s: %d queries, %d ok, %d failed, %.0f s",
                    k, n_chunks,
                    paste(batch$genus[1], batch$species[1]),
                    paste(batch$genus[nrow(batch)], batch$species[nrow(batch)]),
                    nrow(batch), nrow(batch) - nrow(res$failed_specs),
                    nrow(res$failed_specs),
                    as.numeric(difftime(Sys.time(), t0, units = "secs"))))
    agent_parts[[k]] <- res$agent
    pattern_parts[[k]] <- res$pattern
    failed <- bind_rows(failed, res$failed_specs)
    if (nrow(res$failed_specs) == 0) {
      saveRDS(res[c("agent", "pattern")], ckpt)
    }
  }

  errors <- list()
  if (nrow(failed) > 0) {
    retry <- expand_spec_years(failed)
    message(sprintf("  Re-issuing %d failed queries as %d single-year queries",
                    nrow(failed), nrow(retry)))
    res <- run_specs(retry, max_active = FALLBACK_MAX_ACTIVE)
    n_queries <- n_queries + nrow(retry)
    agent_parts[[length(agent_parts) + 1]] <- res$agent
    pattern_parts[[length(pattern_parts) + 1]] <- res$pattern
    errors <- res$errors
    for (e in errors) {
      warning(sprintf("  FAILED: %s: %s", spec_label(as.data.frame(e[1:7])), e$error))
    }
  }

  list(
    agent = bind_rows(agent_parts),
    pattern = bind_rows(pattern_parts),
    errors = errors,
    n_queries = n_queries
  )
}

#' Rename the parser output to the raw-file column names
finalize_scrape <- function(df, tab) {
  if (is.null(df) || nrow(df) == 0) return(NULL)
  df <- df %>%
    rename(pct_resistant = narms_now_pct_resistant,
           n_resistant = narms_now_n_resistant,
           n_tested = narms_now_n_tested)
  cols <- if (tab == "agent") {
    c("site_name", "year", "genus", "species_serotype", "antimicrobial_class",
      "antimicrobial_agent", "test_method", "pct_resistant", "n_resistant",
      "n_tested")
  } else {
    c("site_name", "year", "genus", "species_serotype", "pattern",
      "test_method", "pct_resistant", "n_resistant", "n_tested")
  }
  df %>% select(all_of(cols)) %>% distinct()
}

#' Years in which the national rows differ between the new pre-pass and the
#' stored raw file (either direction), restricted to the organisms scraped
national_changed_years <- function(new, path) {
  old <- vroom::vroom(path, show_col_types = FALSE) %>%
    filter(is.na(site_name)) %>%
    semi_join(distinct(new, genus, species_serotype, test_method),
              by = c("genus", "species_serotype", "test_method"))
  keys <- names(new)
  sort(unique(c(anti_join(new, old, by = keys)$year,
                anti_join(old, new, by = keys)$year)))
}

#' Last year with at least one tested isolate across both tabs
last_year_with_data <- function(...) {
  years <- unlist(lapply(list(...), function(d) {
    if (is.null(d)) return(integer())
    d$year[!is.na(d$n_tested) & d$n_tested > 0]
  }))
  if (length(years) == 0) NA_integer_ else as.integer(max(years))
}

#' Record the error log (or remove a stale one), update process state and stop
fail_scrape <- function(errors, state, errors_path) {
  jsonlite::write_json(errors, errors_path, pretty = TRUE, auto_unbox = TRUE)
  state$n_errors <- length(errors)
  state$last_check_result <- "failed"
  if (!is_test_run) {
    process$narms_now_state <<- state
    dcf::dcf_process_record(updated = process)
  }
  stop(sprintf("%d NARMS Now queries failed after retries; see %s",
               length(errors), errors_path))
}


# =============================================================================
# Main scrape: national pre-pass, change detection, state scrape of the years
# that changed. Writes raw/narms_now_agent.csv.gz and raw/narms_now_pattern.csv.gz
# =============================================================================

raw_agent_path <- "raw/narms_now_agent.csv.gz"
raw_pattern_path <- "raw/narms_now_pattern.csv.gz"
out_agent_path <- file.path(NARMS_NOW_OUT_DIR, "narms_now_agent.csv.gz")
out_pattern_path <- file.path(NARMS_NOW_OUT_DIR, "narms_now_pattern.csv.gz")
errors_path <- file.path(NARMS_NOW_OUT_DIR, "narms_now_scrape_errors.json")

if (NARMS_NOW_MODE != "standardize") {
  dir.create(NARMS_NOW_OUT_DIR, showWarnings = FALSE, recursive = TRUE)
  prev_state <- process$narms_now_state
  state <- list(
    last_check_date = as.character(Sys.Date()),
    last_check_result = NA_character_,
    last_scrape_date = prev_state$last_scrape_date,
    last_scrape_year_from = prev_state$last_scrape_year_from,
    year_from_ast = YEAR_FROM_AST,
    year_from_wgs = YEAR_FROM_WGS,
    year_to_requested = YEAR_TO,
    year_to_data = prev_state$year_to_data,
    n_sites = length(sites) + 1L,
    n_queries = 0L,
    n_errors = 0L,
    n_agent_rows = prev_state$n_agent_rows,
    n_pattern_rows = prev_state$n_pattern_rows
  )

  # ---------------------------------------------------------------------------
  # 1. National pre-pass, all years (also supplies the national rows)
  # ---------------------------------------------------------------------------
  message(sprintf("=== NARMS Now national pre-pass (mode = %s, %d organisms, years to %d) ===",
                  NARMS_NOW_MODE, length(organisms), YEAR_TO))
  nat <- scrape_specs(make_query_specs(list(NULL), organisms, test_methods, YEAR_TO))
  state$n_queries <- state$n_queries + nat$n_queries
  if (length(nat$errors) > 0) fail_scrape(nat$errors, state, errors_path)

  nat_agent <- finalize_scrape(nat$agent, "agent")
  nat_pattern <- finalize_scrape(nat$pattern, "pattern")
  year_to_data <- last_year_with_data(nat_agent, nat_pattern)
  if (is.na(year_to_data)) {
    fail_scrape(list(list(site = "All (national)", error = "pre-pass returned no tested cells")),
                state, errors_path)
  }
  nat_agent <- nat_agent %>% filter(year <= year_to_data)
  nat_pattern <- nat_pattern %>% filter(year <= year_to_data)

  # ---------------------------------------------------------------------------
  # 2. Which years need the state scrape?
  # The national counts are sums over states, so a year whose national rows
  # are unchanged is taken as unchanged for every state too. The state scrape
  # covers the earliest changed year through YEAR_TO; earlier years are kept
  # from the stored raw files.
  # ---------------------------------------------------------------------------
  stored_ok <- file.exists(raw_agent_path) && file.exists(raw_pattern_path)
  earliest_year <- min(YEAR_FROM_AST, YEAR_FROM_WGS)

  if (NARMS_NOW_MODE == "full") {
    scrape_from <- earliest_year
    state$last_check_result <- "forced"
  } else if (is.null(prev_state$year_to_data) || !stored_ok) {
    scrape_from <- earliest_year
    state$last_check_result <- "first_run"
  } else {
    changed_years <- c(
      national_changed_years(nat_agent, raw_agent_path),
      national_changed_years(nat_pattern, raw_pattern_path)
    )
    if (year_to_data > prev_state$year_to_data) {
      changed_years <- c(changed_years, seq(prev_state$year_to_data + 1L, year_to_data))
      state$last_check_result <- "new_year"
    } else if (length(changed_years) > 0) {
      state$last_check_result <- "changed"
    } else {
      state$last_check_result <- "unchanged"
    }
    scrape_from <- if (length(changed_years) > 0) as.integer(min(changed_years)) else NA_integer_
    if (!is.na(scrape_from)) {
      message(sprintf("Changed years nationally: %s",
                      paste(sort(unique(changed_years)), collapse = ", ")))
      if (YEAR_TO - scrape_from + 1L > FULL_SCRAPE_YEARS) {
        message(sprintf("%d years to re-scrape (> %d): falling back to a full all-years scrape",
                        YEAR_TO - scrape_from + 1L, FULL_SCRAPE_YEARS))
        scrape_from <- earliest_year
      }
    }
  }
  do_scrape <- !is.na(scrape_from)
  message(sprintf("Pre-pass result: %s (latest year with data: %d%s)",
                  state$last_check_result, year_to_data,
                  if (do_scrape) sprintf("; state scrape from %d", scrape_from) else ""))

  # ---------------------------------------------------------------------------
  # 3. State scrape for years >= scrape_from
  # ---------------------------------------------------------------------------
  if (do_scrape) {
    specs <- make_query_specs(as.list(sites), organisms, test_methods, YEAR_TO,
                              year_from_min = scrape_from)
    message(sprintf("=== NARMS Now state scrape: %d sites x %d organisms, years %d-%d (%d queries, %d in flight) ===",
                    length(sites), length(organisms), scrape_from, YEAR_TO,
                    nrow(specs), MAX_ACTIVE))
    st <- scrape_specs(specs)
    state$n_queries <- state$n_queries + st$n_queries
    errors <- st$errors

    # Shigella DSC state rows: the standard agent query leaks national counts
    # for Ciprofloxacin (DSC) when filtered by NARMSSiteName, so those cells are
    # replaced with a query that groups by the NARMS fact table's SiteName.
    # These 6 queries are cheap, so they always cover every year.
    dsc_df <- NULL
    shigella_in_run <- any(vapply(organisms, function(o) o$genus == "Shigella", logical(1)))
    if (shigella_in_run) {
      message("=== Correcting Shigella DSC state-level data (6 queries) ===")
      dsc_specs <- tidyr::expand_grid(species = c("flexneri", "other", "sonnei"),
                                      test_method = test_methods)
      dsc_reqs <- lapply(seq_len(nrow(dsc_specs)), function(i) {
        tm <- dsc_specs$test_method[i]
        build_powerbi_request(build_dsc_state_query(
          dsc_specs$species[i], tm,
          year_from = if (tm == "WGS") YEAR_FROM_WGS else YEAR_FROM_AST,
          year_to = YEAR_TO
        ))
      })
      dsc_resps <- perform_powerbi_batch(dsc_reqs)
      state$n_queries <- state$n_queries + length(dsc_reqs)
      dsc_parts <- list()
      for (i in seq_along(dsc_resps)) {
        r <- dsc_resps[[i]]
        err <- if (inherits(r, "condition")) conditionMessage(r) else powerbi_error_message(r)
        parsed <- if (is.null(err)) {
          suppressWarnings(parse_dsc_state_response(r, dsc_specs$species[i], dsc_specs$test_method[i]))
        }
        if (is.null(err) && is.null(parsed)) err <- "No data returned"
        if (!is.null(err)) {
          errors[[length(errors) + 1]] <- list(
            site = "states (DSC fix)", genus = "Shigella", species = dsc_specs$species[i],
            test_method = dsc_specs$test_method[i], tab = "agent",
            year_from = if (dsc_specs$test_method[i] == "WGS") YEAR_FROM_WGS else YEAR_FROM_AST,
            year_to = YEAR_TO, error = err
          )
        } else {
          dsc_parts[[length(dsc_parts) + 1]] <- parsed
        }
      }
      if (length(dsc_parts) > 0) dsc_df <- finalize_scrape(bind_rows(dsc_parts), "agent")
    }

    if (length(errors) > 0) fail_scrape(errors, state, errors_path)

    # Assemble: national rows (all years, fresh) + state rows for years before
    # scrape_from (kept from the stored file) + state rows just scraped.
    kept_state_rows <- function(path) {
      if (scrape_from <= earliest_year || !stored_ok) return(NULL)
      vroom::vroom(path, show_col_types = FALSE) %>%
        filter(!is.na(site_name), year < scrape_from) %>%
        semi_join(distinct(nat_agent, genus, species_serotype) %>%
                    bind_rows(distinct(nat_pattern, genus, species_serotype)) %>% distinct(),
                  by = c("genus", "species_serotype"))
    }
    agent_df <- bind_rows(nat_agent, kept_state_rows(raw_agent_path),
                          finalize_scrape(st$agent, "agent"))
    if (!is.null(dsc_df)) {
      agent_df <- agent_df %>%
        filter(!(genus == "Shigella" &
                   antimicrobial_agent == "Ciprofloxacin (DSC)" &
                   !is.na(site_name))) %>%
        bind_rows(dsc_df)
    }
    pattern_df <- bind_rows(nat_pattern, kept_state_rows(raw_pattern_path),
                            finalize_scrape(st$pattern, "pattern"))

    year_to_data <- last_year_with_data(agent_df, pattern_df)
    agent_df <- agent_df %>% filter(year <= year_to_data) %>% distinct() %>%
      arrange(site_name, genus, species_serotype, test_method, antimicrobial_class,
              antimicrobial_agent, year)
    pattern_df <- pattern_df %>% filter(year <= year_to_data) %>% distinct() %>%
      arrange(site_name, genus, species_serotype, test_method, pattern, year)

    vroom::vroom_write(agent_df, out_agent_path, delim = ",")
    vroom::vroom_write(pattern_df, out_pattern_path, delim = ",")
    message(sprintf("Wrote %d rows to %s", nrow(agent_df), out_agent_path))
    message(sprintf("Wrote %d rows to %s", nrow(pattern_df), out_pattern_path))
    unlink(file.path(NARMS_NOW_OUT_DIR, "narms_now_checkpoints"), recursive = TRUE)

    state$last_scrape_date <- as.character(Sys.Date())
    state$last_scrape_year_from <- scrape_from
    state$n_agent_rows <- nrow(agent_df)
    state$n_pattern_rows <- nrow(pattern_df)
  }

  # ---------------------------------------------------------------------------
  # 4. Record state; a clean run removes any stale error log
  # ---------------------------------------------------------------------------
  state$year_to_data <- year_to_data
  if (file.exists(errors_path)) file.remove(errors_path)
  if (!is_test_run) {
    process$narms_now_state <- state
    dcf::dcf_process_record(updated = process)
  } else {
    message("Test run (NARMS_NOW_OUT_DIR set): process.json left unchanged")
  }
  message("=== NARMS Now scraping complete ===")
}

# =============================================================================
# Standardize NARMS Now raw data → standard output
# (Runs whenever raw files exist, even if scraping was skipped)
# =============================================================================

if (file.exists("raw/narms_now_agent.csv.gz")) {
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  site_to_fips <- all_fips %>%
    filter(nchar(geography) == 2, geography != "00",
           !is.na(geography_name)) %>%
    select(geography, geography_name) %>%
    mutate(site_name = if_else(
      geography_name == "District of Columbia",
      "District Of Columbia",
      geography_name
    )) %>%
    select(geography, site_name)

  agent_raw <- vroom::vroom("raw/narms_now_agent.csv.gz", show_col_types = FALSE)
  agent_long <- agent_raw %>%
    left_join(site_to_fips, by = "site_name") %>%
    mutate(
      geography = if_else(is.na(site_name), "00", geography),
      time = paste0(year, "-12-31"),
      narms_flag = case_when(
        is.na(pct_resistant) & is.na(n_resistant) & is.na(n_tested) ~ "not_tested",
        !is.na(n_tested) & n_tested == 0 ~ "no_isolates_tested",
        TRUE ~ "tested"
      ),
      genus_species_serotype = combine_organism(genus, species_serotype),
      antimicrobial = antimicrobial_agent
    ) %>%
    select(geography, time, genus_species_serotype,
           test_method, antimicrobial,
           pct_resistant, n_resistant, n_tested, narms_flag) %>%
    distinct()

  # Validate: warn if any pct_resistant > 100
  bad_rows <- agent_long %>% filter(pct_resistant > 100)
  if (nrow(bad_rows) > 0) {
    warning(sprintf(
      "%d agent rows have pct_resistant > 100%%. Top offenders: %s",
      nrow(bad_rows),
      paste(unique(bad_rows$antimicrobial)[1:min(5, length(unique(bad_rows$antimicrobial)))],
            collapse = ", ")
    ))
  }

  agent_standard <- agent_long %>%
    rename(
      narms_pct_resistant = pct_resistant,
      narms_n_resistant   = n_resistant,
      narms_n_tested      = n_tested
    ) %>%
    arrange(geography, time, genus_species_serotype, test_method, antimicrobial)

  vroom::vroom_write(agent_standard, "standard/data_resistance_agent.csv.gz", delim = ",")
  message(sprintf("Wrote %d rows to standard/data_resistance_agent.csv.gz", nrow(agent_standard)))
}

if (file.exists("raw/narms_now_pattern.csv.gz")) {
  if (!exists("site_to_fips")) {
    all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
    site_to_fips <- all_fips %>%
      filter(nchar(geography) == 2, geography != "00") %>%
      select(geography, geography_name) %>%
      mutate(site_name = if_else(
        geography_name == "District of Columbia",
        "District Of Columbia",
        geography_name
      )) %>%
      select(geography, site_name)
  }

  pattern_raw <- vroom::vroom("raw/narms_now_pattern.csv.gz", show_col_types = FALSE)
  pattern_long <- pattern_raw %>%
    left_join(site_to_fips, by = "site_name") %>%
    mutate(
      geography = if_else(is.na(site_name), "00", geography),
      time = paste0(year, "-12-31"),
      narms_flag = case_when(
        is.na(pct_resistant) & is.na(n_resistant) & is.na(n_tested) ~ "not_tested",
        !is.na(n_tested) & n_tested == 0 ~ "no_isolates_tested",
        TRUE ~ "tested"
      ),
      genus_species_serotype = combine_organism(genus, species_serotype),
    ) %>%
    select(geography, time, genus_species_serotype,
           test_method, pattern,
           pct_resistant, n_resistant, n_tested, narms_flag) %>%
    distinct()

  bad_rows <- pattern_long %>% filter(pct_resistant > 100)
  if (nrow(bad_rows) > 0) {
    warning(sprintf(
      "%d pattern rows have pct_resistant > 100%%. Top offenders: %s",
      nrow(bad_rows),
      paste(unique(bad_rows$pattern)[1:min(5, length(unique(bad_rows$pattern)))],
            collapse = ", ")
    ))
  }

  pattern_standard <- pattern_long %>%
    rename(
      narms_pct_resistant = pct_resistant,
      narms_n_resistant   = n_resistant,
      narms_n_tested      = n_tested
    ) %>%
    arrange(geography, time, genus_species_serotype, test_method, pattern)

  vroom::vroom_write(pattern_standard, "standard/data_resistance_pattern.csv.gz", delim = ",")
  message(sprintf("Wrote %d rows to standard/data_resistance_pattern.csv.gz", nrow(pattern_standard)))
}

#' Resolve a worksheet name by regex
#' FDA renames sheets as they extend the year range (e.g. "2017-2021_data"
#' became "2017-2024_data"), so match on a stable pattern instead of the
#' literal name. Falls back to the first sheet, with a warning, if nothing matches.
resolve_sheet <- function(path, pattern) {
  sheets <- readxl::excel_sheets(path)
  matched <- grep(pattern, sheets, value = TRUE)
  if (length(matched) == 0) {
    warning(sprintf(
      "No sheet matching '%s' in %s (found: %s); falling back to '%s'",
      pattern, basename(path), paste(sheets, collapse = ", "), sheets[1]
    ))
    return(sheets[1])
  }
  matched[1]
}

# =============================================================================
# Source 3: NARMS Retail Meats Data (FDA/CVM)
# Source: FDA NARMS Integrated Reports/Summaries
# URL: https://www.fda.gov/animal-veterinary/national-antimicrobial-resistance-monitoring-system/integrated-reportssummaries
# File: raw/narms-retail-meats.xlsx
# =============================================================================

retail_raw_path <- "raw/narms-retail-meats.xlsx"
retail_url <- "https://www.fda.gov/files/animal%20%26%20veterinary/published/cvm-narms-retail-meats_0.xlsx"

tryCatch(
  download.file(retail_url, retail_raw_path, mode = "wb", quiet = TRUE),
  error = function(e) {
    if (file.exists(retail_raw_path)) {
      message("Retail meats download failed (", conditionMessage(e), "); using existing raw file")
    } else {
      stop(e)
    }
  }
)
current_retail_state <- list(hash = as.character(tools::md5sum(retail_raw_path)))

# Always standardise from the local raw file. The hash is recorded for change
# reporting only -- gating on it would freeze the output at whatever format the
# code had when the raw data last changed.
message("Processing NARMS retail meats data...")

library(readxl)
library(tidyr)

# SIR (Susceptible / Intermediate / Resistant) column codes
sir_codes <- c(
  "AMC", "AMI", "AMP", "ATM", "AVL", "AXO", "AZI", "BAC",
  "CAZ", "CCV", "CEP", "CEQ", "CHL", "CIP", "CIP2", "CLI",
  "COL", "COT", "CTC", "CTX", "DAP", "DOX", "ERY", "FEP",
  "FFN", "FIS", "FLA", "FOX", "GEN", "IMI", "KAN", "LIN",
  "LZD", "MER", "NAL", "NIT", "PEN", "PTZ", "QDA", "SAL",
  "SMX", "STR", "SUF", "TEL", "TET", "TGC", "TIO", "TYL", "VAN"
)
sir_col_names <- paste0(sir_codes, " SIR")

# Full antimicrobial names from the FDA NARMS data dictionary
# (https://www.fda.gov/media/110404/download)
# FLA, SAL, SUF, CIP2 are veterinary-specific and not in the standard
# data dictionary; identified from genus-specificity in the data:
# FLA/SAL = Enterococcus only; SUF/CIP2 = Salmonella/E. coli only
antimicrobial_names <- c(
  AMC  = "Amoxicillin-clavulanic acid",
  AMI  = "Amikacin",
  AMP  = "Ampicillin",
  ATM  = "Aztreonam",
  AVL  = "Avilamycin",
  AXO  = "Ceftriaxone",
  AZI  = "Azithromycin",
  BAC  = "Bacitracin",
  CAZ  = "Ceftazidime",
  CCV  = "Ceftiofur",
  CEP  = "Cephalothin",
  CEQ  = "Cefquinome",
  CHL  = "Chloramphenicol",
  CIP  = "Ciprofloxacin",
  CIP2 = "Ciprofloxacin (2nd breakpoint)",
  CLI  = "Clindamycin",
  COL  = "Colistin",
  COT  = "Trimethoprim-sulfamethoxazole",
  CTC  = "Chlortetracycline",
  CTX  = "Cefotaxime",
  DAP  = "Daptomycin",
  DOX  = "Doxycycline",
  ERY  = "Erythromycin",
  FEP  = "Cefepime",
  FFN  = "Florfenicol",
  FIS  = "Sulfisoxazole",
  FLA  = "Flaveomycin",
  FOX  = "Cefoxitin",
  GEN  = "Gentamicin",
  IMI  = "Imipenem",
  KAN  = "Kanamycin",
  LIN  = "Lincomycin",
  LZD  = "Linezolid",
  MER  = "Meropenem",
  NAL  = "Nalidixic acid",
  NIT  = "Nitrofurantoin",
  PEN  = "Penicillin",
  PTZ  = "Piperacillin-tazobactam",
  QDA  = "Quinupristin-dalfopristin",
  SAL  = "Salinomycin",
  SMX  = "Sulfamethoxazole",
  STR  = "Streptomycin",
  SUF  = "Sulfonamides",
  TEL  = "Telithromycin",
  TET  = "Tetracycline",
  TGC  = "Tigecycline",
  TIO  = "Ceftiofur",
  TYL  = "Tylosin",
  VAN  = "Vancomycin"
)

# FIPS lookup: state abbreviation -> 2-digit FIPS
all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
state_fips_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(geography, state)

retail_raw <- readxl::read_excel(
  retail_raw_path,
  sheet = resolve_sheet(retail_raw_path, "^Retail")
)

# MIC concentration columns (bare antibiotic codes, no suffix)
mic_col_names <- sir_codes

# Filter to positive cultures and add a row ID for joining
retail_filtered <- retail_raw %>%
  filter(GROWTH == "YES") %>%
  mutate(.row_id = row_number())

# Pivot SIR values to long format
sir_long <- retail_filtered %>%
  select(.row_id, any_of(sir_col_names)) %>%
  pivot_longer(
    cols = any_of(sir_col_names),
    names_to = "antimicrobial",
    values_to = "sir"
  ) %>%
  mutate(antimicrobial = sub(" SIR$", "", antimicrobial))

# Pivot MIC values to long format
mic_long <- retail_filtered %>%
  select(.row_id, any_of(mic_col_names)) %>%
  pivot_longer(
    cols = any_of(mic_col_names),
    names_to = "antimicrobial",
    values_to = "mic"
  ) %>%
  mutate(mic = as.numeric(mic))

# Join SIR + MIC by row and antibiotic, map codes to full names
retail_long <- sir_long %>%
  left_join(mic_long, by = c(".row_id", "antimicrobial")) %>%
  left_join(
    retail_filtered %>% select(.row_id, YEAR, GENUS_NAME, SPECIES, SEROTYPE, SOURCE, STATE),
    by = ".row_id"
  ) %>%
  mutate(antimicrobial = antimicrobial_names[antimicrobial]) %>%
  filter(!is.na(sir)) %>%
  select(-.row_id)

# Aggregate by state, converting abbreviation to FIPS
retail_agg <- retail_long %>%
  left_join(state_fips_lookup, by = c("STATE" = "state")) %>%
  filter(!is.na(geography)) %>%
  group_by(YEAR, GENUS_NAME, SPECIES, SEROTYPE, SOURCE, antimicrobial, geography) %>%
  summarize(
    n_tested      = n(),
    n_resistant   = sum(sir == "R"),
    mic50         = median(mic, na.rm = TRUE),
    mic90         = quantile(mic, 0.90, na.rm = TRUE),
    .groups       = "drop"
  ) %>%
  mutate(
    pct_resistant = n_resistant / n_tested * 100,
    time  = paste0(YEAR, "-12-31"),
    genus_species_serotype = combine_organism(GENUS_NAME, SPECIES, SEROTYPE),
  ) %>%
  rename(meat_source = SOURCE) %>%
  select(
    geography, time, genus_species_serotype, meat_source,
    antimicrobial, pct_resistant, n_resistant, n_tested, mic50, mic90
  )

retail_standard <- retail_agg %>%
  mutate(narms_flag = if_else(is.na(mic50), "tested_no_mic", "tested")) %>%
  rename(
    narms_pct_resistant = pct_resistant,
    narms_n_resistant   = n_resistant,
    narms_n_tested      = n_tested,
    narms_mic50         = mic50,
    narms_mic90         = mic90
  ) %>%
  arrange(geography, time, genus_species_serotype, meat_source, antimicrobial)

vroom::vroom_write(
  retail_standard,
  "standard/data_retail_meats.csv.gz",
  delim = ","
)
message(sprintf(
  "Wrote %d rows to standard/data_retail_meats.csv.gz",
  nrow(retail_standard)
))

process$retail_meats_state <- current_retail_state
dcf::dcf_process_record(updated = process)

# =============================================================================
# Source 4: NARMS Animal Pathogen Data (FDA/CVM - Vet-LIRN/NAHLN)
# Source: FDA NARMS Integrated Reports/Summaries
# URL: https://www.fda.gov/animal-veterinary/national-antimicrobial-resistance-monitoring-system/integrated-reportssummaries
# =============================================================================

animal_path_raw_path <- "raw/narms-animal-pathogen.xlsx"
animal_path_url <- "https://www.fda.gov/media/132928/download?attachment"

tryCatch(
  download.file(animal_path_url, animal_path_raw_path, mode = "wb", quiet = TRUE),
  error = function(e) {
    if (file.exists(animal_path_raw_path)) {
      message("Animal pathogen download failed (", conditionMessage(e), "); using existing raw file")
    } else {
      stop(e)
    }
  }
)
current_animal_path_state <- list(hash = as.character(tools::md5sum(animal_path_raw_path)))

# Always standardise (see note in the retail meats section above).
message("Processing NARMS animal pathogen data...")

if (!requireNamespace("readxl", quietly = TRUE)) library(readxl)

# FIPS lookup: full state names -> 2-digit FIPS
all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
state_fips_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(geography, geography_name)

animal_raw <- readxl::read_excel(
  animal_path_raw_path,
  sheet = resolve_sheet(animal_path_raw_path, "_data$")
)

# Data is already in long format with one row per isolate/drug
# Filter to interpretable results, exclude non-US (Canada)
animal_agg <- animal_raw %>%
  filter(Interpretation != "Non-Interpretable") %>%
  left_join(state_fips_lookup, by = c("State" = "geography_name")) %>%
  filter(!is.na(geography)) %>%
  mutate(MIC = as.numeric(MIC)) %>%
  group_by(
    geography, Year, Genus, `Host Species`, `Collection Source`, `Drug Name`
  ) %>%
  summarize(
    n_tested      = n(),
    n_resistant   = sum(Interpretation == "Resistant"),
    mic50         = median(MIC, na.rm = TRUE),
    mic90         = quantile(MIC, 0.90, na.rm = TRUE),
    .groups       = "drop"
  ) %>%
  mutate(
    pct_resistant = n_resistant / n_tested * 100,
    time          = paste0(Year, "-12-31"),
    antimicrobial = `Drug Name`
  ) %>%
  rename(
    genus             = Genus,
    host_species      = `Host Species`,
    collection_source = `Collection Source`
  ) %>%
  select(
    geography, time, genus, host_species, collection_source,
    antimicrobial, pct_resistant, n_resistant, n_tested, mic50, mic90
  )

animal_standard <- animal_agg %>%
  mutate(narms_flag = if_else(is.na(mic50), "tested_no_mic", "tested")) %>%
  rename(
    narms_pct_resistant = pct_resistant,
    narms_n_resistant   = n_resistant,
    narms_n_tested      = n_tested,
    narms_mic50         = mic50,
    narms_mic90         = mic90
  ) %>%
  arrange(geography, time, genus, host_species, collection_source, antimicrobial)

vroom::vroom_write(
  animal_standard,
  "standard/data_animal_pathogen.csv.gz",
  delim = ","
)
message(sprintf(
  "Wrote %d rows to standard/data_animal_pathogen.csv.gz",
  nrow(animal_standard)
))

process$animal_pathogen_state <- current_animal_path_state
dcf::dcf_process_record(updated = process)

# =============================================================================
# Source 5: NARMS Food-Producing Animals (HACCP, Cecal, Minor Species)
# Source: FDA NARMS Integrated Reports/Summaries
# URL: https://www.fda.gov/animal-veterinary/national-antimicrobial-resistance-monitoring-system/integrated-reportssummaries
# =============================================================================

food_animal_files <- list(
  list(
    url   = "https://www.fda.gov/media/93333/download?attachment",
    raw   = "raw/narms-haccp-1997-2005.xlsx",
    sheet_pattern = "^HACCP_1997",
    source_label = "HACCP",
    year_col = "Year"
  ),
  list(
    url   = "https://www.fda.gov/media/93344/download?attachment",
    raw   = "raw/narms-haccp-2006-present.xlsx",
    sheet_pattern = "^HACCP_2006",
    source_label = "HACCP",
    year_col = "Year"
  ),
  list(
    url   = "https://www.fda.gov/media/93351/download?attachment",
    raw   = "raw/narms-cecal-2013-present.xlsx",
    sheet_pattern = "^Cecal",
    source_label = "Cecal",
    year_col = "Year"
  ),
  list(
    url   = "https://www.fda.gov/media/183419/download?attachment",
    raw   = "raw/narms-minor-species.xlsx",
    sheet_pattern = "^Minor Species",
    source_label = "Minor Species",
    year_col = "YEAR"
  )
)

# Download all files and compute combined hash
food_animal_hashes <- character()
for (f in food_animal_files) {
  tryCatch(
    download.file(f$url, f$raw, mode = "wb", quiet = TRUE),
    error = function(e) {
      if (file.exists(f$raw)) {
        message("Download failed for ", f$raw, " (", conditionMessage(e), "); using existing raw file")
      } else {
        stop(e)
      }
    }
  )
  food_animal_hashes <- c(food_animal_hashes, as.character(tools::md5sum(f$raw)))
}
current_food_animal_state <- list(hash = paste(food_animal_hashes, collapse = "_"))

# Always standardise from the local raw files. The hash below records what was
# processed, but must NOT gate this block: gating it means a code change to the
# standardisation never reaches the output files on the automated monthly run.
message("Processing NARMS food-producing animal data...")

library(readxl)
library(tidyr)

# SIR column codes and antimicrobial name lookup (same as retail meats section)
sir_codes <- c(
  "AMC", "AMI", "AMP", "ATM", "AVL", "AXO", "AZI", "BAC",
  "CAZ", "CCV", "CEP", "CEQ", "CHL", "CIP", "CIP2", "CLI",
  "COL", "COT", "CTC", "CTX", "DAP", "DOX", "ERY", "FEP",
  "FFN", "FIS", "FLA", "FOX", "GEN", "IMI", "KAN", "LIN",
  "LZD", "MER", "NAL", "NIT", "PEN", "PTZ", "QDA", "SAL",
  "SMX", "STR", "SUF", "TEL", "TET", "TGC", "TIO", "TYL", "VAN"
)
sir_col_names <- paste0(sir_codes, " SIR")
antimicrobial_names <- c(
  AMC  = "Amoxicillin-clavulanic acid",
  AMI  = "Amikacin",
  AMP  = "Ampicillin",
  ATM  = "Aztreonam",
  AVL  = "Avilamycin",
  AXO  = "Ceftriaxone",
  AZI  = "Azithromycin",
  BAC  = "Bacitracin",
  CAZ  = "Ceftazidime",
  CCV  = "Ceftiofur",
  CEP  = "Cephalothin",
  CEQ  = "Cefquinome",
  CHL  = "Chloramphenicol",
  CIP  = "Ciprofloxacin",
  CIP2 = "Ciprofloxacin (2nd breakpoint)",
  CLI  = "Clindamycin",
  COL  = "Colistin",
  COT  = "Trimethoprim-sulfamethoxazole",
  CTC  = "Chlortetracycline",
  CTX  = "Cefotaxime",
  DAP  = "Daptomycin",
  DOX  = "Doxycycline",
  ERY  = "Erythromycin",
  FEP  = "Cefepime",
  FFN  = "Florfenicol",
  FIS  = "Sulfisoxazole",
  FLA  = "Flaveomycin",
  FOX  = "Cefoxitin",
  GEN  = "Gentamicin",
  IMI  = "Imipenem",
  KAN  = "Kanamycin",
  LIN  = "Lincomycin",
  LZD  = "Linezolid",
  MER  = "Meropenem",
  NAL  = "Nalidixic acid",
  NIT  = "Nitrofurantoin",
  PEN  = "Penicillin",
  PTZ  = "Piperacillin-tazobactam",
  QDA  = "Quinupristin-dalfopristin",
  SAL  = "Salinomycin",
  SMX  = "Sulfamethoxazole",
  STR  = "Streptomycin",
  SUF  = "Sulfonamides",
  TEL  = "Telithromycin",
  TET  = "Tetracycline",
  TGC  = "Tigecycline",
  TIO  = "Ceftiofur",
  TYL  = "Tylosin",
  VAN  = "Vancomycin"
)

#' Process a single food-animal Excel file into long format
#' @param file_info list with raw, sheet, source_label, year_col
process_food_animal_file <- function(file_info) {
  raw <- readxl::read_excel(
    file_info$raw,
    sheet = resolve_sheet(file_info$raw, file_info$sheet_pattern)
  )

  # Standardise year column name
  if (file_info$year_col != "YEAR") {
    raw <- raw %>% rename(YEAR = !!file_info$year_col)
  }

  # Filter to positive cultures
  filtered <- raw %>%
    filter(GROWTH == "YES") %>%
    mutate(.row_id = row_number())

  # Pivot SIR columns
  sir_long <- filtered %>%
    select(.row_id, any_of(sir_col_names)) %>%
    pivot_longer(cols = any_of(sir_col_names),
                 names_to = "antimicrobial", values_to = "sir") %>%
    mutate(antimicrobial = sub(" SIR$", "", antimicrobial))

  # Pivot MIC columns (coerce all to character first to avoid type conflicts)
  mic_cols_present <- intersect(sir_codes, names(filtered))
  mic_data <- filtered %>%
    select(.row_id, any_of(sir_codes)) %>%
    mutate(across(any_of(mic_cols_present), as.character))
  mic_long <- mic_data %>%
    pivot_longer(cols = any_of(sir_codes),
                 names_to = "antimicrobial", values_to = "mic") %>%
    mutate(mic = as.numeric(mic))

  # Join and attach metadata
  sir_long %>%
    left_join(mic_long, by = c(".row_id", "antimicrobial")) %>%
    left_join(
      filtered %>% select(.row_id, YEAR, GENUS_NAME, SPECIES, SEROTYPE,
                          HOST_SPECIES, SOURCE),
      by = ".row_id"
    ) %>%
    mutate(
      antimicrobial = antimicrobial_names[antimicrobial],
      source_program = file_info$source_label
    ) %>%
    filter(!is.na(sir)) %>%
    select(-.row_id)
}

# Process all four files and combine
all_food_long <- bind_rows(lapply(food_animal_files, process_food_animal_file))

# Aggregate nationally (no state data in these files)
food_animal_agg <- all_food_long %>%
  group_by(YEAR, GENUS_NAME, SPECIES, SEROTYPE, HOST_SPECIES, SOURCE,
           antimicrobial) %>%
  summarize(
    n_tested      = n(),
    n_resistant   = sum(sir == "R"),
    mic50         = median(mic, na.rm = TRUE),
    mic90         = quantile(mic, 0.90, na.rm = TRUE),
    .groups       = "drop"
  ) %>%
  mutate(
    pct_resistant = n_resistant / n_tested * 100,
    geography     = "00",
    time          = paste0(YEAR, "-12-31"),
    genus_species_serotype = combine_organism(GENUS_NAME, SPECIES, SEROTYPE),
  ) %>%
  rename(
    host_species = HOST_SPECIES,
    source_type  = SOURCE
  ) %>%
  select(
    geography, time, genus_species_serotype,
    source_type, host_species, antimicrobial,
    pct_resistant, n_resistant, n_tested, mic50, mic90
  )

food_animal_standard <- food_animal_agg %>%
  mutate(narms_flag = if_else(is.na(mic50), "tested_no_mic", "tested")) %>%
  rename(
    narms_pct_resistant = pct_resistant,
    narms_n_resistant   = n_resistant,
    narms_n_tested      = n_tested,
    narms_mic50         = mic50,
    narms_mic90         = mic90
  ) %>%
  arrange(geography, time, genus_species_serotype, source_type, host_species, antimicrobial)

vroom::vroom_write(
  food_animal_standard,
  "standard/data_food_animals.csv.gz",
  delim = ","
)
message(sprintf(
  "Wrote %d rows to standard/data_food_animals.csv.gz",
  nrow(food_animal_standard)
))

process$food_animal_state <- current_food_animal_state
dcf::dcf_process_record(updated = process)
