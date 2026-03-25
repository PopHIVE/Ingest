# =============================================================================
# NARMS Data Ingestion
# Source 1: NARMS Now Human Data - Resistance by Agent (Power BI API)
# Source 2: NARMS Now Human Data - Resistance by Pattern (Power BI API)
# =============================================================================

library(dplyr)
library(vroom)
library(httr2)
library(jsonlite)

if (!file.exists("process.json")) {
  process <- list(narms_now_state = NULL, retail_meats_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

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
QUERY_DELAY <- 0.5  # seconds between API requests

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
YEAR_TO <- 2024

# --- Site definitions ---
# NARMSSiteName entity values from the Power BI model (51 states + DC)
# The main loop prepends NULL (= national "All" with no site filter)
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

# =============================================================================
# Helper Functions
# =============================================================================

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
                              year_from = YEAR_FROM, year_to = YEAR_TO) {

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
                                year_from = YEAR_FROM, year_to = YEAR_TO) {

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

#' Execute a Power BI querydata request
execute_powerbi_query <- function(query_body) {
  resp <- request(POWERBI_ENDPOINT) |>
    req_headers(
      `X-PowerBI-ResourceKey` = POWERBI_RESOURCE_KEY,
      `Content-Type` = "application/json;charset=UTF-8",
      Accept = "application/json, text/plain, */*",
      Origin = "https://app.powerbigov.us",
      Referer = "https://app.powerbigov.us/"
    ) |>
    req_body_json(query_body, auto_unbox = TRUE) |>
    req_retry(max_tries = 3, backoff = ~ 5) |>
    req_timeout(120) |>
    req_perform()

  # Power BI returns text/plain content type even though body is JSON;
  # httr2::resp_body_json() rejects non-JSON content types, so parse manually
  jsonlite::fromJSON(resp_body_string(resp), simplifyVector = FALSE)
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

        # Track previous value for R (repeat) handling
        prev_value <- NULL

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

  for (pattern_group in pattern_groups) {
    pattern_idx <- pattern_group[[grep("^G", names(pattern_group))[1]]]
    pattern_name <- pattern_names[[pattern_idx + 1]]

    if (is.null(pattern_group$X)) next
    cells <- pattern_group$X

    prev_value <- NULL

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

# =============================================================================
# Main Scraping Loop — writes raw data to raw/narms_now_agent.csv.gz
# and raw/narms_now_pattern.csv.gz
# =============================================================================

# Determine if we need to scrape new data:
# - First run (no state): full scrape
# - New year available (YEAR_TO > last scraped year): incremental scrape
# - Monthly refresh (>30 days since last scrape): incremental for latest year
last_scrape <- process$narms_now_state$last_scrape_date
last_year_to <- process$narms_now_state$year_to

needs_scrape <- is.null(last_scrape) ||
  as.Date(last_scrape) < Sys.Date() - 30 ||
  (!is.null(last_year_to) && YEAR_TO > last_year_to)

if (needs_scrape) {
  # Determine year range for this scrape
  if (is.null(last_year_to)) {
    # First run: full scrape
    scrape_year_from_ast <- YEAR_FROM_AST
    scrape_year_from_wgs <- YEAR_FROM_WGS
    message("=== Full NARMS Now scrape (first run) ===")
  } else {
    # Incremental: only scrape from last scraped year onward
    # (re-scrape the last year too in case it was incomplete)
    scrape_year_from_ast <- last_year_to
    scrape_year_from_wgs <- max(last_year_to, YEAR_FROM_WGS)
    message(sprintf("=== Incremental NARMS Now scrape (years %d-%d) ===",
                    scrape_year_from_ast, YEAR_TO))
  }

  n_sites <- length(sites)
  total_queries <- length(organisms) * length(test_methods) * 2 * (n_sites + 1)

  message(sprintf("Organisms: %d | Test methods: %d | Sites: %d (+ national) | Total queries: ~%d",
                  length(organisms), length(test_methods), n_sites, total_queries))
  message(sprintf("Estimated time: ~%.0f minutes (%.1fs delay between queries)",
                  total_queries * QUERY_DELAY / 60, QUERY_DELAY))

  all_agent_data <- list()
  all_pattern_data <- list()
  error_log <- list()
  query_count <- 0

  # Iterate: national (NULL) first, then each state
  site_list <- c(list(NULL), as.list(sites))

  for (site in site_list) {
    site_label <- if (is.null(site)) "All (national)" else site

    for (org in organisms) {
      for (tm in test_methods) {
        year_from <- if (tm == "WGS") scrape_year_from_wgs else scrape_year_from_ast

        # --- Resistance by Agent ---
        query_count <- query_count + 1
        message(sprintf("  [%d/%d] %s / %s / %s / %s (agent)",
                        query_count, total_queries, site_label,
                        org$genus, org$species, tm))

        tryCatch({
          query <- build_agent_query(org$genus, org$species, tm, site_name = site,
                                     year_from = year_from)
          response <- execute_powerbi_query(query)
          parsed <- parse_agent_response(response, org$genus, org$species, tm)

          if (!is.null(parsed) && nrow(parsed) > 0) {
            parsed$site_name <- if (is.null(site)) NA_character_ else site
            all_agent_data[[length(all_agent_data) + 1]] <- parsed
            message(sprintf("    -> %d rows", nrow(parsed)))
          } else {
            message("    -> No data")
          }
        }, error = function(e) {
          error_log[[length(error_log) + 1]] <<- list(
            site = site_label, genus = org$genus, species = org$species,
            test_method = tm, tab = "agent",
            error = conditionMessage(e)
          )
          warning(sprintf("    -> ERROR: %s", conditionMessage(e)))
        })

        Sys.sleep(QUERY_DELAY)

        # --- Resistance by Pattern ---
        query_count <- query_count + 1
        message(sprintf("  [%d/%d] %s / %s / %s / %s (pattern)",
                        query_count, total_queries, site_label,
                        org$genus, org$species, tm))

        tryCatch({
          query <- build_pattern_query(org$genus, org$species, tm, site_name = site,
                                       year_from = year_from)
          response <- execute_powerbi_query(query)
          parsed <- parse_pattern_response(response, org$genus, org$species, tm)

          if (!is.null(parsed) && nrow(parsed) > 0) {
            parsed$site_name <- if (is.null(site)) NA_character_ else site
            all_pattern_data[[length(all_pattern_data) + 1]] <- parsed
            message(sprintf("    -> %d rows", nrow(parsed)))
          } else {
            message("    -> No data")
          }
        }, error = function(e) {
          error_log[[length(error_log) + 1]] <<- list(
            site = site_label, genus = org$genus, species = org$species,
            test_method = tm, tab = "pattern",
            error = conditionMessage(e)
          )
          warning(sprintf("    -> ERROR: %s", conditionMessage(e)))
        })

        Sys.sleep(QUERY_DELAY)
      }
    }
  }

  # --- Write raw scraped data ---

  if (length(all_agent_data) > 0) {
    new_agent_df <- do.call(rbind, all_agent_data) %>%
      rename(pct_resistant = narms_now_pct_resistant,
             n_resistant = narms_now_n_resistant,
             n_tested = narms_now_n_tested)

    # Incremental: load existing raw, remove overlapping years, append new
    if (!is.null(last_year_to) && file.exists("raw/narms_now_agent.csv.gz")) {
      existing <- vroom::vroom("raw/narms_now_agent.csv.gz", show_col_types = FALSE)
      existing <- existing %>% filter(year < scrape_year_from_ast)
      new_agent_df <- bind_rows(existing, new_agent_df)
    }

    vroom::vroom_write(new_agent_df, "raw/narms_now_agent.csv.gz", delim = ",")
    message(sprintf("Wrote %d rows to raw/narms_now_agent.csv.gz", nrow(new_agent_df)))
  }

  if (length(all_pattern_data) > 0) {
    new_pattern_df <- do.call(rbind, all_pattern_data) %>%
      rename(pct_resistant = narms_now_pct_resistant,
             n_resistant = narms_now_n_resistant,
             n_tested = narms_now_n_tested)

    # Incremental: load existing raw, remove overlapping years, append new
    if (!is.null(last_year_to) && file.exists("raw/narms_now_pattern.csv.gz")) {
      existing <- vroom::vroom("raw/narms_now_pattern.csv.gz", show_col_types = FALSE)
      existing <- existing %>% filter(year < scrape_year_from_ast)
      new_pattern_df <- bind_rows(existing, new_pattern_df)
    }

    vroom::vroom_write(new_pattern_df, "raw/narms_now_pattern.csv.gz", delim = ",")
    message(sprintf("Wrote %d rows to raw/narms_now_pattern.csv.gz", nrow(new_pattern_df)))
  }

  # Log errors
  if (length(error_log) > 0) {
    jsonlite::write_json(error_log, "raw/narms_now_scrape_errors.json", pretty = TRUE)
    warning(sprintf("%d scraping errors occurred. See raw/narms_now_scrape_errors.json",
                    length(error_log)))
  }

  # Update process state
  process$narms_now_state <- list(
    last_scrape_date = as.character(Sys.Date()),
    n_agent_rows = if (length(all_agent_data) > 0) nrow(new_agent_df) else 0,
    n_pattern_rows = if (length(all_pattern_data) > 0) nrow(new_pattern_df) else 0,
    n_errors = length(error_log),
    n_sites = n_sites + 1,
    year_from_ast = YEAR_FROM_AST,
    year_from_wgs = YEAR_FROM_WGS,
    year_to = YEAR_TO
  )
  dcf::dcf_process_record(updated = process)

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
  agent_standard <- agent_raw %>%
    left_join(site_to_fips, by = "site_name") %>%
    mutate(
      geography = if_else(is.na(site_name), "00", geography),
      time = paste0(year, "-12-31")
    ) %>%
    select(geography, time, genus, species_serotype,
           antimicrobial_class, antimicrobial_agent, test_method,
           pct_resistant, n_resistant, n_tested)

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
  pattern_standard <- pattern_raw %>%
    left_join(site_to_fips, by = "site_name") %>%
    mutate(
      geography = if_else(is.na(site_name), "00", geography),
      time = paste0(year, "-12-31")
    ) %>%
    select(geography, time, genus, species_serotype,
           pattern, test_method,
           pct_resistant, n_resistant, n_tested)

  vroom::vroom_write(pattern_standard, "standard/data_resistance_pattern.csv.gz", delim = ",")
  message(sprintf("Wrote %d rows to standard/data_resistance_pattern.csv.gz", nrow(pattern_standard)))
}

# =============================================================================
# Source 3: NARMS Retail Meats Data (FDA/CVM)
# Source: FDA NARMS Integrated Reports/Summaries
# URL: https://www.fda.gov/animal-veterinary/national-antimicrobial-resistance-monitoring-system/integrated-reportssummaries
# File: raw/cvm-narms-retail-meats.xlsx
# =============================================================================

retail_raw_path <- "raw/cvm-narms-retail-meats.xlsx"
retail_url <- "https://www.fda.gov/files/animal%20%26%20veterinary/published/cvm-narms-retail-meats_0.xlsx"

download.file(retail_url, retail_raw_path, mode = "wb", quiet = TRUE)
current_retail_state <- list(hash = as.character(tools::md5sum(retail_raw_path)))

if (!identical(process$retail_meats_state, current_retail_state)) {
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

    retail_raw <- readxl::read_excel(retail_raw_path, sheet = "Retail_Meats")

    # Filter to positive cultures; pivot antibiotic SIR columns to long format
    retail_long <- retail_raw %>%
      filter(GROWTH == "YES") %>%
      select(YEAR, GENUS_NAME, SPECIES, SEROTYPE, SOURCE, STATE,
             any_of(sir_col_names)) %>%
      pivot_longer(
        cols = any_of(sir_col_names),
        names_to = "antimicrobial",
        values_to = "sir"
      ) %>%
      mutate(
        antimicrobial = sub(" SIR$", "", antimicrobial),
        antimicrobial = antimicrobial_names[antimicrobial]
      ) %>%
      filter(!is.na(sir))

    # Aggregate by state, converting abbreviation to FIPS
    retail_standard <- retail_long %>%
      left_join(state_fips_lookup, by = c("STATE" = "state")) %>%
      filter(!is.na(geography)) %>%
      group_by(YEAR, GENUS_NAME, SPECIES, SEROTYPE, SOURCE, antimicrobial, geography) %>%
      summarize(
        n_tested    = n(),
        n_resistant = sum(sir == "R"),
        .groups     = "drop"
      ) %>%
      mutate(
        value = n_resistant / n_tested * 100,
        time  = paste0(YEAR, "-12-31")
      ) %>%
      rename(
        genus       = GENUS_NAME,
        species     = SPECIES,
        serotype    = SEROTYPE,
        meat_source = SOURCE
      ) %>%
      select(
        geography, time, genus, species, serotype, meat_source,
        antimicrobial, value, n_resistant, n_tested
      )

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
}
