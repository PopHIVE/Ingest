# =============================================================================
# Build Data Source Documentation
# Generates HTML documentation from measure_info.json files
# =============================================================================

library(jsonlite)
library(vroom)
library(arrow)
library(htmltools)
library(glue)

# -----------------------------------------------------------------------------
# Helper: Null coalescing operator (avoids rlang dependency)
# -----------------------------------------------------------------------------
`%||%` <- function(x, y) if (is.null(x) || (is.character(x) && nchar(x) == 0)) y else x

# GitHub raw base for linking bundle source files in the docs. Defined here so
# the bundle render functions below can use it (a matching GITHUB_RAW_BASE is set
# later for the manifest section).
GH_RAW_BASE <- "https://raw.githubusercontent.com/PopHIVE/Ingest/main"

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# Standard variable definitions for common columns not in measure_info.json
STANDARD_VARS <- list(

geography = list(
    short_name = "Geography",
    description = "FIPS code identifier (00 = national, 2-digit = state, 5-digit = county)",
    measure_type = "identifier",
    unit = "FIPS code"
  ),
  time = list(
    short_name = "Time",
    description = "Date in MM-DD-YYYY format (Saturday for weekly data)",
    measure_type = "date",
    unit = "date"
  ),
  age = list(
    short_name = "Age Group",
    description = "Age group category",
    measure_type = "category",
    unit = ""
  ),
  sex = list(
    short_name = "Sex",
    description = "Sex category (Male, Female, Overall)",
    measure_type = "category",
    unit = ""
  ),
  race_ethnicity = list(
    short_name = "Race/Ethnicity",
    description = "Race/ethnicity category",
    measure_type = "category",
    unit = ""
  ),
  virus = list(
    short_name = "Virus",
    description = "Pathogen type (rsv, influenza, covid)",
    measure_type = "category",
    unit = ""
  ),
  grade = list(
    short_name = "Grade",
    description = "School grade level",
    measure_type = "category",
    unit = ""
  ),
  vaccine = list(
    short_name = "Vaccine",
    description = "Vaccine type",
    measure_type = "category",
    unit = ""
  ),
  serotype = list(
    short_name = "Serotype",
    description = "Disease serotype/variant",
    measure_type = "category",
    unit = ""
  ),
  survey_type = list(
    short_name = "Survey Type",
    description = "Type of survey conducted",
    measure_type = "category",
    unit = ""
  ),
  year = list(
    short_name = "Year",
    description = "Calendar year",
    measure_type = "date",
    unit = "year"
  ),
  date = list(
    short_name = "Date",
    description = "Date (Saturday for weekly data)",
    measure_type = "date",
    unit = "date"
  ),
  week = list(
    short_name = "ISO Week",
    description = "ISO week number within the year",
    measure_type = "integer",
    unit = "week number"
  ),
  source = list(
    short_name = "Source",
    description = "Data source identifier for tall-format files",
    measure_type = "category",
    unit = ""
  ),
  outcome_name = list(
    short_name = "Outcome",
    description = "Health outcome name (e.g., Diabetes, Obesity)",
    measure_type = "category",
    unit = ""
  ),
  fips = list(
    short_name = "FIPS Code",
    description = "FIPS geographic identifier",
    measure_type = "identifier",
    unit = "FIPS code"
  )
)

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------

#' Get CSV column names from a gzipped CSV file
get_csv_columns <- function(filepath) {
  tryCatch({
    # Read just the first row to get column names
    cols <- names(vroom::vroom(filepath, n_max = 0, show_col_types = FALSE))
    return(cols)
  }, error = function(e) {
    return(character(0))
  })
}

#' Get column names from a parquet file
get_parquet_columns <- function(filepath) {
  tryCatch({
    pf <- arrow::read_parquet(filepath, as_data_frame = FALSE)
    return(names(pf))
  }, error = function(e) {
    return(character(0))
  })
}

#' Find all standard data files for a source
get_standard_files <- function(source_dir) {
  standard_dir <- file.path(source_dir, "standard")
  if (!dir.exists(standard_dir)) return(character(0))

  files <- list.files(standard_dir, pattern = "\\.csv\\.gz$", full.names = TRUE)
  return(files)
}

#' Find all dist parquet files for a bundle
get_dist_files <- function(bundle_dir) {
  dist_dir <- file.path(bundle_dir, "dist")
  if (!dir.exists(dist_dir)) return(character(0))

  files <- list.files(dist_dir, pattern = "\\.parquet$", full.names = TRUE)
  return(files)
}

#' Extract variable metadata from measure_info.json
#' Handles template variables with {variant}, {category}, etc. patterns and resolves them
get_variable_info <- function(measure_info, var_name) {
  # Direct match
  if (var_name %in% names(measure_info)) {
    return(measure_info[[var_name]])
  }

  # Check standard variables
  if (var_name %in% names(STANDARD_VARS)) {
    return(STANDARD_VARS[[var_name]])
  }

  # Try template matching by constructing names from known keys
  for (key in names(measure_info)) {
    if (!grepl("\\{", key)) next

    info <- measure_info[[key]]

    # Get available values for each placeholder type
    variant_keys <- if (!is.null(info$variants)) names(info$variants) else NULL
    category_keys <- if (!is.null(info$categories)) names(info$categories) else NULL

    # Determine which placeholders are in this template
    has_variant <- grepl("\\{variant", key)
    has_category <- grepl("\\{category", key)

    # Build list of possible combinations to try
    if (has_category && has_variant && !is.null(category_keys) && !is.null(variant_keys)) {
      # Both placeholders - try all combinations
      for (cat_val in category_keys) {
        for (var_val in variant_keys) {
          # "blank" is a special variant key that expands to empty string
          effective_var_val <- if (var_val == "blank") "" else var_val
          test_name <- key
          test_name <- gsub("\\{category(\\.[^}]*)?\\}", cat_val, test_name)
          test_name <- gsub("\\{variant(\\.[^}]*)?\\}", effective_var_val, test_name)
          if (test_name == var_name) {
            captured <- list(category = cat_val, variant = var_val)
            return(resolve_template(info, captured))
          }
        }
      }
    } else if (has_variant && !is.null(variant_keys)) {
      # Only variant placeholder
      for (var_val in variant_keys) {
        effective_var_val <- if (var_val == "blank") "" else var_val
        test_name <- gsub("\\{variant(\\.[^}]*)?\\}", effective_var_val, key)
        if (test_name == var_name) {
          captured <- list(variant = var_val)
          return(resolve_template(info, captured))
        }
      }
    } else if (has_category && !is.null(category_keys)) {
      # Only category placeholder
      for (cat_val in category_keys) {
        test_name <- gsub("\\{category(\\.[^}]*)?\\}", cat_val, key)
        if (test_name == var_name) {
          captured <- list(category = cat_val)
          return(resolve_template(info, captured))
        }
      }
    }
  }

  return(NULL)
}

#' Look up variable info for a bundle column using the path-prefixed key schema
get_bundle_variable_info <- function(measure_info, bundle_name, filename, col_name) {
  # Bundle keys are: bundle_name/dist/filename.parquet|col_name
  full_key <- paste0(bundle_name, "/dist/", filename, "|", col_name)

  # Try full key lookup (direct or template via get_variable_info)
  info <- get_variable_info(measure_info, full_key)
  if (!is.null(info)) return(info)

  # Bundle-specific override for geography: state/national files use names,
  # county-level files use 5-digit FIPS codes
  if (col_name == "geography") {
    return(list(
      short_name = "Geography",
      description = "Geographic area name (state or country name for state/national files; 5-digit FIPS code for county-level files)",
      measure_type = "identifier",
      unit = "name or FIPS code"
    ))
  }

  # Fall back to standard vars using just the column name
  if (col_name %in% names(STANDARD_VARS)) {
    return(STANDARD_VARS[[col_name]])
  }

  return(NULL)
}

#' Resolve template placeholders in measure info
resolve_template <- function(info, captured) {
  # Look up details from variants/categories objects
  lookup_objects <- list()

  if (!is.null(info$variants) && "variant" %in% names(captured)) {
    variant_key <- captured[["variant"]]
    if (variant_key %in% names(info$variants)) {
      lookup_objects[["variant"]] <- info$variants[[variant_key]]
    }
  }
  if (!is.null(info$categories) && "category" %in% names(captured)) {
    category_key <- captured[["category"]]
    if (category_key %in% names(info$categories)) {
      lookup_objects[["category"]] <- info$categories[[category_key]]
    }
  }

  # Create resolved copy of info
  resolved <- info

  # Function to replace all template placeholders in a string
  replace_templates <- function(text) {
    if (!is.character(text) || length(text) != 1) return(text)

    # Replace simple placeholders {name} with captured values
    for (ph_name in names(captured)) {
      ph_val <- captured[[ph_name]]
      # "blank" variant expands to empty string in display
      if (ph_name == "variant" && ph_val == "blank") ph_val <- ""
      text <- gsub(paste0("\\{", ph_name, "\\}"), ph_val, text)
    }

    # Replace dotted placeholders {name.field} with lookup values
    for (obj_name in names(lookup_objects)) {
      obj <- lookup_objects[[obj_name]]
      if (!is.null(obj)) {
        for (field in names(obj)) {
          placeholder <- paste0("\\{", obj_name, "\\.", field, "\\}")
          replacement <- as.character(obj[[field]])
          text <- gsub(placeholder, replacement, text)
        }
      }
    }

    return(text)
  }

  # Apply replacements to key string fields
  string_fields <- c("id", "short_name", "long_name",
                     "short_description", "long_description")
  for (field in string_fields) {
    if (!is.null(resolved[[field]])) {
      resolved[[field]] <- replace_templates(resolved[[field]])
    }
  }

  # Also check if variant has overriding field values
  if (!is.null(lookup_objects[["variant"]])) {
    override_fields <- c("short_name", "short_description",
                         "long_description", "measure_type", "unit")
    for (field in override_fields) {
      if (!is.null(lookup_objects[["variant"]][[field]])) {
        resolved[[field]] <- replace_templates(
          lookup_objects[["variant"]][[field]]
        )
      }
    }
  }

  return(resolved)
}

#' Format a source name for display
format_source_name <- function(name) {
  # Convert underscores to spaces and title case
  name <- gsub("_", " ", name)
  name <- tools::toTitleCase(name)
  # Handle special cases
  name <- gsub("Cdc", "CDC", name)
  name <- gsub("Jhu", "JHU", name)
  name <- gsub("Mmr", "MMR", name)
  name <- gsub("Cms", "CMS", name)
  name <- gsub("Nssp", "NSSP", name)
  name <- gsub("Nis", "NIS", name)
  name <- gsub("Nrevss", "NREVSS", name)
  name <- gsub("Nchs", "NCHS", name)
  name <- gsub("Brfss", "BRFSS", name)
  name <- gsub("Vaers", "VAERS", name)
  name <- gsub("Amr", "AMR", name)
  name <- gsub("Ili", "ILI", name)
  name <- gsub("Nhsn", "NHSN", name)
  name <- gsub("Nnds", "NNDS", name)
  return(name)
}

#' Format a bundle name for display
format_bundle_name <- function(name) {
  display <- sub("^bundle_", "", name)
  display <- gsub("_", " ", display)
  display <- tools::toTitleCase(display)
  paste0("Bundle: ", display)
}

# Short one-sentence summary of a (possibly long) description. Used only as a
# fallback for brand-new datasets with no hand-written summary yet -- see the
# update-data-sources-index skill, which is how a real summary should get in.
short_summary <- function(x, max_chars = 300) {
  x <- x %||% ""
  x <- trimws(gsub("\\s+", " ", x))
  if (!nzchar(x)) return("")

  # A period/!/? followed by whitespace isn't always a sentence end -- it's
  # also how abbreviations like "U.S." or initials like "J." look. Split
  # naively, then merge fragments back while the accumulated text ends on one
  # of those, so the "first sentence" doesn't get cut off after "U.S.".
  abbreviations <- c("U.S.", "U.K.", "e.g.", "i.e.", "Mr.", "Mrs.", "Ms.", "Dr.",
                      "Jr.", "Sr.", "St.", "vs.", "etc.", "No.", "Fig.", "Vol.",
                      "Inc.", "Ph.D.", "M.D.")
  ends_with_abbrev <- function(s) {
    s <- trimws(s)
    grepl("\\b[A-Z]\\.$", s) || any(endsWith(s, abbreviations))
  }

  parts <- strsplit(x, "(?<=[.!?])\\s+", perl = TRUE)[[1]]
  first <- parts[1]
  i <- 1
  while (i < length(parts) && ends_with_abbrev(first)) {
    i <- i + 1
    first <- paste(first, parts[i])
  }
  if (is.na(first) || !nzchar(first)) first <- x

  if (nchar(first) > max_chars) {
    # Trim to the last full word so a long, punctuation-sparse sentence (e.g.
    # one using semicolons instead of periods) doesn't get cut mid-word.
    truncated <- substr(first, 1, max_chars - 1)
    last_space <- max(gregexpr("\\s", truncated)[[1]])
    if (last_space > 0) truncated <- substr(truncated, 1, last_space - 1)
    first <- paste0(trimws(truncated), "…")
  }
  first
}

#' Generate HTML badge list for levels of a tall-format column
make_levels_display <- function(levels_info) {
  if (is.null(levels_info) || length(levels_info) == 0) return(NULL)

  level_badges <- lapply(names(levels_info), function(lvl) {
    tags$span(class = "badge bg-secondary me-1 mb-1", style = "font-weight: normal;", lvl)
  })

  tags$div(class = "mt-1",
    tags$small(class = "text-muted", tags$em("Values: ")),
    level_badges
  )
}

#' Generate HTML for a single variable row (source files)
make_variable_row <- function(var_name, var_info) {
  short_name <- var_info$short_name %||% var_name
  description <- var_info$short_description %||% var_info$description %||%
                 var_info$long_description %||% ""
  measure_type <- var_info$measure_type %||% ""
  unit <- var_info$unit %||% ""

  tags$tr(
    tags$td(tags$code(var_name)),
    tags$td(short_name),
    tags$td(description),
    tags$td(measure_type),
    tags$td(unit)
  )
}

#' Generate HTML for a single variable row in a bundle file
#' Handles levels (tall-format columns) and source_id references
make_bundle_variable_row <- function(col_name, var_info) {
  short_name <- var_info$short_name %||% col_name
  description <- var_info$short_description %||% var_info$description %||%
                 var_info$long_description %||% ""
  measure_type <- var_info$measure_type %||% ""
  unit <- var_info$unit %||% ""

  # Note source_id reference in description
  if (!is.null(var_info$source_id) && nchar(var_info$source_id) > 0) {
    ref_note <- paste0("(source variable: ", var_info$source_id, ")")
    description <- if (nchar(description) > 0) paste(description, ref_note) else ref_note
  }

  # Show levels for tall-format columns
  levels_display <- if (!is.null(var_info$levels)) {
    make_levels_display(var_info$levels)
  } else NULL

  tags$tr(
    tags$td(tags$code(col_name)),
    tags$td(short_name),
    tags$td(description, levels_display),
    tags$td(measure_type),
    tags$td(unit)
  )
}

#' Generate HTML for a data file section (source CSV files)
make_file_section <- function(filepath, measure_info) {
  filename <- basename(filepath)
  columns <- get_csv_columns(filepath)

  if (length(columns) == 0) return(NULL)

  rows <- lapply(columns, function(col) {
    var_info <- get_variable_info(measure_info, col)
    if (is.null(var_info)) {
      var_info <- list(short_name = col, description = "", measure_type = "", unit = "")
    }
    make_variable_row(col, var_info)
  })

  tagList(
    tags$h5(class = "mt-3", tags$code(filename)),
    tags$div(class = "table-responsive",
      tags$table(class = "table table-striped table-sm",
        tags$thead(
          tags$tr(
            tags$th("Variable"),
            tags$th("Short Name"),
            tags$th("Description"),
            tags$th("Type"),
            tags$th("Unit")
          )
        ),
        tags$tbody(rows)
      )
    )
  )
}

#' Generate HTML for a bundle dist parquet file section
make_bundle_file_section <- function(filepath, measure_info, bundle_name) {
  filename <- basename(filepath)
  columns <- get_parquet_columns(filepath)

  if (length(columns) == 0) return(NULL)

  rows <- lapply(columns, function(col) {
    var_info <- get_bundle_variable_info(measure_info, bundle_name, filename, col)
    if (is.null(var_info)) {
      var_info <- list(short_name = col, description = "", measure_type = "", unit = "")
    }
    make_bundle_variable_row(col, var_info)
  })

  # Source files that contribute to THIS parquet (from the _bundle block)
  bundle_meta <- measure_info[["_bundle"]]
  file_meta <- if (!is.null(bundle_meta) && !is.null(bundle_meta$dist_files)) {
    bundle_meta$dist_files[[filename]]
  } else NULL
  src_files <- if (!is.null(file_meta)) unlist(file_meta$source_files) else NULL
  source_files_display <- if (!is.null(src_files) && length(src_files) > 0) {
    tags$div(class = "mb-2 small",
      tags$span(class = "text-muted", tags$em("Source files: ")),
      lapply(src_files, function(sf) {
        tags$a(href = paste0(GH_RAW_BASE, "/data/", sf), target = "_blank",
               class = "me-2 text-decoration-none", tags$code(sf))
      })
    )
  } else NULL

  tagList(
    tags$h5(class = "mt-3", tags$code(filename)),
    source_files_display,
    tags$div(class = "table-responsive",
      tags$table(class = "table table-striped table-sm",
        tags$thead(
          tags$tr(
            tags$th("Variable"),
            tags$th("Short Name"),
            tags$th("Description"),
            tags$th("Type"),
            tags$th("Unit")
          )
        ),
        tags$tbody(rows)
      )
    )
  )
}

#' Generate HTML for source links
make_source_links <- function(sources_info) {
  if (is.null(sources_info) || length(sources_info) == 0) return(NULL)

  links <- lapply(names(sources_info), function(key) {
    if (key == "_sources") return(NULL)
    src <- sources_info[[key]]
    if (is.null(src$url) && is.null(src$organization_url)) return(NULL)

    items <- tagList()
    if (!is.null(src$url) && nchar(src$url) > 0) {
      items <- tagList(items,
        tags$a(href = src$url, target = "_blank", "Data Source"),
        " | "
      )
    }
    if (!is.null(src$organization_url) && nchar(src$organization_url) > 0) {
      items <- tagList(items,
        tags$a(href = src$organization_url, target = "_blank", src$organization %||% "Organization")
      )
    }
    if (!is.null(src$location_url) && nchar(src$location_url) > 0) {
      items <- tagList(items,
        " | ",
        tags$a(href = src$location_url, target = "_blank", "API/Data Location")
      )
    }

    tags$li(items)
  })

  links <- Filter(Negate(is.null), links)
  if (length(links) == 0) return(NULL)

  tags$ul(class = "list-unstyled", links)
}

#' Generate HTML for a single data source section
make_source_section <- function(source_name, source_dir) {
  measure_info_path <- file.path(source_dir, "measure_info.json")

  # Read measure_info.json
  measure_info <- tryCatch({
    fromJSON(measure_info_path, simplifyVector = FALSE)
  }, error = function(e) {
    return(list())
  })

  # Get _sources metadata
  sources_meta <- measure_info[["_sources"]]

  # Get description from first source in _sources
  description <- ""
  if (!is.null(sources_meta) && length(sources_meta) > 0) {
    first_source <- sources_meta[[1]]
    description <- first_source$description %||% ""
  }

  # Collect restrictions from _sources
  restrictions_list <- list()
  if (!is.null(sources_meta) && length(sources_meta) > 0) {
    for (src_key in names(sources_meta)) {
      src <- sources_meta[[src_key]]
      if (!is.null(src$restrictions) && nchar(src$restrictions) > 0) {
        restrictions_list[[src$name %||% src_key]] <- src$restrictions
      }
    }
  }

  # Get all standard files
  standard_files <- get_standard_files(source_dir)

  # Generate file sections
  file_sections <- lapply(standard_files, function(f) {
    make_file_section(f, measure_info)
  })
  file_sections <- Filter(Negate(is.null), file_sections)

  # Build the section
  section_id <- gsub("[^a-zA-Z0-9]", "-", source_name)

  tagList(
    tags$section(id = section_id, class = "mb-5",
      tags$h2(class = "border-bottom pb-2", format_source_name(source_name)),

      # Description
      if (nchar(description) > 0) {
        tags$p(class = "lead", description)
      },

      # Source links
      if (!is.null(sources_meta)) {
        tagList(
          tags$h5("Sources"),
          make_source_links(sources_meta)
        )
      },

      # Restrictions
      if (length(restrictions_list) > 0) {
        if (length(restrictions_list) == 1) {
          # Single source - show inline
          tags$div(class = "alert alert-warning",
            tags$strong("Restrictions: "), restrictions_list[[1]]
          )
        } else {
          # Multiple sources - show as list
          tags$div(class = "alert alert-warning",
            tags$strong("Restrictions:"),
            tags$ul(class = "mb-0 mt-2",
              lapply(names(restrictions_list), function(src_name) {
                tags$li(tags$strong(src_name, ": "), restrictions_list[[src_name]])
              })
            )
          )
        }
      },

      # Variable tables
      if (length(file_sections) > 0) {
        tagList(
          tags$h4(class = "mt-4", "Variables"),
          file_sections
        )
      } else {
        tags$p(class = "text-muted", "No standard data files found.")
      }
    )
  )
}

#' Generate HTML for a bundle section
make_bundle_section <- function(bundle_name, bundle_dir) {
  measure_info_path <- file.path(bundle_dir, "measure_info.json")

  measure_info <- tryCatch({
    fromJSON(measure_info_path, simplifyVector = FALSE)
  }, error = function(e) {
    return(list())
  })

  # Get dist parquet files
  dist_files <- get_dist_files(bundle_dir)

  # Generate file sections
  file_sections <- lapply(dist_files, function(f) {
    make_bundle_file_section(f, measure_info, bundle_name)
  })
  file_sections <- Filter(Negate(is.null), file_sections)

  section_id <- gsub("[^a-zA-Z0-9]", "-", bundle_name)

  # Data source sets feeding this bundle (from the _bundle block). Each dataset
  # is shown by the same header text used for its own documentation section
  # (format_source_name) and links to that section, which is its data dictionary.
  bundle_meta <- measure_info[["_bundle"]]
  bundle_srcs <- if (!is.null(bundle_meta)) unlist(bundle_meta$sources) else NULL
  sources_display <- if (!is.null(bundle_srcs) && length(bundle_srcs) > 0) {
    src_links <- list()
    for (i in seq_along(bundle_srcs)) {
      s <- bundle_srcs[[i]]
      src_links[[length(src_links) + 1]] <- tags$a(
        href = paste0("#", gsub("[^a-zA-Z0-9]", "-", s)),
        class = "bundle-source-link",
        format_source_name(s)
      )
      if (i < length(bundle_srcs)) src_links[[length(src_links) + 1]] <- "; "
    }
    tags$p(class = "mb-3", tags$strong("Data sources: "), src_links)
  } else NULL

  tagList(
    tags$section(id = section_id, class = "mb-5",
      tags$h2(class = "border-bottom pb-2", format_bundle_name(bundle_name)),

      tags$p(class = "text-muted",
        tags$em(sprintf("Combined output bundle. Dist files: %d parquet file(s).",
                        length(dist_files)))
      ),

      sources_display,

      if (length(file_sections) > 0) {
        tagList(
          tags$h4(class = "mt-4", "Output Files (dist/)"),
          file_sections
        )
      } else {
        tags$p(class = "text-muted", "No dist parquet files found.")
      }
    )
  )
}

# -----------------------------------------------------------------------------
# Main Script
# -----------------------------------------------------------------------------

cat("Building data source documentation...\n")

# Find all data source and bundle directories
data_dir <- "data"
all_dirs <- list.dirs(data_dir, recursive = FALSE, full.names = TRUE)

# Separate sources from bundles
source_dirs <- all_dirs[!grepl("bundle_", basename(all_dirs))]
bundle_dirs  <- all_dirs[grepl("bundle_", basename(all_dirs))]

# Filter to only those with measure_info.json
source_dirs <- source_dirs[sapply(source_dirs, function(d) {
  file.exists(file.path(d, "measure_info.json"))
})]
bundle_dirs <- bundle_dirs[sapply(bundle_dirs, function(d) {
  file.exists(file.path(d, "measure_info.json"))
})]

cat(sprintf("Found %d data sources with measure_info.json\n", length(source_dirs)))
cat(sprintf("Found %d bundles with measure_info.json\n", length(bundle_dirs)))

# Sort alphabetically
source_dirs <- source_dirs[order(basename(source_dirs))]
bundle_dirs  <- bundle_dirs[order(basename(bundle_dirs))]
source_names <- basename(source_dirs)
bundle_names  <- basename(bundle_dirs)

# Generate navigation items for sources
nav_items_sources <- lapply(source_names, function(name) {
  section_id <- gsub("[^a-zA-Z0-9]", "-", name)
  tags$li(class = "nav-item",
    tags$a(class = "nav-link", href = paste0("#", section_id), format_source_name(name))
  )
})

# Generate navigation items for bundles
nav_items_bundles <- lapply(bundle_names, function(name) {
  section_id <- gsub("[^a-zA-Z0-9]", "-", name)
  tags$li(class = "nav-item",
    tags$a(class = "nav-link", href = paste0("#", section_id), format_bundle_name(name))
  )
})

# Generate source sections
cat("Generating data source sections...\n")
source_sections <- lapply(seq_along(source_dirs), function(i) {
  cat(sprintf("  Processing %s (%d/%d)\n", source_names[i], i, length(source_dirs)))
  make_source_section(source_names[i], source_dirs[i])
})

# Generate bundle sections
cat("Generating bundle sections...\n")
bundle_sections <- lapply(seq_along(bundle_dirs), function(i) {
  cat(sprintf("  Processing bundle %s (%d/%d)\n", bundle_names[i], i, length(bundle_dirs)))
  make_bundle_section(bundle_names[i], bundle_dirs[i])
})

# Build the full HTML page
html_page <- tags$html(lang = "en",
  tags$head(
    tags$meta(charset = "UTF-8"),
    tags$meta(name = "viewport", content = "width=device-width, initial-scale=1"),
    tags$title("PopHIVE Data Source Documentation"),
    tags$link(
      href = "https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css",
      rel = "stylesheet"
    ),
    tags$style(HTML("
      body { padding-top: 60px; }
      .navbar { background-color: #2c3e50; }
      .nav-pills .nav-link { color: #495057; padding: 0.25rem 0.5rem; font-size: 0.875rem; }
      .nav-pills .nav-link:hover { background-color: #e9ecef; }
      section { scroll-margin-top: 70px; }
      code { background-color: #f8f9fa; padding: 0.125rem 0.25rem; border-radius: 0.25rem; }
      .table th { background-color: #f8f9fa; }
      .sidebar-heading { font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; }
    "))
  ),
  tags$body(
    # Fixed navbar
    tags$nav(class = "navbar navbar-dark fixed-top",
      tags$div(class = "container-fluid",
        tags$a(class = "navbar-brand", href = "#", "PopHIVE Data Documentation"),
        tags$div(class = "d-flex align-items-center",
          tags$a(class = "btn btn-outline-light btn-sm me-3", href = "data-table.html",
                 "Data Table →"),
          tags$span(class = "navbar-text text-light",
            sprintf("Last updated: %s", format(Sys.Date(), "%B %d, %Y"))
          )
        )
      )
    ),

    # Main container
    tags$div(class = "container-fluid",
      tags$div(class = "row",
        # Sidebar navigation
        tags$nav(class = "col-md-3 col-lg-2 d-md-block bg-light sidebar collapse",
          style = "position: fixed; top: 56px; bottom: 0; overflow-y: auto; padding-top: 1rem;",
          tags$div(class = "position-sticky",
            tags$h6(class = "sidebar-heading px-3 mt-1 mb-1 text-muted", "Data Sources"),
            tags$ul(class = "nav flex-column nav-pills", nav_items_sources),
            tags$hr(class = "mx-3"),
            tags$h6(class = "sidebar-heading px-3 mt-2 mb-1 text-muted", "Bundles"),
            tags$ul(class = "nav flex-column nav-pills", nav_items_bundles)
          )
        ),

        # Main content
        tags$main(class = "col-md-9 ms-sm-auto col-lg-10 px-md-4",
          tags$div(class = "pt-3",
            tags$h1("PopHIVE Data Source Documentation"),
            tags$p(class = "lead text-muted",
              "This page documents all data sources and output bundles in the PopHIVE/Ingest repository, ",
              "including variable definitions, data types, and source information."
            ),
            tags$hr(),

            # Data source sections
            tags$h2(class = "text-muted mb-4", id = "data-sources", "Data Sources"),
            source_sections,

            tags$hr(class = "my-5"),

            # Bundle sections
            tags$h2(class = "text-muted mb-4", id = "bundles", "Output Bundles"),
            tags$p(class = "text-muted",
              "Bundles combine multiple data sources into consolidated parquet files for visualization. ",
              "Columns marked with values indicate tall-format (long) data where the listed column ",
              "identifies which measure each row contains."
            ),
            bundle_sections
          )
        )
      )
    ),

    # Bootstrap JS
    tags$script(src = "https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js")
  )
)

# Create docs directory if it doesn't exist
if (!dir.exists("docs")) {
  dir.create("docs")
}

# Write the HTML file
output_path <- "docs/index.html"
cat(sprintf("Writing documentation to %s...\n", output_path))
save_html(html_page, output_path)

# -----------------------------------------------------------------------------
# Build master sources JSON
# Collects all _sources entries from each measure_info.json
# -----------------------------------------------------------------------------

cat("Building master sources JSON...\n")

master_sources <- list()

all_info_dirs <- c(source_dirs, bundle_dirs)
all_info_names <- c(source_names, bundle_names)

for (i in seq_along(all_info_dirs)) {
  measure_info_path <- file.path(all_info_dirs[i], "measure_info.json")
  measure_info <- tryCatch({
    fromJSON(measure_info_path, simplifyVector = FALSE)
  }, error = function(e) list())

  sources_meta <- measure_info[["_sources"]]
  if (!is.null(sources_meta) && length(sources_meta) > 0) {
    for (src_key in names(sources_meta)) {
      entry <- sources_meta[[src_key]]
      # Tag which data directory this source belongs to
      entry[["data_source"]] <- all_info_names[i]
      # Use source key as identifier; if already seen, append data_source
      if (src_key %in% names(master_sources)) {
        existing <- master_sources[[src_key]]
        existing_ds <- existing[["data_source"]]
        existing[["data_source"]] <- unique(c(existing_ds, all_info_names[i]))
        master_sources[[src_key]] <- existing
      } else {
        master_sources[[src_key]] <- entry
      }
    }
  }
}

master_sources_path <- "resources/sources_master.json"
write(toJSON(master_sources, auto_unbox = TRUE, pretty = TRUE), master_sources_path)
cat(sprintf("Master sources JSON written to %s (%d sources)\n",
            master_sources_path, length(master_sources)))

# =====================================================================
# Build machine-readable data manifest (bundles + sources)
# Includes URLs, columns, units, descriptions for programmatic use
# =====================================================================

cat("Building data manifest JSON...\n")

# Configuration: set GitHub repo URL (customize as needed)
GITHUB_REPO <- "PopHIVE/Ingest"
GITHUB_RAW_BASE <- sprintf("https://raw.githubusercontent.com/%s/main", GITHUB_REPO)

# Helper: Build manifest entry for a dist file
make_manifest_file_entry <- function(filepath, measure_info, bundle_name) {
  filename <- basename(filepath)
  columns <- get_parquet_columns(filepath)

  if (length(columns) == 0) {
    return(NULL)
  }

  # Build URL to the dist file (relative path from repo root)
  rel_path <- file.path("data", bundle_name, "dist", filename)
  rel_path <- gsub("\\\\", "/", rel_path)  # Convert Windows backslashes to forward slashes
  file_url <- paste0(GITHUB_RAW_BASE, "/", rel_path)

  # Build column entries
  col_entries <- lapply(columns, function(col) {
    var_info <- get_bundle_variable_info(measure_info, bundle_name, filename, col)
    if (is.null(var_info)) {
      var_info <- list(
        short_name = col,
        description = "",
        measure_type = "",
        unit = ""
      )
    }

    list(
      name = col,
      short_name = var_info$short_name %||% col,
      description = var_info$short_description %||% var_info$description %||%
                   var_info$long_description %||% "",
      measure_type = var_info$measure_type %||% "",
      unit = var_info$unit %||% "",
      levels = var_info$levels  # Include levels for tall-format columns
    )
  })

  list(
    filename = filename,
    path = rel_path,
    url = file_url,
    columns = col_entries
  )
}

# Helper: Build manifest entry for a source file
make_manifest_source_file_entry <- function(filepath, measure_info) {
  filename <- basename(filepath)
  columns <- get_csv_columns(filepath)

  if (length(columns) == 0) {
    return(NULL)
  }

  # Build column entries
  col_entries <- lapply(columns, function(col) {
    var_info <- get_variable_info(measure_info, col)
    if (is.null(var_info)) {
      var_info <- list(
        short_name = col,
        description = "",
        measure_type = "",
        unit = ""
      )
    }

    list(
      name = col,
      short_name = var_info$short_name %||% col,
      description = var_info$short_description %||% var_info$description %||%
                   var_info$long_description %||% "",
      measure_type = var_info$measure_type %||% "",
      unit = var_info$unit %||% ""
    )
  })

  list(
    filename = filename,
    columns = col_entries
  )
}

# Build manifest structure
manifest <- list(
  generated = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
  repository = GITHUB_REPO,
  github_raw_base = GITHUB_RAW_BASE,
  bundles = list(),
  data_sources = list()
)

# Add bundle entries
cat("Adding bundle entries to manifest...\n")
for (i in seq_along(bundle_dirs)) {
  bundle_name <- bundle_names[i]
  bundle_dir <- bundle_dirs[i]
  measure_info_path <- file.path(bundle_dir, "measure_info.json")

  measure_info <- tryCatch({
    fromJSON(measure_info_path, simplifyVector = FALSE)
  }, error = function(e) list())

  dist_files <- get_dist_files(bundle_dir)

  # Build file entries
  file_entries <- lapply(dist_files, function(f) {
    make_manifest_file_entry(f, measure_info, bundle_name)
  })
  file_entries <- Filter(Negate(is.null), file_entries)

  if (length(file_entries) > 0) {
    manifest$bundles[[bundle_name]] <- list(
      name = bundle_name,
      display_name = format_bundle_name(bundle_name),
      dist_files = file_entries
    )
    cat(sprintf("  Added bundle %s with %d dist file(s)\n", bundle_name, length(file_entries)))
  }
}

# Add data source entries
cat("Adding data source entries to manifest...\n")
for (i in seq_along(source_dirs)) {
  source_name <- source_names[i]
  source_dir <- source_dirs[i]
  measure_info_path <- file.path(source_dir, "measure_info.json")

  measure_info <- tryCatch({
    fromJSON(measure_info_path, simplifyVector = FALSE)
  }, error = function(e) list())

  standard_files <- get_standard_files(source_dir)

  # Build file entries
  file_entries <- lapply(standard_files, function(f) {
    make_manifest_source_file_entry(f, measure_info)
  })
  file_entries <- Filter(Negate(is.null), file_entries)

  if (length(file_entries) > 0) {
    # Get source metadata
    sources_meta <- measure_info[["_sources"]]
    description <- ""
    if (!is.null(sources_meta) && length(sources_meta) > 0) {
      first_source <- sources_meta[[1]]
      description <- first_source$description %||% ""
    }

    manifest$data_sources[[source_name]] <- list(
      name = source_name,
      display_name = format_source_name(source_name),
      description = description,
      standard_files = file_entries
    )
    cat(sprintf("  Added source %s with %d standard file(s)\n", source_name, length(file_entries)))
  }
}

# Write manifest to JSON
manifest_path <- "resources/data_manifest.json"
write(toJSON(manifest, auto_unbox = TRUE, pretty = TRUE), manifest_path)
cat(sprintf("Data manifest written to %s\n", manifest_path))
cat(sprintf("  - %d bundles\n", length(manifest$bundles)))
cat(sprintf("  - %d data sources\n", length(manifest$data_sources)))

# =====================================================================
# Build data sources index (docs/data_sources_index.json)
# Lightweight per-source catalog: name, github_folder, data_url,
# data_dictionary, latest_date, search_terms, bucket, summary, and a `files`
# array (one entry per standardized csv.gz, each with a direct `dataset_link`
# and a short `dataset_stratification` blurb).
#
# docs/data_sources_index.json is fully GENERATED -- never hand-edit it. The
# five hand-written fields live in each source's measure_info.json under a
# "_catalog" block and are read from there on every build:
#
#   "_catalog": {
#     "name": "Display name",            # optional override of _sources[].name
#     "summary": "One to two sentences.",
#     "search_terms": ["Respiratory", "rsv"],
#     "bucket": ["Respiratory"],
#     "files": { "data.csv.gz": "How this file is stratified." }
#   }
#
# When a field is absent it is derived: name from _sources[].name, summary as a
# concise extractive first sentence, search_terms and bucket both from bundle
# membership (bundle_* -> human-readable label, identical starting values that
# can then be edited independently in _catalog), and dataset_stratification from
# the file name. An empty [] in _catalog is respected as an intentional "none"
# and is not re-derived; to re-derive, delete the field from _catalog. All other
# fields (links, latest_date) are always computed from the repo contents.
# =====================================================================

cat("Building data sources index JSON...\n")

# Direct raw-content URL to a standardized csv.gz file on GitHub.
github_raw_file <- function(source_name, filename) {
  paste0(GITHUB_RAW_BASE, "/data/", source_name, "/standard/", filename)
}

# Format a bundle directory name as a human-readable label: strip the
# "bundle_" prefix, turn remaining underscores into spaces, and capitalize the
# first letter (e.g. bundle_chronic_diseases -> "Chronic diseases"). Used as
# the default value for both the `search_terms` and `bucket` index fields.
format_bundle_label <- function(bundle_name) {
  x <- sub("^bundle_", "", bundle_name)
  x <- gsub("_", " ", x)
  if (nchar(x) > 0) x <- paste0(toupper(substr(x, 1, 1)), substr(x, 2, nchar(x)))
  x
}

# Fallback stratification blurb derived from a standard file's name, used only
# when measure_info.json's _catalog has no hand-written blurb for the file.
# Strips the data/ prefix, extension, and geography tokens, leaving the
# distinguishing tokens that describe the file's stratification dimension.
derive_stratification <- function(filename) {
  stem <- sub("\\.csv\\.gz$", "", basename(filename))
  stem <- sub("^data_?", "", stem)
  tokens <- strsplit(stem, "_")[[1]]
  geo_tokens <- c("", "state", "county", "national", "nation", "us", "usa", "overall")
  tokens <- tokens[!tolower(tokens) %in% geo_tokens]
  if (length(tokens) == 0) {
    return("Overall; no stratification beyond time and geography.")
  }
  paste0("Stratified by ", gsub("_", " ", paste(tokens, collapse = " ")), ".")
}

# Helper: find the most recent date across a source's standard files
get_latest_date <- function(source_dir) {
  files <- get_standard_files(source_dir)
  if (length(files) == 0) return(NA_character_)

  date_candidates <- c("time", "date", "week_end", "week_ending",
                       "week_ending_date", "week_end_date")
  latest_dates <- as.Date(character(0))
  latest_year <- NA_integer_

  # Read a single named column from a csv.gz (altrep off to force materializing)
  read_column <- function(filepath, col_name) {
    tryCatch(
      vroom::vroom(filepath, show_col_types = FALSE, altrep = FALSE)[[col_name]],
      error = function(e) NULL
    )
  }

  # Parse dates defensively: as.Date() errors on non-standard strings, so try
  # a few explicit formats. strptime parsing is lenient (e.g. format "%Y-%m-%d"
  # will happily misparse "09-01-2009" as year 9), so a format is only trusted
  # once every non-missing value matches its exact shape via regex - not merely
  # once as.Date() manages to produce a non-NA value for at least one row.
  parse_dates <- function(x) {
    x <- as.character(x)
    non_na <- x[!is.na(x) & nzchar(x)]
    if (length(non_na) == 0) return(as.Date(rep(NA_character_, length(x))))

    formats <- list(
      "%Y-%m-%d" = "^\\d{4}-\\d{2}-\\d{2}$",
      "%Y/%m/%d" = "^\\d{4}/\\d{2}/\\d{2}$",
      "%m/%d/%Y" = "^\\d{2}/\\d{2}/\\d{4}$",
      "%m-%d-%Y" = "^\\d{2}-\\d{2}-\\d{4}$"
    )

    for (fmt in names(formats)) {
      if (all(grepl(formats[[fmt]], non_na))) {
        return(suppressWarnings(as.Date(x, format = fmt)))
      }
    }
    as.Date(rep(NA_character_, length(x)))
  }

  for (f in files) {
    cols <- get_csv_columns(f)
    if (length(cols) == 0) next
    lower_cols <- tolower(cols)

    match_idx <- match(date_candidates, lower_cols)
    match_idx <- match_idx[!is.na(match_idx)]

    if (length(match_idx) > 0) {
      d <- parse_dates(read_column(f, cols[match_idx[1]]))
      d <- d[!is.na(d)]
      if (length(d) > 0) latest_dates <- c(latest_dates, max(d))
    } else if ("year" %in% lower_cols) {
      vals <- read_column(f, cols[match("year", lower_cols)])
      yrs <- suppressWarnings(as.integer(vals))
      yrs <- yrs[!is.na(yrs)]
      if (length(yrs) > 0) latest_year <- max(c(latest_year, yrs), na.rm = TRUE)
    }
  }

  if (length(latest_dates) > 0) {
    return(format(max(latest_dates), "%Y-%m-%d"))
  } else if (!is.na(latest_year)) {
    return(sprintf("%d-12-31", latest_year))
  }
  return(NA_character_)
}

# Map each source -> the bundles that consume it, by scanning each bundle's
# build.R for references to `../<source>/standard/...`. build.R is what actually
# reads the source files, so it is the source of truth. (The bundle
# process.json `source_files` record was previously used but is unreliable: it
# reflects the LAST build, so it goes stale under old source names -- e.g.
# `epic` after the source was split into epic_* dirs, or `vaccine_exemptions_kiang`
# after a rename -- and is empty for bundles that have not been rebuilt.)
source_to_bundles <- list()
for (i in seq_along(bundle_dirs)) {
  build_r <- file.path(bundle_dirs[i], "build.R")
  if (!file.exists(build_r)) next
  lines <- readLines(build_r, warn = FALSE)
  lines <- lines[!grepl("^\\s*#", lines)]  # drop full-line comments
  matches <- unlist(regmatches(
    lines, gregexpr("\\.\\./[A-Za-z0-9_]+/standard/", lines)
  ))
  srcs <- unique(sub("^\\.\\./([A-Za-z0-9_]+)/standard/$", "\\1", matches))
  for (s in srcs) {
    source_to_bundles[[s]] <- unique(c(source_to_bundles[[s]], bundle_names[i]))
  }
}

# Only index sources that have at least one standard data file (mirrors the
# manifest); this excludes template/scratch dirs with no output.
index_source_idx <- Filter(
  function(i) length(get_standard_files(source_dirs[i])) > 0,
  seq_along(source_dirs)
)

index_datasets <- lapply(index_source_idx, function(i) {
  source_name <- source_names[i]
  source_dir <- source_dirs[i]

  measure_info <- tryCatch(
    fromJSON(file.path(source_dir, "measure_info.json"), simplifyVector = FALSE),
    error = function(e) list()
  )
  sources_meta <- measure_info[["_sources"]]
  first_source <- if (!is.null(sources_meta) && length(sources_meta) > 0) {
    sources_meta[[1]]
  } else {
    list()
  }
  # Hand-written catalog text lives in measure_info.json under "_catalog".
  catalog <- measure_info[["_catalog"]]
  if (is.null(catalog)) catalog <- list()

  # Concise extractive fallback (first sentence of each source description).
  # Descriptions span ALL sources so multi-source datasets aren't
  # misrepresented by only the first (e.g. NCHS covers overdose AND 21 causes
  # of mortality). Used only for a brand-new dataset with no summary yet.
  short_descs <- character(0)
  if (!is.null(sources_meta)) {
    for (s in sources_meta) {
      ss <- short_summary(trimws(s$description %||% ""))
      if (nzchar(ss)) short_descs <- c(short_descs, ss)
    }
  }
  fallback_summary <- paste(unique(short_descs), collapse = " ")

  # name: a _catalog override wins; otherwise the measure_info source name.
  display_name <- catalog$name %||% first_source$name %||%
    format_source_name(source_name)
  # The hand-written _catalog summary wins; a source without one falls back to
  # a concise extractive summary.
  if (!nzchar(catalog$summary %||% "")) {
    cat(sprintf(
      "  WARNING: %s has no hand-written summary in measure_info.json _catalog -- using an auto-derived fallback. Write a real one via the update-data-sources-index skill.\n",
      source_name
    ))
  }
  dataset_summary <- catalog$summary %||% fallback_summary

  section_id <- gsub("[^a-zA-Z0-9]", "-", source_name)

  # search_terms and bucket: both taken verbatim whenever _catalog HAS the
  # field (even an empty [] -- so clearing one sticks). Both derive from bundle
  # membership only when the field is entirely absent from _catalog, i.e. a
  # brand-new dataset -- they start out identical, then diverge as each is
  # edited independently. To re-derive later, delete the field from _catalog.
  bundles <- source_to_bundles[[source_name]]
  if (is.null(bundles)) bundles <- character(0)
  bundle_labels <- sort(unique(vapply(bundles, format_bundle_label, character(1))))

  catalog_list <- function(field) {
    if (is.null(catalog[[field]])) return(NULL)
    vals <- unlist(catalog[[field]])
    if (is.null(vals)) character(0) else as.character(vals)
  }

  search_terms <- catalog_list("search_terms")
  if (is.null(search_terms)) search_terms <- bundle_labels

  bucket <- catalog_list("bucket")
  if (is.null(bucket)) bucket <- bundle_labels

  cat(sprintf("  Indexing %s (%d/%d)\n", source_name, i, length(source_dirs)))

  # One entry per standardized csv.gz: a short stratification blurb and a direct
  # link. Blurb precedence: the hand-written _catalog$files value, then a value
  # derived from the file name.
  catalog_files <- catalog$files
  if (is.null(catalog_files)) catalog_files <- list()
  standard_files <- get_standard_files(source_dir)
  files_entry <- lapply(sort(basename(standard_files)), function(fn) {
    strat <- catalog_files[[fn]]
    if (is.null(strat) || !nzchar(strat)) strat <- derive_stratification(fn)
    list(
      dataset_stratification = strat,
      dataset_link = github_raw_file(source_name, fn)
    )
  })

  entry <- list(
    dataset = source_name,
    name = display_name,
    github_folder = sprintf("https://github.com/%s/tree/main/data/%s/standard",
                            GITHUB_REPO, source_name),
    data_url = first_source$url %||% "",
    data_dictionary = sprintf("https://pophive.github.io/Ingest/#%s", section_id),
    latest_date = get_latest_date(source_dir),
    search_terms = I(search_terms),
    bucket = I(bucket),
    summary = if (nchar(dataset_summary) > 0) dataset_summary else NA
  )
  entry$files <- I(files_entry)
  entry
})

data_sources_index <- list(
  description = "Index of PopHIVE/Ingest standardized data sources (excludes bundle_* directories).",
  repository = GITHUB_REPO,
  n_datasets = length(index_datasets),
  datasets = index_datasets
)

if (!dir.exists("docs")) dir.create("docs")
index_path <- "docs/data_sources_index.json"
write(
  toJSON(data_sources_index, auto_unbox = TRUE, pretty = TRUE, na = "null"),
  index_path
)
cat(sprintf("Data sources index written to %s (%d datasets)\n",
            index_path, length(index_datasets)))

cat("Done! Documentation generated successfully.\n")
