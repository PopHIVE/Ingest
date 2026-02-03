# =============================================================================
# Build Data Source Documentation
# Generates HTML documentation from measure_info.json files
# =============================================================================

library(jsonlite)
library(vroom)
library(htmltools)
library(glue)

# -----------------------------------------------------------------------------
# Helper: Null coalescing operator (avoids rlang dependency)
# -----------------------------------------------------------------------------
`%||%` <- function(x, y) if (is.null(x) || (is.character(x) && nchar(x) == 0)) y else x

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

#' Find all standard data files for a source
get_standard_files <- function(source_dir) {
  standard_dir <- file.path(source_dir, "standard")
  if (!dir.exists(standard_dir)) return(character(0))

  files <- list.files(standard_dir, pattern = "\\.csv\\.gz$", full.names = TRUE)
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
          test_name <- key
          test_name <- gsub("\\{category(\\.[^}]*)?\\}", cat_val, test_name)
          test_name <- gsub("\\{variant(\\.[^}]*)?\\}", var_val, test_name)
          if (test_name == var_name) {
            captured <- list(category = cat_val, variant = var_val)
            return(resolve_template(info, captured))
          }
        }
      }
    } else if (has_variant && !is.null(variant_keys)) {
      # Only variant placeholder
      for (var_val in variant_keys) {
        test_name <- gsub("\\{variant(\\.[^}]*)?\\}", var_val, key)
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
      text <- gsub(paste0("\\{", ph_name, "\\}"), captured[[ph_name]], text)
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

#' Generate HTML for a single variable row
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

#' Generate HTML for a data file section
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

# -----------------------------------------------------------------------------
# Main Script
# -----------------------------------------------------------------------------

cat("Building data source documentation...\n")

# Find all data sources (exclude bundles)
data_dir <- "data"
all_dirs <- list.dirs(data_dir, recursive = FALSE, full.names = TRUE)
source_dirs <- all_dirs[!grepl("bundle_", basename(all_dirs))]

# Filter to only those with measure_info.json
source_dirs <- source_dirs[sapply(source_dirs, function(d) {
  file.exists(file.path(d, "measure_info.json"))
})]

cat(sprintf("Found %d data sources with measure_info.json\n", length(source_dirs)))

# Sort alphabetically
source_dirs <- source_dirs[order(basename(source_dirs))]
source_names <- basename(source_dirs)

# Generate navigation items
nav_items <- lapply(source_names, function(name) {
  section_id <- gsub("[^a-zA-Z0-9]", "-", name)
  tags$li(class = "nav-item",
    tags$a(class = "nav-link", href = paste0("#", section_id), format_source_name(name))
  )
})

# Generate source sections
cat("Generating documentation sections...\n")
source_sections <- lapply(seq_along(source_dirs), function(i) {
  cat(sprintf("  Processing %s (%d/%d)\n", source_names[i], i, length(source_dirs)))
  make_source_section(source_names[i], source_dirs[i])
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
    "))
  ),
  tags$body(
    # Fixed navbar
    tags$nav(class = "navbar navbar-dark fixed-top",
      tags$div(class = "container-fluid",
        tags$a(class = "navbar-brand", href = "#", "PopHIVE Data Documentation"),
        tags$span(class = "navbar-text text-light",
          sprintf("Last updated: %s", format(Sys.Date(), "%B %d, %Y"))
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
            tags$ul(class = "nav flex-column nav-pills", nav_items)
          )
        ),

        # Main content
        tags$main(class = "col-md-9 ms-sm-auto col-lg-10 px-md-4",
          tags$div(class = "pt-3",
            tags$h1("PopHIVE Data Source Documentation"),
            tags$p(class = "lead text-muted",
              "This page documents all data sources in the PopHIVE/Ingest repository, ",
              "including variable definitions, data types, and source information."
            ),
            tags$hr(),
            source_sections
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

cat("Done! Documentation generated successfully.\n")
