#!/usr/bin/env Rscript
# =============================================================================
# Render scripts/validate_dataset.Rmd for standard files, outside the repository
#
# Usage (from anywhere inside the repository):
#   Rscript render_plots.R data/<source>/standard/<file>.csv.gz [more files]
#                          [--base <ref>|none] [--head <ref>] [--maps]
#
#   --base   version to compare with (default: merge-base of origin/main and
#            the reviewed version)
#   --head   read the reviewed files from this ref instead of the working tree
#   --maps   also draw the choropleths (needs internet for the map shapes)
#
# The reports go to a folder under the system temp directory. One line is
# printed per file:
#   REPORT <file> | <path to html> | base=<ref> maps=<yes|no>
#   SKIP <file> | <reason>
#
# The comparison file is extracted here and passed as old_data_file because the
# git comparison inside validate_dataset.Rmd does not run from a script.
# =============================================================================

Sys.setenv(LANGUAGE = "en")
for (loc in c("en_US.UTF-8", "C.UTF-8", "English_United States.utf8")) {
  if (nzchar(suppressWarnings(Sys.setlocale("LC_CTYPE", loc)))) break
}

argv <- commandArgs(trailingOnly = TRUE)
opt <- list(base = NULL, head = NULL, maps = FALSE)
files <- character()
i <- 1
while (i <= length(argv)) {
  a <- argv[i]
  if (a == "--maps") {
    opt$maps <- TRUE
  } else if (a %in% c("--base", "--head")) {
    i <- i + 1
    opt[[sub("^--", "", a)]] <- argv[i]
  } else if (grepl("^--(base|head)=", a)) {
    opt[[sub("^--", "", sub("=.*$", "", a))]] <- sub("^[^=]*=", "", a)
  } else {
    files <- c(files, a)
  }
  i <- i + 1
}
if (!length(files)) {
  cat("SKIP all | give at least one standard file\n")
  quit(save = "no", status = 2)
}
for (p in c("rmarkdown", "sys")) {
  if (!requireNamespace(p, quietly = TRUE)) {
    cat("SKIP all | the", p, "package is not installed\n")
    quit(save = "no", status = 2)
  }
}

top <- tryCatch(sys::exec_internal("git", c("rev-parse", "--show-toplevel"), error = FALSE), error = function(e) NULL)
if (is.null(top) || top$status != 0) {
  cat("SKIP all | not inside a git repository\n")
  quit(save = "no", status = 2)
}
ROOT <- normalizePath(trimws(rawToChar(top$stdout)), winslash = "/")
git_line <- function(args) {
  r <- tryCatch(sys::exec_internal("git", c("-C", ROOT, args), error = FALSE), error = function(e) NULL)
  if (is.null(r) || r$status != 0) return(NULL)
  trimws(rawToChar(r$stdout))
}
git_file <- function(ref, rel, dest) {
  con <- file(dest, "wb")
  st <- tryCatch(sys::exec_wait("git", c("-C", ROOT, "show", paste0(ref, ":", rel)), std_out = con, std_err = FALSE),
                 error = function(e) 1L)
  close(con)
  identical(as.integer(st), 0L) && file.size(dest) > 0
}

# pandoc: RStudio and Quarto ship one, plain Rscript does not see it
if (!rmarkdown::pandoc_available()) {
  cand <- c(Sys.getenv("RSTUDIO_PANDOC"),
            "/Applications/RStudio.app/Contents/Resources/app/quarto/bin/tools/aarch64",
            "/Applications/RStudio.app/Contents/Resources/app/quarto/bin/tools/x86_64",
            "/Applications/RStudio.app/Contents/Resources/app/quarto/bin/tools",
            "/Applications/quarto/bin/tools",
            "C:/Program Files/RStudio/resources/app/bin/quarto/bin/tools",
            "C:/Program Files/Quarto/bin/tools",
            "/usr/lib/rstudio/resources/app/bin/quarto/bin/tools")
  for (d in cand[nzchar(cand)]) {
    if (dir.exists(d) && length(list.files(d, pattern = "^pandoc"))) {
      Sys.setenv(RSTUDIO_PANDOC = d)
      rmarkdown::find_pandoc(cache = FALSE)
      if (rmarkdown::pandoc_available()) break
    }
  }
  if (!rmarkdown::pandoc_available() && requireNamespace("pandoc", quietly = TRUE)) {
    try(pandoc::pandoc_activate("latest", quiet = TRUE), silent = TRUE)
  }
}
if (!rmarkdown::pandoc_available()) {
  cat("SKIP all | pandoc not found, see the Pandoc requirement section of CLAUDE.md\n")
  quit(save = "no", status = 2)
}

base <- opt$base
if (is.null(base)) {
  base <- git_line(c("merge-base", "origin/main", if (is.null(opt$head)) "HEAD" else opt$head))
} else if (identical(base, "none")) {
  base <- NULL
}

out_dir <- file.path(dirname(tempdir()), paste0("ingest-review-", format(Sys.time(), "%Y%m%d-%H%M%S"), "-", Sys.getpid()))
if (startsWith(normalizePath(dirname(out_dir), winslash = "/"), paste0(ROOT, "/"))) {
  cat("SKIP all | the temp directory is inside the repository\n")
  quit(save = "no", status = 2)
}
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
rmd <- file.path(out_dir, "validate_dataset.Rmd")
if (is.null(opt$head) || !git_file(opt$head, "scripts/validate_dataset.Rmd", rmd)) {
  invisible(file.copy(file.path(ROOT, "scripts", "validate_dataset.Rmd"), rmd, overwrite = TRUE))
}

for (f in files) {
  rel <- gsub("\\\\", "/", f)
  if (startsWith(rel, ROOT)) rel <- sub("^/", "", substring(rel, nchar(ROOT) + 1))
  if (!grepl("\\.csv(\\.gz)?$", rel)) {
    cat("SKIP ", rel, " | only csv standard files are plotted\n", sep = "")
    next
  }
  tag <- gsub("[^A-Za-z0-9]+", "_", sub("^data/", "", rel))
  if (is.null(opt$head)) {
    cur <- file.path(ROOT, rel)
    if (!file.exists(cur)) {
      cat("SKIP ", rel, " | file not found\n", sep = "")
      next
    }
  } else {
    # keep the folder layout, the Rmd looks for measure_info.json two levels above the data file
    cur <- file.path(out_dir, "head", rel)
    dir.create(dirname(cur), recursive = TRUE, showWarnings = FALSE)
    if (!git_file(opt$head, rel, cur)) {
      cat("SKIP ", rel, " | not found at ", opt$head, "\n", sep = "")
      next
    }
    mi <- file.path(dirname(dirname(rel)), "measure_info.json")
    if (!git_file(opt$head, mi, file.path(out_dir, "head", mi))) unlink(file.path(out_dir, "head", mi))
  }
  old <- ""
  if (!is.null(base)) {
    old <- file.path(out_dir, paste0("base_", tag, ".csv.gz"))
    if (!git_file(base, rel, old)) {
      unlink(old)
      old <- ""
    }
  }
  render_once <- function(maps) {
    suppressWarnings(suppressMessages(rmarkdown::render(
      rmd,
      output_file = paste0("validate_", tag, ".html"),
      output_dir = out_dir,
      intermediates_dir = out_dir,
      knit_root_dir = out_dir,
      params = list(data_file = cur, old_data_file = old, git_compare = FALSE, project_root = ROOT, make_maps = maps),
      envir = new.env(),
      quiet = TRUE
    )))
  }
  maps <- opt$maps
  html <- tryCatch(render_once(maps), error = function(e) e)
  if (inherits(html, "error") && maps) {
    maps <- FALSE
    html <- tryCatch(render_once(FALSE), error = function(e) e)
  }
  if (inherits(html, "error")) {
    cat("SKIP ", rel, " | render failed: ", gsub("[\r\n]+", " ", conditionMessage(html)), "\n", sep = "")
  } else {
    cat("REPORT ", rel, " | ", normalizePath(html, winslash = "/"), " | base=",
        if (nzchar(old)) substr(base, 1, 10) else "none", " maps=", if (maps) "yes" else "no", "\n", sep = "")
  }
}
quit(save = "no", status = 0)
