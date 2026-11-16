#!/usr/bin/env Rscript
# =============================================================================
# Read-only QC checks for data/<dir> folders (sources and bundles)
#
# Usage (from anywhere inside the repository):
#   Rscript review_checks.R data/<dir> [more dirs] [--base <ref>|none]
#                           [--head <ref>] [--files a.csv.gz,b.parquet]
#                           [--max-rows N]
#
#   --base      ref to compare with. Default: merge-base of origin/main and the
#               reviewed version. "none" turns the comparison off (folder audit).
#   --head      read the reviewed files from this ref instead of the working tree
#   --files     only check these standard/dist files (base names)
#   --max-rows  rows read per file (default 3000000)
#
# One finding per line:
#   <FAIL|WARN|ASK|HUMAN|INFO|PASS|SKIP> <ID> <path>[:<column>] | <message> | k=v; k=v
# FAIL and WARN keep their level only when the problem is new or worse than on
# the base version. Problems that were already there are counted on one INFO
# line, or printed as INFO with "already on base".
#
# Nothing is written inside the repository. Files read from a ref go to a
# session temp folder. Exit status: 0 the checks ran, 2 they could not run
# (ENV FAIL line), 3 they ran but a check stopped with an R error (FAIL X00).
# =============================================================================

Sys.setenv(LANGUAGE = "en")
for (loc in c("en_US.UTF-8", "C.UTF-8", "English_United States.utf8")) {
  if (nzchar(suppressWarnings(Sys.setlocale("LC_CTYPE", loc)))) break
}
options(warn = 1, scipen = 999)

stop_env <- function(...) {
  cat("ENV FAIL | ", ..., "\n", sep = "")
  quit(save = "no", status = 2)
}
need <- c("jsonlite", "vroom", "sys")
miss <- need[!vapply(need, requireNamespace, NA, quietly = TRUE)]
if (length(miss)) stop_env("missing R packages: ", paste(miss, collapse = ", "))

# -----------------------------------------------------------------------------
# Arguments
# -----------------------------------------------------------------------------
argv <- commandArgs(trailingOnly = TRUE)
opt <- list(base = NULL, head = NULL, files = NULL, `max-rows` = "3000000")
dirs <- character()
i <- 1
while (i <= length(argv)) {
  a <- argv[i]
  if (grepl("^--", a)) {
    key <- sub("^--", "", sub("=.*$", "", a))
    if (!key %in% names(opt)) stop_env("unknown option ", a)
    if (grepl("=", a)) {
      opt[[key]] <- sub("^[^=]*=", "", a)
    } else {
      i <- i + 1
      opt[[key]] <- argv[i]
    }
  } else {
    dirs <- c(dirs, a)
  }
  i <- i + 1
}
if (!length(dirs)) stop_env("give at least one data/<dir> folder")
MAX_ROWS <- as.numeric(opt[["max-rows"]])
ONLY_FILES <- if (is.null(opt$files)) NULL else strsplit(opt$files, ",")[[1]]

# -----------------------------------------------------------------------------
# Git access (no shell, binary safe)
# -----------------------------------------------------------------------------
if (!nzchar(Sys.which("git"))) stop_env("git is not on the PATH")
top <- tryCatch(sys::exec_internal("git", c("rev-parse", "--show-toplevel"), error = FALSE),
                error = function(e) NULL)
if (is.null(top) || top$status != 0) stop_env("not inside a git repository")
ROOT <- normalizePath(trimws(rawToChar(top$stdout)), winslash = "/")

git <- function(args) {
  r <- tryCatch(sys::exec_internal("git", c("-C", ROOT, "-c", "core.quotepath=false", args), error = FALSE),
                error = function(e) NULL)
  if (is.null(r) || r$status != 0) return(NULL)
  out <- rawToChar(r$stdout)
  Encoding(out) <- "UTF-8"
  strsplit(out, "\n", fixed = TRUE)[[1]]
}
git_to_file <- function(args, dest) {
  con <- file(dest, "wb")
  st <- tryCatch(sys::exec_wait("git", c("-C", ROOT, args), std_out = con, std_err = FALSE),
                 error = function(e) 1L)
  close(con)
  identical(as.integer(st), 0L)
}

HEAD_REF <- opt$head
if (!is.null(HEAD_REF) && is.null(git(c("rev-parse", "--verify", "--quiet", paste0(HEAD_REF, "^{commit}"))))) {
  stop_env("--head ", HEAD_REF, " is not a commit in this repository")
}
BASE_REF <- opt$base
if (is.null(BASE_REF)) {
  BASE_REF <- git(c("merge-base", "origin/main", if (is.null(HEAD_REF)) "HEAD" else HEAD_REF))
  if (is.null(BASE_REF) || !length(BASE_REF)) BASE_REF <- NULL
} else if (identical(BASE_REF, "none")) {
  BASE_REF <- NULL
} else if (is.null(git(c("rev-parse", "--verify", "--quiet", paste0(BASE_REF, "^{commit}"))))) {
  stop_env("--base ", BASE_REF, " is not a commit in this repository")
}

short_sha <- function(ref) {
  r <- git(c("rev-parse", "--short=10", paste0(ref, "^{commit}")))
  if (is.null(r) || !length(r)) ref else r[1]
}
if (!is.null(HEAD_REF) && !is.null(BASE_REF) &&
    identical(git(c("rev-parse", paste0(BASE_REF, "^{commit}"))), git(c("rev-parse", paste0(HEAD_REF, "^{commit}"))))) {
  stop_env("base and head are the same commit. For a merged PR pass --base <merge commit>^")
}
# Did any ingest.R, build.R or shared script change between base and head?
# Value changes with no code change are revisions by the data source.
CODE_CHANGED <- TRUE
if (!is.null(BASE_REF)) {
  ch <- git(c("diff", "--name-only", BASE_REF, HEAD_REF, "--", "data", "scripts"))
  if (is.null(HEAD_REF)) ch <- c(ch, git(c("ls-files", "--others", "--exclude-standard", "--", "data", "scripts")))
  CODE_CHANGED <- any(grepl("^data/[^/]+/(ingest|build)\\.R$|^scripts/", as.character(ch)))
}

TMP <- tempfile("ingest_review_")
dir.create(TMP)

side_ref <- function(side) if (side == "head") HEAD_REF else BASE_REF

# Local path of a repo file on one side, or NULL when it does not exist there.
fetch <- function(side, rel) {
  ref <- side_ref(side)
  if (side == "head" && is.null(ref)) {
    p <- file.path(ROOT, rel)
    return(if (file.exists(p) && !dir.exists(p)) p else NULL)
  }
  if (is.null(ref)) return(NULL)
  out <- file.path(TMP, side, rel)
  if (file.exists(out)) return(out)
  dir.create(dirname(out), recursive = TRUE, showWarnings = FALSE)
  if (!git_to_file(c("show", paste0(ref, ":", rel)), out)) {
    unlink(out)
    return(NULL)
  }
  out
}

# Files under a folder on one side: data frame with path and size (bytes).
ls_side <- function(side, dir_rel) {
  ref <- side_ref(side)
  empty <- data.frame(path = character(), size = numeric(), stringsAsFactors = FALSE)
  if (side == "base" && is.null(ref)) return(empty)
  if (is.null(ref)) {
    f <- as.character(unique(c(git(c("ls-files", "--", dir_rel)),
                               git(c("ls-files", "--others", "--exclude-standard", "--", dir_rel)))))
    f <- f[!is.na(f) & nzchar(f)]
    f <- f[file.exists(file.path(ROOT, f))]
    return(data.frame(path = f, size = file.size(file.path(ROOT, f)), stringsAsFactors = FALSE))
  }
  l <- git(c("ls-tree", "-r", "-l", ref, "--", dir_rel))
  if (is.null(l) || !length(l)) return(empty)
  meta <- strsplit(sub("\t.*$", "", l), " +")
  data.frame(path = sub("^[^\t]*\t", "", l),
             size = suppressWarnings(as.numeric(vapply(meta, function(m) m[length(m)], ""))),
             stringsAsFactors = FALSE)
}

blob_id <- function(side, rel) {
  ref <- side_ref(side)
  if (side == "head" && is.null(ref)) {
    if (!file.exists(file.path(ROOT, rel))) return(NA_character_)
    r <- git(c("hash-object", "--", rel))
  } else {
    if (is.null(ref)) return(NA_character_)
    r <- git(c("rev-parse", "--verify", "--quiet", paste0(ref, ":", rel)))
  }
  if (is.null(r) || !length(r)) NA_character_ else r[1]
}
same_as_base <- function(rel) {
  a <- blob_id("head", rel)
  b <- blob_id("base", rel)
  !is.na(a) && !is.na(b) && identical(a, b)
}

# -----------------------------------------------------------------------------
# Output
# -----------------------------------------------------------------------------
COUNT <- new.env()
reset_counts <- function() for (s in c("FAIL", "WARN", "ASK", "HUMAN", "INFO", "PASS", "SKIP")) assign(s, 0L, COUNT)
clip <- function(x, n = 240) {
  x <- gsub("[\r\n\t]+", " ", x)
  ifelse(nchar(x) > n, paste0(substr(x, 1, n), "..."), x)
}
some <- function(x, n = 5) {
  x <- unique(as.character(x))
  paste0(paste(utils::head(x, n), collapse = ", "), if (length(x) > n) sprintf(" (+%d more)", length(x) - n) else "")
}
emit <- function(sev, id, target, msg, kv = "") {
  assign(sev, get(sev, COUNT) + 1L, COUNT)
  cat(sev, " ", id, " ", target, " | ", clip(msg, 400), if (nzchar(kv)) paste0(" | ", clip(kv, 400)), "\n", sep = "")
}

# Issues are collected per file so the same check can run on the base version.
new_acc <- function() {
  e <- new.env()
  e$l <- list()
  e
}
add <- function(acc, sev, id, target, msg, kv = "", key = "", n = 1, items = NULL) {
  acc$l[[length(acc$l) + 1]] <- list(sev = sev, id = id, target = target, msg = msg, kv = kv,
                                     key = paste(id, key), n = as.numeric(n), items = unique(as.character(items)))
  invisible(NULL)
}
SEV_RANK <- c(PASS = 0, SKIP = 0, INFO = 1, HUMAN = 1, ASK = 2, WARN = 3, FAIL = 4)
# A folder-level finding whose inputs are identical to base is not news.
emit_base <- function(sev, id, target, msg, kv = "", same = FALSE) {
  if (same && sev %in% c("FAIL", "WARN", "ASK")) {
    emit("INFO", id, target, paste0(msg, " (already on base, this change does not touch it)"), kv)
  } else {
    emit(sev, id, target, msg, kv)
  }
}
# Print head issues. A FAIL, WARN or ASK collapses into one INFO line per check
# when the base had the same finding at the same or a higher level and nothing
# new: the same offending items, or for plain counts no more than on base after
# allowing for the growth of the file. HUMAN pointers follow their finding: they
# are dropped when it collapses or when the base already had that item, and at
# most four are printed per finding.
report <- function(acc, base_acc, rel, show_old = TRUE) {
  bl <- if (is.null(base_acc)) list() else base_acc$l
  bk <- vapply(bl, function(x) x$key, "")
  grow <- 1
  if (!is.null(base_acc) && isTRUE(base_acc$rows > 0) && isTRUE(acc$rows > 0)) grow <- max(1, acc$rows / base_acc$rows) * 1.02
  old <- list()
  collapsed <- character()
  shown <- list()
  for (x in acc$l) {
    j <- match(x$key, bk)
    if (x$sev == "HUMAN") {
      if (x$key %in% collapsed) next
      if (!is.na(j) && length(x$items) && all(x$items %in% bl[[j]]$items)) next
      shown[[x$key]] <- c(shown[[x$key]], 1)
      if (length(shown[[x$key]]) > 4) next
      emit(x$sev, x$id, x$target, x$msg, x$kv)
      next
    }
    known <- FALSE
    fresh <- character()
    if (x$sev %in% c("FAIL", "WARN", "ASK") && !is.na(j) && SEV_RANK[[bl[[j]]$sev]] >= SEV_RANK[[x$sev]]) {
      if (length(x$items)) {
        fresh <- setdiff(x$items, bl[[j]]$items)
        known <- !length(fresh)
      } else {
        known <- bl[[j]]$n * grow >= x$n
      }
    }
    if (known) {
      old[[x$id]] <- c(old[[x$id]], x$sev)
      collapsed <- c(collapsed, x$key)
    } else {
      kv <- x$kv
      if (length(fresh) && length(fresh) < length(x$items)) kv <- paste0(kv, if (nzchar(kv)) "; ", "new_since_base=", length(fresh), ": ", some(fresh, 4))
      emit(x$sev, x$id, x$target, x$msg, kv)
    }
  }
  if (show_old) for (id in names(old)) {
    emit("INFO", id, rel, "already on base and not made worse",
         paste0("items=", length(old[[id]]), "; level=", paste(sort(unique(old[[id]])), collapse = "/")))
  }
}

# -----------------------------------------------------------------------------
# Project conventions
# -----------------------------------------------------------------------------
RULE <- list(monthly = "last", annual = "12-31")
invisible(local({
  p <- file.path(ROOT, "CLAUDE.md")
  if (!file.exists(p)) return()
  l <- readLines(p, warn = FALSE, encoding = "UTF-8")
  s <- grep("^#+ +Time Standards", l)
  if (!length(s)) return()
  blk <- l[s[1]:min(length(l), s[1] + 12)]
  m <- grep("Monthly data", blk, value = TRUE)
  if (length(m)) RULE$monthly <<- if (grepl("first", m[1], ignore.case = TRUE)) "first" else "last"
  a <- regmatches(blk, regexpr("YYYY-[0-9]{2}-[0-9]{2}", blk))
  if (length(a)) RULE$annual <<- sub("YYYY-", "", a[1])
}))

STANDARD_VARS <- c("geography", "time", "age", "sex", "race_ethnicity", "virus", "date", "year")
invisible(local({
  p <- file.path(ROOT, "scripts", "build_docs.R")
  if (!file.exists(p)) return()
  l <- readLines(p, warn = FALSE, encoding = "UTF-8")
  s <- grep("^STANDARD_VARS *<- *list\\(", l)
  if (!length(s)) return()
  e <- s[1] + which(grepl("^\\)", l[(s[1] + 1):length(l)]))[1]
  if (is.na(e)) return()
  nm <- regmatches(l[s[1]:e], regexpr("^ {0,2}[A-Za-z_][A-Za-z0-9_]*(?= *= *list\\()", l[s[1]:e], perl = TRUE))
  STANDARD_VARS <<- unique(c(STANDARD_VARS, trimws(nm)))
}))

FIPS <- local({
  p <- file.path(ROOT, "resources", "all_fips.csv.gz")
  if (!file.exists(p)) return(character())
  d <- vroom::vroom(p, col_types = vroom::cols(.default = vroom::col_character()),
                    show_col_types = FALSE, progress = FALSE, altrep = FALSE)
  unique(d$geography)
})

ISO <- "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
FLAG_RE <- "(^|_)(suppress|suppressed|suppression|flag|not_asked|not_collected|not_reported|imputed|is_state_estimate|low_coverage)(_|$)"
SUPP_RE <- "suppress"
BOUND_RE <- "(^|_)(lcl|ucl|lower|upper|lb|ub|se)$"
NUM_STRAT <- "^(fips|[a-z]+_fips|age|age_group|sex|race|race_ethnicity|ethnicity|hispanic|grade|year|week|season|month|quarter|mmwr_week|mmwr_year|epiweek)$"
VOCAB_COLS <- c("age", "age_group", "sex", "race_ethnicity", "race", "ethnicity", "income", "insurance",
                "urban", "poverty", "education", "grade")
AGG <- c("Total", "Overall", "All", "All ages", "All Ages", "Both sexes", "All races")
# stratifier names and source-label rules as in the MCP server's verify/onboarding.py
MCP_STRAT <- "^(age|sex|race|race_ethnicity|ethnic|ethnicity|vaccine|vax|dose|grade|type|intent|serotype|variant|insurance|urban|urbanicity|income|poverty|subtype|cause|drug|category|survey_type|payer)$"
MCP_LABEL_OK <- "^[A-Za-z0-9 ,.%()/+&'\u2013:\u2265<>-]+$"
MCP_LABEL_BAD <- "http|www\\.|\\b(ignore|instructions?|system|assistant|prompt|you must|disregard|override)\\b|the standard|the primary|the official|the most reliable|the best|authoritative|gold standard|main source|definitive|trustworthy|the canonical"
META_RE <- "^date_|_date$|scraped|updated|accessed|^vintage$"
NUM_TYPE <- "^(double|float|halffloat|int|uint|decimal)"
MCP_EXTRA <- c("value", "value_lcl", "value_ucl", "lcl", "ucl", "sample_size", "n", "suppressed_flag",
               "suppressed", "is_state_estimate")
COUNT_RE <- "(^|_)(n|count|counts|cases|deaths|visits|admissions|events|births|hospitalizations)(_|$)"
PCT_NAME <- "(^|_)(pct|percent)(_|$)"
PCT_SKIP_NAME <- "_pct_[0-9]+$|percentile"
PCT_OVER_OK <- "expected|occupan|(^|_)occ(_|$)|ratio|change|increase"

num <- function(x) suppressWarnings(as.numeric(x))
SEP_NUM <- "^-?[0-9]{1,3}(,[0-9]{3})+(\\.[0-9]+)?$|^-?[0-9]+(\\.[0-9]+)? ?%$"

# -----------------------------------------------------------------------------
# Reading
# -----------------------------------------------------------------------------
# Every table is held as character columns. For parquet the stored types are
# kept in $types so numeric columns do not have to be guessed.
read_table <- function(path) {
  if (grepl("\\.parquet$", path, ignore.case = TRUE)) {
    if (!requireNamespace("arrow", quietly = TRUE)) stop("the arrow package is needed to read parquet")
    tab <- arrow::read_parquet(path, as_data_frame = FALSE)
    n_all <- tab$num_rows
    if (n_all > MAX_ROWS) tab <- tab$Slice(0, MAX_ROWS)
    types <- vapply(tab$schema$fields, function(f) f$type$ToString(), "")
    names(types) <- names(tab)
    d <- as.data.frame(tab)
    d <- as.data.frame(lapply(d, function(x) {
      if (inherits(x, "Date") || inherits(x, "POSIXt")) format(x, "%Y-%m-%d") else as.character(x)
    }), stringsAsFactors = FALSE, check.names = FALSE)
    return(list(d = d, types = types, rows = n_all, cut = n_all > MAX_ROWS, delim = NA))
  }
  first <- tryCatch(readLines(path, n = 1, warn = FALSE), error = function(e) "")
  delim <- if (length(first) && grepl("\t", first) && !grepl(",", first)) "\t" else ","
  d <- vroom::vroom(path, delim = delim, col_types = vroom::cols(.default = vroom::col_character()),
                    na = c("", "NA"), altrep = FALSE, show_col_types = FALSE, progress = FALSE,
                    n_max = MAX_ROWS + 1)
  d <- as.data.frame(d, stringsAsFactors = FALSE, check.names = FALSE)
  cut <- nrow(d) > MAX_ROWS
  if (cut) d <- d[seq_len(MAX_ROWS), , drop = FALSE]
  enc <- character()
  for (v in names(d)) {
    bad <- !is.na(d[[v]]) & !validUTF8(d[[v]])
    if (any(bad)) {
      d[[v]][bad] <- iconv(d[[v]][bad], "latin1", "UTF-8")
      enc <- c(enc, v)
    }
  }
  list(d = d, types = NULL, rows = nrow(d), cut = cut, delim = delim, enc = enc)
}

# force_values: columns to treat as values whatever they hold (numeric on base,
# text now). meta: scrape and update dates, kept out of keys, strata and values.
roles <- function(tab, force_values = character()) {
  d <- tab$d
  n <- names(d)
  if (is.null(tab$types)) {
    share <- vapply(d, function(x) {
      x <- x[!is.na(x)]
      if (!length(x)) 1 else mean(!is.na(num(x)))
    }, 0)
    loose <- vapply(d, function(x) {
      x <- x[!is.na(x)]
      if (!length(x)) 1 else mean(!is.na(num(x)) | grepl(SEP_NUM, x))
    }, 0)
    is_num <- loose >= 0.5
  } else {
    is_num <- grepl("^(double|float|halffloat|int|uint|decimal)", tab$types[n])
    share <- ifelse(is_num, 1, 0)
  }
  names(is_num) <- n
  names(share) <- n
  geo <- intersect("geography", n)
  tcol <- intersect(c("time", "date", "week_end", "year"), n)[1]
  rest <- setdiff(n, c(geo, tcol))
  meta <- rest[grepl(META_RE, rest, ignore.case = TRUE)]
  rest <- setdiff(rest, meta)
  flag_like <- rest[grepl(FLAG_RE, rest, ignore.case = TRUE)]
  flags <- flag_like[vapply(flag_like, function(f) {
    all(unique(d[[f]]) %in% c("0", "1", "TRUE", "FALSE", "true", "false", NA))
  }, NA)]
  rest <- setdiff(rest, flags)
  bounds <- rest[grepl(BOUND_RE, rest) & is_num[rest]]
  rest <- setdiff(rest, bounds)
  strata <- rest[!is_num[rest] | grepl(NUM_STRAT, rest)]
  strata <- setdiff(strata, force_values)
  values <- setdiff(rest, strata)
  list(geo = geo, time = tcol, flags = flags, bounds = bounds, strata = strata, values = values, meta = meta,
       text = values[share[values] < 1])
}

key_of <- function(d, cols) {
  if (!length(cols)) return(rep("all", nrow(d)))
  do.call(paste, c(unname(as.list(d[cols])), sep = "\r"))
}
show_key <- function(k) gsub("\r", " / ", k, fixed = TRUE)

cadence_of <- function(dt) {
  if (length(dt) < 2) return("single")
  gap <- as.numeric(diff(dt))
  mg <- as.numeric(names(which.max(table(gap))))
  if (mg == 1) "daily" else if (mg >= 6 && mg <= 8) "weekly" else if (mg >= 27 && mg <= 35) "monthly" else
    if (mg >= 85 && mg <= 95) "quarterly" else if (mg >= 350 && mg <= 380) "annual" else "irregular"
}

# Flags are matched to value columns by the name tokens they share. A value
# column keeps only the flag or flags that share the most tokens with it, so a
# general suppressed_flag covers the columns that have no flag of their own.
flag_map <- function(flags, values) {
  out <- structure(vector("list", length(flags)), names = flags)
  if (!length(flags) || !length(values)) return(out)
  drop <- c("suppressed", "suppress", "suppression", "flag", "is", "not", "asked", "collected", "reported", "imputed")
  vt <- strsplit(tolower(values), "_")
  score <- vapply(flags, function(f) {
    tk <- setdiff(strsplit(tolower(f), "_")[[1]], drop)
    if (!length(tk)) rep(0, length(values)) else vapply(vt, function(v) if (all(tk %in% v)) length(tk) else -1, 0)
  }, numeric(length(values)))
  score <- matrix(score, nrow = length(values))
  for (i in seq_along(values)) {
    if (all(score[i, ] < 0)) next
    for (f in flags[score[i, ] == max(score[i, ])]) out[[f]] <- c(out[[f]], values[i])
  }
  out
}
is_on <- function(x) x %in% c("1", "TRUE", "true")

# -----------------------------------------------------------------------------
# measure_info
# -----------------------------------------------------------------------------
read_info_json <- function(path) {
  info <- jsonlite::read_json(path)
  prep <- function(l) {
    if (is.null(l)) return(list(list(default = "")))
    if (!is.list(l)) l <- structure(lapply(unlist(l), function(e) list()), names = unlist(l))
    for (n in names(l)) {
      if (!is.list(l[[n]])) l[[n]] <- list()
      if (is.null(l[[n]]$default)) l[[n]]$default <- if (n == "blank") "" else n
    }
    l
  }
  fill <- function(x, pat, src) {
    for (t in unique(regmatches(x, gregexpr(pat, x, perl = TRUE))[[1]])) {
      f <- sub("^\\{[^.}]+\\.?", "", sub("\\}$", "", t))
      v <- src[[if (f == "" || is.null(src[[f]])) "default" else f]]
      x <- gsub(t, if (is.null(v)) "" else as.character(v), x, fixed = TRUE)
    }
    x
  }
  cp <- "\\{categor(?:y|ies)(?:\\.[^}]+?)?\\}"
  vp <- "\\{variants?(?:\\.[^}]+?)?\\}"
  out <- list()
  for (n in names(info)) {
    e <- info[[n]]
    if (startsWith(n, "_") || !is.list(e) || (is.null(e$variants) && is.null(e$categories))) {
      out[[n]] <- e
      next
    }
    base <- e
    base$variants <- NULL
    base$categories <- NULL
    for (cs in prep(e$categories)) for (vs in prep(e$variants)) {
      ent <- lapply(base, function(v) if (is.character(v) && length(v) == 1) fill(fill(v, cp, cs), vp, vs) else v)
      extra <- c(cs, vs[!names(vs) %in% names(cs)])
      extra <- extra[!names(extra) %in% c("default", "name", names(ent))]
      out[[fill(fill(n, cp, cs), vp, vs)]] <- c(ent, extra)
    }
  }
  out
}
# Returns list(raw =, rendered =, error =). Rendering expands {variant} keys.
load_info <- function(side, dir_rel) {
  p <- fetch(side, file.path(dir_rel, "measure_info.json"))
  if (is.null(p)) return(list(raw = NULL, rendered = list(), error = "missing"))
  raw <- tryCatch(jsonlite::read_json(p), error = function(e) e)
  if (inherits(raw, "error")) return(list(raw = NULL, rendered = list(), error = conditionMessage(raw)))
  rendered <- NULL
  if (requireNamespace("dcf", quietly = TRUE)) {
    rendered <- tryCatch(suppressWarnings(suppressMessages(
      dcf::dcf_measure_info(p, render = TRUE, write = FALSE, verbose = FALSE, open_after = FALSE))),
      error = function(e) NULL)
  }
  if (is.null(rendered)) rendered <- tryCatch(read_info_json(p), error = function(e) raw)
  list(raw = raw, rendered = rendered, error = NULL)
}
info_ids <- function(rendered) {
  unique(c(names(rendered), unlist(lapply(rendered, function(e) if (is.list(e)) e$source_id))))
}
entry_for <- function(rendered, col, file_id = NULL) {
  e <- rendered[[col]]
  if (is.null(e) && !is.null(file_id)) e <- rendered[[paste0(file_id, "|", col)]]
  if (is.list(e)) e else NULL
}
txt <- function(x) if (is.null(x)) "" else paste(unlist(x), collapse = " ")

# -----------------------------------------------------------------------------
# Checks on one standard file
# -----------------------------------------------------------------------------
geo_class <- function(g) {
  ifelse(is.na(g), "NA",
  ifelse(g == "00", "national",
  ifelse(grepl("^[0-9]{2}$", g), "state",
  ifelse(grepl("^[0-9]{5}$", g), "county",
  ifelse(grepl("^hhs_([1-9]|10)$", g), "hhs",
  ifelse(grepl("^[0-9]{3}$", g), "3digit",
  ifelse(grepl("^[0-9]+$", g), "other_digits",
  ifelse(grepl("^[a-z0-9_]+$", g), "slug", "name"))))))))
}

check_standard <- function(tab, rel, rendered, acc, quiet = FALSE, force_values = character()) {
  d <- tab$d
  r <- roles(tab, force_values)
  n <- names(d)
  acc$rows <- nrow(d)
  base_name <- basename(rel)
  dir_name <- basename(dirname(dirname(rel)))
  say <- function(...) if (!quiet) add(acc, ...)

  if (identical(tab$delim, "\t")) add(acc, "FAIL", "S05", rel, "file is tab-delimited, standard files are comma-delimited", key = "tab")
  if (length(tab$enc)) add(acc, "FAIL", "D10", rel, "text that is not valid UTF-8 (read here as Latin-1)", paste0("columns=", some(tab$enc)), key = "encoding", items = tab$enc)
  if (tab$cut) say("INFO", "D00", rel, sprintf("only the first %d rows were read", nrow(d)))
  say("INFO", "D00", rel, "columns by role",
      sprintf("rows=%d; time=%s; strata=%s; values=%d; bounds=%d; flags=%s", tab$rows,
              if (is.na(r$time)) "none" else r$time, if (length(r$strata)) some(r$strata, 8) else "none",
              length(r$values), length(r$bounds), if (length(r$flags)) some(r$flags, 6) else "none"))
  if (length(r$meta)) say("INFO", "D00", rel, "scrape or update date columns, left out of keys and comparisons", paste0("columns=", some(r$meta)))

  bad_names <- n[!grepl("^[a-z][a-z0-9_]*$", n)]
  if (length(bad_names)) add(acc, "WARN", "D11", rel, "column names are not lowercase with underscores",
                            paste0("columns=", some(bad_names)), key = paste(sort(bad_names), collapse = ","), n = length(bad_names))

  if (!nrow(d)) {
    add(acc, "FAIL", "S05", rel, "file has no rows", key = "empty")
    return(invisible(r))
  }

  # --- geography -------------------------------------------------------------
  if (!length(r$geo)) {
    add(acc, "FAIL", "D01", rel, "no geography column", key = "geo")
  } else {
    g <- d[[r$geo]]
    cls <- geo_class(g)
    ok <- c("national", "state", "county")
    if (grepl("region|hhs", base_name)) ok <- c(ok, "hhs")
    if (grepl("substate|city|local", base_name)) ok <- c(ok, "slug", "hhs")
    if (grepl("zcta|zip", base_name)) ok <- c(ok, "3digit")
    if (grepl("dma|hsa|site|school", base_name)) ok <- unique(cls)
    tb <- table(cls)
    say("INFO", "D02", rel, "geography values by class", paste(names(tb), tb, sep = "=", collapse = "; "))
    for (k in setdiff(names(tb), ok)) {
      what <- switch(k, `NA` = "missing geography", name = "geography holds names, not FIPS codes",
                     slug = "geography holds text codes, not FIPS codes", hhs = "HHS regions mixed into a state or county file",
                     `3digit` = "3-digit geography codes", "geography codes that are not 2 or 5 digits")
      add(acc, "FAIL", "D02", paste0(rel, ":", r$geo), what,
          sprintf("rows=%d; examples=%s", tb[[k]], some(g[cls == k], 4)), key = k, n = tb[[k]] / nrow(d),
          items = if (k != "NA") g[cls == k])
    }
    if (length(FIPS)) {
      unk <- unique(g[cls %in% c("state", "county") & !g %in% FIPS])
      if (length(unk)) add(acc, "WARN", "D02", paste0(rel, ":", r$geo), "codes not in resources/all_fips.csv.gz",
                           sprintf("codes=%d; examples=%s", length(unk), some(unk, 6)), key = "unknown", items = unk)
    }
  }

  # --- time --------------------------------------------------------------------
  dt <- as.Date(character())
  w <- numeric()
  cad <- "none"
  if (is.na(r$time)) {
    add(acc, "FAIL", "D01", rel, "no time column", key = "time")
  } else {
    tv <- d[[r$time]]
    if (r$time == "year" && all(grepl("^[0-9]{4}$", tv[!is.na(tv)]))) {
      say("INFO", "D03", rel, "time is a year column, date checks skipped")
    } else {
      iso <- grepl(ISO, tv)
      n_na <- sum(is.na(tv))
      if (n_na) add(acc, "FAIL", "D03", paste0(rel, ":", r$time), "missing time values", sprintf("rows=%d", n_na), key = "na", n = n_na / nrow(d))
      bad <- !iso & !is.na(tv)
      if (any(bad)) add(acc, "FAIL", "D03", paste0(rel, ":", r$time), "time is not YYYY-mm-dd",
                        sprintf("rows=%d; examples=%s", sum(bad), some(tv[bad], 3)), key = "format", items = tv[bad])
      ut <- unique(tv[iso])
      ud <- as.Date(ut, format = "%Y-%m-%d")
      imp <- ut[is.na(ud)]
      if (length(imp)) add(acc, "FAIL", "D03", paste0(rel, ":", r$time), "dates that do not exist in the calendar",
                           sprintf("rows=%d; examples=%s", sum(tv %in% imp), some(imp, 3)), key = "impossible", items = imp)
      dt <- sort(ud[!is.na(ud)])
      w <- as.numeric(table(factor(tv[iso], levels = format(dt, "%Y-%m-%d"))))
    }
  }
  if (length(dt)) {
    core <- dt[w >= 0.5 * stats::quantile(w, 0.9, names = FALSE)]
    cad <- cadence_of(if (length(core) >= 3) core else dt)
    say("INFO", "D04", rel, "time range and cadence",
        sprintf("cadence=%s; dates=%d; from=%s; to=%s", cad, length(dt), min(dt), max(dt)))
    fut <- dt[dt > Sys.Date() + 7]
    if (length(fut)) add(acc, "WARN", "D03", paste0(rel, ":", r$time), "dates in the future",
                         sprintf("dates=%d; latest=%s", length(fut), max(fut)), key = "future", items = format(fut))
    md <- format(dt, "%m-%d")
    dom <- ifelse(format(dt + 1, "%d") == "01", "last", ifelse(format(dt, "%d") == "01", "first", "other"))
    if (cad == "weekly") {
      off <- dt[format(dt, "%u") != "6"]
      if (length(off)) add(acc, "FAIL", "D04", paste0(rel, ":", r$time), "weekly dates that are not Saturdays",
                           sprintf("dates=%d of %d; rows=%d of %d; examples=%s", length(off), length(dt),
                                   sum(w[format(dt, "%u") != "6"]), sum(w), some(off, 3)), key = "saturday", items = format(off))
      else say("PASS", "D04", rel, "all weekly dates are Saturdays")
    } else if (cad == "monthly") {
      tb <- table(dom)
      if (length(tb) > 1) {
        add(acc, "FAIL", "D04", paste0(rel, ":", r$time), "monthly dates mix different days of the month",
            paste0(paste(names(tb), tb, sep = "=", collapse = "; "), "; rows: ",
                   paste(names(tb), as.numeric(tapply(w, dom, sum)[names(tb)]), sep = "=", collapse = "; "),
                   "; examples=", some(dt[dom == names(which.min(tapply(w, dom, sum)))], 3)),
            key = "mixed", n = min(tb))
      } else if (names(tb) != RULE$monthly) {
        add(acc, "WARN", "D04", paste0(rel, ":", r$time),
            sprintf("monthly dates use the %s day of the month, CLAUDE.md says the %s day", names(tb), RULE$monthly),
            sprintf("dates=%d", length(dt)), key = "monthly", n = length(dt))
        add(acc, "ASK", "D04", paste0(rel, ":", r$time),
            "the SOP says first day of the month and CLAUDE.md says last day, confirm which one this source should follow",
            key = "monthly_ask", n = length(dt))
      } else {
        say("PASS", "D04", rel, sprintf("monthly dates are on the %s day of the month", RULE$monthly))
      }
    } else if (cad == "annual") {
      school <- "grade" %in% n || grepl("school|exemption", dir_name)
      off <- md[md != RULE$annual]
      if (!length(off)) {
        say("PASS", "D04", rel, paste0("annual dates are YYYY-", RULE$annual))
      } else if (school && all(off == "09-01")) {
        say("INFO", "D04", rel, "school-year dates (09-01)")
      } else {
        add(acc, "WARN", "D04", paste0(rel, ":", r$time), paste0("annual dates are not YYYY-", RULE$annual),
            paste0("month_day=", some(names(sort(table(off), decreasing = TRUE)), 4)), key = "annual", items = off)
      }
    }
  }

  # --- keys --------------------------------------------------------------------
  keys <- c(r$geo, if (!is.na(r$time)) r$time, r$strata)
  k <- key_of(d, keys)
  dup <- duplicated(k)
  vals <- r$values
  nv <- lapply(d[c(vals, r$bounds)], num)
  if (length(vals)) {
    rng <- vapply(utils::head(vals, 12), function(v) {
      x <- nv[[v]]
      if (all(is.na(x))) return(paste0(v, " all missing"))
      sprintf("%s %.4g..%.4g (median %.4g, missing %d)", v, min(x, na.rm = TRUE), max(x, na.rm = TRUE), stats::median(x, na.rm = TRUE), sum(is.na(x)))
    }, "")
    say("INFO", "D00", rel, "value ranges", paste0(paste(rng, collapse = "; "), if (length(vals) > 12) sprintf("; (+%d more columns)", length(vals) - 12) else ""))
  }
  if (any(dup)) {
    codes <- vals[vapply(vals, function(v) {
      x <- nv[[v]]
      !anyNA(x) && all(x == round(x)) && length(unique(x)) <= 60
    }, NA)]
    if (length(codes) && !anyDuplicated(key_of(d, c(keys, codes)))) {
      add(acc, "WARN", "D05", rel, paste0("rows are unique only when the numeric column(s) ", some(codes), " are read as stratifiers, the other checks treat them as values"),
          key = paste("numkey", paste(sort(codes), collapse = ",")))
    } else {
      add(acc, "FAIL", "D05", rel, paste0("duplicate rows on ", paste(keys, collapse = " + ")),
          sprintf("rows=%d; examples=%s", sum(dup), some(show_key(k[dup]), 3)), key = "dup", items = k[dup])
    }
  } else say("PASS", "D05", rel, paste0("rows are unique on ", paste(keys, collapse = " + ")))

  # --- values ------------------------------------------------------------------
  if (length(r$text)) {
    for (v in r$text) {
      x <- d[[v]]
      tok <- x[!is.na(x) & is.na(num(x))]
      sep <- grepl("^-?[0-9]{1,3}(,[0-9]{3})+(\\.[0-9]+)?$|%$", tok)
      top <- names(sort(table(tok), decreasing = TRUE))
      if (any(sep)) add(acc, "FAIL", "D10", paste0(rel, ":", v), "numbers written with thousands separators or a percent sign",
                        sprintf("cells=%d; examples=%s", sum(sep), some(tok[sep], 3)), key = paste(v, "sep"), n = sum(sep))
      if (any(!sep)) add(acc, "WARN", "D10", paste0(rel, ":", v), "text in a numeric column",
                         sprintf("cells=%d; values=%s", sum(!sep), some(top, 4)), key = paste(v, "text"), n = sum(!sep))
    }
  }
  all_na <- n[vapply(d, function(x) all(is.na(x)), NA)]
  if (length(all_na)) add(acc, "WARN", "D08", rel, "columns that are entirely missing", paste0("columns=", some(all_na)),
                          key = "all_na", items = all_na)
  live <- setdiff(vals, all_na)
  if (length(live) > 1 && nrow(d) > 20) {
    twin <- live[duplicated(nv[live])]
    if (length(twin)) add(acc, "WARN", "D08", rel, "value columns that are identical to another column",
                          paste0("columns=", some(twin)), key = "twin", items = twin)
  }
  neg <- character()
  sen <- character()
  for (v in c(vals, r$bounds)) {
    x <- nv[[v]]
    x <- x[!is.na(x)]
    if (!length(x)) next
    if (grepl("(^|_)(lon|lat|longitude|latitude)(_|$)", v)) next
    if (any(x %in% c(-99, -999, -9999, 9999, 99999))) sen <- c(sen, v) else if (min(x) < 0) neg <- c(neg, v)
  }
  if (length(sen)) add(acc, "WARN", "D07", rel, "sentinel values such as -999 used in place of missing",
                       paste0("columns=", some(sen)), key = "sentinel", items = sen)
  if (length(neg)) add(acc, "WARN", "M09", rel, "negative values", paste0("columns=", some(neg)), key = "negative", items = neg)

  # --- percent scale -------------------------------------------------------------
  no_unit <- vals[vapply(vals, function(v) !nzchar(trimws(txt(entry_for(rendered, v)$unit))), NA)]
  if (length(no_unit)) say("SKIP", "M08", rel, "value columns with no unit in measure_info, their scale is checked only when the name says pct or percent",
                           sprintf("n=%d; columns=%s", length(no_unit), some(no_unit, 5)))
  for (v in c(vals, r$bounds)) {
    e <- entry_for(rendered, v)
    unit <- paste(txt(e$unit), txt(e$measure_type))
    is_pct <- (grepl("percent|%", unit, ignore.case = TRUE) && !grepl("proportion|probability|0 ?[-–] ?1", unit, ignore.case = TRUE)) ||
      (!nzchar(trimws(unit)) && grepl(PCT_NAME, v))
    if (!is_pct || grepl(PCT_SKIP_NAME, v)) next
    x <- nv[[v]]
    x <- x[!is.na(x)]
    if (length(x) < 20) next
    mx <- max(x)
    if (mx <= 1 && mx > 0) {
      q75 <- stats::quantile(x, 0.75, names = FALSE)
      if (q75 >= 0.2) add(acc, "FAIL", "M08", paste0(rel, ":", v), "declared as a percent but values run from 0 to 1",
                          sprintf("max=%.3g; q75=%.3g; unit=%s", mx, q75, txt(e$unit)), key = paste(v, "le1"))
      else add(acc, "WARN", "M08", paste0(rel, ":", v), "percent column with every value at or below 1, check the scale against the raw data",
               sprintf("max=%.3g; q75=%.3g", mx, q75), key = paste(v, "le1"))
    } else if (mx > 100) {
      lvl <- if (v %in% r$bounds || grepl(PCT_OVER_OK, v)) "INFO" else "WARN"
      add(acc, lvl, "M08", paste0(rel, ":", v), "percent values above 100",
          sprintf("max=%.4g; rows=%d", mx, sum(x > 100)), key = paste(v, "gt100"), n = sum(x > 100))
    }
  }

  # --- suppression ---------------------------------------------------------------
  sflags <- r$flags[grepl(SUPP_RE, r$flags, ignore.case = TRUE)]
  fmap <- flag_map(sflags, vals)
  # every flag (suppressed, not asked, ...) explains a missing or placeholder value in the series checks
  fmap_all <- c(fmap, flag_map(setdiff(r$flags, sflags), vals))
  flagged_cols <- unique(unlist(fmap))
  for (f in r$flags) {
    fv <- d[[f]]
    on <- is_on(fv)
    tb <- table(ifelse(is.na(fv), "NA", fv))
    if (!f %in% sflags) {
      say("INFO", "D07", paste0(rel, ":", f), "flag values", paste(names(tb), tb, sep = "=", collapse = "; "))
      next
    }
    cols <- fmap[[f]]
    cls <- character()
    for (v in cols) {
      x <- d[[v]][on]
      if (!length(x)) next
      xn <- num(x)
      top <- sort(table(xn), decreasing = TRUE)
      what <- if (all(is.na(x))) "NA" else if (length(top) && top[[1]] >= 0.9 * sum(!is.na(x))) paste0("constant ", names(top)[1]) else "mixed"
      cls <- c(cls, what)
      if (identical(what, "constant 0")) add(acc, "WARN", "D07", paste0(rel, ":", v),
        "suppressed cells are stored as 0, which cannot be told apart from a true zero",
        sprintf("flag=%s; rows=%d", f, sum(on)), key = paste(v, "zero"), n = sum(on))
    }
    say("INFO", "D07", paste0(rel, ":", f), "suppression flag",
        sprintf("%s; applies_to=%s; flagged_values=%s", paste(names(tb), tb, sep = "=", collapse = "; "),
                if (length(cols)) some(cols, 4) else "no matching value column",
                if (length(cls)) paste(names(table(cls)), collapse = "/") else "none"))
  }
  five_cols <- character()
  for (v in vals[grepl(COUNT_RE, vals)]) {
    x <- nv[[v]]
    marked <- rep(FALSE, length(x))
    for (f in names(fmap)) if (v %in% fmap[[f]]) marked <- marked | d[[f]] %in% c("1", "TRUE", "true")
    x <- x[!is.na(x) & !marked]
    if (length(x) < 50 || any(x != round(x))) next
    if (sum(x == 5) >= 10 && !any(x %in% c(1, 2, 3, 4, 6, 7, 8, 9))) {
      add(acc, if (!length(sflags) || v %in% flagged_cols) "FAIL" else "WARN", "D07", paste0(rel, ":", v),
          if (v %in% flagged_cols) "counts of exactly 5 and nothing else between 1 and 9 in rows where the suppression flag is off"
          else if (length(sflags)) "counts of exactly 5 and nothing else between 1 and 9, and no suppression flag that matches this column by name"
          else "counts of exactly 5 and nothing else between 1 and 9, with no suppression flag column",
          sprintf("rows_equal_5=%d", sum(x == 5)), key = paste(v, "five"), n = sum(x == 5))
      five_cols <- c(five_cols, v)
    }
  }
  # one exact value that fills far more cells than any other, outside the flagged rows
  # (a value at the top or bottom of the range, such as 100 percent, piles up naturally and is left alone)
  for (v in setdiff(vals, c(five_cols, sen))) {
    x <- nv[[v]]
    marked <- rep(FALSE, length(x))
    for (f in names(fmap_all)) if (v %in% fmap_all[[f]]) marked <- marked | is_on(d[[f]])
    x <- x[!is.na(x) & !marked & x != 0]
    if (length(x) < 500) next
    tb <- sort(table(x), decreasing = TRUE)
    if (length(tb) < 30) next
    if (as.numeric(names(tb)[1]) %in% range(x)) next
    if (tb[[1]] >= 0.02 * length(x) && tb[[1]] >= 8 * tb[[2]]) {
      add(acc, "WARN", "D07", paste0(rel, ":", v), "one exact value fills far more cells than any other and no flag marks those rows, check for imputed or placeholder values",
          sprintf("value=%s; cells=%d of %d; next_most_common=%s (%d)", names(tb)[1], tb[[1]], length(x), names(tb)[2], tb[[2]]),
          key = paste(v, "heap"), n = tb[[1]])
    }
  }

  # --- categories ------------------------------------------------------------------
  for (s in intersect(r$strata, VOCAB_COLS)) {
    lv <- sort(unique(d[[s]][!is.na(d[[s]])]))
    agg <- intersect(AGG, lv)
    say("INFO", "D09", paste0(rel, ":", s), "levels",
        sprintf("n=%d; all_groups_label=%s; levels=%s", length(lv), if (length(agg)) paste(agg, collapse = "/") else "none", some(lv, 30)))
    if (all(c("Total", "Overall") %in% lv)) add(acc, "WARN", "D09", paste0(rel, ":", s), "both Total and Overall appear in one column", key = paste(s, "both"))
  }

  # --- dates present for every series ----------------------------------------------
  if (length(dt) > 1 && any(dup)) say("SKIP", "D06", rel, "duplicate keys, series checks not run")
  if (length(dt) > 1 && !any(dup)) {
    nested <- r$strata[vapply(r$strata, function(s) {
      length(unique(d[[s]])) > 1 && all(tapply(d[[s]], d[[r$time]], function(v) length(unique(v))) <= 1)
    }, NA)]
    sc <- setdiff(c(r$geo, r$strata), nested)
    # a column that never separates two rows of the same date describes the series, it does not split it
    left_out <- character()
    n_keys <- length(unique(key_of(d, c(sc, r$time))))
    for (s in setdiff(r$strata, nested)) {
      if (length(setdiff(sc, s)) && length(unique(key_of(d, c(setdiff(sc, s), r$time)))) == n_keys) {
        sc <- setdiff(sc, s)
        left_out <- c(left_out, s)
      }
    }
    if (length(left_out)) say("INFO", "D06", rel, "descriptive columns left out of the series key", paste0("columns=", some(left_out, 8)))
    sk <- key_of(d, sc)
    ti <- match(d[[r$time]], format(dt, "%Y-%m-%d"))
    okr <- !is.na(ti)
    idx <- split(ti[okr], sk[okr])
    span <- vapply(idx, function(i) max(i) - min(i) + 1, 0)
    miss <- span - lengths(idx)
    run <- vapply(idx, function(i) if (length(i) > 1) max(diff(sort(i))) - 1 else 0, 0)
    need_run <- switch(cad, weekly = 3, daily = 3, monthly = 2, 1)
    big <- !is.na(geo_class(sub("\r.*$", "", names(idx)))) & run >= need_run
    if (length(r$geo)) {
      g1 <- vapply(strsplit(names(idx), "\r", fixed = TRUE), function(p) p[1], "")
      big <- geo_class(g1) %in% c("national", "state") & run >= need_run
    }
    share <- sum(miss) / sum(span)
    if (sum(miss) > 0) {
      worst <- names(sort(miss, decreasing = TRUE))[1:min(30, sum(miss > 0))]
      eg <- vapply(worst, function(w) {
        i <- sort(idx[[w]])
        gap <- which(diff(i) > 1)[1]
        sprintf("%s: nothing between %s and %s", show_key(w), dt[i[gap]], dt[i[gap + 1]])
      }, "")
      lvl <- if (share >= 0.01 || any(big)) "WARN" else "INFO"
      add(acc, lvl, "D06", rel, "dates missing inside some series",
          sprintf("missing_cells=%d (%.1f%%); series_with_gaps=%d of %d; national_or_state_series_with_long_gaps=%d",
                  sum(miss), 100 * share, sum(miss > 0), length(idx), sum(big)), key = "grid",
          items = show_key(names(miss)[miss > 0]))
      for (i in seq_along(eg)) say("HUMAN", "D06", rel, paste0("look at ", eg[i]), key = "grid", items = show_key(worst[i]))
    } else {
      say("PASS", "D06", rel, "every series has every date inside its own range", sprintf("series=%d", length(idx)))
    }

    # runs of missing or constant values and one-point spikes, national and state series only
    top_geo <- if (length(r$geo)) geo_class(d[[r$geo]]) %in% c("national", "state") else rep(TRUE, nrow(d))
    scan_cols <- utils::head(setdiff(vals, all_na), 150)
    if (any(top_geo & okr) && length(scan_cols)) {
      rows <- which(top_geo & okr)
      rows <- rows[order(sk[rows], ti[rows])]
      grp <- sk[rows]
      tr <- ti[rows]
      members <- split(seq_along(grp), factor(grp, levels = unique(grp)))
      members <- members[lengths(members) >= 6]
      const_need <- switch(cad, weekly = 8, daily = 14, monthly = 6, 4)
      na_hits <- character()
      const_hits <- character()
      spike_hits <- character()
      for (v in scan_cols) {
        x <- nv[[v]][rows]
        expl <- rep(FALSE, length(x))
        for (f in names(fmap_all)) if (v %in% fmap_all[[f]]) expl <- expl | is_on(d[[f]][rows])
        if (length(unique(x[!is.na(x)])) <= 5) next
        for (si in seq_along(members)) {
          s <- names(members)[si]
          w <- members[[si]]
          xs <- x[w]
          rl <- rle(ifelse(is.na(xs), "NA", ifelse(expl[w], "F", format(xs, digits = 15))))
          end <- cumsum(rl$lengths)
          start <- end - rl$lengths + 1
          for (j in seq_along(rl$values)) {
            if (rl$values[j] == "NA" && start[j] > 1 && end[j] < length(xs) && rl$lengths[j] >= max(need_run, 2) && !all(expl[w][start[j]:end[j]])) {
              na_hits <- c(na_hits, sprintf("%s %s: missing from %s to %s", v, show_key(s), dt[tr[w][start[j]]], dt[tr[w][end[j]]]))
            }
            if (!rl$values[j] %in% c("NA", "F") && rl$lengths[j] >= const_need) {
              val <- xs[start[j]]
              # rounded values repeat naturally, so only values with 3 or more significant digits count
              if (val != 0 && nchar(gsub("[^1-9]", "", sub("0+$", "", format(val, digits = 15)))) >= 3) {
                const_hits <- c(const_hits, sprintf("%s %s: constant at %s from %s to %s", v, show_key(s), format(val), dt[tr[w][start[j]]], dt[tr[w][end[j]]]))
              }
            }
          }
          if (length(xs) >= 3) {
            mid <- 2:(length(xs) - 1)
            a <- xs[mid - 1]
            b <- xs[mid]
            c2 <- xs[mid + 1]
            sp <- which(!is.na(a) & !is.na(b) & !is.na(c2) & a > 0 & c2 > 0 & !expl[w][mid] &
                        pmax(a, c2) <= 3 * pmin(a, c2) &
                        (b > 10 * pmax(a, c2) | (b >= 0 & b < 0.1 * pmin(a, c2))) & pmax(a, b, c2) >= 20)
            if (length(sp)) spike_hits <- c(spike_hits, sprintf("%s %s: one-point jump at %s (%s, neighbours %s and %s)", v, show_key(s),
                                                                 dt[tr[w][mid[sp[1]]]], format(b[sp[1]]), format(a[sp[1]]), format(c2[sp[1]])))
          }
        }
      }
      if (length(na_hits)) {
        add(acc, "WARN", "D06", rel, "values missing for several periods in a row inside national or state series",
            sprintf("runs=%d", length(na_hits)), key = "na_run", items = sub(": missing from .*$", "", na_hits))
        # a series that already had such a run on base is not new: filling cells splits one run into several
        for (h in na_hits[!duplicated(sub(": missing from .*$", "", na_hits))][seq_len(min(200, length(unique(sub(": missing from .*$", "", na_hits)))))]) {
          say("HUMAN", "D06", rel, paste0("look at ", h), key = "na_run", items = sub(": missing from .*$", "", h))
        }
      }
      if (length(const_hits)) {
        add(acc, "INFO", "D06", rel, "values that stay exactly constant for many periods in national or state series",
            sprintf("runs=%d", length(const_hits)), key = "const_run", items = sub(": constant at .*$", "", const_hits))
        for (h in utils::head(const_hits[!duplicated(sub(": constant at .*$", "", const_hits))], 200)) {
          say("HUMAN", "D06", rel, paste0("look at ", h), key = "const_run", items = sub(": constant at .*$", "", h))
        }
      }
      if (length(spike_hits)) {
        add(acc, "WARN", "D06", rel, "one-point jumps of more than tenfold in national or state series",
            sprintf("points=%d", length(spike_hits)), key = "spike", items = spike_hits)
        for (h in utils::head(spike_hits, 200)) say("HUMAN", "D06", rel, paste0("look at ", h), key = "spike", items = h)
      }
    }
  }
  invisible(r)
}

# -----------------------------------------------------------------------------
# Comparison with the base version (standard and dist)
# -----------------------------------------------------------------------------
# Columns that were numeric on base and hold text or TRUE/FALSE now.
flipped_cols <- function(a_tab, b_tab) {
  ra <- roles(a_tab)
  rb <- roles(b_tab)
  intersect(ra$strata, c(rb$values, rb$bounds))
}

compare_tables <- function(a_tab, b_tab, rel, is_dist, force_values = character()) {
  a <- a_tab$d
  b <- b_tab$d
  ra <- roles(a_tab, force_values)
  rb <- roles(b_tab)
  hard <- if (is_dist) "FAIL" else "WARN"
  changelog <- FALSE
  emit("INFO", "C00", rel, "compared with base", sprintf("rows=%d -> %d; columns=%d -> %d", b_tab$rows, a_tab$rows, ncol(b), ncol(a)))
  gone <- setdiff(names(b), names(a))
  added <- setdiff(names(a), names(b))
  if (length(gone)) {
    emit(hard, "C02", rel, "columns dropped", paste0("columns=", some(gone, 8)))
    changelog <- TRUE
  }
  if (length(added)) emit("INFO", "C02", rel, "columns added", paste0("columns=", some(added, 8)))
  if (length(force_values)) {
    tok <- unlist(lapply(force_values, function(v) {
      x <- a[[v]]
      x[!is.na(x) & is.na(num(x))]
    }))
    emit("FAIL", "C02", rel, "columns that were numeric on base now hold text or TRUE/FALSE",
         sprintf("columns=%s; now_holds=%s", some(force_values, 6), some(names(sort(table(tok), decreasing = TRUE)), 4)))
    changelog <- TRUE
  }
  if (!is.null(a_tab$types) && !is.null(b_tab$types)) {
    sh <- intersect(names(a), names(b))
    ch <- sh[a_tab$types[sh] != b_tab$types[sh]]
    for (v in ch) {
      emit(if (v %in% c("geography", "geography_fips", "fips", "time", "date", "year")) "FAIL" else "WARN", "C02",
           paste0(rel, ":", v), "column type changed", sprintf("%s -> %s", b_tab$types[[v]], a_tab$types[[v]]))
    }
  }
  if (a_tab$cut || b_tab$cut) {
    emit("SKIP", "C05", rel, "file is larger than --max-rows, values not compared")
    return(invisible(changelog))
  }
  for (s in intersect(c(ra$geo, ra$strata), names(b))) {
    if (s %in% c(rb$values, rb$bounds)) next
    ua <- unique(a[[s]])
    ub <- unique(b[[s]])
    lost <- setdiff(ub, ua)
    new <- setdiff(ua, ub)
    what <- if (identical(s, "geography")) "geographies" else "levels"
    if (length(new) && length(ub) <= 5000) emit("INFO", "C03", paste0(rel, ":", s), paste(what, "added"), sprintf("n=%d; %s", length(new), some(new, 6)))
    if (!length(lost)) next
    if (identical(s, "geography")) {
      lvl <- if ("00" %in% lost || length(lost) > 0.05 * length(ub)) "FAIL" else "WARN"
      emit(lvl, "C03", paste0(rel, ":", s), "geographies dropped", sprintf("n=%d of %d; examples=%s", length(lost), length(ub), some(lost, 6)))
      changelog <- TRUE
    } else if (length(ub) <= 500) {
      emit(hard, "C03", paste0(rel, ":", s), "levels dropped", sprintf("n=%d; levels=%s", length(lost), some(lost, 6)))
      changelog <- TRUE
    }
  }
  tcol <- ra$time
  ta <- if (!is.na(tcol)) a[[tcol]][!is.na(a[[tcol]])] else character()
  tb <- if (!is.na(tcol) && tcol %in% names(b)) b[[tcol]][!is.na(b[[tcol]])] else character()
  moved <- FALSE
  if (length(ta) && length(tb)) {
    da <- sort(unique(ta))
    db <- sort(unique(tb))
    if (length(da) == length(db) && length(da) > 1 && !any(da %in% db) && all(grepl(ISO, c(da, db)))) {
      shift <- as.numeric(as.Date(da, format = "%Y-%m-%d") - as.Date(db, format = "%Y-%m-%d"))
      if (all(!is.na(shift)) && max(abs(shift)) <= 31) {
        moved <- TRUE
        emit("WARN", "C04", paste0(rel, ":", tcol), "every date is relabelled against base, rows are matched in date order for the comparison",
             sprintf("shift_days=%s; first=%s -> %s; last=%s -> %s", some(sort(unique(shift)), 6), db[1], da[1], db[length(db)], da[length(da)]))
        b[[tcol]] <- da[match(b[[tcol]], db)]
        tb <- b[[tcol]][!is.na(b[[tcol]])]
        changelog <- TRUE
      }
    }
  }
  if (length(tb) && !length(ta)) {
    emit("FAIL", "D03", paste0(rel, ":", tcol), "no time values left, base had them", sprintf("latest_on_base=%s", max(tb)))
  } else if (!moved && length(ta) && length(tb) && all(grepl(ISO, c(max(ta), max(tb)))) && max(ta) < max(tb)) {
    emit("WARN", "D03", paste0(rel, ":", tcol), "latest date is earlier than on base", sprintf("%s -> %s", max(tb), max(ta)))
  }
  keys <- intersect(c(ra$geo, if (!is.na(tcol)) tcol, ra$strata), names(b))
  ka <- key_of(a, keys)
  kb <- key_of(b, keys)
  if (anyDuplicated(ka) || anyDuplicated(kb)) {
    emit("INFO", "C06", rel, "rows are not unique on the shared key columns, values not compared", paste0("key=", paste(keys, collapse = " + ")))
    return(invisible(changelog))
  }
  # the most recent periods are where sources revise their own numbers
  dates <- if (!is.na(tcol)) sort(unique(c(a[[tcol]], if (tcol %in% names(b)) b[[tcol]]))) else character()
  iso_dates <- dates[grepl(ISO, dates)]
  cad <- if (length(iso_dates) > 1) cadence_of(as.Date(iso_dates, format = "%Y-%m-%d")) else "single"
  k_tail <- switch(cad, daily = 14, weekly = 8, monthly = 3, quarterly = 2, 1)
  tail_dates <- if (length(dates) >= 4 * k_tail) utils::tail(dates, k_tail) else character()
  lost <- !kb %in% ka
  if (any(lost)) {
    old <- if (length(ta) && length(tb)) b[[tcol]][lost] < min(ta) else rep(FALSE, sum(lost))
    old[is.na(old)] <- FALSE
    if (any(old)) emit("INFO", "C04", rel, "rows before the new start date are gone", sprintf("rows=%d", sum(old)))
    fc <- rep(FALSE, sum(lost))
    if ("forecast_day" %in% names(b)) fc <- !old & (num(b$forecast_day[lost]) > 0) %in% TRUE
    if (any(fc)) emit("INFO", "C04", rel, "forecast rows on base have rolled off", sprintf("rows=%d", sum(fc)))
    keep <- !old & !fc
    inner <- sum(keep)
    if (inner) {
      where <- ""
      li <- which(lost)[keep]
      if (length(ra$geo) && ra$geo %in% names(b)) {
        gt <- sort(table(b[[ra$geo]][li]), decreasing = TRUE)
        where <- paste0("; geographies=", length(gt), " (", some(paste0(names(gt), ":", gt), 4), ")")
      }
      if (!is.na(tcol) && tcol %in% names(b)) {
        ld <- sort(unique(b[[tcol]][li]))
        where <- paste0(where, sprintf("; dates=%d (%s to %s)", length(ld), ld[1], ld[length(ld)]))
      }
      recent <- !CODE_CHANGED && length(tail_dates) && !is.na(tcol) && all(b[[tcol]][li] %in% tail_dates) && inner <= 0.05 * length(kb)
      if (recent) {
        emit("INFO", "C04", rel, "rows in the most recent periods are gone, withdrawn by the source, no code change in this diff",
             sprintf("rows=%d of %d%s; examples=%s", inner, length(kb), where, some(show_key(kb[li]), 3)))
      } else {
        emit(if (inner > 0.05 * length(kb)) "FAIL" else "WARN", "C04", rel, "rows on base that are no longer present",
             sprintf("rows=%d of %d%s; examples=%s", inner, length(kb), where, some(show_key(kb[li]), 3)))
        changelog <- TRUE
      }
    }
  }
  m <- match(ka, kb)
  ok <- !is.na(m)
  vals <- intersect(c(ra$values, ra$bounds), names(b))
  if (any(!ok)) {
    nd <- if (!is.na(tcol)) sort(unique(a[[tcol]][!ok])) else character()
    na_share <- if (length(vals)) mean(is.na(unlist(a[!ok, vals, drop = FALSE], use.names = FALSE))) else NA
    emit("INFO", "C04", rel, "rows added",
         sprintf("rows=%d%s%s", sum(!ok), if (length(nd)) sprintf("; dates=%d (%s to %s)", length(nd), nd[1], nd[length(nd)]) else "",
                 if (!is.na(na_share)) sprintf("; missing_value_cells=%.0f%%", 100 * na_share) else ""))
  }
  if (!any(ok)) return(invisible(changelog))

  in_tail <- if (!is.na(tcol)) a[[tcol]][ok] %in% tail_dates else rep(FALSE, sum(ok))
  t_ok <- if (!is.na(tcol)) a[[tcol]][ok] else rep(NA_character_, sum(ok))

  # flags: report flips
  for (f in intersect(ra$flags, names(b))) {
    x <- a[[f]][ok]
    y <- b[[f]][m[ok]]
    on <- !is_on(y) & is_on(x)
    off <- is_on(y) & !is_on(x)
    if (!any(on | off)) next
    where <- ""
    if (length(ra$geo)) {
      gt <- sort(table(a[[ra$geo]][ok][on | off]), decreasing = TRUE)
      where <- paste0("; geographies=", length(gt), " (", some(paste0(names(gt), ":", gt), 4), ")")
    }
    older <- sum((on | off) & !in_tail)
    emit(if (CODE_CHANGED && older) "WARN" else "INFO", "C05", paste0(rel, ":", f), "flag changed on rows that were already on base",
         sprintf("turned_on=%d; turned_off=%d; outside_recent_periods=%d%s", sum(on), sum(off), older, where))
    if (CODE_CHANGED && older) changelog <- TRUE
  }
  for (f in gone[grepl(SUPP_RE, gone, ignore.case = TRUE)]) {
    on_b <- is_on(b[[f]][m[ok]])
    for (v in vals) {
      x <- num(a[[v]][ok])[on_b]
      y <- num(b[[v]][m[ok]])[on_b]
      filled <- !is.na(x) & (is.na(y) | abs(x - y) > 1e-9)
      if (sum(filled) >= 5) {
        tb <- sort(table(x[filled]), decreasing = TRUE)
        emit("FAIL", "D07", paste0(rel, ":", v), "the suppression flag column is dropped and cells it marked on base now hold new values",
             sprintf("flag=%s; cells=%d of %d flagged; most_common_value=%s (%d cells)", f, sum(filled), sum(on_b), names(tb)[1], tb[[1]]))
      }
    }
  }
  if (!length(vals)) return(invisible(changelog))

  row_out <- rep(FALSE, sum(ok))     # row has a change outside the recent periods
  res <- lapply(vals, function(v) {
    x <- num(a[[v]][ok])
    y <- num(b[[v]][m[ok]])
    ch <- (abs(x - y) > 1e-9 + 1e-6 * abs(y))
    ch[is.na(ch)] <- FALSE
    to_na <- is.na(x) & !is.na(y)
    from_na <- !is.na(x) & is.na(y)
    any_ch <- ch | to_na | from_na
    both <- ch & y != 0 & x != 0
    ratio <- if (sum(both) >= 10) stats::median(x[both] / y[both]) else NA
    near <- if (!is.na(ratio)) mean(abs(x[both] / y[both] / ratio - 1) < 0.02) else 0
    scale_col <- grepl("(^|_)scaled?(_|$)", v)
    if (!scale_col) row_out[any_ch & !in_tail] <<- TRUE
    fv <- if (any(from_na)) sort(table(x[from_na]), decreasing = TRUE) else NULL
    list(v = v, n = sum(!is.na(y) | !is.na(x)), n_base = sum(!is.na(y)), n_head = sum(!is.na(x)),
         changed = sum(ch), to_na = sum(to_na), from_na = sum(from_na),
         outside = if (scale_col) 0 else sum(any_ch & !in_tail), ratio = ratio, near = near, n_ratio = sum(both),
         when = if (any(both) && !is.na(tcol)) paste(range(t_ok[both]), collapse = " to ") else "",
         fill = if (!is.null(fv) && fv[[1]] == sum(from_na)) paste0(" all ", names(fv)[1]) else "",
         max_abs = if (any(ch)) max(abs(x - y)[ch]) else NA)
  })
  tot <- function(f) sum(vapply(res, function(r) r[[f]], 0))
  cells <- tot("n")
  any_change <- tot("changed") + tot("to_na") + tot("from_na")
  if (!any_change) {
    emit("PASS", "C05", rel, "values on shared rows are unchanged", sprintf("rows=%d; columns=%d", sum(ok), length(vals)))
    return(invisible(changelog))
  }
  one <- function(r) {
    bits <- c(if (r$changed) sprintf("changed %d%s", r$changed, if (is.na(r$max_abs)) "" else sprintf(" (max diff %.4g)", r$max_abs)),
              if (r$from_na) sprintf("filled %d%s", r$from_na, r$fill), if (r$to_na) sprintf("now missing %d", r$to_na))
    sprintf("%s: %s of %d", r$v, paste(bits, collapse = ", "), r$n)
  }
  moved_n <- function(r) r$changed + r$to_na + r$from_na
  top <- Filter(function(r) moved_n(r) > 0, res[order(-vapply(res, moved_n, 0))])
  detail <- paste(vapply(utils::head(top, 5), one, ""), collapse = "; ")
  if (length(top) > 5) detail <- paste0(detail, sprintf("; (+%d more columns)", length(top) - 5))

  wiped <- Filter(function(r) r$n_base >= 5 && r$n_head == 0, res)
  if (length(wiped)) emit("FAIL", "C05", rel, "columns that lost every value they had on base",
                          sprintf("n=%d; columns=%s", length(wiped), some(vapply(wiped, function(r) r$v, ""), 8)))
  const <- Filter(function(r) !is.na(r$ratio) && r$near >= 0.8 && r$n_ratio >= 10 && (r$ratio >= 1.5 || r$ratio <= 0.67), res)
  ten <- vapply(const, function(r) any(abs(r$ratio / c(100, 0.01, 1000, 0.001) - 1) < 0.02), NA)
  # one line per factor, however many columns moved by it
  fac <- vapply(const, function(r) format(signif(r$ratio, 3)), "")
  for (f in unique(fac)) {
    grp <- const[fac == f]
    is_ten <- all(ten[fac == f])
    emit(if (is_ten) "FAIL" else "WARN", "C05", rel,
         if (is_ten) "values rescaled against base" else "values changed by one constant factor against base",
         sprintf("ratio=%s; columns=%d: %s; cells=%d of %d; dates=%s", f, length(grp), some(vapply(grp, function(r) r$v, ""), 6),
                 sum(vapply(grp, function(r) r$n_ratio, 0)), sum(vapply(grp, function(r) r$n, 0)),
                 paste(range(unlist(lapply(grp, function(r) strsplit(r$when, " to ", fixed = TRUE)[[1]]))), collapse = " to ")))
  }
  rescaled <- length(wiped) > 0 || length(const) > 0

  # where the older changes sit: columns, series, measures
  cols5 <- Filter(function(r) r$n > 0 && r$outside >= 0.05 * r$n && r$outside >= 5, res)
  ser_cols <- setdiff(keys, tcol)
  rewritten <- character()
  if (length(ser_cols) && any(row_out)) {
    ser <- key_of(a[ok, , drop = FALSE], ser_cols)
    n_ser <- table(ser)
    n_out <- table(ser[row_out])
    hit <- n_out[n_out >= 5 & n_out >= 0.2 * n_ser[names(n_out)]]
    hit <- sort(hit, decreasing = TRUE)
    rewritten <- sprintf("%s: %d of %d rows", show_key(names(hit)), as.integer(hit), as.integer(n_ser[names(hit)]))
  }
  by <- ""
  for (g in intersect(c("measure", "outcome_name", "condition", "source"), keys)) {
    if (!any(row_out)) break
    tg <- sort(table(a[[g]][ok][row_out]), decreasing = TRUE)
    by <- paste0(by, sprintf("; rows_by_%s=%s", g, some(paste0(names(tg), ":", tg), 5)))
  }
  outside <- tot("outside")
  window <- if (!length(tail_dates)) "the most recent periods" else if (k_tail == 1) "the last period" else sprintf("the last %d periods", k_tail)
  notable <- length(cols5) || length(rewritten) || outside >= 0.01 * cells || tot("to_na") > 0.005 * cells
  if (notable) {
    where <- paste0(if (length(cols5)) paste0("; columns_changed_5pct_or_more=", some(vapply(cols5, function(r) r$v, ""), 6)),
                    if (length(rewritten)) sprintf("; series_rewritten=%d (%s)", length(rewritten), some(rewritten, 3)), by)
    kv <- sprintf("cells=%d of %d; now_missing=%d; newly_filled=%d%s; %s", outside, cells, tot("to_na"), tot("from_na"), where, detail)
    if (CODE_CHANGED || rescaled) {
      emit("WARN", "C05", rel, paste0("values changed outside ", window), kv)
      changelog <- TRUE
    } else {
      emit("INFO", "C05", rel, paste0("values changed outside ", window, ", revised by the source, no code change in this diff"), kv)
    }
  } else if (outside <= 0.1 * any_change) {
    emit("INFO", "C05", rel, paste0("revisions in ", window, " only"), sprintf("cells=%d; %s", any_change, detail))
  } else {
    emit("INFO", "C05", rel, "a small share of older values changed", sprintf("cells=%d of %d%s; %s", outside, cells, by, detail))
  }
  if (rescaled) changelog <- TRUE
  invisible(changelog)
}

# -----------------------------------------------------------------------------
# Folder level: structure and metadata
# -----------------------------------------------------------------------------
strip_comments <- function(l) sub("(^|[^\"'])#.*$", "\\1", l)

check_structure <- function(dir_rel, kind, files, base_files) {
  name <- basename(dir_rel)
  script <- if (kind == "bundle") "build.R" else "ingest.R"
  has_base <- !is.null(BASE_REF) && nrow(base_files) > 0
  same_proc <- has_base && same_as_base(file.path(dir_rel, "process.json"))
  same_script <- has_base && same_as_base(file.path(dir_rel, script))
  bproc <- NULL
  if (has_base) {
    bpp <- fetch("base", file.path(dir_rel, "process.json"))
    if (!is.null(bpp)) bproc <- tryCatch(jsonlite::read_json(bpp), error = function(e) NULL)
  }
  ident <- function(p) list(txt(p$name), txt(p$type), vapply(p$scripts, function(s) txt(s$path), ""))
  same_listing <- has_base && setequal(files$path, base_files$path)
  pp <- fetch("head", file.path(dir_rel, "process.json"))
  proc <- NULL
  if (is.null(pp)) {
    emit_base("FAIL", "S01", file.path(dir_rel, "process.json"), "process.json is missing, the folder was not created with dcf", same = same_listing)
  } else {
    proc <- tryCatch(jsonlite::read_json(pp), error = function(e) e)
    if (inherits(proc, "error")) {
      emit_base("FAIL", "S01", file.path(dir_rel, "process.json"), "process.json does not parse", conditionMessage(proc), same = same_proc)
      proc <- NULL
    }
  }
  if (!is.null(proc)) {
    target <- file.path(dir_rel, "process.json")
    want_type <- if (kind == "bundle") "bundle" else "source"
    paths <- vapply(proc$scripts, function(s) txt(s$path), "")
    bad <- c(if (!identical(txt(proc$name), name)) sprintf("name is '%s'", txt(proc$name)),
             if (!identical(txt(proc$type), want_type)) sprintf("type is '%s', expected '%s'", txt(proc$type), want_type),
             if (!script %in% paths) sprintf("scripts lists '%s', expected '%s'", paste(paths, collapse = ","), script))
    if (length(bad)) emit_base("FAIL", "S01", target, "process.json does not match the folder", paste(bad, collapse = "; "),
                               same = !is.null(bproc) && identical(ident(proc), ident(bproc)))
    else emit("PASS", "S01", target, "name, type and scripts match the folder")
    run_ok <- TRUE
    for (s in proc$scripts) {
      if (identical(s$last_status$success, FALSE)) {
        emit_base("FAIL", "S02", target, "the last recorded run failed", clip(txt(s$last_status$log)), same = same_proc)
        run_ok <- FALSE
      }
      lg <- unlist(s$last_status$log)
      lg <- lg[grepl("warning|failed to download|error", lg, ignore.case = TRUE) &
               !grepl("conflicted package|Attaching package|masked from|processing (source|bundle)|was built under R version", lg)]
      if (length(lg)) {
        emit_base("WARN", "S02", target, "warnings or errors in the last recorded run", clip(some(trimws(lg), 3), 300), same = same_proc)
        run_ok <- FALSE
      }
    }
    if (run_ok && length(proc$scripts)) emit("PASS", "S02", target, "the last recorded run succeeded with no warnings in its log",
                                             paste0("last_run=", txt(proc$scripts[[1]]$last_run)))
    absolute <- grep("^(/|[A-Za-z]:|~)", names(c(proc$standard_state, proc$dist_state, proc$source_state)), value = TRUE)
    if (length(absolute)) emit_base("FAIL", "S09", target, "absolute paths in the recorded state", some(absolute, 3),
                                    same = !is.null(bproc) && all(absolute %in% names(c(bproc$standard_state, bproc$dist_state, bproc$source_state))))
    else emit("PASS", "S09", target, "recorded state uses relative paths")
    own <- c(proc$standard_state, proc$dist_state)
    stale <- character()
    for (k in names(own)) {
      rel <- sub("^\\./", "", k)
      if (!startsWith(rel, paste0("data/", name, "/")) || grepl("datapackage\\.json$", rel)) next
      p <- fetch("head", rel)
      if (is.null(p)) next
      if (!identical(unname(tools::md5sum(p)), txt(own[[k]]))) stale <- c(stale, rel)
    }
    meta_stale <- stale[basename(stale) == "measure_info.json"]
    stale <- setdiff(stale, meta_stale)
    if (length(meta_stale)) emit("INFO", "S04", target, "measure_info.json was edited after the last recorded run")
    if (length(stale)) {
      moved <- stale[!vapply(stale, same_as_base, NA)]
      emit(if (length(moved) && !is.null(BASE_REF)) "WARN" else "INFO", "S04", target,
           "recorded md5 differs from the file, so the file was changed without a recorded run",
           paste0("files=", some(basename(stale), 6)))
    } else if (length(own)) {
      emit("PASS", "S04", target, "recorded md5 matches the data files")
    }
    # a bundle records the hashes of the source files it was built from
    src_state <- proc$source_state
    if (kind == "bundle") src_state <- c(src_state, proc$standard_state[!startsWith(sub("^\\./", "", names(proc$standard_state)), paste0("data/", name, "/"))])
    src_state <- src_state[!duplicated(names(src_state))]
    if (kind == "bundle" && length(src_state)) {
      old <- character()
      for (k in names(src_state)) {
        if (grepl("datapackage\\.json$|measure_info\\.json$", k)) next
        p <- fetch("head", sub("^\\./", "", k))
        if (!is.null(p) && !identical(unname(tools::md5sum(p)), txt(src_state[[k]]))) old <- c(old, sub("^\\./data/", "", k))
      }
      if (length(old)) emit("INFO", "S04", target, "source files have changed since this bundle was last built", paste0("files=", some(old, 6)))
    }
  }

  sp <- fetch("head", file.path(dir_rel, script))
  if (is.null(sp)) {
    emit_base("FAIL", "S01", file.path(dir_rel, script), paste0(script, " is missing"), same = same_listing)
  } else if (kind != "bundle") {
    code <- strip_comments(readLines(sp, warn = FALSE, encoding = "UTF-8"))
    rec <- grep("dcf_process_record\\( *updated", code)
    wr <- grep("vroom_write\\(|write_csv\\(|write\\.csv\\(|fwrite\\(|write_parquet\\(|write_json\\(|download\\.file\\([^)]*standard", code)
    target <- file.path(dir_rel, script)
    if (!any(grepl("dcf_process_record", code))) {
      emit_base("WARN", "S03", target, "no dcf::dcf_process_record() call, the run is not tracked", same = same_script)
    } else if (!length(rec)) {
      emit_base("WARN", "S03", target, "the processed state is never written back with dcf_process_record(updated = )", same = same_script)
    } else if (length(wr) && max(wr) > max(rec)) {
      emit_base("WARN", "S03", sprintf("%s:%d", target, max(rec)),
                "state is recorded before the last output is written, a failure after this line is skipped on the next run",
                sprintf("last_state_line=%d; later_write_lines=%s", max(rec), some(wr[wr > max(rec)], 5)), same = same_script)
    } else {
      emit("PASS", "S03", target, "the run is tracked and the state is recorded after the last write")
    }
    # code changed, data did not: an ingest skips processing when its raw data is unchanged
    std <- files$path[startsWith(files$path, file.path(dir_rel, "standard/")) & !grepl("datapackage\\.json$", files$path)]
    if (has_base && !same_script && file.path(dir_rel, script) %in% base_files$path && length(std) &&
        all(vapply(std, same_as_base, NA))) {
      emit("WARN", "S04", target, "ingest.R changed, standard files identical, the ingest was probably not re-run")
    }
  }

  extra <- grep("\\.R$", basename(files$path[dirname(files$path) == dir_rel]), value = TRUE, ignore.case = TRUE)
  extra <- setdiff(extra, script)
  new_extra <- setdiff(file.path(dir_rel, extra), base_files$path)
  if (length(new_extra) && (!is.null(BASE_REF) || length(extra))) {
    emit("INFO", "S01", dir_rel, "more than one R script in the folder, the convention is a single script", paste0("files=", some(extra)))
  }

  odd <- grep("( 2| copy|\\([0-9]+\\)|conflicted copy|\\.orig|\\.rej)(\\.[A-Za-z.]+)?$|\\.sync-conflict", basename(files$path), value = TRUE)
  if (length(odd)) emit_base("FAIL", "S08", dir_rel, "conflict or duplicate copies of files", paste0("files=", some(odd)), same = same_listing)

  raw <- files[startsWith(files$path, file.path(dir_rel, "raw/")), , drop = FALSE]
  out_dir <- if (kind == "bundle") "dist/" else "standard/"
  out <- files[startsWith(files$path, file.path(dir_rel, out_dir)), , drop = FALSE]
  if (nrow(raw)) {
    raw_mb <- sum(raw$size, na.rm = TRUE) / 1e6
    std_mb <- sum(out$size, na.rm = TRUE) / 1e6
    new_raw <- raw[!raw$path %in% base_files$path, , drop = FALSE]
    add_mb <- sum(new_raw$size, na.rm = TRUE) / 1e6
    kv <- sprintf("raw_mb=%.1f; standard_mb=%.1f; raw_files=%d", raw_mb, std_mb, nrow(raw))
    big <- raw_mb >= 50 || (!is.null(BASE_REF) && add_mb >= 10)
    if (big && (is.null(BASE_REF) || nrow(new_raw))) {
      emit("WARN", "S06", file.path(dir_rel, "raw"), "large raw data, the SOP says to process it outside Ingest and mirror the standard files", kv)
    } else {
      emit("INFO", "S06", file.path(dir_rel, "raw"), "raw data size", kv)
    }
    if (!is.null(BASE_REF) && nrow(new_raw)) {
      emit("ASK", "S06", file.path(dir_rel, "raw"),
           "raw files are added. Repo practice commits them, the guardrail skill says not to. Confirm they are not proprietary and may be redistributed",
           sprintf("files=%d; mb=%.1f", nrow(new_raw), add_mb))
    }
  }
  invisible(proc)
}

check_metadata <- function(dir_rel, kind, info, info_base, headers, value_cols, info_changed = TRUE) {
  target <- file.path(dir_rel, "measure_info.json")
  same_info <- !is.null(BASE_REF) && !info_changed
  if (!is.null(info$error)) {
    emit_base("FAIL", "M01", target, if (info$error == "missing") "measure_info.json is missing" else "measure_info.json does not parse",
              if (info$error == "missing") "" else info$error, same = same_info || (info$error == "missing" && identical(info_base$error, "missing") && !is.null(BASE_REF) && length(attr(headers, "base"))))
    return(invisible(NULL))
  }
  raw <- info$raw
  rendered <- info$rendered
  if (is.null(names(rendered))) {
    emit("FAIL", "M01", target, "measure_info.json has no entries")
    rendered <- structure(list(), names = character())
  }
  ids <- info_ids(rendered)
  base_ids <- info_ids(info_base$rendered)
  base_names <- names(info_base$rendered)
  emit("INFO", "M01", target, "measure_info.json parses", sprintf("entries=%d", sum(!startsWith(names(rendered), "_"))))

  # columns without an entry, using the rule dcf_check applies
  all_cols <- character()
  for (rel in names(headers)) {
    cols <- headers[[rel]]
    all_cols <- union(all_cols, cols)
    file_id <- sub("^data/", "", rel)
    cand <- setdiff(cols, c(STANDARD_VARS, if (kind != "bundle") character() else c("source", "geography_fips", "fips")))
    cand <- cand[!grepl(FLAG_RE, cand, ignore.case = TRUE) | kind == "bundle"]
    none <- cand[!(cand %in% ids | paste0(file_id, "|", cand) %in% ids)]
    if (!length(none)) next
    had <- none[none %in% base_ids | paste0(file_id, "|", none) %in% base_ids]
    base_cols <- attr(headers, "base")[[rel]]
    fresh <- if (is.null(BASE_REF)) none else setdiff(none, base_cols)
    lost <- intersect(had, none)
    if (length(fresh)) emit(if (is.null(BASE_REF)) "WARN" else "FAIL", "M02", rel, "columns with no measure_info entry", paste0("columns=", some(fresh, 8)))
    if (length(setdiff(none, fresh))) emit("INFO", "M02", rel, "columns with no measure_info entry that were already undocumented on base",
                                           sprintf("n=%d; columns=%s", length(setdiff(none, fresh)), some(setdiff(none, fresh), 5)))
  }
  if (kind != "bundle" && length(all_cols) && is.null(ONLY_FILES)) {
    keys <- names(rendered)[!startsWith(names(rendered), "_")]
    orphan <- setdiff(keys, all_cols)
    fresh <- if (is.null(BASE_REF)) orphan else setdiff(orphan, base_names)
    if (length(fresh)) emit("WARN", "M03", target, "entries that match no column in standard/", paste0("entries=", some(fresh, 8)))
  }

  # required fields on value entries
  keys <- names(rendered)[!startsWith(names(rendered), "_")]
  fresh_keys <- if (is.null(BASE_REF)) keys else setdiff(keys, base_names)
  thin <- character()
  for (k in fresh_keys) {
    e <- rendered[[k]]
    if (!is.list(e) || !is.null(e$levels) || !is.null(e$source_id) || !k %in% value_cols) next
    lack <- c("short_name", "measure_type", "unit")[vapply(c("short_name", "measure_type", "unit"), function(f) !nzchar(trimws(txt(e[[f]]))), NA)]
    if (length(lack)) thin <- c(thin, sprintf("%s (%s)", k, paste(lack, collapse = ",")))
  }
  if (length(thin) && kind != "bundle") emit("WARN", "M04", target, "entries without short_name, measure_type or unit", sprintf("n=%d; entries=%s", length(thin), some(thin, 5)))

  # _sources
  src <- raw[["_sources"]]
  if (kind != "bundle" || length(src)) {
    if (!length(src)) {
      emit_base("FAIL", "M05", target, "no _sources block", same = same_info)
    } else {
      req <- c("name", "url", "organization", "organization_url", "description", "restrictions")
      for (s in names(src)) {
        lack <- req[vapply(req, function(f) !nzchar(trimws(txt(src[[s]][[f]]))), NA)]
        same <- identical(src[[s]], info_base$raw[["_sources"]][[s]])
        if (length(lack)) emit(if (same) "INFO" else "FAIL", "M05", paste0(target, ":_sources.", s), "required source fields are empty", paste0("fields=", paste(lack, collapse = ",")))
      }
      first <- src[[1]]
      desc <- txt(first$description)
      if (!same_info) emit("HUMAN", "M06", target,
           if (length(src) > 1) "only the first _sources description is shown on the site. Read it and confirm it covers every dataset in this folder"
           else "read the source description and restrictions against the source's own documentation",
           sprintf("first=%s; chars=%d; other_sources=%s; text=%s", names(src)[1], nchar(desc),
                   if (length(src) > 1) some(names(src)[-1], 6) else "none", clip(desc, 300)))
      if (length(src) > 1 && nchar(desc) < 150) emit_base("WARN", "M06", target, "the first _sources description is short for a folder with several sources", sprintf("chars=%d", nchar(desc)), same = same_info)
      for (s in names(src)) {
        old <- info_base$raw[["_sources"]][[s]]
        if (!is.null(old) && !identical(txt(old$restrictions), txt(src[[s]]$restrictions))) {
          emit("HUMAN", "M10", paste0(target, ":_sources.", s), "restrictions text changed, check it against the source's terms", clip(txt(src[[s]]$restrictions), 200))
        }
      }
    }
  }

  # unit changes
  moves <- list()
  for (k in intersect(keys, base_names)) {
    e <- rendered[[k]]
    o <- info_base$rendered[[k]]
    if (!is.list(e) || !is.list(o)) next
    for (f in c("unit", "measure_type")) {
      if (nzchar(trimws(txt(o[[f]]))) && !identical(txt(e[[f]]), txt(o[[f]]))) {
        mv <- sprintf("%s: %s -> %s", f, txt(o[[f]]), txt(e[[f]]))
        moves[[mv]] <- c(moves[[mv]], k)
      }
    }
  }
  for (mv in names(moves)) emit("WARN", "M10", target, paste0(sub(":.*$", "", mv), " changed"),
                                sprintf("%s; entries=%d; %s", sub("^[^:]*: ", "", mv), length(moves[[mv]]), some(moves[[mv]], 4)))

  # _catalog (sources only)
  if (kind != "bundle") {
    cat_block <- raw[["_catalog"]]
    changed <- is.null(BASE_REF) || !identical(cat_block, info_base$raw[["_catalog"]])
    lvl <- if (changed) "WARN" else "INFO"
    std <- basename(grep("\\.csv\\.gz$", attr(headers, "all"), value = TRUE))
    if (is.null(cat_block)) {
      emit(lvl, "M07", target, "no _catalog block, the data sources index will fall back to a stub")
    } else {
      bad <- c(if (!nzchar(trimws(txt(cat_block$summary)))) "summary is empty",
               if (length(setdiff(std, names(cat_block$files)))) paste0("files missing: ", some(setdiff(std, names(cat_block$files)))),
               if (is.null(ONLY_FILES) && length(setdiff(names(cat_block$files), std))) paste0("files listed but not in standard/: ", some(setdiff(names(cat_block$files), std))),
               if (any(grepl("^Stratified by |^Overall; no stratification", unlist(cat_block$files)))) "placeholder file blurbs")
      if (length(bad)) emit(lvl, "M07", paste0(target, ":_catalog"), "_catalog is incomplete", paste(bad, collapse = "; "))
      else emit("PASS", "M07", paste0(target, ":_catalog"), "_catalog covers every standard file")
    }
  }
  invisible(NULL)
}

# -----------------------------------------------------------------------------
# Raw against standard (sources that transform raw files)
# -----------------------------------------------------------------------------
tally_raw <- function(path) {
  tab <- tryCatch(read_table(path), error = function(e) NULL)
  if (is.null(tab)) return(NULL)
  d <- tab$d
  na <- 0
  marks <- character()
  seps <- 0
  sep_cols <- character()
  id_like <- "fips|geo|(^|_)(id|code|zip|zcta|year|week|month|season|hsa|county|state)(_|$)"
  for (v in names(d)[!grepl(id_like, names(d), ignore.case = TRUE)]) {
    x <- d[[v]]
    y <- x[!is.na(x)]
    if (!length(y)) next
    yn <- num(gsub(",", "", y, fixed = TRUE))
    if (mean(!is.na(yn)) < 0.5) next
    na <- na + sum(is.na(x))
    marks <- c(marks, y[is.na(yn)])
    ns <- sum(grepl("^-?[0-9]{1,3}(,[0-9]{3})+(\\.[0-9]+)?$", y))
    if (ns) sep_cols <- c(sep_cols, v)
    seps <- seps + ns
  }
  flag_cols <- grep("flag|suppress|footnote|symbol", names(d), value = TRUE, ignore.case = TRUE)
  flag_txt <- vapply(flag_cols, function(f) {
    t <- sort(table(d[[f]]), decreasing = TRUE)
    paste0(f, "(", paste(utils::head(paste(names(t), t, sep = ":"), 4), collapse = " "), ")")
  }, "")
  list(rows = tab$rows, na = na, marks = sort(table(marks), decreasing = TRUE), seps = seps, sep_cols = sep_cols,
       flags = flag_txt, cut = tab$cut)
}

check_raw <- function(dir_rel, files, std_tabs, std_rels, script_path, same_script = FALSE) {
  raw <- files[startsWith(files$path, file.path(dir_rel, "raw/")), , drop = FALSE]
  raw <- raw[!startsWith(basename(raw$path), "."), , drop = FALSE]
  if (!nrow(raw)) {
    emit("SKIP", "R01", file.path(dir_rel, "raw"), "no raw files in the repository, raw and standard counts not compared")
    return(invisible(NULL))
  }
  readable <- grepl("\\.(csv|tsv)(\\.(gz|xz|bz2))?$|\\.parquet$", raw$path, ignore.case = TRUE)
  other <- raw[!readable & !grepl("\\.json$", raw$path), , drop = FALSE]
  if (nrow(other)) {
    ext <- table(tolower(sub("^.*\\.", "", other$path)))
    emit("SKIP", "R01", file.path(dir_rel, "raw"), "raw files in formats this script does not read",
         paste(names(ext), ext, sep = "=", collapse = "; "))
  }
  raw <- raw[readable & raw$size <= 200e6, , drop = FALSE]
  raw <- raw[order(-raw$size), , drop = FALSE]
  if (nrow(raw) > 10) {
    emit("INFO", "R01", file.path(dir_rel, "raw"), "only the 10 largest raw files are tallied", sprintf("files=%d", nrow(raw)))
    raw <- raw[1:10, , drop = FALSE]
  }
  raw_marks <- 0
  raw_seps <- 0
  for (rel in raw$path) {
    p <- fetch("head", rel)
    t <- if (is.null(p)) NULL else tally_raw(p)
    if (is.null(t)) {
      emit("SKIP", "R01", rel, "could not be read")
      next
    }
    raw_marks <- raw_marks + sum(t$marks)
    raw_seps <- raw_seps + t$seps
    emit("INFO", "R01", rel, "raw file: missing cells and non-numeric markers in numeric columns",
         sprintf("rows=%d%s; missing_cells=%d; markers=%s; with_thousands_separator=%d%s%s", t$rows, if (t$cut) "+" else "", t$na,
                 if (length(t$marks)) paste(utils::head(paste(names(t$marks), t$marks, sep = ":"), 8), collapse = " ") else "none",
                 t$seps, if (length(t$sep_cols)) paste0(" (", some(t$sep_cols, 4), ")") else "",
                 if (length(t$flags)) paste0("; flag_columns=", some(t$flags, 3)) else ""))
  }
  std_na <- 0
  std_flag <- 0
  std_text <- 0
  has_flag <- FALSE
  for (rel in std_rels) {
    tab <- std_tabs[[rel]]
    if (is.null(tab)) {
      p <- fetch("head", rel)
      tab <- if (is.null(p)) NULL else tryCatch(read_table(p), error = function(e) NULL)
    }
    if (is.null(tab)) next
    r <- roles(tab)
    na <- sum(vapply(tab$d[r$values], function(x) sum(is.na(x)), 0))
    mk <- sum(vapply(tab$d[r$values], function(x) sum(!is.na(x) & is.na(num(x))), 0))
    sf <- r$flags[grepl(SUPP_RE, r$flags, ignore.case = TRUE)]
    on <- sum(vapply(tab$d[sf], function(x) sum(is_on(x)), 0))
    std_na <- std_na + na
    std_flag <- std_flag + on
    std_text <- std_text + mk
    has_flag <- has_flag || length(sf) > 0
    emit("INFO", "R01", rel, "standard file: missing cells, text markers and flagged cells",
         sprintf("rows=%d; missing_value_cells=%d; text_markers=%d; suppression_flags=%s; flagged=%d", tab$rows, na, mk, if (length(sf)) some(sf, 4) else "none", on))
  }
  if (std_text > 0) {
    emit_base("WARN", "R02", file.path(dir_rel, "standard"), "raw markers are carried into the standard file as text",
              sprintf("text_markers=%d", std_text), same = same_script)
  }
  if (raw_marks > 0 && !has_flag) {
    emit_base(if (std_na == 0 && std_text == 0) "WARN" else "INFO", "R02", file.path(dir_rel, "standard"),
              if (std_na == 0 && std_text == 0) "raw data has suppression or not-reported markers but the standard files have no flags and no missing values"
              else "raw markers with no flag column in the standard files, confirm none of them is imputed",
              sprintf("raw_markers=%d; standard_missing=%d; standard_text_markers=%d", raw_marks, std_na, std_text), same = same_script)
  }
  if (raw_seps > 0 && !is.null(script_path)) {
    code <- strip_comments(readLines(script_path, warn = FALSE, encoding = "UTF-8"))
    if (!any(grepl("col_number|parse_number|gsub\\( *[\"'],[\"']", code))) {
      emit_base("WARN", "R04", file.path(dir_rel, "ingest.R"), "raw numbers carry thousands separators and the script has no number parser, check they are not read as NA",
                sprintf("cells=%d", raw_seps), same = same_script)
    }
  }
  if (same_script) emit("INFO", "R01", dir_rel, "ingest.R is unchanged against base, reconcile the tallies above only if raw and standard moved apart (SOP item 3)")
  else emit("HUMAN", "R01", dir_rel, "reconcile the raw and standard tallies above after applying the row filters in ingest.R (SOP item 3)")
  invisible(NULL)
}

# -----------------------------------------------------------------------------
# Bundles
# -----------------------------------------------------------------------------
check_dist <- function(tab, rel, rendered, acc, quiet = FALSE) {
  d <- tab$d
  n <- names(d)
  ty <- tab$types
  say <- function(...) if (!quiet) add(acc, ...)
  file_id <- sub("^data/", "", rel)
  is_num <- grepl(NUM_TYPE, ty[n])
  names(is_num) <- n
  acc$rows <- nrow(d)
  say("INFO", "B00", rel, "schema", sprintf("rows=%d; %s", tab$rows, clip(paste(n, ty[n], sep = ":", collapse = ", "), 300)))
  if (tab$cut) say("INFO", "B00", rel, sprintf("only the first %d rows were read", nrow(d)))
  if (!tab$rows) add(acc, "FAIL", "B02", rel, "file has no rows", key = "empty")
  bad <- n[!grepl("^[A-Za-z][A-Za-z0-9_]{0,63}$", n)]
  if (length(bad)) add(acc, "FAIL", "B02", rel, "column names the MCP server rejects", paste0("columns=", some(bad)), key = paste(bad, collapse = ","))

  tcol <- intersect(c("time", "date", "week_end", "year"), n)[1]
  if (is.na(tcol)) {
    add(acc, "FAIL", "B03", rel, "no time column (time, date, week_end or year)", key = "time")
  } else if (tcol != "year") {
    if (grepl("string|utf8", ty[[tcol]])) add(acc, "WARN", "B03", paste0(rel, ":", tcol), "dates are stored as text, not as a date type", key = "text_dates")
    dt <- sort(as.Date(unique(d[[tcol]][grepl(ISO, d[[tcol]])]), format = "%Y-%m-%d"))
    cad <- cadence_of(dt[!is.na(dt)])
    if (cad %in% c("irregular", "single")) add(acc, "WARN", "B03", paste0(rel, ":", tcol),
      "cadence cannot be detected from the dates, the MCP server will not draft this file", sprintf("distinct_dates=%d", length(dt)), key = "cadence")
    else say("INFO", "B03", rel, "cadence", sprintf("cadence=%s; from=%s; to=%s", cad, min(dt), max(dt)))
  }

  gcols <- intersect(c("geography", "fips", "geography_fips"), n)
  if (!length(gcols)) {
    add(acc, "WARN", "B04", rel, "no geography, fips or geography_fips column", key = "nogeo")
  } else {
    for (g in gcols) {
      nna <- sum(is.na(d[[g]]) | d[[g]] %in% c("", "NA"))
      if (nna) add(acc, "FAIL", "B04", paste0(rel, ":", g), "missing geography", sprintf("rows=%d", nna), key = paste(g, "na"), n = nna)
      if (is_num[[g]]) add(acc, "WARN", "B04", paste0(rel, ":", g), "FIPS codes stored as a number, leading zeros are lost", paste0("type=", ty[[g]]), key = paste(g, "numeric"))
    }
    codes <- any(vapply(gcols, function(g) isTRUE(mean(geo_class(d[[g]]) %in% c("national", "state", "county")) > 0.9), NA))
    if (tab$rows && !codes) add(acc, "WARN", "B04", rel, "geography is stored as names with no FIPS column next to it", key = "names")
  }

  if (!"source" %in% n) {
    add(acc, "WARN", "B05", rel, "no source column, the MCP server needs one to serve this file", key = "nosource")
  } else {
    sv <- unique(d$source[!is.na(d$source)])
    odd <- sv[nchar(sv) > 90 | !grepl(MCP_LABEL_OK, sv, perl = TRUE) | grepl("[<>](?!=?\\s*\\d)", sv, perl = TRUE) |
              grepl(MCP_LABEL_BAD, sv, ignore.case = TRUE, perl = TRUE)]
    if (length(odd)) add(acc, "WARN", "B05", paste0(rel, ":source"),
                         "source labels the MCP server rejects (allowed: letters, digits, spaces and , . % ( ) / + & ' - :, 90 characters at most)",
                         some(odd, 3), key = "label", items = odd)
    say("INFO", "B05", paste0(rel, ":source"), "source labels", some(sv, 8))
  }

  if (!"value" %in% n || !is_num[["value"]]) {
    add(acc, "ASK", "B06", rel, "no numeric value column, so this is a wide file. create-bundle allows wide files, the guardrail skill and the MCP server expect long",
        key = "novalue")
  } else {
    extra <- setdiff(n[is_num & grepl("^(value|pct_|rate_|n_|percent_)", n)], MCP_EXTRA)
    if (length(extra)) add(acc, "ASK", "B06", rel, "numeric columns beyond value that the MCP server reads as a wide file",
                           paste0("columns=", some(extra, 6)), key = paste(sort(extra), collapse = ","))
  }
  if (all(c("stratum_type", "stratum") %in% n)) say("INFO", "B06", rel, "generic stratum_type and stratum pair, the MCP server does not draft these")

  for (s in n[grepl(MCP_STRAT, n, ignore.case = TRUE) & !is_num]) {
    lv <- unique(d[[s]][!is.na(d[[s]])])
    if (length(lv) < 2) next
    if (length(lv) > 60) add(acc, "WARN", "B07", paste0(rel, ":", s), "more than 60 levels in a stratifier column", sprintf("levels=%d", length(lv)), key = paste(s, "many"))
    else if (!any(AGG %in% lv)) add(acc, "WARN", "B07", paste0(rel, ":", s),
      "no all-groups row (Total, Overall, All, All ages, All Ages, Both sexes or All races), the MCP server needs one per stratifier",
      sprintf("levels=%s", some(lv, 6)), key = paste(s, "noagg"))
  }

  disc <- intersect(c("measure", "outcome_name", "condition"), n)
  if (length(disc)) {
    for (k in disc) {
      e <- rendered[[paste0(file_id, "|", k)]]
      vals <- unique(d[[k]][!is.na(d[[k]])])
      if (is.null(e) || is.null(e$levels)) {
        add(acc, "WARN", "B08", paste0(rel, ":", k), "no levels entry in measure_info for this measure column", sprintf("values=%d", length(vals)), key = paste(k, "nolevels"))
        next
      }
      lost <- setdiff(vals, names(e$levels))
      if (length(lost)) add(acc, "FAIL", "B08", paste0(rel, ":", k), "values in the data with no level in measure_info",
                            sprintf("n=%d; values=%s", length(lost), some(lost, 5)), key = paste(k, "missing"), n = length(lost))
      bare <- names(e$levels)[vapply(e$levels, function(l) !nzchar(txt(l$unit)) || !nzchar(txt(l$measure_type)), NA)]
      bare <- intersect(bare, vals)
      if (length(bare)) add(acc, "WARN", "B08", paste0(rel, ":", k), "levels without measure_type and unit written on the level itself, the MCP server does not follow source_id",
                            sprintf("n=%d of %d; levels=%s", length(bare), length(vals), some(bare, 5)), key = paste(k, "units"), n = length(bare))
    }
  } else if ("value" %in% n) {
    e <- rendered[[paste0(file_id, "|value")]]
    add(acc, "WARN", "B08", paste0(rel, ":value"), "no measure, outcome_name or condition column, so the MCP server cannot read units for this file", key = "value_unit")
  }
  if ("value" %in% n && is_num[["value"]] && tab$rows) {
    by <- if (length(disc)) d[[disc[1]]] else rep("value", nrow(d))
    v <- num(d$value)
    rg <- tapply(v, by, function(x) if (all(is.na(x))) "all missing" else sprintf("%.4g..%.4g (missing %d)", min(x, na.rm = TRUE), max(x, na.rm = TRUE), sum(is.na(x))))
    say("INFO", "B00", paste0(rel, ":value"), if (length(disc)) paste0("value range by ", disc[1]) else "value range",
        paste0(paste(utils::head(paste0(names(rg), " ", rg), 12), collapse = "; "), if (length(rg) > 12) sprintf("; (+%d more)", length(rg) - 12) else ""))
  }

  kc <- n[!is_num | n %in% c("year", tcol, gcols) | grepl(MCP_STRAT, n, ignore.case = TRUE) | grepl(NUM_STRAT, n)]
  kc <- kc[!grepl(META_RE, kc, ignore.case = TRUE) | kc %in% tcol]
  if (length(kc) && tab$rows && !tab$cut) {
    k <- key_of(d, kc)
    dup <- sum(duplicated(k))
    if (dup) add(acc, "WARN", "D05", rel, "duplicate rows on the non-numeric columns", sprintf("rows=%d; examples=%s", dup, some(show_key(k[duplicated(k)]), 2)), key = "dup", n = dup)
  }
  invisible(NULL)
}

build_reads <- function(path) {
  src <- strip_comments(readLines(path, warn = FALSE, encoding = "UTF-8"))
  lit <- gsub("^[\"']|[\"']$", "", unlist(regmatches(src, gregexpr("([\"'])[^\"']*\\1", src))))
  full <- lit[grepl("(^|/)[A-Za-z0-9_]+/(standard|dist)/[^/]+\\.(csv\\.gz|csv|parquet|json)$", lit)]
  full <- full[!grepl("%[sd]|[{*]", full)]   # patterns filled in at run time are not paths
  dyn_lines <- grep("(sprintf|file\\.path|paste0?)\\(.*(standard|\\.\\./)", src)
  dyn_lines <- dyn_lines[!vapply(src[dyn_lines], function(l) {
    s <- gsub("^[\"']|[\"']$", "", unlist(regmatches(l, gregexpr("([\"'])[^\"']*\\1", l))))
    any(grepl("(standard|dist)/[^/]+\\.(csv\\.gz|csv|parquet|json)$", s)) && !any(grepl("%[sd]|[{*]", s))
  }, NA)]
  list(paths = sort(unique(sub("^(\\.\\./)+(data/)?", "", full))), dynamic = dyn_lines,
       uses_process = any(grepl("process\\$source_files|source_files", src)),
       remote = grep("download\\.file\\(|https?://|/raw/", src))
}

check_bundle <- function(dir_rel, proc, info, files, base_files, info_changed = TRUE) {
  target <- file.path(dir_rel, "build.R")
  bp <- fetch("head", target)
  has_base <- !is.null(BASE_REF) && nrow(base_files) > 0
  same_code <- has_base && same_as_base(target)
  bproc <- NULL
  if (has_base) {
    bpp <- fetch("base", file.path(dir_rel, "process.json"))
    if (!is.null(bpp)) bproc <- tryCatch(jsonlite::read_json(bpp), error = function(e) NULL)
  }
  # process.json changes on every build, what matters here is the list of source files
  same_build <- same_code && !is.null(bproc) && setequal(names(proc$source_files), names(bproc$source_files))
  in_dist <- function(f) f$path[startsWith(f$path, file.path(dir_rel, "dist/"))]
  same_dist <- has_base && setequal(in_dist(files), in_dist(base_files))
  if (!is.null(bp) && !is.null(proc)) {
    r <- build_reads(bp)
    own <- paste0(basename(dir_rel), "/dist/")
    reads <- r$paths[!startsWith(r$paths, own)]
    sf <- names(proc$source_files)
    not_listed <- setdiff(reads, sf)
    not_read <- setdiff(sf, reads)
    ptarget <- file.path(dir_rel, "process.json")
    if (length(not_listed)) emit_base("FAIL", "B09", ptarget, "build.R reads files that are not in source_files, changes to them will not trigger a rebuild", paste0("files=", some(not_listed, 6)), same = same_build)
    if (length(r$dynamic)) {
      if (!same_code) emit("HUMAN", "B09", target, "paths are built at run time, check by hand that each file read is in source_files", paste0("lines=", some(r$dynamic, 8)))
    } else if (length(not_read) && !r$uses_process) {
      emit_base("WARN", "B09", ptarget, "source_files lists files that build.R does not read", paste0("files=", some(not_read, 6)), same = same_build)
    }
    if (!length(not_listed) && !length(r$dynamic)) emit("PASS", "B09", ptarget, "every file build.R reads is in source_files", sprintf("files=%d", length(reads)))
    gone <- sf[vapply(sf, function(f) is.null(fetch("head", file.path("data", f))), NA)]
    if (length(gone)) emit_base("FAIL", "B09", ptarget, "source_files lists files that do not exist", paste0("files=", some(gone, 6)),
                                same = same_build && all(vapply(gone, function(f) is.null(fetch("base", file.path("data", f))), NA)))
    if (length(r$remote)) emit_base("WARN", "B09", target, "build.R downloads or reads raw data, bundles should only read standard files", paste0("lines=", some(r$remote, 6)), same = same_build)
  }
  dist <- files$path[startsWith(files$path, file.path(dir_rel, "dist/"))]
  stray <- dist[!grepl("\\.parquet$|datapackage\\.json$|\\.gitignore$", dist)]
  if (length(stray)) emit_base("FAIL", "B01", file.path(dir_rel, "dist"), "files in dist/ that are not parquet", paste0("files=", some(basename(stray))),
                               same = has_base && all(stray %in% base_files$path))
  else emit("PASS", "B01", file.path(dir_rel, "dist"), "dist/ holds only parquet files")
  pq <- basename(grep("\\.parquet$", dist, value = TRUE))
  bundle <- info$raw[["_bundle"]]
  if (is.null(bundle)) {
    emit(if (is.null(BASE_REF) || is.null(fetch("base", file.path(dir_rel, "measure_info.json")))) "WARN" else "INFO", "B10",
         file.path(dir_rel, "measure_info.json"), "no _bundle block listing the dist files and their sources")
  } else {
    lost <- setdiff(pq, names(bundle$dist_files))
    fresh <- lost[!file.path(dir_rel, "dist", lost) %in% base_files$path]
    if (length(fresh)) emit(if (is.null(BASE_REF)) "WARN" else "FAIL", "B10", file.path(dir_rel, "measure_info.json"),
                            if (is.null(BASE_REF)) "parquet files missing from _bundle.dist_files" else "new parquet files missing from _bundle.dist_files",
                            paste0("files=", some(fresh)))
    if (length(setdiff(lost, fresh))) emit("INFO", "B10", file.path(dir_rel, "measure_info.json"), "parquet files missing from _bundle.dist_files that were already missing on base", paste0("files=", some(setdiff(lost, fresh))))
    extra <- setdiff(names(bundle$dist_files), pq)
    if (length(extra)) emit_base("WARN", "B10", file.path(dir_rel, "measure_info.json"), "_bundle.dist_files lists parquet files that are not in dist/", paste0("files=", some(extra)),
                                 same = same_dist && !info_changed)
    if (!length(lost) && !length(extra)) emit("PASS", "B10", file.path(dir_rel, "measure_info.json"), "_bundle.dist_files matches dist/")
  }
  if (!is.null(BASE_REF) && !is.null(bp) && !same_as_base(target) && !is.null(fetch("base", target))) {
    pqs <- grep("\\.parquet$", dist, value = TRUE)
    if (length(pqs) && all(vapply(pqs, same_as_base, NA))) {
      emit("WARN", "B11", target, "build.R changed but every dist file is identical to base, rebuild with dcf::dcf_process(\"<bundle>\", clear_state = TRUE)")
    }
  }
  invisible(NULL)
}

# process.json of every bundle on the reviewed side, read once.
BUNDLES <- NULL
bundle_procs <- function() {
  if (!is.null(BUNDLES)) return(BUNDLES)
  if (is.null(HEAD_REF)) {
    dirs_b <- basename(dirname(Sys.glob(file.path(ROOT, "data", "bundle_*", "process.json"))))
  } else {
    l <- git(c("ls-tree", "--name-only", HEAD_REF, "data/"))
    dirs_b <- basename(grep("^data/bundle_", as.character(l), value = TRUE))
  }
  out <- list()
  for (b in dirs_b) {
    p <- fetch("head", file.path("data", b, "process.json"))
    pr <- if (is.null(p)) NULL else tryCatch(jsonlite::read_json(p), error = function(e) NULL)
    if (!is.null(pr)) out[[b]] <- pr
  }
  BUNDLES <<- out
  out
}

# Bundles that read a standard file of this source.
downstream <- function(dir_rel) {
  name <- basename(dir_rel)
  bp <- bundle_procs()
  for (b in names(bp)) {
    sf <- as.character(names(bp[[b]]$source_files))
    hit <- sf[startsWith(sf, paste0(name, "/"))]
    if (!length(hit)) next
    outs <- unique(unlist(bp[[b]]$source_files[hit]))
    emit("INFO", "C07", dir_rel, "feeds a bundle, so changes here reach pophive.org",
         sprintf("bundle=%s; files=%s; dist=%s", b, some(basename(hit), 4), some(outs, 5)))
    if (is.null(BASE_REF)) next
    moved <- hit[!vapply(file.path("data", hit), same_as_base, NA)]
    dist <- file.path("data", b, "dist", unique(unlist(bp[[b]]$source_files[moved])))
    dist <- dist[grepl("\\.parquet$", dist)]
    if (length(moved) && length(dist) && all(vapply(dist, same_as_base, NA))) {
      emit(if (CODE_CHANGED) "WARN" else "INFO", "C07", file.path("data", b),
           "bundle not rebuilt in this change: these source files differ from base and the dist files built from them do not",
           sprintf("files=%s; dist=%s", some(moved, 4), some(basename(dist), 4)))
    }
  }
}

read_header <- function(path) {
  if (grepl("\\.parquet$", path, ignore.case = TRUE)) return(arrow::ParquetFileReader$create(path)$GetSchema()$names)
  first <- tryCatch(readLines(path, n = 1, warn = FALSE), error = function(e) "")
  delim <- if (length(first) && grepl("\t", first) && !grepl(",", first)) "\t" else ","
  names(vroom::vroom(path, delim = delim, col_types = vroom::cols(.default = vroom::col_character()), n_max = 0,
                     show_col_types = FALSE, progress = FALSE))
}

# -----------------------------------------------------------------------------
# One folder
# -----------------------------------------------------------------------------
CRASHED <- FALSE
guard <- function(step, target, expr) {
  tryCatch(expr, error = function(e) {
    CRASHED <<- TRUE
    emit("FAIL", "X00", target, paste0(step, " stopped with an R error, its remaining checks did not run"), conditionMessage(e))
    NULL
  })
}
summary_line <- function(dir_rel, kind) {
  cat(sprintf("== SUMMARY dir=%s kind=%s fail=%d warn=%d ask=%d human=%d info=%d pass=%d skip=%d\n", dir_rel, kind,
              get("FAIL", COUNT), get("WARN", COUNT), get("ASK", COUNT), get("HUMAN", COUNT), get("INFO", COUNT),
              get("PASS", COUNT), get("SKIP", COUNT)))
}
STD_IDS <- c("S05", "D01", "D02", "D03", "D04", "D05", "D06", "D07", "D08", "D09", "D10", "D11", "M08", "M09")
DIST_IDS <- c("B02", "B03", "B04", "B05", "B06", "B07", "B08", "D05")

review_dir <- function(dir_arg) {
  reset_counts()
  dir_rel <- sub("/+$", "", gsub("\\\\", "/", dir_arg))
  full <- if (dir.exists(dir_rel)) normalizePath(dir_rel, winslash = "/") else dir_rel
  if (startsWith(tolower(full), tolower(paste0(ROOT, "/")))) dir_rel <- substring(full, nchar(ROOT) + 2)
  dir_rel <- sub("^(\\./)+", "", dir_rel)
  files <- ls_side("head", dir_rel)
  base_files <- ls_side("base", dir_rel)
  files <- files[basename(files$path) != ".DS_Store", , drop = FALSE]
  has <- function(f) file.path(dir_rel, f) %in% files$path
  kind <- if (grepl("^bundle_", basename(dir_rel)) || has("build.R")) "bundle" else "source"
  if (kind == "source" && has("ingest.R")) {
    code <- strip_comments(readLines(fetch("head", file.path(dir_rel, "ingest.R")), warn = FALSE, encoding = "UTF-8"))
    if (any(grepl("raw\\.githubusercontent\\.com/PopHIVE/", code))) kind <- "pull"
  }
  cat(sprintf("@@ %s kind=%s head=%s base=%s\n", dir_rel, kind, if (is.null(HEAD_REF)) "working tree" else short_sha(HEAD_REF),
              if (is.null(BASE_REF)) "none" else short_sha(BASE_REF)))
  if (!nrow(files)) {
    if (nrow(base_files)) {
      emit("WARN", "C01", dir_rel, "the whole folder is deleted", sprintf("files=%d", nrow(base_files)))
      name <- basename(dir_rel)
      users <- character()
      for (b in names(bundle_procs())) {
        listed <- any(startsWith(as.character(names(bundle_procs()[[b]]$source_files)), paste0(name, "/")))
        bp <- fetch("head", file.path("data", b, "build.R"))
        reads <- !is.null(bp) && any(grepl(paste0("(^|[^A-Za-z0-9_])", name, "/(standard|dist)"),
                                           strip_comments(readLines(bp, warn = FALSE, encoding = "UTF-8"))))
        if (listed || reads) users <- c(users, b)
      }
      if (length(users)) emit("FAIL", "C01", dir_rel, "bundles still list or read files of the deleted folder", paste0("bundles=", some(users, 8)))
      else emit("PASS", "C01", dir_rel, "no bundle lists or reads the deleted folder")
    } else {
      emit("FAIL", "S01", dir_rel, "folder not found")
    }
    summary_line(dir_rel, kind)
    return(invisible(NULL))
  }
  if (kind == "pull") emit("INFO", "S01", dir_rel, "pull script: standard files are produced in another PopHIVE repository and mirrored here, review the transformation there")

  has_base <- !is.null(BASE_REF) && nrow(base_files) > 0
  proc <- guard("check_structure", dir_rel, check_structure(dir_rel, kind, files, base_files))
  info <- load_info("head", dir_rel)
  info_base <- load_info("base", dir_rel)
  info_changed <- !same_as_base(file.path(dir_rel, "measure_info.json"))

  out_dir <- file.path(dir_rel, if (kind == "bundle") "dist" else "standard")
  in_out <- function(p) startsWith(p, paste0(out_dir, "/")) & !grepl("datapackage\\.json$|\\.gitignore$", p)
  data_files <- files$path[in_out(files$path)]
  all_data_files <- data_files
  base_data <- base_files$path[in_out(base_files$path)]
  if (!is.null(ONLY_FILES)) {
    data_files <- data_files[basename(data_files) %in% ONLY_FILES]
    base_data <- base_data[basename(base_data) %in% ONLY_FILES]
  }
  if (!length(data_files)) emit_base("WARN", "S05", out_dir, "no data files found", same = has_base && setequal(files$path, base_files$path))

  # deleted files
  feeds <- as.character(unlist(lapply(bundle_procs(), function(p) names(p$source_files))))
  changelog <- FALSE
  for (rel in setdiff(base_data, data_files)) {
    used <- sub("^data/", "", rel) %in% feeds
    emit(if (kind == "bundle" || used) "FAIL" else "WARN", "C01", rel,
         if (kind == "bundle") "dist file deleted, anything reading it from the site or the MCP server will break"
         else if (used) "standard file deleted but a bundle still lists it in source_files" else "standard file deleted")
    changelog <- TRUE
  }

  headers <- list()
  base_headers <- list()
  std_tabs <- list()
  std_rels <- character()
  any_changed <- FALSE
  value_cols <- character()
  for (rel in data_files) {
    readable <- grepl(if (kind == "bundle") "\\.parquet$" else "\\.csv\\.gz$|\\.csv$|\\.parquet$", rel)
    if (kind != "bundle" && !grepl("\\.csv\\.gz$", rel)) {
      emit(if (rel %in% base_data) "INFO" else "FAIL", "S05", rel, "standard files must be gzip-compressed csv (.csv.gz)")
    }
    if (!readable) next
    p <- fetch("head", rel)
    if (grepl("\\.gz$", rel)) {
      magic <- readBin(p, "raw", 2)
      if (!identical(as.integer(magic), c(31L, 139L))) emit("FAIL", "S05", rel, "file is named .gz but is not gzip-compressed")
    }
    is_new <- !rel %in% base_files$path
    unchanged <- !is_new && same_as_base(rel)
    if (!unchanged) any_changed <- TRUE
    if (kind != "bundle") std_rels <- c(std_rels, rel)
    # identical to base and nothing new to check it against: the header is enough
    if (unchanged && !info_changed) {
      hd <- tryCatch(read_header(p), error = function(e) NULL)
      if (!is.null(hd)) {
        headers[[rel]] <- hd
        base_headers[[rel]] <- hd
        emit("PASS", "C05", rel, "identical to base, data checks skipped")
        next
      }
    }
    tab <- tryCatch(read_table(p), error = function(e) e)
    if (inherits(tab, "error")) {
      emit("FAIL", "S05", rel, "file cannot be read", conditionMessage(tab))
      next
    }
    headers[[rel]] <- names(tab$d)
    btab <- NULL
    if (!is_new && !is.null(BASE_REF)) {
      bp <- fetch("base", rel)
      if (!is.null(bp)) {
        if (unchanged) {
          base_headers[[rel]] <- names(tab$d)
        } else {
          btab <- tryCatch(read_table(bp), error = function(e) NULL)
          if (!is.null(btab)) base_headers[[rel]] <- names(btab$d)
        }
      }
    }
    if (kind != "bundle") std_tabs[[rel]] <- tab
    if (unchanged && !info_changed) {
      emit("PASS", "C05", rel, "identical to base, data checks skipped")
      next
    }
    if (is_new && !is.null(BASE_REF)) emit("INFO", "C00", rel, "new file")
    flip <- if (!is.null(btab)) tryCatch(flipped_cols(tab, btab), error = function(e) character()) else character()
    value_cols <- union(value_cols, if (kind == "bundle") {
      paste0(sub("^data/", "", rel), "|", names(tab$d)[grepl(NUM_TYPE, tab$types)])
    } else {
      rr <- roles(tab, flip)
      c(rr$values, rr$bounds)
    })
    # A file identical to base is only re-checked because measure_info changed,
    # so it is checked quietly and only new findings are printed.
    acc <- new_acc()
    bacc <- NULL
    ran <- if (kind == "bundle") {
      guard("data checks", rel, {
        check_dist(tab, rel, info$rendered, acc, quiet = unchanged)
        TRUE
      })
    } else {
      guard("data checks", rel, {
        check_standard(tab, rel, info$rendered, acc, quiet = unchanged, force_values = flip)
        TRUE
      })
    }
    if (unchanged || !is.null(btab)) {
      bacc <- new_acc()
      checker <- if (kind == "bundle") check_dist else check_standard
      guard("data checks on the base version", rel, checker(if (unchanged) tab else btab, rel, info_base$rendered, bacc, quiet = TRUE))
    }
    before <- sum(vapply(c("FAIL", "WARN", "ASK"), function(x) get(x, COUNT), 0L))
    report(acc, bacc, rel, show_old = !unchanged)
    after <- sum(vapply(c("FAIL", "WARN", "ASK"), function(x) get(x, COUNT), 0L))
    if (unchanged && after == before) emit("PASS", "C05", rel, "identical to base, nothing new from the measure_info change")
    if (!unchanged && isTRUE(ran)) {
      raised <- unique(vapply(Filter(function(x) x$sev %in% c("FAIL", "WARN", "ASK"), acc$l), function(x) x$id, ""))
      clean <- setdiff(if (kind == "bundle") DIST_IDS else STD_IDS, raised)
      if (length(clean)) emit("PASS", if (kind == "bundle") "B00" else "D00", rel, "checks that raised nothing on this file", paste(clean, collapse = ", "))
    }
    if (!is.null(btab)) changelog <- isTRUE(guard("comparison with base", rel, compare_tables(tab, btab, rel, kind == "bundle", flip))) || changelog
  }
  attr(headers, "base") <- base_headers
  attr(headers, "all") <- all_data_files
  guard("check_metadata", file.path(dir_rel, "measure_info.json"), check_metadata(dir_rel, kind, info, info_base, headers, value_cols, info_changed))

  if (kind == "bundle") {
    guard("check_bundle", dir_rel, check_bundle(dir_rel, proc, info, files, base_files, info_changed))
  } else {
    guard("downstream bundles", dir_rel, downstream(dir_rel))
    if (kind == "source") {
      same_script <- has_base && same_as_base(file.path(dir_rel, "ingest.R"))
      raw_list <- function(f) {
        r <- f[startsWith(f$path, file.path(dir_rel, "raw/")), , drop = FALSE]
        sort(paste(r$path, r$size))
      }
      if (has_base && same_script && !any_changed && identical(raw_list(files), raw_list(base_files))) {
        emit("SKIP", "R01", file.path(dir_rel, "raw"), "raw files, standard files and ingest.R are identical to base, raw and standard counts not tallied again")
      } else {
        guard("check_raw", file.path(dir_rel, "raw"),
              check_raw(dir_rel, files, std_tabs, std_rels, fetch("head", file.path(dir_rel, "ingest.R")), same_script))
      }
    } else {
      emit("SKIP", "R01", dir_rel, "pull script, raw and standard counts are compared in the source repository")
    }
  }
  if (changelog) emit("HUMAN", "C08", dir_rel, "published values, rows or levels changed. If this is a major fix, add an entry at the end of the Major change log section of README.md")
  summary_line(dir_rel, kind)
  invisible(NULL)
}

# -----------------------------------------------------------------------------
# Run
# -----------------------------------------------------------------------------
om <- git(c("log", "-1", "--format=%h %cd", "--date=short", "origin/main"))
cat(sprintf("# review_checks  root=%s  origin/main=%s  rules: weekly=Saturday monthly=%s day annual=YYYY-%s (CLAUDE.md)%s\n",
            ROOT, if (is.null(om)) "not found" else om[1], RULE$monthly, RULE$annual,
            if (is.null(BASE_REF)) "" else paste0("  code_changed=", if (CODE_CHANGED) "yes" else "no")))
for (d in dirs) {
  tryCatch(review_dir(d), error = function(e) {
    CRASHED <<- TRUE
    cat("ENV FAIL | ", d, ": ", conditionMessage(e), "\n", sep = "")
  })
}
unlink(TMP, recursive = TRUE)
quit(save = "no", status = if (CRASHED) 3 else 0)
