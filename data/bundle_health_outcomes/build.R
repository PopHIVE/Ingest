# =============================================================================
# Bundle: Health Outcomes
# Combines ASCVD risk, alcohol, opioid overdose, and maternal health outcomes.
#
# Sources:
#   - ascvd/standard/data.csv.gz    (Veradigm 10-year ASCVD risk, ZCTA3)
#   - alcohol/standard/data.csv.gz  (NEMSIS alcohol-related EMS rate, county)
#   - opioid/standard/data.csv.gz   (NEMSIS opioid overdose EMS rate, county)
#   - maternal/standard/data.csv.gz (HealthVerity severe obstetric
#                                    complications, ZCTA3)
#
# Outputs (one wide-format parquet per source in dist/). The sources are not
# merged into one table: they differ in geography (FIPS vs ZCTA3), measure type
# (mean risk vs binned rate ranges), and demographic stratification, and the
# binned rate ranges must never be collapsed to a point estimate.
#   - dist/ascvd.parquet
#   - dist/alcohol.parquet
#   - dist/opioid.parquet
#   - dist/maternal.parquet
# =============================================================================

library(dplyr)
library(vroom)
library(arrow)

# geography must stay character: ZCTA3 codes can have leading zeros
# (e.g. "006") and FIPS codes are fixed-width strings.
read_standard <- function(path) {
  vroom(
    path,
    col_types = cols(
      geography = col_character(),
      geography_type = col_character(),
      time = col_date()
    ),
    show_col_types = FALSE
  )
}

SOURCES <- c(
  ascvd = "../ascvd/standard/data.csv.gz",
  alcohol = "../alcohol/standard/data.csv.gz",
  opioid = "../opioid/standard/data.csv.gz",
  maternal = "../maternal/standard/data.csv.gz"
)

dir.create("dist", showWarnings = FALSE)

for (name in names(SOURCES)) {
  data <- read_standard(SOURCES[[name]]) %>%
    arrange(geography, time)
  write_parquet(data, file.path("dist", paste0(name, ".parquet")))
  message(
    "bundle_health_outcomes: ", name, ".parquet : ", nrow(data), " rows, ",
    n_distinct(data$geography), " geographies (",
    paste(sort(unique(data$geography_type)), collapse = ", "), ")"
  )
}
