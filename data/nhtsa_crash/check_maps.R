# =============================================================================
# NHTSA FARS – County Fatality Rate Choropleth Maps (3 years)
# Run from: data/nhtsa_crash/
# =============================================================================

library(dplyr)
library(tidyr)
library(vroom)
library(sf)
library(tigris)
library(ggplot2)
library(scales)

# Three years spread across the 2000-2023 range
selected_years <- c("2005", "2013", "2023")
selected_times <- paste0(selected_years, "-12-31")

# -----------------------------------------------------------------------------
# 1. Load county-level NHTSA data
#    Fix SD FIPS: FARS uses 46113 (Shannon Co.) throughout all years, but
#    2023 tigris geometry uses 46102 (Oglala Lakota Co.) after the 2015 rename.
# -----------------------------------------------------------------------------
cat("Loading NHTSA county data...\n")
county_data <- vroom("standard/data.csv.gz", show_col_types = FALSE) |>
  filter(nchar(geography) == 5) |>
  mutate(
    geography = if_else(geography == "46113", "46102", geography),
    time      = as.character(time)
  )

# -----------------------------------------------------------------------------
# 2. Build county geometry
#    - Base: cached 2023 counties (excludes AK, HI, territories)
#    - CT fix: 2023 geometry has new planning region codes (09110-09190) but
#      FARS uses old county codes (09001-09015). Replace CT with year=2021
#      boundaries so geometries match FARS FIPS codes.
# -----------------------------------------------------------------------------
cat("Loading county geometries...\n")
counties_base <- readRDS("../heat_risk/raw/counties.rds") |>
  mutate(geography = as.character(geography)) |>
  filter(
    !substr(geography, 1, 2) %in% c("02", "15"),   # drop AK, HI
    substr(geography, 1, 2) != "09"                 # drop new CT planning regions
  )

cat("Fetching old CT county boundaries (year=2021)...\n")
ct_old <- tigris::counties(
  state = "09", cb = TRUE, resolution = "5m",
  year = 2021, progress_bar = FALSE
) |>
  select(geography = GEOID) |>
  mutate(geography = as.character(geography))

counties_sf <- bind_rows(counties_base, ct_old)

# -----------------------------------------------------------------------------
# 3. Build complete county × year grid; fill missing with 0
#    Missing = county in geometry with no FARS rows for that year → zero
#    fatalities, not a boundary mismatch (those were fixed above).
# -----------------------------------------------------------------------------
complete_grid <- expand.grid(
  geography = counties_sf$geography,
  time      = selected_times,
  stringsAsFactors = FALSE
)

county_filled <- complete_grid |>
  left_join(
    county_data |> filter(time %in% selected_times),
    by = c("geography", "time")
  ) |>
  mutate(across(
    c(nhtsa_fatalities, nhtsa_fatal_crashes, nhtsa_fatality_rate),
    \(x) replace_na(x, 0)
  ))

# -----------------------------------------------------------------------------
# 4. Join geometry and prepare map data
# -----------------------------------------------------------------------------
map_data <- counties_sf |>
  left_join(county_filled, by = "geography") |>
  mutate(
    date_label = factor(
      time,
      levels = selected_times,
      labels = paste("Year", selected_years)
    )
  )

cat(
  "Rate range (per 100k):",
  min(county_filled$nhtsa_fatality_rate, na.rm = TRUE),
  "to",
  max(county_filled$nhtsa_fatality_rate, na.rm = TRUE), "\n"
)

# Cap display at 99th percentile to reduce outlier distortion from small counties
rate_cap <- quantile(
  county_filled$nhtsa_fatality_rate[county_filled$nhtsa_fatality_rate > 0],
  0.99, na.rm = TRUE
)
map_data <- map_data |>
  mutate(rate_display = pmin(nhtsa_fatality_rate, rate_cap))

# -----------------------------------------------------------------------------
# 5. Plot
# -----------------------------------------------------------------------------
cat("Rendering maps...\n")
p <- ggplot(map_data) +
  geom_sf(aes(fill = rate_display), color = NA) +
  scale_fill_viridis_c(
    option    = "inferno",
    direction = 1,
    trans     = "sqrt",
    name      = "Traffic\nFatalities\nper 100k",
    labels    = label_number(accuracy = 1),
    na.value  = "grey80",
    guide     = guide_colorbar(
      barwidth  = unit(0.5, "cm"),
      barheight = unit(6,   "cm"),
      ticks     = TRUE
    )
  ) +
  facet_wrap(~date_label, ncol = 1) +
  coord_sf(crs = 5070, datum = NA) +
  labs(
    title    = "NHTSA FARS \u2013 County Traffic Fatality Rate",
    subtitle = paste0(
      "Fatalities per 100,000 population (2021 census denominator)\n",
      "Zero-fatality counties shown as black; ",
      "fill capped at 99th pctile (~", round(rate_cap, 0), " per 100k); sqrt scale"
    ),
    caption = paste0(
      "Source: NHTSA Fatality Analysis Reporting System (FARS)\n",
      "https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/"
    )
  ) +
  theme_void(base_size = 11) +
  theme(
    plot.title      = element_text(face = "bold", hjust = 0.5, size = 13),
    plot.subtitle   = element_text(hjust = 0.5, color = "grey40", size = 8),
    plot.caption    = element_text(hjust = 0.5, color = "grey50", size = 7),
    strip.text      = element_text(face = "bold", size = 10),
    legend.position = "right",
    plot.margin     = margin(10, 10, 10, 10)
  )

out_path <- "standard/nhtsa_fatality_rate_choropleth.png"
ggsave(out_path, p, width = 9, height = 14, dpi = 150, bg = "white")
cat("Saved:", out_path, "\n")
