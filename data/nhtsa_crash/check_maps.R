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
library(patchwork)

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
View(county_data)
# -----------------------------------------------------------------------------
# 2. Build county geometry
#    - Base: cached 2023 counties (excludes AK, HI, territories)
#    - CT fix: 2023 geometry has new planning region codes (09110-09190) but
#      FARS uses old county codes (09001-09015). Replace CT with year=2021
#      boundaries so geometries match FARS FIPS codes.
# -----------------------------------------------------------------------------
cat("Loading county geometries...\n")
counties_base <- readRDS("C:/Users/ashap/OneDrive/Desktop/Ingest/data/noaa_heat_risk/raw/counties.rds") |>
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
p
out_path <- "standard/nhtsa_fatality_rate_choropleth.png"
ggsave(out_path, p, width = 9, height = 14, dpi = 150, bg = "white")
cat("Saved:", out_path, "\n")

# =============================================================================
# Shared: State geometry (reused by both NHTSA state and WISQARS sections)
# =============================================================================

cat("Loading state geometries...\n")
states_sf <- tigris::states(cb = TRUE, resolution = "5m", year = 2021,
                             progress_bar = FALSE) |>
  filter(!STUSPS %in% c("AK", "HI", "PR", "VI", "GU", "MP", "AS")) |>
  select(geography = GEOID) |>
  mutate(geography = as.character(geography))

# =============================================================================
# NHTSA FARS – State Fatality Rate Choropleth Maps (3 years)
# =============================================================================

cat("Loading NHTSA state data...\n")
nhtsa_state_data <- vroom("standard/data.csv.gz", show_col_types = FALSE) |>
  filter(
    nchar(geography) == 2,
    geography != "00",
    time %in% selected_times
  ) |>
  select(geography, time, nhtsa_fatality_rate) |>
  mutate(time = as.character(time))

complete_nhtsa_state_grid <- expand.grid(
  geography = states_sf$geography,
  time      = selected_times,
  stringsAsFactors = FALSE
)

nhtsa_state_filled <- complete_nhtsa_state_grid |>
  left_join(nhtsa_state_data, by = c("geography", "time")) |>
  mutate(nhtsa_fatality_rate = replace_na(nhtsa_fatality_rate, 0))

nhtsa_state_map_data <- states_sf |>
  left_join(nhtsa_state_filled, by = "geography") |>
  mutate(
    date_label = factor(
      time,
      levels = selected_times,
      labels = paste("Year", selected_years)
    )
  )

nhtsa_state_rate_cap <- quantile(
  nhtsa_state_filled$nhtsa_fatality_rate[nhtsa_state_filled$nhtsa_fatality_rate > 0],
  0.99, na.rm = TRUE
)
nhtsa_state_map_data <- nhtsa_state_map_data |>
  mutate(rate_display = pmin(nhtsa_fatality_rate, nhtsa_state_rate_cap))

cat("Rendering NHTSA state maps...\n")
p_nhtsa_state <- ggplot(nhtsa_state_map_data) +
  geom_sf(aes(fill = rate_display), color = "white", linewidth = 0.2) +
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
    title    = "NHTSA FARS – State Traffic Fatality Rate",
    subtitle = paste0(
      "Fatalities per 100,000 population (2021 census denominator, crude)\n",
      "Fill capped at 99th pctile (~", round(nhtsa_state_rate_cap, 1), " per 100k); sqrt scale"
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
p_nhtsa_state
out_path_nhtsa_state <- "standard/nhtsa_state_fatality_rate_choropleth.png"
ggsave(out_path_nhtsa_state, p_nhtsa_state, width = 9, height = 14, dpi = 150, bg = "white")
cat("Saved:", out_path_nhtsa_state, "\n")

# =============================================================================
# WISQARS – State Motor Vehicle Traffic Fatality Rate Choropleth Maps (3 years)
# =============================================================================

# wisqars uses YYYY-01-01 as time; match the same 3 reference years
wisqars_times <- paste0(selected_years, "-01-01")

cat("Loading WISQARS state data...\n")
wisqars_data <- vroom("../wisqars/standard/data.csv.gz", show_col_types = FALSE) |>
  filter(
    nchar(geography) == 2,
    geography != "00",
    age == "Total",
    sex == "All",
    race == "All",
    ethnicity == "All",
    time %in% wisqars_times
  ) |>
  select(geography, time, wisqars_rate_motor_vehicle_traffic) |>
  mutate(time = as.character(time))

complete_state_grid <- expand.grid(
  geography = states_sf$geography,
  time      = wisqars_times,
  stringsAsFactors = FALSE
)

wisqars_filled <- complete_state_grid |>
  left_join(wisqars_data, by = c("geography", "time"))

wisqars_map_data <- states_sf |>
  left_join(wisqars_filled, by = "geography") |>
  mutate(
    date_label = factor(
      time,
      levels = wisqars_times,
      labels = paste("Year", selected_years)
    )
  )

cat(
  "WISQARS rate range (per 100k):",
  min(wisqars_filled$wisqars_rate_motor_vehicle_traffic, na.rm = TRUE),
  "to",
  max(wisqars_filled$wisqars_rate_motor_vehicle_traffic, na.rm = TRUE), "\n"
)

wisqars_rate_cap <- quantile(
  wisqars_filled$wisqars_rate_motor_vehicle_traffic,
  0.99, na.rm = TRUE
)
wisqars_map_data <- wisqars_map_data |>
  mutate(rate_display = pmin(wisqars_rate_motor_vehicle_traffic, wisqars_rate_cap))

cat("Rendering WISQARS maps...\n")
p_wisqars <- ggplot(wisqars_map_data) +
  geom_sf(aes(fill = rate_display), color = "white", linewidth = 0.2) +
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
    title    = "WISQARS – State Motor Vehicle Traffic Fatality Rate",
    subtitle = paste0(
      "Fatalities per 100,000 population (age-adjusted, all ages)\n",
      "Fill capped at 99th pctile (~", round(wisqars_rate_cap, 1), " per 100k); sqrt scale"
    ),
    caption = paste0(
      "Source: CDC WISQARS Fatal Injury Reports\n",
      "https://wisqars.cdc.gov/"
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
p_wisqars
out_path_wisqars <- "standard/wisqars_motor_vehicle_rate_choropleth.png"
ggsave(out_path_wisqars, p_wisqars, width = 9, height = 14, dpi = 150, bg = "white")
cat("Saved:", out_path_wisqars, "\n")

# =============================================================================
# Combined: NHTSA state (left) vs WISQARS (right), side by side
# =============================================================================

cat("Rendering combined NHTSA state vs WISQARS maps...\n")
p_combined <- patchwork::wrap_plots(p_nhtsa_state, p_wisqars, ncol = 2) +
  plot_annotation(
    title   = "Motor Vehicle Traffic Fatality Rate by State: NHTSA FARS vs. WISQARS",
    subtitle = paste0(
      "Left: NHTSA FARS crude rate (fatalities per 100k, 2021 census denominator)  |  ",
      "Right: WISQARS age-adjusted rate (fatalities per 100k)\n",
      "Years: ", paste(selected_years, collapse = ", ")
    ),
    theme = theme(
      plot.title    = element_text(face = "bold", hjust = 0.5, size = 14),
      plot.subtitle = element_text(hjust = 0.5, color = "grey40", size = 9)
    )
  )
p_combined
out_path_combined <- "standard/nhtsa_vs_wisqars_state_choropleth.png"
ggsave(out_path_combined, p_combined, width = 18, height = 14, dpi = 150, bg = "white")
cat("Saved:", out_path_combined, "\n")

# =============================================================================
# Scatter: NHTSA vs WISQARS state rates, faceted by year
# Each point = one state; dashed diagonal = perfect agreement
# =============================================================================

state_abbrevs <- vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE) |>
  filter(nchar(geography) == 2, geography != "00") |>
  select(geography, state)

nhtsa_all_years <- vroom("standard/data.csv.gz", show_col_types = FALSE) |>
  filter(nchar(geography) == 2, geography != "00") |>
  mutate(year = substr(as.character(time), 1, 4)) |>
  filter(as.integer(year) >= 2005) |>
  select(geography, year, nhtsa_fatality_rate)

wisqars_all_years <- vroom("../wisqars/standard/data.csv.gz", show_col_types = FALSE) |>
  filter(
    nchar(geography) == 2, geography != "00",
    age == "Total", sex == "All", race == "All", ethnicity == "All"
  ) |>
  mutate(year = substr(as.character(time), 1, 4)) |>
  filter(as.integer(year) >= 2005) |>
  select(geography, year, wisqars_rate_motor_vehicle_traffic)

overlap_years <- sort(intersect(unique(nhtsa_all_years$year), unique(wisqars_all_years$year)))
cat("Overlapping years:", paste(overlap_years, collapse = ", "), "\n")

scatter_data <- nhtsa_all_years |>
  filter(year %in% overlap_years) |>
  inner_join(
    wisqars_all_years |> filter(year %in% overlap_years),
    by = c("geography", "year")
  ) |>
  left_join(state_abbrevs, by = "geography") |>
  mutate(year = factor(year, levels = overlap_years))

rate_max <- max(
  max(scatter_data$nhtsa_fatality_rate, na.rm = TRUE),
  max(scatter_data$wisqars_rate_motor_vehicle_traffic, na.rm = TRUE)
) * 1.05

p_scatter <- ggplot(scatter_data,
                    aes(x = nhtsa_fatality_rate,
                        y = wisqars_rate_motor_vehicle_traffic)) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "grey55") +
  geom_point(size = 2.2, alpha = 0.75, color = "#2166ac") +
  geom_text(aes(label = state), size = 2.2, vjust = -0.6, check_overlap = TRUE) +
  facet_wrap(~year, ncol = ceiling(sqrt(length(overlap_years)))) +
  coord_fixed(xlim = c(0, rate_max), ylim = c(0, rate_max)) +
  labs(
    title    = "NHTSA FARS vs. WISQARS Motor Vehicle Traffic Fatality Rate by State",
    subtitle = paste0("Each point = one state. Dashed line = 1:1 reference. Years: ",
                      min(overlap_years), "–", max(overlap_years)),
    x        = "NHTSA crude rate (fatalities per 100k)",
    y        = "WISQARS age-adjusted rate (fatalities per 100k)",
    caption  = "Sources: NHTSA Fatality Analysis Reporting System; CDC WISQARS Fatal Injury Reports."
  ) +
  theme_bw(base_size = 11) +
  theme(
    plot.title    = element_text(face = "bold", hjust = 0.5, size = 13),
    plot.subtitle = element_text(hjust = 0.5, color = "grey40", size = 9),
    strip.text    = element_text(face = "bold", size = 11),
    plot.caption  = element_text(color = "grey50", size = 7)
  )
p_scatter
out_path_scatter <- "standard/nhtsa_vs_wisqars_scatter.png"
n_cols  <- ceiling(sqrt(length(overlap_years)))
n_rows  <- ceiling(length(overlap_years) / n_cols)
ggsave(out_path_scatter, p_scatter,
       width  = n_cols * 4,
       height = n_rows * 4 + 1.5,
       dpi    = 150, bg = "white")
cat("Saved:", out_path_scatter, "\n")
