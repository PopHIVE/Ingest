# =============================================================================
# Heat Risk County Choropleth Maps – 3 Summer Dates
# Run from: data/heat_risk/
# =============================================================================

library(dplyr)
library(vroom)
library(sf)
library(ggplot2)

# Three summer dates from different years in the archive
selected_dates <- c("2024-08-15", "2025-07-04", "2025-08-15")

# Load county heat risk data (archive rows only)
cat("Loading county data...\n")
county_data <- vroom("standard/data_county.csv.gz", show_col_types = FALSE) %>%
  filter(forecast_day == 0L,
         time %in% selected_dates) %>%
  mutate(time = as.character(time))

# Load cached county geometries (created by ingest.R)
cat("Loading county geometries...\n")
counties_sf <- readRDS("raw/counties.rds") %>%
  mutate(geography = as.character(geography))

# Join geometry with heat risk values
map_data <- counties_sf %>%
  left_join(county_data, by = "geography") %>%
  filter(!is.na(time))

# Ordered facet labels
map_data <- map_data %>%
  mutate(
    date_label = factor(
      time,
      levels = selected_dates,
      labels = c("August 15, 2024", "July 4, 2025", "August 15, 2025")
    ),
    value = pmin(pmax(value, 0), 4)   # clamp to 0-4 scale
  )

# NOAA HeatRisk color ramp: grey → yellow → orange → red → dark red
heat_colors <- c("#d3d3d3", "#ffff00", "#ffa500", "#ff0000", "#8b0000")

cat("Rendering maps...\n")
p <- ggplot(map_data) +
  geom_sf(aes(fill = value), color = NA) +
  scale_fill_gradientn(
    colours = heat_colors,
    values  = scales::rescale(c(0, 1, 2, 3, 4), to = c(0, 1)),
    limits  = c(0, 4),
    breaks  = 0:4,
    labels  = c("0 – No Risk", "1 – Minor", "2 – Moderate", "3 – Major", "4 – Extreme"),
    name    = "Mean Heat\nRisk Score",
    guide   = guide_colorbar(
      barwidth  = unit(0.5, "cm"),
      barheight = unit(6,   "cm"),
      ticks     = TRUE
    )
  ) +
  facet_wrap(~date_label, ncol = 1) +
  coord_sf(crs = 5070, datum = NA) +   # Albers Equal Area CONUS
  labs(
    title    = "NOAA HeatRisk – County Mean Heat Risk Score",
    subtitle = "Three summer snapshots (archive data, forecast_day = 0)",
    caption  = "Source: NOAA Weather Prediction Center HeatRisk\nhttps://www.wpc.ncep.noaa.gov/heatrisk/"
  ) +
  theme_void(base_size = 11) +
  theme(
    plot.title    = element_text(face = "bold", hjust = 0.5, size = 13),
    plot.subtitle = element_text(hjust = 0.5, color = "grey40", size = 9),
    plot.caption  = element_text(hjust = 0.5, color = "grey50", size = 7),
    strip.text    = element_text(face = "bold", size = 10),
    legend.position = "right",
    plot.margin   = margin(10, 10, 10, 10)
  )

out_path <- "standard/heat_risk_choropleth.png"
ggsave(out_path, p, width = 9, height = 14, dpi = 150, bg = "white")
cat("Saved:", out_path, "\n")
