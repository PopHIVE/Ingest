# Visual check of full_series population by state and age group
# Run from: data/abcs/

library(dplyr)
library(ggplot2)
library(vroom)

full_series <- vroom::vroom('raw/abcs_census_age_stratified_pop_full.csv')

cat("Rows:", nrow(full_series),
    " | States:", paste(sort(unique(full_series$state)), collapse = ", "),
    " | Years:", min(full_series$year), "-", max(full_series$year), "\n")

age_levels <- c("0-4", "5-17", "18-49", "50-64", "65+", "Total")
full_series <- full_series %>%
  mutate(age = factor(age, levels = intersect(age_levels, unique(age))))

p <- ggplot(full_series, aes(x = year, y = pop, color = age)) +
  geom_line() +
  geom_point(size = 0.6) +
  geom_vline(xintercept = 2008.5, linetype = "dashed", color = "grey50") +
  facet_wrap(~ state, scales = "free_y", ncol = 3) +
  scale_y_continuous(labels = scales::comma) +
  labs(title    = "ABCs surveillance-area population, 1998-2024",
       subtitle = "Dashed line = boundary between pre-2009 intercensal/Census 2000 (left) and ACS (right)",
       x = "Year", y = "Population", color = "Age group") +
  theme_bw(base_size = 11) +
  theme(legend.position = "bottom")
ggsave("raw/abcs_pop_full_series.png", p, width = 14, height = 10)

p_log <- p + scale_y_log10(labels = scales::comma) +
  labs(subtitle = paste(p$labels$subtitle, "(log y-axis)"))
ggsave("raw/abcs_pop_full_series_log.png", p_log, width = 14, height = 10)

# =============================================================================
# Serotype 19F incidence by year, faceted by state, for each age group
# =============================================================================
std <- vroom::vroom("standard/data.csv.gz", show_col_types = FALSE)

all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

state_abbr <- all_fips %>%
  filter(nchar(geography) == 2, geography != "00") %>%
  select(geography, state) %>%
  bind_rows(tibble(geography = "00", state = "All Sites"))

age_order <- c("<5 years", "5-49 years", "50+ years")

ipd_19F <- std %>%
  filter(serotype == "19F", age != "Total") %>%
  left_join(state_abbr, by = "geography") %>%
  mutate(
    year  = as.integer(format(as.Date(time), "%Y")),
    state = factor(state, levels = c("All Sites", sort(setdiff(unique(state[!is.na(state)]), "All Sites")))),
    age   = factor(age, levels = age_order)
  ) %>%
  filter(!is.na(state))

for (ag in age_order) {
  dat <- filter(ipd_19F, age == ag)
  if (nrow(dat) == 0) next
  n_states   <- nlevels(dat$state)
  all_sites  <- which(levels(dat$state) == "All Sites")
  pal        <- rep("#2C7BB6", n_states)
  pal[all_sites] <- "#D7301F"

  p_ag <- ggplot(dat, aes(x = year, y = rate_IPD, color = state)) +
    geom_line(linewidth = 1) +
    geom_point(size = 2) +
    facet_wrap(~ state, scales = "free_y", ncol = 4) +
    scale_x_continuous(breaks = seq(2000, 2025, 5)) +
    scale_color_manual(values = pal) +
    labs(
      title   = paste0("IPD rate of serotype 19F by year, ", ag),
      x       = "Year",
      y       = "Rate per 100,000",
      caption = "Source: CDC Active Bacterial Core surveillance (ABCs)"
    ) +
    theme_bw(base_size = 14) +
    theme(
      legend.position     = "none",
      plot.title          = element_text(face = "bold", size = 16,
                                         margin = margin(b = 12)),
      strip.text          = element_text(face = "bold", size = 13,
                                         color = "grey20"),
      strip.background    = element_rect(fill = "white", color = NA),
      panel.border        = element_blank(),
      panel.grid.minor    = element_blank(),
      panel.grid.major.x  = element_blank(),
      panel.grid.major.y  = element_line(color = "grey88"),
      panel.spacing       = unit(1.2, "lines"),
      axis.line           = element_line(color = "grey60"),
      axis.text           = element_text(size = 11, color = "grey30"),
      axis.title          = element_text(size = 13),
      plot.caption        = element_text(color = "grey50", size = 9,
                                         hjust = 0, margin = margin(t = 8))
    )
  fname <- paste0("raw/abcs_19F_ipd_", gsub("[^a-zA-Z0-9]", "_", ag), ".png")
  ggsave(fname, p_ag, width = 16, height = 7, dpi = 150)
}
