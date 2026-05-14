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
p

p_log <- p + scale_y_log10(labels = scales::comma) +
  labs(subtitle = paste(p$labels$subtitle, "(log y-axis)"))
p_log
