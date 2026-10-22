library(tidyverse)
library(cdlTools)
library(dcf)
#
# Download and add files to the raw directory
#

process <- dcf::dcf_process_record()
raw_state <- dcf::dcf_download_cdc(
  "rdmq-nq56",
  "raw",
  process$raw_state
)


#####################
# Reformat STATE Level
######################


if (!identical(process$raw_state, raw_state)) {
  data_state <- vroom::vroom("./raw/rdmq-nq56.csv.xz", show_col_types = FALSE) %>%
    filter(county=='All'  ) %>%
    rename(state=geography, date='week_end') %>%
    dplyr::select(fips, date, percent_visits_rsv, percent_visits_influenza, percent_visits_covid) %>%
    collect() %>%
    rename(time = date, 
           geography = fips ,
           percent_visits_flu = percent_visits_influenza
           )%>%
    mutate(geography = geography,
           geography = sprintf("%05d", geography),
           geography = substr(geography,1,2)) %>%
    dplyr::select(geography,time, percent_visits_rsv, percent_visits_flu, percent_visits_covid) %>%
    mutate(
      percent_visits_rsv  = if_else(geography=='56' & time >= '2024-07-01', NA_real_,percent_visits_rsv ), 
      percent_visits_flu  = if_else(geography=='56' & time >= '2024-07-01', NA_real_,percent_visits_flu ) ,
      percent_visits_covid  = if_else(geography=='56' & time >= '2024-07-01', NA_real_,percent_visits_covid ) 
                 ) #Wyoming stops reporting in July 2024

  #####################
  # Reformat COUNTY Level
  ######################
    #for states without county info fill in with state-level from data_state_merge
    data_state_merge <- data_state %>%
      rename(
        percent_visits_rsv_state = percent_visits_rsv,
        percent_visits_covid_state = percent_visits_covid,
        percent_visits_flu_state = percent_visits_flu  
      ) %>%
      dplyr::select(
        geography,
        time,
        percent_visits_rsv_state,
        percent_visits_covid_state,
        percent_visits_flu_state
      ) %>%
      rename(state_fips = geography,
             week_end = time)
    
    data_county <- vroom::vroom("./raw/rdmq-nq56.csv.xz", show_col_types = FALSE) %>%
      filter(county != 'All') %>%
      rename(state = geography) %>%
      mutate(fips = sprintf("%05d", fips),
             state_fips = substr(fips,1,2)
             ) %>%
      dplyr::select(
        state,
        county,
        fips,
        state_fips,
        week_end,
        percent_visits_rsv,
        percent_visits_covid,
        percent_visits_influenza
      ) %>%
     left_join(data_state_merge, by = c('week_end', 'state_fips')) %>%
      rename(fips_code = fips) %>%
      mutate(
        is_state_estimate = if_else(
          is.na(percent_visits_covid) | is.na(percent_visits_influenza) | is.na(percent_visits_rsv),
          1L, 0L
        ),
        percent_visits_covid = if_else(
          is.na(percent_visits_covid),
          percent_visits_covid_state,
          percent_visits_covid
        ),
        percent_visits_flu = if_else(
          is.na(percent_visits_influenza),
          percent_visits_flu_state,
          percent_visits_influenza
        ),
        percent_visits_rsv = if_else(
          is.na(percent_visits_rsv),
          percent_visits_rsv_state,
          percent_visits_rsv
        ),
        
        #fix CT county coding
        fips_code = if_else(
          state == 'Connecticut' & county == 'Fairfield',
          '09001',
          if_else(
            state == 'Connecticut' & county == 'Hartford',
            '09003',
            if_else(
              state == 'Connecticut' & county == 'Litchfield',
              '09005',
              if_else(
                state == 'Connecticut' & county == 'Middlesex',
                '09007',
                if_else(
                  state == 'Connecticut' & county == 'New Haven',
                  '09009',
                  if_else(
                    state == 'Connecticut' & county == 'New London',
                    '09011',
                    if_else(
                      state == 'Connecticut' & county == 'Tolland',
                      '09013',
                      if_else(
                        state == 'Connecticut' & county == 'Windham',
                        '09015',
                        fips_code
                      )
                    )
                  )
                )
              )
            )
          )
        )
      ) %>%
      mutate(
        percent_visits_rsv  = if_else(state_fips=='56' & week_end >= '2024-07-01', NA_real_,percent_visits_rsv ), 
        percent_visits_flu  = if_else(state_fips=='56' & week_end >= '2024-07-01', NA_real_,percent_visits_flu) ,
        percent_visits_covid  = if_else(state_fips=='56' & week_end >= '2024-07-01', NA_real_,percent_visits_covid ) 
      ) %>% #Wyoming stops reporting in July 2024
      as.data.frame() %>%
      rename(geography = fips_code) %>%
      rename(time=week_end) %>%
      dplyr::select(
        geography,
        time,
        percent_visits_covid,
        percent_visits_flu,
        percent_visits_rsv,
        is_state_estimate
      )


  data <- bind_rows(data_state, data_county) %>%
    mutate(geography = if_else(geography == "46113", "46102", geography))

  vroom::vroom_write(
    data,
    "standard/data.csv.gz",
    ","
  )
  
  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}



#####################
# NSSP Commercial Laboratory Data (state level)
# https://data.cdc.gov/d/jqxy-cyu2
# Output: standard/data_lab.csv.gz (variables prefixed with lab_)
######################

lab_state <- dcf::dcf_download_cdc(
  "jqxy-cyu2",
  "raw",
  process$lab_state
)

if (!identical(process$lab_state, lab_state)) {
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, state)

  lab_raw <- vroom::vroom("./raw/jqxy-cyu2.csv.xz", show_col_types = FALSE)

  lab_state_long <- lab_raw %>%
    filter(geography_type == "state") %>%
    mutate(
      pathogen = c("COVID-19" = "covid", "Influenza" = "flu", "RSV" = "rsv")[pathogen],
      time = format(as.Date(mmwr_week_end), "%Y-%m-%d")
    ) %>%
    left_join(state_fips_lookup, by = c("geography_value" = "state")) %>%
    filter(!is.na(geography), !is.na(pathogen)) %>%
    # suppressed when positives < 10 (denominator > 0 but positives missing)
    mutate(suppressed_flag = if_else(test_count > 0 & is.na(positive_count), 1L, 0L)) %>%
    # impute suppressed positive counts as 5 (midpoint of 0-9) and recompute percent positivity
    mutate(
      positive_count = if_else(suppressed_flag == 1L, 5, positive_count),
      percent_pos = if_else(suppressed_flag == 1L, 100 * 5 / test_count, percent_pos)
    ) %>%
    select(geography, time, pathogen, test_count, positive_count, percent_pos, suppressed_flag)

  # National totals: sum across states at each time point. Suppressed states
  # contribute their imputed positives (5), so the national positive count is
  # approximate (suppressed_flag is NA for national since some state is nearly
  # always suppressed) and percent_pos is total positives / total tests.
  lab_national <- lab_state_long %>%
    group_by(time, pathogen) %>%
    summarize(
      positive_count = sum(positive_count, na.rm = TRUE),
      test_count = sum(test_count, na.rm = TRUE),
      suppressed_flag = NA_integer_,
      .groups = "drop"
    ) %>%
    mutate(
      percent_pos = if_else(test_count > 0, 100 * positive_count / test_count, NA_real_),
      geography = "00"
    ) %>%
    select(geography, time, pathogen, test_count, positive_count, percent_pos, suppressed_flag)

  data_lab <- bind_rows(lab_state_long, lab_national) %>%
    tidyr::pivot_wider(
      names_from = pathogen,
      values_from = c(test_count, positive_count, percent_pos, suppressed_flag),
      names_glue = "lab_{.value}_{pathogen}"
    ) %>%
    arrange(geography, time)

  vroom::vroom_write(data_lab, "standard/data_lab.csv.gz", ",")

  process$lab_state <- lab_state
  dcf::dcf_process_record(updated = process)
}
