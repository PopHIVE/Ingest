# =============================================================================
# Area Health Resource File (AHRF) 2024-2025 Data Ingestion
# Source: HRSA Area Health Resource File
# URL: https://data.hrsa.gov/topics/health-workforce/ahrf
# =============================================================================

library(dplyr)
library(tidyr)
library(vroom)

# Initialize process record (creates process.json if it doesn't exist)
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

zip_path <- "raw/AHRF_2024-2025_CSV.zip"

if (!file.exists(zip_path)) {
  stop(
    "AHRF zip not found at ", zip_path, ". ",
    "Download AHRF 2024-2025 CSV from https://data.hrsa.gov/topics/health-workforce/ahrf"
  )
}

current_hash <- list(hash = unname(tools::md5sum(zip_path)))

if (!identical(process$raw_state, current_hash)) {

  tmp_dir <- tempfile()
  dir.create(tmp_dir)
  on.exit(unlink(tmp_dir, recursive = TRUE))

  unzip(zip_path, exdir = tmp_dir)
  ahrf_dir <- file.path(tmp_dir, "NCHWA-2024-2025+AHRF+COUNTY+CSV")

  # Pivot columns named {measure}_{2digit_year} to long (fips, year, measure cols)
  pivot_years <- function(df, id_col = "fips_st_cnty") {
    df %>%
      pivot_longer(
        -all_of(id_col),
        names_to  = c(".value", "yr"),
        names_pattern = "(.+)_(\\d{2})$"
      ) %>%
      mutate(year = as.integer(yr) + 2000) %>%
      select(-yr)
  }

  # ---------------------------------------------------------------------------
  # 1. Geographic designations: HPSA shortage areas and rural-urban code
  # ---------------------------------------------------------------------------
  geo_raw <- vroom(
    file.path(ahrf_dir, "AHRF2025geo.csv"),
    col_select = c(
      fips_st_cnty,
      hpsa_prim_care_24, hpsa_prim_care_25,
      hpsa_dent_24,      hpsa_dent_25,
      hpsa_mentl_hlth_24, hpsa_mentl_hlth_25,
      rural_urban_contnm_23
    ),
    show_col_types = FALSE
  )

  geo_hpsa <- geo_raw %>%
    select(-rural_urban_contnm_23) %>%
    rename(
      ahrf_hpsa_prim_care_24     = hpsa_prim_care_24,
      ahrf_hpsa_prim_care_25     = hpsa_prim_care_25,
      ahrf_hpsa_dental_24        = hpsa_dent_24,
      ahrf_hpsa_dental_25        = hpsa_dent_25,
      ahrf_hpsa_mental_health_24 = hpsa_mentl_hlth_24,
      ahrf_hpsa_mental_health_25 = hpsa_mentl_hlth_25
    ) %>%
    pivot_years()

  # Rural-urban continuum code is static (2023 classification); join to all rows
  geo_static <- geo_raw %>%
    select(fips_st_cnty, ahrf_rural_urban_code = rural_urban_contnm_23)

  # ---------------------------------------------------------------------------
  # 2. Health professions: provider counts and primary care rates
  # ---------------------------------------------------------------------------
  hp_raw <- vroom(
    file.path(ahrf_dir, "AHRF2025hp.csv"),
    col_select = c(
      fips_st_cnty,
      phys_nf_prim_care_pc_exc_rsdt_22,
      phys_nf_prim_care_pc_exc_rsdt_23,
      md_nf_all_pc_22, md_nf_all_pc_23,
      md_nf_psych_22,  md_nf_psych_23,
      dent_npi_23,     dent_npi_24
    ),
    show_col_types = FALSE
  )

  hp_long <- hp_raw %>%
    rename(
      ahrf_pcp_per_100k_22  = phys_nf_prim_care_pc_exc_rsdt_22,
      ahrf_pcp_per_100k_23  = phys_nf_prim_care_pc_exc_rsdt_23,
      ahrf_md_per_100k_22   = md_nf_all_pc_22,
      ahrf_md_per_100k_23   = md_nf_all_pc_23,
      ahrf_psychiatrists_22 = md_nf_psych_22,
      ahrf_psychiatrists_23 = md_nf_psych_23,
      ahrf_dentists_23      = dent_npi_23,
      ahrf_dentists_24      = dent_npi_24
    ) %>%
    pivot_years()

  # ---------------------------------------------------------------------------
  # 3. Health facilities: hospitals and critical access hospitals
  # ---------------------------------------------------------------------------
  hf_raw <- vroom(
    file.path(ahrf_dir, "AHRF2025hf.csv"),
    col_select = c(
      fips_st_cnty,
      hosp_22, hosp_23,
      critcl_access_hosp_22, critcl_access_hosp_23
    ),
    show_col_types = FALSE
  )

  hf_long <- hf_raw %>%
    rename(
      ahrf_hospitals_22            = hosp_22,
      ahrf_hospitals_23            = hosp_23,
      ahrf_critical_access_hosp_22 = critcl_access_hosp_22,
      ahrf_critical_access_hosp_23 = critcl_access_hosp_23
    ) %>%
    pivot_years()

  # ---------------------------------------------------------------------------
  # 4. Population
  # ---------------------------------------------------------------------------
  pop_raw <- vroom(
    file.path(ahrf_dir, "AHRF2025pop.csv"),
    col_select = c(fips_st_cnty, popn_22, popn_23),
    show_col_types = FALSE
  )

  pop_long <- pop_raw %>%
    rename(
      ahrf_population_22 = popn_22,
      ahrf_population_23 = popn_23
    ) %>%
    pivot_years()

  # ---------------------------------------------------------------------------
  # 5. Environment: air quality (time-varying) and PM2.5 / density (static)
  # ---------------------------------------------------------------------------
  env_raw <- vroom(
    file.path(ahrf_dir, "AHRF2025env.csv"),
    col_select = c(
      fips_st_cnty,
      good_air_qulty_dys_pct_23,
      good_air_qulty_dys_pct_24,
      annul_partclt_mattr_2_5_avg_20,
      popn_densty_per_squr_mi_20
    ),
    show_col_types = FALSE
  )

  env_air <- env_raw %>%
    select(
      fips_st_cnty,
      ahrf_good_air_pct_23 = good_air_qulty_dys_pct_23,
      ahrf_good_air_pct_24 = good_air_qulty_dys_pct_24
    ) %>%
    pivot_years()

  # PM2.5 and population density are from the 2020 decennial census / AQS
  # and do not vary annually in this dataset; join as static county attributes
  env_static <- env_raw %>%
    select(
      fips_st_cnty,
      ahrf_pm25        = annul_partclt_mattr_2_5_avg_20,
      ahrf_pop_density = popn_densty_per_squr_mi_20
    )

  # ---------------------------------------------------------------------------
  # 6. Medicare FFS expenditure
  # ---------------------------------------------------------------------------
  exp_raw <- vroom(
    file.path(ahrf_dir, "AHRF2025exp.csv"),
    col_select = c(
      fips_st_cnty,
      actl_per_cap_ffs_cost_22,
      actl_per_cap_ffs_cost_23
    ),
    show_col_types = FALSE
  )

  exp_long <- exp_raw %>%
    rename(
      ahrf_medicare_per_capita_22 = actl_per_cap_ffs_cost_22,
      ahrf_medicare_per_capita_23 = actl_per_cap_ffs_cost_23
    ) %>%
    pivot_years()

  # ---------------------------------------------------------------------------
  # 7. Utilization: ED visits per 1k Medicare FFS beneficiaries
  # ---------------------------------------------------------------------------
  utl_raw <- vroom(
    file.path(ahrf_dir, "AHRF2025utl.csv"),
    col_select = c(
      fips_st_cnty,
      ed_vists_per_1k_medcr_ffs_22,
      ed_vists_per_1k_medcr_ffs_23
    ),
    show_col_types = FALSE
  )

  utl_long <- utl_raw %>%
    rename(
      ahrf_ed_per_1k_medicare_22 = ed_vists_per_1k_medcr_ffs_22,
      ahrf_ed_per_1k_medicare_23 = ed_vists_per_1k_medcr_ffs_23
    ) %>%
    pivot_years()

  # ---------------------------------------------------------------------------
  # 8. Join all measures and format standard output
  # ---------------------------------------------------------------------------
  data_standard <- geo_hpsa %>%
    full_join(hp_long,  by = c("fips_st_cnty", "year")) %>%
    full_join(hf_long,  by = c("fips_st_cnty", "year")) %>%
    full_join(pop_long, by = c("fips_st_cnty", "year")) %>%
    full_join(env_air,  by = c("fips_st_cnty", "year")) %>%
    full_join(exp_long, by = c("fips_st_cnty", "year")) %>%
    full_join(utl_long, by = c("fips_st_cnty", "year")) %>%
    left_join(geo_static, by = "fips_st_cnty") %>%
    left_join(env_static,  by = "fips_st_cnty") %>%
    mutate(
      geography = fips_st_cnty,
      time      = paste0(year, "-12-31")
    ) %>%
    filter(nchar(geography) == 5) %>%
    select(geography, time, starts_with("ahrf_")) %>%
    arrange(geography, time)

  vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")

  process$raw_state <- current_hash
  dcf::dcf_process_record(updated = process)
}