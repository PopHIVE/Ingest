# =============================================================================
# Area Health Resource File (AHRF) 2024-2025 Data Ingestion
# Source: HRSA Area Health Resource File
# URL: https://data.hrsa.gov/topics/health-workforce/ahrf
# =============================================================================

library(dplyr)
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

  # ---------------------------------------------------------------------------
  # 1. Geographic designations: HPSA shortage areas and rural-urban code
  #    Most recent: HPSA 2025, rural-urban 2023
  # ---------------------------------------------------------------------------
  geo <- vroom(
    file.path(ahrf_dir, "AHRF2025geo.csv"),
    col_select = c(
      fips_st_cnty,
      hpsa_prim_care_25,
      hpsa_dent_25,
      hpsa_mentl_hlth_25,
      rural_urban_contnm_23
    ),
    show_col_types = FALSE
  ) %>%
    rename(
      ahrf_hpsa_prim_care     = hpsa_prim_care_25,
      ahrf_hpsa_dental        = hpsa_dent_25,
      ahrf_hpsa_mental_health = hpsa_mentl_hlth_25,
      ahrf_rural_urban_code   = rural_urban_contnm_23
    )

  # ---------------------------------------------------------------------------
  # 2. Health professions: provider counts and rates
  #    Most recent: physicians/psychiatrists 2023, dentists 2024
  # ---------------------------------------------------------------------------
  hp <- vroom(
    file.path(ahrf_dir, "AHRF2025hp.csv"),
    col_select = c(
      fips_st_cnty,
      phys_nf_prim_care_pc_exc_rsdt_23,
      md_nf_all_pc_23,
      md_nf_psych_23,
      dent_npi_24
    ),
    show_col_types = FALSE
  ) %>%
    rename(
      ahrf_pcp_per_100k  = phys_nf_prim_care_pc_exc_rsdt_23,
      ahrf_md_per_100k   = md_nf_all_pc_23,
      ahrf_psychiatrists = md_nf_psych_23,
      ahrf_dentists      = dent_npi_24
    )

  # ---------------------------------------------------------------------------
  # 3. Health facilities: hospitals and critical access hospitals
  #    Most recent: 2023
  # ---------------------------------------------------------------------------
  hf <- vroom(
    file.path(ahrf_dir, "AHRF2025hf.csv"),
    col_select = c(
      fips_st_cnty,
      hosp_23,
      critcl_access_hosp_23
    ),
    show_col_types = FALSE
  ) %>%
    rename(
      ahrf_hospitals            = hosp_23,
      ahrf_critical_access_hosp = critcl_access_hosp_23
    )

  # ---------------------------------------------------------------------------
  # 4. Population
  #    Most recent: 2023
  # ---------------------------------------------------------------------------
  pop <- vroom(
    file.path(ahrf_dir, "AHRF2025pop.csv"),
    col_select = c(fips_st_cnty, popn_23),
    show_col_types = FALSE
  ) %>%
    rename(ahrf_population = popn_23)

  # ---------------------------------------------------------------------------
  # 5. Environment: air quality (2024), PM2.5 and density (2020 Census/AQS)
  # ---------------------------------------------------------------------------
  env <- vroom(
    file.path(ahrf_dir, "AHRF2025env.csv"),
    col_select = c(
      fips_st_cnty,
      good_air_qulty_dys_pct_24,
      annul_partclt_mattr_2_5_avg_20,
      popn_densty_per_squr_mi_20
    ),
    show_col_types = FALSE
  ) %>%
    rename(
      ahrf_good_air_pct = good_air_qulty_dys_pct_24,
      ahrf_pm25         = annul_partclt_mattr_2_5_avg_20,
      ahrf_pop_density  = popn_densty_per_squr_mi_20
    )

  # ---------------------------------------------------------------------------
  # 6. Medicare FFS expenditure
  #    Most recent: 2023
  # ---------------------------------------------------------------------------
  exp <- vroom(
    file.path(ahrf_dir, "AHRF2025exp.csv"),
    col_select = c(fips_st_cnty, actl_per_cap_ffs_cost_23),
    show_col_types = FALSE
  ) %>%
    rename(ahrf_medicare_per_capita = actl_per_cap_ffs_cost_23)

  # ---------------------------------------------------------------------------
  # 7. Utilization: ED visits per 1k Medicare FFS beneficiaries
  #    Most recent: 2023
  # ---------------------------------------------------------------------------
  utl <- vroom(
    file.path(ahrf_dir, "AHRF2025utl.csv"),
    col_select = c(fips_st_cnty, ed_vists_per_1k_medcr_ffs_23),
    show_col_types = FALSE
  ) %>%
    rename(ahrf_ed_per_1k_medicare = ed_vists_per_1k_medcr_ffs_23)

  # ---------------------------------------------------------------------------
  # 8. Join all measures and format standard output
  #    One row per county; time = release year of this AHRF edition
  # ---------------------------------------------------------------------------
  data_standard <- geo %>%
    left_join(hp,  by = "fips_st_cnty") %>%
    left_join(hf,  by = "fips_st_cnty") %>%
    left_join(pop, by = "fips_st_cnty") %>%
    left_join(env, by = "fips_st_cnty") %>%
    left_join(exp, by = "fips_st_cnty") %>%
    left_join(utl, by = "fips_st_cnty") %>%
    mutate(
      geography = fips_st_cnty,
      time      = "2025-12-31"
    ) %>%
    select(geography, time, starts_with("ahrf_")) %>%
    arrange(geography)

  vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")

  process$raw_state <- current_hash
  dcf::dcf_process_record(updated = process)
}