
#
# Download
#

process <- dcf::dcf_process_record()
raw_state <- dcf::dcf_download_cdc(
  "ijqb-a7ye",
  "raw",
  process$raw_state
)

if (!identical(process$raw_state, raw_state)) {

  # Load FIPS crosswalk (preferred over cdlTools::fips())
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, geography_name)

  data <- vroom::vroom("./raw/ijqb-a7ye.csv.xz", show_col_types = FALSE) %>%
    #filter(!grepl('Exemption',dose)) %>%
    rename(vaccine = "Vaccine/Exemption") %>%
    mutate(
      vaccine = tolower(vaccine),
      vax = if_else(is.na(Dose), vaccine,
        if_else(
        Dose == 'Any Exemption',
        'full_exempt',
        if_else(
          Dose == 'Medical Exemption',
          'medical_exempt',
          if_else(
            Dose == 'Non-Medical Exemption',
            'personal_exempt',
            vaccine
          )
        )
      )
      ),
      vax = if_else(
        vaccine == "dtp, dtap, or dt",
        'dtap',
        if_else(vaccine == "hepatitis b", 'hep_b', vax)
      ),
      grade = 'Kindergarten'
    ) %>%
    rename(
      year = 'School Year',
      N = "Population Size",
      value = "Estimate (%)",
      percent_surveyed =  "Percent Surveyed",
      survey_type = 'Survey Type',
      statename = Geography
    ) %>%
    filter(statename %in% c(state.name, 'District of Columbia', 'United States')) %>%
    # Use FIPS lookup instead of cdlTools::fips()
    left_join(state_fips_lookup, by = c("statename" = "geography_name")) %>%
    mutate(geography = if_else(statename == 'United States', "00", geography),
           time = paste(substr(year,1,4),'09','01', sep='-'), #set date to start of academic year (Sept 1,YYYY)
           vax = if_else(
             grepl('1 dose', Dose), NA_character_, vax  #removes the 1 dose varicella category
           )
           ) %>%
    filter(vax != '') %>%
    dplyr::select(time, geography, grade, N, vax, value, percent_surveyed, survey_type) %>%
    distinct() 
    
  
  
  exemptions <- data %>%
    filter(grepl('exempt', tolower(vax)))
  
  vroom::vroom_write(
    exemptions,
    "standard/data_exemptions.csv.gz",
    ","
  )
  
  
  vax2 <- data %>%
    filter(!grepl('exempt', tolower(vax))) %>%
    filter(!grepl('pac', vax))
  

    vroom::vroom_write(
      vax2,
      "standard/data.csv.gz",
      ","
    )
  
  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}