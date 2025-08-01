
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
  
  data <- vroom::vroom("./raw/ijqb-a7ye.csv.xz", show_col_types = FALSE) %>%
    #filter(!grepl('Exemption',dose)) %>%
    rename(vaccine = "Vaccine/Exemption") %>%
    mutate(
      vaccine = tolower(vaccine),
      vax = if_else(
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
    mutate(geography = cdlTools::fips(statename, to='FIPS'),
           time = paste(substr(year,1,4),'09','01', sep='-') #set date to start of academic year (Sept 1,YYYY)
           ) %>%
    dplyr::select(time, geography, grade, N, vax, value, percent_surveyed, survey_type)
  
  
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
      data,
      "standard/data.csv.gz",
      ","
    )
  
  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}