library(dcf)
library(tidyverse)
library(tidyr)
# cdlTools was attached here but never called - geography is resolved by
# merging resources/all_fips.csv.gz, which CLAUDE.md prefers anyway. Dropping
# the unused dependency so this ingest runs without it installed.
#
# Download
#
all_fips <- vroom::vroom('../../resources/all_fips.csv.gz') %>%
  filter(geography_name %in% c(state.name, 'District of Columbia', 'United States')
         & geography !='11001')

process <- dcf::dcf_process_record()
raw_state <- dcf::dcf_download_cdc(
  "qvzb-qs6p",
  "raw",
  process$raw_state
)

# add files to the `raw` directory

#
# Reformat
#
if (!identical(process$raw_state, raw_state)) {
  
   data_age <-  vroom::vroom("./raw/qvzb-qs6p.csv.xz", show_col_types = FALSE) %>%
    rename(
      agec = "Age Group (years)",
      year = Year,
      st = 'IPD Serotype',
      N_IPD = 'Frequency Count',
      site = Site,
    ) %>%
    mutate(
      st = if_else(st == '16', '16F', st),
      agec1 = if_else(agec %in% c("Age <2", "Age 2-4"), 1, 2),
      agec = gsub('Age ', '', agec),
      agec2 = if_else(
        agec %in% c('<2', '2-4'),
        '<5',
        if_else(
          agec %in% c('5-17', '18-49'),
          '5-49',
          if_else(agec %in% c('50-64', '65+'), '50+', NA)
        )
      ),
      agec2 = factor(
        agec2,
        levels = c('<5', '5-49', '50+'),
        labels = c('<5 years', '5-49 years', '50+ years')
      )
    ) %>%
    group_by(site, st, agec2, year) %>%
    summarize(N_IPD = sum(N_IPD)) %>%
    ungroup() %>%
    mutate(time = as.Date(paste(year,'01','01',sep='-'))) %>%
    rename(age= agec2,
           serotype=st) %>%
    dplyr::select( site, age, serotype,  time, N_IPD)%>%
    tidyr::complete(site,serotype,age,time, fill=list(N_IPD=0)) %>%
    left_join(all_fips, by=c('site'='state')) %>%
    mutate(geography = if_else(site=='All_Sites', '00', geography)) %>%
    group_by(geography, age, time) %>%
    mutate(pct_IPD = 100* N_IPD / sum(N_IPD)) %>%
    dplyr::select( geography, age, serotype,  time, N_IPD,pct_IPD) %>%
    ungroup() %>%
     filter(geography != '00')
   
   # data_age %>%
   #   group_by(time, geography) %>%
   #   summarize(N_IPD=sum(N_IPD))
   # 
   ##Note re-calculate 'All-sites' to just be the 8 sites that consistently report from 1998 onwards
   data_age_nat <- data_age %>%
     filter(geography %in% c('06','09','13','24','27','36','41','47') ) %>%
     group_by(age, serotype, time) %>%
     summarize(N_IPD = sum(N_IPD, na.rm=T)) %>%
     ungroup() %>%
     group_by(age, time) %>%
     mutate(pct_IPD = 100* N_IPD / sum(N_IPD),
            geography = '00') %>%
     dplyr::select( geography, age, serotype,  time, N_IPD,pct_IPD)
     
   data_age2 <- bind_rows(data_age,data_age_nat) 
   
   
   data_total <- data_age2 %>%
     group_by(geography, time,serotype) %>%
     summarize(N_IPD = sum(N_IPD)) %>%
     ungroup() %>%
     group_by(geography, time) %>%
     mutate(pct_IPD = 100* N_IPD / sum(N_IPD),
            age = 'Total') %>%
     ungroup()
   
  
   data2 <- bind_rows(data_age2, data_total)

   # Load denominators and merge into data2, aligning age groups
   denoms_raw <- read.csv("raw/abcs_census_age_stratified_pop_full.csv")

   state_fips_lookup <- all_fips %>%
     filter(nchar(geography) == 2) %>%
     select(geography, state)

   # Map denoms age groups to data2's age groups and sum populations
   denoms_state <- denoms_raw %>%
     left_join(state_fips_lookup, by = "state") %>%
     filter(!is.na(geography)) %>%
     mutate(age = case_when(
       age == "0-4"               ~ "<5 years",
       age %in% c("5-17", "18-49") ~ "5-49 years",
       age %in% c("50-64", "65+") ~ "50+ years",
       age == "Total"             ~ "Total",
       TRUE                       ~ NA_character_
     )) %>%
     filter(!is.na(age)) %>%
     group_by(geography, year, age) %>%
     summarize(pop = sum(pop, na.rm = TRUE), .groups = "drop")

   # Sum across the 8 selected states for the national total
   denoms_nat <- denoms_state %>%
     filter(geography %in% c('06','09','13','24','27','36','41','47')) %>%
     group_by(year, age) %>%
     summarize(pop = sum(pop, na.rm = TRUE), .groups = "drop") %>%
     mutate(geography = '00')

   denoms_all <- bind_rows(denoms_state, denoms_nat)

   data2 <- data2 %>%
     mutate(year = as.integer(format(time, "%Y")),
            age = as.character(age)) %>%
     left_join(denoms_all, by = c("geography", "year", "age")) %>%
     mutate(rate_IPD = N_IPD / pop * 100000) %>%
     select(-year)

  vroom::vroom_write(
    data2,
    "standard/data.csv.gz",
    ","
  )
  
  uad <- read_csv(
    '../abcs/raw/ramirez_ofid_2025_ofae727.csv'
  )  %>%
    mutate(N_SSUAD = over65 + a50_64_with_indication + a50_64_no_indication,
           time=as.Date('2020-01-01'),
           geography= 'KY-TN-CT-IL') %>%
    rename(serotype=st) %>%
    dplyr::select(geography, time,serotype, N_SSUAD)

  vroom::vroom_write(
    uad,
    "standard/uad.csv.gz",
    ","
  )
  
  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}

# =============================================================================
# ABCs Group A + Group B Streptococcus
#
# CDC publishes ABCs as one dataset per pathogen. The pneumococcal serotype
# data above is qvzb-qs6p; Group A (9y49-tura) and Group B (95m5-agj4) are
# handled here, in this same source folder per the single-ingest.R convention.
# Each dataset's download state is tracked separately in process.json.
#
# The two strep datasets share a 7-column raw layout, so rates, counts and
# antibiotic resistance are merged into single files keyed by a `pathogen`
# column with organism-agnostic measure names. Syndromes are NOT merged (Group
# A reports a rate per 100,000, Group B a percent) and neither is typing
# (Group A emm types vs Group B capsular serotypes and ALPH genes).
#
# NOTE ON THE 2026 CDC RESTRUCTURE (Group A). Two topics changed shape AND
# meaning between the 2025 and 2026 releases:
#
#   emm types  Was a single topic "emm types" whose `viewby` held either the
#              emm type or "Number of isolates". Now each type is its OWN topic
#              ("emm 1", "emm 12", ..., "Other"), `viewby` holds that type's
#              ISOLATE COUNT, and `value` is a PROPORTION of all typed isolates
#              - not a percent, despite units saying "Percent". Verified:
#              1997 emm1 viewby=99, sum(viewby)=440, value=0.225, and
#              99/440 = 0.225. The old release reported 22.5 percent on 440
#              isolates, so value * 100 reproduces it exactly.
#
#   syndromes  Was a percent of cases (1997+). Is now a RATE PER 100,000
#              population (2001+), hence the rate_syndrome_* column names.
#
# More broadly, every value under units "Percent" is a PROPORTION (0-1) in the
# current release and is scaled by 100 here; a guard stops the script if that
# stops being true, rather than inflating everything a hundredfold.
# =============================================================================

raw_state_gas <- dcf::dcf_download_cdc("9y49-tura", "raw", process$raw_state_gas)
raw_state_gbs <- dcf::dcf_download_cdc("95m5-agj4", "raw", process$raw_state_gbs)

if (!identical(process$raw_state_gas, raw_state_gas) ||
    !identical(process$raw_state_gbs, raw_state_gbs)) {

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Strip non-ASCII and squeeze whitespace before ANY comparison. Age labels
  # arrive as "<U+2265>65 years old" and the adult group as both
  # "Adults, <U+2265>65 years old" and "Adults, <U+2265> 65 years old". Under
  # the C locale that dcf_process runs in, a non-ASCII literal cannot be
  # translated to the native encoding, so matching one directly silently drops
  # every affected row.
  ascii_label <- function(x) trimws(gsub(" +", " ", gsub("[^ -~]", "", x)))

  read_abcs <- function(file, pathogen) {
    d <- vroom::vroom(file, show_col_types = FALSE, altrep = FALSE) %>%
      rename(
        year = Year, value = Value, units = Units, bacteria = Bacteria,
        topic = Topic, viewby = ViewBy, viewby2 = ViewBy2
      ) %>%
      mutate(
        pathogen  = pathogen,
        topic_l   = tolower(trimws(topic)),
        viewby_a  = ascii_label(viewby),
        viewby2_a = ascii_label(viewby2),
        time      = as.Date(paste0(year, "-12-31")),
        geography = "00"
      )

    pct <- d$value[trimws(d$units) == "Percent"]
    if (any(pct > 1.5, na.rm = TRUE)) {
      stop(
        "ABCs ", pathogen, ": 'Percent' values exceed 1.5 (max ",
        max(pct, na.rm = TRUE), "), so the source now reports true percents ",
        "rather than proportions. Remove the * 100 scaling."
      )
    }
    d
  }

  # proportion -> percent, rounded to shed binary float noise
  # (0.131 * 100 = 13.100000000000001)
  as_pct <- function(x) round(x * 100, 6)

  # Emit one 0/1 flag per named group of columns that the source may not report.
  # NAs here mean "not reported by CDC", not "suppressed", so the value stays NA
  # rather than being imputed. Errors if any NA-bearing column is unflagged, so
  # a future change in what CDC reports fails loudly instead of shipping
  # undocumented NAs.
  add_not_reported_flags <- function(df, id_cols, groups) {
    meas <- setdiff(names(df), id_cols)
    for (nm in names(groups)) {
      cols <- intersect(groups[[nm]], meas)
      if (!length(cols)) next
      df[[paste0("abcs_not_reported_flag_", nm)]] <-
        as.integer(Reduce(`|`, lapply(cols, function(cc) is.na(df[[cc]]))))
    }
    covered <- unlist(groups, use.names = FALSE)
    has_na <- meas[vapply(df[meas], function(x) any(is.na(x)), logical(1))]
    missed <- setdiff(has_na, covered)
    if (length(missed)) {
      stop(
        "ABCs: these columns contain NAs but have no not-reported flag: ",
        paste(missed, collapse = ", "), ". Add them to the flag groups."
      )
    }
    df
  }

  pivot_check <- function(df, id_cols, label) {
    out <- df %>%
      group_by(across(all_of(c(id_cols, "measure")))) %>%
      # Duplicate entries exist in some years; keep the larger, which is the
      # national estimate rather than the surveillance-area count
      slice_max(value, n = 1, with_ties = FALSE) %>%
      ungroup() %>%
      tidyr::pivot_wider(names_from = measure, values_from = value) %>%
      arrange(across(all_of(id_cols)))
    if (anyDuplicated(out[id_cols])) {
      stop("ABCs ", label, ": duplicate index rows after pivot.")
    }
    out
  }

  # ---------------------------------------------------------------------------
  # Label maps (keys are ASCII-normalised source labels)
  # ---------------------------------------------------------------------------

  # Fine age bands, as ABCs reports them for case and death rates
  AGE_FINE <- c(
    "<1 year old"     = "<1",
    "1 year old"      = "1",
    "1 years old"     = "1",
    "2-4 years old"   = "2-4",
    "5-17 years old"  = "5-17",
    "18-34 years old" = "18-34",
    "35-49 years old" = "35-49",
    "50-64 years old" = "50-64",
    "65 years old"    = "65+"
  )

  RACE_MAP <- c("Black" = "Black", "White" = "White", "Other races" = "Other")

  # Group B's population groups conflate an age band with infant onset timing,
  # so they decompose into both. Collapsing them to age alone would map the two
  # infant groups onto a single "<1" and create duplicate index rows.
  GROUP_AGE <- c(
    "Overall"                      = "Total",
    "Infants, early-onset disease" = "<1",
    "Infants, late-onset disease"  = "<1",
    "Adults, 18-64 years old"      = "18-64",
    "Adults, 65 years old"         = "65+"
  )
  GROUP_ONSET <- c(
    "Overall"                      = "Overall",
    "Infants, early-onset disease" = "Early-onset",
    "Infants, late-onset disease"  = "Late-onset",
    "Adults, 18-64 years old"      = "Overall",
    "Adults, 65 years old"         = "Overall"
  )

  gas <- read_abcs("raw/9y49-tura.csv.xz", "Group A Streptococcus")
  gbs <- read_abcs("raw/95m5-agj4.csv.xz", "Group B Streptococcus")
  both <- bind_rows(gas, gbs)

  strep_ids <- c("geography", "time", "pathogen")

  # ---------------------------------------------------------------------------
  # 1. Case and death rates (merged across pathogens)
  # ---------------------------------------------------------------------------
  rate_topics <- c("case rates", "death rates")

  rate_base <- both %>%
    filter(topic_l %in% rate_topics) %>%
    mutate(
      measure = if_else(topic_l == "case rates",
                        "abcs_rate_cases", "abcs_rate_deaths"),
      age = "Total", sex = "Overall", race_ethnicity = "Overall",
      onset = "Overall"
    )

  rates_overall <- rate_base %>% filter(viewby_a == "Overall")

  rates_age <- rate_base %>%
    filter(viewby_a == "Age", viewby2_a %in% names(AGE_FINE)) %>%
    mutate(age = AGE_FINE[viewby2_a])

  if (!any(rates_age$age == "65+")) {
    stop("ABCs: no 65+ age rows matched - check the label normalisation.")
  }

  rates_sex <- rate_base %>%
    filter(viewby_a == "Sex", viewby2_a %in% c("Male", "Female")) %>%
    mutate(sex = viewby2_a)

  rates_race <- rate_base %>%
    filter(viewby_a == "Race", viewby2_a %in% names(RACE_MAP)) %>%
    mutate(race_ethnicity = RACE_MAP[viewby2_a])

  # Group B only: infant early- vs late-onset rates, and the same by race
  rates_onset <- rate_base %>%
    filter(viewby_a == "Infants, early and late-onset",
           viewby2_a %in% c("Early-onset", "Late-onset")) %>%
    mutate(age = "<1", onset = viewby2_a)

  ONSET_BY_RACE <- c("Early-onset, by race" = "Early-onset",
                     "Late-onset, by race"  = "Late-onset")
  rates_onset_race <- rate_base %>%
    filter(viewby_a %in% names(ONSET_BY_RACE),
           viewby2_a %in% names(RACE_MAP)) %>%
    mutate(age = "<1", onset = ONSET_BY_RACE[viewby_a],
           race_ethnicity = RACE_MAP[viewby2_a])

  rate_ids <- c(strep_ids, "age", "sex", "race_ethnicity", "onset")

  strep_rates <- bind_rows(
    rates_overall, rates_age, rates_sex, rates_race,
    rates_onset, rates_onset_race
  ) %>%
    select(all_of(c(rate_ids, "measure", "value"))) %>%
    pivot_check(rate_ids, "rates") %>%
    add_not_reported_flags(
      rate_ids,
      # Death rates are reported only overall and by age, never by sex, race,
      # or infant onset timing
      list(rate_deaths = "abcs_rate_deaths")
    )

  vroom::vroom_write(strep_rates, "standard/strep_rates.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 2. Case, death and survival counts (merged across pathogens)
  #    `viewby` conflates two things for Group B: "Overall"/"ALL" is the
  #    all-ages series, while "Infants" carries early/late-onset case counts.
  #    Both age and onset are needed - the all-ages row and the infant rows are
  #    different populations, and 1997 Group B reads 16,600 all-ages cases
  #    (= 1,600 deaths + 15,000 survivals) against 2,600 + 1,300 infant cases.
  # ---------------------------------------------------------------------------
  count_ids <- c(strep_ids, "age", "onset")

  counts_all <- both %>%
    filter(topic_l == "number of cases and deaths",
           viewby_a %in% c("Overall", "ALL")) %>%
    mutate(
      v2 = tolower(viewby2_a),
      measure = case_when(
        v2 == "total cases"         ~ "abcs_N_cases",
        v2 == "number of deaths"    ~ "abcs_N_deaths",
        v2 == "number of survivals" ~ "abcs_N_survivals",
        TRUE ~ NA_character_
      ),
      age = "Total", onset = "Overall"
    ) %>%
    filter(!is.na(measure))

  counts_onset <- both %>%
    filter(topic_l == "number of cases and deaths", viewby_a == "Infants",
           viewby2_a %in% c("Early-onset cases", "Late-onset cases")) %>%
    mutate(
      measure = "abcs_N_cases",
      age     = "<1",
      onset   = sub(" cases$", "", viewby2_a)
    )

  strep_counts <- bind_rows(counts_all, counts_onset) %>%
    select(all_of(c(count_ids, "measure", "value"))) %>%
    pivot_check(count_ids, "counts") %>%
    add_not_reported_flags(
      count_ids,
      # Deaths and survivals are reported only for the all-ages series, never
      # split by infant onset timing, and are NA on exactly the same rows
      list(deaths_survivals = c("abcs_N_deaths", "abcs_N_survivals"))
    )

  vroom::vroom_write(strep_counts, "standard/strep_counts.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 3. Antibiotic resistance (merged across pathogens)
  #    The drug sits in a different column per pathogen: `viewby` for Group A,
  #    `viewby2` for Group B (whose `viewby` holds the population group).
  #    Footnote markers on drug names vary by year (Clindamycin**, ***).
  # ---------------------------------------------------------------------------
  res_ids <- c(strep_ids, "age", "onset")

  res_gas <- gas %>%
    filter(topic_l == "antibiotic resistance") %>%
    mutate(drug_raw = viewby_a, age = "Total", onset = "Overall")

  res_gbs <- gbs %>%
    filter(topic_l == "antibiotic resistance") %>%
    mutate(
      drug_raw = viewby2_a,
      is_count = viewby_a == "Number of isolates",
      grp      = if_else(is_count, "Overall", viewby_a),
      age      = GROUP_AGE[grp],
      onset    = GROUP_ONSET[grp]
    ) %>%
    filter(!is.na(age)) %>%
    mutate(drug_raw = if_else(is_count, "Number of isolates", drug_raw))

  strep_resistance <- bind_rows(res_gas, res_gbs) %>%
    mutate(
      drug     = trimws(sub("\\*+$", "", drug_raw)),
      is_count = tolower(drug) == "number of isolates",
      measure  = if_else(is_count, "abcs_n_isolates",
                         paste0("abcs_pct_resistant_", tolower(drug))),
      value    = if_else(is_count, value, as_pct(value))
    ) %>%
    select(all_of(c(res_ids, "measure", "value"))) %>%
    pivot_check(res_ids, "resistance") %>%
    add_not_reported_flags(
      res_ids,
      list(
        n_isolates  = "abcs_n_isolates",
        # Group A reports these two; Group B does not test either, and
        # linezolid was only added to the Group A panel partway through
        drug_panel  = c("abcs_pct_resistant_tetracycline",
                        "abcs_pct_resistant_linezolid")
      )
    )

  vroom::vroom_write(strep_resistance, "standard/strep_resistance.csv.gz",
                     delim = ",")

  # ---------------------------------------------------------------------------
  # 4. Clinical syndromes - NOT merged: Group A reports a rate per 100,000
  #    (2001+), Group B a percent of cases within a population group.
  # ---------------------------------------------------------------------------
  GAS_SYNDROME <- c(
    "Cellulitis"                = "cellulitis",
    "Bacteremia without focus"  = "bacteremia_without_focus",
    "Pneumonia"                 = "pneumonia",
    "Necrotizing fasciitis"     = "necrotizing_fasciitis",
    "Streptococcal toxic shock" = "strep_toxic_shock",
    "Other"                     = "other"
  )

  gas_syn_rows <- gas %>%
    filter(topic_l == "syndromes", viewby_a %in% names(GAS_SYNDROME))
  if (nrow(gas_syn_rows) == 0) {
    stop("ABCs Group A: no syndrome rows matched - the source layout changed.")
  }
  if (!all(grepl("100,000", unique(trimws(gas_syn_rows$units))))) {
    stop(
      "ABCs Group A: syndrome units are now ",
      paste(unique(trimws(gas_syn_rows$units)), collapse = "/"),
      " - expected a rate per 100,000. Revisit the column naming."
    )
  }

  gas_syndromes <- gas_syn_rows %>%
    mutate(
      measure = paste0("abcs_gas_rate_syndrome_", GAS_SYNDROME[viewby_a]),
      age = "Total", onset = "Overall"
    ) %>%
    select(all_of(c(strep_ids, "age", "onset", "measure", "value"))) %>%
    pivot_check(c(strep_ids, "age", "onset"), "gas syndromes") %>%
    add_not_reported_flags(
      c(strep_ids, "age", "onset"),
      list(strep_toxic_shock = "abcs_gas_rate_syndrome_strep_toxic_shock")
    )

  vroom::vroom_write(gas_syndromes, "standard/gas_syndromes.csv.gz", delim = ",")

  # Shared builder for the Group B topics that are keyed on a population group
  # in `viewby` and a level in `viewby2`.
  build_gbs_group_table <- function(topic_name, prefix, label) {
    rows <- gbs %>% filter(topic_l == topic_name, viewby_a %in% names(GROUP_AGE))
    if (nrow(rows) == 0) {
      stop("ABCs Group B: no rows matched topic '", topic_name,
           "' - the source layout changed.")
    }
    ids <- c(strep_ids, "age", "onset")
    rows %>%
      mutate(
        age      = GROUP_AGE[viewby_a],
        onset    = GROUP_ONSET[viewby_a],
        is_count = viewby2_a == "Number of isolates",
        level    = gsub("^_|_$", "", gsub("[^a-z0-9]+", "_", tolower(viewby2_a))),
        measure  = if_else(is_count, "abcs_gbs_n_isolates",
                           paste0("abcs_gbs_pct_", prefix, "_", level)),
        value    = if_else(is_count, value, as_pct(value))
      ) %>%
      select(all_of(c(ids, "measure", "value"))) %>%
      pivot_check(ids, label)
  }

  gbs_syndromes <- build_gbs_group_table("syndromes", "syndrome",
                                         "gbs syndromes") %>%
    add_not_reported_flags(c(strep_ids, "age", "onset"), list())

  vroom::vroom_write(gbs_syndromes, "standard/gbs_syndromes.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 5. Typing - NOT merged: Group A emm types vs Group B capsular serotypes
  #    and ALPH surface-gene types.
  # ---------------------------------------------------------------------------
  gbs_serotypes <- build_gbs_group_table("serotypes", "serotype",
                                         "gbs serotypes") %>%
    add_not_reported_flags(
      c(strep_ids, "age", "onset"),
      # CDC's grouping of the rarer serotypes changed over the series, so each
      # label variant covers only part of it
      list(
        serotype_vi_grouping = c("abcs_gbs_pct_serotype_vi_vii_viii_or_ix",
                                 "abcs_gbs_pct_serotype_vi_viii",
                                 "abcs_gbs_pct_serotype_vi")
      )
    )

  vroom::vroom_write(gbs_serotypes, "standard/gbs_serotypes.csv.gz", delim = ",")

  gbs_alph <- build_gbs_group_table("alph", "alph", "gbs alph") %>%
    add_not_reported_flags(
      c(strep_ids, "age", "onset"),
      list(
        alph_alp23 = "abcs_gbs_pct_alph_alp23",
        alph_neg   = "abcs_gbs_pct_alph_neg"
      )
    )

  vroom::vroom_write(gbs_alph, "standard/gbs_alph.csv.gz", delim = ",")

  # Group A emm types: one topic per type, `viewby` holds that type's isolate
  # count, `value` is a proportion of all typed isolates (see header note).
  emm_rows <- gas %>%
    filter(grepl("^emm[ _]", topic_l) | topic_l == "other") %>%
    mutate(
      emm_clean = gsub("[^a-z0-9]+", "_", topic_l),
      n_type    = suppressWarnings(as.numeric(viewby))
    )

  if (nrow(emm_rows) == 0) {
    stop("ABCs Group A: no emm-type rows matched - the source layout changed.")
  }
  if (all(is.na(emm_rows$n_type))) {
    stop("ABCs Group A: emm `viewby` no longer parses as an isolate count - ",
         "the source layout changed again.")
  }

  emm_long <- bind_rows(
    emm_rows %>% transmute(
      geography, time, pathogen,
      measure = paste0("abcs_gas_emm_pct_", emm_clean), value = as_pct(value)
    ),
    emm_rows %>% filter(!is.na(n_type)) %>% transmute(
      geography, time, pathogen,
      measure = paste0("abcs_gas_emm_n_", emm_clean), value = n_type
    ),
    emm_rows %>% group_by(geography, time, pathogen) %>%
      summarise(value = sum(n_type, na.rm = TRUE), .groups = "drop") %>%
      mutate(measure = "abcs_gas_emm_n_isolates_total")
  ) %>%
    mutate(age = "Total", onset = "Overall")

  emm_ids <- c(strep_ids, "age", "onset")

  # Types CDC has not broken out in every year. Each type's percent and count
  # are absent on exactly the same rows, so they share one flag.
  emm_flag_types <- c("emm_43", "emm_49", "emm_59", "emm_60", "emm_81", "emm_91")

  gas_emm <- emm_long %>%
    select(all_of(c(emm_ids, "measure", "value"))) %>%
    pivot_check(emm_ids, "gas emm") %>%
    add_not_reported_flags(
      emm_ids,
      setNames(
        lapply(emm_flag_types, function(t)
          paste0(c("abcs_gas_emm_pct_", "abcs_gas_emm_n_"), t)),
        emm_flag_types
      )
    )

  vroom::vroom_write(gas_emm, "standard/gas_emm.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 6. Record processed state for both strep datasets
  # ---------------------------------------------------------------------------
  process$raw_state_gas <- raw_state_gas
  process$raw_state_gbs <- raw_state_gbs
  dcf::dcf_process_record(updated = process)
}

