```mermaid
flowchart LR
    classDef pass stroke:#66bb6a
    classDef warn stroke:#ffa726
    classDef fail stroke:#f44336
    s0(("<strong><a href="https://www.cdc.gov/abcs/index.html" target="_blank" rel="noreferrer">Active Bacterial Core surveillance (ABCs)</a></strong>"))
    s2(("<strong><a href="https://pubmed.ncbi.nlm.nih.gov/39758745/" target="_blank" rel="noreferrer">Serotype-Specific Urinary Antigen Detection (SSUAD) Study</a></strong>"))
    s4(("<strong><a href="https://data.hrsa.gov/topics/health-workforce/ahrf" target="_blank" rel="noreferrer">Area Health Resource File (AHRF)</a></strong>"))
    s6(("<strong><a href="https://www.cdc.gov/brfss/index.html" target="_blank" rel="noreferrer">Behavioral Risk Factor Surveillance System (BRFSS)</a></strong>"))
    s8(("<strong><a href="https://data.cdc.gov/Public-Health-Surveillance/CDC-Epidemic-Trends-and-Rt/5dqz-y4ea/" target="_blank" rel="noreferrer">CDC Epidemic Trends and Rt</a></strong>"))
    s9(("<strong><a href="https://www.census.gov/programs-surveys/acs/data.html" target="_blank" rel="noreferrer">2024 American Community Survey 5-Year Estimates, Powered by Metopio</a></strong>"))
    s11(("<strong><a href="https://www.census.gov/programs-surveys/geography/guidance/geo-areas/urban-rural.html" target="_blank" rel="noreferrer">2020 Census Urban Area to County Allocation File</a></strong>"))
    s13(("<strong><a href="https://data.cdc.gov" target="_blank" rel="noreferrer">Center of Medicare and Medicaid Services (CMS)</a></strong>"))
    s15(("<strong><a href="https://data.cms.gov/tools/mapping-medicare-disparities-by-population" target="_blank" rel="noreferrer">Mapping Medicare Disparities by Population Tool</a></strong>"))
    s16(("<strong><a href="https://www.countyhealthrankings.org" target="_blank" rel="noreferrer">County Health Rankings & Roadmaps</a></strong>"))
    s18(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/doctor-visits.html" target="_blank" rel="noreferrer">CMU Delphi COVIDcast - Doctor Visits</a></strong>"))
    s20(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/" target="_blank" rel="noreferrer">CMU Delphi</a></strong>"))
    s22(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/hospital-admissions.html" target="_blank" rel="noreferrer">CMU Delphi COVIDcast - Hospital Admissions</a></strong>"))
    s23(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/" target="_blank" rel="noreferrer">CMU Delphi Epidata</a></strong>"))
    s25(("<strong><a href="https://www.cdc.gov/flu/weekly/overview.htm" target="_blank" rel="noreferrer">CDC ILINet</a></strong>"))
    s26(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/fluview.html" target="_blank" rel="noreferrer">CMU Delphi Epidata - FluView (ILINet)</a></strong>"))
    s27(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/nhsn.html" target="_blank" rel="noreferrer">CMU Delphi COVIDcast - NHSN Respiratory Hospitalizations</a></strong>"))
    s28(("<strong><a href="https://cosmos.epic.com/" target="_blank" rel="noreferrer">Epic Cosmos</a></strong>"))
    s29(("<strong><a href="https://trends.google.com" target="_blank" rel="noreferrer">Google Trends</a></strong>"))
    s31(("<strong><a href="https://apiv2.kinsainsights.com/api/v1/docs" target="_blank" rel="noreferrer">Kinsa Insights API</a></strong>"))
    s33(("<strong><a href="https://www.cdc.gov/measles/data-research/index.html" target="_blank" rel="noreferrer">CDC Measles Cases and Outbreaks - Age and Vaccination Status</a></strong>"))
    s34(("<strong><a href="https://www.cdc.gov/measles/data-research/index.html" target="_blank" rel="noreferrer">CDC Measles Cases and Outbreaks</a></strong>"))
    s35(("<strong><a href="https://github.com/CSSEGISandData/measles_data" target="_blank" rel="noreferrer">Johns Hopkins University Measles Tracking Team</a></strong>"))
    s36(("<strong><a href="https://data.medicaid.gov/datasets?theme%5B0%5D=Quality" target="_blank" rel="noreferrer">Medicaid and CHIP Adult and Child Core Set Quality Measures</a></strong>"))
    s38(("<strong><a href="https://github.com/eric-gengzhou/MMR_vaccine_estimates" target="_blank" rel="noreferrer">HealthMap MMR Vaccine Coverage Estimates</a></strong>"))
    s39(("<strong><a href="https://www.cdc.gov/narms/data/index.html" target="_blank" rel="noreferrer">NARMS Now: Human Data - Antimicrobial Resistance</a></strong>"))
    s41(("<strong><a href="https://www.fda.gov/animal-veterinary/national-antimicrobial-resistance-monitoring-system/integrated-reportssummaries" target="_blank" rel="noreferrer">FDA NARMS Retail Meats Surveillance Data</a></strong>"))
    s42(("<strong><a href="https://www.fda.gov/animal-veterinary/national-antimicrobial-resistance-monitoring-system/integrated-reportssummaries" target="_blank" rel="noreferrer">FDA NARMS Animal Pathogen Surveillance Data</a></strong>"))
    s43(("<strong><a href="https://www.fda.gov/animal-veterinary/national-antimicrobial-resistance-monitoring-system/integrated-reportssummaries" target="_blank" rel="noreferrer">FDA NARMS Food-Producing Animals Surveillance Data</a></strong>"))
    s44(("<strong><a href="https://nccrexplorer.ccdi.cancer.gov/" target="_blank" rel="noreferrer">National Childhood Cancer Registry Explorer (NCCR*Explorer)</a></strong>"))
    s46(("<strong><a href="https://data.cdc.gov/d/xkb8-kh2a" target="_blank" rel="noreferrer">NCHS VSRR Provisional Drug Overdose Death Counts (State)</a></strong>"))
    s47(("<strong><a href="https://data.cdc.gov/d/gb4e-yj24" target="_blank" rel="noreferrer">NCHS VSRR Provisional County-Level Drug Overdose Death Counts</a></strong>"))
    s48(("<strong><a href="https://data.cdc.gov/d/489q-934x" target="_blank" rel="noreferrer">NCHS VSRR Quarterly Provisional Estimates for Selected Indicators of Mortality</a></strong>"))
    s49(("<strong><a href="https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/" target="_blank" rel="noreferrer">Fatality Analysis Reporting System (FARS)</a></strong>"))
    s51(("<strong><a href="https://www.cdc.gov/nis/about/index.html" target="_blank" rel="noreferrer">National Immunization Survey (NIS)</a></strong>"))
    s52(("<strong><a href="https://www.cdc.gov/nis/about/index.html" target="_blank" rel="noreferrer">National Immunization Survey</a></strong>"))
    s54(("<strong><a href="https://www.cdc.gov/nndss/" target="_blank" rel="noreferrer">National Notifiable Diseases Surveillance System (NNDSS)</a></strong>"))
    s55(("<strong><a href="https://data.cdc.gov" target="_blank" rel="noreferrer">Centers for Disease Control and Prevention</a></strong>"))
    s57(("<strong><a href="https://data.cdc.gov/resource/3cxc-4k8q" target="_blank" rel="noreferrer">National Respiratory and Enteric Virus Surveillance System (NREVSS)</a></strong>"))
    s58(("<strong><a href="https://www.cdc.gov/nssp/index.html" target="_blank" rel="noreferrer">National Syndromic Surveillance Program (NSSP)</a></strong>"))
    s60(("<strong><a href="https://www.cdc.gov/resp-net/dashboard/index.html" target="_blank" rel="noreferrer">Respiratory Virus Hospitalization Surveillance Network (RESP-NET)</a></strong>"))
    s64(("<strong><a href="https://github.com/washingtonpost/data-school-vaccination-rates" target="_blank" rel="noreferrer">Washington Post School Vaccination Rates</a></strong>"))
    s65(("<strong><a href="https://www.tn.gov/health/cedep/immunization/school-immunization-requirements.html" target="_blank" rel="noreferrer">Tennessee Kindergarten Immunization Compliance Assessment</a></strong>"))
    s66(("<strong><a href="https://www.cdc.gov/schoolvaxview/index.html" target="_blank" rel="noreferrer">SchoolVaxView</a></strong>"))
    s68(("<strong><a href="https://jamanetwork.com/journals/jama/fullarticle/2843870" target="_blank" rel="noreferrer">Medical Exemptions From Childhood Vaccination in the US (Kiang et al. 2025)</a></strong>"))
    s69(("<strong><a href="https://data.cdc.gov/d/akvg-8vrb" target="_blank" rel="noreferrer">CDC National Wastewater Surveillance System (NWSS) - Measles</a></strong>"))
    s70(("<strong><a href="https://www.cdc.gov/nwss/" target="_blank" rel="noreferrer">CDC National Wastewater Surveillance System (NWSS)</a></strong>"))
    s72(("<strong><a href="https://wisqars.cdc.gov/" target="_blank" rel="noreferrer">Web-based Injury Statistics Query and Reporting System (WISQARS)</a></strong>"))
    s74(("<strong><a href="https://yrbs-explorer.services.cdc.gov/" target="_blank" rel="noreferrer">CDC Youth Risk Behavior Surveillance System (YRBSS)</a></strong>"))
    subgraph abcs["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/abcs" target="_blank" rel="noreferrer">abcs</a></strong>`"]
        direction LR
        n1["`data.csv-MWMJ0G3P8D.gz<br/><br/><ul><li><code>missing_info: pop</code></li></ul>`"]:::warn
        n2["`data.csv.gz<br/><br/><ul><li><code>missing_info: pop</code></li></ul>`"]:::warn
        n3["`uad.csv.gz`"]:::pass
    end
    subgraph area_health_resource_file["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/area_health_resource_file" target="_blank" rel="noreferrer">area_health_resource_file</a></strong>`"]
        direction LR
        n4["`data.csv.gz`"]:::pass
    end
    subgraph atlas_amr["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/atlas_amr" target="_blank" rel="noreferrer">atlas_amr</a></strong>`"]
        direction LR
    end
    subgraph brfss["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/brfss" target="_blank" rel="noreferrer">brfss</a></strong>`"]
        direction LR
        n5["`data_survey.csv.gz`"]:::pass
        n6["`data.csv.gz`"]:::pass
    end
    subgraph cdc_cfa_rt["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/cdc_cfa_rt" target="_blank" rel="noreferrer">cdc_cfa_rt</a></strong>`"]
        direction LR
        n7["`data.csv.gz`"]:::pass
    end
    subgraph census["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/census" target="_blank" rel="noreferrer">census</a></strong>`"]
        direction LR
        n8["`data_county.csv.gz`"]:::pass
        n9["`data_state.csv.gz`"]:::pass
        n10["`data_zcta_2019_2020.csv-MWMJ0G3P8D.gz<br/><br/><ul><li><code>missing_info: geography_zcta</code></li></ul>`"]:::warn
        n11["`data_zcta_2019_2020.csv.gz<br/><br/><ul><li><code>missing_info: geography_zcta</code></li></ul>`"]:::warn
        n12["`data_zcta_2021_2022.csv.gz<br/><br/><ul><li><code>missing_info: geography_zcta</code></li></ul>`"]:::warn
        n13["`data_zcta_2023_2024.csv.gz<br/><br/><ul><li><code>missing_info: geography_zcta</code></li></ul>`"]:::warn
    end
    subgraph cms_mmd["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/cms_mmd" target="_blank" rel="noreferrer">cms_mmd</a></strong>`"]
        direction LR
        n14["`data_state_county_age_by_race.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
        n15["`data_state_county_age_by_sex.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
        n16["`data_state_county_age.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
    end
    subgraph county_health_rankings["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/county_health_rankings" target="_blank" rel="noreferrer">county_health_rankings</a></strong>`"]
        direction LR
        n17["`data_county.csv.gz<br/><br/><ul><li><code>missing_info: chr_diabetes_monitoring, chr_binge_drinking, chr_college_degrees, chr_single_parent_households, chr_air_pollution_ozone_days, chr_access_to_healthy_foods, chr_hospice_use, chr_inadequate_social_support, chr_liquor_store_density, chr_violent_crime, chr_air_pollution_particulate_matter_days, chr_no_recent_dental_visit, chr_smoking_during_pregnancy, chr_motor_vehicle_crash_occupancy_rate, chr_on_road_motor_vehicle_crash_related_er_visits, chr_off_road_motor_vehicle_crash_related_er_visits, chr_municipal_water_wi, chr_lead_poisoned_children, chr_did_not_get_needed_health_care, chr_contaminants_in_municipal_water_wi, chr_high_housing_costs, chr_illiteracy, chr_access_to_recreational_facilities, chr_excessive_drinking_fl, chr_adequate_social_emotional_support_fl, chr_adult_smoking_fl, chr_overweight_or_obese_adults_fl, chr_fruit_and_vegetable_consumption_fl, chr_adults_who_have_a_personal_doctor_fl, chr_adults_engaging_in_moderate_physical_activity_fl, chr_insured_adults_fl, chr_binge_drinking_ny, chr_dental_visit_within_the_past_year_ny, chr_fair_or_poor_health_ny, chr_excessive_drinking_ny, chr_no_leisure_time_physical_activity_ny, chr_obese_adults_ny, chr_adult_smoking_ny, chr_fast_food_restaurants, chr_health_care_costs, chr_could_not_see_doctor_due_to_cost, chr_male_population_0_17, chr_male_population_18_44, chr_male_population_45_64, chr_male_population_65, chr_total_male_population, chr_female_population_0_17, chr_female_population_18_44, chr_female_population_45_64, chr_female_population_65, chr_total_female_population, chr_population_growth, chr_cancer_incidence, chr_coronary_heart_disease_hospitalizations, chr_cerebrovascular_disease_hospitalizations, chr_influenza_immunizations_65, chr_childhood_immunizations, chr_communicable_disease, chr_self_inflicted_injury_hospitalizations, chr_injury_hospitalizations, chr_fall_fatalities_65, chr_drug_arrests, chr_alcohol_related_hospitalizations, chr_breastfeeding, chr_dental_utilization, chr_local_health_department_staffing, chr_cervical_cancer_screening, chr_colon_cancer_screening, chr_cholesterol_screening, chr_reading_proficiency, chr_w_2_enrollment, chr_poverty, chr_child_abuse, chr_older_adults_living_alone, chr_hate_crimes, chr_year_structure_built, chr_residential_segregation_non_white_white, chr_drug_overdose_deaths_modeled, chr_opioid_hospital_visits, chr_juvenile_arrests, chr_covid_19_age_adjusted_mortality</code></li></ul>`"]:::warn
        n18["`data_state.csv.gz<br/><br/><ul><li><code>missing_info: chr_diabetes_monitoring, chr_binge_drinking, chr_college_degrees, chr_single_parent_households, chr_air_pollution_ozone_days, chr_access_to_healthy_foods, chr_hospice_use, chr_inadequate_social_support, chr_liquor_store_density, chr_violent_crime, chr_air_pollution_particulate_matter_days, chr_no_recent_dental_visit, chr_smoking_during_pregnancy, chr_motor_vehicle_crash_occupancy_rate, chr_on_road_motor_vehicle_crash_related_er_visits, chr_off_road_motor_vehicle_crash_related_er_visits, chr_municipal_water_wi, chr_lead_poisoned_children, chr_did_not_get_needed_health_care, chr_contaminants_in_municipal_water_wi, chr_high_housing_costs, chr_illiteracy, chr_access_to_recreational_facilities, chr_excessive_drinking_fl, chr_adequate_social_emotional_support_fl, chr_adult_smoking_fl, chr_overweight_or_obese_adults_fl, chr_fruit_and_vegetable_consumption_fl, chr_adults_who_have_a_personal_doctor_fl, chr_adults_engaging_in_moderate_physical_activity_fl, chr_insured_adults_fl, chr_binge_drinking_ny, chr_dental_visit_within_the_past_year_ny, chr_fair_or_poor_health_ny, chr_excessive_drinking_ny, chr_no_leisure_time_physical_activity_ny, chr_obese_adults_ny, chr_adult_smoking_ny, chr_fast_food_restaurants, chr_health_care_costs, chr_could_not_see_doctor_due_to_cost, chr_male_population_0_17, chr_male_population_18_44, chr_male_population_45_64, chr_male_population_65, chr_total_male_population, chr_female_population_0_17, chr_female_population_18_44, chr_female_population_45_64, chr_female_population_65, chr_total_female_population, chr_population_growth, chr_cancer_incidence, chr_coronary_heart_disease_hospitalizations, chr_cerebrovascular_disease_hospitalizations, chr_influenza_immunizations_65, chr_childhood_immunizations, chr_communicable_disease, chr_self_inflicted_injury_hospitalizations, chr_injury_hospitalizations, chr_fall_fatalities_65, chr_drug_arrests, chr_alcohol_related_hospitalizations, chr_breastfeeding, chr_dental_utilization, chr_local_health_department_staffing, chr_cervical_cancer_screening, chr_colon_cancer_screening, chr_cholesterol_screening, chr_reading_proficiency, chr_w_2_enrollment, chr_poverty, chr_child_abuse, chr_older_adults_living_alone, chr_hate_crimes, chr_year_structure_built, chr_residential_segregation_non_white_white, chr_drug_overdose_deaths_modeled, chr_opioid_hospital_visits, chr_juvenile_arrests, chr_covid_19_age_adjusted_mortality</code></li></ul>`"]:::warn
    end
    subgraph delphi_doctors_claims["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_doctors_claims" target="_blank" rel="noreferrer">delphi_doctors_claims</a></strong>`"]
        direction LR
        n19["`data.csv.gz`"]:::pass
    end
    subgraph delphi_hospital_claims["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_hospital_claims" target="_blank" rel="noreferrer">delphi_hospital_claims</a></strong>`"]
        direction LR
        n20["`data.csv.gz`"]:::pass
    end
    subgraph delphi_ili_fluview["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_ili_fluview" target="_blank" rel="noreferrer">delphi_ili_fluview</a></strong>`"]
        direction LR
        n21["`data.csv.gz`"]:::pass
    end
    subgraph delphi_nhsn["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_nhsn" target="_blank" rel="noreferrer">delphi_nhsn</a></strong>`"]
        direction LR
        n22["`data.csv.gz`"]:::pass
    end
    subgraph epic_chronic["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_chronic" target="_blank" rel="noreferrer">epic_chronic</a></strong>`"]
        direction LR
        n23["`county_no_time.csv.gz<br/><br/><ul><li><code>missing_info: bmi_30_49.8, obesity_(%), n_obesity_county, Year</code></li></ul>`"]:::warn
        n24["`county_year.csv.gz`"]:::pass
        n25["`state_no_time.csv.gz<br/><br/><ul><li><code>missing_info: bmi_30_49.8, dm_(%), n_patients, Year</code></li></ul>`"]:::warn
        n26["`state_year.csv.gz`"]:::pass
    end
    subgraph epic_hepb_vax["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_hepb_vax" target="_blank" rel="noreferrer">epic_hepb_vax</a></strong>`"]
        direction LR
        n27["`data.csv.gz<br/><br/><ul><li><code>missing_info: suppressed_flag</code></li></ul>`"]:::warn
    end
    subgraph epic_injury["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_injury" target="_blank" rel="noreferrer">epic_injury</a></strong>`"]
        direction LR
        n28["`heat_year_county.csv.gz<br/><br/><ul><li><code>missing_info: heat_ed_patients, total_ed_patients, heat_ed_incidence, heat_suppressed, geography_name</code></li></ul>`"]:::warn
        n29["`monthly_injury.csv.gz<br/><br/><ul><li><code>missing_info: epic_n_ed_firearm, epic_rate_ed_firearm, epic_n_ed_heat, epic_rate_ed_heat, suppressed_opioid, suppressed_firearm, suppressed_heat</code></li></ul>`"]:::warn
        n30["`yearly_injury.csv.gz<br/><br/><ul><li><code>missing_info: epic_n_ed_firearm, epic_rate_ed_firearm, epic_n_ed_heat, epic_rate_ed_heat, suppressed_opioid, suppressed_firearm, suppressed_heat</code></li></ul>`"]:::warn
    end
    subgraph epic_resp_infections["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_resp_infections" target="_blank" rel="noreferrer">epic_resp_infections</a></strong>`"]
        direction LR
        n31["`monthly_tests.csv.gz`"]:::pass
        n32["`no_geo.csv.gz`"]:::pass
        n33["`quarterly_gas.csv.gz<br/><br/><ul><li><code>missing_info: state_name</code></li></ul>`"]:::warn
        n34["`weekly.csv.gz`"]:::pass
    end
    subgraph gtrends["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/gtrends" target="_blank" rel="noreferrer">gtrends</a></strong>`"]
        direction LR
        n35["`data_dma_year.csv.gz`"]:::pass
        n36["`data_dma.csv.gz`"]:::pass
        n37["`data_year.csv.gz`"]:::pass
        n38["`data.csv.gz`"]:::pass
    end
    subgraph heat_risk["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/heat_risk" target="_blank" rel="noreferrer">heat_risk</a></strong>`"]
        direction LR
    end
    subgraph kinsa_ili["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/kinsa_ili" target="_blank" rel="noreferrer">kinsa_ili</a></strong>`"]
        direction LR
        n39["`data.csv-MWMJ0G3P8D.gz<br /><br />Script Failed:<br />Kinsa credentials not found. Set KINSA_EMAIL and KINSA_PASSWORD.`"]:::fail
        n40["`data.csv.gz<br /><br />Script Failed:<br />Kinsa credentials not found. Set KINSA_EMAIL and KINSA_PASSWORD.`"]:::fail
    end
    subgraph measles_age_cdc2["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/measles_age_cdc2" target="_blank" rel="noreferrer">measles_age_cdc2</a></strong>`"]
        direction LR
        n41["`data.csv.gz<br/><br/><ul><li><code>missing_info: year, week</code></li></ul>`"]:::warn
    end
    subgraph measles_cdc["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/measles_cdc" target="_blank" rel="noreferrer">measles_cdc</a></strong>`"]
        direction LR
        n42["`data.csv.gz`"]:::pass
    end
    subgraph measles_jhu["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/measles_jhu" target="_blank" rel="noreferrer">measles_jhu</a></strong>`"]
        direction LR
        n43["`data_county.csv.gz`"]:::pass
        n44["`data_state.csv.gz`"]:::pass
        n45["`data.csv.gz`"]:::pass
    end
    subgraph medicaid_quality["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/medicaid_quality" target="_blank" rel="noreferrer">medicaid_quality</a></strong>`"]
        direction LR
        n46["`data.csv.gz<br/><br/><ul><li><code>missing_info: geography_level, age, sex, race_ethnicity, payer, domain, medicaid_awc_ch_pct_25, medicaid_awc_ch_pct_75, medicaid_lbw_ch_pct_25, medicaid_lbw_ch_pct_75, medicaid_ima_ch_pct_25, medicaid_ima_ch_pct_75, medicaid_aba_ad_pct_25, medicaid_aba_ad_pct_75, medicaid_w34_ch_pct_25, medicaid_w34_ch_pct_75, medicaid_ldl_ad_pct_25, medicaid_ldl_ad_pct_75, medicaid_pdent_ch_pct_25, medicaid_pdent_ch_pct_75, medicaid_amm_ad_pct_25, medicaid_amm_ad_pct_75, medicaid_amb_ch_pct_25, medicaid_amb_ch_pct_75, medicaid_hpv_ch_pct_25, medicaid_hpv_ch_pct_75, medicaid_fuh_ch_30d_pct_25, medicaid_fuh_ch_30d_pct_75, medicaid_fuh_ch_7d_pct_25, medicaid_fuh_ch_7d_pct_75, medicaid_fpc_ch_pct_25, medicaid_fpc_ch_pct_75, medicaid_chl_ch_pct_25, medicaid_chl_ch_pct_75, medicaid_cap_ch_pct_25, medicaid_cap_ch_pct_75, medicaid_fuh_ad_30d_pct_25, medicaid_fuh_ad_30d_pct_75, medicaid_bcs_ad_pct_25, medicaid_bcs_ad_pct_75, medicaid_ccs_ad_pct_25, medicaid_ccs_ad_pct_75, medicaid_mma_ch_pct_25, medicaid_mma_ch_pct_75, medicaid_wcc_ch_pct_25, medicaid_wcc_ch_pct_75, medicaid_chl_ad_pct_25, medicaid_chl_ad_pct_75, medicaid_mpm_ad_pct_25, medicaid_mpm_ad_pct_75, medicaid_cis_ch_pct_25, medicaid_cis_ch_pct_75, medicaid_add_ch_cont_pct_25, medicaid_add_ch_cont_pct_75, medicaid_ppc_ad_pct_25, medicaid_ppc_ad_pct_75, medicaid_ppc_ch_pct_25, medicaid_ppc_ch_pct_75, medicaid_add_ch_init_pct_25, medicaid_add_ch_init_pct_75, medicaid_w15_ch_pct_25, medicaid_w15_ch_pct_75, medicaid_ha1c_ad_pct_25, medicaid_ha1c_ad_pct_75, medicaid_tdent_ch_pct_25, medicaid_tdent_ch_pct_75, medicaid_fuh_ad_7d_pct_25, medicaid_fuh_ad_7d_pct_75, medicaid_msc_ad_pct_25, medicaid_msc_ad_pct_75, medicaid_iet_ad_pct_25, medicaid_iet_ad_pct_75, medicaid_seal_ch_pct_25, medicaid_seal_ch_pct_75, medicaid_saa_ad_pct_25, medicaid_saa_ad_pct_75, medicaid_dev_ch_pct_25, medicaid_dev_ch_pct_75, medicaid_apc_ch_pct_25, medicaid_apc_ch_pct_75, medicaid_add_ch_30d_pct_25, medicaid_add_ch_30d_pct_75, medicaid_cbp_ad_pct_25, medicaid_cbp_ad_pct_75, medicaid_ssd_ad_pct_25, medicaid_ssd_ad_pct_75, medicaid_pqi08_ad_pct_25, medicaid_pqi08_ad_pct_75, medicaid_pqi01_ad_pct_25, medicaid_pqi01_ad_pct_75, medicaid_pqi15_ad_pct_25, medicaid_pqi15_ad_pct_75, medicaid_pqi05_ad_pct_25, medicaid_pqi05_ad_pct_75, medicaid_hpc_ad_pct_25, medicaid_hpc_ad_pct_75, medicaid_app_ch_pct_25, medicaid_app_ch_pct_75, medicaid_amr_ch_pct_25, medicaid_amr_ch_pct_75, medicaid_ccw_ch_pct_25, medicaid_ccw_ch_pct_75, medicaid_ccp_ch_pct_25, medicaid_ccp_ch_pct_75, medicaid_fua_fum_ad_7d_pct_25, medicaid_fua_fum_ad_7d_pct_75, medicaid_fua_fum_ad_30d_pct_25, medicaid_fua_fum_ad_30d_pct_75, medicaid_amr_ad_pct_25, medicaid_amr_ad_pct_75, medicaid_ccp_ad_pct_25, medicaid_ccp_ad_pct_75, medicaid_pcr_ad_pct_25, medicaid_pcr_ad_pct_75, medicaid_ohd_ad_pct_25, medicaid_ohd_ad_pct_75, medicaid_fua_ad_7d_pct_25, medicaid_fua_ad_7d_pct_75, medicaid_fua_ad_30d_pct_25, medicaid_fua_ad_30d_pct_75, medicaid_fum_ad_7d_pct_25, medicaid_fum_ad_7d_pct_75, medicaid_fum_ad_30d_pct_25, medicaid_fum_ad_30d_pct_75, medicaid_apm_ch_gluc_pct_25, medicaid_apm_ch_gluc_pct_75, medicaid_apm_ch_chol_pct_25, medicaid_apm_ch_chol_pct_75, medicaid_apm_ch_gluc_chol_pct_25, medicaid_apm_ch_gluc_chol_pct_75, medicaid_cob_ad_pct_25, medicaid_cob_ad_pct_75, medicaid_ccw_ad_pct_25, medicaid_ccw_ad_pct_75, medicaid_fva_ad_pct_25, medicaid_fva_ad_pct_75, medicaid_ncidds_ad_pct_25, medicaid_ncidds_ad_pct_75, medicaid_sfm_ch_pct_25, medicaid_sfm_ch_pct_75, medicaid_lrcd_ch_pct_25, medicaid_lrcd_ch_pct_75, medicaid_wcv_ch_pct_25, medicaid_wcv_ch_pct_75, medicaid_w30_ch_pct_25, medicaid_w30_ch_pct_75, medicaid_oud_ad_pct_25, medicaid_oud_ad_pct_75, medicaid_fua_ch_30d_pct_25, medicaid_fua_ch_30d_pct_75, medicaid_fum_ch_7d_pct_25, medicaid_fum_ch_7d_pct_75, medicaid_fum_ch_30d_pct_25, medicaid_fum_ch_30d_pct_75, medicaid_oev_ch_pct_25, medicaid_oev_ch_pct_75, medicaid_tfl_ch_pct_25, medicaid_tfl_ch_pct_75, medicaid_aab_ad_pct_25, medicaid_aab_ad_pct_75, medicaid_fua_ch_7d_pct_25, medicaid_fua_ch_7d_pct_75, medicaid_aab_ch_pct_25, medicaid_aab_ch_pct_75, medicaid_cpc_ch_pct_25, medicaid_cpc_ch_pct_75, medicaid_lsc_ch_pct_25, medicaid_lsc_ch_pct_75, medicaid_amm_ad_cont_pct_25, medicaid_amm_ad_cont_pct_75, medicaid_hbd_ad_pct_25, medicaid_hbd_ad_pct_75, medicaid_cpa_ad_pct_25, medicaid_cpa_ad_pct_75, medicaid_col_ad_pct_25, medicaid_col_ad_pct_75</code></li></ul>`"]:::warn
    end
    subgraph mmr_healthmap["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/mmr_healthmap" target="_blank" rel="noreferrer">mmr_healthmap</a></strong>`"]
        direction LR
        n47["`data_county.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
        n48["`data_state.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
        n49["`data_zcta.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
    end
    subgraph narms["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/narms" target="_blank" rel="noreferrer">narms</a></strong>`"]
        direction LR
        n50["`data_animal_pathogen.csv.gz<br/><br/><ul><li><code>missing_info: genus, host_species, collection_source, antimicrobial</code></li></ul><br />Script Failed:<br />Sheet '2017-2021_data' not found`"]:::fail
        n51["`data_food_animals.csv.gz<br/><br/><ul><li><code>missing_info: source_program, source_type, genus, species, serotype, host_species, antimicrobial</code></li></ul><br />Script Failed:<br />Sheet '2017-2021_data' not found`"]:::fail
        n52["`data_resistance_agent.csv.gz<br/><br/><ul><li><code>missing_info: genus, species_serotype, antimicrobial_class, antimicrobial_agent, test_method</code></li></ul><br />Script Failed:<br />Sheet '2017-2021_data' not found`"]:::fail
        n53["`data_resistance_pattern.csv.gz<br/><br/><ul><li><code>missing_info: genus, species_serotype, pattern, test_method</code></li></ul><br />Script Failed:<br />Sheet '2017-2021_data' not found`"]:::fail
        n54["`data_retail_meats.csv<br/><br/><ul><li><code>not_compressed</code></li><li><code>missing_info: genus, species, serotype, meat_source, antimicrobial</code></li></ul><br />Script Failed:<br />Sheet '2017-2021_data' not found`"]:::fail
        n55["`data_retail_meats.csv.gz<br/><br/><ul><li><code>missing_info: genus, species, serotype, meat_source, antimicrobial</code></li></ul><br />Script Failed:<br />Sheet '2017-2021_data' not found`"]:::fail
    end
    subgraph nccr["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nccr" target="_blank" rel="noreferrer">nccr</a></strong>`"]
        direction LR
        n56["`data.csv.gz<br/><br/><ul><li><code>missing_info: age, sex, race_ethnicity</code></li></ul>`"]:::warn
    end
    subgraph nchs_mortality["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nchs_mortality" target="_blank" rel="noreferrer">nchs_mortality</a></strong>`"]
        direction LR
        n57["`data_county.csv.gz`"]:::pass
        n58["`data_state_21_causes.csv.gz`"]:::pass
        n59["`data.csv.gz`"]:::pass
    end
    subgraph nhtsa_crash["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nhtsa_crash" target="_blank" rel="noreferrer">nhtsa_crash</a></strong>`"]
        direction LR
        n60["`data_age_sex.csv.gz<br/><br/><ul><li><code>geography_dropped</code></li><li><code>missing_info: age, sex</code></li></ul>`"]:::warn
        n61["`data_crash_type.csv.gz<br/><br/><ul><li><code>missing_info: age, sex</code></li></ul>`"]:::warn
        n62["`data_person_type.csv.gz<br/><br/><ul><li><code>geography_dropped</code></li><li><code>missing_info: person_type</code></li></ul>`"]:::warn
        n63["`data.csv.gz<br/><br/><ul><li><code>geography_dropped</code></li></ul>`"]:::warn
    end
    subgraph nis["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nis" target="_blank" rel="noreferrer">nis</a></strong>`"]
        direction LR
        n64["`data_insurance.csv.gz`"]:::pass
        n65["`data_urban.csv.gz`"]:::pass
        n66["`data.csv.gz`"]:::pass
    end
    subgraph nnds["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nnds" target="_blank" rel="noreferrer">nnds</a></strong>`"]
        direction LR
        n67["`data.csv.gz<br/><br/><ul><li><code>missing_info: mmwr_year, mmwr_week, anthrax, cholera, plague, rabies_human, rubella_congenital_syndrome, novel_influenza_a_virus_infections_total, novel_influenza_a_virus_infections_confirmed</code></li></ul><br />Script Failed:<br />In argument: 'time = +...'.`"]:::fail
    end
    subgraph noaa_heat_risk["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/noaa_heat_risk" target="_blank" rel="noreferrer">noaa_heat_risk</a></strong>`"]
        direction LR
        n68["`data_county.csv-MWMJ0G3P8D.gz<br/><br/><ul><li><code>missing_info: value, forecast_day, low_coverage_flag</code></li></ul><br />Script Failed:<br />`"]:::fail
        n69["`data_county.csv.gz<br/><br/><ul><li><code>missing_info: value, forecast_day, low_coverage_flag</code></li></ul><br />Script Failed:<br />`"]:::fail
        n70["`data_state.csv-MWMJ0G3P8D.gz<br/><br/><ul><li><code>missing_info: value, forecast_day</code></li></ul><br />Script Failed:<br />`"]:::fail
        n71["`data_state.csv.gz<br/><br/><ul><li><code>missing_info: value, forecast_day</code></li></ul><br />Script Failed:<br />`"]:::fail
    end
    subgraph NREVSS["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/NREVSS" target="_blank" rel="noreferrer">NREVSS</a></strong>`"]
        direction LR
        n72["`data.csv.gz`"]:::pass
    end
    subgraph nssp["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nssp" target="_blank" rel="noreferrer">nssp</a></strong>`"]
        direction LR
        n73["`data.csv.gz`"]:::pass
    end
    subgraph 44["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/44" target="_blank" rel="noreferrer">44</a></strong>`"]
        direction LR
    end
    subgraph respnet["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/respnet" target="_blank" rel="noreferrer">respnet</a></strong>`"]
        direction LR
        n74["`data.csv.gz<br /><br />Script Failed:<br />In argument: '&...'.`"]:::fail
    end
    subgraph schoolvax_washpost["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/schoolvax_washpost" target="_blank" rel="noreferrer">schoolvax_washpost</a></strong>`"]
        direction LR
        n75["`data_counties.csv.gz<br/><br/><ul><li><code>geography_dropped</code></li></ul>`"]:::warn
        n76["`data_schools.csv.gz`"]:::pass
    end
    subgraph schoolvaxview["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/schoolvaxview" target="_blank" rel="noreferrer">schoolvaxview</a></strong>`"]
        direction LR
        n77["`data_exemptions.csv.gz`"]:::pass
        n78["`data.csv.gz`"]:::pass
    end
    subgraph vaccine_exemptions_fattah["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/vaccine_exemptions_fattah" target="_blank" rel="noreferrer">vaccine_exemptions_fattah</a></strong>`"]
        direction LR
        n79["`data_county.csv.gz<br/><br/><ul><li><code>missing_info: is_state_estimate</code></li><li><code>type_changed: geography</code></li></ul>`"]:::warn
        n80["`data_state.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
        n81["`data.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
    end
    subgraph vaers["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/vaers" target="_blank" rel="noreferrer">vaers</a></strong>`"]
        direction LR
    end
    subgraph wastewater_measles["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wastewater_measles" target="_blank" rel="noreferrer">wastewater_measles</a></strong>`"]
        direction LR
        n82["`data_county.csv.gz`"]:::pass
        n83["`data.csv.gz`"]:::pass
    end
    subgraph wastewater["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wastewater" target="_blank" rel="noreferrer">wastewater</a></strong>`"]
        direction LR
        n84["`data.csv.gz`"]:::pass
    end
    subgraph wisqars["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wisqars" target="_blank" rel="noreferrer">wisqars</a></strong>`"]
        direction LR
        n85["`data.csv.gz`"]:::pass
    end
    subgraph yrbss["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/yrbss" target="_blank" rel="noreferrer">yrbss</a></strong>`"]
        direction LR
        n86["`data_age_ethnicity.csv.gz<br/><br/><ul><li><code>missing_info: age, race_ethnicity</code></li></ul><br />Script Failed:<br />`"]:::fail
        n87["`data_age_sex.csv.gz<br/><br/><ul><li><code>missing_info: age, sex</code></li></ul><br />Script Failed:<br />`"]:::fail
        n88["`data_age.csv.gz<br/><br/><ul><li><code>missing_info: age</code></li></ul><br />Script Failed:<br />`"]:::fail
    end
    subgraph bundle_antimicrobial_resistance["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_antimicrobial_resistance" target="_blank" rel="noreferrer">bundle_antimicrobial_resistance</a></strong>`"]
        direction LR
        n89["`resistance_by_agent.parquet`"]
        n90["`resistance_by_pattern.parquet`"]
    end
    subgraph bundle_cancer_screening["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_cancer_screening" target="_blank" rel="noreferrer">bundle_cancer_screening</a></strong>`"]
        direction LR
        n91["`cms_cancer_screening_by_race.parquet`"]
        n92["`cms_cancer_screening_by_sex.parquet`"]
        n93["`cms_cancer_screening_state.parquet`"]
        n94["`combined_cancer_screening.parquet`"]
        n95["`medicaid_cancer_screening.parquet`"]
    end
    subgraph bundle_childhood_immunizations["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_childhood_immunizations" target="_blank" rel="noreferrer">bundle_childhood_immunizations</a></strong>`"]
        direction LR
        n96["`nis_insurance.parquet`"]
        n97["`nis_overall.parquet`"]
        n98["`nis_urban.parquet`"]
        n99["`overall_rates_by_source.parquet`"]
        n100["`schoolvaxview_exemptions.parquet`"]
        n101["`schoolvaxview_overall.parquet`"]
        n102["`state_compare.parquet`"]
        n103["`wapo_vax_counties.parquet`"]
        n104["`wapo_vax_schools.parquet`"]
    end
    subgraph bundle_chronic_diseases["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_chronic_diseases" target="_blank" rel="noreferrer">bundle_chronic_diseases</a></strong>`"]
        direction LR
        n105["`brfss_prevalence_by_geography.parquet`"]
        n106["`county_opioid_by_source.parquet`"]
        n107["`deaths_cause_age.parquet`"]
        n108["`epic_prevalence_by_geography_county_and_source.parquet`"]
        n109["`epic_prevalence_by_geography_county.parquet`"]
        n110["`epic_prevalence_by_geography_year.parquet`"]
        n111["`epic_prevalence_by_geography.parquet`"]
        n112["`overdose_by_geography_and_source_county.parquet`"]
        n113["`overdose_by_geography_and_source.parquet`"]
        n114["`overdose_deaths_county.parquet`"]
        n115["`overdose_deaths_state.parquet`"]
        n116["`prevalence_by_geography_and_source.csv`"]
        n117["`prevalence_by_geography_and_source.parquet`"]
        n118["`prevalence_by_geography_and_year_and_source.parquet`"]
        n119["`prevalence_by_geography_year_and_source.parquet`"]
    end
    subgraph bundle_county_access["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_county_access" target="_blank" rel="noreferrer">bundle_county_access</a></strong>`"]
        direction LR
        n120["`county_access.parquet`"]
    end
    subgraph bundle_county_chronic["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_county_chronic" target="_blank" rel="noreferrer">bundle_county_chronic</a></strong>`"]
        direction LR
        n121["`county_chronic.parquet`"]
    end
    subgraph bundle_injury_overdose["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_injury_overdose" target="_blank" rel="noreferrer">bundle_injury_overdose</a></strong>`"]
        direction LR
        n122["`brfss_prevalence_by_geography.parquet`"]
        n123["`county_opioid_by_source.parquet`"]
        n124["`deaths_cause_age_demographics.parquet`"]
        n125["`deaths_cause_age.parquet`"]
        n126["`epic_prevalence_by_geography_year.parquet`"]
        n127["`firearms_by_demographics.parquet`"]
        n128["`firearms_by_geography_and_source_state_year.parquet`"]
        n129["`firearms_geography_source.parquet`"]
        n130["`google_dma.parquet`"]
        n131["`heat_by_geography_and_source_state_year.parquet`"]
        n132["`heat_related_geography_source.parquet`"]
        n133["`heat_risk-MWMJ0G3P8D.parquet`"]
        n134["`heat_risk.parquet`"]
        n135["`medicaid_injury_overdose.parquet`"]
        n136["`overdose_by_demographics.parquet`"]
        n137["`overdose_by_geography_and_source_county.parquet`"]
        n138["`overdose_by_geography_and_source_state_year.parquet`"]
        n139["`overdose_by_geography_and_source.parquet`"]
        n140["`overdose_deaths_county.parquet`"]
        n141["`overdose_deaths_state.parquet`"]
        n142["`state_opioid_by_source.parquet`"]
    end
    subgraph bundle_maternal_health["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_maternal_health" target="_blank" rel="noreferrer">bundle_maternal_health</a></strong>`"]
        direction LR
        n143["`maternal_county.parquet`"]
        n144["`maternal_state.parquet`"]
    end
    subgraph bundle_measles["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_measles" target="_blank" rel="noreferrer">bundle_measles</a></strong>`"]
        direction LR
        n145["`measles_cases_by_age.parquet`"]
        n146["`measles_county.parquet`"]
        n147["`measles_state.parquet`"]
    end
    subgraph bundle_respiratory["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_respiratory" target="_blank" rel="noreferrer">bundle_respiratory</a></strong>`"]
        direction LR
        n148["`covid_ed_visits_by_county.parquet`"]
        n149["`covid_overall_trends.parquet`"]
        n150["`covid_trends_by_age.parquet`"]
        n151["`flu_ed_visits_by_county.parquet`"]
        n152["`flu_overall_trends.parquet`"]
        n153["`flu_trends_by_age.parquet`"]
        n154["`pneumococcus_by_geography_year.parquet`"]
        n155["`pneumococcus_by_geography.parquet`"]
        n156["`pneumococcus_comparison.parquet`"]
        n157["`pneumococcus_serotype_trends-MWMJ0G3P8D.parquet`"]
        n158["`pneumococcus_serotype_trends.parquet`"]
        n159["`rsv_ed_visits_by_county.parquet`"]
        n160["`rsv_google_dma.parquet`"]
        n161["`rsv_overall_trends.parquet`"]
        n162["`rsv_positive_tests.parquet`"]
        n163["`rsv_testing_pct.parquet`"]
        n164["`rsv_trends_by_age.parquet`"]
    end
    s0---s1["<strong><a href="https://data.cdc.gov/resource/qvzb-qs6p/" target="_blank" rel="noreferrer">Serotype Data for Invasive Pneumococcal Disease Cases by Age Group from Active Bacterial Core surveillance</a></strong>"]
    s1 --> n1
    s1 --> n2
    s1 --> n3
    s2---s3["<strong><a href="https://pubmed.ncbi.nlm.nih.gov/39758745/" target="_blank" rel="noreferrer">Open Forum for Infectious Diseases</a></strong>"]
    s3 --> n3
    s4---s5["<strong><a href="https://data.hrsa.gov/topics/health-workforce/ahrf" target="_blank" rel="noreferrer">AHRF County-Level Data Files</a></strong>"]
    s5 --> n4
    s6---s7["<strong><a href="https://data.cdc.gov/Behavioral-Risk-Factors/Behavioral-Risk-Factor-Surveillance-System-BRFSS-P/dttw-5yxu/about_data" target="_blank" rel="noreferrer">Behavioral Risk Factor Surveillance System (BRFSS) Prevalence Data (2011 to present)</a></strong>"]
    s7 --> n5
    s7 --> n6
    s8 --> n7
    s9---s10["<strong><a href="https://api.census.gov/data.html" target="_blank" rel="noreferrer">Census API — ACS 5-Year Detailed Tables and Subject Tables</a></strong>"]
    s10 --> n8
    s11---s12["<strong><a href="https://www2.census.gov/geo/docs/reference/ua/2020_UA_COUNTY.xlsx" target="_blank" rel="noreferrer">2020 Census Urban Area to County Allocation File (XLSX)</a></strong>"]
    s12 --> n8
    s10 --> n9
    s10 --> n10
    s10 --> n11
    s10 --> n12
    s10 --> n13
    s13---s14["<strong><a href="https://data.cms.gov/tools/mapping-medicare-disparities-by-population" target="_blank" rel="noreferrer">Mapping Medicare Disparities by Population Tool</a></strong>"]
    s14 --> n14
    s15 --> n14
    s14 --> n15
    s15 --> n15
    s14 --> n16
    s15 --> n16
    s16---s17["<strong><a href="https://www.countyhealthrankings.org/health-data/methodology-and-sources/data-documentation" target="_blank" rel="noreferrer">County Health Rankings & Roadmaps Annual Data</a></strong>"]
    s17 --> n17
    s17 --> n18
    s18---s19["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/" target="_blank" rel="noreferrer">COVIDcast Epidata API</a></strong>"]
    s19 --> n19
    s20---s21["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/hospital-admissions.html" target="_blank" rel="noreferrer">COVIDcast > Hospital Admissions</a></strong>"]
    s21 --> n20
    s19 --> n20
    s23---s24["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/fluview.html" target="_blank" rel="noreferrer">FluView API</a></strong>"]
    s24 --> n21
    s25 --> n21
    s19 --> n21
    s19 --> n22
    s28 --> n23
    s28 --> n24
    s28 --> n25
    s28 --> n26
    s28 --> n27
    s28 --> n29
    s28 --> n30
    s28 --> n31
    s28 --> n32
    s28 --> n33
    s28 --> n34
    s29---s30["<strong><a href="https://github.com/DISSC-yale/gtrends_collection" target="_blank" rel="noreferrer">Yale Data-Intensive Social Sciences, Google Trends Collection Framework</a></strong>"]
    s30 --> n35
    s30 --> n36
    s30 --> n37
    s30 --> n38
    s31---s32["<strong><a href="https://apiv2.kinsainsights.com/api/v1/docs" target="_blank" rel="noreferrer">Kinsa Insights API - Signal Endpoint</a></strong>"]
    s32 --> n39
    s32 --> n40
    s33 --> n41
    s34 --> n42
    s35 --> n43
    s35 --> n44
    s35 --> n45
    s36---s37["<strong><a href="https://data.medicaid.gov/datasets?theme%5B0%5D=Quality" target="_blank" rel="noreferrer">Medicaid.gov Open Data – Quality Measures datasets (2014–2023)</a></strong>"]
    s37 --> n46
    s38 --> n47
    s38 --> n48
    s38 --> n49
    s39---s40["<strong><a href="https://app.powerbigov.us/view?r=eyJrIjoiZmU5ZjA2ZDItNTU0MS00M2EzLWEyZmQtZmY3Y2RlZjdjYTdjIiwidCI6IjljZTcwODY5LTYwZGItNDRmZC1hYmU4LWQyNzY3MDc3ZmM4ZiJ9" target="_blank" rel="noreferrer">NARMS Now Interactive Dashboard - Human Data</a></strong>"]
    s40 --> n50
    s41 --> n50
    s42 --> n50
    s43 --> n50
    s40 --> n51
    s41 --> n51
    s42 --> n51
    s43 --> n51
    s40 --> n52
    s41 --> n52
    s42 --> n52
    s43 --> n52
    s40 --> n53
    s41 --> n53
    s42 --> n53
    s43 --> n53
    s40 --> n54
    s41 --> n54
    s42 --> n54
    s43 --> n54
    s40 --> n55
    s41 --> n55
    s42 --> n55
    s43 --> n55
    s44---s45["<strong><a href="https://nccrexplorer.ccdi.cancer.gov/application.html" target="_blank" rel="noreferrer">NCCR*Explorer: An interactive website for NCCR cancer statistics</a></strong>"]
    s45 --> n56
    s46 --> n57
    s47 --> n57
    s48 --> n58
    s46 --> n59
    s47 --> n59
    s49---s50["<strong><a href="https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/" target="_blank" rel="noreferrer">NHTSA File Downloads — FARS National CSV archives</a></strong>"]
    s50 --> n60
    s50 --> n61
    s50 --> n62
    s50 --> n63
    s51 --> n64
    s52---s53["<strong><a href="https://www.cdc.gov/nis/about/index.html" target="_blank" rel="noreferrer">About the National Immunization Surveys (NIS)</a></strong>"]
    s53 --> n64
    s51 --> n65
    s53 --> n65
    s51 --> n66
    s53 --> n66
    s54 --> n67
    s55---s56["<strong><a href="https://data.cdc.gov/resource/3cxc-4k8q" target="_blank" rel="noreferrer">Percent Positivity of Respiratory Syncytial Virus Nucleic Acid Amplification Tests by HHS Region, National Respiratory and Enteric Virus Surveillance System</a></strong>"]
    s56 --> n72
    s57 --> n72
    s58---s59["<strong><a href="https://data.cdc.gov/resource/rdmq-nq56" target="_blank" rel="noreferrer">National Syndromic Surveillance Program</a></strong>"]
    s59 --> n73
    s60---s61["<strong><a href="https://healthdata.gov/CDC/Weekly-Rates-of-Laboratory-Confirmed-COVID-19-Hosp/gk5r-vjtt/about_data" target="_blank" rel="noreferrer">Weekly Rates of Laboratory-Confirmed COVID-19 Hospitalizations from the COVID-NET Surveillance System</a></strong>"]
    s61 --> n74
    s60---s62["<strong><a href="https://data.cdc.gov/Public-Health-Surveillance/Weekly-Rates-of-Laboratory-Confirmed-RSV-Hospitali/29hc-w46k/about_data" target="_blank" rel="noreferrer">Weekly Rates of Laboratory-Confirmed RSV Hospitalizations from the RSV-NET Surveillance System</a></strong>"]
    s62 --> n74
    s60---s63["<strong><a href="https://data.cdc.gov/Public-Health-Surveillance/Rates-of-Laboratory-Confirmed-RSV-COVID-19-and-Flu/kvib-3txy/about_data" target="_blank" rel="noreferrer">Rates of Laboratory-Confirmed RSV, COVID-19, and Flu Hospitalizations from the RESP-NET Surveillance Systems</a></strong>"]
    s63 --> n74
    s64 --> n75
    s65 --> n75
    s64 --> n76
    s66---s67["<strong><a href="https://data.cdc.gov/Vaccinations/Vaccination-Coverage-and-Exemptions-among-Kinderga/ijqb-a7ye/about_data" target="_blank" rel="noreferrer">Vaccination Coverage and Exemptions among Kindergartners</a></strong>"]
    s67 --> n77
    s67 --> n78
    s68 --> n79
    s68 --> n80
    s68 --> n81
    s69 --> n82
    s69 --> n83
    s70---s71["<strong><a href="https://data.cdc.gov/Public-Health-Surveillance/CDC-Wastewater-Viral-Activity-Level-for-SARS-CoV-2/atcp-73re/" target="_blank" rel="noreferrer">CDC Wastewater Viral Activity Level for SARS-CoV-2, Influenza A and RSV</a></strong>"]
    s71 --> n84
    s72---s73["<strong><a href="https://wisqars.cdc.gov/reports/?o=MORT&i=8&m=20810&s=0&r=0&ry=2&y1=2018&y2=2023&a=ALL&g1=0&g2=199&a1=0&a2=199&r1=MECH&r2=AGEGP&r3=STATE&r4=YEAR&r5=NONE&r6=NONE&g=00&e=0&yp=65&me=0&t=0" target="_blank" rel="noreferrer">Fatal Injury Report</a></strong>"]
    s73 --> n85
    s74 --> n86
    s74 --> n87
    s74 --> n88
    n52 --> bundle_antimicrobial_resistance
    n53 --> bundle_antimicrobial_resistance
    n55 --> bundle_antimicrobial_resistance
    n50 --> bundle_antimicrobial_resistance
    n51 --> bundle_antimicrobial_resistance
    n16 --> bundle_cancer_screening
    n15 --> bundle_cancer_screening
    n14 --> bundle_cancer_screening
    n46 --> bundle_cancer_screening
    n78 --> bundle_childhood_immunizations
    n77 --> bundle_childhood_immunizations
    n75 --> bundle_childhood_immunizations
    n76 --> bundle_childhood_immunizations
    n66 --> bundle_childhood_immunizations
    n65 --> bundle_childhood_immunizations
    n64 --> bundle_childhood_immunizations
    n6 --> bundle_chronic_diseases
    n16 --> bundle_chronic_diseases
    n6 --> bundle_injury_overdose
    n16 --> bundle_injury_overdose
    n59 --> bundle_injury_overdose
    n57 --> bundle_injury_overdose
    n85 --> bundle_injury_overdose
    n83 --> bundle_measles
    n41 --> bundle_measles
    n38 --> bundle_respiratory
    n84 --> bundle_respiratory
    n2 --> bundle_respiratory
    n3 --> bundle_respiratory
    n72 --> bundle_respiratory
    n73 --> bundle_respiratory
    n74 --> bundle_respiratory
```
