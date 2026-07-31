```mermaid
flowchart LR
    classDef pass stroke:#66bb6a
    classDef warn stroke:#ffa726
    classDef fail stroke:#f44336
    s0(("<strong><a href="https://www.cdc.gov/abcs/index.html" target="_blank" rel="noreferrer">Active Bacterial Core surveillance (ABCs)</a></strong>"))
    s2(("<strong><a href="https://pubmed.ncbi.nlm.nih.gov/39758745/" target="_blank" rel="noreferrer">Serotype-Specific Urinary Antigen Detection (SSUAD) Study</a></strong>"))
    s4(("<strong><a href="https://data.hrsa.gov/topics/health-workforce/ahrf" target="_blank" rel="noreferrer">Area Health Resource File (AHRF)</a></strong>"))
    s6(("<strong><a href="https://data.cdc.gov/Foodborne-Waterborne-and-Related-Diseases/BEAM-Dashboard-Report-Data/jbhn-e8xn/about_data" target="_blank" rel="noreferrer">BEAM Dashboard - Report Data</a></strong>"))
    s8(("<strong><a href="https://www.cdc.gov/brfss/index.html" target="_blank" rel="noreferrer">Behavioral Risk Factor Surveillance System (BRFSS)</a></strong>"))
    s10(("<strong><a href="https://data.cdc.gov/Public-Health-Surveillance/CDC-Epidemic-Trends-and-Rt/5dqz-y4ea/" target="_blank" rel="noreferrer">CDC Epidemic Trends and Rt</a></strong>"))
    s11(("<strong><a href="https://data.cdc.gov/d/e2d5-ggg7" target="_blank" rel="noreferrer">NCHS VSRR Provisional Maternal Death Counts and Rates</a></strong>"))
    s12(("<strong><a href="https://www.census.gov/programs-surveys/acs/data.html" target="_blank" rel="noreferrer">2024 American Community Survey 5-Year Estimates, Powered by Metopio</a></strong>"))
    s14(("<strong><a href="https://www.census.gov/programs-surveys/geography/guidance/geo-areas/urban-rural.html" target="_blank" rel="noreferrer">2020 Census Urban Area to County Allocation File</a></strong>"))
    s16(("<strong><a href="https://data.cdc.gov" target="_blank" rel="noreferrer">Center of Medicare and Medicaid Services (CMS)</a></strong>"))
    s18(("<strong><a href="https://data.cms.gov/tools/mapping-medicare-disparities-by-population" target="_blank" rel="noreferrer">Mapping Medicare Disparities by Population Tool</a></strong>"))
    s19(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/doctor-visits.html" target="_blank" rel="noreferrer">CMU Delphi COVIDcast - Doctor Visits</a></strong>"))
    s21(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/" target="_blank" rel="noreferrer">CMU Delphi</a></strong>"))
    s23(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/hospital-admissions.html" target="_blank" rel="noreferrer">CMU Delphi COVIDcast - Hospital Admissions</a></strong>"))
    s24(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/" target="_blank" rel="noreferrer">CMU Delphi Epidata</a></strong>"))
    s26(("<strong><a href="https://www.cdc.gov/flu/weekly/overview.htm" target="_blank" rel="noreferrer">CDC ILINet</a></strong>"))
    s27(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/fluview.html" target="_blank" rel="noreferrer">CMU Delphi Epidata - FluView (ILINet)</a></strong>"))
    s28(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/nhsn.html" target="_blank" rel="noreferrer">CMU Delphi COVIDcast - NHSN Respiratory Hospitalizations</a></strong>"))
    s29(("<strong><a href="https://cosmos.epic.com/" target="_blank" rel="noreferrer">Epic Cosmos</a></strong>"))
    s30(("<strong><a href="https://www.epicresearch.org/health-alerts/" target="_blank" rel="noreferrer">Epic Research Health Alerts</a></strong>"))
    s31(("<strong><a href="https://trends.google.com" target="_blank" rel="noreferrer">Google Trends</a></strong>"))
    s33(("<strong><a href="https://apiv2.kinsainsights.com/api/v1/docs" target="_blank" rel="noreferrer">Kinsa Insights API</a></strong>"))
    s35(("<strong><a href="https://www.cdc.gov/measles/data-research/index.html" target="_blank" rel="noreferrer">CDC Measles Cases and Outbreaks - Age and Vaccination Status</a></strong>"))
    s36(("<strong><a href="https://www.cdc.gov/measles/data-research/index.html" target="_blank" rel="noreferrer">CDC Measles Cases and Outbreaks</a></strong>"))
    s37(("<strong><a href="https://github.com/CSSEGISandData/measles_data" target="_blank" rel="noreferrer">Johns Hopkins University Measles Tracking Team</a></strong>"))
    s38(("<strong><a href="https://data.medicaid.gov/datasets?theme%5B0%5D=Quality" target="_blank" rel="noreferrer">Medicaid and CHIP Adult and Child Core Set Quality Measures</a></strong>"))
    s40(("<strong><a href="https://github.com/eric-gengzhou/MMR_vaccine_estimates" target="_blank" rel="noreferrer">HealthMap MMR Vaccine Coverage Estimates</a></strong>"))
    s41(("<strong><a href="https://www.cdc.gov/narms/data/index.html" target="_blank" rel="noreferrer">NARMS Now: Human Data - Antimicrobial Resistance</a></strong>"))
    s43(("<strong><a href="https://www.fda.gov/animal-veterinary/national-antimicrobial-resistance-monitoring-system/integrated-reportssummaries" target="_blank" rel="noreferrer">FDA NARMS Retail Meats Surveillance Data</a></strong>"))
    s44(("<strong><a href="https://www.fda.gov/animal-veterinary/national-antimicrobial-resistance-monitoring-system/integrated-reportssummaries" target="_blank" rel="noreferrer">FDA NARMS Animal Pathogen Surveillance Data</a></strong>"))
    s45(("<strong><a href="https://www.fda.gov/animal-veterinary/national-antimicrobial-resistance-monitoring-system/integrated-reportssummaries" target="_blank" rel="noreferrer">FDA NARMS Food-Producing Animals Surveillance Data</a></strong>"))
    s46(("<strong><a href="https://nccrexplorer.ccdi.cancer.gov/" target="_blank" rel="noreferrer">National Childhood Cancer Registry Explorer (NCCR*Explorer)</a></strong>"))
    s48(("<strong><a href="https://data.cdc.gov/d/xkb8-kh2a" target="_blank" rel="noreferrer">NCHS VSRR Provisional Drug Overdose Death Counts (State)</a></strong>"))
    s49(("<strong><a href="https://data.cdc.gov/d/gb4e-yj24" target="_blank" rel="noreferrer">NCHS VSRR Provisional County-Level Drug Overdose Death Counts</a></strong>"))
    s50(("<strong><a href="https://data.cdc.gov/d/489q-934x" target="_blank" rel="noreferrer">NCHS VSRR Quarterly Provisional Estimates for Selected Indicators of Mortality</a></strong>"))
    s51(("<strong><a href="https://www.cpsc.gov/Research--Statistics/NEISS-Injury-Data" target="_blank" rel="noreferrer">National Electronic Injury Surveillance System (NEISS)</a></strong>"))
    s53(("<strong><a href="https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/" target="_blank" rel="noreferrer">Fatality Analysis Reporting System (FARS)</a></strong>"))
    s55(("<strong><a href="https://www.cdc.gov/nis/about/index.html" target="_blank" rel="noreferrer">National Immunization Survey (NIS)</a></strong>"))
    s56(("<strong><a href="https://www.cdc.gov/nis/about/index.html" target="_blank" rel="noreferrer">National Immunization Survey</a></strong>"))
    s58(("<strong><a href="https://www.cdc.gov/nndss/" target="_blank" rel="noreferrer">National Notifiable Diseases Surveillance System (NNDSS)</a></strong>"))
    s59(("<strong><a href="https://www.wpc.ncep.noaa.gov/heatrisk/data/archive/" target="_blank" rel="noreferrer">NOAA WPC HeatRisk</a></strong>"))
    s61(("<strong><a href="https://data.cdc.gov" target="_blank" rel="noreferrer">Centers for Disease Control and Prevention</a></strong>"))
    s63(("<strong><a href="https://data.cdc.gov/resource/3cxc-4k8q" target="_blank" rel="noreferrer">National Respiratory and Enteric Virus Surveillance System (NREVSS)</a></strong>"))
    s64(("<strong><a href="https://www.cdc.gov/nssp/index.html" target="_blank" rel="noreferrer">National Syndromic Surveillance Program (NSSP)</a></strong>"))
    s66(("<strong><a href="https://www.cdc.gov/resp-net/dashboard/index.html" target="_blank" rel="noreferrer">Respiratory Virus Hospitalization Surveillance Network (RESP-NET)</a></strong>"))
    s70(("<strong><a href="https://github.com/washingtonpost/data-school-vaccination-rates" target="_blank" rel="noreferrer">Washington Post School Vaccination Rates</a></strong>"))
    s71(("<strong><a href="https://www.tn.gov/health/cedep/immunization/school-immunization-requirements.html" target="_blank" rel="noreferrer">Tennessee Kindergarten Immunization Compliance Assessment</a></strong>"))
    s72(("<strong><a href="https://www.cdc.gov/schoolvaxview/index.html" target="_blank" rel="noreferrer">SchoolVaxView</a></strong>"))
    s74(("<strong><a href="https://jamanetwork.com/journals/jama/fullarticle/2843870" target="_blank" rel="noreferrer">Medical Exemptions From Childhood Vaccination in the US (Kiang et al. 2025)</a></strong>"))
    s75(("<strong><a href="https://data.cdc.gov/d/akvg-8vrb" target="_blank" rel="noreferrer">CDC National Wastewater Surveillance System (NWSS) - Measles</a></strong>"))
    s76(("<strong><a href="https://www.cdc.gov/nwss/" target="_blank" rel="noreferrer">CDC National Wastewater Surveillance System (NWSS)</a></strong>"))
    s78(("<strong><a href="https://wisqars.cdc.gov/" target="_blank" rel="noreferrer">Web-based Injury Statistics Query and Reporting System (WISQARS)</a></strong>"))
    s80(("<strong><a href="https://yrbs-explorer.services.cdc.gov/" target="_blank" rel="noreferrer">CDC Youth Risk Behavior Surveillance System (YRBSS)</a></strong>"))
    subgraph abcs["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/abcs" target="_blank" rel="noreferrer">abcs</a></strong>`"]
        direction LR
        n1["`data.csv.gz<br/><br/><ul><li><code>missing_info: pop</code></li></ul>`"]:::warn
        n2["`uad.csv.gz`"]:::pass
    end
    subgraph area_health_resource_file["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/area_health_resource_file" target="_blank" rel="noreferrer">area_health_resource_file</a></strong>`"]
        direction LR
        n3["`data.csv.gz`"]:::pass
    end
    subgraph atlas_amr["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/atlas_amr" target="_blank" rel="noreferrer">atlas_amr</a></strong>`"]
        direction LR
    end
    subgraph beam["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/beam" target="_blank" rel="noreferrer">beam</a></strong>`"]
        direction LR
        n4["`data.csv.gz`"]:::pass
    end
    subgraph brfss["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/brfss" target="_blank" rel="noreferrer">brfss</a></strong>`"]
        direction LR
        n5["`data_survey.csv.gz`"]:::pass
        n6["`data.csv.gz<br/><br/><ul><li><code>type_changed: pct_depression_sample_size, pct_diabetes_sample_size, pct_heavy_drink_sample_size, pct_obesity_sample_size</code></li></ul>`"]:::warn
    end
    subgraph cdc_cfa_rt["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/cdc_cfa_rt" target="_blank" rel="noreferrer">cdc_cfa_rt</a></strong>`"]
        direction LR
        n7["`data.csv.gz`"]:::pass
    end
    subgraph cdc_vssr["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/cdc_vssr" target="_blank" rel="noreferrer">cdc_vssr</a></strong>`"]
        direction LR
        n8["`data.csv.gz`"]:::pass
    end
    subgraph census["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/census" target="_blank" rel="noreferrer">census</a></strong>`"]
        direction LR
        n9["`data_county.csv.gz`"]:::pass
        n10["`data_state.csv.gz`"]:::pass
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
    end
    subgraph delphi_doctors_claims["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_doctors_claims" target="_blank" rel="noreferrer">delphi_doctors_claims</a></strong>`"]
        direction LR
        n17["`data.csv.gz`"]:::pass
    end
    subgraph delphi_hospital_claims["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_hospital_claims" target="_blank" rel="noreferrer">delphi_hospital_claims</a></strong>`"]
        direction LR
        n18["`data.csv.gz`"]:::pass
    end
    subgraph delphi_ili_fluview["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_ili_fluview" target="_blank" rel="noreferrer">delphi_ili_fluview</a></strong>`"]
        direction LR
        n19["`data.csv.gz`"]:::pass
    end
    subgraph delphi_nhsn["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_nhsn" target="_blank" rel="noreferrer">delphi_nhsn</a></strong>`"]
        direction LR
        n20["`data.csv.gz`"]:::pass
    end
    subgraph epic_chronic["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_chronic" target="_blank" rel="noreferrer">epic_chronic</a></strong>`"]
        direction LR
        n21["`county_no_time.csv.gz<br/><br/><ul><li><code>missing_info: bmi_30_49.8, obesity_(%), n_obesity_county, Year</code></li></ul>`"]:::warn
        n22["`county_year.csv.gz`"]:::pass
        n23["`state_no_time.csv.gz<br/><br/><ul><li><code>missing_info: bmi_30_49.8, dm_(%), n_patients, Year</code></li></ul>`"]:::warn
        n24["`state_year.csv.gz`"]:::pass
    end
    subgraph epic_diarrhea["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_diarrhea" target="_blank" rel="noreferrer">epic_diarrhea</a></strong>`"]
        direction LR
        n25["`data_weekly.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
        n26["`weekly_tests.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
    end
    subgraph epic_health_alerts["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_health_alerts" target="_blank" rel="noreferrer">epic_health_alerts</a></strong>`"]
        direction LR
        n27["`data.csv.gz`"]:::pass
    end
    subgraph epic_hepb_vax["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_hepb_vax" target="_blank" rel="noreferrer">epic_hepb_vax</a></strong>`"]
        direction LR
        n28["`data.csv.gz`"]:::pass
    end
    subgraph epic_injury["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_injury" target="_blank" rel="noreferrer">epic_injury</a></strong>`"]
        direction LR
        n29["`heat_year_county.csv.gz<br/><br/><ul><li><code>missing_info: geography_name</code></li></ul>`"]:::warn
        n30["`monthly_injury.csv.gz`"]:::pass
        n31["`yearly_injury.csv.gz`"]:::pass
    end
    subgraph epic_resp_infections["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_resp_infections" target="_blank" rel="noreferrer">epic_resp_infections</a></strong>`"]
        direction LR
        n32["`monthly_tests.csv.gz`"]:::pass
        n33["`no_geo.csv.gz`"]:::pass
        n34["`quarterly_gas.csv.gz<br/><br/><ul><li><code>missing_info: state_name</code></li></ul>`"]:::warn
        n35["`weekly.csv.gz`"]:::pass
    end
    subgraph gtrends["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/gtrends" target="_blank" rel="noreferrer">gtrends</a></strong>`"]
        direction LR
        n36["`data_dma_year.csv.gz`"]:::pass
        n37["`data_dma.csv.gz`"]:::pass
        n38["`data_year.csv.gz`"]:::pass
        n39["`data.csv.gz`"]:::pass
    end
    subgraph kinsa_ili["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/kinsa_ili" target="_blank" rel="noreferrer">kinsa_ili</a></strong>`"]
        direction LR
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
        n54["`data_retail_meats.csv.gz<br/><br/><ul><li><code>missing_info: genus, species, serotype, meat_source, antimicrobial</code></li></ul><br />Script Failed:<br />Sheet '2017-2021_data' not found`"]:::fail
    end
    subgraph nccr["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nccr" target="_blank" rel="noreferrer">nccr</a></strong>`"]
        direction LR
        n55["`data.csv.gz<br/><br/><ul><li><code>missing_info: age, sex, race_ethnicity</code></li></ul>`"]:::warn
    end
    subgraph nchs_mortality["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nchs_mortality" target="_blank" rel="noreferrer">nchs_mortality</a></strong>`"]
        direction LR
        n56["`data_county.csv.gz<br /><br />Script Failed:<br />In argument: 'N_deaths = sum('Data Value')'.`"]:::fail
        n57["`data_state_21_causes.csv.gz<br /><br />Script Failed:<br />In argument: 'N_deaths = sum('Data Value')'.`"]:::fail
        n58["`data.csv.gz<br /><br />Script Failed:<br />In argument: 'N_deaths = sum('Data Value')'.`"]:::fail
    end
    subgraph neiss["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/neiss" target="_blank" rel="noreferrer">neiss</a></strong>`"]
        direction LR
        n59["`data_agegroup_diagnosis_rate.csv.gz`"]:::pass
        n60["`data_agegroup_diagnosis.csv.gz`"]:::pass
        n61["`data_agegroup_product_rate.csv.gz`"]:::pass
        n62["`data_agegroup_product.csv.gz`"]:::pass
        n63["`data_infant_diagnosis_rate.csv.gz`"]:::pass
        n64["`data_infant_diagnosis.csv.gz`"]:::pass
        n65["`data_infant_product_rate.csv.gz`"]:::pass
        n66["`data_infant_product.csv.gz`"]:::pass
    end
    subgraph nhtsa_crash["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nhtsa_crash" target="_blank" rel="noreferrer">nhtsa_crash</a></strong>`"]
        direction LR
        n67["`data_age_sex.csv.gz<br/><br/><ul><li><code>geography_dropped</code></li><li><code>missing_info: age, sex</code></li></ul>`"]:::warn
        n68["`data_crash_type.csv.gz<br/><br/><ul><li><code>missing_info: age, sex</code></li></ul>`"]:::warn
        n69["`data_person_type.csv.gz<br/><br/><ul><li><code>geography_dropped</code></li><li><code>missing_info: person_type</code></li></ul>`"]:::warn
        n70["`data.csv.gz<br/><br/><ul><li><code>geography_dropped</code></li></ul>`"]:::warn
    end
    subgraph nis["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nis" target="_blank" rel="noreferrer">nis</a></strong>`"]
        direction LR
        n71["`data_insurance.csv.gz`"]:::pass
        n72["`data_urban.csv.gz`"]:::pass
        n73["`data.csv.gz`"]:::pass
    end
    subgraph nnds["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nnds" target="_blank" rel="noreferrer">nnds</a></strong>`"]
        direction LR
        n74["`data.csv.gz<br/><br/><ul><li><code>missing_info: mmwr_year, mmwr_week, anthrax, plague, rabies_human, rubella_congenital_syndrome</code></li></ul>`"]:::warn
    end
    subgraph noaa_heat_risk["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/noaa_heat_risk" target="_blank" rel="noreferrer">noaa_heat_risk</a></strong>`"]
        direction LR
        n75["`data_county.csv.gz`"]:::pass
        n76["`data_state.csv.gz`"]:::pass
    end
    subgraph NREVSS["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/NREVSS" target="_blank" rel="noreferrer">NREVSS</a></strong>`"]
        direction LR
        n77["`data.csv.gz<br /><br />Script Failed:<br />character string is not in a standard unambiguous format`"]:::fail
    end
    subgraph nssp["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nssp" target="_blank" rel="noreferrer">nssp</a></strong>`"]
        direction LR
        n78["`data.csv.gz`"]:::pass
    end
    subgraph respnet["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/respnet" target="_blank" rel="noreferrer">respnet</a></strong>`"]
        direction LR
        n79["`data.csv.gz`"]:::pass
    end
    subgraph schoolvax_washpost["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/schoolvax_washpost" target="_blank" rel="noreferrer">schoolvax_washpost</a></strong>`"]
        direction LR
        n80["`data_counties.csv.gz<br/><br/><ul><li><code>geography_dropped</code></li></ul>`"]:::warn
        n81["`data_schools.csv.gz`"]:::pass
    end
    subgraph schoolvaxview["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/schoolvaxview" target="_blank" rel="noreferrer">schoolvaxview</a></strong>`"]
        direction LR
        n82["`data_exemptions.csv.gz`"]:::pass
        n83["`data.csv.gz`"]:::pass
    end
    subgraph vaccine_exemptions_fattah["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/vaccine_exemptions_fattah" target="_blank" rel="noreferrer">vaccine_exemptions_fattah</a></strong>`"]
        direction LR
        n84["`data_county.csv.gz<br/><br/><ul><li><code>missing_info: is_state_estimate</code></li><li><code>type_changed: geography</code></li></ul>`"]:::warn
        n85["`data_state.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
        n86["`data.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
    end
    subgraph vaers["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/vaers" target="_blank" rel="noreferrer">vaers</a></strong>`"]
        direction LR
    end
    subgraph wastewater_measles["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wastewater_measles" target="_blank" rel="noreferrer">wastewater_measles</a></strong>`"]
        direction LR
        n87["`data_county.csv.gz`"]:::pass
        n88["`data.csv.gz`"]:::pass
    end
    subgraph wastewater["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wastewater" target="_blank" rel="noreferrer">wastewater</a></strong>`"]
        direction LR
        n89["`data.csv.gz`"]:::pass
    end
    subgraph wisqars["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wisqars" target="_blank" rel="noreferrer">wisqars</a></strong>`"]
        direction LR
        n90["`data.csv.gz`"]:::pass
    end
    subgraph yrbss["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/yrbss" target="_blank" rel="noreferrer">yrbss</a></strong>`"]
        direction LR
        n91["`data_age_ethnicity.csv.gz<br/><br/><ul><li><code>missing_info: age, race_ethnicity</code></li></ul><br />Script Failed:<br />invalid connection`"]:::fail
        n92["`data_age_sex.csv.gz<br/><br/><ul><li><code>missing_info: age, sex</code></li></ul><br />Script Failed:<br />invalid connection`"]:::fail
        n93["`data_age.csv.gz<br/><br/><ul><li><code>missing_info: age</code></li></ul><br />Script Failed:<br />invalid connection`"]:::fail
    end
    subgraph bundle_antimicrobial_resistance["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_antimicrobial_resistance" target="_blank" rel="noreferrer">bundle_antimicrobial_resistance</a></strong>`"]
        direction LR
        n94["`resistance_by_agent.parquet`"]
        n95["`resistance_by_pattern.parquet`"]
    end
    subgraph bundle_cancer_screening["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_cancer_screening" target="_blank" rel="noreferrer">bundle_cancer_screening</a></strong>`"]
        direction LR
        n96["`cms_cancer_screening_by_race.parquet`"]
        n97["`cms_cancer_screening_by_sex.parquet`"]
        n98["`cms_cancer_screening_state.parquet`"]
        n99["`combined_cancer_screening.parquet`"]
        n100["`medicaid_cancer_screening.parquet`"]
    end
    subgraph bundle_childhood_immunizations["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_childhood_immunizations" target="_blank" rel="noreferrer">bundle_childhood_immunizations</a></strong>`"]
        direction LR
        n101["`nis_insurance.parquet`"]
        n102["`nis_overall.parquet`"]
        n103["`nis_urban.parquet`"]
        n104["`overall_rates_by_source.parquet`"]
        n105["`schoolvaxview_exemptions.parquet`"]
        n106["`schoolvaxview_overall.parquet`"]
        n107["`state_compare.parquet`"]
        n108["`wapo_vax_counties.parquet`"]
        n109["`wapo_vax_schools.parquet`"]
    end
    subgraph bundle_chronic_diseases["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_chronic_diseases" target="_blank" rel="noreferrer">bundle_chronic_diseases</a></strong>`"]
        direction LR
        n110["`brfss_prevalence_by_geography.parquet`"]
        n111["`epic_prevalence_by_geography_county_and_source.parquet`"]
        n112["`epic_prevalence_by_geography_year.parquet`"]
        n113["`prevalence_by_geography_and_source.csv`"]
        n114["`prevalence_by_geography_and_year_and_source.parquet`"]
    end
    subgraph bundle_county_access["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_county_access" target="_blank" rel="noreferrer">bundle_county_access</a></strong>`"]
        direction LR
        n115["`county_access.parquet`"]
    end
    subgraph bundle_county_chronic["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_county_chronic" target="_blank" rel="noreferrer">bundle_county_chronic</a></strong>`"]
        direction LR
        n116["`county_chronic.parquet`"]
    end
    subgraph bundle_enteric_diseases["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_enteric_diseases" target="_blank" rel="noreferrer">bundle_enteric_diseases</a></strong>`"]
        direction LR
        n117["`enteric_diseases.parquet`"]
        n118["`epic_diarrhea.parquet`"]
        n119["`resistance_by_agent.parquet`"]
        n120["`resistance_by_pattern.parquet`"]
    end
    subgraph bundle_injury_overdose["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_injury_overdose" target="_blank" rel="noreferrer">bundle_injury_overdose</a></strong>`"]
        direction LR
        n121["`county_opioid_by_source.parquet`"]
        n122["`deaths_cause_age_demographics.parquet`"]
        n123["`deaths_cause_age.parquet`"]
        n124["`firearms_by_demographics.parquet`"]
        n125["`firearms_by_geography_and_source_state_year.parquet`"]
        n126["`firearms_geography_source.parquet`"]
        n127["`google_dma.parquet`"]
        n128["`heat_by_geography_and_source_state_year.parquet`"]
        n129["`heat_risk.parquet`"]
        n130["`medicaid_injury_overdose.parquet`"]
        n131["`overdose_by_demographics.parquet`"]
        n132["`overdose_by_geography_and_source_county.parquet`"]
        n133["`overdose_by_geography_and_source_state_year.parquet`"]
        n134["`overdose_by_geography_and_source.parquet`"]
        n135["`overdose_deaths_county.parquet`"]
        n136["`overdose_deaths_state.parquet`"]
        n137["`state_opioid_by_source.parquet`"]
    end
    subgraph bundle_maternal_health["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_maternal_health" target="_blank" rel="noreferrer">bundle_maternal_health</a></strong>`"]
        direction LR
        n138["`maternal_county.parquet`"]
        n139["`maternal_mortality.parquet`"]
        n140["`maternal_state.parquet`"]
    end
    subgraph bundle_measles["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_measles" target="_blank" rel="noreferrer">bundle_measles</a></strong>`"]
        direction LR
        n141["`measles_cases_by_age.parquet`"]
        n142["`measles_county.parquet`"]
        n143["`measles_state.parquet`"]
    end
    subgraph bundle_preventative_services["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_preventative_services" target="_blank" rel="noreferrer">bundle_preventative_services</a></strong>`"]
        direction LR
        n144["`cms_preventative_services_by_race.parquet`"]
        n145["`cms_preventative_services_by_sex.parquet`"]
        n146["`cms_preventative_services_state.parquet`"]
        n147["`combined_preventative_services.parquet`"]
        n148["`medicaid_preventative_services.parquet`"]
    end
    subgraph bundle_respiratory["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_respiratory" target="_blank" rel="noreferrer">bundle_respiratory</a></strong>`"]
        direction LR
        n149["`covid_ed_visits_by_county.parquet`"]
        n150["`covid_overall_trends.parquet`"]
        n151["`covid_trends_by_age.parquet`"]
        n152["`flu_ed_visits_by_county.parquet`"]
        n153["`flu_overall_trends.parquet`"]
        n154["`flu_trends_by_age.parquet`"]
        n155["`other_measures_trends.parquet`"]
        n156["`pneumococcus_by_geography_year.parquet`"]
        n157["`pneumococcus_by_geography.parquet`"]
        n158["`pneumococcus_comparison.parquet`"]
        n159["`pneumococcus_serotype_trends.parquet`"]
        n160["`rsv_ed_visits_by_county.parquet`"]
        n161["`rsv_google_dma.parquet`"]
        n162["`rsv_overall_trends.parquet`"]
        n163["`rsv_positive_tests.parquet`"]
        n164["`rsv_testing_pct.parquet`"]
        n165["`rsv_trends_by_age.parquet`"]
    end
    subgraph bundle_youth_wellbeing["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_youth_wellbeing" target="_blank" rel="noreferrer">bundle_youth_wellbeing</a></strong>`"]
        direction LR
        n166["`chr_county.parquet`"]
        n167["`chr_state.parquet`"]
        n168["`epic_chronic_county_age.parquet`"]
        n169["`epic_chronic_state_age.parquet`"]
        n170["`epic_injury_state_age_month.parquet`"]
        n171["`epic_injury_state_age_year.parquet`"]
        n172["`immunizations_state_age_vaccine.parquet`"]
        n173["`medicaid_state_payer.parquet`"]
        n174["`neiss_diagnosis_age_sex_year.parquet`"]
        n175["`neiss_product_age_sex_year.parquet`"]
        n176["`nhtsa_county_age_sex.parquet`"]
        n177["`nhtsa_state_age_sex.parquet`"]
        n178["`noaa_heat_risk_county.parquet`"]
        n179["`noaa_heat_risk_state.parquet`"]
        n180["`wisqars_state_age_demographics.parquet`"]
        n181["`yrbss_state_age_demographics.parquet`"]
    end
    s0---s1["<strong><a href="https://data.cdc.gov/resource/qvzb-qs6p/" target="_blank" rel="noreferrer">Serotype Data for Invasive Pneumococcal Disease Cases by Age Group from Active Bacterial Core surveillance</a></strong>"]
    s1 --> n1
    s1 --> n2
    s2---s3["<strong><a href="https://pubmed.ncbi.nlm.nih.gov/39758745/" target="_blank" rel="noreferrer">Open Forum for Infectious Diseases</a></strong>"]
    s3 --> n2
    s4---s5["<strong><a href="https://data.hrsa.gov/topics/health-workforce/ahrf" target="_blank" rel="noreferrer">AHRF County-Level Data Files</a></strong>"]
    s5 --> n3
    s6---s7["<strong><a href="https://www.cdc.gov/beam/dashboard/" target="_blank" rel="noreferrer">BEAM (Bacteria, Enterics, Amoeba, and Mycotics) Dashboard</a></strong>"]
    s7 --> n4
    s8---s9["<strong><a href="https://data.cdc.gov/Behavioral-Risk-Factors/Behavioral-Risk-Factor-Surveillance-System-BRFSS-P/dttw-5yxu/about_data" target="_blank" rel="noreferrer">Behavioral Risk Factor Surveillance System (BRFSS) Prevalence Data (2011 to present)</a></strong>"]
    s9 --> n5
    s9 --> n6
    s10 --> n7
    s11 --> n8
    s12---s13["<strong><a href="https://api.census.gov/data.html" target="_blank" rel="noreferrer">Census API — ACS 5-Year Detailed Tables and Subject Tables</a></strong>"]
    s13 --> n9
    s14---s15["<strong><a href="https://www2.census.gov/geo/docs/reference/ua/2020_UA_COUNTY.xlsx" target="_blank" rel="noreferrer">2020 Census Urban Area to County Allocation File (XLSX)</a></strong>"]
    s15 --> n9
    s13 --> n10
    s13 --> n11
    s13 --> n12
    s13 --> n13
    s16---s17["<strong><a href="https://data.cms.gov/tools/mapping-medicare-disparities-by-population" target="_blank" rel="noreferrer">Mapping Medicare Disparities by Population Tool</a></strong>"]
    s17 --> n14
    s18 --> n14
    s17 --> n15
    s18 --> n15
    s17 --> n16
    s18 --> n16
    s19---s20["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/" target="_blank" rel="noreferrer">COVIDcast Epidata API</a></strong>"]
    s20 --> n17
    s21---s22["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/hospital-admissions.html" target="_blank" rel="noreferrer">COVIDcast > Hospital Admissions</a></strong>"]
    s22 --> n18
    s20 --> n18
    s24---s25["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/fluview.html" target="_blank" rel="noreferrer">FluView API</a></strong>"]
    s25 --> n19
    s26 --> n19
    s20 --> n19
    s20 --> n20
    s29 --> n21
    s29 --> n22
    s29 --> n23
    s29 --> n24
    s29 --> n25
    s29 --> n26
    s30 --> n27
    s29 --> n28
    s29 --> n29
    s29 --> n30
    s29 --> n31
    s29 --> n32
    s29 --> n33
    s29 --> n34
    s29 --> n35
    s31---s32["<strong><a href="https://github.com/DISSC-yale/gtrends_collection" target="_blank" rel="noreferrer">Yale Data-Intensive Social Sciences, Google Trends Collection Framework</a></strong>"]
    s32 --> n36
    s32 --> n37
    s32 --> n38
    s32 --> n39
    s33---s34["<strong><a href="https://apiv2.kinsainsights.com/api/v1/docs" target="_blank" rel="noreferrer">Kinsa Insights API - Signal Endpoint</a></strong>"]
    s34 --> n40
    s35 --> n41
    s36 --> n42
    s37 --> n43
    s37 --> n44
    s37 --> n45
    s38---s39["<strong><a href="https://data.medicaid.gov/datasets?theme%5B0%5D=Quality" target="_blank" rel="noreferrer">Medicaid.gov Open Data – Quality Measures datasets (2014–2023)</a></strong>"]
    s39 --> n46
    s40 --> n47
    s40 --> n48
    s40 --> n49
    s41---s42["<strong><a href="https://app.powerbigov.us/view?r=eyJrIjoiZmU5ZjA2ZDItNTU0MS00M2EzLWEyZmQtZmY3Y2RlZjdjYTdjIiwidCI6IjljZTcwODY5LTYwZGItNDRmZC1hYmU4LWQyNzY3MDc3ZmM4ZiJ9" target="_blank" rel="noreferrer">NARMS Now Interactive Dashboard - Human Data</a></strong>"]
    s42 --> n50
    s43 --> n50
    s44 --> n50
    s45 --> n50
    s42 --> n51
    s43 --> n51
    s44 --> n51
    s45 --> n51
    s42 --> n52
    s43 --> n52
    s44 --> n52
    s45 --> n52
    s42 --> n53
    s43 --> n53
    s44 --> n53
    s45 --> n53
    s42 --> n54
    s43 --> n54
    s44 --> n54
    s45 --> n54
    s46---s47["<strong><a href="https://nccrexplorer.ccdi.cancer.gov/application.html" target="_blank" rel="noreferrer">NCCR*Explorer: An interactive website for NCCR cancer statistics</a></strong>"]
    s47 --> n55
    s48 --> n56
    s49 --> n56
    s50 --> n57
    s48 --> n58
    s49 --> n58
    s51---s52["<strong><a href="https://www.cpsc.gov/cgibin/NEISSQuery/" target="_blank" rel="noreferrer">NEISS public query / archived data files</a></strong>"]
    s52 --> n59
    s52 --> n60
    s52 --> n61
    s52 --> n62
    s52 --> n63
    s52 --> n64
    s52 --> n65
    s52 --> n66
    s53---s54["<strong><a href="https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/" target="_blank" rel="noreferrer">NHTSA File Downloads — FARS National CSV archives</a></strong>"]
    s54 --> n67
    s54 --> n68
    s54 --> n69
    s54 --> n70
    s55 --> n71
    s56---s57["<strong><a href="https://www.cdc.gov/nis/about/index.html" target="_blank" rel="noreferrer">About the National Immunization Surveys (NIS)</a></strong>"]
    s57 --> n71
    s55 --> n72
    s57 --> n72
    s55 --> n73
    s57 --> n73
    s58 --> n74
    s59---s60["<strong><a href="https://www.wpc.ncep.noaa.gov/heatrisk/data.html" target="_blank" rel="noreferrer">HeatRisk GeoTIFF Archive and 7-Day Forecast</a></strong>"]
    s60 --> n75
    s60 --> n76
    s61---s62["<strong><a href="https://data.cdc.gov/resource/3cxc-4k8q" target="_blank" rel="noreferrer">Percent Positivity of Respiratory Syncytial Virus Nucleic Acid Amplification Tests by HHS Region, National Respiratory and Enteric Virus Surveillance System</a></strong>"]
    s62 --> n77
    s63 --> n77
    s64---s65["<strong><a href="https://data.cdc.gov/resource/rdmq-nq56" target="_blank" rel="noreferrer">National Syndromic Surveillance Program</a></strong>"]
    s65 --> n78
    s66---s67["<strong><a href="https://healthdata.gov/CDC/Weekly-Rates-of-Laboratory-Confirmed-COVID-19-Hosp/gk5r-vjtt/about_data" target="_blank" rel="noreferrer">Weekly Rates of Laboratory-Confirmed COVID-19 Hospitalizations from the COVID-NET Surveillance System</a></strong>"]
    s67 --> n79
    s66---s68["<strong><a href="https://data.cdc.gov/Public-Health-Surveillance/Weekly-Rates-of-Laboratory-Confirmed-RSV-Hospitali/29hc-w46k/about_data" target="_blank" rel="noreferrer">Weekly Rates of Laboratory-Confirmed RSV Hospitalizations from the RSV-NET Surveillance System</a></strong>"]
    s68 --> n79
    s66---s69["<strong><a href="https://data.cdc.gov/Public-Health-Surveillance/Rates-of-Laboratory-Confirmed-RSV-COVID-19-and-Flu/kvib-3txy/about_data" target="_blank" rel="noreferrer">Rates of Laboratory-Confirmed RSV, COVID-19, and Flu Hospitalizations from the RESP-NET Surveillance Systems</a></strong>"]
    s69 --> n79
    s70 --> n80
    s71 --> n80
    s70 --> n81
    s72---s73["<strong><a href="https://data.cdc.gov/Vaccinations/Vaccination-Coverage-and-Exemptions-among-Kinderga/ijqb-a7ye/about_data" target="_blank" rel="noreferrer">Vaccination Coverage and Exemptions among Kindergartners</a></strong>"]
    s73 --> n82
    s73 --> n83
    s74 --> n84
    s74 --> n85
    s74 --> n86
    s75 --> n87
    s75 --> n88
    s76---s77["<strong><a href="https://data.cdc.gov/Public-Health-Surveillance/CDC-Wastewater-Viral-Activity-Level-for-SARS-CoV-2/atcp-73re/" target="_blank" rel="noreferrer">CDC Wastewater Viral Activity Level for SARS-CoV-2, Influenza A and RSV</a></strong>"]
    s77 --> n89
    s78---s79["<strong><a href="https://wisqars.cdc.gov/reports/?o=MORT&i=8&m=20810&s=0&r=0&ry=2&y1=2018&y2=2023&a=ALL&g1=0&g2=199&a1=0&a2=199&r1=MECH&r2=AGEGP&r3=STATE&r4=YEAR&r5=NONE&r6=NONE&g=00&e=0&yp=65&me=0&t=0" target="_blank" rel="noreferrer">Fatal Injury Report</a></strong>"]
    s79 --> n90
    s80 --> n91
    s80 --> n92
    s80 --> n93
    n52 --> bundle_antimicrobial_resistance
    n53 --> bundle_antimicrobial_resistance
    n54 --> bundle_antimicrobial_resistance
    n50 --> bundle_antimicrobial_resistance
    n51 --> bundle_antimicrobial_resistance
    n16 --> bundle_cancer_screening
    n15 --> bundle_cancer_screening
    n14 --> bundle_cancer_screening
    n46 --> bundle_cancer_screening
    n83 --> bundle_childhood_immunizations
    n82 --> bundle_childhood_immunizations
    n73 --> bundle_childhood_immunizations
    n72 --> bundle_childhood_immunizations
    n71 --> bundle_childhood_immunizations
    n80 --> bundle_childhood_immunizations
    n81 --> bundle_childhood_immunizations
    n5 --> bundle_chronic_diseases
    n24 --> bundle_chronic_diseases
    n22 --> bundle_chronic_diseases
    n16 --> bundle_chronic_diseases
    n74 --> bundle_enteric_diseases
    n4 --> bundle_enteric_diseases
    n52 --> bundle_enteric_diseases
    n54 --> bundle_enteric_diseases
    n50 --> bundle_enteric_diseases
    n51 --> bundle_enteric_diseases
    n53 --> bundle_enteric_diseases
    n25 --> bundle_enteric_diseases
    n90 --> bundle_injury_overdose
    n16 --> bundle_injury_overdose
    n58 --> bundle_injury_overdose
    n56 --> bundle_injury_overdose
    n38 --> bundle_injury_overdose
    n30 --> bundle_injury_overdose
    n31 --> bundle_injury_overdose
    n46 --> bundle_injury_overdose
    n36 --> bundle_injury_overdose
    n10 --> bundle_maternal_health
    n9 --> bundle_maternal_health
    n46 --> bundle_maternal_health
    n8 --> bundle_maternal_health
    n88 --> bundle_measles
    n86 --> bundle_measles
    n44 --> bundle_measles
    n48 --> bundle_measles
    n42 --> bundle_measles
    n74 --> bundle_measles
    n43 --> bundle_measles
    n47 --> bundle_measles
    n84 --> bundle_measles
    n80 --> bundle_measles
    n87 --> bundle_measles
    n81 --> bundle_measles
    n41 --> bundle_measles
    n46 --> bundle_preventative_services
    n16 --> bundle_preventative_services
    n15 --> bundle_preventative_services
    n14 --> bundle_preventative_services
    n35 --> bundle_respiratory
    n32 --> bundle_respiratory
    n39 --> bundle_respiratory
    n37 --> bundle_respiratory
    n78 --> bundle_respiratory
    n79 --> bundle_respiratory
    n89 --> bundle_respiratory
    n17 --> bundle_respiratory
    n18 --> bundle_respiratory
    n20 --> bundle_respiratory
    n19 --> bundle_respiratory
    n40 --> bundle_respiratory
    n77 --> bundle_respiratory
    n1 --> bundle_respiratory
    n2 --> bundle_respiratory
    n7 --> bundle_respiratory
    n57 --> bundle_respiratory
    n74 --> bundle_respiratory
    n90 --> bundle_youth_wellbeing
    n67 --> bundle_youth_wellbeing
    n93 --> bundle_youth_wellbeing
    n92 --> bundle_youth_wellbeing
    n91 --> bundle_youth_wellbeing
    n31 --> bundle_youth_wellbeing
    n30 --> bundle_youth_wellbeing
    n24 --> bundle_youth_wellbeing
    n22 --> bundle_youth_wellbeing
    n46 --> bundle_youth_wellbeing
    n76 --> bundle_youth_wellbeing
    n75 --> bundle_youth_wellbeing
    n62 --> bundle_youth_wellbeing
    n66 --> bundle_youth_wellbeing
    n60 --> bundle_youth_wellbeing
    n64 --> bundle_youth_wellbeing
```
