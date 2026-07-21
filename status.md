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
    subgraph _["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/_" target="_blank" rel="noreferrer">_</a></strong>`"]
        direction LR
    end
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
    subgraph beam["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/beam" target="_blank" rel="noreferrer">beam</a></strong>`"]
        direction LR
        n5["`data.csv.gz`"]:::pass
    end
    subgraph brfss["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/brfss" target="_blank" rel="noreferrer">brfss</a></strong>`"]
        direction LR
        n6["`data_survey.csv.gz`"]:::pass
        n7["`data.csv.gz<br/><br/><ul><li><code>type_changed: pct_depression_sample_size, pct_diabetes_sample_size, pct_heavy_drink_sample_size, pct_obesity_sample_size</code></li></ul>`"]:::warn
    end
    subgraph cdc_cfa_rt["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/cdc_cfa_rt" target="_blank" rel="noreferrer">cdc_cfa_rt</a></strong>`"]
        direction LR
        n8["`data.csv.gz<br/><br/><ul><li><code>geography_dropped</code></li><li><code>type_changed: cdc_rt_covid, cdc_rt_covid_lower, cdc_rt_covid_upper, cdc_rt_covid_p_growing, cdc_rt_flu, cdc_rt_flu_lower, cdc_rt_flu_upper, cdc_rt_flu_p_growing, cdc_rt_rsv, cdc_rt_rsv_lower, cdc_rt_rsv_upper, cdc_rt_rsv_p_growing</code></li></ul>`"]:::warn
    end
    subgraph cdc_vssr["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/cdc_vssr" target="_blank" rel="noreferrer">cdc_vssr</a></strong>`"]
        direction LR
        n9["`data.csv.gz`"]:::pass
    end
    subgraph census["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/census" target="_blank" rel="noreferrer">census</a></strong>`"]
        direction LR
        n10["`data_county.csv.gz`"]:::pass
        n11["`data_state.csv.gz`"]:::pass
        n12["`data_zcta_2019_2020.csv-MWMJ0G3P8D.gz<br/><br/><ul><li><code>missing_info: geography_zcta</code></li></ul>`"]:::warn
        n13["`data_zcta_2019_2020.csv.gz<br/><br/><ul><li><code>missing_info: geography_zcta</code></li></ul>`"]:::warn
        n14["`data_zcta_2021_2022.csv.gz<br/><br/><ul><li><code>missing_info: geography_zcta</code></li></ul>`"]:::warn
        n15["`data_zcta_2023_2024.csv.gz<br/><br/><ul><li><code>missing_info: geography_zcta</code></li></ul>`"]:::warn
    end
    subgraph cms_mmd["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/cms_mmd" target="_blank" rel="noreferrer">cms_mmd</a></strong>`"]
        direction LR
        n16["`data_state_county_age_by_race.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
        n17["`data_state_county_age_by_sex.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
        n18["`data_state_county_age.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
    end
    subgraph county_health_rankings["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/county_health_rankings" target="_blank" rel="noreferrer">county_health_rankings</a></strong>`"]
        direction LR
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
    subgraph epic_diarrhea["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_diarrhea" target="_blank" rel="noreferrer">epic_diarrhea</a></strong>`"]
        direction LR
        n27["`data_weekly.csv.gz`"]:::pass
        n28["`weekly_tests.csv.gz`"]:::pass
    end
    subgraph epic_health_alerts["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_health_alerts" target="_blank" rel="noreferrer">epic_health_alerts</a></strong>`"]
        direction LR
        n29["`data.csv.gz<br /><br />Script Failed:<br />`"]:::fail
    end
    subgraph epic_hepb_vax["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_hepb_vax" target="_blank" rel="noreferrer">epic_hepb_vax</a></strong>`"]
        direction LR
        n30["`data.csv.gz`"]:::pass
    end
    subgraph epic_injury["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_injury" target="_blank" rel="noreferrer">epic_injury</a></strong>`"]
        direction LR
        n31["`heat_year_county.csv.gz<br/><br/><ul><li><code>missing_info: geography_name</code></li></ul>`"]:::warn
        n32["`monthly_injury.csv.gz`"]:::pass
        n33["`yearly_injury.csv.gz`"]:::pass
    end
    subgraph epic_resp_infections["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_resp_infections" target="_blank" rel="noreferrer">epic_resp_infections</a></strong>`"]
        direction LR
        n34["`monthly_tests.csv.gz`"]:::pass
        n35["`no_geo.csv.gz`"]:::pass
        n36["`quarterly_gas.csv.gz<br/><br/><ul><li><code>missing_info: state_name</code></li></ul>`"]:::warn
        n37["`weekly.csv.gz`"]:::pass
    end
    subgraph gtrends["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/gtrends" target="_blank" rel="noreferrer">gtrends</a></strong>`"]
        direction LR
        n38["`data_dma_year.csv.gz`"]:::pass
        n39["`data_dma.csv.gz`"]:::pass
        n40["`data_year.csv.gz`"]:::pass
        n41["`data.csv.gz`"]:::pass
    end
    subgraph heat_risk["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/heat_risk" target="_blank" rel="noreferrer">heat_risk</a></strong>`"]
        direction LR
    end
    subgraph kinsa_ili["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/kinsa_ili" target="_blank" rel="noreferrer">kinsa_ili</a></strong>`"]
        direction LR
        n42["`data.csv-MWMJ0G3P8D.gz<br /><br />Script Failed:<br />Kinsa credentials not found. Set KINSA_EMAIL and KINSA_PASSWORD.`"]:::fail
        n43["`data.csv.gz<br /><br />Script Failed:<br />Kinsa credentials not found. Set KINSA_EMAIL and KINSA_PASSWORD.`"]:::fail
    end
    subgraph measles_age_cdc2["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/measles_age_cdc2" target="_blank" rel="noreferrer">measles_age_cdc2</a></strong>`"]
        direction LR
        n44["`data.csv.gz<br/><br/><ul><li><code>missing_info: year, week</code></li></ul>`"]:::warn
    end
    subgraph measles_cdc["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/measles_cdc" target="_blank" rel="noreferrer">measles_cdc</a></strong>`"]
        direction LR
        n45["`data.csv.gz`"]:::pass
    end
    subgraph measles_jhu["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/measles_jhu" target="_blank" rel="noreferrer">measles_jhu</a></strong>`"]
        direction LR
        n46["`data_county.csv.gz<br /><br />Script Failed:<br />`"]:::fail
        n47["`data_state.csv.gz<br /><br />Script Failed:<br />`"]:::fail
        n48["`data.csv.gz<br /><br />Script Failed:<br />`"]:::fail
    end
    subgraph medicaid_quality["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/medicaid_quality" target="_blank" rel="noreferrer">medicaid_quality</a></strong>`"]
        direction LR
        n49["`data.csv.gz<br/><br/><ul><li><code>missing_info: geography_level, age, sex, race_ethnicity, payer, domain, medicaid_awc_ch_pct_25, medicaid_awc_ch_pct_75, medicaid_lbw_ch_pct_25, medicaid_lbw_ch_pct_75, medicaid_ima_ch_pct_25, medicaid_ima_ch_pct_75, medicaid_aba_ad_pct_25, medicaid_aba_ad_pct_75, medicaid_w34_ch_pct_25, medicaid_w34_ch_pct_75, medicaid_ldl_ad_pct_25, medicaid_ldl_ad_pct_75, medicaid_pdent_ch_pct_25, medicaid_pdent_ch_pct_75, medicaid_amm_ad_pct_25, medicaid_amm_ad_pct_75, medicaid_amb_ch_pct_25, medicaid_amb_ch_pct_75, medicaid_hpv_ch_pct_25, medicaid_hpv_ch_pct_75, medicaid_fuh_ch_30d_pct_25, medicaid_fuh_ch_30d_pct_75, medicaid_fuh_ch_7d_pct_25, medicaid_fuh_ch_7d_pct_75, medicaid_fpc_ch_pct_25, medicaid_fpc_ch_pct_75, medicaid_chl_ch_pct_25, medicaid_chl_ch_pct_75, medicaid_cap_ch_pct_25, medicaid_cap_ch_pct_75, medicaid_fuh_ad_30d_pct_25, medicaid_fuh_ad_30d_pct_75, medicaid_bcs_ad_pct_25, medicaid_bcs_ad_pct_75, medicaid_ccs_ad_pct_25, medicaid_ccs_ad_pct_75, medicaid_mma_ch_pct_25, medicaid_mma_ch_pct_75, medicaid_wcc_ch_pct_25, medicaid_wcc_ch_pct_75, medicaid_chl_ad_pct_25, medicaid_chl_ad_pct_75, medicaid_mpm_ad_pct_25, medicaid_mpm_ad_pct_75, medicaid_cis_ch_pct_25, medicaid_cis_ch_pct_75, medicaid_add_ch_cont_pct_25, medicaid_add_ch_cont_pct_75, medicaid_ppc_ad_pct_25, medicaid_ppc_ad_pct_75, medicaid_ppc_ch_pct_25, medicaid_ppc_ch_pct_75, medicaid_add_ch_init_pct_25, medicaid_add_ch_init_pct_75, medicaid_w15_ch_pct_25, medicaid_w15_ch_pct_75, medicaid_ha1c_ad_pct_25, medicaid_ha1c_ad_pct_75, medicaid_tdent_ch_pct_25, medicaid_tdent_ch_pct_75, medicaid_fuh_ad_7d_pct_25, medicaid_fuh_ad_7d_pct_75, medicaid_msc_ad_pct_25, medicaid_msc_ad_pct_75, medicaid_iet_ad_pct_25, medicaid_iet_ad_pct_75, medicaid_seal_ch_pct_25, medicaid_seal_ch_pct_75, medicaid_saa_ad_pct_25, medicaid_saa_ad_pct_75, medicaid_dev_ch_pct_25, medicaid_dev_ch_pct_75, medicaid_apc_ch_pct_25, medicaid_apc_ch_pct_75, medicaid_add_ch_30d_pct_25, medicaid_add_ch_30d_pct_75, medicaid_cbp_ad_pct_25, medicaid_cbp_ad_pct_75, medicaid_ssd_ad_pct_25, medicaid_ssd_ad_pct_75, medicaid_pqi08_ad_pct_25, medicaid_pqi08_ad_pct_75, medicaid_pqi01_ad_pct_25, medicaid_pqi01_ad_pct_75, medicaid_pqi15_ad_pct_25, medicaid_pqi15_ad_pct_75, medicaid_pqi05_ad_pct_25, medicaid_pqi05_ad_pct_75, medicaid_hpc_ad_pct_25, medicaid_hpc_ad_pct_75, medicaid_app_ch_pct_25, medicaid_app_ch_pct_75, medicaid_amr_ch_pct_25, medicaid_amr_ch_pct_75, medicaid_ccw_ch_pct_25, medicaid_ccw_ch_pct_75, medicaid_ccp_ch_pct_25, medicaid_ccp_ch_pct_75, medicaid_fua_fum_ad_7d_pct_25, medicaid_fua_fum_ad_7d_pct_75, medicaid_fua_fum_ad_30d_pct_25, medicaid_fua_fum_ad_30d_pct_75, medicaid_amr_ad_pct_25, medicaid_amr_ad_pct_75, medicaid_ccp_ad_pct_25, medicaid_ccp_ad_pct_75, medicaid_pcr_ad_pct_25, medicaid_pcr_ad_pct_75, medicaid_ohd_ad_pct_25, medicaid_ohd_ad_pct_75, medicaid_fua_ad_7d_pct_25, medicaid_fua_ad_7d_pct_75, medicaid_fua_ad_30d_pct_25, medicaid_fua_ad_30d_pct_75, medicaid_fum_ad_7d_pct_25, medicaid_fum_ad_7d_pct_75, medicaid_fum_ad_30d_pct_25, medicaid_fum_ad_30d_pct_75, medicaid_apm_ch_gluc_pct_25, medicaid_apm_ch_gluc_pct_75, medicaid_apm_ch_chol_pct_25, medicaid_apm_ch_chol_pct_75, medicaid_apm_ch_gluc_chol_pct_25, medicaid_apm_ch_gluc_chol_pct_75, medicaid_cob_ad_pct_25, medicaid_cob_ad_pct_75, medicaid_ccw_ad_pct_25, medicaid_ccw_ad_pct_75, medicaid_fva_ad_pct_25, medicaid_fva_ad_pct_75, medicaid_ncidds_ad_pct_25, medicaid_ncidds_ad_pct_75, medicaid_sfm_ch_pct_25, medicaid_sfm_ch_pct_75, medicaid_lrcd_ch_pct_25, medicaid_lrcd_ch_pct_75, medicaid_wcv_ch_pct_25, medicaid_wcv_ch_pct_75, medicaid_w30_ch_pct_25, medicaid_w30_ch_pct_75, medicaid_oud_ad_pct_25, medicaid_oud_ad_pct_75, medicaid_fua_ch_30d_pct_25, medicaid_fua_ch_30d_pct_75, medicaid_fum_ch_7d_pct_25, medicaid_fum_ch_7d_pct_75, medicaid_fum_ch_30d_pct_25, medicaid_fum_ch_30d_pct_75, medicaid_oev_ch_pct_25, medicaid_oev_ch_pct_75, medicaid_tfl_ch_pct_25, medicaid_tfl_ch_pct_75, medicaid_aab_ad_pct_25, medicaid_aab_ad_pct_75, medicaid_fua_ch_7d_pct_25, medicaid_fua_ch_7d_pct_75, medicaid_aab_ch_pct_25, medicaid_aab_ch_pct_75, medicaid_cpc_ch_pct_25, medicaid_cpc_ch_pct_75, medicaid_lsc_ch_pct_25, medicaid_lsc_ch_pct_75, medicaid_amm_ad_cont_pct_25, medicaid_amm_ad_cont_pct_75, medicaid_hbd_ad_pct_25, medicaid_hbd_ad_pct_75, medicaid_cpa_ad_pct_25, medicaid_cpa_ad_pct_75, medicaid_col_ad_pct_25, medicaid_col_ad_pct_75</code></li></ul>`"]:::warn
    end
    subgraph mmr_healthmap["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/mmr_healthmap" target="_blank" rel="noreferrer">mmr_healthmap</a></strong>`"]
        direction LR
        n50["`data_county.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
        n51["`data_state.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
        n52["`data_zcta.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
    end
    subgraph narms["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/narms" target="_blank" rel="noreferrer">narms</a></strong>`"]
        direction LR
        n53["`data_animal_pathogen.csv.gz<br/><br/><ul><li><code>missing_info: genus, host_species, collection_source, antimicrobial</code></li></ul><br />Script Failed:<br />Sheet '2017-2021_data' not found`"]:::fail
        n54["`data_food_animals.csv.gz<br/><br/><ul><li><code>missing_info: source_program, source_type, genus, species, serotype, host_species, antimicrobial</code></li></ul><br />Script Failed:<br />Sheet '2017-2021_data' not found`"]:::fail
        n55["`data_resistance_agent.csv.gz<br/><br/><ul><li><code>missing_info: genus, species_serotype, antimicrobial_class, antimicrobial_agent, test_method</code></li></ul><br />Script Failed:<br />Sheet '2017-2021_data' not found`"]:::fail
        n56["`data_resistance_pattern.csv.gz<br/><br/><ul><li><code>missing_info: genus, species_serotype, pattern, test_method</code></li></ul><br />Script Failed:<br />Sheet '2017-2021_data' not found`"]:::fail
        n57["`data_retail_meats.csv<br/><br/><ul><li><code>not_compressed</code></li><li><code>missing_info: genus, species, serotype, meat_source, antimicrobial</code></li></ul><br />Script Failed:<br />Sheet '2017-2021_data' not found`"]:::fail
        n58["`data_retail_meats.csv.gz<br/><br/><ul><li><code>missing_info: genus, species, serotype, meat_source, antimicrobial</code></li></ul><br />Script Failed:<br />Sheet '2017-2021_data' not found`"]:::fail
    end
    subgraph nccr["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nccr" target="_blank" rel="noreferrer">nccr</a></strong>`"]
        direction LR
        n59["`data.csv.gz<br/><br/><ul><li><code>missing_info: age, sex, race_ethnicity</code></li></ul>`"]:::warn
    end
    subgraph nchs_mortality["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nchs_mortality" target="_blank" rel="noreferrer">nchs_mortality</a></strong>`"]
        direction LR
        n60["`data_county.csv.gz<br /><br />Script Failed:<br />In argument: 'N_deaths = sum('Data Value')'.`"]:::fail
        n61["`data_state_21_causes.csv.gz<br /><br />Script Failed:<br />In argument: 'N_deaths = sum('Data Value')'.`"]:::fail
        n62["`data.csv.gz<br /><br />Script Failed:<br />In argument: 'N_deaths = sum('Data Value')'.`"]:::fail
    end
    subgraph neiss["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/neiss" target="_blank" rel="noreferrer">neiss</a></strong>`"]
        direction LR
        n63["`data_agegroup_diagnosis_rate.csv.gz`"]:::pass
        n64["`data_agegroup_diagnosis.csv.gz`"]:::pass
        n65["`data_agegroup_product_rate.csv.gz`"]:::pass
        n66["`data_agegroup_product.csv.gz`"]:::pass
        n67["`data_infant_diagnosis_rate.csv.gz`"]:::pass
        n68["`data_infant_diagnosis.csv.gz`"]:::pass
        n69["`data_infant_product_rate.csv.gz`"]:::pass
        n70["`data_infant_product.csv.gz`"]:::pass
    end
    subgraph nhtsa_crash["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nhtsa_crash" target="_blank" rel="noreferrer">nhtsa_crash</a></strong>`"]
        direction LR
        n71["`data_age_sex.csv.gz<br/><br/><ul><li><code>geography_dropped</code></li><li><code>missing_info: age, sex</code></li></ul>`"]:::warn
        n72["`data_crash_type.csv.gz<br/><br/><ul><li><code>missing_info: age, sex</code></li></ul>`"]:::warn
        n73["`data_person_type.csv.gz<br/><br/><ul><li><code>geography_dropped</code></li><li><code>missing_info: person_type</code></li></ul>`"]:::warn
        n74["`data.csv.gz<br/><br/><ul><li><code>geography_dropped</code></li></ul>`"]:::warn
    end
    subgraph nis["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nis" target="_blank" rel="noreferrer">nis</a></strong>`"]
        direction LR
        n75["`data_insurance.csv.gz`"]:::pass
        n76["`data_urban.csv.gz`"]:::pass
        n77["`data.csv.gz`"]:::pass
    end
    subgraph nnds["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nnds" target="_blank" rel="noreferrer">nnds</a></strong>`"]
        direction LR
        n78["`data.csv.gz<br/><br/><ul><li><code>missing_info: mmwr_year, mmwr_week, anthrax, plague, rabies_human, rubella_congenital_syndrome</code></li></ul>`"]:::warn
    end
    subgraph noaa_heat_risk["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/noaa_heat_risk" target="_blank" rel="noreferrer">noaa_heat_risk</a></strong>`"]
        direction LR
        n79["`data_county.csv-MWMJ0G3P8D.gz<br /><br />Script Failed:<br />`"]:::fail
        n80["`data_county.csv.gz<br /><br />Script Failed:<br />`"]:::fail
        n81["`data_state.csv-MWMJ0G3P8D.gz<br /><br />Script Failed:<br />`"]:::fail
        n82["`data_state.csv.gz<br /><br />Script Failed:<br />`"]:::fail
    end
    subgraph NREVSS["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/NREVSS" target="_blank" rel="noreferrer">NREVSS</a></strong>`"]
        direction LR
        n83["`data.csv.gz<br /><br />Script Failed:<br />character string is not in a standard unambiguous format`"]:::fail
    end
    subgraph nssp["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nssp" target="_blank" rel="noreferrer">nssp</a></strong>`"]
        direction LR
        n84["`data.csv.gz`"]:::pass
    end
    subgraph 53["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/53" target="_blank" rel="noreferrer">53</a></strong>`"]
        direction LR
    end
    subgraph respnet["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/respnet" target="_blank" rel="noreferrer">respnet</a></strong>`"]
        direction LR
        n85["`data.csv.gz`"]:::pass
    end
    subgraph schoolvax_washpost["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/schoolvax_washpost" target="_blank" rel="noreferrer">schoolvax_washpost</a></strong>`"]
        direction LR
        n86["`data_counties.csv.gz<br/><br/><ul><li><code>geography_dropped</code></li></ul>`"]:::warn
        n87["`data_schools.csv.gz`"]:::pass
    end
    subgraph schoolvaxview["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/schoolvaxview" target="_blank" rel="noreferrer">schoolvaxview</a></strong>`"]
        direction LR
        n88["`data_exemptions.csv.gz`"]:::pass
        n89["`data.csv.gz`"]:::pass
    end
    subgraph vaccine_exemptions_fattah["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/vaccine_exemptions_fattah" target="_blank" rel="noreferrer">vaccine_exemptions_fattah</a></strong>`"]
        direction LR
        n90["`data_county.csv.gz<br/><br/><ul><li><code>missing_info: is_state_estimate</code></li><li><code>type_changed: geography</code></li></ul>`"]:::warn
        n91["`data_state.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
        n92["`data.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
    end
    subgraph vaers["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/vaers" target="_blank" rel="noreferrer">vaers</a></strong>`"]
        direction LR
    end
    subgraph wastewater_measles["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wastewater_measles" target="_blank" rel="noreferrer">wastewater_measles</a></strong>`"]
        direction LR
        n93["`data_county.csv.gz`"]:::pass
        n94["`data.csv.gz`"]:::pass
    end
    subgraph wastewater["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wastewater" target="_blank" rel="noreferrer">wastewater</a></strong>`"]
        direction LR
        n95["`data.csv.gz`"]:::pass
    end
    subgraph wisqars["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wisqars" target="_blank" rel="noreferrer">wisqars</a></strong>`"]
        direction LR
        n96["`data.csv.gz`"]:::pass
    end
    subgraph yrbss["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/yrbss" target="_blank" rel="noreferrer">yrbss</a></strong>`"]
        direction LR
        n97["`data_age_ethnicity.csv.gz<br/><br/><ul><li><code>missing_info: age, race_ethnicity</code></li></ul><br />Script Failed:<br />`"]:::fail
        n98["`data_age_sex.csv.gz<br/><br/><ul><li><code>missing_info: age, sex</code></li></ul><br />Script Failed:<br />`"]:::fail
        n99["`data_age.csv.gz<br/><br/><ul><li><code>missing_info: age</code></li></ul><br />Script Failed:<br />`"]:::fail
    end
    subgraph bundle_antimicrobial_resistance["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_antimicrobial_resistance" target="_blank" rel="noreferrer">bundle_antimicrobial_resistance</a></strong>`"]
        direction LR
        n100["`resistance_by_agent.parquet`"]
        n101["`resistance_by_pattern.parquet`"]
    end
    subgraph bundle_cancer_screening["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_cancer_screening" target="_blank" rel="noreferrer">bundle_cancer_screening</a></strong>`"]
        direction LR
        n102["`cms_cancer_screening_by_race.parquet`"]
        n103["`cms_cancer_screening_by_sex.parquet`"]
        n104["`cms_cancer_screening_state.parquet`"]
        n105["`combined_cancer_screening.parquet`"]
        n106["`medicaid_cancer_screening.parquet`"]
    end
    subgraph bundle_childhood_immunizations["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_childhood_immunizations" target="_blank" rel="noreferrer">bundle_childhood_immunizations</a></strong>`"]
        direction LR
        n107["`nis_insurance.parquet`"]
        n108["`nis_overall.parquet`"]
        n109["`nis_urban.parquet`"]
        n110["`overall_rates_by_source.parquet`"]
        n111["`schoolvaxview_exemptions.parquet`"]
        n112["`schoolvaxview_overall.parquet`"]
        n113["`state_compare.parquet`"]
        n114["`wapo_vax_counties.parquet`"]
        n115["`wapo_vax_schools.parquet`"]
    end
    subgraph bundle_chronic_diseases["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_chronic_diseases" target="_blank" rel="noreferrer">bundle_chronic_diseases</a></strong>`"]
        direction LR
        n116["`brfss_prevalence_by_geography.parquet`"]
        n117["`county_opioid_by_source.parquet`"]
        n118["`deaths_cause_age.parquet`"]
        n119["`epic_prevalence_by_geography_county_and_source.parquet`"]
        n120["`epic_prevalence_by_geography_county.parquet`"]
        n121["`epic_prevalence_by_geography_year.parquet`"]
        n122["`epic_prevalence_by_geography.parquet`"]
        n123["`overdose_by_geography_and_source_county.parquet`"]
        n124["`overdose_by_geography_and_source.parquet`"]
        n125["`overdose_deaths_county.parquet`"]
        n126["`overdose_deaths_state.parquet`"]
        n127["`prevalence_by_geography_and_source.csv`"]
        n128["`prevalence_by_geography_and_source.parquet`"]
        n129["`prevalence_by_geography_and_year_and_source.parquet`"]
        n130["`prevalence_by_geography_year_and_source.parquet`"]
    end
    subgraph bundle_county_access["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_county_access" target="_blank" rel="noreferrer">bundle_county_access</a></strong>`"]
        direction LR
        n131["`county_access.parquet`"]
    end
    subgraph bundle_county_chronic["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_county_chronic" target="_blank" rel="noreferrer">bundle_county_chronic</a></strong>`"]
        direction LR
        n132["`county_chronic.parquet`"]
    end
    subgraph bundle_enteric_diseases["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_enteric_diseases" target="_blank" rel="noreferrer">bundle_enteric_diseases</a></strong>`"]
        direction LR
        n133["`enteric_diseases.parquet`"]
        n134["`resistance_by_agent.parquet`"]
        n135["`resistance_by_pattern.parquet`"]
    end
    subgraph bundle_injury_overdose["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_injury_overdose" target="_blank" rel="noreferrer">bundle_injury_overdose</a></strong>`"]
        direction LR
        n136["`brfss_prevalence_by_geography.parquet`"]
        n137["`county_opioid_by_source.parquet`"]
        n138["`deaths_cause_age_demographics.parquet`"]
        n139["`deaths_cause_age.parquet`"]
        n140["`epic_prevalence_by_geography_year.parquet`"]
        n141["`firearms_by_demographics.parquet`"]
        n142["`firearms_by_geography_and_source_state_year.parquet`"]
        n143["`firearms_geography_source.parquet`"]
        n144["`google_dma.parquet`"]
        n145["`heat_by_geography_and_source_state_year.parquet`"]
        n146["`heat_related_geography_source.parquet`"]
        n147["`heat_risk.parquet`"]
        n148["`medicaid_injury_overdose.parquet`"]
        n149["`overdose_by_demographics.parquet`"]
        n150["`overdose_by_geography_and_source_county.parquet`"]
        n151["`overdose_by_geography_and_source_state_year.parquet`"]
        n152["`overdose_by_geography_and_source.parquet`"]
        n153["`overdose_deaths_county.parquet`"]
        n154["`overdose_deaths_state.parquet`"]
        n155["`state_opioid_by_source.parquet`"]
    end
    subgraph bundle_maternal_health["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_maternal_health" target="_blank" rel="noreferrer">bundle_maternal_health</a></strong>`"]
        direction LR
        n156["`maternal_county.parquet`"]
        n157["`maternal_mortality.parquet`"]
        n158["`maternal_state.parquet`"]
    end
    subgraph bundle_measles["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_measles" target="_blank" rel="noreferrer">bundle_measles</a></strong>`"]
        direction LR
        n159["`measles_cases_by_age.parquet`"]
        n160["`measles_county.parquet`"]
        n161["`measles_imported_indigenous.parquet`"]
        n162["`measles_state.parquet`"]
    end
    subgraph bundle_preventative_services["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_preventative_services" target="_blank" rel="noreferrer">bundle_preventative_services</a></strong>`"]
        direction LR
        n163["`cms_preventative_services_by_race.parquet`"]
        n164["`cms_preventative_services_by_sex.parquet`"]
        n165["`cms_preventative_services_state.parquet`"]
        n166["`combined_preventative_services.parquet`"]
        n167["`medicaid_preventative_services.parquet`"]
    end
    subgraph bundle_respiratory["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_respiratory" target="_blank" rel="noreferrer">bundle_respiratory</a></strong>`"]
        direction LR
        n168["`covid_ed_visits_by_county.parquet`"]
        n169["`covid_overall_trends.parquet`"]
        n170["`covid_trends_by_age.parquet`"]
        n171["`flu_ed_visits_by_county.parquet`"]
        n172["`flu_overall_trends.parquet`"]
        n173["`flu_trends_by_age.parquet`"]
        n174["`other_measures_trends.parquet`"]
        n175["`pneumococcus_by_geography_year.parquet`"]
        n176["`pneumococcus_by_geography.parquet`"]
        n177["`pneumococcus_comparison.parquet`"]
        n178["`pneumococcus_serotype_trends.parquet`"]
        n179["`rsv_ed_visits_by_county.parquet`"]
        n180["`rsv_google_dma.parquet`"]
        n181["`rsv_overall_trends.parquet`"]
        n182["`rsv_positive_tests.parquet`"]
        n183["`rsv_testing_pct.parquet`"]
        n184["`rsv_trends_by_age.parquet`"]
    end
    subgraph bundle_youth_wellbeing["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_youth_wellbeing" target="_blank" rel="noreferrer">bundle_youth_wellbeing</a></strong>`"]
        direction LR
        n185["`cms_youth_wellbeing_by_race.parquet`"]
        n186["`cms_youth_wellbeing_by_sex.parquet`"]
        n187["`cms_youth_wellbeing_state.parquet`"]
        n188["`medicaid_youth_wellbeing.parquet`"]
    end
    s0---s1["<strong><a href="https://data.cdc.gov/resource/qvzb-qs6p/" target="_blank" rel="noreferrer">Serotype Data for Invasive Pneumococcal Disease Cases by Age Group from Active Bacterial Core surveillance</a></strong>"]
    s1 --> n1
    s1 --> n2
    s1 --> n3
    s2---s3["<strong><a href="https://pubmed.ncbi.nlm.nih.gov/39758745/" target="_blank" rel="noreferrer">Open Forum for Infectious Diseases</a></strong>"]
    s3 --> n3
    s4---s5["<strong><a href="https://data.hrsa.gov/topics/health-workforce/ahrf" target="_blank" rel="noreferrer">AHRF County-Level Data Files</a></strong>"]
    s5 --> n4
    s6---s7["<strong><a href="https://www.cdc.gov/beam/dashboard/" target="_blank" rel="noreferrer">BEAM (Bacteria, Enterics, Amoeba, and Mycotics) Dashboard</a></strong>"]
    s7 --> n5
    s8---s9["<strong><a href="https://data.cdc.gov/Behavioral-Risk-Factors/Behavioral-Risk-Factor-Surveillance-System-BRFSS-P/dttw-5yxu/about_data" target="_blank" rel="noreferrer">Behavioral Risk Factor Surveillance System (BRFSS) Prevalence Data (2011 to present)</a></strong>"]
    s9 --> n6
    s9 --> n7
    s10 --> n8
    s11 --> n9
    s12---s13["<strong><a href="https://api.census.gov/data.html" target="_blank" rel="noreferrer">Census API — ACS 5-Year Detailed Tables and Subject Tables</a></strong>"]
    s13 --> n10
    s14---s15["<strong><a href="https://www2.census.gov/geo/docs/reference/ua/2020_UA_COUNTY.xlsx" target="_blank" rel="noreferrer">2020 Census Urban Area to County Allocation File (XLSX)</a></strong>"]
    s15 --> n10
    s13 --> n11
    s13 --> n12
    s13 --> n13
    s13 --> n14
    s13 --> n15
    s16---s17["<strong><a href="https://data.cms.gov/tools/mapping-medicare-disparities-by-population" target="_blank" rel="noreferrer">Mapping Medicare Disparities by Population Tool</a></strong>"]
    s17 --> n16
    s18 --> n16
    s17 --> n17
    s18 --> n17
    s17 --> n18
    s18 --> n18
    s19---s20["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/" target="_blank" rel="noreferrer">COVIDcast Epidata API</a></strong>"]
    s20 --> n19
    s21---s22["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/hospital-admissions.html" target="_blank" rel="noreferrer">COVIDcast > Hospital Admissions</a></strong>"]
    s22 --> n20
    s20 --> n20
    s24---s25["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/fluview.html" target="_blank" rel="noreferrer">FluView API</a></strong>"]
    s25 --> n21
    s26 --> n21
    s20 --> n21
    s20 --> n22
    s29 --> n23
    s29 --> n24
    s29 --> n25
    s29 --> n26
    s29 --> n27
    s29 --> n28
    s30 --> n29
    s29 --> n30
    s29 --> n31
    s29 --> n32
    s29 --> n33
    s29 --> n34
    s29 --> n35
    s29 --> n36
    s29 --> n37
    s31---s32["<strong><a href="https://github.com/DISSC-yale/gtrends_collection" target="_blank" rel="noreferrer">Yale Data-Intensive Social Sciences, Google Trends Collection Framework</a></strong>"]
    s32 --> n38
    s32 --> n39
    s32 --> n40
    s32 --> n41
    s33---s34["<strong><a href="https://apiv2.kinsainsights.com/api/v1/docs" target="_blank" rel="noreferrer">Kinsa Insights API - Signal Endpoint</a></strong>"]
    s34 --> n42
    s34 --> n43
    s35 --> n44
    s36 --> n45
    s37 --> n46
    s37 --> n47
    s37 --> n48
    s38---s39["<strong><a href="https://data.medicaid.gov/datasets?theme%5B0%5D=Quality" target="_blank" rel="noreferrer">Medicaid.gov Open Data – Quality Measures datasets (2014–2023)</a></strong>"]
    s39 --> n49
    s40 --> n50
    s40 --> n51
    s40 --> n52
    s41---s42["<strong><a href="https://app.powerbigov.us/view?r=eyJrIjoiZmU5ZjA2ZDItNTU0MS00M2EzLWEyZmQtZmY3Y2RlZjdjYTdjIiwidCI6IjljZTcwODY5LTYwZGItNDRmZC1hYmU4LWQyNzY3MDc3ZmM4ZiJ9" target="_blank" rel="noreferrer">NARMS Now Interactive Dashboard - Human Data</a></strong>"]
    s42 --> n53
    s43 --> n53
    s44 --> n53
    s45 --> n53
    s42 --> n54
    s43 --> n54
    s44 --> n54
    s45 --> n54
    s42 --> n55
    s43 --> n55
    s44 --> n55
    s45 --> n55
    s42 --> n56
    s43 --> n56
    s44 --> n56
    s45 --> n56
    s42 --> n57
    s43 --> n57
    s44 --> n57
    s45 --> n57
    s42 --> n58
    s43 --> n58
    s44 --> n58
    s45 --> n58
    s46---s47["<strong><a href="https://nccrexplorer.ccdi.cancer.gov/application.html" target="_blank" rel="noreferrer">NCCR*Explorer: An interactive website for NCCR cancer statistics</a></strong>"]
    s47 --> n59
    s48 --> n60
    s49 --> n60
    s50 --> n61
    s48 --> n62
    s49 --> n62
    s51---s52["<strong><a href="https://www.cpsc.gov/cgibin/NEISSQuery/" target="_blank" rel="noreferrer">NEISS public query / archived data files</a></strong>"]
    s52 --> n63
    s52 --> n64
    s52 --> n65
    s52 --> n66
    s52 --> n67
    s52 --> n68
    s52 --> n69
    s52 --> n70
    s53---s54["<strong><a href="https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/" target="_blank" rel="noreferrer">NHTSA File Downloads — FARS National CSV archives</a></strong>"]
    s54 --> n71
    s54 --> n72
    s54 --> n73
    s54 --> n74
    s55 --> n75
    s56---s57["<strong><a href="https://www.cdc.gov/nis/about/index.html" target="_blank" rel="noreferrer">About the National Immunization Surveys (NIS)</a></strong>"]
    s57 --> n75
    s55 --> n76
    s57 --> n76
    s55 --> n77
    s57 --> n77
    s58 --> n78
    s59---s60["<strong><a href="https://www.wpc.ncep.noaa.gov/heatrisk/data.html" target="_blank" rel="noreferrer">HeatRisk GeoTIFF Archive and 7-Day Forecast</a></strong>"]
    s60 --> n79
    s60 --> n80
    s60 --> n81
    s60 --> n82
    s61---s62["<strong><a href="https://data.cdc.gov/resource/3cxc-4k8q" target="_blank" rel="noreferrer">Percent Positivity of Respiratory Syncytial Virus Nucleic Acid Amplification Tests by HHS Region, National Respiratory and Enteric Virus Surveillance System</a></strong>"]
    s62 --> n83
    s63 --> n83
    s64---s65["<strong><a href="https://data.cdc.gov/resource/rdmq-nq56" target="_blank" rel="noreferrer">National Syndromic Surveillance Program</a></strong>"]
    s65 --> n84
    s66---s67["<strong><a href="https://healthdata.gov/CDC/Weekly-Rates-of-Laboratory-Confirmed-COVID-19-Hosp/gk5r-vjtt/about_data" target="_blank" rel="noreferrer">Weekly Rates of Laboratory-Confirmed COVID-19 Hospitalizations from the COVID-NET Surveillance System</a></strong>"]
    s67 --> n85
    s66---s68["<strong><a href="https://data.cdc.gov/Public-Health-Surveillance/Weekly-Rates-of-Laboratory-Confirmed-RSV-Hospitali/29hc-w46k/about_data" target="_blank" rel="noreferrer">Weekly Rates of Laboratory-Confirmed RSV Hospitalizations from the RSV-NET Surveillance System</a></strong>"]
    s68 --> n85
    s66---s69["<strong><a href="https://data.cdc.gov/Public-Health-Surveillance/Rates-of-Laboratory-Confirmed-RSV-COVID-19-and-Flu/kvib-3txy/about_data" target="_blank" rel="noreferrer">Rates of Laboratory-Confirmed RSV, COVID-19, and Flu Hospitalizations from the RESP-NET Surveillance Systems</a></strong>"]
    s69 --> n85
    s70 --> n86
    s71 --> n86
    s70 --> n87
    s72---s73["<strong><a href="https://data.cdc.gov/Vaccinations/Vaccination-Coverage-and-Exemptions-among-Kinderga/ijqb-a7ye/about_data" target="_blank" rel="noreferrer">Vaccination Coverage and Exemptions among Kindergartners</a></strong>"]
    s73 --> n88
    s73 --> n89
    s74 --> n90
    s74 --> n91
    s74 --> n92
    s75 --> n93
    s75 --> n94
    s76---s77["<strong><a href="https://data.cdc.gov/Public-Health-Surveillance/CDC-Wastewater-Viral-Activity-Level-for-SARS-CoV-2/atcp-73re/" target="_blank" rel="noreferrer">CDC Wastewater Viral Activity Level for SARS-CoV-2, Influenza A and RSV</a></strong>"]
    s77 --> n95
    s78---s79["<strong><a href="https://wisqars.cdc.gov/reports/?o=MORT&i=8&m=20810&s=0&r=0&ry=2&y1=2018&y2=2023&a=ALL&g1=0&g2=199&a1=0&a2=199&r1=MECH&r2=AGEGP&r3=STATE&r4=YEAR&r5=NONE&r6=NONE&g=00&e=0&yp=65&me=0&t=0" target="_blank" rel="noreferrer">Fatal Injury Report</a></strong>"]
    s79 --> n96
    s80 --> n97
    s80 --> n98
    s80 --> n99
    n55 --> bundle_antimicrobial_resistance
    n56 --> bundle_antimicrobial_resistance
    n58 --> bundle_antimicrobial_resistance
    n53 --> bundle_antimicrobial_resistance
    n54 --> bundle_antimicrobial_resistance
    n18 --> bundle_cancer_screening
    n17 --> bundle_cancer_screening
    n16 --> bundle_cancer_screening
    n49 --> bundle_cancer_screening
    n89 --> bundle_childhood_immunizations
    n88 --> bundle_childhood_immunizations
    n86 --> bundle_childhood_immunizations
    n87 --> bundle_childhood_immunizations
    n77 --> bundle_childhood_immunizations
    n76 --> bundle_childhood_immunizations
    n75 --> bundle_childhood_immunizations
    n7 --> bundle_chronic_diseases
    n18 --> bundle_chronic_diseases
    n7 --> bundle_injury_overdose
    n18 --> bundle_injury_overdose
    n62 --> bundle_injury_overdose
    n60 --> bundle_injury_overdose
    n96 --> bundle_injury_overdose
    n94 --> bundle_measles
    n44 --> bundle_measles
    n41 --> bundle_respiratory
    n95 --> bundle_respiratory
    n2 --> bundle_respiratory
    n3 --> bundle_respiratory
    n83 --> bundle_respiratory
    n84 --> bundle_respiratory
    n85 --> bundle_respiratory
```
