```mermaid
flowchart LR
    classDef pass stroke:#66bb6a
    classDef warn stroke:#ffa726
    classDef fail stroke:#f44336
    s0(("<strong><a href="https://www.cdc.gov/abcs/index.html" target="_blank" rel="noreferrer">Active Bacterial Core surveillance (ABCs)</a></strong>"))
    s2(("<strong><a href="https://data.cdc.gov/d/95m5-agj4" target="_blank" rel="noreferrer">Active Bacterial Core surveillance (ABCs) Group B Streptococcus</a></strong>"))
    s4(("<strong><a href="https://data.cdc.gov/d/9y49-tura" target="_blank" rel="noreferrer">Active Bacterial Core surveillance (ABCs) Group A Streptococcus</a></strong>"))
    s5(("<strong><a href="https://pubmed.ncbi.nlm.nih.gov/39758745/" target="_blank" rel="noreferrer">Serotype-Specific Urinary Antigen Detection (SSUAD) Study</a></strong>"))
    s7(("<strong><a href="https://www.cdc.gov/mosquitoes/php/arbonet/index.html" target="_blank" rel="noreferrer">ArboNET Arboviral Disease Surveillance System</a></strong>"))
    s9(("<strong><a href="https://data.hrsa.gov/topics/health-workforce/ahrf" target="_blank" rel="noreferrer">Area Health Resource File (AHRF)</a></strong>"))
    s11(("<strong><a href="https://data.cdc.gov/Foodborne-Waterborne-and-Related-Diseases/BEAM-Dashboard-Report-Data/jbhn-e8xn/about_data" target="_blank" rel="noreferrer">BEAM Dashboard - Report Data</a></strong>"))
    s13(("<strong><a href="https://www.bls.gov/lau/" target="_blank" rel="noreferrer">Local Area Unemployment Statistics (LAUS)</a></strong>"))
    s15(("<strong><a href="https://www.cdc.gov/brfss/index.html" target="_blank" rel="noreferrer">Behavioral Risk Factor Surveillance System (BRFSS)</a></strong>"))
    s17(("<strong><a href="https://data.cdc.gov/Public-Health-Surveillance/CDC-Epidemic-Trends-and-Rt/5dqz-y4ea/" target="_blank" rel="noreferrer">CDC Epidemic Trends and Rt</a></strong>"))
    s18(("<strong><a href="https://data.cdc.gov/d/e2d5-ggg7" target="_blank" rel="noreferrer">NCHS VSRR Provisional Maternal Death Counts and Rates</a></strong>"))
    s19(("<strong><a href="https://wonder.cdc.gov/natality-expanded-current.html" target="_blank" rel="noreferrer">CDC WONDER Natality, 2016-2024 expanded</a></strong>"))
    s21(("<strong><a href="https://www.census.gov/programs-surveys/acs/data.html" target="_blank" rel="noreferrer">2024 American Community Survey 5-Year Estimates, Powered by Metopio</a></strong>"))
    s23(("<strong><a href="https://www.census.gov/programs-surveys/geography/guidance/geo-areas/urban-rural.html" target="_blank" rel="noreferrer">2020 Census Urban Area to County Allocation File</a></strong>"))
    s25(("<strong><a href="https://www.census.gov/programs-surveys/decennial-census/decade/2020/planning-management/process/data-quality.html" target="_blank" rel="noreferrer">2020 Census Operational Quality Metrics</a></strong>"))
    s27(("<strong><a href="https://www.census.gov/programs-surveys/popest.html" target="_blank" rel="noreferrer">Population Estimates Program (PEP): Annual Population Estimates by Age, Sex, Race, and Hispanic Origin</a></strong>"))
    s29(("<strong><a href="https://www.census.gov/programs-surveys/sahie.html" target="_blank" rel="noreferrer">Small Area Health Insurance Estimates (SAHIE)</a></strong>"))
    s31(("<strong><a href="https://www.census.gov/programs-surveys/saipe.html" target="_blank" rel="noreferrer">Small Area Income and Poverty Estimates (SAIPE)</a></strong>"))
    s33(("<strong><a href="https://data.cms.gov/provider-data/dataset/hbf-map" target="_blank" rel="noreferrer">Birthing Friendly Hospitals with Geocoded Addresses</a></strong>"))
    s34(("<strong><a href="https://data.cdc.gov" target="_blank" rel="noreferrer">Center of Medicare and Medicaid Services (CMS)</a></strong>"))
    s36(("<strong><a href="https://data.cms.gov/tools/mapping-medicare-disparities-by-population" target="_blank" rel="noreferrer">Mapping Medicare Disparities by Population Tool</a></strong>"))
    s37(("<strong><a href="https://github.com/ColinVu/CountyBuddy/blob/main/county_data.csv" target="_blank" rel="noreferrer">County Buddy</a></strong>"))
    s38(("<strong><a href="https://delphi.cmu.edu/epidata/v5/" target="_blank" rel="noreferrer">CMU Delphi Epidata - Outpatient Claims</a></strong>"))
    s40(("<strong><a href="https://delphi.cmu.edu/epidata/v5/" target="_blank" rel="noreferrer">CMU Delphi Epidata - Inpatient Claims</a></strong>"))
    s41(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/" target="_blank" rel="noreferrer">CMU Delphi Epidata</a></strong>"))
    s43(("<strong><a href="https://www.cdc.gov/flu/weekly/overview.htm" target="_blank" rel="noreferrer">CDC ILINet</a></strong>"))
    s44(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/fluview.html" target="_blank" rel="noreferrer">CMU Delphi Epidata - FluView (ILINet)</a></strong>"))
    s45(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/nhsn.html" target="_blank" rel="noreferrer">CMU Delphi COVIDcast - NHSN Respiratory Hospitalizations</a></strong>"))
    s46(("<strong><a href="https://cosmos.epic.com/" target="_blank" rel="noreferrer">Epic Cosmos</a></strong>"))
    s47(("<strong><a href="https://www.epicresearch.org/health-alerts/" target="_blank" rel="noreferrer">Epic Research Health Alerts</a></strong>"))
    s48(("<strong><a href="https://trends.google.com" target="_blank" rel="noreferrer">Google Trends</a></strong>"))
    s50(("<strong><a href="https://www.huduser.gov/portal/datasets/cp.html" target="_blank" rel="noreferrer">Comprehensive Housing Affordability Strategy (CHAS) data</a></strong>"))
    s52(("<strong><a href="https://apiv2.kinsainsights.com/api/v1/docs" target="_blank" rel="noreferrer">Kinsa Insights API</a></strong>"))
    s54(("<strong><a href="https://www.cdc.gov/measles/data-research/index.html" target="_blank" rel="noreferrer">CDC Measles Cases and Outbreaks - Age and Vaccination Status</a></strong>"))
    s55(("<strong><a href="https://www.cdc.gov/measles/data-research/index.html" target="_blank" rel="noreferrer">CDC Measles Cases and Outbreaks</a></strong>"))
    s56(("<strong><a href="https://github.com/CSSEGISandData/measles_data" target="_blank" rel="noreferrer">Johns Hopkins University Measles Tracking Team</a></strong>"))
    s57(("<strong><a href="https://data.medicaid.gov/datasets?theme%5B0%5D=Quality" target="_blank" rel="noreferrer">Medicaid and CHIP Adult and Child Core Set Quality Measures</a></strong>"))
    s59(("<strong><a href="https://github.com/eric-gengzhou/MMR_vaccine_estimates" target="_blank" rel="noreferrer">HealthMap MMR Vaccine Coverage Estimates</a></strong>"))
    s60(("<strong><a href="https://www.cdc.gov/narms/data/index.html" target="_blank" rel="noreferrer">NARMS Now: Human Data - Antimicrobial Resistance</a></strong>"))
    s62(("<strong><a href="https://www.fda.gov/animal-veterinary/national-antimicrobial-resistance-monitoring-system/integrated-reportssummaries" target="_blank" rel="noreferrer">FDA NARMS Retail Meats Surveillance Data</a></strong>"))
    s63(("<strong><a href="https://www.fda.gov/animal-veterinary/national-antimicrobial-resistance-monitoring-system/integrated-reportssummaries" target="_blank" rel="noreferrer">FDA NARMS Animal Pathogen Surveillance Data</a></strong>"))
    s64(("<strong><a href="https://www.fda.gov/animal-veterinary/national-antimicrobial-resistance-monitoring-system/integrated-reportssummaries" target="_blank" rel="noreferrer">FDA NARMS Food-Producing Animals Surveillance Data</a></strong>"))
    s65(("<strong><a href="https://nccrexplorer.ccdi.cancer.gov/" target="_blank" rel="noreferrer">National Childhood Cancer Registry Explorer (NCCR*Explorer)</a></strong>"))
    s67(("<strong><a href="https://data.cdc.gov/d/xkb8-kh2a" target="_blank" rel="noreferrer">NCHS VSRR Provisional Drug Overdose Death Counts (State)</a></strong>"))
    s68(("<strong><a href="https://data.cdc.gov/d/gb4e-yj24" target="_blank" rel="noreferrer">NCHS VSRR Provisional County-Level Drug Overdose Death Counts</a></strong>"))
    s69(("<strong><a href="https://data.cdc.gov/d/489q-934x" target="_blank" rel="noreferrer">NCHS VSRR Quarterly Provisional Estimates for Selected Indicators of Mortality</a></strong>"))
    s70(("<strong><a href="https://www.cpsc.gov/Research--Statistics/NEISS-Injury-Data" target="_blank" rel="noreferrer">National Electronic Injury Surveillance System (NEISS)</a></strong>"))
    s72(("<strong><a href="https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/" target="_blank" rel="noreferrer">Fatality Analysis Reporting System (FARS)</a></strong>"))
    s74(("<strong><a href="https://www.cdc.gov/nis/about/index.html" target="_blank" rel="noreferrer">National Immunization Survey-Teen (NIS-Teen)</a></strong>"))
    s76(("<strong><a href="https://www.cdc.gov/nis/about/index.html" target="_blank" rel="noreferrer">National Immunization Survey (NIS)</a></strong>"))
    s77(("<strong><a href="https://www.cdc.gov/nis/about/index.html" target="_blank" rel="noreferrer">National Immunization Survey</a></strong>"))
    s79(("<strong><a href="https://www.cdc.gov/nndss/" target="_blank" rel="noreferrer">National Notifiable Diseases Surveillance System (NNDSS)</a></strong>"))
    s80(("<strong><a href="https://www.wpc.ncep.noaa.gov/heatrisk/data/archive/" target="_blank" rel="noreferrer">NOAA WPC HeatRisk</a></strong>"))
    s82(("<strong><a href="https://data.cdc.gov" target="_blank" rel="noreferrer">Centers for Disease Control and Prevention</a></strong>"))
    s84(("<strong><a href="https://data.cdc.gov/resource/3cxc-4k8q" target="_blank" rel="noreferrer">National Respiratory and Enteric Virus Surveillance System (NREVSS)</a></strong>"))
    s85(("<strong><a href="https://www.cdc.gov/nssp/index.html" target="_blank" rel="noreferrer">National Syndromic Surveillance Program (NSSP)</a></strong>"))
    s87(("<strong><a href="https://www.cdc.gov/resp-net/dashboard/index.html" target="_blank" rel="noreferrer">Respiratory Virus Hospitalization Surveillance Network (RESP-NET)</a></strong>"))
    s91(("<strong><a href="https://github.com/PopHIVE/school_immunizations" target="_blank" rel="noreferrer">PopHIVE school immunization assessments</a></strong>"))
    s93(("<strong><a href="https://github.com/washingtonpost/data-school-vaccination-rates" target="_blank" rel="noreferrer">Washington Post School Vaccination Rates</a></strong>"))
    s94(("<strong><a href="https://www.tn.gov/health/cedep/immunization/school-immunization-requirements.html" target="_blank" rel="noreferrer">Tennessee Kindergarten Immunization Compliance Assessment</a></strong>"))
    s95(("<strong><a href="https://www.cdc.gov/schoolvaxview/index.html" target="_blank" rel="noreferrer">SchoolVaxView</a></strong>"))
    s97(("<strong><a href="https://www.ers.usda.gov/data-products/food-environment-atlas/" target="_blank" rel="noreferrer">Food Environment Atlas</a></strong>"))
    s99(("<strong><a href="https://jamanetwork.com/journals/jama/fullarticle/2843870" target="_blank" rel="noreferrer">Medical Exemptions From Childhood Vaccination in the US (Kiang et al. 2025)</a></strong>"))
    s100(("<strong><a href="https://data.cdc.gov/d/akvg-8vrb" target="_blank" rel="noreferrer">CDC National Wastewater Surveillance System (NWSS) - Measles</a></strong>"))
    s101(("<strong><a href="https://www.cdc.gov/nwss/" target="_blank" rel="noreferrer">CDC National Wastewater Surveillance System (NWSS)</a></strong>"))
    s103(("<strong><a href="https://wisqars.cdc.gov/" target="_blank" rel="noreferrer">Web-based Injury Statistics Query and Reporting System (WISQARS)</a></strong>"))
    s105(("<strong><a href="https://yrbs-explorer.services.cdc.gov/" target="_blank" rel="noreferrer">CDC Youth Risk Behavior Surveillance System (YRBSS)</a></strong>"))
    subgraph abcs["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/abcs" target="_blank" rel="noreferrer">abcs</a></strong>`"]
        direction LR
        n1["`data.csv.gz`"]:::pass
        n2["`gas_emm.csv.gz`"]:::pass
        n3["`gas_syndromes.csv.gz`"]:::pass
        n4["`gbs_alph.csv.gz`"]:::pass
        n5["`gbs_serotypes.csv.gz`"]:::pass
        n6["`gbs_syndromes.csv.gz`"]:::pass
        n7["`strep_counts.csv.gz`"]:::pass
        n8["`strep_rates.csv.gz`"]:::pass
        n9["`strep_resistance.csv.gz`"]:::pass
        n10["`uad.csv.gz`"]:::pass
    end
    subgraph arbonet["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/arbonet" target="_blank" rel="noreferrer">arbonet</a></strong>`"]
        direction LR
        n11["`data_county.csv.gz`"]:::pass
        n12["`data_state.csv.gz`"]:::pass
    end
    subgraph area_health_resource_file["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/area_health_resource_file" target="_blank" rel="noreferrer">area_health_resource_file</a></strong>`"]
        direction LR
        n13["`data.csv.gz`"]:::pass
    end
    subgraph atlas_amr["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/atlas_amr" target="_blank" rel="noreferrer">atlas_amr</a></strong>`"]
        direction LR
    end
    subgraph beam["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/beam" target="_blank" rel="noreferrer">beam</a></strong>`"]
        direction LR
        n14["`data.csv.gz`"]:::pass
    end
    subgraph bls_laus["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bls_laus" target="_blank" rel="noreferrer">bls_laus</a></strong>`"]
        direction LR
        n15["`data_county.csv.gz`"]:::pass
        n16["`data_state.csv.gz`"]:::pass
    end
    subgraph brfss["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/brfss" target="_blank" rel="noreferrer">brfss</a></strong>`"]
        direction LR
        n17["`data_survey.csv.gz<br/><br/><ul><li><code>missing_info: sex, race_ethnicity, prev_insured_survey, prev_insured_survey_lcl, prev_insured_survey_ucl, sample_size_insured</code></li></ul>`"]:::warn
        n18["`data.csv.gz`"]:::pass
    end
    subgraph cdc_cfa_rt["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/cdc_cfa_rt" target="_blank" rel="noreferrer">cdc_cfa_rt</a></strong>`"]
        direction LR
        n19["`data.csv.gz`"]:::pass
    end
    subgraph cdc_vssr["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/cdc_vssr" target="_blank" rel="noreferrer">cdc_vssr</a></strong>`"]
        direction LR
        n20["`data.csv.gz`"]:::pass
    end
    subgraph cdc_wonder_natality["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/cdc_wonder_natality" target="_blank" rel="noreferrer">cdc_wonder_natality</a></strong>`"]
        direction LR
        n21["`data_state_detail.csv.gz`"]:::pass
        n22["`data_state.csv.gz`"]:::pass
    end
    subgraph census["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/census" target="_blank" rel="noreferrer">census</a></strong>`"]
        direction LR
        n23["`data_county.csv.gz`"]:::pass
        n24["`data_oqm.csv.gz`"]:::pass
        n25["`data_pep.csv.gz`"]:::pass
        n26["`data_sahie.csv.gz`"]:::pass
        n27["`data_saipe.csv.gz`"]:::pass
        n28["`data_state.csv.gz`"]:::pass
    end
    subgraph cms_birth["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/cms_birth" target="_blank" rel="noreferrer">cms_birth</a></strong>`"]
        direction LR
        n29["`data_county.csv.gz`"]:::pass
        n30["`data_state.csv.gz`"]:::pass
    end
    subgraph cms_mmd["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/cms_mmd" target="_blank" rel="noreferrer">cms_mmd</a></strong>`"]
        direction LR
        n31["`data_state_county_age_by_race.csv.gz`"]:::pass
        n32["`data_state_county_age_by_sex.csv.gz`"]:::pass
        n33["`data_state_county_age.csv.gz`"]:::pass
    end
    subgraph county_health_rankings["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/county_health_rankings" target="_blank" rel="noreferrer">county_health_rankings</a></strong>`"]
        direction LR
    end
    subgraph countybuddy["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/countybuddy" target="_blank" rel="noreferrer">countybuddy</a></strong>`"]
        direction LR
        n34["`data.csv.gz`"]:::pass
    end
    subgraph delphi_doctors_claims["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_doctors_claims" target="_blank" rel="noreferrer">delphi_doctors_claims</a></strong>`"]
        direction LR
        n35["`data.csv.gz`"]:::pass
    end
    subgraph delphi_hospital_claims["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_hospital_claims" target="_blank" rel="noreferrer">delphi_hospital_claims</a></strong>`"]
        direction LR
        n36["`data.csv.gz`"]:::pass
    end
    subgraph delphi_ili_fluview["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_ili_fluview" target="_blank" rel="noreferrer">delphi_ili_fluview</a></strong>`"]
        direction LR
        n37["`data.csv.gz`"]:::pass
    end
    subgraph delphi_nhsn["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_nhsn" target="_blank" rel="noreferrer">delphi_nhsn</a></strong>`"]
        direction LR
        n38["`data.csv.gz`"]:::pass
    end
    subgraph epic_chronic["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_chronic" target="_blank" rel="noreferrer">epic_chronic</a></strong>`"]
        direction LR
        n39["`county_no_time.csv.gz<br/><br/><ul><li><code>missing_info: bmi_30_49.8, obesity_(%), n_obesity_county, Year</code></li></ul>`"]:::warn
        n40["`county_year.csv.gz`"]:::pass
        n41["`state_no_time.csv.gz<br/><br/><ul><li><code>missing_info: bmi_30_49.8, dm_(%), n_patients, Year</code></li></ul>`"]:::warn
        n42["`state_year.csv.gz`"]:::pass
    end
    subgraph epic_concussions["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_concussions" target="_blank" rel="noreferrer">epic_concussions</a></strong>`"]
        direction LR
        n43["`data.csv.gz<br/><br/><ul><li><code>missing_info: age, sex</code></li></ul>`"]:::warn
    end
    subgraph epic_diarrhea["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_diarrhea" target="_blank" rel="noreferrer">epic_diarrhea</a></strong>`"]
        direction LR
        n44["`data_weekly.csv.gz`"]:::pass
        n45["`weekly_tests.csv.gz`"]:::pass
    end
    subgraph epic_health_alerts["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_health_alerts" target="_blank" rel="noreferrer">epic_health_alerts</a></strong>`"]
        direction LR
        n46["`data.csv.gz`"]:::pass
    end
    subgraph epic_injury["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_injury" target="_blank" rel="noreferrer">epic_injury</a></strong>`"]
        direction LR
        n47["`heat_year_county.csv.gz<br/><br/><ul><li><code>missing_info: geography_name</code></li></ul>`"]:::warn
        n48["`monthly_injury.csv.gz`"]:::pass
        n49["`yearly_injury.csv.gz`"]:::pass
    end
    subgraph epic_resp_infections["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_resp_infections" target="_blank" rel="noreferrer">epic_resp_infections</a></strong>`"]
        direction LR
        n50["`monthly_tests.csv.gz`"]:::pass
        n51["`quarterly_gas.csv.gz`"]:::pass
        n52["`weekly.csv.gz`"]:::pass
    end
    subgraph gtrends["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/gtrends" target="_blank" rel="noreferrer">gtrends</a></strong>`"]
        direction LR
        n53["`data_dma_year.csv.gz`"]:::pass
        n54["`data_dma.csv.gz`"]:::pass
        n55["`data_year.csv.gz`"]:::pass
        n56["`data.csv.gz`"]:::pass
    end
    subgraph hud_chas["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/hud_chas" target="_blank" rel="noreferrer">hud_chas</a></strong>`"]
        direction LR
        n57["`data_county.csv.gz`"]:::pass
        n58["`data_state.csv.gz`"]:::pass
    end
    subgraph kinsa_ili["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/kinsa_ili" target="_blank" rel="noreferrer">kinsa_ili</a></strong>`"]
        direction LR
        n59["`data.csv.gz<br /><br />Script Failed:<br />Kinsa credentials not found. Set KINSA_EMAIL and KINSA_PASSWORD.`"]:::fail
    end
    subgraph measles_age_cdc2["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/measles_age_cdc2" target="_blank" rel="noreferrer">measles_age_cdc2</a></strong>`"]
        direction LR
        n60["`data.csv.gz<br/><br/><ul><li><code>missing_info: year, week</code></li></ul>`"]:::warn
    end
    subgraph measles_cdc["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/measles_cdc" target="_blank" rel="noreferrer">measles_cdc</a></strong>`"]
        direction LR
        n61["`data.csv.gz`"]:::pass
    end
    subgraph measles_jhu["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/measles_jhu" target="_blank" rel="noreferrer">measles_jhu</a></strong>`"]
        direction LR
        n62["`data_county.csv.gz`"]:::pass
        n63["`data_state.csv.gz`"]:::pass
        n64["`data.csv.gz`"]:::pass
    end
    subgraph medicaid_quality["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/medicaid_quality" target="_blank" rel="noreferrer">medicaid_quality</a></strong>`"]
        direction LR
        n65["`data.csv.gz<br/><br/><ul><li><code>missing_info: geography_level, age, sex, race_ethnicity, payer, domain, medicaid_awc_ch_pct_25, medicaid_awc_ch_pct_75, medicaid_lbw_ch_pct_25, medicaid_lbw_ch_pct_75, medicaid_ima_ch_pct_25, medicaid_ima_ch_pct_75, medicaid_aba_ad_pct_25, medicaid_aba_ad_pct_75, medicaid_w34_ch_pct_25, medicaid_w34_ch_pct_75, medicaid_ldl_ad_pct_25, medicaid_ldl_ad_pct_75, medicaid_pdent_ch_pct_25, medicaid_pdent_ch_pct_75, medicaid_amm_ad_pct_25, medicaid_amm_ad_pct_75, medicaid_amb_ch_pct_25, medicaid_amb_ch_pct_75, medicaid_hpv_ch_pct_25, medicaid_hpv_ch_pct_75, medicaid_fuh_ch_30d_pct_25, medicaid_fuh_ch_30d_pct_75, medicaid_fuh_ch_7d_pct_25, medicaid_fuh_ch_7d_pct_75, medicaid_fpc_ch_pct_25, medicaid_fpc_ch_pct_75, medicaid_chl_ch_pct_25, medicaid_chl_ch_pct_75, medicaid_cap_ch_pct_25, medicaid_cap_ch_pct_75, medicaid_fuh_ad_30d_pct_25, medicaid_fuh_ad_30d_pct_75, medicaid_bcs_ad_pct_25, medicaid_bcs_ad_pct_75, medicaid_ccs_ad_pct_25, medicaid_ccs_ad_pct_75, medicaid_mma_ch_pct_25, medicaid_mma_ch_pct_75, medicaid_wcc_ch_pct_25, medicaid_wcc_ch_pct_75, medicaid_chl_ad_pct_25, medicaid_chl_ad_pct_75, medicaid_mpm_ad_pct_25, medicaid_mpm_ad_pct_75, medicaid_cis_ch_pct_25, medicaid_cis_ch_pct_75, medicaid_add_ch_cont_pct_25, medicaid_add_ch_cont_pct_75, medicaid_ppc_ad_pct_25, medicaid_ppc_ad_pct_75, medicaid_ppc_ch_pct_25, medicaid_ppc_ch_pct_75, medicaid_add_ch_init_pct_25, medicaid_add_ch_init_pct_75, medicaid_w15_ch_pct_25, medicaid_w15_ch_pct_75, medicaid_ha1c_ad_pct_25, medicaid_ha1c_ad_pct_75, medicaid_tdent_ch_pct_25, medicaid_tdent_ch_pct_75, medicaid_fuh_ad_7d_pct_25, medicaid_fuh_ad_7d_pct_75, medicaid_msc_ad_pct_25, medicaid_msc_ad_pct_75, medicaid_iet_ad_pct_25, medicaid_iet_ad_pct_75, medicaid_seal_ch_pct_25, medicaid_seal_ch_pct_75, medicaid_saa_ad_pct_25, medicaid_saa_ad_pct_75, medicaid_dev_ch_pct_25, medicaid_dev_ch_pct_75, medicaid_apc_ch_pct_25, medicaid_apc_ch_pct_75, medicaid_add_ch_30d_pct_25, medicaid_add_ch_30d_pct_75, medicaid_cbp_ad_pct_25, medicaid_cbp_ad_pct_75, medicaid_ssd_ad_pct_25, medicaid_ssd_ad_pct_75, medicaid_pqi08_ad_pct_25, medicaid_pqi08_ad_pct_75, medicaid_pqi01_ad_pct_25, medicaid_pqi01_ad_pct_75, medicaid_ima_ch_hpv_pct_25, medicaid_ima_ch_hpv_pct_75, medicaid_pqi15_ad_pct_25, medicaid_pqi15_ad_pct_75, medicaid_pqi05_ad_pct_25, medicaid_pqi05_ad_pct_75, medicaid_hpc_ad_pct_25, medicaid_hpc_ad_pct_75, medicaid_app_ch_pct_25, medicaid_app_ch_pct_75, medicaid_amr_ch_pct_25, medicaid_amr_ch_pct_75, medicaid_ccw_ch_pct_25, medicaid_ccw_ch_pct_75, medicaid_ccp_ch_pct_25, medicaid_ccp_ch_pct_75, medicaid_fua_fum_ad_7d_pct_25, medicaid_fua_fum_ad_7d_pct_75, medicaid_fua_fum_ad_30d_pct_25, medicaid_fua_fum_ad_30d_pct_75, medicaid_amr_ad_pct_25, medicaid_amr_ad_pct_75, medicaid_ccp_ad_pct_25, medicaid_ccp_ad_pct_75, medicaid_pcr_ad_pct_25, medicaid_pcr_ad_pct_75, medicaid_ohd_ad_pct_25, medicaid_ohd_ad_pct_75, medicaid_fua_ad_7d_pct_25, medicaid_fua_ad_7d_pct_75, medicaid_fua_ad_30d_pct_25, medicaid_fua_ad_30d_pct_75, medicaid_fum_ad_7d_pct_25, medicaid_fum_ad_7d_pct_75, medicaid_fum_ad_30d_pct_25, medicaid_fum_ad_30d_pct_75, medicaid_apm_ch_gluc_pct_25, medicaid_apm_ch_gluc_pct_75, medicaid_apm_ch_chol_pct_25, medicaid_apm_ch_chol_pct_75, medicaid_apm_ch_gluc_chol_pct_25, medicaid_apm_ch_gluc_chol_pct_75, medicaid_cob_ad_pct_25, medicaid_cob_ad_pct_75, medicaid_ccw_ad_pct_25, medicaid_ccw_ad_pct_75, medicaid_fva_ad_pct_25, medicaid_fva_ad_pct_75, medicaid_ncidds_ad_pct_25, medicaid_ncidds_ad_pct_75, medicaid_sfm_ch_pct_25, medicaid_sfm_ch_pct_75, medicaid_lrcd_ch_pct_25, medicaid_lrcd_ch_pct_75, medicaid_wcv_ch_3_11_pct_25, medicaid_wcv_ch_3_11_pct_75, medicaid_wcv_ch_12_17_pct_25, medicaid_wcv_ch_12_17_pct_75, medicaid_wcv_ch_18_21_pct_25, medicaid_wcv_ch_18_21_pct_75, medicaid_wcv_ch_pct_25, medicaid_wcv_ch_pct_75, medicaid_w30_ch_pct_25, medicaid_w30_ch_pct_75, medicaid_oud_ad_pct_25, medicaid_oud_ad_pct_75, medicaid_fua_ch_30d_pct_25, medicaid_fua_ch_30d_pct_75, medicaid_fum_ch_7d_pct_25, medicaid_fum_ch_7d_pct_75, medicaid_fum_ch_30d_pct_25, medicaid_fum_ch_30d_pct_75, medicaid_oev_ch_pct_25, medicaid_oev_ch_pct_75, medicaid_tfl_ch_pct_25, medicaid_tfl_ch_pct_75, medicaid_aab_ad_pct_25, medicaid_aab_ad_pct_75, medicaid_fua_ch_7d_pct_25, medicaid_fua_ch_7d_pct_75, medicaid_aab_ch_pct_25, medicaid_aab_ch_pct_75, medicaid_cpc_ch_pct_25, medicaid_cpc_ch_pct_75, medicaid_lsc_ch_pct_25, medicaid_lsc_ch_pct_75, medicaid_amm_ad_cont_pct_25, medicaid_amm_ad_cont_pct_75, medicaid_hbd_ad_pct_25, medicaid_hbd_ad_pct_75, medicaid_cpa_ad_pct_25, medicaid_cpa_ad_pct_75, medicaid_col_ad_pct_25, medicaid_col_ad_pct_75</code></li></ul>`"]:::warn
    end
    subgraph mmr_healthmap["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/mmr_healthmap" target="_blank" rel="noreferrer">mmr_healthmap</a></strong>`"]
        direction LR
        n66["`data_county.csv.gz`"]:::pass
        n67["`data_state.csv.gz`"]:::pass
        n68["`data_zcta.csv.gz`"]:::pass
    end
    subgraph narms["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/narms" target="_blank" rel="noreferrer">narms</a></strong>`"]
        direction LR
        n69["`data_animal_pathogen.csv.gz`"]:::pass
        n70["`data_food_animals.csv.gz`"]:::pass
        n71["`data_resistance_agent.csv.gz`"]:::pass
        n72["`data_resistance_pattern.csv.gz`"]:::pass
        n73["`data_retail_meats.csv.gz`"]:::pass
    end
    subgraph nccr["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nccr" target="_blank" rel="noreferrer">nccr</a></strong>`"]
        direction LR
        n74["`data.csv.gz<br/><br/><ul><li><code>missing_info: age, sex, race_ethnicity</code></li></ul>`"]:::warn
    end
    subgraph nchs_mortality["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nchs_mortality" target="_blank" rel="noreferrer">nchs_mortality</a></strong>`"]
        direction LR
        n75["`data_county.csv.gz`"]:::pass
        n76["`data_state_21_causes.csv.gz`"]:::pass
        n77["`data.csv.gz`"]:::pass
    end
    subgraph neiss["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/neiss" target="_blank" rel="noreferrer">neiss</a></strong>`"]
        direction LR
        n78["`data_agegroup_diagnosis_rate.csv.gz`"]:::pass
        n79["`data_agegroup_diagnosis.csv.gz`"]:::pass
        n80["`data_agegroup_product_rate.csv.gz`"]:::pass
        n81["`data_agegroup_product.csv.gz`"]:::pass
        n82["`data_infant_diagnosis_rate.csv.gz`"]:::pass
        n83["`data_infant_diagnosis.csv.gz`"]:::pass
        n84["`data_infant_product_rate.csv.gz`"]:::pass
        n85["`data_infant_product.csv.gz`"]:::pass
    end
    subgraph nhtsa_crash["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nhtsa_crash" target="_blank" rel="noreferrer">nhtsa_crash</a></strong>`"]
        direction LR
        n86["`data_age_sex.csv.gz<br/><br/><ul><li><code>missing_info: age, sex</code></li></ul>`"]:::warn
        n87["`data_crash_type.csv.gz<br/><br/><ul><li><code>missing_info: age, sex</code></li></ul>`"]:::warn
        n88["`data_person_type.csv.gz<br/><br/><ul><li><code>missing_info: person_type</code></li></ul>`"]:::warn
        n89["`data.csv.gz`"]:::pass
    end
    subgraph nis_teen["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nis_teen" target="_blank" rel="noreferrer">nis_teen</a></strong>`"]
        direction LR
        n90["`data_insurance.csv.gz<br/><br/><ul><li><code>missing_info: sex</code></li></ul>`"]:::warn
        n91["`data_poverty.csv.gz<br/><br/><ul><li><code>missing_info: sex</code></li></ul>`"]:::warn
        n92["`data_race_ethnicity.csv.gz<br/><br/><ul><li><code>missing_info: race_ethnicity, sex</code></li></ul>`"]:::warn
        n93["`data_urban.csv.gz<br/><br/><ul><li><code>missing_info: sex</code></li></ul>`"]:::warn
        n94["`data.csv.gz<br/><br/><ul><li><code>missing_info: age, sex</code></li></ul>`"]:::warn
    end
    subgraph nis["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nis" target="_blank" rel="noreferrer">nis</a></strong>`"]
        direction LR
        n95["`data_insurance.csv.gz`"]:::pass
        n96["`data_urban.csv.gz`"]:::pass
        n97["`data.csv.gz`"]:::pass
    end
    subgraph nnds["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nnds" target="_blank" rel="noreferrer">nnds</a></strong>`"]
        direction LR
        n98["`data.csv.gz<br/><br/><ul><li><code>missing_info: mmwr_year, mmwr_week, anthrax, plague, rabies_human, rubella_congenital_syndrome, cronobacter_invasive_infection_infants_confirmed</code></li></ul>`"]:::warn
    end
    subgraph noaa_heat_risk["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/noaa_heat_risk" target="_blank" rel="noreferrer">noaa_heat_risk</a></strong>`"]
        direction LR
        n99["`data_county.csv.gz`"]:::pass
        n100["`data_state.csv.gz`"]:::pass
    end
    subgraph NREVSS["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/NREVSS" target="_blank" rel="noreferrer">NREVSS</a></strong>`"]
        direction LR
        n101["`data.csv.gz`"]:::pass
    end
    subgraph nssp["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nssp" target="_blank" rel="noreferrer">nssp</a></strong>`"]
        direction LR
        n102["`data.csv.gz`"]:::pass
    end
    subgraph respnet["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/respnet" target="_blank" rel="noreferrer">respnet</a></strong>`"]
        direction LR
        n103["`data.csv.gz`"]:::pass
    end
    subgraph school_immunizations_adolescent["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/school_immunizations_adolescent" target="_blank" rel="noreferrer">school_immunizations_adolescent</a></strong>`"]
        direction LR
        n104["`data.csv.gz`"]:::pass
    end
    subgraph schoolvax_washpost["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/schoolvax_washpost" target="_blank" rel="noreferrer">schoolvax_washpost</a></strong>`"]
        direction LR
        n105["`data_counties.csv.gz`"]:::pass
        n106["`data_schools.csv.gz`"]:::pass
    end
    subgraph schoolvaxview["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/schoolvaxview" target="_blank" rel="noreferrer">schoolvaxview</a></strong>`"]
        direction LR
        n107["`data_exemptions.csv.gz`"]:::pass
        n108["`data.csv.gz`"]:::pass
    end
    subgraph usda_food_access["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/usda_food_access" target="_blank" rel="noreferrer">usda_food_access</a></strong>`"]
        direction LR
        n109["`data_county.csv.gz`"]:::pass
    end
    subgraph vaccine_exemptions_fattah["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/vaccine_exemptions_fattah" target="_blank" rel="noreferrer">vaccine_exemptions_fattah</a></strong>`"]
        direction LR
        n110["`data_county.csv.gz<br/><br/><ul><li><code>missing_info: is_state_estimate</code></li></ul>`"]:::warn
        n111["`data_state.csv.gz`"]:::pass
        n112["`data.csv.gz`"]:::pass
    end
    subgraph vaers["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/vaers" target="_blank" rel="noreferrer">vaers</a></strong>`"]
        direction LR
    end
    subgraph wastewater_measles["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wastewater_measles" target="_blank" rel="noreferrer">wastewater_measles</a></strong>`"]
        direction LR
        n113["`data_county.csv.gz`"]:::pass
        n114["`data.csv.gz`"]:::pass
    end
    subgraph wastewater["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wastewater" target="_blank" rel="noreferrer">wastewater</a></strong>`"]
        direction LR
        n115["`data.csv.gz`"]:::pass
    end
    subgraph wisqars["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wisqars" target="_blank" rel="noreferrer">wisqars</a></strong>`"]
        direction LR
        n116["`data.csv.gz`"]:::pass
    end
    subgraph yrbss["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/yrbss" target="_blank" rel="noreferrer">yrbss</a></strong>`"]
        direction LR
        n117["`data_age_ethnicity.csv.gz<br/><br/><ul><li><code>geography_dropped</code></li><li><code>missing_info: age, race_ethnicity, pct_no_pe_classes, pct_no_pe_classes_lcl, pct_no_pe_classes_ucl, pct_no_pe_classes_suppressed, pct_no_pe_classes_not_asked, pct_no_condom_last_sex, pct_no_condom_last_sex_lcl, pct_no_condom_last_sex_ucl, pct_no_condom_last_sex_suppressed, pct_no_condom_last_sex_not_asked, pct_no_birth_control_pills, pct_no_birth_control_pills_lcl, pct_no_birth_control_pills_ucl, pct_no_birth_control_pills_suppressed, pct_no_birth_control_pills_not_asked, pct_never_tested_hiv, pct_never_tested_hiv_lcl, pct_never_tested_hiv_ucl, pct_never_tested_hiv_suppressed, pct_never_tested_hiv_not_asked, pct_not_tested_std, pct_not_tested_std_lcl, pct_not_tested_std_ucl, pct_not_tested_std_suppressed, pct_not_tested_std_not_asked</code></li><li><code>type_changed: pct_no_pe_classes, pct_no_pe_classes_lcl, pct_no_pe_classes_ucl, pct_no_condom_last_sex, pct_no_condom_last_sex_lcl, pct_no_condom_last_sex_ucl, pct_no_birth_control_pills, pct_no_birth_control_pills_lcl, pct_no_birth_control_pills_ucl, pct_never_tested_hiv, pct_never_tested_hiv_lcl, pct_never_tested_hiv_ucl, pct_not_tested_std, pct_not_tested_std_lcl, pct_not_tested_std_ucl</code></li></ul>`"]:::warn
        n118["`data_age_sex.csv.gz<br/><br/><ul><li><code>geography_dropped</code></li><li><code>missing_info: age, sex, pct_no_pe_classes, pct_no_pe_classes_lcl, pct_no_pe_classes_ucl, pct_no_pe_classes_suppressed, pct_no_pe_classes_not_asked, pct_no_condom_last_sex, pct_no_condom_last_sex_lcl, pct_no_condom_last_sex_ucl, pct_no_condom_last_sex_suppressed, pct_no_condom_last_sex_not_asked, pct_no_birth_control_pills, pct_no_birth_control_pills_lcl, pct_no_birth_control_pills_ucl, pct_no_birth_control_pills_suppressed, pct_no_birth_control_pills_not_asked, pct_never_tested_hiv, pct_never_tested_hiv_lcl, pct_never_tested_hiv_ucl, pct_never_tested_hiv_suppressed, pct_never_tested_hiv_not_asked, pct_not_tested_std, pct_not_tested_std_lcl, pct_not_tested_std_ucl, pct_not_tested_std_suppressed, pct_not_tested_std_not_asked</code></li><li><code>type_changed: pct_no_pe_classes, pct_no_pe_classes_lcl, pct_no_pe_classes_ucl, pct_no_condom_last_sex, pct_no_condom_last_sex_lcl, pct_no_condom_last_sex_ucl, pct_no_birth_control_pills, pct_no_birth_control_pills_lcl, pct_no_birth_control_pills_ucl, pct_never_tested_hiv, pct_never_tested_hiv_lcl, pct_never_tested_hiv_ucl, pct_not_tested_std, pct_not_tested_std_lcl, pct_not_tested_std_ucl</code></li></ul>`"]:::warn
        n119["`data_age.csv.gz<br/><br/><ul><li><code>geography_dropped</code></li><li><code>missing_info: age, pct_no_pe_classes, pct_no_pe_classes_lcl, pct_no_pe_classes_ucl, pct_no_pe_classes_suppressed, pct_no_pe_classes_not_asked, pct_no_condom_last_sex, pct_no_condom_last_sex_lcl, pct_no_condom_last_sex_ucl, pct_no_condom_last_sex_suppressed, pct_no_condom_last_sex_not_asked, pct_no_birth_control_pills, pct_no_birth_control_pills_lcl, pct_no_birth_control_pills_ucl, pct_no_birth_control_pills_suppressed, pct_no_birth_control_pills_not_asked, pct_never_tested_hiv, pct_never_tested_hiv_lcl, pct_never_tested_hiv_ucl, pct_never_tested_hiv_suppressed, pct_never_tested_hiv_not_asked, pct_not_tested_std, pct_not_tested_std_lcl, pct_not_tested_std_ucl, pct_not_tested_std_suppressed, pct_not_tested_std_not_asked</code></li><li><code>type_changed: pct_no_pe_classes, pct_no_pe_classes_lcl, pct_no_pe_classes_ucl, pct_no_condom_last_sex, pct_no_condom_last_sex_lcl, pct_no_condom_last_sex_ucl, pct_no_birth_control_pills, pct_no_birth_control_pills_lcl, pct_no_birth_control_pills_ucl, pct_never_tested_hiv, pct_never_tested_hiv_lcl, pct_never_tested_hiv_ucl, pct_not_tested_std, pct_not_tested_std_lcl, pct_not_tested_std_ucl</code></li></ul>`"]:::warn
    end
    subgraph bundle_adolescent_vaccination["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_adolescent_vaccination" target="_blank" rel="noreferrer">bundle_adolescent_vaccination</a></strong>`"]
        direction LR
        n120["`adolescent_vax_county.parquet`"]
        n121["`adolescent_vax_demographics.parquet`"]
        n122["`adolescent_vax_state.parquet`"]
    end
    subgraph bundle_antimicrobial_resistance["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_antimicrobial_resistance" target="_blank" rel="noreferrer">bundle_antimicrobial_resistance</a></strong>`"]
        direction LR
        n123["`resistance_by_agent.parquet`"]
        n124["`resistance_by_pattern.parquet`"]
    end
    subgraph bundle_cancer_screening["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_cancer_screening" target="_blank" rel="noreferrer">bundle_cancer_screening</a></strong>`"]
        direction LR
        n125["`cms_cancer_screening_by_race.parquet`"]
        n126["`cms_cancer_screening_by_sex.parquet`"]
        n127["`cms_cancer_screening_state.parquet`"]
        n128["`combined_cancer_screening.parquet`"]
        n129["`medicaid_cancer_screening.parquet`"]
        n130["`nccr_incidence.parquet`"]
    end
    subgraph bundle_census["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_census" target="_blank" rel="noreferrer">bundle_census</a></strong>`"]
        direction LR
        n131["`census_county.parquet`"]
        n132["`census_state.parquet`"]
    end
    subgraph bundle_childhood_immunizations["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_childhood_immunizations" target="_blank" rel="noreferrer">bundle_childhood_immunizations</a></strong>`"]
        direction LR
        n133["`nis_insurance.parquet`"]
        n134["`nis_overall.parquet`"]
        n135["`nis_urban.parquet`"]
        n136["`overall_rates_by_source.parquet`"]
        n137["`schoolvaxview_exemptions.parquet`"]
        n138["`schoolvaxview_overall.parquet`"]
        n139["`state_compare.parquet`"]
        n140["`wapo_vax_counties.parquet`"]
        n141["`wapo_vax_schools.parquet`"]
    end
    subgraph bundle_chronic_diseases["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_chronic_diseases" target="_blank" rel="noreferrer">bundle_chronic_diseases</a></strong>`"]
        direction LR
        n142["`brfss_prevalence_by_geography.parquet`"]
        n143["`epic_prevalence_by_geography_county_and_source.parquet`"]
        n144["`epic_prevalence_by_geography_year.parquet`"]
        n145["`prevalence_by_geography_and_source.csv`"]
        n146["`prevalence_by_geography_and_year_and_source.parquet`"]
    end
    subgraph bundle_county_access["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_county_access" target="_blank" rel="noreferrer">bundle_county_access</a></strong>`"]
        direction LR
        n147["`county_access.parquet`"]
        n148["`county_determinants.parquet`"]
        n149["`state_determinants.parquet`"]
    end
    subgraph bundle_county_chronic["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_county_chronic" target="_blank" rel="noreferrer">bundle_county_chronic</a></strong>`"]
        direction LR
        n150["`county_chronic.parquet`"]
    end
    subgraph bundle_enteric_diseases["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_enteric_diseases" target="_blank" rel="noreferrer">bundle_enteric_diseases</a></strong>`"]
        direction LR
        n151["`enteric_diseases.parquet`"]
        n152["`epic_diarrhea.parquet`"]
        n153["`epic_health_alerts.parquet`"]
        n154["`resistance_by_agent.parquet`"]
        n155["`resistance_by_pattern.parquet`"]
    end
    subgraph bundle_injury_overdose["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_injury_overdose" target="_blank" rel="noreferrer">bundle_injury_overdose</a></strong>`"]
        direction LR
        n156["`county_opioid_by_source.parquet`"]
        n157["`deaths_cause_age_demographics.parquet`"]
        n158["`deaths_cause_age.parquet`"]
        n159["`firearms_by_demographics.parquet`"]
        n160["`firearms_by_geography_and_source_state_year.parquet`"]
        n161["`firearms_geography_source.parquet`"]
        n162["`google_dma.parquet`"]
        n163["`heat_by_geography_and_source_state_year.parquet`"]
        n164["`heat_risk.parquet`"]
        n165["`medicaid_injury_overdose.parquet`"]
        n166["`overdose_by_demographics.parquet`"]
        n167["`overdose_by_geography_and_source_county.parquet`"]
        n168["`overdose_by_geography_and_source_state_year.parquet`"]
        n169["`overdose_by_geography_and_source.parquet`"]
        n170["`overdose_deaths_county.parquet`"]
        n171["`overdose_deaths_state.parquet`"]
        n172["`state_opioid_by_source.parquet`"]
    end
    subgraph bundle_maternal_health["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_maternal_health" target="_blank" rel="noreferrer">bundle_maternal_health</a></strong>`"]
        direction LR
        n173["`maternal_county.parquet`"]
        n174["`maternal_mortality.parquet`"]
        n175["`maternal_state.parquet`"]
    end
    subgraph bundle_measles["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_measles" target="_blank" rel="noreferrer">bundle_measles</a></strong>`"]
        direction LR
        n176["`measles_cases_by_age.parquet`"]
        n177["`measles_county.parquet`"]
        n178["`measles_state.parquet`"]
    end
    subgraph bundle_preventative_services["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_preventative_services" target="_blank" rel="noreferrer">bundle_preventative_services</a></strong>`"]
        direction LR
        n179["`cms_preventative_services_by_race.parquet`"]
        n180["`cms_preventative_services_by_sex.parquet`"]
        n181["`cms_preventative_services_state.parquet`"]
        n182["`combined_preventative_services.parquet`"]
        n183["`medicaid_preventative_services.parquet`"]
    end
    subgraph bundle_respiratory["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_respiratory" target="_blank" rel="noreferrer">bundle_respiratory</a></strong>`"]
        direction LR
        n184["`abcs_strep.parquet`"]
        n185["`covid_ed_visits_by_county.parquet`"]
        n186["`covid_overall_trends.parquet`"]
        n187["`covid_trends_by_age.parquet`"]
        n188["`flu_ed_visits_by_county.parquet`"]
        n189["`flu_overall_trends.parquet`"]
        n190["`flu_trends_by_age.parquet`"]
        n191["`gas_state.parquet`"]
        n192["`other_measures_trends.parquet`"]
        n193["`pneumococcus_by_geography_year.parquet`"]
        n194["`pneumococcus_by_geography.parquet`"]
        n195["`pneumococcus_comparison.parquet`"]
        n196["`pneumococcus_serotype_trends.parquet`"]
        n197["`rsv_ed_visits_by_county.parquet`"]
        n198["`rsv_google_dma.parquet`"]
        n199["`rsv_overall_trends.parquet`"]
        n200["`rsv_positive_tests.parquet`"]
        n201["`rsv_testing_pct.parquet`"]
        n202["`rsv_trends_by_age.parquet`"]
    end
    subgraph bundle_sti["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_sti" target="_blank" rel="noreferrer">bundle_sti</a></strong>`"]
        direction LR
        n203["`sti_county.parquet`"]
        n204["`sti_quarterly.parquet`"]
        n205["`sti_state.parquet`"]
        n206["`sti_weekly.parquet`"]
        n207["`sti_youth.parquet`"]
    end
    subgraph bundle_vector_borne["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_vector_borne" target="_blank" rel="noreferrer">bundle_vector_borne</a></strong>`"]
        direction LR
        n208["`vector_borne.parquet`"]
    end
    subgraph bundle_youth_wellbeing["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_youth_wellbeing" target="_blank" rel="noreferrer">bundle_youth_wellbeing</a></strong>`"]
        direction LR
        n209["`chr_county.parquet`"]
        n210["`chr_state.parquet`"]
        n211["`epic_chronic_county_age.parquet`"]
        n212["`epic_chronic_state_age.parquet`"]
        n213["`epic_injury_state_age_month.parquet`"]
        n214["`epic_injury_state_age_year.parquet`"]
        n215["`immunizations_state_age_vaccine.parquet`"]
        n216["`medicaid_state_payer.parquet`"]
        n217["`neiss_diagnosis_age_sex_year.parquet`"]
        n218["`neiss_product_age_sex_year.parquet`"]
        n219["`nhtsa_county_age_sex.parquet`"]
        n220["`nhtsa_state_age_sex.parquet`"]
        n221["`noaa_heat_risk_county.parquet`"]
        n222["`noaa_heat_risk_state.parquet`"]
        n223["`wisqars_state_age_demographics.parquet`"]
        n224["`yrbss_state_age_demographics.parquet`"]
    end
    s0---s1["<strong><a href="https://data.cdc.gov/resource/qvzb-qs6p/" target="_blank" rel="noreferrer">Serotype Data for Invasive Pneumococcal Disease Cases by Age Group from Active Bacterial Core surveillance</a></strong>"]
    s1 --> n1
    s1 --> n2
    s2---s3["<strong><a href="https://www.cdc.gov/abcs/reports/" target="_blank" rel="noreferrer">CDC ABCs surveillance reports</a></strong>"]
    s3 --> n2
    s1 --> n3
    s3 --> n3
    s1 --> n4
    s3 --> n4
    s1 --> n5
    s3 --> n5
    s1 --> n6
    s3 --> n6
    s1 --> n7
    s3 --> n7
    s1 --> n8
    s3 --> n8
    s1 --> n9
    s3 --> n9
    s1 --> n10
    s5---s6["<strong><a href="https://pubmed.ncbi.nlm.nih.gov/39758745/" target="_blank" rel="noreferrer">Open Forum for Infectious Diseases</a></strong>"]
    s6 --> n10
    s7---s8["<strong><a href="https://www.cdc.gov/west-nile-virus/data-maps/historic-data.html" target="_blank" rel="noreferrer">CDC arboviral historic data dashboards</a></strong>"]
    s8 --> n11
    s8 --> n12
    s9---s10["<strong><a href="https://data.hrsa.gov/topics/health-workforce/ahrf" target="_blank" rel="noreferrer">AHRF County-Level Data Files</a></strong>"]
    s10 --> n13
    s11---s12["<strong><a href="https://www.cdc.gov/beam/dashboard/" target="_blank" rel="noreferrer">BEAM (Bacteria, Enterics, Amoeba, and Mycotics) Dashboard</a></strong>"]
    s12 --> n14
    s13---s14["<strong><a href="https://www.bls.gov/developers/api_signature_v2.htm" target="_blank" rel="noreferrer">BLS API v2 — api.bls.gov/publicAPI/v2/timeseries/data/</a></strong>"]
    s14 --> n15
    s14 --> n16
    s15---s16["<strong><a href="https://data.cdc.gov/Behavioral-Risk-Factors/Behavioral-Risk-Factor-Surveillance-System-BRFSS-P/dttw-5yxu/about_data" target="_blank" rel="noreferrer">Behavioral Risk Factor Surveillance System (BRFSS) Prevalence Data (2011 to present)</a></strong>"]
    s16 --> n17
    s16 --> n18
    s17 --> n19
    s18 --> n20
    s19---s20["<strong><a href="https://wonder.cdc.gov/natality-expanded-current.html" target="_blank" rel="noreferrer">Natality, 2016-2024 expanded (Single Race), database D149</a></strong>"]
    s20 --> n21
    s20 --> n22
    s21---s22["<strong><a href="https://api.census.gov/data.html" target="_blank" rel="noreferrer">Census API — ACS 5-Year Detailed Tables and Subject Tables</a></strong>"]
    s22 --> n23
    s23---s24["<strong><a href="https://www2.census.gov/geo/docs/reference/ua/2020_UA_COUNTY.xlsx" target="_blank" rel="noreferrer">2020 Census Urban Area to County Allocation File (XLSX)</a></strong>"]
    s24 --> n23
    s25---s26["<strong><a href="https://www2.census.gov/programs-surveys/decennial/2020/data/operational-quality-metrics/census-operational-quality-metrics-release_4.xlsx" target="_blank" rel="noreferrer">Release 4 county-level file (October 2022)</a></strong>"]
    s26 --> n24
    s27---s28["<strong><a href="https://api.census.gov/data/2023/pep/charv.html" target="_blank" rel="noreferrer">Census API — pep/charv</a></strong>"]
    s28 --> n25
    s29---s30["<strong><a href="https://api.census.gov/data/timeseries/healthins/sahie.html" target="_blank" rel="noreferrer">Census API — timeseries/healthins/sahie</a></strong>"]
    s30 --> n26
    s31---s32["<strong><a href="https://api.census.gov/data/timeseries/poverty/saipe.html" target="_blank" rel="noreferrer">Census API — timeseries/poverty/saipe</a></strong>"]
    s32 --> n27
    s22 --> n28
    s33 --> n29
    s33 --> n30
    s34---s35["<strong><a href="https://data.cms.gov/tools/mapping-medicare-disparities-by-population" target="_blank" rel="noreferrer">Mapping Medicare Disparities by Population Tool</a></strong>"]
    s35 --> n31
    s36 --> n31
    s35 --> n32
    s36 --> n32
    s35 --> n33
    s36 --> n33
    s37 --> n34
    s38---s39["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/" target="_blank" rel="noreferrer">Epidata API, claims_outpatient source</a></strong>"]
    s39 --> n35
    s39 --> n36
    s41---s42["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/fluview.html" target="_blank" rel="noreferrer">FluView API</a></strong>"]
    s42 --> n37
    s43 --> n37
    s39 --> n37
    s39 --> n38
    s46 --> n39
    s46 --> n40
    s46 --> n41
    s46 --> n42
    s46 --> n43
    s46 --> n44
    s46 --> n45
    s47 --> n46
    s46 --> n47
    s46 --> n48
    s46 --> n49
    s46 --> n50
    s46 --> n51
    s46 --> n52
    s48---s49["<strong><a href="https://github.com/DISSC-yale/gtrends_collection" target="_blank" rel="noreferrer">Yale Data-Intensive Social Sciences, Google Trends Collection Framework</a></strong>"]
    s49 --> n53
    s49 --> n54
    s49 --> n55
    s49 --> n56
    s50---s51["<strong><a href="https://www.huduser.gov/portal/datasets/cp.html" target="_blank" rel="noreferrer">CHAS county-level (sumlevel 050) CSV download</a></strong>"]
    s51 --> n57
    s51 --> n58
    s52---s53["<strong><a href="https://apiv2.kinsainsights.com/api/v1/docs" target="_blank" rel="noreferrer">Kinsa Insights API - Signal Endpoint</a></strong>"]
    s53 --> n59
    s54 --> n60
    s55 --> n61
    s56 --> n62
    s56 --> n63
    s56 --> n64
    s57---s58["<strong><a href="https://data.medicaid.gov/datasets?theme%5B0%5D=Quality" target="_blank" rel="noreferrer">Medicaid.gov Open Data – Quality Measures datasets (2014–2023)</a></strong>"]
    s58 --> n65
    s59 --> n66
    s59 --> n67
    s59 --> n68
    s60---s61["<strong><a href="https://app.powerbigov.us/view?r=eyJrIjoiZmU5ZjA2ZDItNTU0MS00M2EzLWEyZmQtZmY3Y2RlZjdjYTdjIiwidCI6IjljZTcwODY5LTYwZGItNDRmZC1hYmU4LWQyNzY3MDc3ZmM4ZiJ9" target="_blank" rel="noreferrer">NARMS Now Interactive Dashboard - Human Data</a></strong>"]
    s61 --> n69
    s62 --> n69
    s63 --> n69
    s64 --> n69
    s61 --> n70
    s62 --> n70
    s63 --> n70
    s64 --> n70
    s61 --> n71
    s62 --> n71
    s63 --> n71
    s64 --> n71
    s61 --> n72
    s62 --> n72
    s63 --> n72
    s64 --> n72
    s61 --> n73
    s62 --> n73
    s63 --> n73
    s64 --> n73
    s65---s66["<strong><a href="https://nccrexplorer.ccdi.cancer.gov/application.html" target="_blank" rel="noreferrer">NCCR*Explorer: An interactive website for NCCR cancer statistics</a></strong>"]
    s66 --> n74
    s67 --> n75
    s68 --> n75
    s69 --> n76
    s67 --> n77
    s68 --> n77
    s70---s71["<strong><a href="https://www.cpsc.gov/cgibin/NEISSQuery/" target="_blank" rel="noreferrer">NEISS public query / archived data files</a></strong>"]
    s71 --> n78
    s71 --> n79
    s71 --> n80
    s71 --> n81
    s71 --> n82
    s71 --> n83
    s71 --> n84
    s71 --> n85
    s72---s73["<strong><a href="https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/" target="_blank" rel="noreferrer">NHTSA File Downloads — FARS National CSV archives</a></strong>"]
    s73 --> n86
    s73 --> n87
    s73 --> n88
    s73 --> n89
    s74---s75["<strong><a href="https://data.cdc.gov/d/ee48-w5t6" target="_blank" rel="noreferrer">Vaccination Coverage among Adolescents (13-17 Years), TeenVaxView</a></strong>"]
    s75 --> n90
    s75 --> n91
    s75 --> n92
    s75 --> n93
    s75 --> n94
    s76 --> n95
    s77---s78["<strong><a href="https://www.cdc.gov/nis/about/index.html" target="_blank" rel="noreferrer">About the National Immunization Surveys (NIS)</a></strong>"]
    s78 --> n95
    s76 --> n96
    s78 --> n96
    s76 --> n97
    s78 --> n97
    s79 --> n98
    s80---s81["<strong><a href="https://www.wpc.ncep.noaa.gov/heatrisk/data.html" target="_blank" rel="noreferrer">HeatRisk GeoTIFF Archive and 7-Day Forecast</a></strong>"]
    s81 --> n99
    s81 --> n100
    s82---s83["<strong><a href="https://data.cdc.gov/resource/3cxc-4k8q" target="_blank" rel="noreferrer">Percent Positivity of Respiratory Syncytial Virus Nucleic Acid Amplification Tests by HHS Region, National Respiratory and Enteric Virus Surveillance System</a></strong>"]
    s83 --> n101
    s84 --> n101
    s85---s86["<strong><a href="https://data.cdc.gov/resource/rdmq-nq56" target="_blank" rel="noreferrer">National Syndromic Surveillance Program</a></strong>"]
    s86 --> n102
    s87---s88["<strong><a href="https://healthdata.gov/CDC/Weekly-Rates-of-Laboratory-Confirmed-COVID-19-Hosp/gk5r-vjtt/about_data" target="_blank" rel="noreferrer">Weekly Rates of Laboratory-Confirmed COVID-19 Hospitalizations from the COVID-NET Surveillance System</a></strong>"]
    s88 --> n103
    s87---s89["<strong><a href="https://data.cdc.gov/Public-Health-Surveillance/Weekly-Rates-of-Laboratory-Confirmed-RSV-Hospitali/29hc-w46k/about_data" target="_blank" rel="noreferrer">Weekly Rates of Laboratory-Confirmed RSV Hospitalizations from the RSV-NET Surveillance System</a></strong>"]
    s89 --> n103
    s87---s90["<strong><a href="https://data.cdc.gov/Public-Health-Surveillance/Rates-of-Laboratory-Confirmed-RSV-COVID-19-and-Flu/kvib-3txy/about_data" target="_blank" rel="noreferrer">Rates of Laboratory-Confirmed RSV, COVID-19, and Flu Hospitalizations from the RESP-NET Surveillance Systems</a></strong>"]
    s90 --> n103
    s91---s92["<strong><a href="https://github.com/PopHIVE/school_immunizations/blob/main/data/DATA_SOURCES.md" target="_blank" rel="noreferrer">Per-state standard files and source notes</a></strong>"]
    s92 --> n104
    s93 --> n105
    s94 --> n105
    s93 --> n106
    s95---s96["<strong><a href="https://data.cdc.gov/Vaccinations/Vaccination-Coverage-and-Exemptions-among-Kinderga/ijqb-a7ye/about_data" target="_blank" rel="noreferrer">Vaccination Coverage and Exemptions among Kindergartners</a></strong>"]
    s96 --> n107
    s96 --> n108
    s97---s98["<strong><a href="https://www.ers.usda.gov/data-products/food-environment-atlas/data-access-and-documentation-downloads" target="_blank" rel="noreferrer">Food Environment Atlas data download</a></strong>"]
    s98 --> n109
    s99 --> n110
    s99 --> n111
    s99 --> n112
    s100 --> n113
    s100 --> n114
    s101---s102["<strong><a href="https://data.cdc.gov/Public-Health-Surveillance/CDC-Wastewater-Viral-Activity-Level-for-SARS-CoV-2/atcp-73re/" target="_blank" rel="noreferrer">CDC Wastewater Viral Activity Level for SARS-CoV-2, Influenza A and RSV</a></strong>"]
    s102 --> n115
    s103---s104["<strong><a href="https://wisqars.cdc.gov/reports/?o=MORT&i=8&m=20810&s=0&r=0&ry=2&y1=2018&y2=2023&a=ALL&g1=0&g2=199&a1=0&a2=199&r1=MECH&r2=AGEGP&r3=STATE&r4=YEAR&r5=NONE&r6=NONE&g=00&e=0&yp=65&me=0&t=0" target="_blank" rel="noreferrer">Fatal Injury Report</a></strong>"]
    s104 --> n116
    s105 --> n117
    s105 --> n118
    s105 --> n119
    n65 --> bundle_adolescent_vaccination
    n94 --> bundle_adolescent_vaccination
    n90 --> bundle_adolescent_vaccination
    n91 --> bundle_adolescent_vaccination
    n92 --> bundle_adolescent_vaccination
    n93 --> bundle_adolescent_vaccination
    n104 --> bundle_adolescent_vaccination
    n71 --> bundle_antimicrobial_resistance
    n72 --> bundle_antimicrobial_resistance
    n73 --> bundle_antimicrobial_resistance
    n69 --> bundle_antimicrobial_resistance
    n70 --> bundle_antimicrobial_resistance
    n33 --> bundle_cancer_screening
    n32 --> bundle_cancer_screening
    n31 --> bundle_cancer_screening
    n65 --> bundle_cancer_screening
    n74 --> bundle_cancer_screening
    n28 --> bundle_census
    n23 --> bundle_census
    n25 --> bundle_census
    n27 --> bundle_census
    n26 --> bundle_census
    n24 --> bundle_census
    n108 --> bundle_childhood_immunizations
    n107 --> bundle_childhood_immunizations
    n97 --> bundle_childhood_immunizations
    n96 --> bundle_childhood_immunizations
    n95 --> bundle_childhood_immunizations
    n105 --> bundle_childhood_immunizations
    n106 --> bundle_childhood_immunizations
    n17 --> bundle_chronic_diseases
    n42 --> bundle_chronic_diseases
    n40 --> bundle_chronic_diseases
    n33 --> bundle_chronic_diseases
    n57 --> bundle_county_access
    n58 --> bundle_county_access
    n13 --> bundle_county_access
    n15 --> bundle_county_access
    n16 --> bundle_county_access
    n109 --> bundle_county_access
    n28 --> bundle_county_access
    n23 --> bundle_county_access
    n26 --> bundle_county_access
    n27 --> bundle_county_access
    n24 --> bundle_county_access
    n98 --> bundle_enteric_diseases
    n14 --> bundle_enteric_diseases
    n71 --> bundle_enteric_diseases
    n73 --> bundle_enteric_diseases
    n69 --> bundle_enteric_diseases
    n70 --> bundle_enteric_diseases
    n72 --> bundle_enteric_diseases
    n44 --> bundle_enteric_diseases
    n46 --> bundle_enteric_diseases
    n116 --> bundle_injury_overdose
    n33 --> bundle_injury_overdose
    n77 --> bundle_injury_overdose
    n75 --> bundle_injury_overdose
    n55 --> bundle_injury_overdose
    n48 --> bundle_injury_overdose
    n49 --> bundle_injury_overdose
    n65 --> bundle_injury_overdose
    n53 --> bundle_injury_overdose
    n28 --> bundle_maternal_health
    n23 --> bundle_maternal_health
    n65 --> bundle_maternal_health
    n22 --> bundle_maternal_health
    n20 --> bundle_maternal_health
    n114 --> bundle_measles
    n112 --> bundle_measles
    n63 --> bundle_measles
    n67 --> bundle_measles
    n61 --> bundle_measles
    n98 --> bundle_measles
    n62 --> bundle_measles
    n66 --> bundle_measles
    n110 --> bundle_measles
    n105 --> bundle_measles
    n113 --> bundle_measles
    n106 --> bundle_measles
    n60 --> bundle_measles
    n65 --> bundle_preventative_services
    n33 --> bundle_preventative_services
    n32 --> bundle_preventative_services
    n31 --> bundle_preventative_services
    n52 --> bundle_respiratory
    n50 --> bundle_respiratory
    n51 --> bundle_respiratory
    n56 --> bundle_respiratory
    n54 --> bundle_respiratory
    n102 --> bundle_respiratory
    n103 --> bundle_respiratory
    n115 --> bundle_respiratory
    n35 --> bundle_respiratory
    n36 --> bundle_respiratory
    n38 --> bundle_respiratory
    n37 --> bundle_respiratory
    n59 --> bundle_respiratory
    n101 --> bundle_respiratory
    n1 --> bundle_respiratory
    n10 --> bundle_respiratory
    n8 --> bundle_respiratory
    n7 --> bundle_respiratory
    n9 --> bundle_respiratory
    n3 --> bundle_respiratory
    n2 --> bundle_respiratory
    n6 --> bundle_respiratory
    n5 --> bundle_respiratory
    n4 --> bundle_respiratory
    n19 --> bundle_respiratory
    n76 --> bundle_respiratory
    n98 --> bundle_respiratory
    n65 --> bundle_sti
    n33 --> bundle_sti
    n76 --> bundle_sti
    n98 --> bundle_sti
    n119 --> bundle_sti
    n118 --> bundle_sti
    n117 --> bundle_sti
    n12 --> bundle_vector_borne
    n98 --> bundle_vector_borne
    n11 --> bundle_vector_borne
    n116 --> bundle_youth_wellbeing
    n86 --> bundle_youth_wellbeing
    n119 --> bundle_youth_wellbeing
    n118 --> bundle_youth_wellbeing
    n117 --> bundle_youth_wellbeing
    n49 --> bundle_youth_wellbeing
    n48 --> bundle_youth_wellbeing
    n42 --> bundle_youth_wellbeing
    n40 --> bundle_youth_wellbeing
    n65 --> bundle_youth_wellbeing
    n100 --> bundle_youth_wellbeing
    n99 --> bundle_youth_wellbeing
    n81 --> bundle_youth_wellbeing
    n85 --> bundle_youth_wellbeing
    n79 --> bundle_youth_wellbeing
    n83 --> bundle_youth_wellbeing
```
