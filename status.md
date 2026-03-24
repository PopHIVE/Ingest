```mermaid
flowchart LR
    classDef pass stroke:#66bb6a
    classDef warn stroke:#ffa726
    classDef fail stroke:#f44336
    s0(("<strong><a href="https://www.cdc.gov/abcs/index.html" target="_blank" rel="noreferrer">Active Bacterial Core surveillance (ABCs)</a></strong>"))
    s2(("<strong><a href="https://pubmed.ncbi.nlm.nih.gov/39758745/" target="_blank" rel="noreferrer">Serotype-Specific Urinary Antigen Detection (SSUAD) Study</a></strong>"))
    s4(("<strong><a href="https://www.cdc.gov/brfss/index.html" target="_blank" rel="noreferrer">Behavioral Risk Factor Surveillance System (BRFSS)</a></strong>"))
    s6(("<strong><a href="https://www.census.gov/programs-surveys/acs/data.html" target="_blank" rel="noreferrer">2024 American Community Survey 5-Year Estimates, Powered by Metopio</a></strong>"))
    s8(("<strong><a href="https://data.cdc.gov" target="_blank" rel="noreferrer">Center of Medicare and Medicaid Services (CMS)</a></strong>"))
    s10(("<strong><a href="https://data.cms.gov/tools/mapping-medicare-disparities-by-population" target="_blank" rel="noreferrer">Mapping Medicare Disparities by Population Tool</a></strong>"))
    s11(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/doctor-visits.html" target="_blank" rel="noreferrer">CMU Delphi COVIDcast - Doctor Visits</a></strong>"))
    s13(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/" target="_blank" rel="noreferrer">CMU Delphi</a></strong>"))
    s15(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/hospital-admissions.html" target="_blank" rel="noreferrer">CMU Delphi COVIDcast - Hospital Admissions</a></strong>"))
    s16(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/" target="_blank" rel="noreferrer">CMU Delphi Epidata</a></strong>"))
    s18(("<strong><a href="https://www.cdc.gov/flu/weekly/overview.htm" target="_blank" rel="noreferrer">CDC ILINet</a></strong>"))
    s19(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/fluview.html" target="_blank" rel="noreferrer">CMU Delphi Epidata - FluView (ILINet)</a></strong>"))
    s20(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/nhsn.html" target="_blank" rel="noreferrer">CMU Delphi COVIDcast - NHSN Respiratory Hospitalizations</a></strong>"))
    s21(("<strong><a href="https://cosmos.epic.com/" target="_blank" rel="noreferrer">Epic Cosmos</a></strong>"))
    s22(("<strong><a href="https://trends.google.com" target="_blank" rel="noreferrer">Google Trends</a></strong>"))
    s24(("<strong><a href="https://www.cdc.gov/measles/data-research/index.html" target="_blank" rel="noreferrer">CDC Measles Cases and Outbreaks - Age and Vaccination Status</a></strong>"))
    s25(("<strong><a href="https://github.com/CSSEGISandData/measles_data" target="_blank" rel="noreferrer">Johns Hopkins University Measles Tracking Team</a></strong>"))
    s26(("<strong><a href="https://data.medicaid.gov/datasets?theme%5B0%5D=Quality" target="_blank" rel="noreferrer">Medicaid and CHIP Adult and Child Core Set Quality Measures</a></strong>"))
    s28(("<strong><a href="https://github.com/eric-gengzhou/MMR_vaccine_estimates" target="_blank" rel="noreferrer">HealthMap MMR Vaccine Coverage Estimates</a></strong>"))
    s29(("<strong><a href="https://data.cdc.gov/National-Center-for-Health-Statistics/VSRR-Provisional-Drug-Overdose-Death-Counts/xkb8-kh2a/about_data" target="_blank" rel="noreferrer">VSRR Provisional Drug Overdose Death Counts</a></strong>"))
    s30(("<strong><a href="" target="_blank" rel="noreferrer">NVSS - 21 Cause of Death Groupings (state-level)</a></strong>"))
    s31(("<strong><a href="https://www.cdc.gov/nis/about/index.html" target="_blank" rel="noreferrer">National Immunization Survey (NIS)</a></strong>"))
    s32(("<strong><a href="https://www.cdc.gov/nis/about/index.html" target="_blank" rel="noreferrer">National Immunization Survey</a></strong>"))
    s34(("<strong><a href="https://www.cdc.gov/nndss/" target="_blank" rel="noreferrer">National Notifiable Diseases Surveillance System (NNDSS)</a></strong>"))
    s35(("<strong><a href="https://data.cdc.gov" target="_blank" rel="noreferrer">Centers for Disease Control and Prevention</a></strong>"))
    s37(("<strong><a href="https://data.cdc.gov/resource/3cxc-4k8q" target="_blank" rel="noreferrer">National Respiratory and Enteric Virus Surveillance System (NREVSS)</a></strong>"))
    s38(("<strong><a href="https://www.cdc.gov/nssp/index.html" target="_blank" rel="noreferrer">National Syndromic Surveillance Program (NSSP)</a></strong>"))
    s40(("<strong><a href="https://github.com/washingtonpost/data-school-vaccination-rates" target="_blank" rel="noreferrer">Washington Post School Vaccination Rates</a></strong>"))
    s41(("<strong><a href="https://www.cdc.gov/schoolvaxview/index.html" target="_blank" rel="noreferrer">SchoolVaxView</a></strong>"))
    s43(("<strong><a href="https://www.cdc.gov/nwss" target="_blank" rel="noreferrer">National Wastewater Surveillance System</a></strong>"))
    s47(("<strong><a href="https://wisqars.cdc.gov/" target="_blank" rel="noreferrer">Web-based Injury Statistics Query and Reporting System (WISQARS)</a></strong>"))
    subgraph abcs["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/abcs" target="_blank" rel="noreferrer">abcs</a></strong>`"]
        direction LR
        n1["`data.csv.gz`"]:::pass
        n2["`uad.csv.gz`"]:::pass
    end
    subgraph atlas_amr["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/atlas_amr" target="_blank" rel="noreferrer">atlas_amr</a></strong>`"]
        direction LR
    end
    subgraph brfss["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/brfss" target="_blank" rel="noreferrer">brfss</a></strong>`"]
        direction LR
        n3["`data_survey.csv.gz`"]:::pass
        n4["`data.csv.gz`"]:::pass
    end
    subgraph census["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/census" target="_blank" rel="noreferrer">census</a></strong>`"]
        direction LR
        n5["`data_county.csv.gz<br/><br/><ul><li><code>missing_info: value</code></li></ul>`"]:::warn
        n6["`data_state.csv.gz<br/><br/><ul><li><code>missing_info: value</code></li></ul>`"]:::warn
        n7["`data_zcta_2019_2020.csv-MWMJ0G3P8D.gz<br/><br/><ul><li><code>missing_info: geography_zcta</code></li></ul>`"]:::warn
        n8["`data_zcta_2019_2020.csv.gz<br/><br/><ul><li><code>missing_info: geography_zcta</code></li></ul>`"]:::warn
        n9["`data_zcta_2021_2022.csv.gz<br/><br/><ul><li><code>missing_info: geography_zcta</code></li></ul>`"]:::warn
        n10["`data_zcta_2023_2024.csv.gz<br/><br/><ul><li><code>missing_info: geography_zcta</code></li></ul>`"]:::warn
        n11["`data.csv.gz<br/><br/><ul><li><code>missing_info: version https://git-lfs.github.com/spec/v1</code></li></ul>`"]:::warn
    end
    subgraph cms_mmd["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/cms_mmd" target="_blank" rel="noreferrer">cms_mmd</a></strong>`"]
        direction LR
        n12["`data_state_county_age_by_race.csv.gz`"]:::pass
        n13["`data_state_county_age_by_sex.csv.gz`"]:::pass
        n14["`data_state_county_age.csv.gz`"]:::pass
    end
    subgraph delphi_doctors_claims["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_doctors_claims" target="_blank" rel="noreferrer">delphi_doctors_claims</a></strong>`"]
        direction LR
        n15["`data.csv.gz`"]:::pass
    end
    subgraph delphi_hospital_claims["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_hospital_claims" target="_blank" rel="noreferrer">delphi_hospital_claims</a></strong>`"]
        direction LR
        n16["`data.csv.gz`"]:::pass
    end
    subgraph delphi_ili_fluview["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_ili_fluview" target="_blank" rel="noreferrer">delphi_ili_fluview</a></strong>`"]
        direction LR
        n17["`data.csv.gz`"]:::pass
    end
    subgraph delphi_nhsn["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_nhsn" target="_blank" rel="noreferrer">delphi_nhsn</a></strong>`"]
        direction LR
        n18["`data.csv.gz`"]:::pass
    end
    subgraph epic_chronic["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_chronic" target="_blank" rel="noreferrer">epic_chronic</a></strong>`"]
        direction LR
        n19["`county_no_time.csv.gz<br/><br/><ul><li><code>missing_info: bmi_30_49.8, obesity_(%), Year</code></li></ul>`"]:::warn
        n20["`county_year.csv.gz<br/><br/><ul><li><code>missing_info: n_patients_chronic</code></li></ul>`"]:::warn
        n21["`state_no_time.csv.gz<br/><br/><ul><li><code>missing_info: bmi_30_49.8, dm_(%), n_patients, Year</code></li></ul>`"]:::warn
        n22["`state_year.csv.gz<br/><br/><ul><li><code>missing_info: n_patients_chronic</code></li></ul>`"]:::warn
    end
    subgraph epic_hepb_vax["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_hepb_vax" target="_blank" rel="noreferrer">epic_hepb_vax</a></strong>`"]
        direction LR
        n23["`data.csv.gz<br/><br/><ul><li><code>missing_info: suppressed_flag</code></li></ul>`"]:::warn
    end
    subgraph epic_injury["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_injury" target="_blank" rel="noreferrer">epic_injury</a></strong>`"]
        direction LR
        n24["`heat_year_county.csv.gz<br/><br/><ul><li><code>missing_info: heat_ed_patients, total_ed_patients, heat_ed_incidence, heat_suppressed, geography_name</code></li></ul>`"]:::warn
        n25["`monthly_injury.csv.gz<br/><br/><ul><li><code>missing_info: epic_n_ed_firearm, epic_rate_ed_firearm, epic_n_ed_heat, epic_rate_ed_heat, suppressed_opioid, suppressed_firearm, suppressed_heat</code></li></ul>`"]:::warn
        n26["`yearly_injury.csv.gz<br/><br/><ul><li><code>missing_info: epic_n_ed_firearm, epic_rate_ed_firearm, epic_n_ed_heat, epic_rate_ed_heat, suppressed_opioid, suppressed_firearm, suppressed_heat</code></li></ul>`"]:::warn
    end
    subgraph epic_resp_infections["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic_resp_infections" target="_blank" rel="noreferrer">epic_resp_infections</a></strong>`"]
        direction LR
        n27["`monthly_tests.csv.gz`"]:::pass
        n28["`no_geo.csv.gz`"]:::pass
        n29["`weekly.csv.gz`"]:::pass
    end
    subgraph gtrends["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/gtrends" target="_blank" rel="noreferrer">gtrends</a></strong>`"]
        direction LR
        n30["`data_dma_year.csv.gz`"]:::pass
        n31["`data_dma.csv.gz`"]:::pass
        n32["`data_year.csv.gz`"]:::pass
        n33["`data.csv.gz`"]:::pass
    end
    subgraph measles_age_cdc2["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/measles_age_cdc2" target="_blank" rel="noreferrer">measles_age_cdc2</a></strong>`"]
        direction LR
        n34["`data.csv.gz<br/><br/><ul><li><code>missing_info: year, week</code></li></ul>`"]:::warn
    end
    subgraph measles_jhu["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/measles_jhu" target="_blank" rel="noreferrer">measles_jhu</a></strong>`"]
        direction LR
        n35["`data_county.csv.gz<br /><br />Script Failed:<br />`"]:::fail
        n36["`data_state.csv.gz<br /><br />Script Failed:<br />`"]:::fail
        n37["`data.csv.gz<br /><br />Script Failed:<br />`"]:::fail
    end
    subgraph medicaid_quality["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/medicaid_quality" target="_blank" rel="noreferrer">medicaid_quality</a></strong>`"]
        direction LR
        n38["`data.csv.gz<br/><br/><ul><li><code>missing_info: geography_level, age, sex, race_ethnicity, payer, domain, medicaid_fum_ch_7d_pct_25, medicaid_fum_ch_7d_pct_75, medicaid_fum_ch_30d_pct_25, medicaid_fum_ch_30d_pct_75, medicaid_fua_ch_7d_pct_25, medicaid_fua_ch_7d_pct_75, medicaid_fua_ch_30d_pct_25, medicaid_fua_ch_30d_pct_75, medicaid_fuh_ch_7d_pct_25, medicaid_fuh_ch_7d_pct_75, medicaid_fuh_ch_30d_pct_25, medicaid_fuh_ch_30d_pct_75, medicaid_add_ch_30d_pct_25, medicaid_add_ch_30d_pct_75, medicaid_add_ch_cont_pct_25, medicaid_add_ch_cont_pct_75, medicaid_apm_ch_gluc_pct_25, medicaid_apm_ch_gluc_pct_75, medicaid_apm_ch_chol_pct_25, medicaid_apm_ch_chol_pct_75, medicaid_apm_ch_gluc_chol_pct_25, medicaid_apm_ch_gluc_chol_pct_75, medicaid_app_ch_pct_25, medicaid_app_ch_pct_75, medicaid_amb_ch_pct_25, medicaid_amb_ch_pct_75, medicaid_amr_ch_pct_25, medicaid_amr_ch_pct_75, medicaid_aab_ch_pct_25, medicaid_aab_ch_pct_75, medicaid_oev_ch_pct_25, medicaid_oev_ch_pct_75, medicaid_sfm_ch_pct_25, medicaid_sfm_ch_pct_75, medicaid_tfl_ch_pct_25, medicaid_tfl_ch_pct_75, medicaid_cpc_ch_pct_25, medicaid_cpc_ch_pct_75, medicaid_ccw_ch_pct_25, medicaid_ccw_ch_pct_75, medicaid_ccp_ch_pct_25, medicaid_ccp_ch_pct_75, medicaid_lbw_ch_pct_25, medicaid_lbw_ch_pct_75, medicaid_lrcd_ch_pct_25, medicaid_lrcd_ch_pct_75, medicaid_ppc_ch_pct_25, medicaid_ppc_ch_pct_75, medicaid_wcv_ch_pct_25, medicaid_wcv_ch_pct_75, medicaid_cis_ch_pct_25, medicaid_cis_ch_pct_75, medicaid_chl_ch_pct_25, medicaid_chl_ch_pct_75, medicaid_dev_ch_pct_25, medicaid_dev_ch_pct_75, medicaid_ima_ch_pct_25, medicaid_ima_ch_pct_75, medicaid_lsc_ch_pct_25, medicaid_lsc_ch_pct_75, medicaid_wcc_ch_pct_25, medicaid_wcc_ch_pct_75, medicaid_w30_ch_pct_25, medicaid_w30_ch_pct_75, medicaid_saa_ad_pct_25, medicaid_saa_ad_pct_75, medicaid_amm_ad_pct_25, medicaid_amm_ad_pct_75, medicaid_amm_ad_cont_pct_25, medicaid_amm_ad_cont_pct_75, medicaid_ssd_ad_pct_25, medicaid_ssd_ad_pct_75, medicaid_fum_ad_7d_pct_25, medicaid_fum_ad_7d_pct_75, medicaid_fum_ad_30d_pct_25, medicaid_fum_ad_30d_pct_75, medicaid_fua_ad_7d_pct_25, medicaid_fua_ad_7d_pct_75, medicaid_fua_ad_30d_pct_25, medicaid_fua_ad_30d_pct_75, medicaid_fuh_ad_7d_pct_25, medicaid_fuh_ad_7d_pct_75, medicaid_fuh_ad_30d_pct_25, medicaid_fuh_ad_30d_pct_75, medicaid_iet_ad_pct_25, medicaid_iet_ad_pct_75, medicaid_msc_ad_pct_25, medicaid_msc_ad_pct_75, medicaid_oud_ad_pct_25, medicaid_oud_ad_pct_75, medicaid_amr_ad_pct_25, medicaid_amr_ad_pct_75, medicaid_aab_ad_pct_25, medicaid_aab_ad_pct_75, medicaid_cob_ad_pct_25, medicaid_cob_ad_pct_75, medicaid_cbp_ad_pct_25, medicaid_cbp_ad_pct_75, medicaid_hbd_ad_pct_25, medicaid_hbd_ad_pct_75, medicaid_pqi01_ad_pct_25, medicaid_pqi01_ad_pct_75, medicaid_pqi05_ad_pct_25, medicaid_pqi05_ad_pct_75, medicaid_pqi08_ad_pct_25, medicaid_pqi08_ad_pct_75, medicaid_pqi15_ad_pct_25, medicaid_pqi15_ad_pct_75, medicaid_ohd_ad_pct_25, medicaid_ohd_ad_pct_75, medicaid_cpa_ad_pct_25, medicaid_cpa_ad_pct_75, medicaid_ncidds_ad_pct_25, medicaid_ncidds_ad_pct_75, medicaid_ccw_ad_pct_25, medicaid_ccw_ad_pct_75, medicaid_ccp_ad_pct_25, medicaid_ccp_ad_pct_75, medicaid_ppc_ad_pct_25, medicaid_ppc_ad_pct_75, medicaid_bcs_ad_pct_25, medicaid_bcs_ad_pct_75, medicaid_ccs_ad_pct_25, medicaid_ccs_ad_pct_75, medicaid_chl_ad_pct_25, medicaid_chl_ad_pct_75, medicaid_col_ad_pct_25, medicaid_col_ad_pct_75, medicaid_fva_ad_pct_25, medicaid_fva_ad_pct_75, medicaid_pcr_ad_pct_25, medicaid_pcr_ad_pct_75, medicaid_add_ch_init_pct_25, medicaid_add_ch_init_pct_75, medicaid_hpc_ad_pct_25, medicaid_hpc_ad_pct_75, medicaid_pdent_ch_pct_25, medicaid_pdent_ch_pct_75, medicaid_seal_ch_pct_25, medicaid_seal_ch_pct_75, medicaid_awc_ch_pct_25, medicaid_awc_ch_pct_75, medicaid_w15_ch_pct_25, medicaid_w15_ch_pct_75, medicaid_w34_ch_pct_25, medicaid_w34_ch_pct_75, medicaid_aba_ad_pct_25, medicaid_aba_ad_pct_75, medicaid_apc_ch_pct_25, medicaid_apc_ch_pct_75, medicaid_cap_ch_pct_25, medicaid_cap_ch_pct_75, medicaid_mpm_ad_pct_25, medicaid_mpm_ad_pct_75, medicaid_ha1c_ad_pct_25, medicaid_ha1c_ad_pct_75, medicaid_fua_fum_ad_7d_pct_25, medicaid_fua_fum_ad_7d_pct_75, medicaid_fua_fum_ad_30d_pct_25, medicaid_fua_fum_ad_30d_pct_75, medicaid_mma_ch_pct_25, medicaid_mma_ch_pct_75, medicaid_fpc_ch_pct_25, medicaid_fpc_ch_pct_75, medicaid_hpv_ch_pct_25, medicaid_hpv_ch_pct_75, medicaid_ldl_ad_pct_25, medicaid_ldl_ad_pct_75, medicaid_tdent_ch_pct_25, medicaid_tdent_ch_pct_75</code></li></ul>`"]:::warn
    end
    subgraph mmr_epic["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/mmr_epic" target="_blank" rel="noreferrer">mmr_epic</a></strong>`"]
        direction LR
    end
    subgraph mmr_healthmap["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/mmr_healthmap" target="_blank" rel="noreferrer">mmr_healthmap</a></strong>`"]
        direction LR
        n39["`data_county.csv.gz`"]:::pass
        n40["`data_state.csv.gz`"]:::pass
        n41["`data_zcta.csv.gz`"]:::pass
    end
    subgraph narms["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/narms" target="_blank" rel="noreferrer">narms</a></strong>`"]
        direction LR
    end
    subgraph nchs_mortality["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nchs_mortality" target="_blank" rel="noreferrer">nchs_mortality</a></strong>`"]
        direction LR
        n42["`data_county.csv.gz`"]:::pass
        n43["`data_state_21_causes.csv.gz`"]:::pass
        n44["`data.csv.gz`"]:::pass
    end
    subgraph nis["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nis" target="_blank" rel="noreferrer">nis</a></strong>`"]
        direction LR
        n45["`data_insurance.csv.gz`"]:::pass
        n46["`data_urban.csv.gz`"]:::pass
        n47["`data.csv.gz`"]:::pass
    end
    subgraph nnds["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nnds" target="_blank" rel="noreferrer">nnds</a></strong>`"]
        direction LR
        n48["`data.csv.gz<br/><br/><ul><li><code>missing_info: mmwr_year, mmwr_week, anthrax, cholera, plague, rabies_human, rubella_congenital_syndrome, novel_influenza_a_virus_infections_total, novel_influenza_a_virus_infections_confirmed</code></li></ul><br />Script Failed:<br />In argument: 'time = +...'.`"]:::fail
    end
    subgraph NREVSS["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/NREVSS" target="_blank" rel="noreferrer">NREVSS</a></strong>`"]
        direction LR
        n49["`data.csv.gz`"]:::pass
    end
    subgraph nssp["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nssp" target="_blank" rel="noreferrer">nssp</a></strong>`"]
        direction LR
        n50["`data.csv.gz`"]:::pass
    end
    subgraph schoolvax_washpost["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/schoolvax_washpost" target="_blank" rel="noreferrer">schoolvax_washpost</a></strong>`"]
        direction LR
        n51["`data_counties.csv.gz<br/><br/><ul><li><code>type_changed: geography</code></li></ul>`"]:::warn
        n52["`data_schools.csv.gz`"]:::pass
    end
    subgraph schoolvaxview["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/schoolvaxview" target="_blank" rel="noreferrer">schoolvaxview</a></strong>`"]
        direction LR
        n53["`data_exemptions.csv.gz`"]:::pass
        n54["`data.csv.gz`"]:::pass
    end
    subgraph vaers["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/vaers" target="_blank" rel="noreferrer">vaers</a></strong>`"]
        direction LR
    end
    subgraph wastewater["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wastewater" target="_blank" rel="noreferrer">wastewater</a></strong>`"]
        direction LR
        n55["`data.csv.gz`"]:::pass
    end
    subgraph wisqars["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wisqars" target="_blank" rel="noreferrer">wisqars</a></strong>`"]
        direction LR
        n56["`data.csv.gz`"]:::pass
    end
    subgraph bundle_childhood_immunizations["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_childhood_immunizations" target="_blank" rel="noreferrer">bundle_childhood_immunizations</a></strong>`"]
        direction LR
        n57["`mmr_rates_epic.parquet`"]
        n58["`nis_insurance.parquet`"]
        n59["`nis_overall.parquet`"]
        n60["`nis_urban.parquet`"]
        n61["`overall_rates_by_source.parquet`"]
        n62["`schoolvaxview_exemptions.parquet`"]
        n63["`schoolvaxview_overall.parquet`"]
        n64["`state_compare.parquet`"]
        n65["`wapo_vax_counties.parquet`"]
        n66["`wapo_vax_schools.parquet`"]
    end
    subgraph bundle_chronic_diseases["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_chronic_diseases" target="_blank" rel="noreferrer">bundle_chronic_diseases</a></strong>`"]
        direction LR
        n67["`brfss_prevalence_by_geography.parquet`"]
        n68["`county_opioid_by_source.parquet`"]
        n69["`deaths_cause_age.parquet`"]
        n70["`epic_prevalence_by_geography_county_and_source.parquet`"]
        n71["`epic_prevalence_by_geography_county.parquet`"]
        n72["`epic_prevalence_by_geography_year.parquet`"]
        n73["`epic_prevalence_by_geography.parquet`"]
        n74["`overdose_by_geography_and_source_county.parquet`"]
        n75["`overdose_by_geography_and_source.parquet`"]
        n76["`overdose_deaths_county.parquet`"]
        n77["`overdose_deaths_state.parquet`"]
        n78["`prevalence_by_geography_and_source.csv`"]
        n79["`prevalence_by_geography_and_source.parquet`"]
        n80["`prevalence_by_geography_and_year_and_source.parquet`"]
        n81["`prevalence_by_geography_year_and_source.parquet`"]
    end
    subgraph bundle_injury_overdose["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_injury_overdose" target="_blank" rel="noreferrer">bundle_injury_overdose</a></strong>`"]
        direction LR
        n82["`brfss_prevalence_by_geography.parquet`"]
        n83["`county_opioid_by_source.parquet`"]
        n84["`deaths_cause_age_demographics.parquet`"]
        n85["`deaths_cause_age.parquet`"]
        n86["`epic_prevalence_by_geography_year.parquet`"]
        n87["`firearms_by_demographics.parquet`"]
        n88["`firearms_by_geography_and_source_state_year.parquet`"]
        n89["`firearms_geography_source.parquet`"]
        n90["`google_dma.parquet`"]
        n91["`heat_by_geography_and_source_state_year.parquet`"]
        n92["`heat_related_geography_source.parquet`"]
        n93["`overdose_by_demographics.parquet`"]
        n94["`overdose_by_geography_and_source_county.parquet`"]
        n95["`overdose_by_geography_and_source_state_year.parquet`"]
        n96["`overdose_by_geography_and_source.parquet`"]
        n97["`overdose_deaths_county.parquet`"]
        n98["`overdose_deaths_state.parquet`"]
        n99["`state_opioid_by_source.parquet`"]
    end
    subgraph bundle_measles["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_measles" target="_blank" rel="noreferrer">bundle_measles</a></strong>`"]
        direction LR
        n100["`measles_cases_by_age.parquet`"]
        n101["`measles_county.parquet`"]
        n102["`measles_state.parquet`"]
    end
    subgraph bundle_respiratory["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_respiratory" target="_blank" rel="noreferrer">bundle_respiratory</a></strong>`"]
        direction LR
        n103["`covid_ed_visits_by_county.parquet`"]
        n104["`covid_overall_trends.parquet`"]
        n105["`covid_trends_by_age.parquet`"]
        n106["`flu_ed_visits_by_county.parquet`"]
        n107["`flu_overall_trends.parquet`"]
        n108["`flu_trends_by_age.parquet`"]
        n109["`pneumococcus_by_geography_year.parquet`"]
        n110["`pneumococcus_by_geography.parquet`"]
        n111["`pneumococcus_comparison.parquet`"]
        n112["`pneumococcus_serotype_trends.parquet`"]
        n113["`rsv_ed_visits_by_county.parquet`"]
        n114["`rsv_google_dma.parquet`"]
        n115["`rsv_overall_trends.parquet`"]
        n116["`rsv_positive_tests.parquet`"]
        n117["`rsv_testing_pct.parquet`"]
        n118["`rsv_trends_by_age.parquet`"]
    end
    subgraph measles_cdc["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/measles_cdc" target="_blank" rel="noreferrer">measles_cdc</a></strong>`"]
        direction LR
        n119["`./data/bundle_measles/dist/measles_cases_by_age.parquet`"]
        n120["`./data/bundle_measles/dist/measles_county.parquet`"]
        n121["`./data/bundle_measles/dist/measles_state.parquet`"]
    end
    subgraph respnet["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/respnet" target="_blank" rel="noreferrer">respnet</a></strong>`"]
        direction LR
        n122["`./data/bundle_respiratory/dist/covid_ed_visits_by_county.parquet`"]
        n123["`./data/bundle_respiratory/dist/covid_overall_trends.parquet`"]
        n124["`./data/bundle_respiratory/dist/covid_trends_by_age.parquet`"]
        n125["`./data/bundle_respiratory/dist/flu_ed_visits_by_county.parquet`"]
        n126["`./data/bundle_respiratory/dist/flu_overall_trends.parquet`"]
        n127["`./data/bundle_respiratory/dist/flu_trends_by_age.parquet`"]
        n128["`./data/bundle_respiratory/dist/pneumococcus_by_geography.parquet`"]
        n129["`./data/bundle_respiratory/dist/pneumococcus_by_geography_year.parquet`"]
        n130["`./data/bundle_respiratory/dist/pneumococcus_comparison.parquet`"]
        n131["`./data/bundle_respiratory/dist/pneumococcus_serotype_trends.parquet`"]
        n132["`./data/bundle_respiratory/dist/rsv_ed_visits_by_county.parquet`"]
        n133["`./data/bundle_respiratory/dist/rsv_google_dma.parquet`"]
        n134["`./data/bundle_respiratory/dist/rsv_overall_trends.parquet`"]
        n135["`./data/bundle_respiratory/dist/rsv_positive_tests.parquet`"]
        n136["`./data/bundle_respiratory/dist/rsv_testing_pct.parquet`"]
        n137["`./data/bundle_respiratory/dist/rsv_trends_by_age.parquet`"]
    end
    subgraph vaccine_exemptions_fattah["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/vaccine_exemptions_fattah" target="_blank" rel="noreferrer">vaccine_exemptions_fattah</a></strong>`"]
        direction LR
        n138["`./data/bundle_measles/dist/measles_cases_by_age.parquet`"]
        n139["`./data/bundle_measles/dist/measles_county.parquet`"]
        n140["`./data/bundle_measles/dist/measles_state.parquet`"]
    end
    subgraph wastewater_measles["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wastewater_measles" target="_blank" rel="noreferrer">wastewater_measles</a></strong>`"]
        direction LR
        n141["`./data/bundle_measles/dist/measles_cases_by_age.parquet`"]
        n142["`./data/bundle_measles/dist/measles_county.parquet`"]
        n143["`./data/bundle_measles/dist/measles_state.parquet`"]
    end
    s0---s1["<strong><a href="https://data.cdc.gov/resource/qvzb-qs6p/" target="_blank" rel="noreferrer">Serotype Data for Invasive Pneumococcal Disease Cases by Age Group from Active Bacterial Core surveillance</a></strong>"]
    s1 --> n1
    s1 --> n2
    s2---s3["<strong><a href="https://pubmed.ncbi.nlm.nih.gov/39758745/" target="_blank" rel="noreferrer">Open Forum for Infectious Diseases</a></strong>"]
    s3 --> n2
    s4---s5["<strong><a href="https://data.cdc.gov/Behavioral-Risk-Factors/Behavioral-Risk-Factor-Surveillance-System-BRFSS-P/dttw-5yxu/about_data" target="_blank" rel="noreferrer">Behavioral Risk Factor Surveillance System (BRFSS) Prevalence Data (2011 to present)</a></strong>"]
    s5 --> n3
    s5 --> n4
    s6---s7["<strong><a href="https://api.census.gov/data.html" target="_blank" rel="noreferrer">Census API — ACS 5-Year Detailed Tables and Subject Tables</a></strong>"]
    s7 --> n7
    s7 --> n8
    s7 --> n9
    s7 --> n10
    s8---s9["<strong><a href="https://data.cms.gov/tools/mapping-medicare-disparities-by-population" target="_blank" rel="noreferrer">Mapping Medicare Disparities by Population Tool</a></strong>"]
    s9 --> n12
    s10 --> n12
    s9 --> n13
    s10 --> n13
    s9 --> n14
    s10 --> n14
    s11---s12["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/" target="_blank" rel="noreferrer">COVIDcast Epidata API</a></strong>"]
    s12 --> n15
    s13---s14["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/hospital-admissions.html" target="_blank" rel="noreferrer">COVIDcast > Hospital Admissions</a></strong>"]
    s14 --> n16
    s12 --> n16
    s16---s17["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/fluview.html" target="_blank" rel="noreferrer">FluView API</a></strong>"]
    s17 --> n17
    s18 --> n17
    s12 --> n17
    s12 --> n18
    s21 --> n19
    s21 --> n20
    s21 --> n21
    s21 --> n22
    s21 --> n23
    s21 --> n25
    s21 --> n26
    s21 --> n27
    s21 --> n28
    s21 --> n29
    s22---s23["<strong><a href="https://github.com/DISSC-yale/gtrends_collection" target="_blank" rel="noreferrer">Yale Data-Intensive Social Sciences, Google Trends Collection Framework</a></strong>"]
    s23 --> n30
    s23 --> n31
    s23 --> n32
    s23 --> n33
    s24 --> n34
    s25 --> n35
    s25 --> n36
    s25 --> n37
    s26---s27["<strong><a href="https://data.medicaid.gov/datasets?theme%5B0%5D=Quality" target="_blank" rel="noreferrer">Medicaid.gov Open Data – Quality Measures datasets (2014–2023)</a></strong>"]
    s27 --> n38
    s28 --> n39
    s28 --> n40
    s28 --> n41
    s29 --> n42
    s30 --> n43
    s29 --> n44
    s31 --> n45
    s32---s33["<strong><a href="https://www.cdc.gov/nis/about/index.html" target="_blank" rel="noreferrer">About the National Immunization Surveys (NIS)</a></strong>"]
    s33 --> n45
    s31 --> n46
    s33 --> n46
    s31 --> n47
    s33 --> n47
    s34 --> n48
    s35---s36["<strong><a href="https://data.cdc.gov/resource/3cxc-4k8q" target="_blank" rel="noreferrer">Percent Positivity of Respiratory Syncytial Virus Nucleic Acid Amplification Tests by HHS Region, National Respiratory and Enteric Virus Surveillance System</a></strong>"]
    s36 --> n49
    s37 --> n49
    s38---s39["<strong><a href="https://data.cdc.gov/resource/rdmq-nq56" target="_blank" rel="noreferrer">National Syndromic Surveillance Program</a></strong>"]
    s39 --> n50
    s40 --> n51
    s40 --> n52
    s41---s42["<strong><a href="https://data.cdc.gov/Vaccinations/Vaccination-Coverage-and-Exemptions-among-Kinderga/ijqb-a7ye/about_data" target="_blank" rel="noreferrer">Vaccination Coverage and Exemptions among Kindergartners</a></strong>"]
    s42 --> n53
    s42 --> n54
    s43---s44["<strong><a href="https://www.cdc.gov/nwss/rv/COVID19-statetrend.html" target="_blank" rel="noreferrer">Wastewater COVID-19 State and Territory Trends</a></strong>"]
    s44 --> n55
    s43---s45["<strong><a href="https://www.cdc.gov/nwss/rv/InfluenzaA-statetrend.html" target="_blank" rel="noreferrer">Wastewater Influenza A State and Territory Trends</a></strong>"]
    s45 --> n55
    s43---s46["<strong><a href="https://www.cdc.gov/nwss/rv/RSV-statetrend.html" target="_blank" rel="noreferrer">Wastewater RSV State and Territory Trends</a></strong>"]
    s46 --> n55
    s47---s48["<strong><a href="https://wisqars.cdc.gov/reports/?o=MORT&i=8&m=20810&s=0&r=0&ry=2&y1=2018&y2=2023&a=ALL&g1=0&g2=199&a1=0&a2=199&r1=MECH&r2=AGEGP&r3=STATE&r4=YEAR&r5=NONE&r6=NONE&g=00&e=0&yp=65&me=0&t=0" target="_blank" rel="noreferrer">Fatal Injury Report</a></strong>"]
    s48 --> n56
    n54 --> bundle_childhood_immunizations
    n53 --> bundle_childhood_immunizations
    n51 --> bundle_childhood_immunizations
    n52 --> bundle_childhood_immunizations
    n47 --> bundle_childhood_immunizations
    n46 --> bundle_childhood_immunizations
    n45 --> bundle_childhood_immunizations
    n4 --> bundle_chronic_diseases
    n14 --> bundle_chronic_diseases
    n4 --> bundle_injury_overdose
    n14 --> bundle_injury_overdose
    n44 --> bundle_injury_overdose
    n42 --> bundle_injury_overdose
    n56 --> bundle_injury_overdose
    n34 --> bundle_measles
    n33 --> bundle_respiratory
    n55 --> bundle_respiratory
    n1 --> bundle_respiratory
    n2 --> bundle_respiratory
    n49 --> bundle_respiratory
    n50 --> bundle_respiratory
    n34 --> measles_cdc
    n33 --> respnet
    n55 --> respnet
    n1 --> respnet
    n2 --> respnet
    n49 --> respnet
    n50 --> respnet
    n34 --> vaccine_exemptions_fattah
    n34 --> wastewater_measles
```
