```mermaid
flowchart LR
    classDef pass stroke:#66bb6a
    classDef warn stroke:#ffa726
    classDef fail stroke:#f44336
    s0(("<strong><a href="https://www.cdc.gov/abcs/index.html" target="_blank" rel="noreferrer">Active Bacterial Core surveillance (ABCs)</a></strong>"))
    s2(("<strong><a href="https://pubmed.ncbi.nlm.nih.gov/39758745/" target="_blank" rel="noreferrer">Serotype-Specific Urinary Antigen Detection (SSUAD) Study</a></strong>"))
    s4(("<strong><a href="https://www.cdc.gov/brfss/index.html" target="_blank" rel="noreferrer">Behavioral Risk Factor Surveillance System (BRFSS)</a></strong>"))
    s6(("<strong><a href="https://www.census.gov/programs-surveys/acs/data.html" target="_blank" rel="noreferrer">American Community Survey 5-Year Estimates (via Metopio)</a></strong>"))
    s8(("<strong><a href="https://data.cdc.gov" target="_blank" rel="noreferrer">Center of Medicare and Medicaid Services (CMS)</a></strong>"))
    s10(("<strong><a href="https://data.cms.gov/tools/mapping-medicare-disparities-by-population" target="_blank" rel="noreferrer">Mapping Medicare Disparities by Population Tool</a></strong>"))
    s11(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/doctor-visits.html" target="_blank" rel="noreferrer">CMU Delphi COVIDcast - Doctor Visits</a></strong>"))
    s13(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/" target="_blank" rel="noreferrer">CMU Delphi</a></strong>"))
    s15(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/hospital-admissions.html" target="_blank" rel="noreferrer">CMU Delphi COVIDcast - Hospital Admissions</a></strong>"))
    s16(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/" target="_blank" rel="noreferrer">CMU Delphi Epidata</a></strong>"))
    s18(("<strong><a href="https://www.cdc.gov/flu/weekly/overview.htm" target="_blank" rel="noreferrer">CDC ILINet</a></strong>"))
    s19(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/fluview.html" target="_blank" rel="noreferrer">CMU Delphi Epidata - FluView (ILINet)</a></strong>"))
    s20(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/nhsn.html" target="_blank" rel="noreferrer">CMU Delphi COVIDcast - NHSN Respiratory Hospitalizations</a></strong>"))
    s21(("<strong><a href="https://trends.google.com" target="_blank" rel="noreferrer">Google Trends</a></strong>"))
    s23(("<strong><a href="https://www.cdc.gov/measles/data-research/index.html" target="_blank" rel="noreferrer">CDC Measles Cases and Outbreaks - Age Distribution</a></strong>"))
    s24(("<strong><a href="https://www.cdc.gov/measles/data-research/index.html" target="_blank" rel="noreferrer">CDC Measles Cases and Outbreaks</a></strong>"))
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
    s40(("<strong><a href="https://www.cdc.gov/resp-net/dashboard/index.html" target="_blank" rel="noreferrer">Respiratory Virus Hospitalization Surveillance Network (RESP-NET)</a></strong>"))
    s44(("<strong><a href="https://github.com/washingtonpost/data-school-vaccination-rates" target="_blank" rel="noreferrer">Washington Post School Vaccination Rates</a></strong>"))
    s45(("<strong><a href="https://www.cdc.gov/schoolvaxview/index.html" target="_blank" rel="noreferrer">SchoolVaxView</a></strong>"))
    s47(("<strong><a href="https://jamanetwork.com/journals/jama/fullarticle/2843870" target="_blank" rel="noreferrer">Medical Exemptions From Childhood Vaccination in the US (Kiang et al. 2025)</a></strong>"))
    s48(("<strong><a href="https://www.cdc.gov/nwss" target="_blank" rel="noreferrer">National Wastewater Surveillance System</a></strong>"))
    s52(("<strong><a href="https://data.cdc.gov/d/akvg-8vrb" target="_blank" rel="noreferrer">CDC National Wastewater Surveillance System (NWSS) - Measles</a></strong>"))
    s53(("<strong><a href="https://wisqars.cdc.gov/" target="_blank" rel="noreferrer">Web-based Injury Statistics Query and Reporting System (WISQARS)</a></strong>"))
    subgraph abcs["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/abcs" target="_blank" rel="noreferrer">abcs</a></strong>`"]
        direction LR
        n1["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/abcs/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
        n2["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/abcs/standard/uad.csv.gz" target="_blank" rel="noreferrer">uad.csv.gz</a></strong>`"]:::pass
    end
    subgraph atlas_amr["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/atlas_amr" target="_blank" rel="noreferrer">atlas_amr</a></strong>`"]
        direction LR
    end
    subgraph brfss["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/brfss" target="_blank" rel="noreferrer">brfss</a></strong>`"]
        direction LR
        n3["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/brfss/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
        n4["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/brfss/standard/data_survey.csv" target="_blank" rel="noreferrer">data_survey.csv</a></strong>`"]:::pass
        n5["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/brfss/standard/data_survey.csv.gz" target="_blank" rel="noreferrer">data_survey.csv.gz</a></strong>`"]:::pass
    end
    subgraph census["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/census" target="_blank" rel="noreferrer">census</a></strong>`"]
        direction LR
        n6["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/census/standard/data_county.csv.gz" target="_blank" rel="noreferrer">data_county.csv.gz</a></strong>`"]:::pass
        n7["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/census/standard/data_state.csv.gz" target="_blank" rel="noreferrer">data_state.csv.gz</a></strong>`"]:::pass
        n8["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/census/standard/data_zcta_2019_2020.csv.gz" target="_blank" rel="noreferrer">data_zcta_2019_2020.csv.gz</a></strong>`"]:::pass
        n9["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/census/standard/data_zcta_2021_2022.csv.gz" target="_blank" rel="noreferrer">data_zcta_2021_2022.csv.gz</a></strong>`"]:::pass
        n10["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/census/standard/data_zcta_2023_2024.csv.gz" target="_blank" rel="noreferrer">data_zcta_2023_2024.csv.gz</a></strong>`"]:::pass
    end
    subgraph cms_mmd["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/cms_mmd" target="_blank" rel="noreferrer">cms_mmd</a></strong>`"]
        direction LR
        n11["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a></strong>`"]:::pass
        n12["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age_by_race.csv.gz" target="_blank" rel="noreferrer">data_state_county_age_by_race.csv.gz</a></strong>`"]:::pass
        n13["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age_by_sex.csv.gz" target="_blank" rel="noreferrer">data_state_county_age_by_sex.csv.gz</a></strong>`"]:::pass
    end
    subgraph delphi_doctors_claims["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_doctors_claims" target="_blank" rel="noreferrer">delphi_doctors_claims</a></strong>`"]
        direction LR
        n14["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/delphi_doctors_claims/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph delphi_hospital_claims["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_hospital_claims" target="_blank" rel="noreferrer">delphi_hospital_claims</a></strong>`"]
        direction LR
        n15["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/delphi_hospital_claims/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph delphi_ili_fluview["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_ili_fluview" target="_blank" rel="noreferrer">delphi_ili_fluview</a></strong>`"]
        direction LR
        n16["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/delphi_ili_fluview/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph delphi_nhsn["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_nhsn" target="_blank" rel="noreferrer">delphi_nhsn</a></strong>`"]
        direction LR
        n17["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/delphi_nhsn/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph epic["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic" target="_blank" rel="noreferrer">epic</a></strong>`"]
        direction LR
        n18["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/county_no_time.csv.gz" target="_blank" rel="noreferrer">county_no_time.csv.gz</a></strong>`"]:::pass
        n19["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/county_year.csv.gz" target="_blank" rel="noreferrer">county_year.csv.gz</a></strong>`"]:::pass
        n20["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/heat_year_county.csv" target="_blank" rel="noreferrer">heat_year_county.csv</a></strong>`"]:::pass
        n21["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/monthly.csv.gz" target="_blank" rel="noreferrer">monthly.csv.gz</a></strong>`"]:::pass
        n22["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/monthly_injury.csv.gz" target="_blank" rel="noreferrer">monthly_injury.csv.gz</a></strong>`"]:::pass
        n23["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/monthly_tests.csv.gz" target="_blank" rel="noreferrer">monthly_tests.csv.gz</a></strong>`"]:::pass
        n24["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/no_geo.csv.gz" target="_blank" rel="noreferrer">no_geo.csv.gz</a></strong>`"]:::pass
        n25["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/state_no_time.csv.gz" target="_blank" rel="noreferrer">state_no_time.csv.gz</a></strong>`"]:::pass
        n26["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/state_year.csv.gz" target="_blank" rel="noreferrer">state_year.csv.gz</a></strong>`"]:::pass
        n27["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/weekly.csv.gz" target="_blank" rel="noreferrer">weekly.csv.gz</a></strong>`"]:::pass
        n28["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/yearly_injury.csv.gz" target="_blank" rel="noreferrer">yearly_injury.csv.gz</a></strong>`"]:::pass
    end
    subgraph gtrends["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/gtrends" target="_blank" rel="noreferrer">gtrends</a></strong>`"]
        direction LR
        n29["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/gtrends/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
        n30["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/gtrends/standard/data_dma.csv.gz" target="_blank" rel="noreferrer">data_dma.csv.gz</a></strong>`"]:::pass
        n31["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/gtrends/standard/data_dma_year.csv.gz" target="_blank" rel="noreferrer">data_dma_year.csv.gz</a></strong>`"]:::pass
        n32["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/gtrends/standard/data_year.csv.gz" target="_blank" rel="noreferrer">data_year.csv.gz</a></strong>`"]:::pass
    end
    subgraph measles_age_cdc["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/measles_age_cdc" target="_blank" rel="noreferrer">measles_age_cdc</a></strong>`"]
        direction LR
        n33["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/measles_age_cdc/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph measles_cdc["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/measles_cdc" target="_blank" rel="noreferrer">measles_cdc</a></strong>`"]
        direction LR
        n34["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/measles_cdc/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph measles_jhu["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/measles_jhu" target="_blank" rel="noreferrer">measles_jhu</a></strong>`"]
        direction LR
        n35["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/measles_jhu/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
        n36["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/measles_jhu/standard/data_county.csv.gz" target="_blank" rel="noreferrer">data_county.csv.gz</a></strong>`"]:::pass
        n37["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/measles_jhu/standard/data_state.csv.gz" target="_blank" rel="noreferrer">data_state.csv.gz</a></strong>`"]:::pass
    end
    subgraph medicaid_quality["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/medicaid_quality" target="_blank" rel="noreferrer">medicaid_quality</a></strong>`"]
        direction LR
        n38["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/medicaid_quality/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph mmr_epic["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/mmr_epic" target="_blank" rel="noreferrer">mmr_epic</a></strong>`"]
        direction LR
    end
    subgraph mmr_healthmap["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/mmr_healthmap" target="_blank" rel="noreferrer">mmr_healthmap</a></strong>`"]
        direction LR
        n39["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/mmr_healthmap/standard/data_county.csv.gz" target="_blank" rel="noreferrer">data_county.csv.gz</a></strong>`"]:::pass
        n40["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/mmr_healthmap/standard/data_state.csv.gz" target="_blank" rel="noreferrer">data_state.csv.gz</a></strong>`"]:::pass
        n41["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/mmr_healthmap/standard/data_zcta.csv.gz" target="_blank" rel="noreferrer">data_zcta.csv.gz</a></strong>`"]:::pass
    end
    subgraph narms["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/narms" target="_blank" rel="noreferrer">narms</a></strong>`"]
        direction LR
    end
    subgraph nchs_mortality["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nchs_mortality" target="_blank" rel="noreferrer">nchs_mortality</a></strong>`"]
        direction LR
        n42["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/nchs_mortality/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong><br /><br />Script Failed:<br />In argument: 'geography = if_else(State == 'US', 0, fips(State, to = 'FIPS'))'.`"]:::fail
        n43["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/nchs_mortality/standard/data_county.csv.gz" target="_blank" rel="noreferrer">data_county.csv.gz</a></strong><br /><br />Script Failed:<br />In argument: 'geography = if_else(State == 'US', 0, fips(State, to = 'FIPS'))'.`"]:::fail
        n44["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/nchs_mortality/standard/data_state_21_causes.csv.gz" target="_blank" rel="noreferrer">data_state_21_causes.csv.gz</a></strong><br /><br />Script Failed:<br />In argument: 'geography = if_else(State == 'US', 0, fips(State, to = 'FIPS'))'.`"]:::fail
    end
    subgraph nis["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nis" target="_blank" rel="noreferrer">nis</a></strong>`"]
        direction LR
        n45["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/nis/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
        n46["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/nis/standard/data_insurance.csv.gz" target="_blank" rel="noreferrer">data_insurance.csv.gz</a></strong>`"]:::pass
        n47["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/nis/standard/data_urban.csv.gz" target="_blank" rel="noreferrer">data_urban.csv.gz</a></strong>`"]:::pass
    end
    subgraph nnds["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nnds" target="_blank" rel="noreferrer">nnds</a></strong>`"]
        direction LR
        n48["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/nnds/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph NREVSS["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/NREVSS" target="_blank" rel="noreferrer">NREVSS</a></strong>`"]
        direction LR
        n49["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/NREVSS/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph nssp["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nssp" target="_blank" rel="noreferrer">nssp</a></strong>`"]
        direction LR
        n50["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/nssp/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph respnet["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/respnet" target="_blank" rel="noreferrer">respnet</a></strong>`"]
        direction LR
        n51["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/respnet/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph schoolvax_washpost["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/schoolvax_washpost" target="_blank" rel="noreferrer">schoolvax_washpost</a></strong>`"]
        direction LR
        n52["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/schoolvax_washpost/standard/data_counties.csv.gz" target="_blank" rel="noreferrer">data_counties.csv.gz</a></strong>`"]:::pass
        n53["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/schoolvax_washpost/standard/data_schools.csv.gz" target="_blank" rel="noreferrer">data_schools.csv.gz</a></strong>`"]:::pass
    end
    subgraph schoolvaxview["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/schoolvaxview" target="_blank" rel="noreferrer">schoolvaxview</a></strong>`"]
        direction LR
        n54["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/schoolvaxview/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
        n55["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/schoolvaxview/standard/data_exemptions.csv.gz" target="_blank" rel="noreferrer">data_exemptions.csv.gz</a></strong>`"]:::pass
    end
    subgraph vaccine_exemptions_kiang["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/vaccine_exemptions_kiang" target="_blank" rel="noreferrer">vaccine_exemptions_kiang</a></strong>`"]
        direction LR
        n56["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/vaccine_exemptions_kiang/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
        n57["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/vaccine_exemptions_kiang/standard/data_county.csv.gz" target="_blank" rel="noreferrer">data_county.csv.gz</a></strong>`"]:::pass
        n58["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/vaccine_exemptions_kiang/standard/data_state.csv.gz" target="_blank" rel="noreferrer">data_state.csv.gz</a></strong>`"]:::pass
    end
    subgraph vaers["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/vaers" target="_blank" rel="noreferrer">vaers</a></strong>`"]
        direction LR
    end
    subgraph wastewater["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wastewater" target="_blank" rel="noreferrer">wastewater</a></strong>`"]
        direction LR
        n59["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/wastewater/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph wastewater_measles["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wastewater_measles" target="_blank" rel="noreferrer">wastewater_measles</a></strong>`"]
        direction LR
        n60["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/wastewater_measles/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
        n61["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/wastewater_measles/standard/data_county.csv.gz" target="_blank" rel="noreferrer">data_county.csv.gz</a></strong>`"]:::pass
    end
    subgraph wisqars["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wisqars" target="_blank" rel="noreferrer">wisqars</a></strong>`"]
        direction LR
        n62["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/wisqars/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph bundle_childhood_immunizations["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_childhood_immunizations" target="_blank" rel="noreferrer">bundle_childhood_immunizations</a></strong>`"]
        direction LR
        n63["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/mmr_rates_epic.parquet" target="_blank" rel="noreferrer">mmr_rates_epic.parquet</a></strong>`"]
        n64["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/nis_insurance.parquet" target="_blank" rel="noreferrer">nis_insurance.parquet</a></strong>`"]
        n65["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/nis_overall.parquet" target="_blank" rel="noreferrer">nis_overall.parquet</a></strong>`"]
        n66["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/nis_urban.parquet" target="_blank" rel="noreferrer">nis_urban.parquet</a></strong>`"]
        n67["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/overall_rates_by_source.parquet" target="_blank" rel="noreferrer">overall_rates_by_source.parquet</a></strong>`"]
        n68["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/schoolvaxview_exemptions.parquet" target="_blank" rel="noreferrer">schoolvaxview_exemptions.parquet</a></strong>`"]
        n69["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/schoolvaxview_overall.parquet" target="_blank" rel="noreferrer">schoolvaxview_overall.parquet</a></strong>`"]
        n70["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/state_compare.parquet" target="_blank" rel="noreferrer">state_compare.parquet</a></strong>`"]
        n71["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/wapo_vax_counties.parquet" target="_blank" rel="noreferrer">wapo_vax_counties.parquet</a></strong>`"]
        n72["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/wapo_vax_schools.parquet" target="_blank" rel="noreferrer">wapo_vax_schools.parquet</a></strong>`"]
    end
    subgraph bundle_chronic_diseases["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_chronic_diseases" target="_blank" rel="noreferrer">bundle_chronic_diseases</a></strong>`"]
        direction LR
        n73["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/brfss_prevalence_by_geography.parquet" target="_blank" rel="noreferrer">brfss_prevalence_by_geography.parquet</a></strong>`"]
        n74["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/county_opioid_by_source.parquet" target="_blank" rel="noreferrer">county_opioid_by_source.parquet</a></strong>`"]
        n75["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/deaths_cause_age.parquet" target="_blank" rel="noreferrer">deaths_cause_age.parquet</a></strong>`"]
        n76["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/epic_prevalence_by_geography.parquet" target="_blank" rel="noreferrer">epic_prevalence_by_geography.parquet</a></strong>`"]
        n77["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/epic_prevalence_by_geography_county.parquet" target="_blank" rel="noreferrer">epic_prevalence_by_geography_county.parquet</a></strong>`"]
        n78["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/epic_prevalence_by_geography_county_and_source.parquet" target="_blank" rel="noreferrer">epic_prevalence_by_geography_county_and_source.parquet</a></strong>`"]
        n79["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/epic_prevalence_by_geography_year.parquet" target="_blank" rel="noreferrer">epic_prevalence_by_geography_year.parquet</a></strong>`"]
        n80["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/overdose_by_geography_and_source.parquet" target="_blank" rel="noreferrer">overdose_by_geography_and_source.parquet</a></strong>`"]
        n81["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/overdose_by_geography_and_source_county.parquet" target="_blank" rel="noreferrer">overdose_by_geography_and_source_county.parquet</a></strong>`"]
        n82["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/overdose_deaths_county.parquet" target="_blank" rel="noreferrer">overdose_deaths_county.parquet</a></strong>`"]
        n83["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/overdose_deaths_state.parquet" target="_blank" rel="noreferrer">overdose_deaths_state.parquet</a></strong>`"]
        n84["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/prevalence_by_geography_and_source.csv" target="_blank" rel="noreferrer">prevalence_by_geography_and_source.csv</a></strong>`"]
        n85["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/prevalence_by_geography_and_source.parquet" target="_blank" rel="noreferrer">prevalence_by_geography_and_source.parquet</a></strong>`"]
        n86["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/prevalence_by_geography_and_year_and_source.parquet" target="_blank" rel="noreferrer">prevalence_by_geography_and_year_and_source.parquet</a></strong>`"]
        n87["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/prevalence_by_geography_year_and_source.parquet" target="_blank" rel="noreferrer">prevalence_by_geography_year_and_source.parquet</a></strong>`"]
    end
    subgraph bundle_injury_overdose["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_injury_overdose" target="_blank" rel="noreferrer">bundle_injury_overdose</a></strong>`"]
        direction LR
        n88["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/brfss_prevalence_by_geography.parquet" target="_blank" rel="noreferrer">brfss_prevalence_by_geography.parquet</a></strong>`"]
        n89["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/county_opioid_by_source.parquet" target="_blank" rel="noreferrer">county_opioid_by_source.parquet</a></strong>`"]
        n90["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/deaths_cause_age.parquet" target="_blank" rel="noreferrer">deaths_cause_age.parquet</a></strong>`"]
        n91["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/deaths_cause_age_demographics.parquet" target="_blank" rel="noreferrer">deaths_cause_age_demographics.parquet</a></strong>`"]
        n92["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/epic_prevalence_by_geography_year.parquet" target="_blank" rel="noreferrer">epic_prevalence_by_geography_year.parquet</a></strong>`"]
        n93["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/firearms_by_demographics.parquet" target="_blank" rel="noreferrer">firearms_by_demographics.parquet</a></strong>`"]
        n94["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/firearms_by_geography_and_source_state_year.parquet" target="_blank" rel="noreferrer">firearms_by_geography_and_source_state_year.parquet</a></strong>`"]
        n95["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/firearms_geography_source.parquet" target="_blank" rel="noreferrer">firearms_geography_source.parquet</a></strong>`"]
        n96["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/google_dma.parquet" target="_blank" rel="noreferrer">google_dma.parquet</a></strong>`"]
        n97["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/heat_by_geography_and_source_state_year.parquet" target="_blank" rel="noreferrer">heat_by_geography_and_source_state_year.parquet</a></strong>`"]
        n98["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/heat_related_geography_source.parquet" target="_blank" rel="noreferrer">heat_related_geography_source.parquet</a></strong>`"]
        n99["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/overdose_by_demographics.parquet" target="_blank" rel="noreferrer">overdose_by_demographics.parquet</a></strong>`"]
        n100["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/overdose_by_geography_and_source.parquet" target="_blank" rel="noreferrer">overdose_by_geography_and_source.parquet</a></strong>`"]
        n101["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/overdose_by_geography_and_source_county.parquet" target="_blank" rel="noreferrer">overdose_by_geography_and_source_county.parquet</a></strong>`"]
        n102["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/overdose_by_geography_and_source_state_year.parquet" target="_blank" rel="noreferrer">overdose_by_geography_and_source_state_year.parquet</a></strong>`"]
        n103["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/overdose_deaths_county.parquet" target="_blank" rel="noreferrer">overdose_deaths_county.parquet</a></strong>`"]
        n104["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/overdose_deaths_state.parquet" target="_blank" rel="noreferrer">overdose_deaths_state.parquet</a></strong>`"]
        n105["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/state_opioid_by_source.parquet" target="_blank" rel="noreferrer">state_opioid_by_source.parquet</a></strong>`"]
    end
    subgraph bundle_measles["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_measles" target="_blank" rel="noreferrer">bundle_measles</a></strong>`"]
        direction LR
        n106["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_measles/dist/measles_cases_by_age.parquet" target="_blank" rel="noreferrer">measles_cases_by_age.parquet</a></strong>`"]
        n107["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_measles/dist/measles_county.parquet" target="_blank" rel="noreferrer">measles_county.parquet</a></strong>`"]
        n108["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_measles/dist/measles_state.parquet" target="_blank" rel="noreferrer">measles_state.parquet</a></strong>`"]
    end
    subgraph bundle_respiratory["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_respiratory" target="_blank" rel="noreferrer">bundle_respiratory</a></strong>`"]
        direction LR
        n109["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/covid_ed_visits_by_county.parquet" target="_blank" rel="noreferrer">covid_ed_visits_by_county.parquet</a></strong>`"]
        n110["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/covid_overall_trends.parquet" target="_blank" rel="noreferrer">covid_overall_trends.parquet</a></strong>`"]
        n111["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/covid_trends_by_age.parquet" target="_blank" rel="noreferrer">covid_trends_by_age.parquet</a></strong>`"]
        n112["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/flu_ed_visits_by_county.parquet" target="_blank" rel="noreferrer">flu_ed_visits_by_county.parquet</a></strong>`"]
        n113["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/flu_overall_trends.parquet" target="_blank" rel="noreferrer">flu_overall_trends.parquet</a></strong>`"]
        n114["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/flu_trends_by_age.parquet" target="_blank" rel="noreferrer">flu_trends_by_age.parquet</a></strong>`"]
        n115["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/pneumococcus_by_geography.parquet" target="_blank" rel="noreferrer">pneumococcus_by_geography.parquet</a></strong>`"]
        n116["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/pneumococcus_by_geography_year.parquet" target="_blank" rel="noreferrer">pneumococcus_by_geography_year.parquet</a></strong>`"]
        n117["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/pneumococcus_comparison.parquet" target="_blank" rel="noreferrer">pneumococcus_comparison.parquet</a></strong>`"]
        n118["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/pneumococcus_serotype_trends.parquet" target="_blank" rel="noreferrer">pneumococcus_serotype_trends.parquet</a></strong>`"]
        n119["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/rsv_ed_visits_by_county.parquet" target="_blank" rel="noreferrer">rsv_ed_visits_by_county.parquet</a></strong>`"]
        n120["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/rsv_google_dma.parquet" target="_blank" rel="noreferrer">rsv_google_dma.parquet</a></strong>`"]
        n121["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/rsv_overall_trends.parquet" target="_blank" rel="noreferrer">rsv_overall_trends.parquet</a></strong>`"]
        n122["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/rsv_positive_tests.parquet" target="_blank" rel="noreferrer">rsv_positive_tests.parquet</a></strong>`"]
        n123["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/rsv_testing_pct.parquet" target="_blank" rel="noreferrer">rsv_testing_pct.parquet</a></strong>`"]
        n124["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/rsv_trends_by_age.parquet" target="_blank" rel="noreferrer">rsv_trends_by_age.parquet</a></strong>`"]
    end
    s0---s1["<strong><a href="https://data.cdc.gov/resource/qvzb-qs6p/" target="_blank" rel="noreferrer">Serotype Data for Invasive Pneumococcal Disease Cases by Age Group from Active Bacterial Core surveillance</a></strong>"]
    s1 --> n1
    s1 --> n2
    s2---s3["<strong><a href="https://pubmed.ncbi.nlm.nih.gov/39758745/" target="_blank" rel="noreferrer">Open Forum for Infectious Diseases</a></strong>"]
    s3 --> n2
    s4---s5["<strong><a href="https://data.cdc.gov/Behavioral-Risk-Factors/Behavioral-Risk-Factor-Surveillance-System-BRFSS-P/dttw-5yxu/about_data" target="_blank" rel="noreferrer">Behavioral Risk Factor Surveillance System (BRFSS) Prevalence Data (2011 to present)</a></strong>"]
    s5 --> n3
    s5 --> n4
    s6---s7["<strong><a href="https://api.census.gov/data.html" target="_blank" rel="noreferrer">Census API â€” ACS 5-Year Detailed Tables and Subject Tables</a></strong>"]
    s7 --> n6
    s7 --> n7
    s8---s9["<strong><a href="https://data.cms.gov/tools/mapping-medicare-disparities-by-population" target="_blank" rel="noreferrer">Mapping Medicare Disparities by Population Tool</a></strong>"]
    s9 --> n11
    s10 --> n11
    s9 --> n12
    s10 --> n12
    s9 --> n13
    s10 --> n13
    s11---s12["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/" target="_blank" rel="noreferrer">COVIDcast Epidata API</a></strong>"]
    s12 --> n14
    s13---s14["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/hospital-admissions.html" target="_blank" rel="noreferrer">COVIDcast > Hospital Admissions</a></strong>"]
    s14 --> n15
    s12 --> n15
    s16---s17["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/fluview.html" target="_blank" rel="noreferrer">FluView API</a></strong>"]
    s17 --> n16
    s18 --> n16
    s12 --> n16
    s12 --> n17
    s21---s22["<strong><a href="https://github.com/DISSC-yale/gtrends_collection" target="_blank" rel="noreferrer">Yale Data-Intensive Social Sciences, Google Trends Collection Framework</a></strong>"]
    s22 --> n29
    s22 --> n30
    s22 --> n31
    s22 --> n32
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
    s29 --> n43
    s30 --> n44
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
    s40---s41["<strong><a href="https://data.cdc.gov/Public-Health-Surveillance/Rates-of-Laboratory-Confirmed-RSV-COVID-19-and-Flu/kvib-3txy/about_data" target="_blank" rel="noreferrer">Rates of Laboratory-Confirmed RSV, COVID-19, and Flu Hospitalizations from the RESP-NET Surveillance Systems</a></strong>"]
    s41 --> n51
    s40---s42["<strong><a href="https://healthdata.gov/CDC/Weekly-Rates-of-Laboratory-Confirmed-COVID-19-Hosp/gk5r-vjtt/about_data" target="_blank" rel="noreferrer">Weekly Rates of Laboratory-Confirmed COVID-19 Hospitalizations from the COVID-NET Surveillance System</a></strong>"]
    s42 --> n51
    s40---s43["<strong><a href="https://data.cdc.gov/Public-Health-Surveillance/Weekly-Rates-of-Laboratory-Confirmed-RSV-Hospitali/29hc-w46k/about_data" target="_blank" rel="noreferrer">Weekly Rates of Laboratory-Confirmed RSV Hospitalizations from the RSV-NET Surveillance System</a></strong>"]
    s43 --> n51
    s44 --> n52
    s44 --> n53
    s45---s46["<strong><a href="https://data.cdc.gov/Vaccinations/Vaccination-Coverage-and-Exemptions-among-Kinderga/ijqb-a7ye/about_data" target="_blank" rel="noreferrer">Vaccination Coverage and Exemptions among Kindergartners</a></strong>"]
    s46 --> n54
    s46 --> n55
    s47 --> n56
    s47 --> n57
    s47 --> n58
    s48---s49["<strong><a href="https://www.cdc.gov/nwss/rv/COVID19-statetrend.html" target="_blank" rel="noreferrer">Wastewater COVID-19 State and Territory Trends</a></strong>"]
    s49 --> n59
    s48---s50["<strong><a href="https://www.cdc.gov/nwss/rv/InfluenzaA-statetrend.html" target="_blank" rel="noreferrer">Wastewater Influenza A State and Territory Trends</a></strong>"]
    s50 --> n59
    s48---s51["<strong><a href="https://www.cdc.gov/nwss/rv/RSV-statetrend.html" target="_blank" rel="noreferrer">Wastewater RSV State and Territory Trends</a></strong>"]
    s51 --> n59
    s52 --> n60
    s52 --> n61
    s53---s54["<strong><a href="https://wisqars.cdc.gov/reports/?o=MORT&i=8&m=20810&s=0&r=0&ry=2&y1=2018&y2=2023&a=ALL&g1=0&g2=199&a1=0&a2=199&r1=MECH&r2=AGEGP&r3=STATE&r4=YEAR&r5=NONE&r6=NONE&g=00&e=0&yp=65&me=0&t=0" target="_blank" rel="noreferrer">Fatal Injury Report</a></strong>"]
    s54 --> n62
    n54 --> bundle_childhood_immunizations
    n55 --> bundle_childhood_immunizations
    n52 --> bundle_childhood_immunizations
    n53 --> bundle_childhood_immunizations
    n45 --> bundle_childhood_immunizations
    n47 --> bundle_childhood_immunizations
    n46 --> bundle_childhood_immunizations
    n3 --> bundle_chronic_diseases
    n25 --> bundle_chronic_diseases
    n18 --> bundle_chronic_diseases
    n11 --> bundle_chronic_diseases
    n25 --> bundle_injury_overdose
    n3 --> bundle_injury_overdose
    n11 --> bundle_injury_overdose
    n42 --> bundle_injury_overdose
    n43 --> bundle_injury_overdose
    n62 --> bundle_injury_overdose
    n60 --> bundle_measles
    n56 --> bundle_measles
    n33 --> bundle_measles
    n27 --> bundle_respiratory
    n29 --> bundle_respiratory
    n59 --> bundle_respiratory
    n1 --> bundle_respiratory
    n2 --> bundle_respiratory
    n49 --> bundle_respiratory
    n50 --> bundle_respiratory
    n51 --> bundle_respiratory
```
