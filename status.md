```mermaid
flowchart LR
    classDef pass stroke:#66bb6a
    classDef warn stroke:#ffa726
    classDef fail stroke:#f44336
    s0(("<strong><a href="https://data.cdc.gov" target="_blank" rel="noreferrer">Centers for Disease Control and Prevention</a></strong>"))
    s2(("<strong><a href="https://pubmed.ncbi.nlm.nih.gov/39758745/" target="_blank" rel="noreferrer">Ramirez et al. CAAP</a></strong>"))
    s4(("<strong><a href="https://www.cdc.gov/brfss/index.html" target="_blank" rel="noreferrer">BRFSS (CDC)</a></strong>"))
    s6(("<strong><a href="https://data.cdc.gov" target="_blank" rel="noreferrer">Center of Medicare and Medicaid Services (CMS)</a></strong>"))
    s8(("<strong><a href="https://cmu-delphi.github.io/delphi-epidata/" target="_blank" rel="noreferrer">CMU Delphi</a></strong>"))
    s12(("<strong><a href="https://cosmos.epic.com/" target="_blank" rel="noreferrer">Epic Cosmos</a></strong>"))
    s13(("<strong><a href="https://trends.google.com" target="_blank" rel="noreferrer">Google Trends</a></strong>"))
    s15(("<strong><a href="https://www.cdc.gov/nchs/nvss/vsrr/drug-overdose-data.htm" target="_blank" rel="noreferrer">Centers for Disease Control and Prevention (NCHS)</a></strong>"))
    s17(("<strong><a href="https://www.cdc.gov/nis/about/index.html" target="_blank" rel="noreferrer">National Immunization Survey</a></strong>"))
    s20(("<strong><a href="https://www.cdc.gov/nwss" target="_blank" rel="noreferrer">National Wastewater Surveillance System</a></strong>"))
    subgraph abcs["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/abcs" target="_blank" rel="noreferrer">abcs</a></strong>`"]
        direction LR
        n1["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/abcs/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
        n2["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/abcs/standard/uad.csv.gz" target="_blank" rel="noreferrer">uad.csv.gz</a></strong>`"]:::pass
    end
    subgraph brfss["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/brfss" target="_blank" rel="noreferrer">brfss</a></strong>`"]
        direction LR
        n3["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/brfss/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong><br/><br/><ul><li><code>missing_info: pct_binge_drinking_sample_size</code></li><li><code>missing_info: pct_binge_drinking_value</code></li><li><code>missing_info: pct_binge_drinking_value_lcl</code></li><li><code>missing_info: pct_binge_drinking_value_ucl</code></li></ul>`"]:::warn
    end
    subgraph cms_mmd["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/cms_mmd" target="_blank" rel="noreferrer">cms_mmd</a></strong>`"]
        direction LR
        n4["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a></strong>`"]:::pass
    end
    subgraph delphi_doctors_claims["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_doctors_claims" target="_blank" rel="noreferrer">delphi_doctors_claims</a></strong>`"]
        direction LR
        n5["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/delphi_doctors_claims/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph delphi_hospital_claims["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_hospital_claims" target="_blank" rel="noreferrer">delphi_hospital_claims</a></strong>`"]
        direction LR
        n6["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/delphi_hospital_claims/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph delphi_nhsn["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_nhsn" target="_blank" rel="noreferrer">delphi_nhsn</a></strong>`"]
        direction LR
        n7["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/delphi_nhsn/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph epic["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic" target="_blank" rel="noreferrer">epic</a></strong>`"]
        direction LR
        n8["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/children.csv.gz" target="_blank" rel="noreferrer">children.csv.gz</a></strong><br/><br/><ul><li><code>time_missing</code></li></ul>`"]:::warn
        n9["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/county_no_time.csv.gz" target="_blank" rel="noreferrer">county_no_time.csv.gz</a></strong><br/><br/><ul><li><code>time_missing</code></li></ul>`"]:::warn
        n10["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/monthly.csv.gz" target="_blank" rel="noreferrer">monthly.csv.gz</a></strong><br/><br/><ul><li><code>time_nas</code></li><li><code>missing_info: epic_pct_self_harm</code></li></ul>`"]:::warn
        n11["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/no_geo.csv.gz" target="_blank" rel="noreferrer">no_geo.csv.gz</a></strong><br/><br/><ul><li><code>missing_info: positive_rsv_tests_(%)</code></li></ul>`"]:::warn
        n12["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/state_no_time.csv.gz" target="_blank" rel="noreferrer">state_no_time.csv.gz</a></strong><br/><br/><ul><li><code>time_missing</code></li><li><code>missing_info: n_patients</code></li></ul>`"]:::warn
        n13["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/weekly.csv.gz" target="_blank" rel="noreferrer">weekly.csv.gz</a></strong><br/><br/><ul><li><code>missing_info: epic_positive_rsv_tests_(%)</code></li><li><code>missing_info: epic_rsv_tests</code></li><li><code>missing_info: epic_n_rsv_tests</code></li><li><code>missing_info: epic_pct_rsv</code></li><li><code>missing_info: epic_pct_flu</code></li><li><code>missing_info: epic_pct_covid</code></li></ul>`"]:::warn
    end
    subgraph gtrends["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/gtrends" target="_blank" rel="noreferrer">gtrends</a></strong>`"]
        direction LR
        n14["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/gtrends/standard/data_dma.csv.gz" target="_blank" rel="noreferrer">data_dma.csv.gz</a></strong><br/><br/><ul><li><code>geography_missing</code></li><li><code>missing_info: fips</code></li><li><code>missing_info: value</code></li><li><code>missing_info: term</code></li></ul>`"]:::warn
        n15["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/gtrends/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong><br/><br/><ul><li><code>missing_info: gtrends_drug+overdose</code></li><li><code>missing_info: gtrends_narcan</code></li><li><code>missing_info: gtrends_rsv_adjusted</code></li></ul>`"]:::warn
    end
    subgraph medicaid_quality["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/medicaid_quality" target="_blank" rel="noreferrer">medicaid_quality</a></strong>`"]
        direction LR
    end
    subgraph nchs_mortality["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nchs_mortality" target="_blank" rel="noreferrer">nchs_mortality</a></strong>`"]
        direction LR
        n16["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/nchs_mortality/standard/data_county.csv.gz" target="_blank" rel="noreferrer">data_county.csv.gz</a></strong>`"]:::pass
        n17["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/nchs_mortality/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph nis["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nis" target="_blank" rel="noreferrer">nis</a></strong>`"]
        direction LR
        n18["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/nis/standard/data_insurance.csv.gz" target="_blank" rel="noreferrer">data_insurance.csv.gz</a></strong><br/><br/><ul><li><code>time_missing</code></li><li><code>missing_info: insurance</code></li><li><code>missing_info: birth_year</code></li><li><code>missing_info: vaccine</code></li></ul>`"]:::warn
        n19["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/nis/standard/data_urban.csv.gz" target="_blank" rel="noreferrer">data_urban.csv.gz</a></strong><br/><br/><ul><li><code>time_missing</code></li><li><code>missing_info: urban</code></li><li><code>missing_info: birth_year</code></li><li><code>missing_info: vaccine</code></li></ul>`"]:::warn
        n20["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/nis/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong><br/><br/><ul><li><code>missing_info: birth_year</code></li><li><code>missing_info: age</code></li><li><code>missing_info: vaccine</code></li></ul>`"]:::warn
    end
    subgraph NREVSS["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/NREVSS" target="_blank" rel="noreferrer">NREVSS</a></strong>`"]
        direction LR
        n21["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/NREVSS/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph nssp["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/nssp" target="_blank" rel="noreferrer">nssp</a></strong>`"]
        direction LR
        n22["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/nssp/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong><br /><br />Script Failed:<br />`"]:::fail
    end
    subgraph respnet["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/respnet" target="_blank" rel="noreferrer">respnet</a></strong>`"]
        direction LR
        n23["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/respnet/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph schoolvaxview["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/schoolvaxview" target="_blank" rel="noreferrer">schoolvaxview</a></strong>`"]
        direction LR
        n24["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/schoolvaxview/standard/data_exemptions.csv.gz" target="_blank" rel="noreferrer">data_exemptions.csv.gz</a></strong><br/><br/><ul><li><code>missing_info: grade</code></li><li><code>missing_info: N</code></li><li><code>missing_info: vax</code></li><li><code>missing_info: value</code></li><li><code>missing_info: percent_surveyed</code></li><li><code>missing_info: survey_type</code></li></ul>`"]:::warn
        n25["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/schoolvaxview/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong><br/><br/><ul><li><code>missing_info: grade</code></li><li><code>missing_info: N</code></li><li><code>missing_info: vax</code></li><li><code>missing_info: value</code></li><li><code>missing_info: percent_surveyed</code></li><li><code>missing_info: survey_type</code></li></ul>`"]:::warn
    end
    subgraph wastewater["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/wastewater" target="_blank" rel="noreferrer">wastewater</a></strong>`"]
        direction LR
        n26["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/wastewater/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a></strong>`"]:::pass
    end
    subgraph bundle_childhood_immunizations["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_childhood_immunizations" target="_blank" rel="noreferrer">bundle_childhood_immunizations</a></strong>`"]
        direction LR
        n27["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/mmr_rates_epic.parquet" target="_blank" rel="noreferrer">mmr_rates_epic.parquet</a></strong>`"]
        n28["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/nis_insurance.parquet" target="_blank" rel="noreferrer">nis_insurance.parquet</a></strong>`"]
        n29["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/nis_overall.parquet" target="_blank" rel="noreferrer">nis_overall.parquet</a></strong>`"]
        n30["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/nis_urban.parquet" target="_blank" rel="noreferrer">nis_urban.parquet</a></strong>`"]
        n31["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/overall_rates_by_source.parquet" target="_blank" rel="noreferrer">overall_rates_by_source.parquet</a></strong>`"]
        n32["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/schoolvaxview_exemptions.parquet" target="_blank" rel="noreferrer">schoolvaxview_exemptions.parquet</a></strong>`"]
        n33["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/schoolvaxview_overall.parquet" target="_blank" rel="noreferrer">schoolvaxview_overall.parquet</a></strong>`"]
        n34["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/state_compare.parquet" target="_blank" rel="noreferrer">state_compare.parquet</a></strong>`"]
    end
    subgraph bundle_chronic_diseases["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_chronic_diseases" target="_blank" rel="noreferrer">bundle_chronic_diseases</a></strong>`"]
        direction LR
        n35["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/brfss_prevalence_by_geography.parquet" target="_blank" rel="noreferrer">brfss_prevalence_by_geography.parquet</a></strong>`"]
        n36["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/epic_prevalence_by_geography_county_and_source.parquet" target="_blank" rel="noreferrer">epic_prevalence_by_geography_county_and_source.parquet</a></strong>`"]
        n37["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/epic_prevalence_by_geography_county.parquet" target="_blank" rel="noreferrer">epic_prevalence_by_geography_county.parquet</a></strong>`"]
        n38["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/epic_prevalence_by_geography.parquet" target="_blank" rel="noreferrer">epic_prevalence_by_geography.parquet</a></strong>`"]
        n39["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/overdose_deaths_county.parquet" target="_blank" rel="noreferrer">overdose_deaths_county.parquet</a></strong>`"]
        n40["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/overdose_deaths_state.parquet" target="_blank" rel="noreferrer">overdose_deaths_state.parquet</a></strong>`"]
        n41["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/prevalence_by_geography_and_source.csv" target="_blank" rel="noreferrer">prevalence_by_geography_and_source.csv</a></strong>`"]
        n42["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/prevalence_by_geography_and_source.parquet" target="_blank" rel="noreferrer">prevalence_by_geography_and_source.parquet</a></strong>`"]
        n43["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/prevalence_by_geography_year_and_source.parquet" target="_blank" rel="noreferrer">prevalence_by_geography_year_and_source.parquet</a></strong>`"]
    end
    subgraph bundle_injury_overdose["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_injury_overdose" target="_blank" rel="noreferrer">bundle_injury_overdose</a></strong>`"]
        direction LR
        n44["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/overdose_deaths_county.parquet" target="_blank" rel="noreferrer">overdose_deaths_county.parquet</a></strong>`"]
        n45["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_injury_overdose/dist/overdose_deaths_state.parquet" target="_blank" rel="noreferrer">overdose_deaths_state.parquet</a></strong>`"]
    end
    subgraph bundle_respiratory["`<strong><a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_respiratory" target="_blank" rel="noreferrer">bundle_respiratory</a></strong>`"]
        direction LR
        n46["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/covid_ed_visits_by_county.parquet" target="_blank" rel="noreferrer">covid_ed_visits_by_county.parquet</a></strong>`"]
        n47["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/covid_overall_trends.parquet" target="_blank" rel="noreferrer">covid_overall_trends.parquet</a></strong>`"]
        n48["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/covid_trends_by_age.parquet" target="_blank" rel="noreferrer">covid_trends_by_age.parquet</a></strong>`"]
        n49["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/flu_ed_visits_by_county.parquet" target="_blank" rel="noreferrer">flu_ed_visits_by_county.parquet</a></strong>`"]
        n50["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/flu_overall_trends.parquet" target="_blank" rel="noreferrer">flu_overall_trends.parquet</a></strong>`"]
        n51["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/flu_trends_by_age.parquet" target="_blank" rel="noreferrer">flu_trends_by_age.parquet</a></strong>`"]
        n52["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/pneumococcus_by_geography.parquet" target="_blank" rel="noreferrer">pneumococcus_by_geography.parquet</a></strong>`"]
        n53["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/pneumococcus_comparison.parquet" target="_blank" rel="noreferrer">pneumococcus_comparison.parquet</a></strong>`"]
        n54["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/pneumococcus_serotype_trends.parquet" target="_blank" rel="noreferrer">pneumococcus_serotype_trends.parquet</a></strong>`"]
        n55["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/rsv_ed_visits_by_county.parquet" target="_blank" rel="noreferrer">rsv_ed_visits_by_county.parquet</a></strong>`"]
        n56["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/rsv_google_dma.parquet" target="_blank" rel="noreferrer">rsv_google_dma.parquet</a></strong>`"]
        n57["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/rsv_overall_trends.parquet" target="_blank" rel="noreferrer">rsv_overall_trends.parquet</a></strong>`"]
        n58["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/rsv_positive_tests.parquet" target="_blank" rel="noreferrer">rsv_positive_tests.parquet</a></strong>`"]
        n59["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/rsv_testing_pct.parquet" target="_blank" rel="noreferrer">rsv_testing_pct.parquet</a></strong>`"]
        n60["`<strong><a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/rsv_trends_by_age.parquet" target="_blank" rel="noreferrer">rsv_trends_by_age.parquet</a></strong>`"]
    end
    s0---s1["<strong><a href="https://data.cdc.gov/resource/qvzb-qs6p/" target="_blank" rel="noreferrer">Serotype Data for Invasive Pneumococcal Disease Cases by Age Group from Active Bacterial Core surveillance</a></strong>"]
    s1 --> n1
    s1 --> n2
    s2---s3["<strong><a href="https://pubmed.ncbi.nlm.nih.gov/39758745/" target="_blank" rel="noreferrer">Open Forum for Infectious Diseases</a></strong>"]
    s3 --> n2
    s4---s5["<strong><a href="https://data.cdc.gov/Behavioral-Risk-Factors/Behavioral-Risk-Factor-Surveillance-System-BRFSS-P/dttw-5yxu/about_data" target="_blank" rel="noreferrer">Behavioral Risk Factor Surveillance System (BRFSS) Prevalence Data (2011 to present)</a></strong>"]
    s5 --> n3
    s6---s7["<strong><a href="https://data.cms.gov/tools/mapping-medicare-disparities-by-population" target="_blank" rel="noreferrer">Mapping Medicare Disparities by Population Tool</a></strong>"]
    s7 --> n4
    s8---s9["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/doctor-visits.html" target="_blank" rel="noreferrer">COVIDcast > Doctor Visits</a></strong>"]
    s9 --> n5
    s8---s10["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/hospital-admissions.html" target="_blank" rel="noreferrer">COVIDcast > Hospital Admissions</a></strong>"]
    s10 --> n6
    s8---s11["<strong><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/nhsn.html" target="_blank" rel="noreferrer">COVIDcast > National Healthcare Safety Network Respiratory Hospitalizations</a></strong>"]
    s11 --> n7
    s12 --> n8
    s12 --> n9
    s12 --> n10
    s12 --> n11
    s12 --> n12
    s12 --> n13
    s13---s14["<strong><a href="https://github.com/DISSC-yale/gtrends_collection" target="_blank" rel="noreferrer">Yale Data-Intensive Social Sciences, Google Trends Collection Framework</a></strong>"]
    s14 --> n15
    s15---s16["<strong><a href="https://data.cdc.gov/National-Center-for-Health-Statistics/VSRR-Provisional-Drug-Overdose-Death-Counts/xkb8-kh2a/about_data" target="_blank" rel="noreferrer">VSRR Provisional Drug Overdose Death Counts</a></strong>"]
    s16 --> n16
    s16 --> n17
    s17---s18["<strong><a href="https://www.cdc.gov/nis/about/index.html" target="_blank" rel="noreferrer">About the National Immunization Surveys (NIS)</a></strong>"]
    s18 --> n18
    s18 --> n19
    s18 --> n20
    s0---s19["<strong><a href="https://data.cdc.gov/resource/3cxc-4k8q" target="_blank" rel="noreferrer">Percent Positivity of Respiratory Syncytial Virus Nucleic Acid Amplification Tests by HHS Region, National Respiratory and Enteric Virus Surveillance System</a></strong>"]
    s19 --> n21
    s20---s21["<strong><a href="https://www.cdc.gov/nwss/rv/COVID19-statetrend.html" target="_blank" rel="noreferrer">Wastewater COVID-19 State and Territory Trends</a></strong>"]
    s21 --> n26
    s20---s22["<strong><a href="https://www.cdc.gov/nwss/rv/InfluenzaA-statetrend.html" target="_blank" rel="noreferrer">Wastewater Influenza A State and Territory Trends</a></strong>"]
    s22 --> n26
    s20---s23["<strong><a href="https://www.cdc.gov/nwss/rv/RSV-statetrend.html" target="_blank" rel="noreferrer">Wastewater RSV State and Territory Trends</a></strong>"]
    s23 --> n26
    n25 --> bundle_childhood_immunizations
    n24 --> bundle_childhood_immunizations
    n20 --> bundle_childhood_immunizations
    n19 --> bundle_childhood_immunizations
    n18 --> bundle_childhood_immunizations
    n8 --> bundle_childhood_immunizations
    n3 --> bundle_chronic_diseases
    n12 --> bundle_chronic_diseases
    n4 --> bundle_chronic_diseases
    n12 --> bundle_injury_overdose
    n3 --> bundle_injury_overdose
    n4 --> bundle_injury_overdose
    n17 --> bundle_injury_overdose
    n16 --> bundle_injury_overdose
    n13 --> bundle_respiratory
    n15 --> bundle_respiratory
    n26 --> bundle_respiratory
    n1 --> bundle_respiratory
    n2 --> bundle_respiratory
    n21 --> bundle_respiratory
    n22 --> bundle_respiratory
    n23 --> bundle_respiratory
```
