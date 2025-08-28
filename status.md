```mermaid
flowchart LR
    classDef pass stroke:#66bb6a
    classDef warn stroke:#ffa726
    classDef fail stroke:#f44336
    s0("`<h4><a href="https://data.cdc.gov" target="_blank" rel="noreferrer">Center for Disease Control and Prevention</a></h4><br/><ul><br/><li><code><a href="https://data.cdc.gov/resource/qvzb-qs6p/" target="_blank" rel="noreferrer">Serotype Data for Invasive Pneumococcal Disease Cases by Age Group from Active Bacterial Core surveillance</a></code></li></ul><br/><ul><br/><li><code><a href="https://data.cdc.gov/resource/3cxc-4k8q" target="_blank" rel="noreferrer">Percent Positivity of Respiratory Syncytial Virus Nucleic Acid Amplification Tests by HHS Region, National Respiratory and Enteric Virus Surveillance System</a></code></li></ul>`")
    s1("`<h4><a href="https://pubmed.ncbi.nlm.nih.gov/39758745/" target="_blank" rel="noreferrer">Streptococcus pneumoniae Serotype Distribution Among US Adults Hospitalized With Community-Acquired Pneumonia, 2019-2020</a></h4><br/><ul><br/><li><code><a href="https://pubmed.ncbi.nlm.nih.gov/39758745/" target="_blank" rel="noreferrer">Open Forum for Infectious Diseases</a></code></li></ul>`")
    s2("`<h4><a href="https://data.cdc.gov" target="_blank" rel="noreferrer">Center of Medicare and Medicaid Services (CMS)</a></h4><br/><ul><br/><li><code><a href="https://data.cms.gov/tools/mapping-medicare-disparities-by-population" target="_blank" rel="noreferrer">Mapping Medicare Disparities by Population Tool</a></code></li></ul>`")
    s3("`<h4><a href="https://cmu-delphi.github.io/delphi-epidata/" target="_blank" rel="noreferrer">CMU Delphi</a></h4><br/><ul><br/><li><code><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/doctor-visits.html" target="_blank" rel="noreferrer">COVIDcast > Doctor Visits</a></code></li></ul><br/><ul><br/><li><code><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/hospital-admissions.html" target="_blank" rel="noreferrer">COVIDcast > Hospital Admissions</a></code></li></ul><br/><ul><br/><li><code><a href="https://cmu-delphi.github.io/delphi-epidata/api/covidcast-signals/nhsn.html" target="_blank" rel="noreferrer">COVIDcast > National Healthcare Safety Network Respiratory Hospitalizations</a></code></li></ul>`")
    s4("`<h4><a href="https://cosmos.epic.com/" target="_blank" rel="noreferrer">Epic Cosmos</a></h4>`")
    s5("`<h4><a href="https://trends.google.com" target="_blank" rel="noreferrer">Google Trends</a></h4><br/><ul><br/><li><code><a href="https://github.com/DISSC-yale/gtrends_collection" target="_blank" rel="noreferrer">Yale Data-Intensive Social Sciences, Google Trends Collection Framework</a></code></li></ul>`")
    s6("`<h4><a href="https://www.cdc.gov/nchs/nvss/vsrr/drug-overdose-data.htm" target="_blank" rel="noreferrer">Center for Disease Control and Prevention (NCHS)</a></h4><br/><ul><br/><li><code><a href="https://data.cdc.gov/National-Center-for-Health-Statistics/VSRR-Provisional-Drug-Overdose-Death-Counts/xkb8-kh2a/about_data" target="_blank" rel="noreferrer">VSRR Provisional Drug Overdose Death Counts</a></code></li></ul><br/><ul><br/><li><code><a href="https://data.cdc.gov/National-Center-for-Health-Statistics/VSRR-Provisional-Drug-Overdose-Death-Counts/xkb8-kh2a/about_data" target="_blank" rel="noreferrer">VSRR Provisional Drug Overdose Death counts</a></code></li></ul>`")
    s7("`<h4><a href="https://www.cdc.gov/nis/about/index.html" target="_blank" rel="noreferrer">National Immunization Survey</a></h4><br/><ul><br/><li><code><a href="https://www.cdc.gov/nis/about/index.html" target="_blank" rel="noreferrer">About the National Immunization Surveys (NIS)</a></code></li></ul>`")
    s8("`<h4><a href="https://www.cdc.gov/nwss" target="_blank" rel="noreferrer">National Wastewater Surveillance System</a></h4><br/><ul><br/><li><code><a href="https://www.cdc.gov/nwss/rv/COVID19-statetrend.html" target="_blank" rel="noreferrer">Wastewater COVID-19 State and Territory Trends</a></code></li></ul><br/><ul><br/><li><code><a href="https://www.cdc.gov/nwss/rv/InfluenzaA-statetrend.html" target="_blank" rel="noreferrer">Wastewater Influenza A State and Territory Trends</a></code></li></ul><br/><ul><br/><li><code><a href="https://www.cdc.gov/nwss/rv/RSV-statetrend.html" target="_blank" rel="noreferrer">Wastewater RSV State and Territory Trends</a></code></li></ul>`")
    subgraph abcs["`<a href="https://github.com/PopHIVE/Ingest/tree/main/data/abcs" target="_blank" rel="noreferrer">abcs</a>`"]
        direction LR
        n1["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/abcs/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a>`"]:::pass
        n2["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/abcs/standard/uad.csv.gz" target="_blank" rel="noreferrer">uad.csv.gz</a>`"]:::pass
    end
    subgraph brfss["`<a href="https://github.com/PopHIVE/Ingest/tree/main/data/brfss" target="_blank" rel="noreferrer">brfss</a>`"]
        direction LR
        n3["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/brfss/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a>`"]:::pass
    end
    subgraph cms_mmd["`<a href="https://github.com/PopHIVE/Ingest/tree/main/data/cms_mmd" target="_blank" rel="noreferrer">cms_mmd</a>`"]
        direction LR
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_acute_myocardial_infarction</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_alzheimers</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_anemia</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_asthma</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_atrial_fibrilation</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_chronic_kidney</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_colorectal_breast_prostate_lung_cancer</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_copd</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_depression</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_diabetes</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_glaucoma</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_heart_failure_non_ischemic</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_hip_pelvic_fracture</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_hyperlidipemia</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_hypertension</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_ischemic_heart_disease</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_obesity</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_osteoporosis</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_parkinsons</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_rheumoatoid_arthritis</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_schizophrenia_and_psycotic</code></li></ul>`"]:::warn
        n4["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/cms_mmd/standard/data_state_county_age.csv.gz" target="_blank" rel="noreferrer">data_state_county_age.csv.gz</a><ul><br/><li><code>missing_info: cms_stroke_ischemic_attack</code></li></ul>`"]:::warn
    end
    subgraph delphi_doctors_claims["`<a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_doctors_claims" target="_blank" rel="noreferrer">delphi_doctors_claims</a>`"]
        direction LR
        n5["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/delphi_doctors_claims/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a>`"]:::pass
    end
    subgraph delphi_hospital_claims["`<a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_hospital_claims" target="_blank" rel="noreferrer">delphi_hospital_claims</a>`"]
        direction LR
        n6["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/delphi_hospital_claims/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a>`"]:::pass
    end
    subgraph delphi_nhsn["`<a href="https://github.com/PopHIVE/Ingest/tree/main/data/delphi_nhsn" target="_blank" rel="noreferrer">delphi_nhsn</a>`"]
        direction LR
        n7["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/delphi_nhsn/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a>`"]:::pass
    end
    subgraph epic["`<a href="https://github.com/PopHIVE/Ingest/tree/main/data/epic" target="_blank" rel="noreferrer">epic</a>`"]
        direction LR
        n8["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/children.csv.gz" target="_blank" rel="noreferrer">children.csv.gz</a><ul><br/><li><code>time_missing</code></li></ul>`"]:::warn
        n9["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/county_no_time.csv.gz" target="_blank" rel="noreferrer">county_no_time.csv.gz</a><ul><br/><li><code>time_missing</code></li></ul>`"]:::warn
        n10["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/no_geo.csv.gz" target="_blank" rel="noreferrer">no_geo.csv.gz</a><ul><br/><li><code>missing_info: positive_rsv_tests_(%)</code></li></ul>`"]:::warn
        n11["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/state_no_time.csv.gz" target="_blank" rel="noreferrer">state_no_time.csv.gz</a><ul><br/><li><code>time_missing</code></li></ul>`"]:::warn
        n11["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/state_no_time.csv.gz" target="_blank" rel="noreferrer">state_no_time.csv.gz</a><ul><br/><li><code>missing_info: n_self_harm</code></li></ul>`"]:::warn
        n11["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/state_no_time.csv.gz" target="_blank" rel="noreferrer">state_no_time.csv.gz</a><ul><br/><li><code>missing_info: n_patients</code></li></ul>`"]:::warn
        n12["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/weekly.csv.gz" target="_blank" rel="noreferrer">weekly.csv.gz</a><ul><br/><li><code>missing_info: epic_positive_rsv_tests_(%)</code></li></ul>`"]:::warn
        n12["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/weekly.csv.gz" target="_blank" rel="noreferrer">weekly.csv.gz</a><ul><br/><li><code>missing_info: epic_rsv_tests</code></li></ul>`"]:::warn
        n12["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/epic/standard/weekly.csv.gz" target="_blank" rel="noreferrer">weekly.csv.gz</a><ul><br/><li><code>missing_info: epic_n_rsv_tests</code></li></ul>`"]:::warn
    end
    subgraph gtrends["`<a href="https://github.com/PopHIVE/Ingest/tree/main/data/gtrends" target="_blank" rel="noreferrer">gtrends</a>`"]
        direction LR
        n13["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/gtrends/standard/data_dma.csv.gz" target="_blank" rel="noreferrer">data_dma.csv.gz</a><ul><br/><li><code>geography_missing</code></li></ul>`"]:::warn
        n13["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/gtrends/standard/data_dma.csv.gz" target="_blank" rel="noreferrer">data_dma.csv.gz</a><ul><br/><li><code>missing_info: fips</code></li></ul>`"]:::warn
        n13["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/gtrends/standard/data_dma.csv.gz" target="_blank" rel="noreferrer">data_dma.csv.gz</a><ul><br/><li><code>missing_info: value</code></li></ul>`"]:::warn
        n13["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/gtrends/standard/data_dma.csv.gz" target="_blank" rel="noreferrer">data_dma.csv.gz</a><ul><br/><li><code>missing_info: term</code></li></ul>`"]:::warn
        n14["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/gtrends/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a><ul><br/><li><code>missing_info: gtrends_rsv_adjusted</code></li></ul>`"]:::warn
    end
    subgraph nchs_mortality["`<a href="https://github.com/PopHIVE/Ingest/tree/main/data/nchs_mortality" target="_blank" rel="noreferrer">nchs_mortality</a>`"]
        direction LR
        n15["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/nchs_mortality/standard/data_county.csv.gz" target="_blank" rel="noreferrer">data_county.csv.gz</a>`"]:::pass
        n16["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/nchs_mortality/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a>`"]:::pass
    end
    subgraph nis["`<a href="https://github.com/PopHIVE/Ingest/tree/main/data/nis" target="_blank" rel="noreferrer">nis</a>`"]
        direction LR
        n17["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/nis/standard/data_insurance.csv.gz" target="_blank" rel="noreferrer">data_insurance.csv.gz</a><ul><br/><li><code>time_missing</code></li></ul>`"]:::warn
        n17["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/nis/standard/data_insurance.csv.gz" target="_blank" rel="noreferrer">data_insurance.csv.gz</a><ul><br/><li><code>missing_info: insurance</code></li></ul>`"]:::warn
        n17["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/nis/standard/data_insurance.csv.gz" target="_blank" rel="noreferrer">data_insurance.csv.gz</a><ul><br/><li><code>missing_info: birth_year</code></li></ul>`"]:::warn
        n17["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/nis/standard/data_insurance.csv.gz" target="_blank" rel="noreferrer">data_insurance.csv.gz</a><ul><br/><li><code>missing_info: vaccine</code></li></ul>`"]:::warn
        n18["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/nis/standard/data_urban.csv.gz" target="_blank" rel="noreferrer">data_urban.csv.gz</a><ul><br/><li><code>time_missing</code></li></ul>`"]:::warn
        n18["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/nis/standard/data_urban.csv.gz" target="_blank" rel="noreferrer">data_urban.csv.gz</a><ul><br/><li><code>missing_info: urban</code></li></ul>`"]:::warn
        n18["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/nis/standard/data_urban.csv.gz" target="_blank" rel="noreferrer">data_urban.csv.gz</a><ul><br/><li><code>missing_info: birth_year</code></li></ul>`"]:::warn
        n18["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/nis/standard/data_urban.csv.gz" target="_blank" rel="noreferrer">data_urban.csv.gz</a><ul><br/><li><code>missing_info: vaccine</code></li></ul>`"]:::warn
        n19["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/nis/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a><ul><br/><li><code>missing_info: birth_year</code></li></ul>`"]:::warn
        n19["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/nis/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a><ul><br/><li><code>missing_info: age</code></li></ul>`"]:::warn
        n19["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/nis/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a><ul><br/><li><code>missing_info: vaccine</code></li></ul>`"]:::warn
    end
    subgraph NREVSS["`<a href="https://github.com/PopHIVE/Ingest/tree/main/data/NREVSS" target="_blank" rel="noreferrer">NREVSS</a>`"]
        direction LR
        n20["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/NREVSS/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a>`"]:::pass
    end
    subgraph nssp["`<a href="https://github.com/PopHIVE/Ingest/tree/main/data/nssp" target="_blank" rel="noreferrer">nssp</a>`"]
        direction LR
        n21["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/nssp/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a>`"]:::pass
    end
    subgraph respnet["`<a href="https://github.com/PopHIVE/Ingest/tree/main/data/respnet" target="_blank" rel="noreferrer">respnet</a>`"]
        direction LR
        n22["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/respnet/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a>`"]:::pass
    end
    subgraph schoolvaxview["`<a href="https://github.com/PopHIVE/Ingest/tree/main/data/schoolvaxview" target="_blank" rel="noreferrer">schoolvaxview</a>`"]
        direction LR
        n23["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/schoolvaxview/standard/data_exemptions.csv.gz" target="_blank" rel="noreferrer">data_exemptions.csv.gz</a><ul><br/><li><code>missing_info: grade</code></li></ul>`"]:::warn
        n23["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/schoolvaxview/standard/data_exemptions.csv.gz" target="_blank" rel="noreferrer">data_exemptions.csv.gz</a><ul><br/><li><code>missing_info: N</code></li></ul>`"]:::warn
        n23["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/schoolvaxview/standard/data_exemptions.csv.gz" target="_blank" rel="noreferrer">data_exemptions.csv.gz</a><ul><br/><li><code>missing_info: vax</code></li></ul>`"]:::warn
        n23["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/schoolvaxview/standard/data_exemptions.csv.gz" target="_blank" rel="noreferrer">data_exemptions.csv.gz</a><ul><br/><li><code>missing_info: value</code></li></ul>`"]:::warn
        n23["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/schoolvaxview/standard/data_exemptions.csv.gz" target="_blank" rel="noreferrer">data_exemptions.csv.gz</a><ul><br/><li><code>missing_info: percent_surveyed</code></li></ul>`"]:::warn
        n23["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/schoolvaxview/standard/data_exemptions.csv.gz" target="_blank" rel="noreferrer">data_exemptions.csv.gz</a><ul><br/><li><code>missing_info: survey_type</code></li></ul>`"]:::warn
        n24["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/schoolvaxview/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a><ul><br/><li><code>missing_info: grade</code></li></ul>`"]:::warn
        n24["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/schoolvaxview/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a><ul><br/><li><code>missing_info: N</code></li></ul>`"]:::warn
        n24["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/schoolvaxview/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a><ul><br/><li><code>missing_info: vax</code></li></ul>`"]:::warn
        n24["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/schoolvaxview/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a><ul><br/><li><code>missing_info: value</code></li></ul>`"]:::warn
        n24["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/schoolvaxview/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a><ul><br/><li><code>missing_info: percent_surveyed</code></li></ul>`"]:::warn
        n24["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/schoolvaxview/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a><ul><br/><li><code>missing_info: survey_type</code></li></ul>`"]:::warn
    end
    subgraph wastewater["`<a href="https://github.com/PopHIVE/Ingest/tree/main/data/wastewater" target="_blank" rel="noreferrer">wastewater</a>`"]
        direction LR
        n25["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/wastewater/standard/data.csv.gz" target="_blank" rel="noreferrer">data.csv.gz</a>`"]:::pass
    end
    subgraph bundle_childhood_immunizations["`<a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_childhood_immunizations" target="_blank" rel="noreferrer">bundle_childhood_immunizations</a>`"]
        direction LR
        n26["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/mmr_rates_epic.parquet" target="_blank" rel="noreferrer">mmr_rates_epic.parquet</a>`"]
        n27["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/nis_insurance.parquet" target="_blank" rel="noreferrer">nis_insurance.parquet</a>`"]
        n28["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/nis_overall.parquet" target="_blank" rel="noreferrer">nis_overall.parquet</a>`"]
        n29["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/nis_urban.parquet" target="_blank" rel="noreferrer">nis_urban.parquet</a>`"]
        n30["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/schoolvaxview_exemptions.parquet" target="_blank" rel="noreferrer">schoolvaxview_exemptions.parquet</a>`"]
        n31["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/schoolvaxview_overall.parquet" target="_blank" rel="noreferrer">schoolvaxview_overall.parquet</a>`"]
        n32["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_childhood_immunizations/dist/state_compare.parquet" target="_blank" rel="noreferrer">state_compare.parquet</a>`"]
    end
    subgraph bundle_chronic_diseases["`<a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_chronic_diseases" target="_blank" rel="noreferrer">bundle_chronic_diseases</a>`"]
        direction LR
        n33["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/brfss_prevalence_by_geography.parquet" target="_blank" rel="noreferrer">brfss_prevalence_by_geography.parquet</a>`"]
        n34["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/epic_prevalence_by_geography_county_and_source.parquet" target="_blank" rel="noreferrer">epic_prevalence_by_geography_county_and_source.parquet</a>`"]
        n35["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/epic_prevalence_by_geography_county.parquet" target="_blank" rel="noreferrer">epic_prevalence_by_geography_county.parquet</a>`"]
        n36["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/epic_prevalence_by_geography.parquet" target="_blank" rel="noreferrer">epic_prevalence_by_geography.parquet</a>`"]
        n37["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/prevalence_by_geography_and_source.parquet" target="_blank" rel="noreferrer">prevalence_by_geography_and_source.parquet</a>`"]
    end
    subgraph bundle_respiratory["`<a href="https://github.com/PopHIVE/Ingest/tree/main/data/bundle_respiratory" target="_blank" rel="noreferrer">bundle_respiratory</a>`"]
        direction LR
        n38["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/covid_ed_visits_by_county.parquet" target="_blank" rel="noreferrer">covid_ed_visits_by_county.parquet</a>`"]
        n39["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/covid_overall_trends.parquet" target="_blank" rel="noreferrer">covid_overall_trends.parquet</a>`"]
        n40["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/covid_trends_by_age.parquet" target="_blank" rel="noreferrer">covid_trends_by_age.parquet</a>`"]
        n41["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/flu_ed_visits_by_county.parquet" target="_blank" rel="noreferrer">flu_ed_visits_by_county.parquet</a>`"]
        n42["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/flu_overall_trends.parquet" target="_blank" rel="noreferrer">flu_overall_trends.parquet</a>`"]
        n43["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/flu_trends_by_age.parquet" target="_blank" rel="noreferrer">flu_trends_by_age.parquet</a>`"]
        n44["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/pneumococcus_by_geography.parquet" target="_blank" rel="noreferrer">pneumococcus_by_geography.parquet</a>`"]
        n45["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/pneumococcus_comparison.parquet" target="_blank" rel="noreferrer">pneumococcus_comparison.parquet</a>`"]
        n46["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/pneumococcus_serotype_trends.parquet" target="_blank" rel="noreferrer">pneumococcus_serotype_trends.parquet</a>`"]
        n47["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/rsv_ed_visits_by_county.parquet" target="_blank" rel="noreferrer">rsv_ed_visits_by_county.parquet</a>`"]
        n48["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/rsv_google_dma.parquet" target="_blank" rel="noreferrer">rsv_google_dma.parquet</a>`"]
        n49["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/rsv_overall_trends.parquet" target="_blank" rel="noreferrer">rsv_overall_trends.parquet</a>`"]
        n50["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/rsv_positive_tests.parquet" target="_blank" rel="noreferrer">rsv_positive_tests.parquet</a>`"]
        n51["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/rsv_testing_pct.parquet" target="_blank" rel="noreferrer">rsv_testing_pct.parquet</a>`"]
        n52["`<a href="https://github.com/PopHIVE/Ingest/blob/main/data/bundle_respiratory/dist/rsv_trends_by_age.parquet" target="_blank" rel="noreferrer">rsv_trends_by_age.parquet</a>`"]
    end
    s0 --> n1
    s0 --> n2
    s1 --> n2
    s2 --> n4
    s3 --> n5
    s3 --> n6
    s3 --> n7
    s4 --> n8
    s4 --> n9
    s4 --> n10
    s4 --> n11
    s4 --> n12
    s5 --> n14
    s6 --> n15
    s6 --> n16
    s7 --> n17
    s7 --> n18
    s7 --> n19
    s0 --> n20
    s8 --> n25
    n24 --> bundle_childhood_immunizations
    n23 --> bundle_childhood_immunizations
    n19 --> bundle_childhood_immunizations
    n18 --> bundle_childhood_immunizations
    n17 --> bundle_childhood_immunizations
    n8 --> bundle_childhood_immunizations
    n3 --> bundle_chronic_diseases
    n11 --> bundle_chronic_diseases
    n12 --> bundle_respiratory
    n14 --> bundle_respiratory
    n25 --> bundle_respiratory
    n1 --> bundle_respiratory
    n2 --> bundle_respiratory
    n20 --> bundle_respiratory
    n21 --> bundle_respiratory
    n22 --> bundle_respiratory
    nNA --> bundle_respiratory
    nNA --> bundle_respiratory
    nNA --> bundle_respiratory
```
