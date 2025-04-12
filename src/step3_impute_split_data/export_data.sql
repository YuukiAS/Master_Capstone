-- Export confounding variables to csv for imputation
.mode csv
.headers on
.output data_raw.csv


SELECT
    eid,
    `34-0.0` AS birth_year,
    `52-0.0` AS birth_month,
    `20116-0.0` AS smoking,      -- (0=Never, 1=Previous, 2=Current)
    `31-0.0` AS sex,            -- (0=Female, 1=Male)
    `2443-0.0` AS diabetes,      -- (1=Yes, 0=No)
    `4080-0.0` AS systolic_bp,   -- Systolic Blood Pressure
    `21000-0.0` AS ethnicity,    -- (1=White, 2=Mixed, 3=Asian, 4=Black, 5=Chinese, 6=Other)
    `50-0.0` AS height,
    `21002-0.0` AS body_weight,
    `6177-0.0` AS med_male,      -- Medication for Males
    `6153-0.0` AS med_female,    -- Medication for Females
    `6150-0.0` AS diagnosis,     -- Diagnosed Problems
    `30690-0.0` AS total_chol,   -- Total Cholesterol
    `30760-0.0` AS hdl_chol,     -- HDL Cholesterol
    `6138-0.0` AS education,     -- Education Attainment
    `22032-0.0` AS activity,      -- IPAQ Activity Group (0=low, 1=moderate, 2=high)
    `6032-0.0` AS max_workload,  -- Maximum Workload
    `6033-0.0` AS max_heart_rate -- Maximum Heart Rate
FROM
    Variables;