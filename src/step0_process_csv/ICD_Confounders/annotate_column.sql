-- Add annotations to the column
CREATE TABLE
    Annotations (column_name TEXT, annotation TEXT);

INSERT INTO
    Annotations (column_name, annotation)
VALUES
    ('34-0.0', 'Year of birth');

INSERT INTO
    Annotations (column_name, annotation)
VALUES
    ('52-0.0', 'Month of birth');


INSERT INTO
    Annotations (column_name, annotation)
VALUES
    ('20116-0.0', 'Smoking Status (0=Never, 1=Previous, 2=Current, -3=Prefer not to answer)');

INSERT INTO
    Annotations (column_name, annotation)
VALUES
    ('31-0.0', 'Sex (0=Female, 1=Male)');

INSERT INTO
    Annotations (column_name, annotation)
VALUES
    ('2443-0.0', 'Diabetes Status (1=Yes, 0=No, -1=Do not know, -3=Prefer not to answer)');

INSERT INTO
    Annotations (column_name, annotation)
VALUES
    ('4080-0.0', 'Systolic Blood Pressure');

INSERT INTO
    Annotations (column_name, annotation)
VALUES
    ('21000-0.0', 'Ethnicity (1=White, 2=Mixed, 3=Asian or Asian British, 4=Black or Black British, 5=Chinese, 6=Other ethnic group, -1=Do not know, -3=Prefer not to answer)');

INSERT INTO
    Annotations (column_name, annotation)
VALUES
    ('50-0.0', 'Height');

INSERT INTO
    Annotations (column_name, annotation)
VALUES
    ('21002-0.0', 'Weight');

INSERT INTO
    Annotations (column_name, annotation)
VALUES
    (
        '6177-0.0',
        'Medication for Males: (1=Cholesterol lowering medication, 2=Blood pressure medication, 3=Insulin, -7=None of the above, -1=Do not know, -3=Prefer not to answer)'
    );

INSERT INTO
    Annotations (column_name, annotation)
VALUES
    (
        '6153-0.0',
        'Medication for Females: (1=Cholesterol lowering medication, 2=Blood pressure medication, 3=Insulin, 4=Hormone replacement therapy, 5=Oral contraceptive pill or minipill, -7=None of the above, -1=Do not know, -3=Prefer not to answer)'
    );

INSERT INTO
    Annotations (column_name, annotation)
VALUES
    ('6150-0.0', 'Diagnosed Problem: (1=Heart attack, 2=Angina, 3=Stroke, 4=High blood pressure, -7=None of the above, -3=Prefer not to answer)');

INSERT INTO
    Annotations (column_name, annotation)
VALUES
    ('30690-0.0', 'Total Cholesterol');

INSERT INTO
    Annotations (column_name, annotation)
VALUES
    ('30760-0.0', 'HDL Cholesterol');

INSERT INTO
    Annotations (column_name, annotation)
VALUES
    ('6138-0.0', 'Education Attainment (1=College or University degree, 2=A levels/AS levels or equivalent, 3=O levels/GCSEs or equivalent, 4=CSEs or equivalent, 5=NVQ or HND or HNC or equivalent, 6=Other professional qualifications eg: nursing, teaching, -7=None of the above, -3=Prefer not to answer)');

INSERT INTO
    Annotations (column_name, annotation)
VALUES
    ('22032-0.0', 'IPAQ Activity Group (0=low, 1=moderate, 2=high)');


