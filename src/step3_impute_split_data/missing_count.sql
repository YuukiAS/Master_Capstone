-- set the output mode to csv
.mode csv
-- show the headers
.headers on
-- set the output file name
.output missing_count.csv
-- SELECT NAME FROM sqlite_master WHERE TYPE='table';  -- note that "SELECT NAME" is SQLite specific
-- PRAGMA table_info(Variables);

SELECT
    COUNT(*) AS total_rows,
    SUM(
        CASE
            WHEN `34-0.0` IS NOT NULL THEN 0
            ELSE 1
        END
    ) AS null_A_21002,
    SUM(
        CASE
            WHEN `52-0.0` IS NOT NULL THEN 0
            ELSE 1
        END
    ) AS null_A_52,
    SUM(
        CASE
            WHEN `20116-0.0` IS NOT NULL THEN 0
            ELSE 1
        END
    ) AS null_A_20116,
    SUM(
        CASE
            WHEN `31-0.0` IS NOT NULL THEN 0
            ELSE 1
        END
    ) AS null_A_31,
    SUM(
        CASE
            WHEN `2443-0.0` IS NOT NULL THEN 0
            ELSE 1
        END
    ) AS null_A_2443,
    SUM(
        CASE
            WHEN `4080-0.0` IS NOT NULL THEN 0
            ELSE 1
        END
    ) AS null_A_4080,
    SUM(
        CASE
            WHEN `21000-0.0` IS NOT NULL THEN 0
            ELSE 1
        END
    ) AS null_A_21000,
    SUM(
        CASE
            WHEN `50-0.0` IS NOT NULL THEN 0
            ELSE 1
        END
    ) AS null_A_50,
    SUM(
        CASE
            WHEN `21002-0.0` IS NOT NULL THEN 0
            ELSE 1
        END
    ) AS null_A_21002,
    SUM(
        CASE
            WHEN `6177-0.0` IS NOT NULL THEN 0
            ELSE 1
        END
    ) AS null_A_6177,
    SUM(
        CASE
            WHEN `6153-0.0` IS NOT NULL THEN 0
            ELSE 1
        END
    ) AS null_A_6153,
    SUM(
        CASE
            WHEN `6150-0.0` IS NOT NULL THEN 0
            ELSE 1
        END
    ) AS null_A_6150,
    SUM(
        CASE
            WHEN `30690-0.0` IS NOT NULL THEN 0
            ELSE 1
        END
    ) AS null_A_30690,
    SUM(
        CASE
            WHEN `30760-0.0` IS NOT NULL THEN 0
            ELSE 1
        END
    ) AS null_A_30760,
    SUM(
        CASE
            WHEN `6138-0.0` IS NOT NULL THEN 0
            ELSE 1
        END
    ) AS null_A_6138,
    SUM(
        CASE
            WHEN `22032-0.0` IS NOT NULL THEN 0
            ELSE 1
        END
    ) AS null_A_22032,
    SUM(
        CASE
            WHEN `6032-0.0` IS NOT NULL THEN 0
            ELSE 1
        END
    ) AS null_A_6032,
    SUM(
        CASE
            WHEN `6033-0.0` IS NOT NULL THEN 0
            ELSE 1
        END
    ) AS null_A_6033,

    SUM(
        CASE
            WHEN `20116-0.0` = -3 THEN 1
            ELSE 0
        END
    ) AS null_B_20116,
    SUM(
        CASE
            WHEN `2443-0.0` IN (-1, -3) THEN 1
            ELSE 0
        END
    ) AS null_B_2443,
    SUM(
        CASE
            WHEN `21000-0.0` IN (-1, -3) THEN 1
            ELSE 0
        END
    ) AS null_B_21000,
    SUM(
        CASE
            WHEN `6177-0.0` IN (-1, -3) THEN 1
            ELSE 0
        END
    ) AS null_B_6177,
    SUM(
        CASE
            WHEN `6153-0.0` IN (-1, -3) THEN 1
            ELSE 0
        END
    ) AS null_B_6153,
    SUM(
        CASE
            WHEN `6150-0.0` = -3 THEN 1
            ELSE 0
        END
    ) AS null_B_6150,
    SUM(
        CASE
            WHEN `6138-0.0` = -3 THEN 1
            ELSE 0
        END
    ) AS null_B_6150
FROM
    Variables;