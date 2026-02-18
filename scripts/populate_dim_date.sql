-- ============================================================
-- Populate dim_date from 2020-01-01 through 2030-12-31
-- Compatible with SQLite (uses recursive CTE)
-- ============================================================

-- Clear existing data
DELETE FROM dim_date;

-- Generate all dates from 2020-01-01 to 2030-12-31
WITH RECURSIVE dates(d) AS (
    SELECT '2020-01-01'
    UNION ALL
    SELECT date(d, '+1 day')
    FROM dates
    WHERE d < '2030-12-31'
)
INSERT INTO dim_date (
    date_key,
    full_date,
    day_of_week,
    day_name,
    day_of_month,
    day_of_year,
    week_of_year,
    month_num,
    month_name,
    month_short,
    quarter_num,
    quarter_name,
    year_num,
    fiscal_month,
    fiscal_quarter,
    fiscal_year,
    is_weekend,
    is_holiday
)
SELECT
    CAST(strftime('%Y%m%d', d) AS INTEGER) AS date_key,
    d AS full_date,
    CAST(strftime('%w', d) AS INTEGER) AS day_of_week,
    CASE CAST(strftime('%w', d) AS INTEGER)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_name,
    CAST(strftime('%d', d) AS INTEGER) AS day_of_month,
    CAST(strftime('%j', d) AS INTEGER) AS day_of_year,
    CAST(strftime('%W', d) AS INTEGER) AS week_of_year,
    CAST(strftime('%m', d) AS INTEGER) AS month_num,
    CASE CAST(strftime('%m', d) AS INTEGER)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_name,
    CASE CAST(strftime('%m', d) AS INTEGER)
        WHEN 1 THEN 'Jan'
        WHEN 2 THEN 'Feb'
        WHEN 3 THEN 'Mar'
        WHEN 4 THEN 'Apr'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'Jun'
        WHEN 7 THEN 'Jul'
        WHEN 8 THEN 'Aug'
        WHEN 9 THEN 'Sep'
        WHEN 10 THEN 'Oct'
        WHEN 11 THEN 'Nov'
        WHEN 12 THEN 'Dec'
    END AS month_short,
    CASE
        WHEN CAST(strftime('%m', d) AS INTEGER) BETWEEN 1 AND 3 THEN 1
        WHEN CAST(strftime('%m', d) AS INTEGER) BETWEEN 4 AND 6 THEN 2
        WHEN CAST(strftime('%m', d) AS INTEGER) BETWEEN 7 AND 9 THEN 3
        ELSE 4
    END AS quarter_num,
    'Q' || CASE
        WHEN CAST(strftime('%m', d) AS INTEGER) BETWEEN 1 AND 3 THEN '1'
        WHEN CAST(strftime('%m', d) AS INTEGER) BETWEEN 4 AND 6 THEN '2'
        WHEN CAST(strftime('%m', d) AS INTEGER) BETWEEN 7 AND 9 THEN '3'
        ELSE '4'
    END AS quarter_name,
    CAST(strftime('%Y', d) AS INTEGER) AS year_num,
    -- Fiscal calendar (assuming Jan = fiscal month 1; adjust if needed)
    CAST(strftime('%m', d) AS INTEGER) AS fiscal_month,
    CASE
        WHEN CAST(strftime('%m', d) AS INTEGER) BETWEEN 1 AND 3 THEN 1
        WHEN CAST(strftime('%m', d) AS INTEGER) BETWEEN 4 AND 6 THEN 2
        WHEN CAST(strftime('%m', d) AS INTEGER) BETWEEN 7 AND 9 THEN 3
        ELSE 4
    END AS fiscal_quarter,
    CAST(strftime('%Y', d) AS INTEGER) AS fiscal_year,
    CASE WHEN CAST(strftime('%w', d) AS INTEGER) IN (0, 6) THEN 1 ELSE 0 END AS is_weekend,
    0 AS is_holiday
FROM dates;
