/*
======================================================================
Olympic Athlete Biography — Data Quality Checks : Bronze Schema
======================================================================
*/ -- Preview sample data for initial inspection

SELECT born
FROM bronze.olympic_athlete_biography
WHERE LOWER(born) ~ '^\d{4}$';

-- Check if 'athlete_id' contains only numeric values
-- Note: Will raise an error if any value is non-numeric

SELECT athlete_id + 1 AS numeric_check
FROM bronze.olympic_athlete_biography;

-- Check for inconsistencies in 'name' formatting
-- Goal: Ensure proper casing and remove extra whitespace

SELECT name,
       INITCAP(name) AS capitalized_name,
       TRIM(name) AS trimmed_name
FROM bronze.olympic_athlete_biography
WHERE name != INITCAP(name)
  OR name != TRIM(name);

-- Categorize 'name' values based on character type
-- Expectation: Should be alphanumeric only

SELECT CASE
           WHEN name ~ '^[A-Za-z0-9]+$' THEN 'Alphanumeric'
           ELSE 'Non-Alphanumeric'
       END AS name_type,
       COUNT(*)
FROM bronze.olympic_athlete_biography
GROUP BY name_type;

-- Check for missing values in 'name'

SELECT name
FROM bronze.olympic_athlete_biography
WHERE name IS NULL;

-- ===============================================
-- 'Sex' column validation
-- ===============================================
 -- Identify values with leading/trailing whitespace

SELECT sex
FROM bronze.olympic_athlete_biography
WHERE sex != TRIM(sex);

-- Check for proper capitalization (e.g., 'Male', 'Female')

SELECT sex,
       INITCAP(sex)
FROM bronze.olympic_athlete_biography
WHERE sex = INITCAP(sex);

-- Check for missing values in 'sex'

SELECT sex
FROM bronze.olympic_athlete_biography
WHERE sex IS NULL;

-- ===============================================
-- 'Born' column normalization
-- Supports flexible date formats (e.g., 'June 2000', '2000', '14 July 1989')
-- Handles numeric-only or special character inconsistencies
-- ===============================================

SELECT born,
       CASE
           WHEN LOWER(born) ~ '^[A-Za-z]{3,9} \d{4}$' THEN daterange(TO_DATE('01 '||born, 'DD Month YYYY'), (DATE_TRUNC('month', TO_DATE(born, 'Month YYYY')) + INTERVAL '1 month - 1 day')::DATE, '[]')
           WHEN LOWER(born) ~ '^\d{4}$' THEN daterange(TO_DATE(born, 'YYYY'), (DATE_TRUNC('year', TO_DATE(born, 'YYYY')) + INTERVAL '1 year - 1 day')::DATE, '[]')
           WHEN LOWER(born) !~ '^[a-z0-9 ]+$' THEN CASE
                                                       WHEN NULLIF(regexp_replace(born, '[^0-9]', '', 'g'), '')::numeric > 2025 THEN daterange(TO_DATE(FLOOR(NULLIF(regexp_replace(born, '[^0-9]', '', 'g'), '')::numeric / 10000)::text, 'YYYY'), (TO_DATE((NULLIF(regexp_replace(born, '[^0-9]', '', 'g'), '')::numeric % 10000)::text || '-12-01', 'YYYY-MM-DD') + INTERVAL '1 month - 1 day')::DATE, '[]')
                                                       ELSE daterange(TO_DATE(NULLIF(regexp_replace(born, '[^0-9]', '', 'g'), '')::numeric::text, 'YYYY'), TO_DATE(NULLIF(regexp_replace(born, '[^0-9]', '', 'g'), '')::numeric::text || '-12-31', 'YYYY-MM-DD'), '[]')
                                                   END
           ELSE daterange(TO_DATE(born, 'DD Month YYYY'), TO_DATE(born, 'DD Month YYYY'), '[]')
       END AS formatted_dob
FROM bronze.olympic_athlete_biography;

-- formatiing to daterange
-- 1)

SELECT '01 '||born,
       daterange(TO_DATE('01 '||born, 'DD Month YYYY'), (DATE_TRUNC('month', TO_DATE(born, 'Month YYYY')) + INTERVAL '1 month - 1 day')::DATE, '[]')
FROM bronze.olympic_athlete_biography
WHERE LOWER(born) ~ '^[A-Za-z]{3,9} \d{4}$';

-- 2)

SELECT born,
       daterange(TO_DATE(born, 'YYYY'), (DATE_TRUNC('year', TO_DATE(born, 'YYYY')) + INTERVAL '1 year - 1 day')::DATE, '[]')
FROM bronze.olympic_athlete_biography
WHERE LOWER(born) ~ '^\d{4}$';

-- 3)
-- a)

SELECT born,
       daterange(TO_DATE(FLOOR(NULLIF(regexp_replace(born, '[^0-9]', '', 'g'), '')::numeric / 10000)::text, 'YYYY'), (TO_DATE((NULLIF(regexp_replace(born, '[^0-9]', '', 'g'), '')::numeric % 10000)::text || '-12-01', 'YYYY-MM-DD') + INTERVAL '1 month - 1 day')::DATE, '[]')
FROM bronze.olympic_athlete_biography
WHERE LOWER(born) !~ '^[a-z0-9 ]+$'
  AND NULLIF(regexp_replace(born, '[^0-9]', '', 'g'), '')::numeric > 2025;

-- 3)
-- b)

SELECT born,
       daterange(TO_DATE(NULLIF(regexp_replace(born, '[^0-9]', '', 'g'), '')::numeric::text, 'YYYY'), TO_DATE(NULLIF(regexp_replace(born, '[^0-9]', '', 'g'), '')::numeric::text || '-12-31', 'YYYY-MM-DD'), '[]')
FROM bronze.olympic_athlete_biography
WHERE LOWER(born) !~ '^[a-z0-9 ]+$'
  AND NULLIF(regexp_replace(born, '[^0-9]', '', 'g'), '')::numeric < 2025;

-- ===============================================
-- 'Height' column check
-- Validate that all values are numeric or decimal numbers
-- ===============================================

SELECT height
FROM bronze.olympic_athlete_biography
WHERE height::text !~ '^[0-9.]+$';

-- ===============================================
-- 'Weight' column validation and normalization
-- Handles both clean numeric values and ranges (e.g., '55-65')
-- ===============================================
 -- Identify and preview unclean 'weight' entries

SELECT weight,
       CASE
           WHEN weight !~ '^[0-9.]+$' THEN TRIM(weight::text)
           ELSE 'skip'
       END AS cleaned_weight
FROM bronze.olympic_athlete_biography;

-- Normalize weights by averaging min and max in ranges

SELECT b.weight,
       CASE
           WHEN b.weight !~ '^[0-9.]+$' THEN ROUND(((m.matches)[1])::numeric + (m.matches)[3]::numeric) / 2
           ELSE b.weight::numeric
       END AS formatted_weight
FROM bronze.olympic_athlete_biography AS b
LEFT JOIN LATERAL
  (SELECT regexp_matches(TRIM(b.weight::text), '(\d+)([^a-zA-Z0-9])(\d+)', 'g') AS matches) m ON TRUE;

-- ===============================================
-- 'Country' column quality checks
-- ===============================================
 -- Check for extra spaces in 'country' values

SELECT country,
       TRIM(country)
FROM bronze.olympic_athlete_biography
WHERE country != TRIM(country);

-- Detect clearly invalid entries (e.g., only numbers/spaces)

SELECT country
FROM bronze.olympic_athlete_biography
WHERE TRIM(country) ~ '^[0-9 ]+$';

-- ===============================================
-- 'country_noc' (3-letter code) format validation
-- ===============================================

SELECT country_noc
FROM bronze.olympic_athlete_biography
WHERE LENGTH(country_noc) != 3;

-- =============================================================
-- Final Transformation — Cleaned Athlete Biography Table
-- =============================================================

SELECT athlete_id,
       INITCAP(TRIM(name)) AS name,
       CASE
           WHEN LOWER(born) ~ '^[A-Za-z]{3,9} \d{4}$' THEN daterange(TO_DATE('01 '||born, 'DD Month YYYY'), (DATE_TRUNC('month', TO_DATE(born, 'Month YYYY')) + INTERVAL '1 month - 1 day')::DATE, '[]')
           WHEN LOWER(born) ~ '^\d{4}$' THEN daterange(TO_DATE(born, 'YYYY'), (DATE_TRUNC('year', TO_DATE(born, 'YYYY')) + INTERVAL '1 year - 1 day')::DATE, '[]')
           WHEN LOWER(born) !~ '^[a-z0-9 ]+$' THEN CASE
                                                       WHEN NULLIF(regexp_replace(born, '[^0-9]', '', 'g'), '')::numeric > 2025 THEN daterange(TO_DATE(FLOOR(NULLIF(regexp_replace(born, '[^0-9]', '', 'g'), '')::numeric / 10000)::text, 'YYYY'), (TO_DATE((NULLIF(regexp_replace(born, '[^0-9]', '', 'g'), '')::numeric % 10000)::text || '-12-01', 'YYYY-MM-DD') + INTERVAL '1 month - 1 day')::DATE, '[]')
                                                       ELSE daterange(TO_DATE(NULLIF(regexp_replace(born, '[^0-9]', '', 'g'), '')::numeric::text, 'YYYY'), TO_DATE(NULLIF(regexp_replace(born, '[^0-9]', '', 'g'), '')::numeric::text || '-12-31', 'YYYY-MM-DD'), '[]')
                                                   END
           ELSE daterange(TO_DATE(born, 'DD Month YYYY'), TO_DATE(born, 'DD Month YYYY'), '[]')
       END AS born,
       height,
       CASE
           WHEN b.weight !~ '^[0-9.]+$' THEN ROUND(((m.matches)[1]::numeric + (m.matches)[3]::numeric) / 2)
           ELSE b.weight::numeric
       END AS weight,
       TRIM(country) AS country,
       country_noc,
       description,
       special_notes
FROM bronze.olympic_athlete_biography AS b
LEFT JOIN LATERAL
  (SELECT regexp_matches(TRIM(b.weight::text), '(\d+)([^a-zA-Z0-9])(\d+)', 'g') AS matches) m ON TRUE;
