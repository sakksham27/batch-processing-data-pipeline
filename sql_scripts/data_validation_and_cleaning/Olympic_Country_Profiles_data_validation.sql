/*
======================================================
Olympic Country Profile — Data Quality Checks
======================================================
*/

SELECT *
FROM bronze.olympic_country_profiles

SELECT noc
FROM bronze.olympic_country_profiles
WHERE TRIM(noc) !~ '^[A-Za-z]+$'

SELECT country 
FROM bronze.olympic_country_profiles
WHERE TRIM(country) !~ '^[A-Za-z ]+$'

SELECT 
noc,
country
FROM bronze.olympic_country_profiles;
