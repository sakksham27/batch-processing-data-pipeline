/*
======================================================
Olympic Athlete Event Details — Data Quality Checks
======================================================
*/

-- Preview the full dataset for initial inspection
SELECT *
FROM bronze.olympic_athlete_event_details;

-- Extract the numeric year from the 'edition' string (e.g., "2008 Summer Olympics" → 2008)
SELECT (STRING_TO_ARRAY(edition, ' '))[1]::numeric as edition_year
FROM bronze.olympic_athlete_event_details;

/* 
Validate 'edition' column for unexpected characters
- Expectation: Only alphanumeric characters and spaces
- Result: No invalid characters found
*/
SELECT edition 
FROM bronze.olympic_athlete_event_details
WHERE edition !~ '^[a-zA-Z0-9 ]+$';

-- Ensure Trim is not needed
SELECT edition, TRIM(edition)
FROM bronze.olympic_athlete_event_details
WHERE TRIM(edition) != edition;

/* 
Validate 'edition_id' format
- Expectation: Should contain only digits
- Result: No invalid characters found
*/
SELECT edition_id 
FROM bronze.olympic_athlete_event_details
WHERE edition_id::text !~ '^[0-9]+$';

-- Validate 'country_noc' codes
-- Check for length not equal to 3 (ISO 3166-1 alpha-3 standard)
SELECT length(country_noc)
FROM bronze.olympic_athlete_event_details
WHERE length(country_noc) != 3;

-- Check for non-alphabetic characters in 'country_noc' codes
SELECT country_noc
FROM bronze.olympic_athlete_event_details
WHERE country_noc !~ '^[a-zA-Z]+$';

-- Validate 'sport' values
-- Note: Only acceptable numeric entry is '3x3 Basketball'
SELECT sport 
FROM bronze.olympic_athlete_event_details
WHERE LOWER(sport) ~ '^[0-9 ]';

-- Decompose 'event' into 'final_event' and 'gender_category'
-- Assumption: Last word in event denotes gender category (e.g., "100m Men")
SELECT 
	event, 
	(STRING_TO_ARRAY(event, ' '))[array_length(STRING_TO_ARRAY(event, ' '), 1)] AS gender_category,
	ARRAY_TO_STRING((STRING_TO_ARRAY(event, ' '))[:array_length(STRING_TO_ARRAY(event, ' '), 1)-1], ' ') AS final_event
FROM bronze.olympic_athlete_event_details;

-- Count the distribution of distinct gender categories inferred from event names
SELECT 
  DISTINCT((STRING_TO_ARRAY(event, ' '))[array_length(STRING_TO_ARRAY(event, ' '), 1)]) AS gender_category,
  COUNT(*)
FROM bronze.olympic_athlete_event_details
GROUP BY (STRING_TO_ARRAY(event, ' '))[array_length(STRING_TO_ARRAY(event, ' '), 1)];

-- Validate 'result_id' values
-- Expectation: Only numeric values allowed
SELECT result_id 
FROM bronze.olympic_athlete_event_details
WHERE result_id::text ~ '^[a-zA-Z .]+$';

-- Validate 'athlete' names
-- Expectation: Should not be purely numeric
SELECT athlete
FROM bronze.olympic_athlete_event_details
WHERE athlete::text ~ '^[0-9]+$';

-- Validate 'athlete_id' values
-- Expectation: Should contain only digits
SELECT athlete_id 
FROM bronze.olympic_athlete_event_details
WHERE athlete_id::text ~ '^[a-zA-Z .]+$';

-- Normalize 'pos' values
-- Extract the number following '=' if present; otherwise retain original value
SELECT 
	pos,
	CASE
		WHEN POSITION('=' IN pos) > 0 
			THEN (STRING_TO_ARRAY(POS, '='))[2]
		ELSE pos
	END AS ditinct_pos
FROM bronze.olympic_athlete_event_details;

-- Validate 'medal' entries
-- Expectation: Should contain only lowercase alphabetic values or null
SELECT medal 
FROM bronze.olympic_athlete_event_details
WHERE LOWER(medal) !~ '^[a-z]';

-- Analyze distribution of 'isteamsport' values
SELECT DISTINCT(isteamsport), count(*)
FROM bronze.olympic_athlete_event_details
GROUP BY isteamsport;

/*
=========================================================
Final Transformation — Cleaned Olympic Event Information
=========================================================
*/
SELECT 
	edition_id,
	(STRING_TO_ARRAY(edition, ' '))[1]::numeric as edition_year, 
	ARRAY_TO_STRING((STRING_TO_ARRAY(edition, ' '))[2:3], ' ') as edition,
	country_noc,
	sport,
	ARRAY_TO_STRING((STRING_TO_ARRAY(event, ' '))[:array_length(STRING_TO_ARRAY(event, ' '), 1)-1], ' ') AS event,
	(STRING_TO_ARRAY(event, ' '))[array_length(STRING_TO_ARRAY(event, ' '), 1)] AS event_gender_category,
	result_id, 
	athlete, 
	athlete_id,
	CASE
		WHEN POSITION('=' IN pos) > 0 
			THEN (STRING_TO_ARRAY(POS, '='))[2]
		ELSE pos
	END AS pos,
	medal,
	isteamsport
FROM bronze.olympic_athlete_event_details;
