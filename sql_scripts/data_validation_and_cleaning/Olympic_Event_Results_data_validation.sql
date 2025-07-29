/*
======================================================
Olympic Event Results — Data Quality Checks
======================================================
*/

WITH normalized_result_date AS (
  SELECT 
  result_id,
  event_title,
  ARRAY_TO_STRING((STRING_TO_ARRAY(edition, ' '))[2:3], ' ') as edition,
  (STRING_TO_ARRAY(edition, ' '))[1]::text as edition_year,
  edition_id,
  sport,
  sport_url,
  result_location,
  result_participants,
  result_format,
  result_detail,
  result_description,
    REPLACE(
      REPLACE(
        REPLACE(result_date, '–', '-'),  
        '—', '-'                         
      ),
      '−', '-'
    ) AS result_date
  FROM bronze.olympic_event_results
)
SELECT 
result_id,
event_title,
edition,
edition_year, 
edition_id,
sport,
sport_url,
CASE
	WHEN TRIM(result_date) ~ '^[a-zA-z0-9 ]+$' THEN 
		DATERANGE(TO_DATE(result_date, 'DD Month YYYY'),TO_DATE(result_date, 'DD Month YYYY'), '[]')
	WHEN result_date ~ '^\d{1,2} * - *\d{1,2} [A-Za-z]+ \d{4}$' THEN 
		CASE 
			WHEN CARDINALITY(STRING_TO_ARRAY(result_date, ' ')) = 5 THEN 
				daterange(TO_DATE(((STRING_TO_ARRAY(result_date, ' '))[1] || ' ' || 
				(STRING_TO_ARRAY(result_date, ' '))[4] || ' ' ||
				(STRING_TO_ARRAY(result_date, ' '))[5]), 'DD Month YYYY'),
				TO_DATE(((STRING_TO_ARRAY(result_date, ' '))[3] || ' ' || 
				(STRING_TO_ARRAY(result_date, ' '))[4] || ' ' ||
				(STRING_TO_ARRAY(result_date, ' '))[5]), 'DD Month YYYY'),
				'[]')
			WHEN CARDINALITY(STRING_TO_ARRAY(result_date, ' ')) = 6 THEN
				daterange(TO_DATE(((STRING_TO_ARRAY(result_date, ' '))[1] || ' ' || 
				(STRING_TO_ARRAY(result_date, ' '))[5] || ' ' ||
				(STRING_TO_ARRAY(result_date, ' '))[6]), 'DD Month YYYY'),
				TO_DATE(((STRING_TO_ARRAY(result_date, ' '))[4] || ' ' || 
				(STRING_TO_ARRAY(result_date, ' '))[5] || ' ' ||
				(STRING_TO_ARRAY(result_date, ' '))[6]), 'DD Month YYYY'),
				'[]')
		END
	ELSE 
		CASE 
			WHEN CARDINALITY(STRING_TO_ARRAY(result_date, '-')) = 9 OR CARDINALITY(STRING_TO_ARRAY(result_date, '-')) = 7 THEN 
				daterange(TO_DATE(((STRING_TO_ARRAY(result_date, ' '))[1] || ' ' || 
					(STRING_TO_ARRAY(result_date, ' '))[4] || ' ' ||
					(STRING_TO_ARRAY(result_date, ' '))[5]), 'DD Month YYYY'),
					TO_DATE(((STRING_TO_ARRAY(result_date, ' '))[3] || ' ' || 
					(STRING_TO_ARRAY(result_date, ' '))[4] || ' ' ||
					(STRING_TO_ARRAY(result_date, ' '))[5]), 'DD Month YYYY'),
					'[]')
			WHEN CARDINALITY(STRING_TO_ARRAY(result_date, '-')) = 5 THEN
				CASE 
					WHEN length(((STRING_TO_ARRAY(result_date, '-'))[:2])[1]) > 5 THEN 
					daterange(TO_DATE((STRING_TO_ARRAY(result_date, '-'))[1], 'DD Month YYY'),
							  TO_DATE((STRING_TO_ARRAY(result_date, '-'))[1], 'DD Month YYY'), 
							  '[]')
					ELSE daterange(TO_DATE(((STRING_TO_ARRAY(result_date, ' '))[1] || ' ' || 
						(STRING_TO_ARRAY(result_date, ' '))[4] || ' ' ||
						(STRING_TO_ARRAY(result_date, ' '))[5]), 'DD Month YYYY'),
						TO_DATE(((STRING_TO_ARRAY(result_date, ' '))[3] || ' ' || 
						(STRING_TO_ARRAY(result_date, ' '))[4] || ' ' ||
						(STRING_TO_ARRAY(result_date, ' '))[5]), 'DD Month YYYY'),
						'[]')
				END
			WHEN CARDINALITY(STRING_TO_ARRAY(result_date, '-')) = 6 THEN 
				CASE WHEN 
					LENGTH((STRING_TO_ARRAY(TRIM(result_date), '-'))[1]) < 5 
					AND 
					((STRING_TO_ARRAY(result_date, ' '))[6]) = '-' THEN 
							daterange(TO_DATE(((STRING_TO_ARRAY(result_date, ' '))[1] || ' ' || 
						(STRING_TO_ARRAY(result_date, ' '))[4] || ' ' ||
						(STRING_TO_ARRAY(result_date, ' '))[5]), 'DD Month YYYY'),
						TO_DATE(((STRING_TO_ARRAY(result_date, ' '))[3] || ' ' || 
						(STRING_TO_ARRAY(result_date, ' '))[4] || ' ' ||
						(STRING_TO_ARRAY(result_date, ' '))[5]), 'DD Month YYYY'),
						'[]')
					WHEN
					LENGTH((STRING_TO_ARRAY(TRIM(result_date), '-'))[1]) < 5 
					AND 
					((STRING_TO_ARRAY(result_date, ' '))[6]) != '-' THEN
						daterange(TO_DATE(((STRING_TO_ARRAY(result_date, ' '))[1] || ' ' || 
						(STRING_TO_ARRAY(result_date, ' '))[5] || ' ' ||
						(STRING_TO_ARRAY(result_date, ' '))[6]), 'DD Month YYYY'),
						TO_DATE(((STRING_TO_ARRAY(result_date, ' '))[4] || ' ' || 
						(STRING_TO_ARRAY(result_date, ' '))[5] || ' ' ||
						(STRING_TO_ARRAY(result_date, ' '))[6]), 'DD Month YYYY'),
						'[]')
					ELSE 
						daterange(TO_DATE(((STRING_TO_ARRAY(TRIM(result_date), '-'))[1] || (STRING_TO_ARRAY(result_date, ' '))[7]), 'DD Month YYYY'),
				TO_DATE((STRING_TO_ARRAY(TRIM(result_date), '-'))[2], 'DD Month YYYY'), 
				'[]')
				END
			WHEN CARDINALITY(STRING_TO_ARRAY(result_date, '-')) = 4 THEN 
				CASE WHEN 
					LENGTH((STRING_TO_ARRAY(result_date, '-'))[1]) > 5 AND 
					(STRING_TO_ARRAY(result_date, '-'))[1] !~ '\d{4} $' THEN 
						daterange(TO_DATE(((STRING_TO_ARRAY(result_date, '-'))[1] || (STRING_TO_ARRAY(result_date, ' '))[7]),'DD Month YYYY'),
							TO_DATE((STRING_TO_ARRAY(result_date, '-'))[2], 'DD Month YYYY'),'[]')
					WHEN 
					LENGTH((STRING_TO_ARRAY(result_date, '-'))[1]) > 5 AND 
					(STRING_TO_ARRAY(result_date, '-'))[1] ~ '\d{4} $' THEN
						daterange(TO_DATE((STRING_TO_ARRAY(result_date, '-'))[1],'DD Month YYYY'),
								  TO_DATE((STRING_TO_ARRAY(result_date, '-'))[1],'DD Month YYYY'),
								  '[]')
					WHEN LENGTH((STRING_TO_ARRAY(result_date, '-'))[1]) < 5 AND 
						(STRING_TO_ARRAY(result_date, ' '))[6] = '-' THEN 
						daterange(TO_DATE((STRING_TO_ARRAY(result_date, '-'))[1] || (STRING_TO_ARRAY(result_date, ' '))[4] || (STRING_TO_ARRAY(result_date, ' '))[5], 'DD Month YYYY'),
						TO_DATE((STRING_TO_ARRAY(result_date, '-'))[2], 'DD Month YYYY') ,'[]')
					WHEN LENGTH((STRING_TO_ARRAY(result_date, '-'))[1]) < 5 AND 
						(STRING_TO_ARRAY(result_date, ' '))[6] != '-' THEN 
							daterange(TO_DATE((STRING_TO_ARRAY(result_date, '-'))[1] || (STRING_TO_ARRAY(result_date, ' '))[5] || (STRING_TO_ARRAY(result_date, ' '))[6], 'DD Month YYYY'),
						TO_DATE((STRING_TO_ARRAY(result_date, '-'))[2], 'DD Month YYYY') ,'[]')
				END
			WHEN CARDINALITY(STRING_TO_ARRAY(result_date, '-')) = 3 THEN 
				CASE 
					-- special case with no pattern, had to be hardcoded
					WHEN result_date = '29 July - 11 August 1984 - 11:00' or result_date = '30 July - 11 August 1984 - 11:00' or result_date = '31 July - 11 August 1984 - 11:00' THEN
					daterange(TO_DATE(((STRING_TO_ARRAY(result_date, '-'))[1] || (STRING_TO_ARRAY(result_date, ' '))[6]),'DD Month YYYY'),
							TO_DATE((STRING_TO_ARRAY(result_date, '-'))[2], 'DD Month YYYY'),'[]')
					WHEN 
					LENGTH((STRING_TO_ARRAY(result_date, '-'))[1]) > 5 AND 
					(STRING_TO_ARRAY(result_date, '-'))[1] !~ '\d{4} $' THEN 
						daterange(TO_DATE(((STRING_TO_ARRAY(result_date, '-'))[1] || (STRING_TO_ARRAY(result_date, ' '))[7]),'DD Month YYYY'),
							TO_DATE((STRING_TO_ARRAY(result_date, '-'))[2], 'DD Month YYYY'),'[]')
					WHEN 
					LENGTH((STRING_TO_ARRAY(result_date, '-'))[1]) > 5 AND 
					(STRING_TO_ARRAY(result_date, '-'))[1] ~ '\d{4} $' THEN
						daterange(TO_DATE((STRING_TO_ARRAY(result_date, '-'))[1],'DD Month YYYY'),
								  TO_DATE((STRING_TO_ARRAY(result_date, '-'))[1],'DD Month YYYY'),
								  '[]')
					WHEN LENGTH((STRING_TO_ARRAY(result_date, '-'))[1]) < 5 AND 
						(STRING_TO_ARRAY(result_date, ' '))[6] = '-' THEN 
						daterange(TO_DATE((STRING_TO_ARRAY(result_date, '-'))[1] || (STRING_TO_ARRAY(result_date, ' '))[4] || (STRING_TO_ARRAY(result_date, ' '))[5], 'DD Month YYYY'),
						TO_DATE((STRING_TO_ARRAY(result_date, '-'))[2], 'DD Month YYYY') ,'[]')
					WHEN LENGTH((STRING_TO_ARRAY(result_date, '-'))[1]) < 5 AND 
						(STRING_TO_ARRAY(result_date, ' '))[6] != '-' THEN 
							daterange(TO_DATE((STRING_TO_ARRAY(result_date, '-'))[1] || (STRING_TO_ARRAY(result_date, ' '))[5] || (STRING_TO_ARRAY(result_date, ' '))[6], 'DD Month YYYY'),
						TO_DATE((STRING_TO_ARRAY(result_date, '-'))[2], 'DD Month YYYY') ,'[]')
				END
			WHEN CARDINALITY(STRING_TO_ARRAY(result_date, '-')) = 2 THEN 
				CASE WHEN (STRING_TO_ARRAY(result_date, '-'))[1] !~ '\d{4} $' AND 
					(STRING_TO_ARRAY(result_date, ' '))[1] != '-' THEN
						daterange(TO_DATE(((STRING_TO_ARRAY(result_date, '-'))[1] ||(STRING_TO_ARRAY(result_date, ' '))[array_length(STRING_TO_ARRAY(result_date, ' '), 1)]),'DD Month YYYY'),
						TO_DATE((STRING_TO_ARRAY(result_date, '-'))[2],'DD Month YYYY'), '[]')
						WHEN (STRING_TO_ARRAY(result_date, '-'))[1] ~ '\d{4} $' THEN 
						daterange(TO_DATE((STRING_TO_ARRAY(result_date, '-'))[1], 'DD Month YYYY'),
						TO_DATE((STRING_TO_ARRAY(result_date, '-'))[1], 'DD Month YYYY'),'[]')
						WHEN (STRING_TO_ARRAY(result_date, ' '))[1] = '-' THEN
							NULLIF(result_date, '-')::daterange
				END
		END
END AS result_date,
result_location,
result_participants,
result_format,
result_detail,
result_description
FROM normalized_result_date;
