SELECT *
FROM bronze.olympic_medal_tally_history;

SELECT gold 
FROM bronze.olympic_medal_tally_history
WHERE gold::text !~ '^[0-9]+$';

SELECT silver 
FROM bronze.olympic_medal_tally_history
WHERE silver::text !~ '^[0-9]+$';

SELECT bronze 
FROM bronze.olympic_medal_tally_history
WHERE bronze::text !~ '^[0-9]+$';

SELECT total 
FROM bronze.olympic_medal_tally_history
WHERE total::text !~ '^[0-9]+$';

SELECT 
edition, 
edition_id,
year,
country,
country_noc,
gold,
silver, 
bronze, 
total
FROM bronze.olympic_medal_tally_history;
