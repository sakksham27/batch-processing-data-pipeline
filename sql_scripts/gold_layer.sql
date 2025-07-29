-- Reference queries to inspect Silver Layer tables (for data exploration)

SELECT * FROM silver.olympic_athlete_biography;
SELECT * FROM silver.olympic_athlete_event_details;
SELECT * FROM silver.olympic_event_results;
SELECT * FROM silver.olympic_games_summary;

-- Country with the highest total medal count in each Olympic edition
-- Supports leaderboard visuals and comparative analysis by year
WITH ranked_data AS (
  SELECT *,
         RANK() OVER(PARTITION BY year ORDER BY total DESC) AS rank
  FROM silver.olympic_medal_tally_history
)
SELECT
  edition,
  year,
  country,
  total
FROM ranked_data 
WHERE rank = 1;

-- Year-wise performance of a specific country based on total medals
-- Intended for trend analysis and dynamic filtering by country
SELECT *
FROM silver.olympic_medal_tally_history
WHERE country = 'United States'
ORDER BY year;

-- Top 3 athletes with the highest number of gold medals across all editions
-- Enables leaderboard and individual achievement dashboards
SELECT 
  athlete,
  sport,
  country_noc,
  COUNT(*) AS number_of_golds
FROM silver.olympic_athlete_event_details
WHERE medal = 'Gold'
GROUP BY athlete, sport, country_noc
ORDER BY COUNT(*) DESC
LIMIT 3;

-- Distribution of total medals awarded by sport and Olympic edition
-- Useful for identifying dominant sports across different years
SELECT
  edition,
  edition_year,
  sport,
  COUNT(*) AS total_medals
FROM silver.olympic_athlete_event_details
WHERE medal IS NOT NULL
GROUP BY edition, edition_year, sport
ORDER BY total_medals DESC, sport, edition_year;

-- Athlete-level medal summary: gold, silver, bronze, and total medals
-- Supports athlete profile pages and comparative analysis
SELECT
  athlete,
  country_noc,
  sport,
  COUNT(*) FILTER (WHERE medal = 'Gold') AS gold_medals,
  COUNT(*) FILTER (WHERE medal = 'Silver') AS silver_medals,
  COUNT(*) FILTER (WHERE medal = 'Bronze') AS bronze_medals,
  COUNT(*) AS total_medals
FROM silver.olympic_athlete_event_details
WHERE medal IS NOT NULL
GROUP BY athlete, country_noc, sport
ORDER BY total_medals DESC;
