/*
=======================================================================================
Creating the tables for bronze schema in Olympic_Data_Warehouse database

Note: 
--> this file we create all table structure as well as truncate the respective tables 
=======================================================================================
*/

-- bronze.Olympic_Athlete_Biography
CREATE TABLE IF NOT EXISTS bronze.Olympic_Athlete_Biography(
    athlete_id INTEGER, 
    name VARCHAR(100),
    sex VARCHAR(10),
    born VARCHAR(80),
    height VARCHAR(20),
    weight VARCHAR(20), 
    country VARCHAR(80),
    country_noc VARCHAR(10),
    description VARCHAR(10000),
    special_notes VARCHAR(7000)
);
TRUNCATE TABLE bronze.Olympic_Athlete_Biography;

-- bronze.Olympic_Athlete_Event_Details
CREATE TABLE IF NOT EXISTS bronze.Olympic_Athlete_Event_Details(
    edition VARCHAR(50),
    edition_id INTEGER,
    country_noc VARCHAR(10),
    sport VARCHAR(30),
    event VARCHAR(200),
    result_id INTEGER,
    athlete VARCHAR(60),
    athlete_id INTEGER,
    pos VARCHAR(20),
    medal VARCHAR(20),
    isTeamSport VARCHAR(10)
);
TRUNCATE TABLE bronze.Olympic_Athlete_Event_Details;

-- bronze.Olympic_Country_Profiles
CREATE TABLE IF NOT EXISTS bronze.Olympic_Country_Profiles(
    noc VARCHAR(20),
    country VARCHAR(100)
);
TRUNCATE TABLE bronze.Olympic_Country_Profiles;

-- bronze.Olympic_Event_Results
CREATE TABLE IF NOT EXISTS bronze.Olympic_Event_Results(
    result_id INTEGER,
    event_title VARCHAR(200),
    edition VARCHAR(100),
    edition_id INTEGER, 
    sport VARCHAR(100),
    sport_url VARCHAR(100),
    result_date VARCHAR(200),
    result_location VARCHAR(500),
    result_participants VARCHAR(200),
    result_format VARCHAR(2000),
    result_detail VARCHAR(500),
    result_description VARCHAR(15000)
);
TRUNCATE TABLE bronze.Olympic_Event_Results;

-- bronze.Olympic_Games_Summary
CREATE TABLE IF NOT EXISTS bronze.Olympic_Games_Summary(
    edition VARCHAR(100),
    edition_id INTEGER, 
    edition_url VARCHAR(100),
    year INTEGER,
    city VARCHAR(100),
    country_flag_url VARCHAR(200),
    country_noc VARCHAR(20),
    start_date VARCHAR(100), 
    end_date VARCHAR(100),
    competition_date VARCHAR(100),
    isHeld VARCHAR(100)
);
TRUNCATE TABLE bronze.Olympic_Games_Summary;

-- bronze.Olympic_Medal_Tally_History
CREATE TABLE IF NOT EXISTS bronze.Olympic_Medal_Tally_History(
    edition VARCHAR(100),
    edition_id INTEGER, 
    year INTEGER,
    country VARCHAR(100),
    country_noc VARCHAR(20),
    gold INTEGER,
    silver INTEGER,
    bronze INTEGER,
    total INTEGER
);
TRUNCATE TABLE bronze.Olympic_Medal_Tally_History;