/*
=======================================================================================
Creating the tables for silver schema in Olympic_Data_Warehouse database

Note: 
--> this file we create all table structure as well as truncate the respective tables 
=======================================================================================
*/

CREATE TABLE IF NOT EXISTS silver.Olympic_Athlete_Biography(
        athlete_id INTEGER, 
        name VARCHAR(100),
        sex VARCHAR(10),
        born DATERANGE,
        height VARCHAR(20),
        weight INTEGER, 
        country VARCHAR(80),
        country_noc VARCHAR(10),
        description VARCHAR(10000),
        special_notes VARCHAR(7000)
        );
TRUNCATE TABLE silver.Olympic_Athlete_Biography;

CREATE TABLE IF NOT EXISTS silver.Olympic_Athlete_Event_Details(
        edition_id INTEGER,
        edition_year INTEGER,
        edition VARCHAR(50),
        country_noc VARCHAR(10),
        sport VARCHAR(30),
        event VARCHAR(200),
        event_gender_category VARCHAR(100),
        result_id INTEGER,
        athlete VARCHAR(60),
        athlete_id INTEGER,
        pos VARCHAR(20),
        medal VARCHAR(20),
        isTeamSport VARCHAR(10)
        );
TRUNCATE TABLE silver.Olympic_Athlete_Event_Details;

CREATE TABLE IF NOT EXISTS silver.Olympic_Country_Profiles(
        noc VARCHAR(20),
        country VARCHAR(100)
        );
TRUNCATE TABLE silver.Olympic_Country_Profiles;

CREATE TABLE IF NOT EXISTS silver.Olympic_Event_Results(
        result_id INTEGER,
        event_title VARCHAR(200),
        edition VARCHAR(100),
        edition_year INTEGER,
        edition_id INTEGER, 
        sport VARCHAR(100),
        sport_url VARCHAR(100),
        result_date DATERANGE,
        result_location VARCHAR(500),
        result_participants VARCHAR(200),
        result_format VARCHAR(2000),
        result_detail VARCHAR(500),
        result_description VARCHAR(15000)
         );
TRUNCATE TABLE silver.Olympic_Event_Results;

CREATE TABLE IF NOT EXISTS silver.Olympic_Games_Summary(
        edition VARCHAR(100),
        edition_id INTEGER, 
        edition_url VARCHAR(100),
        year INTEGER,
        city VARCHAR(100),
        country_flag_url VARCHAR(200),
        country_noc VARCHAR(20),
        start_date DATERANGE, 
        end_date DATERANGE,
        competition_date DATERANGE,
        isHeld VARCHAR(100),
        reason VARCHAR(100)
                       );
TRUNCATE TABLE silver.Olympic_Games_Summary;

CREATE TABLE IF NOT EXISTS silver.Olympic_Medal_Tally_History(
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
TRUNCATE TABLE silver.Olympic_Medal_Tally_History;

