from airflow import DAG
from airflow.decorators import task 
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id='2.5-load_games_summary_data_into_silver_schema',
    catchup= False,
    schedule= None
) as dag:
    @task 
    def load_data():
        pg_hook = PostgresHook(postgres_conn_id = 'postgres_initial')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
                       INSERT INTO silver.Olympic_Games_Summary
                       WITH normalized_competition_date AS (
                        SELECT 
                        edition,
                        (STRING_TO_ARRAY(edition, ' '))[1]::text as edition_year,
                        edition_id,
                        edition_url,
                        year,
                        city,
                        country_flag_url,
                        country_noc,
                        start_date,
                        end_date, 
                        isheld,
                            REPLACE(
                            REPLACE(
                                REPLACE(competition_date, '–', '-'),  
                                '—', '-'                         
                            ),
                            '−', '-'
                            ) AS competition_date
                        FROM bronze.olympic_games_summary
                        )
                        SELECT
                        edition,
                        edition_id, 
                        edition_url,
                        year,
                        city,
                        country_flag_url,
                        country_noc,
                        daterange(TO_DATE(
                        (STRING_TO_ARRAY(TRIM(start_date), ' '))[1] || ' ' ||
                        (STRING_TO_ARRAY(TRIM(start_date), ' '))[2] ||' ' || 
                        year, 'DD Month YYYY'),
                        TO_DATE(
                        (STRING_TO_ARRAY(TRIM(start_date), ' '))[1] || ' ' ||
                        (STRING_TO_ARRAY(TRIM(start_date), ' '))[2] ||' ' || 
                        year, 'DD Month YYYY'),'[]') AS start_date,
                        daterange(TO_DATE((STRING_TO_ARRAY(TRIM(end_date), ' '))[1] || ' ' ||
                        (STRING_TO_ARRAY(TRIM(end_date), ' '))[2] ||' ' || 
                        year, 'DD Month YYYY'),
                        TO_DATE((STRING_TO_ARRAY(TRIM(end_date), ' '))[1] || ' ' ||
                        (STRING_TO_ARRAY(TRIM(end_date), ' '))[2] ||' ' || 
                        year, 'DD Month YYYY'), '[]') AS end_date,
                        CASE 
                            WHEN competition_date = '-' THEN NULL
                            WHEN LENGTH((STRING_TO_ARRAY(competition_date, '-'))[1]) > 4 THEN 
                                CASE 
                                    WHEN 
                                        CARDINALITY(STRING_TO_ARRAY(competition_date, ' ')) = 5 OR
                                        CARDINALITY(STRING_TO_ARRAY(competition_date, ' ')) = 6 THEN 
                                            daterange(TO_DATE((STRING_TO_ARRAY(competition_date, '-'))[1] || year , 'DD Month YYYY'),
                                                TO_DATE((STRING_TO_ARRAY(competition_date, '-'))[2] || ' '|| year , 'DD Month YYYY'), '[]')
                                    WHEN CARDINALITY(STRING_TO_ARRAY(competition_date, ' ')) = 7 THEN 
                                        daterange(TO_DATE((STRING_TO_ARRAY(competition_date, '-'))[1] || (STRING_TO_ARRAY(competition_date, ' '))[CARDINALITY(STRING_TO_ARRAY(competition_date, ' '))] , 'DD Month YYYY'),
                                TO_DATE((STRING_TO_ARRAY(competition_date, '-'))[2] || ' '|| year , 'DD Month YYYY'), '[]')
                                END
                            ELSE
                                daterange(TO_DATE((STRING_TO_ARRAY(competition_date, '-'))[1] ||
                            (STRING_TO_ARRAY(competition_date, ' '))[CARDINALITY(STRING_TO_ARRAY(competition_date, ' '))] ||' '|| 
                            edition_year, 'DD Month YYYY'),
                            TO_DATE((STRING_TO_ARRAY(competition_date, ' '))[3] ||
                            (STRING_TO_ARRAY(competition_date, ' '))[CARDINALITY(STRING_TO_ARRAY(competition_date, ' '))] ||' '|| 
                            edition_year, 'DD Month YYYY'), '[]')
                        END AS competition_date,
                        CASE 
                            WHEN isheld IS NULL THEN TRUE
                            ELSE FALSE
                        END AS isheld,
                        isheld AS reason
                        FROM normalized_competition_date;
                       """)
        conn.commit()
        conn.close()
        cursor.close()
    load_data()