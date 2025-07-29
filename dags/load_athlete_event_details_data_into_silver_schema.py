from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id= '2.2-load_athlete_event_details_data_into_silver_schema',
    schedule= None,
    catchup= False
) as dag: 
    @task
    def load_data():
        pg_hook = PostgresHook(postgres_conn_id = 'postgres_initial')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
                       INSERT INTO silver.Olympic_Athlete_Event_Details
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
                       """)
        conn.commit()
        conn.close()
        cursor.close()
        
    load_data()