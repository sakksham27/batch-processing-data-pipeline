from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
from pendulum import datetime 


with DAG(
    dag_id = '2.1-load_athlete_biography_data_into_silver_schema',
    schedule= None,
    catchup= False   
) as dag: 
    @task 
    def load_data():
        pg_hook = PostgresHook(postgres_conn_id = 'postgres_initial')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
                       INSERT INTO silver.Olympic_Athlete_Biography
                        SELECT athlete_id,
                            INITCAP(TRIM(name)) AS name,
                            sex,
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
                                            """)
        conn.commit()
        conn.close()
        cursor.close()
        
    load_data()