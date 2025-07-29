from airflow import DAG
from airflow.decorators import task 
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id = '2.6-load_medal_tally_history_data_into_silver_schema',
    catchup= False,
    schedule= None
) as dag: 
    @task 
    def load_data():
        pg_hook = PostgresHook(postgres_conn_id = 'postgres_initial')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
                       INSERT INTO silver.Olympic_Medal_Tally_History
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
                       FROM bronze.olympic_medal_tally_history;""")
        conn.commit()
        conn.close()
        cursor.close()
        
    load_data()