from airflow import DAG
from airflow.decorators import task 
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id= '2.3-load_country_profile_data_into_silver_schema',
    schedule= None,
    catchup= False 
) as dag:
    @task
    def load_data():
        pg_hook = PostgresHook(postgres_conn_id = 'postgres_initial')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
                       INSERT INTO silver.Olympic_Country_Profiles
                       SELECT 
                        noc,
                        country
                       FROM bronze.olympic_country_profiles;""")
        
        conn.commit()
        conn.close()
        cursor.close()
        
    load_data()