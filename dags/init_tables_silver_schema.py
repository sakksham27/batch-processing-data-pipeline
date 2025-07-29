''' 
==================================================
Create tables in Silver Schema 
==================================================
'''

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
from pendulum import datetime 
import os 

with DAG(
    dag_id = '0.3-init_tables_silver_schema',
    start_date=datetime(2025, 1, 1),  
    schedule=None,                    
    catchup=False
) as dag: 
    file_path = Path("/usr/local/airflow/sql_scripts/init-tables-structure/init-tables-silver.sql")
    
    @task
    def init_tables_silver_schema():
        pg_hook = PostgresHook(postgres_conn_id = 'postgres_initial')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        sql = file_path.read_text()
        cursor.execute(sql)
        conn.commit()
        
        cursor.close()
        conn.close()
        
    init_tables_silver_schema()