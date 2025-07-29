''' 
==================================================
Create tables in Bronze Schema 
==================================================
'''

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
from pathlib import Path 
from pendulum import datetime 

with DAG(
    dag_id = '0.2-init_tables_bronze_schema',
    start_date= datetime(2025,1,1),
    schedule= None,
    catchup= False
) as dag: 
    file_path = Path("/usr/local/airflow/sql_scripts/init-tables-structure/init-tables-bronze.sql")
    
    @task
    def init_tables_bronze_schema():
        pg_hook = PostgresHook(postgres_conn_id = 'postgres_initial')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        sql = file_path.read_text()
        cursor.execute(sql)
        conn.commit()
        
        cursor.close()
        conn.close()
        
    init_tables_bronze_schema()
    