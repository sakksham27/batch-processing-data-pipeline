from airflow import DAG
from airflow.decorators import task 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path 
import pandas as pd 
import shutil
import re
import numpy as np 
import os 
from pendulum import datetime
import math 
import logging 
import ast 
import pickle

# constants
SOURCE_DIR = Path("/usr/local/airflow/Data/staged/Olympic_Games_Summary_Files")
ARCHIVE_DIR = Path("/usr/local/airflow/Data/archieve/Olympic_Games_Summary_Files_archieve")
BATCH_SIZE = 5
POSTGRES_CONN_ID= 'postgres_initial'

with DAG(
    dag_id= "1.5-olympic_game_summary_dag",
    start_date=datetime(2025, 6, 25),
    schedule="*/2 * * * *",
    catchup= False
) as dags:
    # this task gets the list of file paths to process
    @task 
    def get_files():
        if('.DS_Store' in os.listdir(SOURCE_DIR)):
            os.remove(SOURCE_DIR/'.DS_Store')
        def natural_key(filename):
            return [int(part) if part.isdigit() else part.lower() for part in re.split(r'(\d+)', filename)]

        files = sorted(os.listdir(SOURCE_DIR), key=natural_key)[:BATCH_SIZE]
        if not files:
            print(f'No files found in {SOURCE_DIR}')
            return []
        print(f'Files to process: {files}')
        return [str(SOURCE_DIR / file) for file in files]
    
    # this function parses the file contents and gets it ready for load 
    @task 
    def parse_files(file_paths):
        try: 
            # Logger Object 
            logger = logging.getLogger('parse_files_logger')
        except: 
            raise RuntimeError("Failed To Create Logger Object.")
        if not file_paths:
            print("No files to parse")
            return ""
            
        def make_list_of_tuples(file_path):
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
                
            df = pd.read_csv(file_path)
            df = df.where(pd.notnull(df), None)
            df = df.map(lambda x: None if pd.isna(x) or (isinstance(x, float) and math.isnan(x)) else x)
            return list(df.itertuples(index=False, name=None))

        total_data = []
        if not file_paths:
            logging.info("No files to parse")
            return ""
    
        for path in file_paths:
            logger.info(f"Processing file: {path}")
            total_data.extend(make_list_of_tuples(path))

        # Create a temporary file to hold the pickled data
        temp_path = Path("/usr/local/airflow/dags/temp4.pkl")

        with temp_path.open('wb') as f:  # note the 'wb' mode for writing bytes
            pickle.dump(total_data, f)

        # Return path as string
        return str(temp_path)
    
    @task
    def load_to_db(tuple_data_file_path):
        
        try: 
            # Logger Object
            logger = logging.getLogger('load_to_db_logger')
        except: 
            raise RuntimeError('Failed To Create Logger Object.')

        tuple_data_file_path = Path(tuple_data_file_path)
        if tuple_data_file_path == "":
            logger.info("No pickle file passed, skipping DB load.")
            return True

        try:
            with tuple_data_file_path.open('rb') as f:  # read binary mode
                tuple_data = pickle.load(f)
        except Exception as e:
            logger.error(f"Failed to load pickle file: {e}")
            tuple_data = []

        if not tuple_data:
            logger.info("No data to load")
            return True
            
        try:
            pg_hook = PostgresHook(postgres_conn_id='postgres_initial')
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            print(f"Loading {len(tuple_data)} records to database")
            cursor.executemany("""
                INSERT INTO bronze.Olympic_Games_Summary (
                    edition, edition_id, edition_url, year, city,
                    country_flag_url, country_noc, start_date, end_date,
                    competition_date, isHeld
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, tuple_data)

            conn.commit()
            rows_inserted = cursor.rowcount
            cursor.close()
            conn.close()
            print(f"Successfully inserted {rows_inserted} rows")
            os.remove(str(tuple_data_file_path))
            return True
        except Exception as e:
            raise RuntimeError(f"Database insertion failed: {e}")
        
    @task 
    def move_files(file_paths, load_success):
        # Only move files if database load was successful
        if not load_success:
            raise RuntimeError("Cannot move files - database load failed")
            
        if not file_paths:
            print("No files to move")
            return
            
        # Ensure archive directory exists
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        
        moved_count = 0
        for file_path in file_paths:
            if os.path.exists(file_path):
                print(f"Moving file: {file_path}")
                shutil.copy(file_path, ARCHIVE_DIR)
                os.remove(file_path)
                moved_count += 1
            else:
                print(f"Warning: File not found for moving: {file_path}")
        
        print(f"Successfully moved {moved_count} files to archive")
            
        
    # dependencies 
    file_paths = get_files()
    parsed_data = parse_files(file_paths)
    load_success = load_to_db(parsed_data)
    move_files(file_paths, load_success)  