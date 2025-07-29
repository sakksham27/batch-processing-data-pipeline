''' 
==================================================
Batch Processing Athlete Biography Data
==================================================
'''

# Essential Libraries
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
import pickle
import ast


# Initializing Constant Variables 
SOURCE_DIR = Path("/usr/local/airflow/Data/staged/Olympic_Athlete_Biography_Files")
ARCHIVE_DIR = Path("/usr/local/airflow/Data/archieve/Olympic_Athlete_Biography_Files_archieve")
BATCH_SIZE = 5
POSTGRES_CONN_ID= 'postgres_initial' # Airflow connection ID


with DAG(
    dag_id= "1.1-olympic_athlete_biography_dag",
    start_date=datetime(2025, 6, 25),
    schedule="*/2 * * * *",
    catchup= False
) as dags:
    """
    Athlete Biography Dag
        - This DAG contains 4 tasks forming a linear flow to batch process Data files into Data warehouse 
        - Tasks: 
            1. get_files
            2. parse_files
            3. load_to_db
            4. move_files
    """
    
    @task 
    def get_files():
        
        """ This task retrieves the list of file path names to process. """

        try: 
            # Logger Object 
            logger = logging.getLogger('get_files_logger') 
        except: 
            raise RuntimeError("Failed To Create Logger Object.")
        
        # Handles Hidden Files Added By System
        if('.DS_Store' in os.listdir(SOURCE_DIR)):
            os.remove(SOURCE_DIR/'.DS_Store')
            
        # Function To Custom Sort Files 
        def natural_key(filename):
            return [int(part) if part.isdigit() else part.lower() for part in re.split(r'(\d+)', filename)]
        
        # List Of Sorted Files
        files = sorted(os.listdir(SOURCE_DIR), key=natural_key)[:BATCH_SIZE]
        
        if not files:
            logger.error(f'No files found in {SOURCE_DIR}')
            return []
        logger.info(f'Files to process: {files}')
        
        # Return File Paths
        return [str(SOURCE_DIR / file) for file in files]
    


    @task 
    def parse_files(file_paths):
        
        """ This task parses the file contents and gets it ready for load. """
        
        try: 
            # Logger Object 
            logger = logging.getLogger('parse_files_logger')
        except: 
            raise RuntimeError("Failed To Create Logger Object.")
        
        '''if not file_paths:
            logger.info("No files to parse")
            return []'''
        
        # Converts Dataframe Into List Of Tuples   
        def make_list_of_tuples(file_path):
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
                
            df = pd.read_csv(file_path)
            df = df.where(pd.notnull(df), None)
            # More thorough NaN handling - convert ALL NaN-like values to None
            df = df.map(lambda x: None if (pd.isna(x) or 
                                        (isinstance(x, float) and math.isnan(x)) or 
                                        str(x).lower() == 'nan') else x)
            return list(df.itertuples(index=False, name=None))

        total_data = []
        if not file_paths:
            logging.info("No files to parse")
            return ""
    
        for path in file_paths:
            logger.info(f"Processing file: {path}")
            total_data.extend(make_list_of_tuples(path))

        # Create a temporary file to hold the pickled data
        temp_path = Path("/usr/local/airflow/dags/temp0.pkl")

        with temp_path.open('wb') as f:  # note the 'wb' mode for writing bytes
            pickle.dump(total_data, f)

        # Return path as string
        return str(temp_path)

    @task
    def load_to_db(tuple_data_file_path):
        """ Load Data Into Database. """

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

        
        # Establishing Database Connection   
        try:
            pg_hook = PostgresHook(postgres_conn_id='postgres_initial')
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            logger.info(f"Loading {len(tuple_data)} records to database")
            cursor.executemany("""
                        INSERT INTO bronze.Olympic_Athlete_Biography (
                            athlete_id, name, sex, born, height,
                            weight, country, country_noc, description, special_notes
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """, tuple_data)
            conn.commit()
            rows_inserted = cursor.rowcount
            cursor.close()
            conn.close()
            logger.info(f"Successfully inserted all rows")
            # remove the temp file 
            os.remove(str(tuple_data_file_path))
            return True
        except Exception as e:
            raise RuntimeError(f"Database insertion failed: {e}")
            
    @task 
    def move_files(file_paths, load_success):
        """ Handles File Movement. """
        
        try: 
            # Logger Object
            logger = logging.getLogger('move_files_logger')
        except: 
            raise RuntimeError('Failed To Create Logger Object.')
        
        # Only move files if database load was successful
        if not load_success:
            raise RuntimeError("Cannot move files - database load failed")
            
        if not file_paths:
            logging.info("No files to move")
            return
            
        # Ensure archive directory exists
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        
        moved_count = 0
        for file_path in file_paths:
            if os.path.exists(file_path):
                logging.info(f"Moving file: {file_path}")
                shutil.copy(file_path, ARCHIVE_DIR)
                os.remove(file_path)
                moved_count += 1
            else:
                logging.info(f"Warning: File not found for moving: {file_path}")
        
        logging.info(f"Successfully moved {moved_count} files to archive")
            
        
    # dependencies 
    file_paths = get_files()
    parsed_data = parse_files(file_paths)
    load_success = load_to_db(parsed_data)
    move_files(file_paths, load_success)  