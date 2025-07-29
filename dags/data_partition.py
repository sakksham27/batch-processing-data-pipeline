import os
import pandas as pd 
import numpy as np 
import shutil 
from pathlib import Path
from airflow import DAG
from airflow.decorators import task 
from airflow.providers.postgres.hooks.postgres import PostgresHook


file_path = Path("/usr/local/airflow/Data/raw")

with DAG(
    dag_id = '0.1-data-partition-dag',
    schedule= None,
    catchup= False 
) as dag: 
    @task
    def make_dir():
        staged_path = file_path.parent / 'staged'
        archieve_path = file_path.parent / 'archieve'
        if staged_path.exists() and staged_path.is_dir():
            shutil.rmtree(staged_path)
        if archieve_path.exists() and archieve_path.is_dir():
            shutil.rmtree(archieve_path)
        archieve_path.mkdir(parents= True, exist_ok= True)
        staged_path.mkdir(parents= True, exist_ok= True)
        if staged_path.exists() and staged_path.is_dir():
            return True
        else:
            return False
    @task 
    def import_file(check):
        """This task is responsible for importing all the files from the data/raw folder"""
        #file_path = Path(os.getcwd()).parent / 'data' / 'raw'
        if check:
            return list(os.listdir(file_path))
        else: 
            []
    
    @task 
    def distribute_files(list_of_files):
        """This task is responsible for distributing the correct file into its respective array structure"""
        files_with_edition_column = []
        files_without_no_year_metric = []
        for i in list_of_files:
            df = pd.read_csv(file_path / i)
            if 'edition' in list(df.columns):
                files_with_edition_column.append(i)
            else: 
                files_without_no_year_metric.append(i)
        return {
            'files_with_edition_column' : files_with_edition_column,
            'files_without_no_year_metric' : files_without_no_year_metric
        }
        
    @task             
    def handle_files_with_no_edition_column(split_content):
        files_without_no_year_metric = split_content['files_without_no_year_metric']
        for i in files_without_no_year_metric:
            counter = 0 
            target_dir = Path(os.getcwd()).parent /'airflow' / 'Data'/'staged'/f"{i[:-4]}_Files"
            target_dir_archieve = Path(os.getcwd()).parent /'airflow' / 'Data'/'archieve'/f"{i[:-4]}_archieve"
            target_dir.mkdir(parents=True, exist_ok=True)
            target_dir_archieve.mkdir(parents=True, exist_ok=True)
            df = pd.read_csv(file_path / i)
            list_of_df = np.array_split(df, 30)
            for j in range(len(list_of_df)):
                path = str(target_dir) + '/'+ str(i[:-4]) + str(counter)+'.csv'
                list_of_df[j].to_csv(path, index = False)
                counter = counter + 1
    
    @task             
    def handle_files_with_edition_column(split_content):
        files_with_edition_column = split_content['files_with_edition_column']
        for i in files_with_edition_column:
            df = pd.read_csv(file_path / i)
            df['year_new'] = df['edition'].astype(str).str[:4]   # extract first 4 chars
            df = df.sort_values(by='year_new')
            counter = 0
            target_dir = Path(os.getcwd()).parent /'airflow' / 'Data'/'staged'/f"{i[:-4]}_Files"
            target_dir_archieve = Path(os.getcwd()).parent /'airflow' / 'Data'/'archieve'/f"{i[:-4]}_archieve"
            target_dir.mkdir(parents=True, exist_ok=True)
            target_dir_archieve.mkdir(parents=True, exist_ok=True)
            for year in df['year_new'].unique():
                path = str(target_dir) + '/'+ str(i[:-4]) + str(counter)+'.csv'
                temp_df = df[df['year_new'] == year]
                temp_df = temp_df.drop(columns = "year_new")
                temp_df.to_csv(path, index=False)
                counter = counter + 1

    check = make_dir()
    files = import_file(check)
    split_content = distribute_files(files)
    handle_files_with_no_edition_column(split_content)
    handle_files_with_edition_column(split_content)