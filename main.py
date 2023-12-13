'''APP INIT'''

import os
from multiprocessing import Process
from multiprocessing import freeze_support
from dotenv import load_dotenv
import psycopg2 as postgres
from src.extract_movies import (
    get_movie_ids_from_search,
    get_full_movie_data_by_ids,
    export_dataframe_to_csv,
)

from src.transform_movies import transform_csv_extract

# Load environment variables into application
load_dotenv()
SEARCH_STRING = os.getenv("SEARCH_STRING")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def connect_db ():
    '''Initiate connection with Postgres database'''
    try:
        conn = postgres.connect(database=DB_NAME, host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD)
        print(f'Connection to DB {DB_NAME} successfully')
        return conn
    except postgres.OperationalError:
        print('Connection to DB failed')
        os.abort()

def init():
    '''Initialize application'''
    
    # Extract movies data from API and load CSV to local/storage account
    movie_ids = get_movie_ids_from_search(SEARCH_STRING)
    movies_df = get_full_movie_data_by_ids(movie_ids)
    export_dataframe_to_csv(movies_df)

    # Transform and clean dataset loading CSV from local/storage account
    transform_csv_extract()
    
    # Initiliaze database connection and load data into local/cloud DB
    c = connect_db()
    if c:
        print('Can pass db connection')
    

# protect the entry point
if __name__ == '__main__':
    # add freeze support for multiprocessing
    freeze_support()
    # configure the child process
    child = Process(target=init)
    # start the child process
    child.start()
    # wait for the child process to terminate
    child.join()
