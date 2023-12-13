'''EXTRACTION MODULE'''

import os
import math
import multiprocessing
import requests
import pandas as pd
from dotenv import load_dotenv

# Load environment variables into application
load_dotenv()

API_URL = os.getenv('API_URL')
API_KEY = os.getenv('API_KEY')
SEARCH_STRING = os.getenv('SEARCH_STRING')

def get_movie_ids_from_search (search_string: str) -> pd.DataFrame | str :
    '''Function to extract all ids from movies retrieved from serach results'''
    
    if len(SEARCH_STRING) < 3 :
        return 'Cannot fetch results if search string is less than 3'
    
    else :
        ids = []
        r = requests.get(f'{API_URL}', params={'apikey': API_KEY, 's': search_string}, timeout=30)
        res = r.json()
        
        if res['Response'] == 'False':
            return res['Error']
        else:
            movie_ids = [movie['imdbID'] for movie in res['Search']]
        
            num_per_page = len(res['Search'])
            total_results = int(res['totalResults'])
            
            print(f'{total_results} results found for movie "{SEARCH_STRING}"')
            
            total_pages = math.ceil(total_results/num_per_page)
            ids.extend(movie_ids)
            
            print('Extracting unique ids from search results')
            
            if total_results % num_per_page > 1:
                page = 1
                page += 1
                while page <= total_pages:
                    r = requests.get(f'{API_URL}', params={'apikey': API_KEY, 's': search_string, 'page': page}, timeout=30)
                    res = r.json()
                    movie_ids = [movie['imdbID'] for movie in res['Search']]
                    ids.extend(movie_ids)
                    page += 1
                    # print(f'{round(len(ids)/total_results * 100, 2)}% extraction complete')
            print(f'{len(ids)} movie ids returned from search')  
            return ids
        
def fetch_movie_by_id (movie_id: int) -> pd.DataFrame:
    '''Fetch movie from URL'''
    r = requests.get(f'{API_URL}', params={'apikey': API_KEY, 'i': movie_id}, timeout=30)
    res = r.json()
    df = pd.json_normalize(res)
    return df
        
def get_full_movie_data_by_ids (ids : list | str) -> pd.DataFrame :
    '''Get full movie data using ids from API'''
    if isinstance(ids, str):
        return ids
    else:
        print('Collating movie data into dataframe for analysis')
        base_df = pd.DataFrame()
        
        with multiprocessing.Pool(4) as pool:
            movie_dfs = pool.map(fetch_movie_by_id, ids)
            
            print('Merging collated dataframes into master dataframe')
            for movie_df in movie_dfs:
                base_df = pd.concat([base_df, movie_df], ignore_index=True)
            
            print('Merge complete')       
            
        base_df = base_df.rename(columns={'imdbID': 'movie_id'})
        base_df = base_df.set_index('movie_id')
        
        return base_df

def export_dataframe_to_csv (df : pd.DataFrame) :
    '''Load extracted movie data into CSV'''
    print(f'Exporting dataframe to CSV into extracted_data/{SEARCH_STRING} folder')
    print('Checking if folder exists')
    if os.path.exists(f'extracted_data/{SEARCH_STRING}'):
        print(f'Folder extracted_data/{SEARCH_STRING} found! Exporting data')
    else:
        print('No folder found! Creating folder and exporting data')
        os.makedirs(f'extracted_data/{SEARCH_STRING}')

    df.to_csv(f'extracted_data/{SEARCH_STRING}/search_results.csv')
    print(f'Extraction complete! File at extracted_data/{SEARCH_STRING}')
