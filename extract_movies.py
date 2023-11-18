'''Extraction script'''

import os
import math
import requests
import pandas as pd


API_URL = 'http://www.omdbapi.com/'
API_KEY = '58cf03e2'
SEARCH_STRING = 'avengers'

def get_movie_ids_by_search (s: str) -> list | str :
    """Function to search movies iteratively from OMDB API"""
    imdb_ids = []
    
    if len(s) < 3:
        return 'Cannot fetch results if search string is less than 3'
    else:
        r = requests.get(API_URL, params={'apikey': API_KEY, 's': s}, timeout=30)
        res = r.json()
        if res['Response'] == 'True':
            ids = [movie['imdbID'] for movie in res['Search']]
            imdb_ids.extend(ids)
            page = 1
            num_of_pages = math.ceil(int(res['totalResults'])/len(res['Search']))
            
            if num_of_pages > 1:
                page += 1
                while page <= num_of_pages :
                    new_r  = requests.get(API_URL, params={'apikey': API_KEY, 's': s, 'page': page}, timeout=30)
                    new_res = new_r.json()
                    ids = [movie['imdbID'] for movie in new_res['Search']]
                    imdb_ids.extend(ids)
                    page += 1
                    
            return imdb_ids
        else:
            return res['Error']

def get_extended_data (movie_ids: list | str) -> pd.DataFrame:
    """Function to fetch extended data for searched movies from OMDB API"""
    if isinstance(movie_ids, str):
        return movie_ids
    else:
        movies_df = pd.DataFrame()
    
        for movie_id in movie_ids:
            r = requests.get(API_URL, params={'apikey': API_KEY, 'i': movie_id}, timeout=300)
            res = r.json()
            temp_df = pd.json_normalize(res)
            movies_df = pd.concat([movies_df, temp_df], ignore_index=0)
        
        movies_df = movies_df.set_index('imdbID')
        
        return movies_df

def save_results_dataframe_to_csv (df: pd.DataFrame | str):
    if isinstance(df, str):
        print(df)
    else:
        path = f'extracted_data/{SEARCH_STRING}'
        if os.path.exists(path=path):
            print(f'Folder path {path} to save search results already exists')
        else:
            os.makedirs(path)
            print(f'Folder path {path} created')
            
        df.to_csv(f'{path}/search_results.csv')
        print(f'Dataframe with search results for {SEARCH_STRING} exported to CSV file at {path}')

save_results_dataframe_to_csv(get_extended_data(get_movie_ids_by_search(SEARCH_STRING)))

