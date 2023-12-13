"""Tranformation logic"""

import os
import re
import datetime as dt
import pandas as pd
import numpy as np

from dotenv import load_dotenv

load_dotenv()

SEARCH_STRING = os.getenv("SEARCH_STRING")

def concatenate_time_stamp(t: list) -> int:
    """Convert tour timestamp to mins"""
    total = 0
    
    for i, ts in enumerate(t):
        if "min" in ts:
            # Handle converting minutes in timestamp
            extracted_min = ts.replace(' ', '').strip("min")
            if extracted_min:
                total += int(extracted_min)
        elif "h" in ts:
            # Handle converting hour in timestamp
            extracted_h = ts.replace(' ', '').strip("h")
            if extracted_h:
                if isinstance(extracted_h, int):
                    total += ts * 60
                else:
                    print(t)
                    total += int(str(ts).strip("h")) * 60
        else:
            # Handle case for when there's mistype integer as string such as 21S instead of 215
            # This also handles case when time type 'min' or 'h' is separated from numeric value by a trailing space
            if ts.isnumeric():
                if t[i+1] == "min":
                    total += int(ts) 
                elif t[i+1] == 'h':
                    total += int(ts) * 60
            else:
                total += 0
                   
    return total


def parse_start_year(year: str):
    """Handle year transformation logic"""
    # Handle years with unicode hyphen minus instead of conventional characters
    if '\u2013' in year:
        years = year.split('\u2013')
        if years[0]:
            start_year = years[0]
            
        return start_year
    else:
        return year
    
def parse_end_year(year: str):
    """Handle year transformation logic"""
    # Handle years with unicode hyphen minus instead of conventional characters
    if '\u2013' in year:
        years = year.split('\u2013')
        if years[1]:
            end_year = years[1]
        else:
            end_year = ''
        
        return end_year
    else:
        return year
    
def parse_genres(src_df: pd.DataFrame, dest_df: pd.DataFrame) -> pd.DataFrame:
    '''Function to split genres into unique'''

    movies_ids = [*src_df.index]
    genres = []
    
    for movie_id in movies_ids:
        genres.extend(src_df.loc[movie_id, 'Genre'].split(', '))
        
    genre_keys = [*map(str.upper, list(set(genres)))]
    genre_names = list(set(genres))
    genre_local_df = pd.DataFrame({
        'key': genre_keys,
        'name': genre_names
    })
    dest_df = pd.concat([dest_df, genre_local_df], ignore_index=True)
    dest_df = dest_df.set_index('key')
    
    return dest_df

def parse_movie_genres(src_df: pd.DataFrame, dest_df: pd.DataFrame) -> pd.DataFrame:
    '''Function to create movie & genres relationship dataframe'''

    movies_ids = [*src_df.index]
    
    movie_genres_dict = {'movie_ids': [], 'genre_keys': []}
    
    for movie_id in movies_ids:
        genre_keys = [*map(str.upper, list(set(src_df.loc[movie_id, 'Genre'].split(', '))))]
        for key in genre_keys:
            movie_genres_dict['movie_ids'].append(movie_id)
            movie_genres_dict['genre_keys'].append(key)

    
    movie_genres_local_df = pd.DataFrame({
        'movie_id': movie_genres_dict['movie_ids'],
        'genre_key': movie_genres_dict['genre_keys']
    })
    dest_df = pd.concat([dest_df, movie_genres_local_df], ignore_index=True)
    dest_df = dest_df.set_index('movie_id')
    
    return dest_df

def parse_actors(src_df: pd.DataFrame, dest_df: pd.DataFrame) -> pd.DataFrame:
    '''Function to split actors into unique'''

    movies_ids = [*src_df.index]
    actors = []
    
    for movie_id in movies_ids:
        actors.extend(src_df.loc[movie_id, 'Actors'].split(', '))
        
    actors = list(set(actors))
    actors_local_df = pd.DataFrame({
        'full_name': actors
    })
    dest_df = pd.concat([dest_df, actors_local_df], ignore_index=True)
    
    return dest_df

def parse_movie_actors(src_df: pd.DataFrame, dest_df: pd.DataFrame) -> pd.DataFrame:
    '''Function to create movie & actors relationship dataframe'''

    movies_ids = [*src_df.index]
    
    movie_actors_dict = {'movie_ids': [], 'actors_name': []}
    
    for movie_id in movies_ids:
        actors_names = list(set(src_df.loc[movie_id, 'Actors'].split(', ')))
        for name in actors_names:
            movie_actors_dict['movie_ids'].append(movie_id)
            movie_actors_dict['actors_name'].append(name)

    
    movie_actors_local_df = pd.DataFrame({
        'movie_id': movie_actors_dict['movie_ids'],
        'actor_name': movie_actors_dict['actors_name']
    })
    dest_df = pd.concat([dest_df, movie_actors_local_df], ignore_index=True)
    dest_df = dest_df.set_index('movie_id')
    
    return dest_df

def parse_writers(src_df: pd.DataFrame, dest_df: pd.DataFrame) -> pd.DataFrame:
    '''Function to split writers into unique'''

    movies_ids = [*src_df.index]
    writers = []
    
    for movie_id in movies_ids:
        writers.extend(src_df.loc[movie_id, 'Writer'].split(', '))
    
    # Split names and remove brackets describing role in some name like ()
    writers = [*map(lambda x: re.sub("\(.*?\)", "", x),list(set(writers)))]
        
    writers_local_df = pd.DataFrame({
        'full_name': writers
    })
    dest_df = pd.concat([dest_df, writers_local_df], ignore_index=True)
    
    return dest_df

def parse_movie_writers(src_df: pd.DataFrame, dest_df: pd.DataFrame) -> pd.DataFrame:
    '''Function to create movie & writers relationship dataframe'''

    movies_ids = [*src_df.index]
    
    movie_writers_dict = {'movie_ids': [], 'writers_name': []}
    
    for movie_id in movies_ids:
        # Split names and remove brackets describing role in some name like ()
        writers_names = [*map(lambda x: re.sub("\(.*?\)", "", x), list(set(src_df.loc[movie_id, 'Writer'].split(', '))))]
        
        for name in writers_names:
            movie_writers_dict['movie_ids'].append(movie_id)
            movie_writers_dict['writers_name'].append(name)

    
    movie_writers_local_df = pd.DataFrame({
        'movie_id': movie_writers_dict['movie_ids'],
        'writer_name': movie_writers_dict['writers_name']
    })
    dest_df = pd.concat([dest_df, movie_writers_local_df], ignore_index=True)
    dest_df = dest_df.set_index('movie_id')
    
    return dest_df

def parse_directors(src_df: pd.DataFrame, dest_df: pd.DataFrame) -> pd.DataFrame:
    '''Function to split directors into unique'''

    movies_ids = [*src_df.index]
    directors = []
    
    for movie_id in movies_ids:
        directors.extend(src_df.loc[movie_id, 'Director'].split(', '))
    
    # Split names and remove brackets describing role in some name like ()
    directors = [*map(lambda x: re.sub("\(.*?\)", "", x),list(set(directors)))]
        
    directors_local_df = pd.DataFrame({
        'full_name': directors
    })
    dest_df = pd.concat([dest_df, directors_local_df], ignore_index=True)
    
    return dest_df

def parse_movie_directors(src_df: pd.DataFrame, dest_df: pd.DataFrame) -> pd.DataFrame:
    '''Function to create movie & directors relationship dataframe'''

    movies_ids = [*src_df.index]
    
    movie_directors_dict = {'movie_ids': [], 'directors_name': []}
    
    for movie_id in movies_ids:
        # Split names and remove brackets describing role in some name like ()
        directors_names = [*map(lambda x: re.sub("\(.*?\)", "", x), list(set(src_df.loc[movie_id, 'Director'].split(', '))))]
        
        for name in directors_names:
            movie_directors_dict['movie_ids'].append(movie_id)
            movie_directors_dict['directors_name'].append(name)

    
    movie_directors_local_df = pd.DataFrame({
        'movie_id': movie_directors_dict['movie_ids'],
        'director_name': movie_directors_dict['directors_name']
    })
    dest_df = pd.concat([dest_df, movie_directors_local_df], ignore_index=True)
    dest_df = dest_df.set_index('movie_id')
    
    return dest_df


def transform_csv_extract():
    """Transform our extracted data from its dataframe format
    :params: None
    :return: None
    """
    print("Starting cleaning & transformation process for extracted data in csv")

    movies_df = pd.read_csv(f"extracted_data/{SEARCH_STRING}/search_results.csv", index_col=0)

    # Transform release date column
    movies_df["Released"] = pd.to_datetime(
        movies_df["Released"], errors="coerce", dayfirst=True, format="%d %b %Y"
    ).dt.strftime("%Y-%m-%d")
    movies_df["Released"] = movies_df["Released"].astype("datetime64[ns]")

    # Transform runtime column
    movies_df['Runtime'] = movies_df['Runtime'].astype(str)
    movies_df.loc[movies_df["Runtime"].astype(str).notna(), 'Runtime'] = movies_df["Runtime"].str.split(' ').apply(concatenate_time_stamp)
    movies_df.loc[movies_df["Runtime"] == 0, 'Runtime'] = np.nan

    # Transform Rating column
    movies_df.loc[movies_df["Rated"] == "Unrated", "Rated"] = "Not Rated"
    movies_df.loc[movies_df["Rated"].isnull(), "Rated"] = "Not Rated"
    movies_df["Rated"] = movies_df["Rated"].astype("category")

    # Tranform Type column
    movies_df["Type"] = movies_df["Type"].astype("category")

    # Transform DVD column
    movies_df["DVD"] = pd.to_datetime(movies_df["DVD"], dayfirst=True, format="%d %b %Y").dt.strftime(
        "%Y-%m-%d"
    )
    movies_df["DVD"] = movies_df["DVD"].astype("datetime64[ns]")

    # Tranform BoxOffice column
    movies_df["BoxOffice"] = movies_df["BoxOffice"].str.strip("$")
    movies_df["BoxOffice"] = movies_df["BoxOffice"].str.replace(",", "")
    
    
    # Tranform Year column
    movies_df['StartYear']= movies_df['Year'].astype(str).apply(parse_start_year)
    movies_df['EndYear']= movies_df['Year'].astype(str).apply(parse_end_year)
    movies_df['StartYear'] = pd.to_datetime(movies_df['StartYear'], format='%Y', yearfirst=True, errors='coerce').dt.strftime('%Y')
    movies_df['EndYear'] = pd.to_datetime(movies_df['EndYear'], format='%Y', yearfirst=True, errors='coerce').dt.strftime('%Y')
    
    # Transform totalSeasons column
    movies_df.loc[movies_df['totalSeasons'].isnull(), 'totalSeasons'] = 0
    movies_df['totalSeasons'] = movies_df['totalSeasons'].apply(int)
    
    # Transform Genres column
    genres_df = pd.DataFrame()
    movie_genres_df = pd.DataFrame()

    # Extract genres into own dataframe
    genres_df = parse_genres(movies_df.loc[movies_df['Genre'].notnull(), ['Genre']].astype(str), genres_df)
    
    # Extract movie genres relationships into own dataframe
    movie_genres_df = parse_movie_genres(movies_df.loc[movies_df['Genre'].notnull(), ['Genre']].astype(str), movie_genres_df)
    
    # Transform Actors column
    actors_df = pd.DataFrame()
    movie_actors_df = pd.DataFrame()
    
    # Extract actors into own dataframe
    actors_df = parse_actors(movies_df.loc[movies_df['Actors'].notnull(), ['Actors']].astype(str), actors_df)
    
    # Extract movie actors relationships into own dataframe
    movie_actors_df = parse_movie_actors(movies_df.loc[movies_df['Actors'].notnull(), ['Actors']].astype(str), movie_actors_df)
    
    # Transform Writers column
    writers_df = pd.DataFrame()
    movie_writers_df = pd.DataFrame()
    
    # Extract writers into own dataframe
    writers_df = parse_writers(movies_df.loc[movies_df['Writer'].notnull(), ['Writer']].astype(str), writers_df)
    
    # Extract movie writers relationships into own dataframe
    movie_writers_df = parse_movie_writers(movies_df.loc[movies_df['Writer'].notnull(), ['Writer']].astype(str), movie_writers_df)
    
    #  Transform Directors column
    directors_df = pd.DataFrame()
    movie_directors_df = pd.DataFrame()
    
    # Extract directors into own dataframe
    directors_df = parse_directors(movies_df.loc[movies_df['Director'].notnull(), ['Director']].astype(str), directors_df)
    
    # Extract movie directors relationships into own dataframe
    movie_directors_df = parse_movie_directors(movies_df.loc[movies_df['Director'].notnull(), ['Director']].astype(str), movie_directors_df)
    
    
    print(movies_df.count())
    
    # Drop redundant columns
    movies_df = movies_df.drop(columns=['Year', 'Response', 'Website', 'Production', 'Genre', 'Actors', 'Writer', 'Director'])

    print(movies_df.dtypes)
    print(movies_df)
    
    # print(genres_df)
    # print(movie_genres_df)
    # print(actors_df)
    # print(movie_actors_df)
    # print(writers_df)
    # print(movie_writers_df)
    # print(directors_df)
    # print(movie_directors_df)
    