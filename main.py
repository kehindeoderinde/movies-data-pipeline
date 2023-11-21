'''APP INIT'''

import os
from dotenv import load_dotenv
from movies.extract_movies import (
    get_movie_ids_from_search,
    get_full_movie_data_by_ids,
    export_dataframe_to_csv,
)

# Load environment variables into application
load_dotenv()

SEARCH_STRING = os.getenv("SEARCH_STRING")

export_dataframe_to_csv(
    get_full_movie_data_by_ids(get_movie_ids_from_search(SEARCH_STRING))
)
