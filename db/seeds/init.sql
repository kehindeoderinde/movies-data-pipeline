BEGIN

CREATE TABLE IF NOT EXISTS movies (
    imdb_id varchar(50) PRIMARY KEY
    name varchar(225) NOT NULL,
    plot varchar(225),
    release_date date NOT NULL,
    runtime integer,
    start_year date NOT NULL,
    end_year date,
    movie_rating varchar(75) REFERENCES pg_ratings(name)
    -- is_on_dvd boolean,
    -- movie_rating varchar
);

CREATE TABLE IF NOT EXISTS genres (
    name varchar(75),
    key varchar(75) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id varchar(50) REFERENCES movies(imdb_id),
    genre_key varchar(75) REFERENCES genres(key),
    PRIMARY KEY(movie_id, genre_key)
);

CREATE TABLE IF NOT EXISTS writers (
    id serial PRIMARY KEY,
    full_name varchar(100)
);

CREATE TABLE IF NOT EXISTS movie_writers (
    movie_id varchar(50) REFERENCES movies(imdb_id),
    writer_id serial REFERENCES writers(id),
    PRIMARY KEY(movie_id, writer_id)
);

CREATE TABLE IF NOT EXISTS directors (
    id serial PRIMARY KEY,
    full_name varchar(100)
);

CREATE TABLE IF NOT EXISTS movie_directors (
    movie_id varchar(50) REFERENCES movies(imdb_id),
    director_id serial REFERENCES directors(id),
    PRIMARY KEY(movie_id, director_id)
);

CREATE TABLE IF NOT EXISTS actors (
    id serial PRIMARY KEY,
    full_name varchar(100)
);

CREATE TABLE IF NOT EXISTS movie_actors (
    movie_id varchar(50) REFERENCES movies(imdb_id),
    actor_id serial REFERENCES actors(id),
    PRIMARY KEY(movie_id, actor_id)
);

CREATE TABLE IF NOT EXISTS languages (
    id serial PRIMARY KEY,
    name varchar(100)
);

CREATE TABLE IF NOT EXISTS movie_languages (
    movie_id varchar(50) REFERENCES movies(imdb_id),
    language_id serial REFERENCES languages(id),
    PRIMARY KEY(movie_id, language_id)
);

CREATE TABLE IF NOT EXISTS pg_ratings(
    name varchar(75) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS movie_pg_ratings(
    movie_id varchar(50) REFERENCES movies(imdb_id),
    pg_rating varchar(75) REFERENCES pg_ratings(name),
    PRIMARY KEY(movie_id, pg_rating)
);

CREATE TABLE IF NOT EXISTS countries (
    id serial PRIMARY KEY,
    name varchar(100)
);

CREATE TABLE IF NOT EXISTS movie_countries (
    movie_id varchar(50) REFERENCES movies(imdb_id),
    country_id serial REFERENCES countries(id),
    PRIMARY KEY(movie_id, country_id)
);

COMMIT;