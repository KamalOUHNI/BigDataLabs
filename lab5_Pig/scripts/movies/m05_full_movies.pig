-- ============================================
-- REQUÊTE 5 : Description complète film + tous les acteurs
-- ============================================
-- Utilisation de COGROUP

-- Charger les films
films_raw = LOAD 'input/movies/films.json' 
    USING JsonLoader('
        _id:chararray,
        title:chararray,
        year:int,
        genre:chararray,
        summary:chararray,
        country:chararray,
        director:(id:chararray),
        actors:{t:(id:chararray,role:chararray)}
    ');

-- Charger les artistes
artists_raw = LOAD 'input/movies/artists.json'
    USING JsonLoader('
        _id:chararray,
        last_name:chararray,
        first_name:chararray,
        birth_date:chararray
    ');

-- Filtrer les films américains
films_usa = FILTER films_raw BY country == 'US';

-- Préparer les films pour le COGROUP
films_for_cogroup = FOREACH films_usa GENERATE 
    _id AS film_id,
    title,
    year,
    genre,
    summary,
    director.id AS director_id;

-- Aplatir les acteurs
films_with_actors = FOREACH films_usa GENERATE 
    _id AS film_id,
    FLATTEN(actors) AS (actor_id:chararray, role:chararray);

-- Jointure avec les artistes
movies_actors_join = JOIN films_with_actors BY actor_id, artists_raw BY _id;

moviesActors = FOREACH movies_actors_join GENERATE 
    films_with_actors::film_id AS film_id,
    artists_raw::_id AS actor_id,
    artists_raw::first_name AS actor_first_name,
    artists_raw::last_name AS actor_last_name,
    films_with_actors::role AS role;

-- COGROUP : regroupe films et acteurs par film_id
fullMovies_cogroup = COGROUP films_for_cogroup BY film_id, 
                              moviesActors BY film_id;

-- Formater le résultat
fullMovies = FOREACH fullMovies_cogroup GENERATE 
    group AS film_id,
    FLATTEN(films_for_cogroup.(title, year, genre, director_id)) 
        AS (title, year, genre, director_id),
    moviesActors.(actor_first_name, actor_last_name, role) AS actors;

-- Trier par année
sorted_fullMovies = ORDER fullMovies BY year;

-- Afficher (limité)
fullMovies_sample = LIMIT sorted_fullMovies 10;
DUMP fullMovies_sample;

-- Sauvegarder
STORE sorted_fullMovies INTO 'pigout/movies/fullMovies' USING PigStorage('|');
