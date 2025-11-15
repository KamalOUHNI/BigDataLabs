-- ============================================
-- REQUÊTE 4 : Association film → description complète acteur
-- ============================================
-- Jointure entre films et artistes

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

-- Aplatir la collection actors
films_with_actors = FOREACH films_usa GENERATE 
    _id AS film_id,
    title AS film_title,
    year,
    actors AS actors_list;

mUSA_acteurs = FOREACH films_with_actors GENERATE 
    film_id,
    film_title,
    year,
    FLATTEN(actors_list) AS (actor_id:chararray, role:chararray);

-- DESCRIBE mUSA_acteurs pour voir les noms de colonnes
DESCRIBE mUSA_acteurs;

-- Jointure : mUSA_acteurs × artists
moviesActors = JOIN mUSA_acteurs BY actor_id, artists_raw BY _id;

-- Formater le résultat : film + acteur complet
moviesActors_formatted = FOREACH moviesActors GENERATE 
    mUSA_acteurs::film_id AS film_id,
    mUSA_acteurs::film_title AS film_title,
    mUSA_acteurs::year AS year,
    artists_raw::_id AS actor_id,
    artists_raw::first_name AS actor_first_name,
    artists_raw::last_name AS actor_last_name,
    artists_raw::birth_date AS actor_birth_date,
    mUSA_acteurs::role AS role;

-- Trier par film_id
sorted_movies_actors = ORDER moviesActors_formatted BY film_id;

-- Afficher
DUMP sorted_movies_actors;

-- Sauvegarder
STORE sorted_movies_actors INTO 'pigout/movies/moviesActors' USING PigStorage(',');
