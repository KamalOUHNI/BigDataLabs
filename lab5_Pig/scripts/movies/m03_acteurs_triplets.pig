-- ============================================
-- REQUÊTE 3 : Triplets (idFilm, idActeur, role)
-- ============================================
-- Aplatissement de la collection actors imbriquée

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

-- Filtrer les films américains
films_usa = FILTER films_raw BY country == 'US';

-- Projection : film_id, title et actors
films_with_actors = FOREACH films_usa GENERATE 
    _id AS film_id,
    title AS film_title,
    actors AS actors_list;

-- FLATTEN : aplatir la collection actors
-- Chaque acteur devient une ligne séparée
mUSA_acteurs = FOREACH films_with_actors GENERATE 
    film_id,
    film_title,
    FLATTEN(actors_list) AS (actor_id:chararray, role:chararray);

-- Créer les triplets (idFilm, idActeur, role)
acteurs_triplets = FOREACH mUSA_acteurs GENERATE 
    film_id,
    actor_id,
    role;

-- Trier par film_id
sorted_acteurs = ORDER acteurs_triplets BY film_id;

-- Afficher
DUMP sorted_acteurs;

-- Sauvegarder
STORE sorted_acteurs INTO 'pigout/movies/mUSA_acteurs' USING PigStorage(',');
