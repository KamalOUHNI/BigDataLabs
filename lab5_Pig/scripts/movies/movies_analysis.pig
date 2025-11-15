-- ============================================
-- ANALYSE DES FILMS AVEC APACHE PIG
-- ============================================
-- Description : Analyse complète des films et artistes
--               Format JSON - Dataset cinéma

-- ============================================
-- CHARGEMENT DES DONNÉES JSON
-- ============================================

-- Charger les films (format JSON)
-- Structure : _id, title, year, genre, summary, country, director, actors
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

-- Charger les artistes (format JSON)
-- Structure : _id, last_name, first_name, birth_date
artists_raw = LOAD 'input/movies/artists.json'
    USING JsonLoader('
        _id:chararray,
        last_name:chararray,
        first_name:chararray,
        birth_date:chararray
    ');

-- Afficher quelques films pour vérification
films_sample = LIMIT films_raw 3;
DUMP films_sample;

-- Afficher quelques artistes pour vérification
artists_sample = LIMIT artists_raw 3;
DUMP artists_sample;

-- ============================================
-- REQUÊTE 1 : Films américains groupés par année
-- ============================================
-- Filtrer les films américains (country = "US")
films_usa = FILTER films_raw BY country == 'US';

-- Grouper par année
mUSA_annee = GROUP films_usa BY year;

-- Formater le résultat : (année, {liste des films})
result_annee = FOREACH mUSA_annee GENERATE 
    group AS annee,
    COUNT(films_usa) AS nb_films,
    films_usa.(title, genre) AS films;

-- Trier par année
sorted_annee = ORDER result_annee BY annee;

-- Afficher les résultats
DUMP sorted_annee;

-- Sauvegarder
STORE sorted_annee INTO 'pigout/movies/mUSA_annee' USING PigStorage('|');


-- ============================================
-- REQUÊTE 2 : Films américains groupés par réalisateur
-- ============================================

-- Grouper par réalisateur
mUSA_director = GROUP films_usa BY director.id;

-- Formater le résultat : (id_director, {liste des films})
result_director = FOREACH mUSA_director GENERATE 
    group AS id_director,
    COUNT(films_usa) AS nb_films,
    films_usa.(title, year) AS films;

-- Afficher les résultats
DUMP result_director;

-- Sauvegarder
STORE result_director INTO 'pigout/movies/mUSA_director' USING PigStorage('|');


-- ============================================
-- REQUÊTE 3 : Films américains - Triplets (idFilm, idActeur, role)
-- ============================================
-- Aplatir la collection actors imbriquée

-- Projection : film_id et actors
films_with_actors = FOREACH films_usa GENERATE 
    _id AS film_id,
    title AS film_title,
    actors AS actors_list;

-- Aplatir (FLATTEN) la collection actors
-- Chaque acteur devient une ligne séparée
mUSA_acteurs = FOREACH films_with_actors GENERATE 
    film_id,
    film_title,
    FLATTEN(actors_list) AS (actor_id:chararray, role:chararray);

-- Sélectionner uniquement les colonnes demandées
acteurs_triplets = FOREACH mUSA_acteurs GENERATE 
    film_id,
    actor_id,
    role;

-- Afficher les résultats
DUMP acteurs_triplets;

-- Sauvegarder
STORE acteurs_triplets INTO 'pigout/movies/mUSA_acteurs' USING PigStorage(',');


-- ============================================
-- REQUÊTE 4 : Association film → description complète acteur
-- ============================================
-- Jointure entre mUSA_acteurs et artists

-- Renommer pour éviter les conflits
acteurs_for_join = FOREACH mUSA_acteurs GENERATE 
    film_id,
    film_title,
    actor_id,
    role;

-- Jointure sur actor_id = artist._id
moviesActors = JOIN acteurs_for_join BY actor_id, artists_raw BY _id;

-- Formater : (film_id, film_title, actor_info, role)
moviesActors_formatted = FOREACH moviesActors GENERATE 
    acteurs_for_join::film_id AS film_id,
    acteurs_for_join::film_title AS film_title,
    artists_raw::_id AS actor_id,
    artists_raw::first_name AS actor_first_name,
    artists_raw::last_name AS actor_last_name,
    artists_raw::birth_date AS actor_birth_date,
    acteurs_for_join::role AS role;

-- Afficher les résultats
DUMP moviesActors_formatted;

-- Sauvegarder
STORE moviesActors_formatted INTO 'pigout/movies/moviesActors' USING PigStorage(',');


-- ============================================
-- REQUÊTE 5 : Description complète film + tous les acteurs
-- ============================================
-- Méthode : COGROUP entre films_usa et moviesActors

-- Préparer les données pour le COGROUP
films_for_cogroup = FOREACH films_usa GENERATE 
    _id AS film_id,
    title,
    year,
    genre,
    country,
    director.id AS director_id;

-- COGROUP : regroupe films et acteurs par film_id
fullMovies_cogroup = COGROUP films_for_cogroup BY film_id, 
                              moviesActors_formatted BY film_id;

-- Formater le résultat
fullMovies = FOREACH fullMovies_cogroup GENERATE 
    group AS film_id,
    FLATTEN(films_for_cogroup.(title, year, genre)) AS (title, year, genre),
    moviesActors_formatted.(actor_first_name, actor_last_name, role) AS actors;

-- Afficher les résultats (limité)
fullMovies_sample = LIMIT fullMovies 5;
DUMP fullMovies_sample;

-- Sauvegarder
STORE fullMovies INTO 'pigout/movies/fullMovies' USING PigStorage('|');


-- ============================================
-- REQUÊTE 6 : Acteurs/Réalisateurs - Films joués et dirigés
-- ============================================

-- Partie 1 : Films dirigés par chaque artiste
films_directed = FOREACH films_usa GENERATE 
    director.id AS artist_id,
    _id AS film_id,
    title AS film_title;

-- Grouper par artiste (réalisateur)
directors_grouped = GROUP films_directed BY artist_id;

directors_list = FOREACH directors_grouped GENERATE 
    group AS artist_id,
    films_directed.(film_id, film_title) AS films_directed;

-- Partie 2 : Films joués par chaque artiste
films_acted = FOREACH mUSA_acteurs GENERATE 
    actor_id AS artist_id,
    film_id,
    film_title,
    role;

-- Grouper par artiste (acteur)
actors_grouped = GROUP films_acted BY artist_id;

actors_list = FOREACH actors_grouped GENERATE 
    group AS artist_id,
    films_acted.(film_id, film_title, role) AS films_acted;

-- Partie 3 : COGROUP pour combiner acteurs et réalisateurs
ActeursRealisateurs = COGROUP directors_list BY artist_id, 
                               actors_list BY artist_id;

-- Formater le résultat final
ActeursRealisateurs_formatted = FOREACH ActeursRealisateurs GENERATE 
    group AS artist_id,
    FLATTEN(directors_list.films_directed) AS films_directed,
    FLATTEN(actors_list.films_acted) AS films_acted;

-- Alternative plus simple : tous les artistes avec leurs films
-- Jointure externe complète (FULL OUTER)
ActeursRealisateurs_full = COGROUP actors_list BY artist_id FULL OUTER, 
                                   directors_list BY artist_id;

ActeursRealisateurs_result = FOREACH ActeursRealisateurs_full GENERATE 
    group AS artist_id,
    (directors_list.films_directed IS NOT NULL ? 
        directors_list.films_directed : {}) AS films_directed,
    (actors_list.films_acted IS NOT NULL ? 
        actors_list.films_acted : {}) AS films_acted;

-- Afficher les résultats
DUMP ActeursRealisateurs_result;

-- Sauvegarder le résultat dans pigout/ActeursRealisateurs
STORE ActeursRealisateurs_result INTO 'pigout/movies/ActeursRealisateurs' USING PigStorage('|');


-- ============================================
-- FIN DU SCRIPT
-- ============================================
