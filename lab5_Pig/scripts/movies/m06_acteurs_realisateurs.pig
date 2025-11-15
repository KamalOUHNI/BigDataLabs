-- ============================================
-- REQUÊTE 6 : Acteurs/Réalisateurs
-- ============================================
-- Pour chaque artiste : films joués ET films dirigés

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

-- ============================================
-- PARTIE 1 : Films dirigés par chaque artiste
-- ============================================

films_directed = FOREACH films_usa GENERATE 
    director.id AS artist_id,
    _id AS film_id,
    title AS film_title;

-- Grouper par artiste (réalisateur)
directors_grouped = GROUP films_directed BY artist_id;

directors_list = FOREACH directors_grouped GENERATE 
    group AS artist_id,
    films_directed.(film_id, film_title) AS films_directed;

-- ============================================
-- PARTIE 2 : Films joués par chaque artiste
-- ============================================

-- Aplatir les acteurs
films_acted_flat = FOREACH films_usa GENERATE 
    _id AS film_id,
    title AS film_title,
    FLATTEN(actors) AS (actor_id:chararray, role:chararray);

-- Sélectionner les colonnes
films_acted = FOREACH films_acted_flat GENERATE 
    actor_id AS artist_id,
    film_id,
    film_title,
    role;

-- Grouper par artiste (acteur)
actors_grouped = GROUP films_acted BY artist_id;

actors_list = FOREACH actors_grouped GENERATE 
    group AS artist_id,
    films_acted.(film_id, film_title, role) AS films_acted;

-- ============================================
-- PARTIE 3 : COGROUP pour combiner
-- ============================================

-- COGROUP avec FULL OUTER pour avoir tous les artistes
ActeursRealisateurs = COGROUP actors_list BY artist_id FULL OUTER, 
                               directors_list BY artist_id;

-- Formater le résultat
-- Format : (artist_id, {films dirigés}, {films joués avec rôle})
ActeursRealisateurs_result = FOREACH ActeursRealisateurs GENERATE 
    group AS artist_id,
    FLATTEN(
        (directors_list.films_directed IS NOT NULL AND 
         SIZE(directors_list.films_directed) > 0) ? 
            directors_list.films_directed : 
            {TOBAG(TOTUPLE('', ''))}
    ) AS films_directed,
    FLATTEN(
        (actors_list.films_acted IS NOT NULL AND 
         SIZE(actors_list.films_acted) > 0) ? 
            actors_list.films_acted : 
            {TOBAG(TOTUPLE('', '', ''))}
    ) AS films_acted;

-- Version simplifiée du résultat
ActeursRealisateurs_simple = FOREACH ActeursRealisateurs GENERATE 
    group AS artist_id,
    (directors_list IS NULL OR SIZE(directors_list) == 0 ? 
        TOBAG(TOTUPLE()) : directors_list.films_directed) AS films_directed,
    (actors_list IS NULL OR SIZE(actors_list) == 0 ? 
        TOBAG(TOTUPLE()) : actors_list.films_acted) AS films_acted;

-- Afficher les résultats
DUMP ActeursRealisateurs_simple;

-- Sauvegarder dans pigout/ActeursRealisateurs
STORE ActeursRealisateurs_simple INTO 'pigout/movies/ActeursRealisateurs' USING PigStorage('|');

-- ============================================
-- Exemples de résultats attendus :
-- ============================================
-- (artist:24,{},{(movie:10,Blade Runner,artist:24,Deckard)})
-- (artist:1,{(movie:1,Vertigo),(movie:2,Psycho)},{})
-- (artist:15,{},{(movie:8,Star Wars,Han Solo),(movie:9,Empire,Han Solo)})
