-- ============================================
-- REQUÊTE 2 : Films américains groupés par réalisateur
-- ============================================

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

-- Grouper par réalisateur
mUSA_director = GROUP films_usa BY director.id;

-- Formater le résultat
result_director = FOREACH mUSA_director GENERATE 
    group AS id_director,
    COUNT(films_usa) AS nb_films,
    films_usa.(title, year) AS films;

-- Trier par nombre de films décroissant
sorted_director = ORDER result_director BY nb_films DESC;

-- Afficher
DUMP sorted_director;

-- Sauvegarder
STORE sorted_director INTO 'pigout/movies/mUSA_director' USING PigStorage('|');
