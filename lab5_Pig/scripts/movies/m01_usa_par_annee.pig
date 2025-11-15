-- ============================================
-- REQUÊTE 1 : Films américains groupés par année
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

-- Grouper par année
mUSA_annee = GROUP films_usa BY year;

-- Formater le résultat
result_annee = FOREACH mUSA_annee GENERATE 
    group AS annee,
    COUNT(films_usa) AS nb_films,
    films_usa.(title, genre) AS films;

-- Trier par année
sorted_annee = ORDER result_annee BY annee;

-- Afficher
DUMP sorted_annee;

-- Sauvegarder
STORE sorted_annee INTO 'pigout/movies/mUSA_annee' USING PigStorage('|');
