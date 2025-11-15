-- ============================================
-- WORDCOUNT - COMPTAGE DE MOTS
-- ============================================
-- Description : Analyse d'un fichier texte pour compter
--               le nombre d'occurrences de chaque mot

-- Charger le fichier texte
lines = LOAD '/shared_volume/alice.txt' AS (line:chararray);

-- Découper chaque ligne en mots
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) AS word;

-- Nettoyer les données (garder uniquement les mots alphanumériques)
clean_words = FILTER words BY word MATCHES '\\w+';

-- Grouper par mot
grouped_words = GROUP clean_words BY word;

-- Compter les occurrences de chaque mot
word_count = FOREACH grouped_words GENERATE 
    group AS word, 
    COUNT(clean_words) AS count;

-- Trier par fréquence décroissante
sorted_word_count = ORDER word_count BY count DESC;

-- Sauvegarder les résultats
STORE sorted_word_count INTO '/shared_volume/pig_out/WORD_COUNT/';

-- Afficher les 20 mots les plus fréquents
top_words = LIMIT sorted_word_count 20;
DUMP top_words;
