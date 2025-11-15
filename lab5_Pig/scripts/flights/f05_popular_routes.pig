-- ============================================
-- ANALYSE 5 : Itinéraires les Plus Fréquentés
-- ============================================
-- Paires d'aéroports non ordonnées (i, j)
-- Tableau de fréquences des routes

-- Charger les données
flights = LOAD 'input/flights/sample_flights.csv' 
    USING PigStorage(',') 
    AS (
        year:int, month:int, day:int, day_of_week:int,
        dep_time:int, crs_dep_time:int, arr_time:int, crs_arr_time:int,
        carrier:chararray, flight_num:int, tail_num:chararray,
        actual_elapsed_time:int, crs_elapsed_time:int, air_time:int,
        arr_delay:int, dep_delay:int,
        origin:chararray, dest:chararray,
        distance:int, taxi_in:int, taxi_out:int,
        cancelled:int, cancellation_code:chararray, diverted:int,
        carrier_delay:int, weather_delay:int, nas_delay:int,
        security_delay:int, late_aircraft_delay:int
    );

-- Filtrer les vols valides
valid_flights = FILTER flights BY cancelled == 0 AND year IS NOT NULL;

-- ============================================
-- PARTIE 1 : Routes Non Ordonnées
-- ============================================

-- Créer des paires d'aéroports non ordonnées
-- (i, j) où i < j alphabétiquement
routes = FOREACH valid_flights GENERATE 
    (origin < dest ? origin : dest) AS airport1,
    (origin < dest ? dest : origin) AS airport2,
    origin,
    dest,
    distance;

-- Grouper par paire d'aéroports
routes_grouped = GROUP routes BY (airport1, airport2);

-- Compter le nombre de vols par route
route_frequencies = FOREACH routes_grouped GENERATE 
    FLATTEN(group) AS (airport1, airport2),
    COUNT(routes) AS flight_count,
    AVG(routes.distance) AS avg_distance;

-- Trier par fréquence décroissante
sorted_routes = ORDER route_frequencies BY flight_count DESC;

-- Top 20 itinéraires
top20_routes = LIMIT sorted_routes 20;

-- Afficher
DUMP top20_routes;

-- Sauvegarder
STORE top20_routes INTO 'pigout/flights/popular_routes' USING PigStorage(',');

-- ============================================
-- PARTIE 2 : Routes Ordonnées (Origine → Destination)
-- ============================================

-- Routes directionnelles
directional_routes = FOREACH valid_flights GENERATE 
    origin,
    dest,
    distance;

-- Grouper
dir_routes_grouped = GROUP directional_routes BY (origin, dest);

-- Compter
dir_route_frequencies = FOREACH dir_routes_grouped GENERATE 
    FLATTEN(group) AS (origin, dest),
    COUNT(directional_routes) AS flight_count,
    AVG(directional_routes.distance) AS avg_distance;

-- Trier
sorted_dir_routes = ORDER dir_route_frequencies BY flight_count DESC;

-- Top 20 routes directionnelles
top20_dir_routes = LIMIT sorted_dir_routes 20;

-- Afficher et sauvegarder
DUMP top20_dir_routes;
STORE top20_dir_routes INTO 'pigout/flights/popular_directional_routes' USING PigStorage(',');

-- ============================================
-- PARTIE 3 : Routes par Transporteur
-- ============================================

-- Routes avec transporteur
carrier_routes = FOREACH valid_flights GENERATE 
    carrier,
    origin,
    dest;

-- Grouper par (transporteur, origine, destination)
carrier_routes_grouped = GROUP carrier_routes BY (carrier, origin, dest);

-- Compter
carrier_route_freq = FOREACH carrier_routes_grouped GENERATE 
    FLATTEN(group) AS (carrier, origin, dest),
    COUNT(carrier_routes) AS flight_count;

-- Trier
sorted_carrier_routes = ORDER carrier_route_freq BY flight_count DESC;

-- Top 20 routes par transporteur
top20_carrier_routes = LIMIT sorted_carrier_routes 20;

-- Afficher et sauvegarder
DUMP top20_carrier_routes;
STORE top20_carrier_routes INTO 'pigout/flights/popular_carrier_routes' USING PigStorage(',');

-- ============================================
-- PARTIE 4 : Routes les Plus Longues
-- ============================================

-- Routes avec distance
routes_with_distance = FOREACH valid_flights GENERATE 
    origin,
    dest,
    distance;

-- Dédupliquer
routes_unique = DISTINCT routes_with_distance;

-- Trier par distance
sorted_by_distance = ORDER routes_unique BY distance DESC;

-- Top 20 routes les plus longues
top20_longest = LIMIT sorted_by_distance 20;

-- Afficher et sauvegarder
DUMP top20_longest;
STORE top20_longest INTO 'pigout/flights/longest_routes' USING PigStorage(',');
