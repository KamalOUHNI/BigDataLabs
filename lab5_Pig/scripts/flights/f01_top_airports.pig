-- ============================================
-- ANALYSE 1 : Top 20 Aéroports par Volume
-- ============================================
-- Calcul du volume total de vols (entrants + sortants)

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

-- Filtrer les vols non annulés et enlever l'en-tête
valid_flights = FILTER flights BY cancelled == 0 AND year IS NOT NULL;

-- ============================================
-- PARTIE 1 : Vols Sortants (Départs)
-- ============================================

departures = FOREACH valid_flights GENERATE origin AS airport;
dep_grouped = GROUP departures BY airport;
dep_counts = FOREACH dep_grouped GENERATE 
    group AS airport,
    COUNT(departures) AS dep_count;

-- ============================================
-- PARTIE 2 : Vols Entrants (Arrivées)
-- ============================================

arrivals = FOREACH valid_flights GENERATE dest AS airport;
arr_grouped = GROUP arrivals BY airport;
arr_counts = FOREACH arr_grouped GENERATE 
    group AS airport,
    COUNT(arrivals) AS arr_count;

-- ============================================
-- PARTIE 3 : Combiner et Calculer le Total
-- ============================================

-- Jointure externe complète (FULL OUTER)
airport_traffic = JOIN dep_counts BY airport FULL OUTER, arr_counts BY airport;

-- Calculer le volume total
airport_volumes = FOREACH airport_traffic GENERATE 
    (dep_counts::airport IS NOT NULL ? dep_counts::airport : arr_counts::airport) AS airport,
    (dep_counts::dep_count IS NOT NULL ? dep_counts::dep_count : 0L) AS departures,
    (arr_counts::arr_count IS NOT NULL ? arr_counts::arr_count : 0L) AS arrivals,
    ((dep_counts::dep_count IS NOT NULL ? dep_counts::dep_count : 0L) + 
     (arr_counts::arr_count IS NOT NULL ? arr_counts::arr_count : 0L)) AS total_flights;

-- Trier par volume total décroissant
sorted_airports = ORDER airport_volumes BY total_flights DESC;

-- Prendre le top 20
top20_airports = LIMIT sorted_airports 20;

-- Afficher les résultats
DUMP top20_airports;

-- Sauvegarder
STORE top20_airports INTO 'pigout/flights/top20_airports' USING PigStorage(',');

-- ============================================
-- VARIATION : Par Année
-- ============================================

-- Vols par aéroport et année
flights_by_year_origin = FOREACH valid_flights GENERATE year, origin AS airport;
flights_by_year_dest = FOREACH valid_flights GENERATE year, dest AS airport;

-- Combiner origine et destination
all_flights_by_year = UNION flights_by_year_origin, flights_by_year_dest;

-- Grouper par année et aéroport
grouped_by_year_airport = GROUP all_flights_by_year BY (year, airport);

-- Compter
volume_by_year = FOREACH grouped_by_year_airport GENERATE 
    FLATTEN(group) AS (year, airport),
    COUNT(all_flights_by_year) AS flight_count;

-- Trier
sorted_by_year = ORDER volume_by_year BY year, flight_count DESC;

-- Sauvegarder
STORE sorted_by_year INTO 'pigout/flights/airports_by_year' USING PigStorage(',');
