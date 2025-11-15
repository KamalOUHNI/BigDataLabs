-- ============================================
-- ANALYSE 2 : Popularité des Transporteurs
-- ============================================
-- Volume de vols par année et transporteur
-- Échelle logarithmique (base 10)

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
-- PARTIE 1 : Volume par Année et Transporteur
-- ============================================

-- Projection : année et transporteur
carrier_year = FOREACH valid_flights GENERATE year, carrier;

-- Grouper par (année, transporteur)
carrier_grouped = GROUP carrier_year BY (year, carrier);

-- Calculer le volume et le log
carrier_volume = FOREACH carrier_grouped GENERATE 
    FLATTEN(group) AS (year, carrier),
    COUNT(carrier_year) AS flight_count,
    LOG10((double)COUNT(carrier_year)) AS log_volume;

-- Trier par année et volume
sorted_carrier_volume = ORDER carrier_volume BY year, flight_count DESC;

-- Afficher
DUMP sorted_carrier_volume;

-- Sauvegarder
STORE sorted_carrier_volume INTO 'pigout/flights/carrier_volume_by_year' USING PigStorage(',');

-- ============================================
-- PARTIE 2 : Volume Médian par Transporteur
-- ============================================

-- Extraire uniquement transporteur et log_volume
carrier_log = FOREACH carrier_volume GENERATE carrier, log_volume;

-- Grouper par transporteur
carrier_stats = GROUP carrier_log BY carrier;

-- Calculer les statistiques
carrier_median = FOREACH carrier_stats {
    sorted_vol = ORDER carrier_log BY log_volume;
    GENERATE 
        group AS carrier,
        COUNT(carrier_log) AS nb_years,
        AVG(carrier_log.log_volume) AS avg_log_volume,
        MIN(carrier_log.log_volume) AS min_log_volume,
        MAX(carrier_log.log_volume) AS max_log_volume;
}

-- Trier par volume moyen décroissant
sorted_carriers = ORDER carrier_median BY avg_log_volume DESC;

-- Afficher
DUMP sorted_carriers;

-- Sauvegarder
STORE sorted_carriers INTO 'pigout/flights/carrier_popularity' USING PigStorage(',');
