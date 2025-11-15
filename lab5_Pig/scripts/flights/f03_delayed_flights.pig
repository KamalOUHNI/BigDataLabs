-- ============================================
-- ANALYSE 3 : Proportion de Vols Retardés
-- ============================================
-- Un vol est retardé si arr_delay > 15 minutes
-- Calcul à différentes granularités temporelles

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
-- Ajouter le Statut de Retard
-- ============================================

-- Déterminer si retardé (> 15 minutes)
flights_with_delay_status = FOREACH valid_flights GENERATE 
    year,
    month,
    day,
    day_of_week,
    arr_delay,
    dep_delay,
    (arr_delay IS NOT NULL AND arr_delay > 15 ? 1 : 0) AS is_delayed;

-- ============================================
-- GRANULARITÉ 1 : Par Année
-- ============================================

by_year = GROUP flights_with_delay_status BY year;

delay_by_year = FOREACH by_year GENERATE 
    group AS year,
    COUNT(flights_with_delay_status) AS total_flights,
    SUM(flights_with_delay_status.is_delayed) AS delayed_flights,
    (double)SUM(flights_with_delay_status.is_delayed) / (double)COUNT(flights_with_delay_status) AS delay_proportion;

-- Trier
sorted_delay_year = ORDER delay_by_year BY year;

-- Afficher et sauvegarder
DUMP sorted_delay_year;
STORE sorted_delay_year INTO 'pigout/flights/delays_by_year' USING PigStorage(',');

-- ============================================
-- GRANULARITÉ 2 : Par Mois
-- ============================================

by_month = GROUP flights_with_delay_status BY (year, month);

delay_by_month = FOREACH by_month GENERATE 
    FLATTEN(group) AS (year, month),
    COUNT(flights_with_delay_status) AS total_flights,
    SUM(flights_with_delay_status.is_delayed) AS delayed_flights,
    (double)SUM(flights_with_delay_status.is_delayed) / (double)COUNT(flights_with_delay_status) AS delay_proportion;

-- Trier
sorted_delay_month = ORDER delay_by_month BY year, month;

-- Sauvegarder
STORE sorted_delay_month INTO 'pigout/flights/delays_by_month' USING PigStorage(',');

-- ============================================
-- GRANULARITÉ 3 : Par Jour de la Semaine
-- ============================================

by_day_of_week = GROUP flights_with_delay_status BY day_of_week;

delay_by_dow = FOREACH by_day_of_week GENERATE 
    group AS day_of_week,
    COUNT(flights_with_delay_status) AS total_flights,
    SUM(flights_with_delay_status.is_delayed) AS delayed_flights,
    (double)SUM(flights_with_delay_status.is_delayed) / (double)COUNT(flights_with_delay_status) AS delay_proportion;

-- Trier
sorted_delay_dow = ORDER delay_by_dow BY day_of_week;

-- Afficher et sauvegarder
DUMP sorted_delay_dow;
STORE sorted_delay_dow INTO 'pigout/flights/delays_by_day_of_week' USING PigStorage(',');

-- ============================================
-- GRANULARITÉ 4 : Par Heure du Jour
-- ============================================

-- Extraire l'heure de départ
flights_with_hour = FOREACH valid_flights GENERATE 
    (dep_time IS NOT NULL ? (int)(dep_time / 100) : -1) AS dep_hour,
    (arr_delay IS NOT NULL AND arr_delay > 15 ? 1 : 0) AS is_delayed;

-- Filtrer les heures valides
valid_hours = FILTER flights_with_hour BY dep_hour >= 0 AND dep_hour < 24;

-- Grouper par heure
by_hour = GROUP valid_hours BY dep_hour;

delay_by_hour = FOREACH by_hour GENERATE 
    group AS hour,
    COUNT(valid_hours) AS total_flights,
    SUM(valid_hours.is_delayed) AS delayed_flights,
    (double)SUM(valid_hours.is_delayed) / (double)COUNT(valid_hours) AS delay_proportion;

-- Trier
sorted_delay_hour = ORDER delay_by_hour BY hour;

-- Afficher et sauvegarder
DUMP sorted_delay_hour;
STORE sorted_delay_hour INTO 'pigout/flights/delays_by_hour' USING PigStorage(',');
