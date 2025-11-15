-- ============================================
-- ANALYSE 4 : Retards par Transporteur
-- ============================================
-- Proportion de vols retardés par transporteur
-- À différentes granularités temporelles

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
-- Préparer les Données
-- ============================================

-- Statut de retard par transporteur
carrier_delays = FOREACH valid_flights GENERATE 
    carrier,
    year,
    month,
    day_of_week,
    (arr_delay IS NOT NULL AND arr_delay > 15 ? 1 : 0) AS is_delayed;

-- ============================================
-- GRANULARITÉ 1 : Par Transporteur (Global)
-- ============================================

carrier_grouped = GROUP carrier_delays BY carrier;

carrier_delay_total = FOREACH carrier_grouped GENERATE 
    group AS carrier,
    COUNT(carrier_delays) AS total_flights,
    SUM(carrier_delays.is_delayed) AS delayed_flights,
    (double)SUM(carrier_delays.is_delayed) / (double)COUNT(carrier_delays) AS delay_rate;

-- Trier par taux de retard décroissant
sorted_carrier_total = ORDER carrier_delay_total BY delay_rate DESC;

-- Afficher et sauvegarder
DUMP sorted_carrier_total;
STORE sorted_carrier_total INTO 'pigout/flights/carrier_delays_total' USING PigStorage(',');

-- ============================================
-- GRANULARITÉ 2 : Par Transporteur et Année
-- ============================================

carrier_year_grouped = GROUP carrier_delays BY (carrier, year);

carrier_delay_by_year = FOREACH carrier_year_grouped GENERATE 
    FLATTEN(group) AS (carrier, year),
    COUNT(carrier_delays) AS total_flights,
    SUM(carrier_delays.is_delayed) AS delayed_flights,
    (double)SUM(carrier_delays.is_delayed) / (double)COUNT(carrier_delays) AS delay_rate;

-- Trier
sorted_carrier_year = ORDER carrier_delay_by_year BY year, delay_rate DESC;

-- Sauvegarder
STORE sorted_carrier_year INTO 'pigout/flights/carrier_delays_by_year' USING PigStorage(',');

-- ============================================
-- GRANULARITÉ 3 : Par Transporteur et Mois
-- ============================================

carrier_month_grouped = GROUP carrier_delays BY (carrier, year, month);

carrier_delay_by_month = FOREACH carrier_month_grouped GENERATE 
    FLATTEN(group) AS (carrier, year, month),
    COUNT(carrier_delays) AS total_flights,
    SUM(carrier_delays.is_delayed) AS delayed_flights,
    (double)SUM(carrier_delays.is_delayed) / (double)COUNT(carrier_delays) AS delay_rate;

-- Trier
sorted_carrier_month = ORDER carrier_delay_by_month BY year, month, delay_rate DESC;

-- Sauvegarder
STORE sorted_carrier_month INTO 'pigout/flights/carrier_delays_by_month' USING PigStorage(',');

-- ============================================
-- GRANULARITÉ 4 : Par Transporteur et Jour de Semaine
-- ============================================

carrier_dow_grouped = GROUP carrier_delays BY (carrier, day_of_week);

carrier_delay_by_dow = FOREACH carrier_dow_grouped GENERATE 
    FLATTEN(group) AS (carrier, day_of_week),
    COUNT(carrier_delays) AS total_flights,
    SUM(carrier_delays.is_delayed) AS delayed_flights,
    (double)SUM(carrier_delays.is_delayed) / (double)COUNT(carrier_delays) AS delay_rate;

-- Trier
sorted_carrier_dow = ORDER carrier_delay_by_dow BY day_of_week, delay_rate DESC;

-- Sauvegarder
STORE sorted_carrier_dow INTO 'pigout/flights/carrier_delays_by_dow' USING PigStorage(',');

-- ============================================
-- Top 10 Transporteurs avec le Plus de Retards
-- ============================================

top10_worst_carriers = LIMIT sorted_carrier_total 10;

-- Afficher
DUMP top10_worst_carriers;
