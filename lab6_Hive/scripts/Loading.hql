-- ============================================
-- Script: Loading.hql
-- Description: Chargement des données dans les tables Hive
-- ============================================

USE hotel_booking;

-- Configuration pour les partitions dynamiques
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=20000;
SET hive.exec.max.dynamic.partitions.pernode=20000;
SET hive.enforce.bucketing=true;

-- 1. Charger les données dans la table clients
LOAD DATA LOCAL INPATH '/shared_volume/hive/data/clients.txt' 
OVERWRITE INTO TABLE clients;

-- 2. Charger les données dans la table hotels
LOAD DATA LOCAL INPATH '/shared_volume/hive/data/hotels.txt' 
OVERWRITE INTO TABLE hotels;

-- 3. Créer une table temporaire pour charger les réservations
CREATE TABLE IF NOT EXISTS reservations_temp (
    reservation_id INT,
    client_id INT,
    hotel_id INT,
    date_debut DATE,
    date_fin DATE,
    prix_total DECIMAL(10,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 4. Charger les données dans la table temporaire
LOAD DATA LOCAL INPATH '/shared_volume/hive/data/reservations.txt' 
OVERWRITE INTO TABLE reservations_temp;

-- 5. Insérer les données dans la table reservations avec partition dynamique
INSERT OVERWRITE TABLE reservations PARTITION(date_debut)
SELECT reservation_id, client_id, hotel_id, date_fin, prix_total, date_debut
FROM reservations_temp;

-- 6. Charger les données dans hotels_partitioned
INSERT OVERWRITE TABLE hotels_partitioned PARTITION(ville)
SELECT hotel_id, nom, etoiles, ville
FROM hotels;

-- 7. Charger les données dans reservations_bucketed
INSERT OVERWRITE TABLE reservations_bucketed
SELECT reservation_id, client_id, hotel_id, date_debut, date_fin, prix_total
FROM reservations_temp;

-- Vérifier le chargement
SELECT COUNT(*) as nb_clients FROM clients;
SELECT COUNT(*) as nb_hotels FROM hotels;
SELECT COUNT(*) as nb_reservations FROM reservations;
SELECT COUNT(*) as nb_hotels_partitioned FROM hotels_partitioned;
SELECT COUNT(*) as nb_reservations_bucketed FROM reservations_bucketed;
