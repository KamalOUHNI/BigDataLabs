-- ============================================
-- Script: Creation.hql
-- Description: Création de la base de données et des tables pour le système de réservation d'hôtels
-- ============================================

-- 1. Créer et utiliser la base de données
CREATE DATABASE IF NOT EXISTS hotel_booking;
USE hotel_booking;

-- 2. Configuration pour les partitions et les buckets
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=20000;
SET hive.exec.max.dynamic.partitions.pernode=20000;
SET hive.enforce.bucketing=true;

-- 3. Créer la table clients
CREATE TABLE IF NOT EXISTS clients (
    client_id INT,
    nom STRING,
    email STRING,
    telephone STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 4. Créer la table hotels
CREATE TABLE IF NOT EXISTS hotels (
    hotel_id INT,
    nom STRING,
    etoiles INT,
    ville STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 5. Créer la table reservations avec partition par date_debut
CREATE TABLE IF NOT EXISTS reservations (
    reservation_id INT,
    client_id INT,
    hotel_id INT,
    date_fin DATE,
    prix_total DECIMAL(10,2)
)
PARTITIONED BY (date_debut DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 6. Créer la table hotels_partitioned (partitionnée par ville)
CREATE TABLE IF NOT EXISTS hotels_partitioned (
    hotel_id INT,
    nom STRING,
    etoiles INT
)
PARTITIONED BY (ville STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 7. Créer la table reservations_bucketed (avec buckets par client_id)
CREATE TABLE IF NOT EXISTS reservations_bucketed (
    reservation_id INT,
    client_id INT,
    hotel_id INT,
    date_debut DATE,
    date_fin DATE,
    prix_total DECIMAL(10,2)
)
CLUSTERED BY (client_id) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Afficher les tables créées
SHOW TABLES;
