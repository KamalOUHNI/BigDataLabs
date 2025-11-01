-- ============================================
-- Script: Cleanup.hql
-- Description: Nettoyage et suppression des données
-- ============================================

USE hotel_booking;

-- 9. NETTOYAGE ET SUPPRESSION DES DONNÉES

-- Supprimer les données des tables
TRUNCATE TABLE clients;
TRUNCATE TABLE hotels;
TRUNCATE TABLE reservations;
TRUNCATE TABLE hotels_partitioned;
TRUNCATE TABLE reservations_bucketed;
TRUNCATE TABLE reservations_temp;

-- Supprimer les tables
DROP TABLE IF EXISTS clients;
DROP TABLE IF EXISTS hotels;
DROP TABLE IF EXISTS reservations;
DROP TABLE IF EXISTS hotels_partitioned;
DROP TABLE IF EXISTS reservations_bucketed;
DROP TABLE IF EXISTS reservations_temp;

-- Supprimer la base de données
DROP DATABASE IF EXISTS hotel_booking CASCADE;

-- Vérifier la suppression
SHOW DATABASES;
