-- ============================================
-- Script: Queries.hql
-- Description: Requêtes d'analyse des données de réservation d'hôtels
-- ============================================

USE hotel_booking;

-- ============================================
-- 5. REQUÊTES SIMPLES
-- ============================================

-- Lister tous les clients
SELECT * FROM clients;

-- Lister tous les hôtels à Paris
SELECT * FROM hotels WHERE ville = 'Paris';

-- Lister toutes les réservations avec les informations sur les hôtels et les clients
SELECT 
    r.reservation_id,
    c.nom AS nom_client,
    c.email,
    h.nom AS nom_hotel,
    h.ville,
    h.etoiles,
    r.date_debut,
    r.date_fin,
    r.prix_total
FROM reservations r
JOIN clients c ON r.client_id = c.client_id
JOIN hotels h ON r.hotel_id = h.hotel_id;

-- ============================================
-- 6. REQUÊTES AVEC JOINTURES
-- ============================================

-- Afficher le nombre de réservations par client
SELECT 
    c.client_id,
    c.nom,
    COUNT(r.reservation_id) AS nombre_reservations
FROM clients c
LEFT JOIN reservations r ON c.client_id = r.client_id
GROUP BY c.client_id, c.nom
ORDER BY nombre_reservations DESC;

-- Afficher les clients qui ont réservé plus de 2 nuitées
SELECT 
    c.client_id,
    c.nom,
    r.reservation_id,
    DATEDIFF(r.date_fin, r.date_debut) AS nombre_nuitees
FROM clients c
JOIN reservations r ON c.client_id = r.client_id
WHERE DATEDIFF(r.date_fin, r.date_debut) > 2
ORDER BY nombre_nuitees DESC;

-- Afficher les hôtels réservés par chaque client
SELECT 
    c.nom AS nom_client,
    h.nom AS nom_hotel,
    h.ville,
    COUNT(r.reservation_id) AS nombre_reservations
FROM clients c
JOIN reservations r ON c.client_id = r.client_id
JOIN hotels h ON r.hotel_id = h.hotel_id
GROUP BY c.nom, h.nom, h.ville
ORDER BY c.nom, nombre_reservations DESC;

-- Afficher les noms des hôtels dans lesquels il y a plus d'une réservation
SELECT 
    h.nom AS nom_hotel,
    h.ville,
    COUNT(r.reservation_id) AS nombre_reservations
FROM hotels h
JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.nom, h.ville
HAVING COUNT(r.reservation_id) > 1
ORDER BY nombre_reservations DESC;

-- Afficher les noms des hôtels dans lesquels il n'y a pas de réservation
SELECT 
    h.hotel_id,
    h.nom AS nom_hotel,
    h.ville,
    h.etoiles
FROM hotels h
LEFT JOIN reservations r ON h.hotel_id = r.hotel_id
WHERE r.reservation_id IS NULL;

-- ============================================
-- 7. REQUÊTES IMBRIQUÉES
-- ============================================

-- Afficher les clients ayant réservé un hôtel avec plus de 4 étoiles
SELECT DISTINCT
    c.client_id,
    c.nom,
    c.email
FROM clients c
WHERE c.client_id IN (
    SELECT DISTINCT r.client_id
    FROM reservations r
    JOIN hotels h ON r.hotel_id = h.hotel_id
    WHERE h.etoiles > 4
);

-- Afficher le total des revenus générés par chaque hôtel
SELECT 
    h.hotel_id,
    h.nom AS nom_hotel,
    h.ville,
    h.etoiles,
    COALESCE(SUM(r.prix_total), 0) AS revenus_totaux
FROM hotels h
LEFT JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.hotel_id, h.nom, h.ville, h.etoiles
ORDER BY revenus_totaux DESC;

-- ============================================
-- 8. FONCTIONS D'AGRÉGATION AVEC PARTITIONS ET BUCKETS
-- ============================================

-- Revenus totaux par ville (partitionnée)
SELECT 
    ville,
    SUM(total_revenus) AS revenus_totaux_ville
FROM (
    SELECT 
        h.ville,
        r.prix_total AS total_revenus
    FROM hotels_partitioned h
    JOIN reservations r ON h.hotel_id = r.hotel_id
) tmp
GROUP BY ville
ORDER BY revenus_totaux_ville DESC;

-- Nombre total de réservations par client (bucketed)
SELECT 
    client_id,
    COUNT(*) AS nombre_reservations,
    SUM(prix_total) AS total_depense
FROM reservations_bucketed
GROUP BY client_id
ORDER BY nombre_reservations DESC, total_depense DESC;

-- Statistiques détaillées par ville
SELECT 
    h.ville,
    COUNT(DISTINCT h.hotel_id) AS nombre_hotels,
    COUNT(r.reservation_id) AS nombre_reservations,
    AVG(h.etoiles) AS moyenne_etoiles,
    SUM(r.prix_total) AS revenus_totaux,
    AVG(r.prix_total) AS prix_moyen_reservation
FROM hotels_partitioned h
LEFT JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.ville
ORDER BY revenus_totaux DESC;

-- Top 5 des clients dépensiers
SELECT 
    c.client_id,
    c.nom,
    COUNT(r.reservation_id) AS nombre_reservations,
    SUM(r.prix_total) AS total_depense,
    AVG(r.prix_total) AS depense_moyenne
FROM clients c
JOIN reservations_bucketed r ON c.client_id = r.client_id
GROUP BY c.client_id, c.nom
ORDER BY total_depense DESC
LIMIT 5;

-- ============================================
-- REQUÊTES SUPPLÉMENTAIRES D'ANALYSE
-- ============================================

-- Taux d'occupation par hôtel
SELECT 
    h.nom AS nom_hotel,
    h.ville,
    COUNT(r.reservation_id) AS nombre_reservations,
    SUM(DATEDIFF(r.date_fin, r.date_debut)) AS total_nuitees
FROM hotels h
LEFT JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.nom, h.ville
ORDER BY total_nuitees DESC;

-- Réservations par mois
SELECT 
    YEAR(date_debut) AS annee,
    MONTH(date_debut) AS mois,
    COUNT(*) AS nombre_reservations,
    SUM(prix_total) AS revenus_mensuels
FROM reservations
GROUP BY YEAR(date_debut), MONTH(date_debut)
ORDER BY annee, mois;
