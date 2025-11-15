-- ============================================
-- REQUÊTE 7 : Nombre total d'employés
-- ============================================

-- Charger les données des employés
employees = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

-- Grouper tous les employés ensemble
all_employees = GROUP employees ALL;

-- Compter le nombre total d'employés
total_count = FOREACH all_employees GENERATE 
    COUNT(employees) AS total_employes;

-- Afficher les résultats
DUMP total_count;

-- Sauvegarder
STORE total_count INTO 'pigout/total_employees' USING PigStorage(',');
