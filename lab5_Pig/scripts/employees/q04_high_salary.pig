-- ============================================
-- REQUÊTE 4 : Employés avec salaire > 60000
-- ============================================

-- Charger les données des employés
employees = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

-- Filtrer les employés avec salaire > 60000
high_salary_emp = FILTER employees BY salaire > 60000;

-- Trier par salaire décroissant
high_salary_sorted = ORDER high_salary_emp BY salaire DESC;

-- Afficher les résultats
DUMP high_salary_sorted;

-- Sauvegarder
STORE high_salary_sorted INTO 'pigout/high_salary_emp' USING PigStorage(',');
