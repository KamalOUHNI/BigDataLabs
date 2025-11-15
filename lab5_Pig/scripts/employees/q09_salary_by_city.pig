-- ============================================
-- REQUÊTE 9 : Salaire total par ville
-- ============================================

-- Charger les données des employés
employees = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

-- Grouper par région (ville)
emp_by_city = GROUP employees BY region;

-- Calculer le salaire total par ville
total_salary_by_city = FOREACH emp_by_city GENERATE 
    group AS ville,
    SUM(employees.salaire) AS salaire_total;

-- Trier par salaire total décroissant
sorted_salary_city = ORDER total_salary_by_city BY salaire_total DESC;

-- Afficher les résultats
DUMP sorted_salary_city;

-- Sauvegarder
STORE sorted_salary_city INTO 'pigout/total_salary_by_city' USING PigStorage(',');
