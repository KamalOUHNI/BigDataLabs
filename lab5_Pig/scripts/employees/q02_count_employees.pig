-- ============================================
-- REQUÊTE 2 : Nombre d'employés par département
-- ============================================

-- Charger les données des employés
employees = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

-- Grouper par département
emp_by_dept = GROUP employees BY depno;

-- Compter les employés par département
count_by_dept = FOREACH emp_by_dept GENERATE 
    group AS depno,
    COUNT(employees) AS nb_employes;

-- Trier par département
sorted_count = ORDER count_by_dept BY depno;

-- Afficher les résultats
DUMP sorted_count;

-- Sauvegarder
STORE sorted_count INTO 'pigout/count_by_dept' USING PigStorage(',');
