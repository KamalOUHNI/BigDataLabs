-- ============================================
-- REQUÊTE 1 : Salaire moyen par département
-- ============================================

-- Charger les données des employés
employees = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

-- Grouper par département
emp_by_dept = GROUP employees BY depno;

-- Calculer le salaire moyen
avg_salary_by_dept = FOREACH emp_by_dept GENERATE 
    group AS depno,
    AVG(employees.salaire) AS salaire_moyen;

-- Trier par département
sorted_avg = ORDER avg_salary_by_dept BY depno;

-- Afficher les résultats
DUMP sorted_avg;

-- Sauvegarder
STORE sorted_avg INTO 'pigout/avg_salary_by_dept' USING PigStorage(',');
