-- ============================================
-- REQUÊTE 5 : Département avec le salaire le plus élevé
-- ============================================

-- Charger les données des employés
employees = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

-- Grouper par département
emp_by_dept = GROUP employees BY depno;

-- Trouver le salaire maximum par département
max_salary_by_dept = FOREACH emp_by_dept GENERATE 
    group AS depno,
    MAX(employees.salaire) AS max_salaire;

-- Trier par salaire maximum décroissant
sorted_max_salary = ORDER max_salary_by_dept BY max_salaire DESC;

-- Prendre le premier (département avec le plus haut salaire)
top_dept = LIMIT sorted_max_salary 1;

-- Afficher les résultats
DUMP top_dept;

-- Sauvegarder
STORE top_dept INTO 'pigout/top_salary_dept' USING PigStorage(',');
