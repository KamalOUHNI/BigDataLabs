-- ============================================
-- REQUÊTE 8 : Employés de Paris
-- ============================================

-- Charger les données
employees = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

departments = LOAD 'input/departments.txt' 
    USING PigStorage(',') 
    AS (depno:int, name:chararray);

-- Filtrer les employés de Paris
paris_emp = FILTER employees BY region == 'Paris';

-- Jointure avec les départements
paris_emp_with_dept = JOIN paris_emp BY depno, departments BY depno;

-- Sélectionner les colonnes pertinentes
paris_emp_list = FOREACH paris_emp_with_dept GENERATE 
    paris_emp::id AS id,
    paris_emp::nom AS nom,
    paris_emp::prenom AS prenom,
    departments::name AS departement,
    paris_emp::salaire AS salaire;

-- Trier par ID
sorted_paris = ORDER paris_emp_list BY id;

-- Afficher les résultats
DUMP sorted_paris;

-- Sauvegarder
STORE sorted_paris INTO 'pigout/paris_employees' USING PigStorage(',');
