-- ============================================
-- REQUÊTE 3 : Employés avec leurs départements
-- ============================================

-- Charger les données
employees = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

departments = LOAD 'input/departments.txt' 
    USING PigStorage(',') 
    AS (depno:int, name:chararray);

-- Jointure entre employés et départements
emp_with_dept = JOIN employees BY depno, departments BY depno;

-- Sélectionner les colonnes pertinentes
emp_dept_list = FOREACH emp_with_dept GENERATE 
    employees::id AS id,
    employees::nom AS nom,
    employees::prenom AS prenom,
    departments::name AS departement,
    employees::region AS region,
    employees::salaire AS salaire;

-- Trier par ID
sorted_list = ORDER emp_dept_list BY id;

-- Afficher les résultats
DUMP sorted_list;

-- Sauvegarder
STORE sorted_list INTO 'pigout/emp_with_dept' USING PigStorage(',');
