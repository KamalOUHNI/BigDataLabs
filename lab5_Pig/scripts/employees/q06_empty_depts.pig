-- ============================================
-- REQUÊTE 6 : Départements sans employés
-- ============================================

-- Charger les données
employees = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

departments = LOAD 'input/departments.txt' 
    USING PigStorage(',') 
    AS (depno:int, name:chararray);

-- Jointure externe à gauche (LEFT OUTER JOIN)
-- Garde tous les départements, même sans employés
all_depts_with_emp = JOIN departments BY depno LEFT OUTER, employees BY depno;

-- Filtrer les départements sans employés (où employees::id est null)
empty_depts = FILTER all_depts_with_emp BY employees::id IS NULL;

-- Sélectionner uniquement les informations du département
empty_depts_list = FOREACH empty_depts GENERATE 
    departments::depno AS depno,
    departments::name AS name;

-- Afficher les résultats
DUMP empty_depts_list;

-- Sauvegarder
STORE empty_depts_list INTO 'pigout/empty_depts' USING PigStorage(',');
