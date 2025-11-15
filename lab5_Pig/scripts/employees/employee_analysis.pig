-- ============================================
-- ANALYSE DES EMPLOYÉS AVEC APACHE PIG
-- ============================================
-- Description : Script complet d'analyse des données employés
--               Répond à 10 questions business

-- ============================================
-- CHARGEMENT DES DONNÉES
-- ============================================

-- Charger les données des employés
-- Format : ID, Nom, Prenom, depno, Région, Salaire
employees = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

-- Charger les données des départements
-- Format : depno, name
departments = LOAD 'input/departments.txt' 
    USING PigStorage(',') 
    AS (depno:int, name:chararray);

-- Afficher les 5 premiers employés (pour vérification)
employees_sample = LIMIT employees 5;
DUMP employees_sample;

-- ============================================
-- REQUÊTE 1 : Salaire moyen par département
-- ============================================
DESCRIBE employees;

-- Grouper par département
emp_by_dept = GROUP employees BY depno;

-- Calculer le salaire moyen
avg_salary_by_dept = FOREACH emp_by_dept GENERATE 
    group AS depno,
    AVG(employees.salaire) AS salaire_moyen;

-- Afficher les résultats
DUMP avg_salary_by_dept;

-- Sauvegarder
STORE avg_salary_by_dept INTO 'pigout/avg_salary_by_dept' USING PigStorage(',');


-- ============================================
-- REQUÊTE 2 : Nombre d'employés par département
-- ============================================

count_by_dept = FOREACH emp_by_dept GENERATE 
    group AS depno,
    COUNT(employees) AS nb_employes;

DUMP count_by_dept;
STORE count_by_dept INTO 'pigout/count_by_dept' USING PigStorage(',');


-- ============================================
-- REQUÊTE 3 : Employés avec leurs départements
-- ============================================

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

DUMP emp_dept_list;
STORE emp_dept_list INTO 'pigout/emp_with_dept' USING PigStorage(',');


-- ============================================
-- REQUÊTE 4 : Employés avec salaire > 60000
-- ============================================

high_salary_emp = FILTER employees BY salaire > 60000;

-- Trier par salaire décroissant
high_salary_sorted = ORDER high_salary_emp BY salaire DESC;

DUMP high_salary_sorted;
STORE high_salary_sorted INTO 'pigout/high_salary_emp' USING PigStorage(',');


-- ============================================
-- REQUÊTE 5 : Département avec le salaire le plus élevé
-- ============================================

-- Trouver le salaire maximum par département
max_salary_by_dept = FOREACH emp_by_dept GENERATE 
    group AS depno,
    MAX(employees.salaire) AS max_salaire;

-- Trier par salaire maximum décroissant
sorted_max_salary = ORDER max_salary_by_dept BY max_salaire DESC;

-- Prendre le premier (département avec le plus haut salaire)
top_dept = LIMIT sorted_max_salary 1;

DUMP top_dept;
STORE top_dept INTO 'pigout/top_salary_dept' USING PigStorage(',');


-- ============================================
-- REQUÊTE 6 : Départements sans employés
-- ============================================

-- Jointure externe à gauche (LEFT OUTER JOIN)
-- Garde tous les départements, même sans employés
all_depts_with_emp = JOIN departments BY depno LEFT OUTER, employees BY depno;

-- Filtrer les départements sans employés (où employees::id est null)
empty_depts = FILTER all_depts_with_emp BY employees::id IS NULL;

-- Sélectionner uniquement les informations du département
empty_depts_list = FOREACH empty_depts GENERATE 
    departments::depno AS depno,
    departments::name AS name;

DUMP empty_depts_list;
STORE empty_depts_list INTO 'pigout/empty_depts' USING PigStorage(',');


-- ============================================
-- REQUÊTE 7 : Nombre total d'employés
-- ============================================

-- Grouper tous les employés
all_employees = GROUP employees ALL;

-- Compter le nombre total
total_count = FOREACH all_employees GENERATE 
    COUNT(employees) AS total_employes;

DUMP total_count;
STORE total_count INTO 'pigout/total_employees' USING PigStorage(',');


-- ============================================
-- REQUÊTE 8 : Employés de Paris
-- ============================================

paris_emp = FILTER employees BY region == 'Paris';

-- Avec informations du département
paris_emp_with_dept = JOIN paris_emp BY depno, departments BY depno;

paris_emp_list = FOREACH paris_emp_with_dept GENERATE 
    paris_emp::id AS id,
    paris_emp::nom AS nom,
    paris_emp::prenom AS prenom,
    departments::name AS departement,
    paris_emp::salaire AS salaire;

DUMP paris_emp_list;
STORE paris_emp_list INTO 'pigout/paris_employees' USING PigStorage(',');


-- ============================================
-- REQUÊTE 9 : Salaire total par ville
-- ============================================

-- Grouper par région (ville)
emp_by_city = GROUP employees BY region;

-- Calculer le salaire total par ville
total_salary_by_city = FOREACH emp_by_city GENERATE 
    group AS ville,
    SUM(employees.salaire) AS salaire_total;

-- Trier par salaire total décroissant
sorted_salary_city = ORDER total_salary_by_city BY salaire_total DESC;

DUMP sorted_salary_city;
STORE sorted_salary_city INTO 'pigout/total_salary_by_city' USING PigStorage(',');


-- ============================================
-- REQUÊTE 10 : Départements avec femmes employées
-- ============================================

-- Filtrer les employées (prénoms féminins)
-- Liste de prénoms féminins courants
femmes = FILTER employees BY (
    prenom == 'Sophie' OR prenom == 'Marie' OR prenom == 'Claire' OR 
    prenom == 'Anne' OR prenom == 'Julie' OR prenom == 'Emma' OR 
    prenom == 'Chloé' OR prenom == 'Léa' OR prenom == 'Camille' OR 
    prenom == 'Manon'
);

-- Jointure avec les départements
femmes_with_dept = JOIN femmes BY depno, departments BY depno;

-- Grouper par département
femmes_by_dept = GROUP femmes_with_dept BY departments::name;

-- Extraire les départements uniques avec des femmes
depts_with_femmes = FOREACH femmes_by_dept GENERATE 
    group AS departement,
    COUNT(femmes_with_dept) AS nb_femmes;

DUMP depts_with_femmes;

-- Sauvegarder le résultat dans pigout/employes_femmes
STORE depts_with_femmes INTO 'pigout/employes_femmes' USING PigStorage(',');

-- ============================================
-- FIN DU SCRIPT
-- ============================================
