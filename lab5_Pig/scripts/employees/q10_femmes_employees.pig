-- ============================================
-- REQUÊTE 10 : Départements avec femmes employées
-- ============================================

-- Charger les données
employees = LOAD 'input/employees.txt' 
    USING PigStorage(',') 
    AS (id:int, nom:chararray, prenom:chararray, depno:int, region:chararray, salaire:double);

departments = LOAD 'input/departments.txt' 
    USING PigStorage(',') 
    AS (depno:int, name:chararray);

-- Filtrer les employées (prénoms féminins)
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

-- Compter le nombre de femmes par département
depts_with_femmes = FOREACH femmes_by_dept GENERATE 
    group AS departement,
    COUNT(femmes_with_dept) AS nb_femmes;

-- Trier par nombre de femmes décroissant
sorted_femmes = ORDER depts_with_femmes BY nb_femmes DESC;

-- Afficher les résultats
DUMP sorted_femmes;

-- Sauvegarder le résultat dans pigout/employes_femmes
STORE sorted_femmes INTO 'pigout/employes_femmes' USING PigStorage(',');
