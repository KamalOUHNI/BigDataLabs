# ============================================
# Script PowerShell pour déployer Hive Lab
# ============================================

Write-Host "=== Déploiement Hive Lab ===" -ForegroundColor Green

# 1. Créer les dossiers dans le volume partagé
Write-Host "`n1. Création des répertoires..." -ForegroundColor Yellow
$baseDir = "C:\Users\mouad\OneDrive - um5.ac.ma\Documents\hadoop_project\hive"
New-Item -ItemType Directory -Force -Path "$baseDir\data" | Out-Null
New-Item -ItemType Directory -Force -Path "$baseDir\scripts" | Out-Null

# 2. Copier les fichiers de données
Write-Host "2. Copie des fichiers de données..." -ForegroundColor Yellow
Copy-Item -Path ".\HIVE\data\*" -Destination "$baseDir\data\" -Force
Write-Host "   - clients.txt" -ForegroundColor Gray
Write-Host "   - hotels.txt" -ForegroundColor Gray
Write-Host "   - reservations.txt" -ForegroundColor Gray

# 3. Copier les scripts HQL
Write-Host "3. Copie des scripts HQL..." -ForegroundColor Yellow
Copy-Item -Path ".\HIVE\scripts\*" -Destination "$baseDir\scripts\" -Force
Write-Host "   - Creation.hql" -ForegroundColor Gray
Write-Host "   - Loading.hql" -ForegroundColor Gray
Write-Host "   - Queries.hql" -ForegroundColor Gray
Write-Host "   - Cleanup.hql" -ForegroundColor Gray

# 4. Vérifier docker-compose
Write-Host "`n4. Vérification de docker-compose.yml..." -ForegroundColor Yellow
if (Test-Path "docker-compose.yml") {
    Write-Host "   ✓ docker-compose.yml trouvé" -ForegroundColor Green
} else {
    Write-Host "   ✗ docker-compose.yml non trouvé!" -ForegroundColor Red
    exit 1
}

# 5. Démarrer les services Docker
Write-Host "`n5. Démarrage des services Docker..." -ForegroundColor Yellow
docker-compose up -d

# 6. Attendre que Hive soit prêt
Write-Host "`n6. Attente du démarrage de HiveServer2 (30 secondes)..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# 7. Vérifier que le conteneur est en cours d'exécution
Write-Host "`n7. Vérification des conteneurs..." -ForegroundColor Yellow
$hiveRunning = docker ps --filter "name=hiveserver2-standalone" --format "{{.Names}}"
if ($hiveRunning -eq "hiveserver2-standalone") {
    Write-Host "   ✓ HiveServer2 est en cours d'exécution" -ForegroundColor Green
} else {
    Write-Host "   ✗ HiveServer2 n'est pas démarré!" -ForegroundColor Red
    exit 1
}

# 8. Instructions finales
Write-Host "`n=== Déploiement terminé avec succès! ===" -ForegroundColor Green
Write-Host "`nPour exécuter les scripts Hive:" -ForegroundColor Cyan
Write-Host "  1. Accéder au conteneur:" -ForegroundColor White
Write-Host "     docker exec -it hiveserver2-standalone bash" -ForegroundColor Gray
Write-Host "`n  2. Créer la base de données:" -ForegroundColor White
Write-Host "     beeline -u jdbc:hive2://localhost:10000 scott tiger -f /shared_volume/hive/scripts/Creation.hql" -ForegroundColor Gray
Write-Host "`n  3. Charger les données:" -ForegroundColor White
Write-Host "     beeline -u jdbc:hive2://localhost:10000 scott tiger -f /shared_volume/hive/scripts/Loading.hql" -ForegroundColor Gray
Write-Host "`n  4. Exécuter les requêtes:" -ForegroundColor White
Write-Host "     beeline -u jdbc:hive2://localhost:10000 scott tiger -f /shared_volume/hive/scripts/Queries.hql" -ForegroundColor Gray
Write-Host "`nAccès Web UI: http://localhost:10002" -ForegroundColor Cyan
