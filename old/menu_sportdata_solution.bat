@echo off

REM -------------------------------------------------------------------------------
REM Initialisation de la console et encodage UTF-8
REM -------------------------------------------------------------------------------
chcp 65001 >nul

REM Définition du titre de la fenêtre
title Sport Data Solution - Interface CLI

REM -------------------------------------------------------------------------------
REM Définition des fichiers Docker Compose à utiliser
REM -------------------------------------------------------------------------------
set COMPOSE_FILES=-f docker-compose.yml ^
  -f docker-compose.override.airflow.yml ^
  -f docker-compose.override.monitoring.yml ^
  -f docker-compose.override.metabase.yml ^
  -f docker-compose.override.pgadmin.yml ^
  -f docker-compose.override.dbt.yml

REM -------------------------------------------------------------------------------
REM Répertoire contenant les scripts Python pour Airflow
REM -------------------------------------------------------------------------------
set SCRIPT_DIR=airflow\scripts

:MENU
cls
color 0A

echo.
echo ================================================================================
echo                       🌐 Sport Data Solution – Interface CLI
echo ================================================================================
echo.

REM --------------------------- Stack Docker --------------------------------------
echo    ----------------------------------------------------------------------------
echo    STACK DOCKER
echo    ----------------------------------------------------------------------------
echo    [ 1]  Démarrer la stack Docker (build + up + user Airflow)
echo    [ 2]  Stopper la stack
echo    [ 3]  Supprimer complètement la stack (volumes + orphelins)
echo    [ 4]  Terminal interactif dans le conteneur sportdata-app
echo.

REM --------------------- Initialisation des données -------------------------------
echo    ----------------------------------------------------------------------------
echo    INITIALISATION DES DONNÉES
echo    ----------------------------------------------------------------------------
echo    [ 5]  Importer les employés (import_employes.py)
echo    [ 6]  Simuler les activités (simulate_activites.py)
echo    [ 7]  Exécuter les deux scripts init l’un après l’autre
echo.

REM -------------------------- Interfaces Web ---------------------------------------
echo    ----------------------------------------------------------------------------
echo    INTERFACES WEB
echo    ----------------------------------------------------------------------------
echo    [ 8]  Metabase        → http://localhost:3001
echo    [ 9]  Airflow         → http://localhost:8082
echo    [10]  MinIO           → http://localhost:9001
echo    [11]  Prometheus      → http://localhost:9090
echo    [12]  Grafana         → http://localhost:3000
echo    [13]  Redpanda        → http://localhost:8085
echo    [14]  ntfy            → http://localhost:8080
echo    [15]  pgAdmin         → http://localhost:5050
echo.

REM -------------------------- DBT & Airflow ----------------------------------------
echo    ----------------------------------------------------------------------------
echo    DBT - AIRFLOW
echo    ----------------------------------------------------------------------------
echo    [16]  dbt run
echo    [17]  Voir les logs d’un service
echo    [18]  Rebuild du conteneur DBT
echo    [19]  Build de l’image Airflow personnalisée
echo    [20]  Initialiser Airflow (airflow db migrate + default connections)
echo    [21]  Terminal dans Airflow webserver
echo    [22]  Logs temps réel du webserver Airflow
echo.

REM --------------- Scripts & Tests Python ----------------------------------------
echo    ----------------------------------------------------------------------------
echo    SCRIPTS - TESTS PYTHON
echo    ----------------------------------------------------------------------------
echo    [23]  Exécuter un script Python manuellement (répertoire airflow/scripts/)
echo    [24]  Lancer tous les tests automatisés (tests/)
echo    [25]  Lancer un test spécifique
echo.

REM ------------------------ Outils Développeur -------------------------------------
echo    ----------------------------------------------------------------------------
echo    OUTILS DÉVELOPPEUR
echo    ----------------------------------------------------------------------------
echo    [26]  Nettoyer les dossiers logs/ et exports/
echo    [27]  Ouvrir le rapport HTML des tests
echo.
echo     [ 0]  Quitter
echo.

REM Lecture du choix utilisateur
set /p choix=🎯 Que veux-tu faire ? :

REM -------------------------------------------------------------------------------
REM Routines de redirection vers les labels correspondants
REM -------------------------------------------------------------------------------
if "%choix%"=="1"  goto DEMARRER
if "%choix%"=="2"  goto STOPPER
if "%choix%"=="3"  goto SUPPRIMER
if "%choix%"=="4"  goto TERMINAL
if "%choix%"=="5"  goto IMPORT_EMPLOYES
if "%choix%"=="6"  goto SIMULATE_ACTIVITES
if "%choix%"=="7"  goto INIT_ALL
if "%choix%"=="8"  start http://localhost:3001
if "%choix%"=="9"  start http://localhost:8082
if "%choix%"=="10" start http://localhost:9001
if "%choix%"=="11" start http://localhost:9090
if "%choix%"=="12" start http://localhost:3000
if "%choix%"=="13" start http://localhost:8085
if "%choix%"=="14" start http://localhost:8080
if "%choix%"=="15" start http://localhost:5050
if "%choix%"=="16" goto DBT_RUN
if "%choix%"=="17" goto LOGS
if "%choix%"=="18" goto DBT_REBUILD
if "%choix%"=="19" goto BUILD_AIRFLOW
if "%choix%"=="20" goto INIT_AIRFLOW
if "%choix%"=="21" goto TERMINAL_AIRFLOW
if "%choix%"=="22" goto LOGS_AIRFLOW
if "%choix%"=="23" goto SCRIPTS
if "%choix%"=="24" goto TOUS_TESTS
if "%choix%"=="25" goto TEST_UNIQUE
if "%choix%"=="26" goto CLEAN
if "%choix%"=="27" goto RAPPORT
if "%choix%"=="0"  exit
goto MENU

REM -------------------------------------------------------------------------------
REM Section : Démarrage de la stack Docker
REM -------------------------------------------------------------------------------
:DEMARRER
cls
echo 🔧 Démarrage de la stack Docker...
docker compose %COMPOSE_FILES% up -d --build
timeout /t 10 /nobreak >nul

REM Initialisation de la base Airflow avec les commandes recommandées
docker compose %COMPOSE_FILES% run --rm sport-airflow-webserver ^
    airflow db migrate && ^
    airflow connections create-default-connections

docker compose %COMPOSE_FILES% run --rm sport-airflow-webserver airflow users create ^
    --username admin ^
    --firstname Xavier ^
    --lastname Rousseau ^
    --role Admin ^
    --email xavier@example.com ^
    --password admin
pause
goto MENU

REM -------------------------------------------------------------------------------
REM Section : Arrêt de la stack Docker
REM -------------------------------------------------------------------------------
:STOPPER
cls
docker compose %COMPOSE_FILES% down
pause
goto MENU

REM -------------------------------------------------------------------------------
REM Section : Suppression complète de la stack Docker
REM -------------------------------------------------------------------------------
:SUPPRIMER
cls
docker compose %COMPOSE_FILES% down -v --remove-orphans
pause
goto MENU

REM -------------------------------------------------------------------------------
REM Section : Shell interactif dans le conteneur sportdata-app
REM -------------------------------------------------------------------------------
:TERMINAL
cls
docker compose %COMPOSE_FILES% exec sportdata-app bash
pause
goto MENU

REM -------------------------------------------------------------------------------
REM Section : Import des employés
REM -------------------------------------------------------------------------------
:IMPORT_EMPLOYES
cls
echo 📥 Import des employés...
docker compose %COMPOSE_FILES% run --rm sportdata-app python %SCRIPT_DIR%\import_employes.py
pause
goto MENU

REM -------------------------------------------------------------------------------
REM Section : Simulation des activités sportives
REM -------------------------------------------------------------------------------
:SIMULATE_ACTIVITES
cls
echo 🏃 Simulation des activités sportives...
docker compose %COMPOSE_FILES% run --rm sportdata-app python %SCRIPT_DIR%\simulate_activites.py
pause
goto MENU

REM -------------------------------------------------------------------------------
REM Section : Exécution complète des scripts d'initialisation
REM -------------------------------------------------------------------------------
:INIT_ALL
cls
echo 🚀 Exécution complète du pipeline RH + activités...
docker compose %COMPOSE_FILES% run --rm sportdata-app python %SCRIPT_DIR%\import_employes.py
docker compose %COMPOSE_FILES% run --rm sportdata-app python %SCRIPT_DIR%\simulate_activites.py
pause
goto MENU

REM -------------------------------------------------------------------------------
REM Section : dbt run via Docker
REM -------------------------------------------------------------------------------
:DBT_RUN
cls
docker compose %COMPOSE_FILES% exec sport-dbt dbt run
pause
goto MENU

REM -------------------------------------------------------------------------------
REM Section : Affichage des logs d'un conteneur
REM -------------------------------------------------------------------------------
:LOGS
cls
set /p container=Nom du conteneur : 
docker compose %COMPOSE_FILES% logs -f %container%
pause
goto MENU

REM -------------------------------------------------------------------------------
REM Section : Rebuild du conteneur DBT
REM -------------------------------------------------------------------------------
:DBT_REBUILD
cls
docker compose %COMPOSE_FILES% build sport-dbt
docker compose %COMPOSE_FILES% up -d sport-dbt
pause
goto MENU

REM -------------------------------------------------------------------------------
REM Section : Construction de l'image Airflow personnalisée
REM -------------------------------------------------------------------------------
:BUILD_AIRFLOW
cls
docker build -f Dockerfile.airflow -t sport-airflow:latest .
pause
goto MENU

REM -------------------------------------------------------------------------------
REM Section : Initialisation de la base Airflow (migrations et connexions)
REM -------------------------------------------------------------------------------
:INIT_AIRFLOW
cls
docker compose %COMPOSE_FILES% run --rm sport-airflow-webserver ^
    airflow db migrate && ^
    airflow connections create-default-connections
pause
goto MENU

REM -------------------------------------------------------------------------------
REM Section : Terminal du webserver Airflow
REM -------------------------------------------------------------------------------
:TERMINAL_AIRFLOW
cls
docker compose %COMPOSE_FILES% exec sport-airflow-webserver bash
pause
goto MENU

REM -------------------------------------------------------------------------------
REM Section : Logs temps réel du webserver Airflow
REM -------------------------------------------------------------------------------
:LOGS_AIRFLOW
cls
docker compose %COMPOSE_FILES% logs -f sport-airflow-webserver
pause
goto MENU

REM -------------------------------------------------------------------------------
REM Section : Exécution manuelle de scripts Python
REM -------------------------------------------------------------------------------
:SCRIPTS
cls
set /p script=Nom du script (ex: analyse.py) :
if "%script%"=="" goto MENU
set /p args=Arguments éventuels :
docker compose %COMPOSE_FILES% exec -w /app sportdata-app python %SCRIPT_DIR%\%script% %args%
pause
goto MENU

REM -------------------------------------------------------------------------------
REM Section : Lancer tous les tests automatisés
REM -------------------------------------------------------------------------------
:TOUS_TESTS
cls
docker compose %COMPOSE_FILES% exec sportdata-app pytest -v %SCRIPT_DIR%\tests\
pause
goto MENU

REM -------------------------------------------------------------------------------
REM Section : Lancer un test spécifique
REM -------------------------------------------------------------------------------
:TEST_UNIQUE
cls
set /p testfile=Nom du fichier test (ex: test_utils_google.py) :
if "%testfile%"=="" goto MENU
docker compose %COMPOSE_FILES% exec sportdata-app pytest -v %SCRIPT_DIR%\tests\%testfile%
pause
goto MENU

REM -------------------------------------------------------------------------------
REM Section : Nettoyage des dossiers logs et exports
REM -------------------------------------------------------------------------------
:CLEAN
cls
rmdir /s /q "%~dp0logs" >nul 2>&1
rmdir /s /q "%~dp0exports" >nul 2>&1
mkdir "%~dp0logs"
mkdir "%~dp0exports"
echo ✅ Répertoires nettoyés et recréés.
pause
goto MENU

REM -------------------------------------------------------------------------------
REM Section : Ouverture du rapport HTML des tests
REM -------------------------------------------------------------------------------
:RAPPORT
cls
start "" "%~dp0exports\rapport_tests.html"
pause
goto MENU
