
## ✅ **1. Nettoyage complet de l’environnement**

```powershell
# 🧹 Stoppe et supprime les conteneurs, volumes, réseaux orphelins
docker compose `
  --env-file .env `
  -f docker-compose.yml `
  -f docker-compose.override.airflow.yml `
  -f docker-compose.override.monitoring.yml `
  -f docker-compose.override.metabase.yml `
  -f docker-compose.override.pgadmin.yml `
  -f docker-compose.override.dbt.yml `
  down --volumes --remove-orphans

# 🧼 Supprime les images personnalisées (ignorer les erreurs si elles n'existent pas)
docker rmi sport-airflow:latest -f
docker rmi sport-base:latest -f

# ❌ Supprime le réseau s’il existe encore (sécurité)
docker network rm sport-network 2>$null
```

---

## ✅ **2. Reconstruction des images personnalisées**

```powershell
# 🔨 Image de base Python personnalisée (poetry, curl, etc.)
docker build -f Dockerfile -t sport-base .

# 🔨 Image Airflow avec providers + UTC + user airflow
docker build -f Dockerfile.airflow -t sport-airflow .
```

---

## ✅ **3. Lancement propre de la stack complète**

```powershell
# 🚀 Démarre tous les services avec tous les fichiers override
docker compose `
  --env-file .env `
  -f docker-compose.yml `
  -f docker-compose.override.airflow.yml `
  -f docker-compose.override.monitoring.yml `
  -f docker-compose.override.metabase.yml `
  -f docker-compose.override.pgadmin.yml `
  -f docker-compose.override.dbt.yml `
  up -d --build
```

---

## ✅ **4. Initialisation Airflow + création utilisateur admin**

```powershell
# 🕓 Attendre quelques secondes pour laisser les services se lancer
Start-Sleep -Seconds 15

# 🗄️ Initialise la base Airflow
docker compose `
  --env-file .env `
  -f docker-compose.yml `
  -f docker-compose.override.airflow.yml `
  run --rm sport-airflow-webserver airflow db init

# 👤 Crée un utilisateur admin Airflow (si pas encore présent)
docker compose `
  --env-file .env `
  -f docker-compose.yml `
  -f docker-compose.override.airflow.yml `
  run --rm sport-airflow-webserver airflow users create `
  --username admin `
  --firstname Xavier `
  --lastname Rousseau `
  --role Admin `
  --email xavier@example.com `
  --password admin
```

⚠️ Si tu as déjà un utilisateur `admin`, tu peux le supprimer avant :

```powershell
docker exec -it sport-airflow-webserver airflow users delete --username admin
```

---

## ✅ **5. Vérifications post-démarrage**

```powershell
# 🔍 Liste des conteneurs
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# 🔍 Vérifie que les services critiques sont "healthy"
docker inspect sport-postgres --format='{{json .State.Health}}'
docker inspect sport-redis --format='{{json .State.Health}}'
docker inspect sport-airflow-webserver --format='{{json .State.Health}}'

# 📜 Logs du webserver pour confirmer que tout est OK
docker logs sport-airflow-webserver --tail 50

# ✅ Vérifie que l’utilisateur admin est bien présent
docker exec -it sport-airflow-webserver airflow users list
```

---

## ✅ **6. Accès aux interfaces Web**

| Service     | URL                                            |
| ----------- | ---------------------------------------------- |
| **Airflow** | [http://localhost:8092](http://localhost:8092) |
| Flower      | [http://localhost:5566](http://localhost:5566) |
| Grafana     | [http://localhost:3000](http://localhost:3000) |
| Metabase    | [http://localhost:3001](http://localhost:3001) |
| pgAdmin     | [http://localhost:5050](http://localhost:5050) |
| MinIO       | [http://localhost:9001](http://localhost:9001) |
| Redpanda    | [http://localhost:8085](http://localhost:8085) |
| Prometheus  | [http://localhost:9090](http://localhost:9090) |
| ntfy        | [http://localhost:8080](http://localhost:8080) |

---
 