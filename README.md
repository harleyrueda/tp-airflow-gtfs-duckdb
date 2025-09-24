# Pipeline ETL : Traiter des données en temps réel et composer un Dashboard Power BI.

----------------------------TP-AIRFLOW-GTFS-DUCKDB -----------------------------------------------------------

## - Dossier dags: 
contient le fichier dag_general_gtfs.py et le dossier fonctions

## - dag_general_gtfs.py:   
Orchestre l’ensemble du flux de traitement

## - Dossier fonctions: dags/fonctions/ 
Regroupe les fonctions Python appelées par le Dag general

## - Dossier exports: 
Répertoire où sont générés les résultats (CSV) pour la visualisation dans Power BI.  

## - Dockerfile et docker-compose.yml: 
Définissent et lancent l’environnement complet:
airflow-init: initialise la base de données et crée l’utilisateur admin
airflow-duckdb: exécute airflow -  le webserver et le scheduler



# Dossiers supplémentaires 

## - Dossier notebooks_de_exploration: 
Notebooks utilisés pour montrer la création et la validation des fonctions avant de les intégrer dans les scripts Python.

## - Dossier old_dags: 
Tests de dags pendant construction

---------------------------------------------------------------------------------------------------------------------------------

# Pour lancer le projet: 

## 1 - créer un .env:

AIRFLOW_USER=user
AIRFLOW_FIRSTNAME=User
AIRFLOW_LASTNAME=Example
AIRFLOW_EMAIL=user@example.com
AIRFLOW_PASSWORD=password

AIRFLOW__CORE__EXECUTOR=SequentialExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db


## 2 - ensuite, lancement du projet avec: docker-compose up --build

## 3 - ensuite ouverture sur airflow 8080

Recommandation : Actuellement ce projet utilise la version Airflow 2.9.3. Pour une amélioration, une prochaine étape serait de migrer vers une version d’Airflow avec PostgreSQL.