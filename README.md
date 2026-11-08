✈️ Flight Tracker - End-to-End Data Engineering Pipeline

📖 Description du projet

Ce projet est une infrastructure complète de Data Engineering permettant d'ingérer, de traiter et d'analyser le trafic aérien mondial en temps réel, tout en intégrant un historique des incidents de vol.

L'objectif est de démontrer la mise en place d'une architecture de données hybride (Batch & Streaming) avec détection d'anomalies en direct.

🏗️ Architecture Technique

Extraction (API) : Récupération continue des positions des avions via l'API OpenSky Network et des données historiques via l'API NTSB (mock).

Ingestion Temps Réel : Publication des données dans un cluster Apache Kafka (conteneurisé via Docker).

Traitement (Streaming) : Consommation des messages Kafka par PySpark. Nettoyage des données, gestion des types, et application de règles métier (détection d'avions dépassant les 1000 km/h).

Orchestration (Batch) : Planification quotidienne avec Apache Airflow pour la mise à jour des tables de dimensions (historique des accidents).

Stockage (Data Warehouse) : Insertion des données transformées dans une base de données cloud Supabase (PostgreSQL) avec un modèle en étoile (Tables de faits et de dimensions).

Visualisation : Tableau de bord temps réel développé avec Streamlit (et analytique avancée prévue sur Power BI).

🗄️ Modélisation des Données (Supabase)

dim_accidents : Historique batch des accidents (chargé par Airflow).

fact_positions : Flux continu des positions géographiques et métriques de vol (chargé par PySpark).

fact_anomalies : Alertes critiques détectées en temps réel par les règles métier PySpark.

🚀 Comment lancer le projet en local

1. Démarrer l'infrastructure Kafka

docker-compose up -d


2. Lancer l'ingestion temps réel (Producer)

source venv/bin/activate
python producer.py


3. Démarrer le traitement PySpark

Nécessite Java 17 et Python 3.11 pour la compatibilité Py4J.

source venv_spark/bin/activate
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_LOCAL_IP="127.0.0.1"
python spark_streaming.py


4. Démarrer le Dashboard Streamlit

source venv/bin/activate
streamlit run dashboard.py


5. Orchestration Batch (Optionnel)

source venv/bin/activate
export AIRFLOW_HOME=~/flight_tracker/airflow
airflow standalone


Accéder ensuite à http://localhost:8080 pour déclencher le DAG.
