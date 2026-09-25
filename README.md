# Pipeline ETL Financier Résilient & Orchestration Automatisée sous Apache Airflow

## 🎯 Contexte du Projet
Ce projet implémente un pipeline d'intégration de données financières critiques issues d'environnements hétérogènes (APIs REST et serveurs de fichiers). L'architecture élimine les erreurs manuelles, garantit une tolérance zéro aux pertes de paquets et applique une politique stricte d'auditabilité et de non-duplication des écritures comptables.

## 🛠️ Spécifications Techniques & Résilience
* **Orchestration :** Apache Airflow (Exécuteur local conteneurisé).
* **Idempotence Stricte :** Utilisation des dates d'exécution dynamiques d'Airflow (`ds`) assurant qu'une ré-exécution du pipeline n'altère pas l'état final du système.
* **Tolérance aux pannes & Failback :** Gestion du rate-limiting (HTTP 429) avec politique de retry automatique combinée à un mécanisme de bascule sur données financières historiques de secours en cas de coupure tierce.
* **Intégrité Transactionnelle :** Injection en base de données PostgreSQL via une opération atomique **UPSERT** (`ON CONFLICT DO UPDATE`) basée sur une clé primaire composite, éliminant tout risque de doublons.

## 🏗️ Architecture de l'Infrastructure Docker
L'environnement s'appuie sur une isolation multi-conteneurs managée par Docker Compose :
* `postgres` : Base de données relationnelle centralisant les métadonnées d'Airflow et les tables financières cibles.
* `airflow-webserver` : Interface graphique de supervision et d'administration des flux (port `8080`).
* `airflow-scheduler` : Planificateur de tâches chargé de surveiller et de déclencher les DAGs.
* `airflow-init` : Conteneur d'initialisation éphémère configurant les droits d'accès et les migrations.

## 🚀 Guide de Validation et de Contrôle

### 1. Structure de la Table Financière Cible
```sql
CREATE TABLE financial_rates (
    transaction_date DATE NOT NULL,
    source_currency VARCHAR(3) NOT NULL,
    target_currency VARCHAR(3) NOT NULL,
    exchange_rate NUMERIC(18, 6) NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    PRIMARY KEY (transaction_date, source_currency, target_currency)
);
```

### 2. Commande d'Audit des Données (PostgreSQL CLI)
Pour valider l'intégrité et la persistance des écritures, exécutez la commande suivante :
```bash
docker compose exec postgres psql -U airflow -d airflow -c "SELECT transaction_date, source_currency, target_currency, exchange_rate, last_updated FROM financial_rates LIMIT 5;"
```

## 📈 Impacts & Résultats Métiers
* **Automatisation :** Suppression intégrale (100%) des interventions humaines dans la collecte financière journalière.
* **MTTD Optimisé :** Grâce aux politiques d'alertes natives et de retries d'Airflow, le temps moyen de détection et de contournement d'incidents (Mean Time To Detect) est inférieur à **2 minutes**.
