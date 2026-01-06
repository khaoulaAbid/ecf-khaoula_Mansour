# DataPulse – Projet Data Warehouse & Analytics 
Bienvenue dans le dépôt **DataPulse – Projet Data Warehouse & Analytics**.

Ce projet présente un **pipeline complet de data engineering de bout en bout**, basé sur une **architecture Lakehouse avec Medallion Architecture (Bronze / Silver / Gold)**. Il a été conçu comme un **projet académique et de portfolio**, aligné avec les **bonnes pratiques modernes en data engineering et data analytics**.

<img width="1536" height="1024" alt="architecture_data" src="https://github.com/user-attachments/assets/3da23056-66d9-45a7-ae15-b5cb8301dcd9" />


## 🏗️ Architecture des données

L’architecture de DataPulse repose sur le modèle **Medallion Architecture** :

* **Couche Bronze** : ingestion des données brutes depuis plusieurs sources
* **Couche Silver** : nettoyage, normalisation et mise en conformité RGPD
* **Couche Gold** : données analytiques prêtes pour la consommation métier

Cette architecture garantit :

* la traçabilité complète des transformations,
* la scalabilité,
* la robustesse,
* une séparation claire des responsabilités.

---

## 📖 Présentation du projet

Le projet couvre l’ensemble du cycle de vie de la donnée :

* **Ingestion** de sources hétérogènes (web scraping, API REST, fichiers Excel)
* **Stockage des données brutes** dans un stockage objet (Data Lake)
* **Traitements ETL** en Python
* **Modélisation Data Warehouse** en schéma en étoile (Star Schema)
* **Analyses SQL** pour la BI et le reporting

🎯 Ce projet met en valeur les compétences suivantes :

* Data Engineering
* Data Warehousing
* Conception de pipelines ETL
* SQL & analytique
* Modélisation de données (Star Schema)

---

## 🛠️ Technologies utilisées

| Couche           | Technologie                          |
| ---------------- | ------------------------------------ |
| Ingestion        | Python (Scraping, API REST, Excel)   |
| Stockage Bronze  | MinIO (Object Storage compatible S3) |
| Transformation   | Python ETL                           |
| Data Warehouse   | PostgreSQL                           |
| Orchestration    | Python CLI                           |
| Analytique       | SQL / pgAdmin                        |
| Conteneurisation | Docker & Docker Compose              |

---

## 🧱 Implémentation des couches Medallion

### 🟤 Couche Bronze – Données brutes

**Objectif :** stocker les données telles qu’elles sont reçues depuis les sources, sans transformation.

**Sources :**

* Web scraping (livres et citations)
* API REST (données géographiques)
* Fichiers Excel (partenaires / librairies)

**Implémentation :**

* Scripts Python d’ingestion
* Stockage des fichiers bruts dans **MinIO** (JSON / CSV / Excel)

---

### ⚪ Couche Silver – Données nettoyées

**Objectif :** préparer les données pour l’analyse via :

* nettoyage,
* normalisation,
* contrôles qualité,
* règles de conformité RGPD.

**Implémentation :**

* Logique de transformation en Python
* Chargement des données nettoyées dans **PostgreSQL**

---

### 🟡 Couche Gold – Données prêtes métier

**Objectif :** fournir des données optimisées pour l’analyse, le reporting et les outils BI.

**Implémentation :**

* Modélisation en schéma en étoile (tables de faits et de dimensions)
* Stockage dans PostgreSQL

---

## ⭐ Couche Gold – Modélisation des données

La couche Gold implémente un **Data Mart orienté ventes**, basé sur un **schéma en étoile**, optimisé pour les requêtes analytiques.

### Tables de dimensions

**`gold.dim_books`**

* book_key (PK)
* book_id
* title
* category
* publication_year
* sentiment

**`gold.dim_authors`**

* author_key (PK)
* author_id
* author_name

**`gold.dim_geo`**

* geo_key (PK)
* postal_code
* city
* department
* country

### Table de faits

**`gold.fact_sales`**

* clés étrangères vers les dimensions
* quantity
* price

**Indicateur métier :**

```
sales_amount = quantity * price
```

---

## 📂 Structure réelle du projet

ecf_complet/
│
├── docker_compose.yml                # Infrastructure Docker
│
├── docs/                             # Documentation du projet
│   ├── architecture_data.png         # Diagramme global d’architecture
│   ├── model_data.png                # Modèle de données (Star Schema)
│   ├── dat.md                        # Dictionnaire / documentation des données
│   └── rgpd_conformite.md            # Conformité RGPD
│
├── config/
│   └── settings.py                   # Configuration centralisée
│
├── storage/
│   └── minio_client.py               # Client MinIO (Data Lake – Bronze)
│
├── sql/                              # Requêtes SQL analytiques
│   ├── analyses.sql                  # Analyses SQL PostgreSQL
│   └── analysis_sql_pandas.sql       # Analyses SQL utilisées avec Pandas
│
├── src/
│   ├── pipeline.py                   # Orchestrateur CLI (Bronze / Silver / Gold)
│   │
│   ├── ingestion/                    # Couche Bronze – ingestion des données
│   │   ├── scrape_books.py           # Web scraping livres
│   │   ├── scrape_quotes.py          # Web scraping citations
│   │   ├── scrape_api_geo.py         # Ingestion API REST
│   │   └── import_excel.py           # Import fichiers Excel
│   │
│   └── transformation/               # Transformations Silver & Gold
│       ├── bronze_to_silver.py       # Nettoyage & normalisation
│       └── silver_to_gold.py         # Modélisation analytique
│
└── .vscode/
    └── settings.json                 # Configuration éditeur

---

## ⚙️ Guide d’exécution

### 1️⃣ Lancer l’infrastructure

```bash
docker-compose up -d
```

---

### 2️⃣ Accéder au conteneur ETL

```bash
docker exec -it datapulse_etl bash
```

---

### 3️⃣ Installer les dépendances Python

```bash
pip install -r requirements.txt
```

---

### 4️⃣ Exécuter le pipeline

**Bronze – Ingestion**

```bash
python src/pipeline.py --step bronze
```

**Silver – Nettoyage et normalisation**

```bash
python src/pipeline.py --step silver
```

**Gold – Modélisation analytique**

```bash
python src/pipeline.py --step gold
```

---

## 📊 Consommation des données

Les données de la couche Gold peuvent être exploitées via :

* des requêtes SQL (PostgreSQL),
* pgAdmin,
* des outils BI (Power BI, Tableau),
* des analyses avancées et data science.


