
# Dossier d’Architecture Technique – DataPulse

## Architecture technique finale

| Couche        | Technologie                   | 
| ------------- | ----------------------------- | 
| Ingestion     | Python (scraping, API, Excel) | 
| Stockage brut | MinIO                         | 
| Nettoyage     | Python ETL                    | 
| DWH / SQL     | PostgreSQL                    | 
| Orchestration | Python CLI                    | 
| Visualisation | SQL / pgAdmin                 | 


## Architecture
Lakehouse avec Medallion Architecture (Bronze / Silver / Gold)


## Description de l’architecture Medallion

L’architecture Medallion repose sur trois couches logiques :

| Couche | Description |
|------|------------|
| Bronze | Données brutes ingérées (scraping, fichiers, API), sans transformation |
| Silver | Données nettoyées, normalisées et conformes RGPD |
| Gold | Données analytiques finales (dimensions et faits) |

---

Le choix d’une architecture **Lakehouse avec Medallion Architecture** permet de construire une solution :

- ✔ Robuste
- ✔ Traçable
- ✔ Conforme au RGPD
- ✔ Scalable et évolutive

Malgré une complexité initiale plus élevée et une redondance de stockage, ce choix est parfaitement adapté à un projet analytique structuré comme **DataPulse** et correspond aux standards actuels de l’ingénierie data.


# Choix des technologies – Projet DataPulse

---

## 1. Contexte

Le projet **DataPulse** repose sur une architecture **Lakehouse avec Medallion Architecture (Bronze / Silver / Gold)**.  
Chaque couche utilise des technologies adaptées à son rôle : stockage des données brutes, données transformées et interrogation analytique.

Ce document justifie les **choix technologiques** réalisés et les compare à des **alternatives possibles**.

---

## 2. Stockage des données brutes (Bronze)

### 2.1 Technologie utilisée : MinIO (Object Storage S3-compatible)

Les données brutes (scraping web, fichiers Excel, réponses API, données e-commerce) sont stockées dans **MinIO**, un système de stockage objet compatible S3.

**Justification :**
- Stockage natif des fichiers bruts (JSON, Excel, CSV)
- Compatible avec l’approche **Data Lake**
- Séparation claire entre données brutes et données transformées
- Déploiement simple via Docker
- API S3 standard (interopérabilité)

**Avantages :**
- Scalabilité horizontale
- Conservation du format original des données
- Très adapté à la couche Bronze
- Solution open-source

---

### 2.2 Alternative : Stockage direct dans PostgreSQL

**Description :**
- Stockage des données brutes directement en tables relationnelles

**Limites :**
- Perte du format original
- Moins flexible pour les données semi-structurées
- Moins adapté à des volumes importants
- Couplage fort entre ingestion et schéma

➡️ **MinIO est plus adapté pour un Data Lake que PostgreSQL.**

---

### 2.3 Autre alternative : AWS S3 / Azure Data Lake

**Avantages :**
- Scalabilité cloud native
- Haute disponibilité

**Limites :**
- Coût
- Dépendance au cloud
- Moins adapté à un environnement pédagogique / local

---

## 3. Stockage des données transformées (Silver & Gold)

### 3.1 Technologie utilisée : PostgreSQL

Les couches **Silver** (données nettoyées, RGPD) et **Gold** (données analytiques) sont stockées dans **PostgreSQL**.

**Justification :**
- Base relationnelle robuste
- Support SQL complet
- Gestion des schémas (bronze / silver / gold)
- Parfaitement adapté aux dimensions et faits
- Facilement interrogeable par des outils BI

**Avantages :**
- Forte cohérence des données
- Transactions ACID
- Modélisation en étoile
- Simplicité d’administration

---

### 3.2 Alternative : Data Warehouse cloud (BigQuery, Snowflake)

**Avantages :**
- Très haute performance analytique
- Scalabilité automatique

**Limites :**
- Coût
- Complexité
- Dépendance à un fournisseur cloud

➡️ **PostgreSQL est suffisant et pertinent pour un projet DataPulse.**

---

### 3.3 Alternative : Delta Lake / Iceberg

**Avantages :**
- Versioning des données
- Optimisé pour le Big Data

**Limites :**
- Complexité élevée
- Surdimensionné pour le volume du projet

---

## 4. Interrogation SQL et analytique

### 4.1 Technologie utilisée : PostgreSQL + SQL standard

L’interrogation des données se fait via :
- SQL standard
- PostgreSQL
- pgAdmin pour l’interface utilisateur

**Justification :**
- SQL universel et largement maîtrisé
- Compatibilité avec les outils BI
- Requêtes analytiques simples et efficaces

**Avantages :**
- Lisibilité des requêtes
- Facilité de maintenance
- Aucune dépendance propriétaire

---

### 4.2 Alternative : Moteurs analytiques distribués (Spark SQL, Trino)

**Avantages :**
- Très performant sur de gros volumes
- Traitement distribué

**Limites :**
- Complexité d’infrastructure
- Overkill pour un projet de taille moyenne

---

## 5. Synthèse des choix technologiques

| Usage | Technologie | Raison principale |
|----|----|----|
| Données brutes | MinIO | Data Lake, formats natifs |
| Données Silver / Gold | PostgreSQL | SQL, cohérence, simplicité |
| Interrogation | SQL / pgAdmin | Standard, analytique |
| Orchestration | Python | Flexibilité, lisibilité |
| Conteneurisation | Docker | Reproductibilité |

---
Les technologies choisies pour ce projet permettent de :

- ✔ Respecter l’architecture Lakehouse
- ✔ Garantir la conformité RGPD
- ✔ Séparer clairement les responsabilités
- ✔ Rester simples, robustes et open-source

Le couple **MinIO + PostgreSQL** constitue un excellent compromis entre flexibilité, performance et simplicité pour un projet analytique structuré.

---
# 4. Modélisation des données – Couche Gold

## 4.1 Modèle de données proposé

Pour la couche finale (**Gold**), le projet **DataPulse** repose sur un **Data Mart de type schéma en étoile (Star Schema)**, orienté **analyse des ventes**.

Ce modèle est spécifiquement conçu pour :
- les requêtes analytiques (OLAP),
- la performance des agrégations,
- la consommation par des outils de BI (Power BI, Tableau, etc.).

La couche Gold contient uniquement des **données propres, agrégées et non personnelles**, conformes au RGPD.

---

## 4.2 Schéma de données (Star Schema)

Le schéma `model_data.png`, situé dans le dossier `docs`, représente le **Sales Data Mart** pour notre projet :


Conforme au modèle fourni pour l’analyse des ventes dans la couche Gold :

gold.dim_books

gold.dim_authors

gold.dim_geo

gold.fact_sales

Calcul metier : sales_amount = quantity * price


⭐ Dimensions :
==>gold.dim_books

book_key SERIAL PRIMARY KEY
book_id TEXT
title TEXT
category TEXT
publication_year INT
sentiment TEXT


==>gold.dim_authors

author_key SERIAL PRIMARY KEY
author_id TEXT
author_name TEXT


==>gold.dim_geo

geo_key SERIAL PRIMARY KEY
postal_code TEXT
city TEXT
department TEXT
country TEXT

⭐ Table de faits
==>gold.fact_sales
