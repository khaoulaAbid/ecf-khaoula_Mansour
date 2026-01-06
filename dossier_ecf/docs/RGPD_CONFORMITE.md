# RGPD_CONFORMITE.md  
**Projet DataPulse – Conformité RGPD**

---

## 1. Contexte

Dans le cadre du projet **DataPulse**, un fichier externe nommé  
`partenaire_librairies.xlsx` est fourni.

Ce fichier contient des informations sur **20 librairies partenaires**, dont certaines données à caractère personnel, soumises au **Règlement Général sur la Protection des Données (RGPD)**.

Ce document décrit les mesures mises en œuvre afin d’assurer la conformité du traitement de ces données.

---

## 2. Inventaire des données collectées

### 2.1 Données du fichier partenaire_librairies.xlsx

| Colonne | Type | Sensibilité RGPD |
|------|------|----------------|
| nom_librairie | string | Donnée publique |
| adresse | string | Donnée publique |
| code_postal | string | Donnée publique |
| ville | string | Donnée publique |
| contact_nom | string | Donnée personnelle |
| contact_email | string | Donnée personnelle |
| contact_telephone | string | Donnée personnelle |
| ca_annuel | float | Donnée confidentielle |
| date_partenariat | date | Donnée publique |
| specialite | string | Donnée publique |

### Données personnelles identifiées

Les données à caractère personnel sont :
- `contact_nom`
- `contact_email`
- `contact_telephone`

---

## 3. Base légale du traitement

Le traitement des données repose sur les bases légales prévues par l’article 6 du RGPD :

| Donnée | Finalité | Base légale |
|------|--------|------------|
| contact_nom | Gestion des partenaires | Intérêt légitime |
| contact_email | Communication professionnelle | Intérêt légitime |
| contact_telephone | Communication professionnelle | Intérêt légitime |
| ca_annuel | Analyse économique | Intérêt légitime |

---

## 4. Mesures de protection mises en œuvre

### 4.1 Principe de *Privacy by Design*

L’architecture du projet **DataPulse** est organisée en trois couches :

| Couche | Description |
|------|------------|
| Bronze | Données brutes importées |
| Silver | Données nettoyées et conformes RGPD |
| Gold | Données analytiques sans données personnelles |

---

### 4.2 Suppression et anonymisation des données personnelles

Dès la couche **Silver**, les mesures suivantes sont appliquées :

#### Suppression des données sensibles
Les colonnes suivantes sont supprimées :
- `contact_email`
- `contact_telephone`

#### Pseudonymisation
Le champ `contact_nom` est anonymisé à l’aide du mécanisme suivant :

```python
df["contact_nom"] = df["contact_nom"].apply(
    lambda x: f"user_{abs(hash(x)) % 10000}"
)
