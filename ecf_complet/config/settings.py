"""
Ce fichier contient toutes les configurations pour :
- MinIO (stockage objet)
"""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Charger les variables d'environnement depuis .env
load_dotenv()


@dataclass
class MinIOConfig:
    """
    Configuration pour MinIO.
    
    MinIO est utilisé pour stocker :
    - Les images des produits
    - Les exports CSV/Parquet
    - Les backups
    """
    endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    secure: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"
    
    # Noms des buckets
    bucket_images: str = "product-images"
    bucket_exports: str = "data-exports"
    bucket_backups: str = "backups"


@dataclass
class ScraperConfig:
    """
    Configuration pour le scraper.
    
    Paramètres pour contrôler le comportement du scraping.
    """
    base_url: str = ["https://books.toscrape.com/","https://quotes.toscrape.com"]
    delay: float = 1.0  # Délai entre requêtes (politesse)
    timeout: int = 30   # Timeout des requêtes HTTP
    max_retries: int = 3  # Nombre de tentatives en cas d'erreur
    max_pages: int = 10   # Nombre max de pages à scraper par catégorie


# Instances globales (singleton pattern)
minio_config = MinIOConfig()
scraper_config = ScraperConfig()


# Affichage de la configuration au chargement (debug)
if __name__ == "__main__":
    print("=== Configuration MinIO ===")
    print(f"Endpoint: {minio_config.endpoint}")
    print(f"Secure: {minio_config.secure}")
    print(f"Buckets: {minio_config.bucket_images}, {minio_config.bucket_exports}")
    
    
    print("\n=== Configuration Scraper ===")
    print(f"Base URL: {scraper_config.base_url}")
    print(f"Delay: {scraper_config.delay}s")
