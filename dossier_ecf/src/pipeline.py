import argparse
from utils.logger import get_logger

from ingestion.scrape_books import run as scrape_books
from ingestion.scrape_quotes import run as scrape_quotes
from ingestion.scrape_ecommerce import run as scrape_products
from ingestion.import_excel import run as import_excel
from ingestion.api_geocoding import run as api_geocoding

from transformation.bronze_to_silver import run as bronze_to_silver
from transformation.silver_to_gold import run as silver_to_gold

# ===============================================================================
# Script Purpose:
#     l'exécution indépendamment de chaque couche 
#     Bronze pour l’ingestion
#     Silver pour le nettoyage et la conformité RGPD
#     Gold pour la modélisation analytique
# ===============================================================================



logger = get_logger("pipeline")


def main(step: str):
    logger.info(f"PIPELINE START – step={step}")

    try:
        if step == "bronze":
            logger.info("Running Bronze ingestion")

            
            scrape_books()
            scrape_quotes()
            import_excel() 
            scrape_products()
            api_geocoding()

        elif step == "silver":
            logger.info("Running Bronze → Silver transformation")
            bronze_to_silver()

        elif step == "gold":
            logger.info("Running Silver → Gold transformation")
            silver_to_gold()

        else:
            raise ValueError("Invalid step. Use: bronze | silver | gold")

    except Exception as e:
        logger.error(f"Pipeline failed at step={step}: {e}", exc_info=True)
        raise

    logger.info(f"PIPELINE END – step={step}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DataPulse ETL Pipeline")
    parser.add_argument(
        "--step",
        required=True,
        choices=["bronze", "silver", "gold"],
        help="Pipeline step to run"
    )

    args = parser.parse_args()
    main(args.step)
