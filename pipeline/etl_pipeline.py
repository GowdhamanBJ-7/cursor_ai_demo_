from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Optional

from pyspark.sql import SparkSession

from config.databricks_config import PipelineConfig
from src.ingestion.read_data import generate_bronze_nyc_taxi_df
from src.transformation.transform_data import (
    transform_to_gold_distance_aggregations,
    transform_to_gold_trips_by_hour,
    transform_to_gold_trips_by_pickup_zone,
    transform_to_silver,
)
from src.write.write_data import ensure_schema_exists, read_table, write_delta_table


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        stream=sys.stdout,
    )


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze → Silver → Gold ETL pipeline")
    parser.add_argument(
        "--catalog",
        default=os.getenv("CATALOG", "main"),
        help="Unity Catalog catalog name (must exist)",
    )
    parser.add_argument(
        "--schema",
        default=os.getenv("SCHEMA", "taxi_medallion"),
        help="Unity Catalog schema name (created if missing)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=int(os.getenv("SYNTHETIC_ROWS", "500")),
        help="Number of synthetic bronze rows to generate (small for <1 minute runtime)",
    )
    return parser.parse_args(argv)


def run() -> None:
    _configure_logging()
    logger = logging.getLogger(__name__)

    args = _parse_args()
    spark = SparkSession.builder.getOrCreate()

    cfg = PipelineConfig(catalog=args.catalog, schema=args.schema)

    try:
        logger.info("ETL: starting pipeline (catalog=%s, schema=%s)", cfg.catalog, cfg.schema)
        ensure_schema_exists(spark, catalog=cfg.catalog, schema=cfg.schema)

        # ----------------------------
        # Bronze
        # ----------------------------
        logger.info("ETL: BRONZE start")
        bronze_df = generate_bronze_nyc_taxi_df(
            spark, num_rows=args.rows, source_name=cfg.source_name
        )
        write_delta_table(bronze_df, full_table_name=cfg.fqtn(cfg.tables.bronze_nyc_taxi))
        logger.info("ETL: BRONZE success")

        # ----------------------------
        # Silver
        # ----------------------------
        logger.info("ETL: SILVER start")
        bronze_read = read_table(spark, full_table_name=cfg.fqtn(cfg.tables.bronze_nyc_taxi))
        silver_df = transform_to_silver(bronze_read)
        write_delta_table(silver_df, full_table_name=cfg.fqtn(cfg.tables.silver_nyc_taxi))
        logger.info("ETL: SILVER success")

        # ----------------------------
        # Gold
        # ----------------------------
        logger.info("ETL: GOLD start")
        silver_read = read_table(spark, full_table_name=cfg.fqtn(cfg.tables.silver_nyc_taxi))

        gold_zone = transform_to_gold_trips_by_pickup_zone(silver_read)
        write_delta_table(
            gold_zone,
            full_table_name=cfg.fqtn(cfg.tables.gold_trips_by_pickup_zone),
            partition_by=["pickup_date"],
        )

        gold_hour = transform_to_gold_trips_by_hour(silver_read)
        write_delta_table(
            gold_hour,
            full_table_name=cfg.fqtn(cfg.tables.gold_trips_by_hour),
            partition_by=["pickup_date"],
        )

        gold_dist = transform_to_gold_distance_aggregations(silver_read)
        write_delta_table(
            gold_dist,
            full_table_name=cfg.fqtn(cfg.tables.gold_distance_aggregations),
            partition_by=["pickup_date"],
        )

        logger.info("ETL: GOLD success")
        logger.info("ETL: pipeline completed successfully")
    except Exception:
        logger.exception("ETL: pipeline failed")
        raise


if __name__ == "__main__":
    run()

