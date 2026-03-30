from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession

def _bootstrap_project_path() -> None:
    """
    Ensure bundle project root is importable in Databricks jobs.

    When spark_python_task executes `pipeline/etl_pipeline.py`, the working
    directory may not include the bundle root on sys.path. We add the parent
    of `pipeline/` explicitly so `config` and `src` imports resolve reliably.
    """

    candidate_roots: list[Path] = []

    # Normal script execution path.
    file_from_globals = globals().get("__file__")
    if file_from_globals:
        candidate_roots.append(Path(file_from_globals).resolve().parent.parent)

    # Databricks may execute via exec(...), where __file__ is missing.
    # In that case, function code metadata still carries the source filename.
    code_filename = _bootstrap_project_path.__code__.co_filename
    if code_filename:
        candidate_roots.append(Path(code_filename).resolve().parent.parent)

    # Fallbacks: working directory and its parent.
    cwd = Path.cwd().resolve()
    candidate_roots.extend([cwd, cwd.parent])

    for root in candidate_roots:
        if (root / "config").exists() and (root / "src").exists():
            root_str = str(root)
            if root_str not in sys.path:
                sys.path.insert(0, root_str)
            return


_bootstrap_project_path()

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

