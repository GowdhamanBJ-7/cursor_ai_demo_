from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def ensure_schema_exists(spark: SparkSession, *, catalog: str, schema: str) -> None:
    """
    Ensure schema exists in Unity Catalog.

    We do NOT create catalogs here; the catalog is expected to exist already.
    """

    try:
        logger.info("Ensuring schema exists: %s.%s", catalog, schema)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    except Exception:
        logger.exception("Failed ensuring schema exists: %s.%s", catalog, schema)
        raise


def write_delta_table(
    df: DataFrame,
    *,
    full_table_name: str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> None:
    """
    Write DataFrame to a Unity Catalog managed Delta table (no paths).
    """

    try:
        logger.info("Writing Delta table: %s (mode=%s)", full_table_name, mode)
        writer = (
            df.write.format("delta")
            .mode(mode)
            .option("overwriteSchema", "true")
        )
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(full_table_name)
        logger.info("Write success: %s", full_table_name)
    except Exception:
        logger.exception("Failed writing Delta table: %s", full_table_name)
        raise


def read_table(spark: SparkSession, *, full_table_name: str) -> DataFrame:
    try:
        logger.info("Reading table: %s", full_table_name)
        return spark.table(full_table_name)
    except Exception:
        logger.exception("Failed reading table: %s", full_table_name)
        raise

