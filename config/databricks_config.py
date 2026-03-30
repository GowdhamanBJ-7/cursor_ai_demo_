from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TableNames:
    bronze_nyc_taxi: str = "bronze_nyc_taxi"
    silver_nyc_taxi: str = "silver_nyc_taxi"
    gold_trips_by_pickup_zone: str = "gold_trips_by_pickup_zone"
    gold_trips_by_hour: str = "gold_trips_by_hour"
    gold_distance_aggregations: str = "gold_distance_aggregations"


@dataclass(frozen=True)
class PipelineConfig:
    """
    Databricks Serverless + Unity Catalog compatible configuration.

    Notes:
    - We intentionally avoid DBFS paths and /databricks-datasets.
    - All persistence is via Unity Catalog managed Delta tables using saveAsTable.
    """

    catalog: str
    schema: str
    source_name: str = "synthetic_data"
    tables: TableNames = TableNames()

    def fqtn(self, table: str) -> str:
        """Fully-qualified table name: catalog.schema.table"""
        return f"{self.catalog}.{self.schema}.{table}"

