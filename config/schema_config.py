from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def nyc_taxi_bronze_schema() -> StructType:
    """
    Enforced schema for synthetic NYC Taxi records.

    Keep this schema stable across the pipeline to ensure predictable,
    production-friendly behavior (especially in Serverless + Unity Catalog).
    """

    return StructType(
        [
            StructField("VendorID", IntegerType(), nullable=False),
            StructField("pickup_datetime", TimestampType(), nullable=False),
            StructField("dropoff_datetime", TimestampType(), nullable=False),
            StructField("passenger_count", IntegerType(), nullable=True),
            StructField("trip_distance", DoubleType(), nullable=True),
            StructField("fare_amount", DoubleType(), nullable=True),
            StructField("PULocationID", IntegerType(), nullable=True),
            StructField("DOLocationID", IntegerType(), nullable=True),
            # audit columns
            StructField("ingestion_time", TimestampType(), nullable=False),
            StructField("source_file", StringType(), nullable=False),
        ]
    )

