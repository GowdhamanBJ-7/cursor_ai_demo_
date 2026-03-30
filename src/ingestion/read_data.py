from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta
from typing import List

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from config.schema_config import nyc_taxi_bronze_schema

logger = logging.getLogger(__name__)


def _generate_synthetic_rows(num_rows: int) -> List[Row]:
    random.seed(42)
    base = datetime(2026, 1, 1, 0, 0, 0)
    rows: List[Row] = []

    for i in range(num_rows):
        vendor_id = random.choice([1, 2])
        pu_loc = random.randint(1, 265)
        do_loc = random.randint(1, 265)
        passenger_count = random.choice([None, 1, 1, 1, 2, 3, 4])

        pickup = base + timedelta(minutes=int(i * 3 + random.randint(0, 10)))
        duration_min = random.randint(3, 45)
        dropoff = pickup + timedelta(minutes=duration_min)

        trip_distance = round(max(0.1, random.gauss(3.2, 2.0)), 2)
        fare_amount = round(max(2.5, 2.5 + trip_distance * random.uniform(1.2, 3.0)), 2)

        rows.append(
            Row(
                VendorID=vendor_id,
                pickup_datetime=pickup,
                dropoff_datetime=dropoff,
                passenger_count=passenger_count,
                trip_distance=trip_distance if random.random() > 0.02 else None,
                fare_amount=fare_amount if random.random() > 0.02 else None,
                PULocationID=pu_loc if random.random() > 0.01 else None,
                DOLocationID=do_loc if random.random() > 0.01 else None,
            )
        )

    # Add a few duplicates to validate Silver dedup logic
    if rows:
        rows.extend(rows[: min(5, len(rows))])

    return rows


def generate_bronze_nyc_taxi_df(
    spark: SparkSession, *, num_rows: int, source_name: str
) -> DataFrame:
    """
    Generate synthetic NYC Taxi data using spark.createDataFrame with schema enforcement,
    plus required audit columns.
    """

    try:
        logger.info("Bronze: generating %s synthetic NYC Taxi rows", num_rows)
        schema = nyc_taxi_bronze_schema()

        base_rows = _generate_synthetic_rows(num_rows=num_rows)
        df = spark.createDataFrame(base_rows)

        df = (
            df.select(
                F.col("VendorID").cast("int").alias("VendorID"),
                F.col("pickup_datetime").cast("timestamp").alias("pickup_datetime"),
                F.col("dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
                F.col("passenger_count").cast("int").alias("passenger_count"),
                F.col("trip_distance").cast("double").alias("trip_distance"),
                F.col("fare_amount").cast("double").alias("fare_amount"),
                F.col("PULocationID").cast("int").alias("PULocationID"),
                F.col("DOLocationID").cast("int").alias("DOLocationID"),
            )
            .withColumn("ingestion_time", F.current_timestamp())
            .withColumn("source_file", F.lit(source_name))
        )

        # enforce final schema order and nullability expectations
        df = spark.createDataFrame(df.rdd, schema=schema)
        return df
    except Exception:
        logger.exception("Bronze: failed to generate synthetic NYC Taxi dataframe")
        raise

