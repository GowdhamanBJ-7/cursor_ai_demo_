from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def transform_to_silver(bronze_df: DataFrame) -> DataFrame:
    """
    Silver layer transformations:
    - Remove duplicates
    - Handle nulls
    - Type casting
    - Derived columns: trip_duration (minutes), pickup_date, pickup_hour
    - Basic data quality checks
    """

    try:
        logger.info("Silver: starting transformation")

        df = bronze_df.dropDuplicates(
            [
                "VendorID",
                "pickup_datetime",
                "dropoff_datetime",
                "PULocationID",
                "DOLocationID",
                "fare_amount",
                "trip_distance",
            ]
        )

        df = (
            df.withColumn("VendorID", F.col("VendorID").cast("int"))
            .withColumn("pickup_datetime", F.col("pickup_datetime").cast("timestamp"))
            .withColumn("dropoff_datetime", F.col("dropoff_datetime").cast("timestamp"))
            .withColumn("passenger_count", F.col("passenger_count").cast("int"))
            .withColumn("trip_distance", F.col("trip_distance").cast("double"))
            .withColumn("fare_amount", F.col("fare_amount").cast("double"))
            .withColumn("PULocationID", F.col("PULocationID").cast("int"))
            .withColumn("DOLocationID", F.col("DOLocationID").cast("int"))
        )

        df = df.fillna(
            {
                "passenger_count": 1,
                "trip_distance": 0.0,
                "fare_amount": 0.0,
            }
        )

        trip_duration = (
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime"))
            / F.lit(60.0)
        )

        df = (
            df.withColumn("trip_duration", trip_duration.cast("double"))
            .withColumn("pickup_date", F.to_date("pickup_datetime"))
            .withColumn("pickup_hour", F.hour("pickup_datetime").cast("int"))
        )

        # Basic DQ checks + filtering of clearly invalid records
        invalid = (
            (F.col("pickup_datetime").isNull())
            | (F.col("dropoff_datetime").isNull())
            | (F.col("dropoff_datetime") < F.col("pickup_datetime"))
            | (F.col("trip_distance") < 0)
            | (F.col("fare_amount") < 0)
            | (F.col("trip_duration") < 0)
        )

        invalid_count = df.filter(invalid).count()
        total_count = df.count()

        if invalid_count > 0:
            logger.warning(
                "Silver: found %s invalid rows out of %s; filtering them out",
                invalid_count,
                total_count,
            )
            df = df.filter(~invalid)
        else:
            logger.info("Silver: data quality checks passed (%s rows)", total_count)

        logger.info("Silver: transformation complete (%s rows)", df.count())
        return df
    except Exception:
        logger.exception("Silver: failed during transformation")
        raise


def transform_to_gold_trips_by_pickup_zone(silver_df: DataFrame) -> DataFrame:
    try:
        logger.info("Gold: building trips by pickup zone aggregation")

        # Minimal synthetic "zone" dimension (Unity Catalog-safe, no external files).
        spark = silver_df.sparkSession
        zones = (
            spark.range(1, 266)
            .select(
                F.col("id").cast("int").alias("LocationID"),
                F.concat(F.lit("Zone-"), F.lpad(F.col("id").cast("string"), 3, "0")).alias(
                    "ZoneName"
                ),
            )
            .cache()
        )

        df = (
            silver_df.join(
                zones, silver_df.PULocationID == zones.LocationID, how="left"
            )
            .withColumn("pickup_zone", F.coalesce(F.col("ZoneName"), F.lit("Unknown")))
            .groupBy("pickup_date", "pickup_zone")
            .agg(
                F.count("*").alias("trip_count"),
                F.round(F.avg("fare_amount"), 2).alias("avg_fare_amount"),
                F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
            )
        )

        return df
    except Exception:
        logger.exception("Gold: failed to build trips by pickup zone")
        raise


def transform_to_gold_trips_by_hour(silver_df: DataFrame) -> DataFrame:
    try:
        logger.info("Gold: building trips by hour aggregation")
        df = (
            silver_df.groupBy("pickup_date", "pickup_hour")
            .agg(
                F.count("*").alias("trip_count"),
                F.round(F.avg("fare_amount"), 2).alias("avg_fare_amount"),
                F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
                F.round(F.avg("trip_duration"), 2).alias("avg_trip_duration_minutes"),
            )
            .orderBy("pickup_date", "pickup_hour")
        )
        return df
    except Exception:
        logger.exception("Gold: failed to build trips by hour")
        raise


def transform_to_gold_distance_aggregations(silver_df: DataFrame) -> DataFrame:
    try:
        logger.info("Gold: building distance-based aggregations")
        distance_bucket = (
            F.when(F.col("trip_distance") < 2, F.lit("short"))
            .when(F.col("trip_distance") < 10, F.lit("medium"))
            .otherwise(F.lit("long"))
        )

        df = (
            silver_df.withColumn("distance_bucket", distance_bucket)
            .groupBy("pickup_date", "distance_bucket")
            .agg(
                F.count("*").alias("trip_count"),
                F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
                F.round(F.sum("trip_distance"), 2).alias("total_trip_distance"),
                F.round(F.avg("fare_amount"), 2).alias("avg_fare_amount"),
                F.round(F.sum("fare_amount"), 2).alias("total_fare_amount"),
            )
        )
        return df
    except Exception:
        logger.exception("Gold: failed to build distance aggregations")
        raise

