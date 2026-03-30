def test_imports():
    from config.databricks_config import PipelineConfig  # noqa: F401
    from config.schema_config import nyc_taxi_bronze_schema  # noqa: F401
    from pipeline.etl_pipeline import run  # noqa: F401
    from src.ingestion.read_data import generate_bronze_nyc_taxi_df  # noqa: F401
    from src.transformation.transform_data import transform_to_silver  # noqa: F401
    from src.write.write_data import write_delta_table  # noqa: F401

