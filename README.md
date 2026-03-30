# Databricks Medallion (Bronze → Silver → Gold) — Synthetic NYC Taxi

Production-ready, Unity Catalog compatible Databricks project that generates synthetic NYC Taxi data using PySpark and persists Bronze/Silver/Gold Delta tables as **managed Unity Catalog tables** (no DBFS paths, no external datasets).

## Structure

- `config/`: table and schema config
- `src/`: ingestion, transformation, and write utilities
- `pipeline/etl_pipeline.py`: end-to-end Bronze→Silver→Gold pipeline entrypoint
- `databricks.yml`: Declarative Automation Bundle (job uses **serverless compute**, scheduled daily 6AM Asia/Kolkata)
- `.github/workflows/deploy.yml`: CI/CD deploy on push to `main`

## Run locally (imports / unit tests)

```bash
pytest -q
```

## Run on Databricks

```bash
databricks bundle validate
databricks bundle deploy --target dev
```

The deployed job runs **only on schedule** (daily at 06:00 Asia/Kolkata) unless triggered manually in the Jobs UI.

