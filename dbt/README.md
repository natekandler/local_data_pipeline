waves dbt project (Core)

This dbt Core project materializes processed wave data from raw Delta payloads written by Dagster.

Prereqs
- Python + dbt adapter (choose one):
  - Databricks: pip install dbt-databricks
  - Spark (e.g., EMR/Glue/Local Spark): pip install dbt-spark[PyHive]
- Access to the raw Delta table written by Dagster at s3://$RAW_BUCKET/$RAW_PREFIX
- AWS credentials and region configured (if reading directly from S3)

Project layout
- models/sources.yml: Defines raw.open_meteo source pointing to your Delta location
- models/wave_data_processed.sql: Explodes JSON payload into hourly rows
- dbt_project.yml: Basic dbt project config

Configure profile
Create ~/.dbt/profiles.yml with a profile named waves (referenced in dbt_project.yml).

Databricks example
waves:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: hive_metastore
      schema: waves
      host: https://<your-workspace>.cloud.databricks.com
      http_path: /sql/1.0/warehouses/<warehouse-id>
      token: {{ env_var('DATABRICKS_TOKEN') }}
      threads: 4

Spark (Thrift) example
waves:
  target: dev
  outputs:
    dev:
      type: spark
      method: thrift
      host: localhost
      port: 10001
      schema: waves
      threads: 4

Environment variables used
- RAW_BUCKET and RAW_PREFIX to point the source at the Delta path
- For Databricks: DATABRICKS_TOKEN
- For AWS/Spark: typical AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_REGION

Run
export RAW_BUCKET=my-data-lake
export RAW_PREFIX=raw-data/waves

# Install adapter, e.g. Databricks
pip install dbt-databricks

# Validate connection
dbt debug --project-dir ./dbt --profiles-dir ~/.dbt

# Build model
dbt run --project-dir ./dbt -s wave_data_processed

Notes
- If your adapter supports path-based sources (Databricks, Spark), the meta.location + meta.format hints will be used by macros to read Delta at the S3 path.
- Alternatively, register the Delta path as a table in your metastore and remove meta from sources.yml, then reference it normally.

