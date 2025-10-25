# dagster_pipeline

## Getting started

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)


## AWS commands
aws s3api create-bucket --bucket nk-data-lake --region us-west-1 --create-bucket-configuration LocationConstraint=us-west-1

aws s3api put-bucket-versioning --bucket nk-data-lake --versioning-configuration Status=Enabled
aws s3api put-bucket-encryption --bucket nk-data-lake --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

aws s3 ls s3://nk-data-lake/raw-data/waves/


DBT
dbt run --project-dir ./dbt -s stg_wave_open_meteo


duckdb /Users/nkandler/projects/dagster/data/waves.duckdb

duckdb commands
duckdb /Users/nkandler/projects/dagster/data/waves.duckdb
-- list all tables (current schema)
.tables
-- list schemas
SELECT schema_name FROM information_schema.schemata ORDER BY 1;
-- list all tables across schemas
SELECT table_schema, table_name, table_type
FROM information_schema.tables
ORDER BY 1,2;
-- list tables in schema 'waves'
SELECT table_name FROM information_schema.tables
WHERE table_schema='waves' AND table_type='BASE TABLE';
-- list views in schema 'waves'
SELECT table_name FROM information_schema.tables
WHERE table_schema='waves' AND table_type='VIEW';

SELECT * FROM raw.open_meteo ORDER BY timestamp DESC LIMIT 5;


## Scheduling nightly runs

This project defines a nightly schedule that materializes all assets.

- Job: `nightly_assets`
- Schedule: `nightly_assets_schedule` (defaults to 00:00 daily)

### Enable in local dev (recommended)

1. Start the web UI and embedded daemon:

   ```bash
   dg dev
   ```

2. Open `http://localhost:3000` and navigate to Schedules. Toggle on `nightly_assets_schedule`.

3. To test immediately without waiting, launch the job once:

   ```bash
   dg job launch -j nightly_assets
   ```

### Enable in a long-running process (production-like)

Run the webserver and daemon as separate processes (or services):

```bash
dagster-webserver -h 0.0.0.0 -p 3000
```

```bash
dagster-daemon run
```

Then toggle `nightly_assets_schedule` on in the UI.

### Customize schedule time and timezone

You can override the cron and timezone via environment variables:

```bash
export NIGHTLY_CRON="0 0 * * *"           # default: midnight daily
export SCHEDULE_TZ="America/Los_Angeles"  # default: UTC
```

Common examples:

- Run at 2:30am local time: `NIGHTLY_CRON="30 2 * * *"`
- Run every day at 1:00am UTC: leave defaults and toggle schedule on.

## Using MotherDuck instead of local DuckDB

This project can write and query to MotherDuck.

### Python asset (Open Meteo) to MotherDuck

Set environment variables before running:

```bash
export MOTHERDUCK_TOKEN="<your_token>"   # obtained from MotherDuck
export MOTHERDUCK_DB="waves"             # database name in MotherDuck
```

The asset will connect to `md:${MOTHERDUCK_DB}` automatically when `MOTHERDUCK_DB` is set. Clear it to revert to local DuckDB.

### dbt profile for MotherDuck

Update `dbt/profiles.yml` to use the `motherduck` output (see `dbt/profiles.sample.yml`):

```yaml
waves:
  target: motherduck
  outputs:
    motherduck:
      type: duckdb
      path: md:waves
      schema: waves
      threads: 4
```

Make sure `MOTHERDUCK_TOKEN` is available in the environment for dbt as well.
