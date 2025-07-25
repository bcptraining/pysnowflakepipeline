# ❄️ SnowPipeline Project

A modular, schema-aware ingestion pipeline built for Snowflake that loads Avro-formatted user data into a target table with column normalization, validation, audit logging, and structured reject tracking.

---

## 🎯 Purpose

This project was designed as a hands-on learning exercise to complement my existing experience with native Snowflake tools—such as Snowpipe, streams and tasks, and dynamic tables—by building a modular ingestion pipeline using Python and Snowpark.

Rather than replicating Snowflake’s managed flows, my goal was to explore the architecture, orchestration, validation patterns, and **observability principles** behind a custom-built ingestion framework:

- 🔧 **Modular Python Design** — Structured with clear separation across ingestion, transformation, validation, and writeback layers.
- 📦 **Pyproject-Based Packaging** — Built as a Python package with dependency management, entry points, and clean directory layout.
- 🧬 **Schema Registry Integration** — Implements a versioned registry for config and schema alignment.
- 🧪 **Layered Validation Logic** — Ensures audit-grade reliability through multiple column checks.
- 🧊 **Snowflake Session Auditing** — Context-managed sessions log usage metadata to `SESSION_AUDIT`.
- 📈 **Observability Features** — Structured logging, reject tracking, and summary outputs for full pipeline visibility.
- 🖥️ **CLI-Compatible Config Loading** — Pipeline runner supports configuration overrides via command-line arguments or environment variables.

> ⚠️ **Note on Transformations**  
> Transformations are intentionally minimal—limited to column projection and renaming—since the use case focused on staging conformed data for downstream consumption. This keeps the pipeline focused, auditable, and extensible without unnecessary complexity.

This project showcases Python engineering skills, Snowflake platform fluency, and architectural awareness when designing extensible, observable data pipelines.  

## 🏗️ Infrastructure Setup

Before running the pipeline, ensure the required Snowflake resources exist. You can bootstrap them by executing the provided script:

### 📜 `setup_snowflake_resources.sql`

This script creates:

- `DEMO_DB`: Database to host pipeline tables  
- `EMP_DETAILS_AVRO_CLS`: Target table for ingested records  
- `EMPLOYEE_AVRO_REJECTS`: Table for capturing rejected records 
- `SESSION_AUDIT`: Table for logging Snowflake sessions and user activity   
- `raw_data_stage`: External stage pointing to S3 `snowflakesmpdata` bucket

---

## 🧭 Pipeline Architecture (Simplified)

```
           +-----------------------+
           |     Source File       |
           |  (Avro on S3/Stage)   |
           +-----------------------+
                      |
                      ▼
           +-----------------------+
           |  Ingestion & Mapping  |   ← column normalization
           +-----------------------+
                      |
                      ▼
           +-----------------------+
           |   Internal Stage CSV  |   ← intermediate write
           +-----------------------+
                      |
                      ▼
           +-----------------------+
           |    COPY INTO Target   |   ← final table ingestion
           +-----------------------+
                      |
                      ▼
           +-----------------------+
           |   Reject Collection   |   ← via VALIDATE(job_id)
           +-----------------------+
```

<sub>🧱 *This pipeline uses a temporary internal stage to write and read intermediate files before final ingestion. This pattern supports row-count verification, retry-safe operations, and separation of transformation from ingestion logic—adding a layer of reliability and observability to the write path.*</sub>

---

## 📁 Project Structure

```
snow_pipelineproject/
├── README.md
├── __init__.py
├── dashboard
│   ├── dashboard.py
│   ├── pipeline_run_history.json
│   ├── pipeline_summary.json
├── environment.yml
├── logs
│   ├── pipeline.log
│   ├── pipeline_v3.log
├── project_structure.txt
├── pyproject.toml
├── requirements.txt
├── setup.py
├── setup_snowflake_resources.sql
├── snow_pipeline_pkg
│   ├── __init__.py
│   ├── __main__.py
│   ├── config
│   │   ├── connection_details.json
│   │   ├── copy_to_snowstg_avro_emp_details_avro_cls.json
│   │   ├── schemas.py
│   │   ├── snowflake_private_key.p8
│   ├── ingest
│   │   ├── __init__.py
│   │   ├── avro_loader.py
│   ├── pipeline_runner.py
│   ├── transform
│   │   ├── __init__.py
│   │   ├── schema_mapper.py
│   ├── utils
│   │   ├── __init__.py
│   │   ├── config_loader.py
│   │   ├── connection_loader.py
│   │   ├── log_setup.py
│   │   ├── snowflake_session.py
│   │   ├── validators
│   ├── validate
│   │   ├── __init__.py
│   │   ├── quality_checks.py
│   ├── writeback
│   │   ├── stage_writer.py
├── snow_pipeline_pkg.egg-info
│   ├── dependency_links.txt
│   ├── top_level.txt
├── tests
│   ├── conftest.py
│   ├── core
│   │   ├── test_parse_args.py
│   ├── validate
│   │   ├── test_column_overlap.py
│   │   ├── test_connection_loader.py
│   │   ├── test_schema_matching.py
├── tree.py
```
---

## ⚙️ Pipeline Configuration

### `copy_to_snowstg_avro_emp_details_avro_cls.json`

```jsonc
{
  "database_name": "DEMO_DB",
  "schema_name": "PUBLIC",
  "target_table": "EMP_DETAILS_AVRO_CLS",
  "reject_table": "EMPLOYEE_AVRO_REJECTS",
  "source_location": "@my_s3_stage/Avro_folder/userdata1.avro",
  "source_file_type": "avro",
  "log_file": "logs/pipeline.log",
  "allow_partial_schema": true,
  "validate_target_columns_against_schema": "emp_details_avro_cls@v1.0",
  "target_columns": [
    "REGISTRATION", "USER_ID", "FIRST_NAME", "LAST_NAME", "USER_EMAIL"
  ],
  "map_columns": {
    "REGISTRATION_DTTM": "REGISTRATION",
    "ID": "USER_ID",
    "FIRST_NAME": "FIRST_NAME",
    "LAST_NAME": "LAST_NAME",
    "EMAIL": "USER_EMAIL"
  }
}
```

---

## 🧪 Validation

Located in `validate/quality_checks.py`

- `validate_schema_matches_table()` checks actual columns vs config
- `validate_config_against_schema_ref()` compares config to registry schema
- Registry defined in `schemas.py`:

```python
schema_registry = {
  "emp_details_avro_cls": {
    "v1.0": StructType([...])
  }
}

```

---

## 🧼 Transformation

Located in `transform/schema_mapper.py`

- `apply_column_mapping()` renames based on config
- `drop_unmapped_columns()` prunes excess columns

---

## 🧊 Snowflake Session Management

Located in `utils/snowflake_session.py`

- `managed_snowflake_session()` handles lifecycle
- Writes audit metadata to `SESSION_AUDIT` table  
  - Session ID, User, Warehouse, Role, Created/Closed timestamps

---

## 📤 Writeback & Staging

Located in `writeback/stage_writer.py`

- Writes DataFrame to temp internal stage as CSV
- Reads back for `COPY INTO` retry-safe ingestion
- Collects rejects via Snowflake’s `VALIDATE()` using query ID
- Saves rejects to configured reject table

---

## 🗃️ Reject Table Structure

| Field            | Description               |
|------------------|---------------------------|
| `ROW_STATUS`     | Row rejection status  
| `ERROR_MESSAGE`  | Description of error  
| `RAW_ROW`        | Raw content of failed row  
| `TARGET_TABLE`   | Destination table  
| `SCHEMA_REF`     | Schema used for validation  

---

## 📊 Summary Output

Saved to: `dashboard/pipeline_summary.json`

```jsonc
{
  "start_time": "...",
  "end_time": "...",
  "duration_sec": 1.2,
  "files_loaded": 1,
  "rows_inserted": 542,
  "rows_rejected": 8,
  "query_id": "01af0e7c...",
  "rejects": {
    "count": 8,
    "columns": [...],
    "sample": [...]
  }
}
```

---

## 🛡️ Environment Validation

Located in `validators/environment_validator.py`

- Ensures presence of:
  - `pandas`
  - `snowflake.connector`
  - `snowflake.snowpark`
- Logs missing dependencies and exits

---

## 📣 Logging Setup

Located in `log_utils.py`

- `SafeConsoleHandler` prevents Unicode console issues  
- `setup_logger()` supports console + file output  
- Writes logs to configurable path

---

## 🧠 Utilities

- `config_loader.py` – loads JSON configs  
- `connection_loader.py` – injects RSA key into Snowflake connection  
- `log_setup.py` – alternate logger initializer used in entry point

---

## 🏁 Running the Pipeline
The pipeline runner supports command-line arguments for customizing execution. Here are the available options:
### ▶️ Default Execution (from repo root):

```bash
python snow_pipeline_pkg/pipeline_runner.py
```

### ⚙️ Override Connection and COPY Configs
You can override both configuration files by providing paths explicitly:

```bash
python snow_pipeline_pkg/pipeline_runner.py \
  --connection-config snow_pipeline_pkg/config/connection_details.json \
  --copy-config snow_pipeline_pkg/config/copy_to_snowstg_avro_emp_details_avro_cls.json
```

### 🐛 Enable Verbose Logging
Useful for debugging and tracing:

```bash
python snow_pipeline_pkg/pipeline_runner.py --verbose
```

### 📘 Available Arguments

| Argument               | Description                                                       | Default                                                                 |
|------------------------|-------------------------------------------------------------------|-------------------------------------------------------------------------|
| `--connection-config`  | Path to Snowflake connection config JSON                          | Env `SNOWFLAKE_CONFIG` or `snow_pipeline_pkg/config/connection_details.json` |
| `--copy-config`        | Path to COPY INTO config JSON                                     | Env `COPY_CONFIG` or `snow_pipeline_pkg/config/copy_to_snowstg_avro_emp_details_avro_cls.json` |
| `--verbose`            | Enables debug-level logging                                       | `False`                                                                 |

> Argument parsing is handled via Python's `argparse`, with optional environment variable support for flexible configuration.


---

## 🌱 Future Enhancements

- YAML support for configs 
- Support additional source file types (beyond AVRO)
- Multi-target orchestration  
- DAG or task scheduler integration  
- Schema diffing and audit mode  
- Dashboard visualizations from summary  

---

## 📄 License

MIT
