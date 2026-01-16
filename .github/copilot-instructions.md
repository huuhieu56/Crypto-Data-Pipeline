# GitHub Copilot Instructions for Cinema 360

You are an AI assistant helping a developer build "**Cinema 360**", a local Data Lakehouse project.
The project is a detailed ETL pipeline analyzing movie data from multiple sources.

## 1. Primary Architecture & Constraints
- **Local Execution**: Single host (Linux), 16GB RAM. NO Cloud allowed.
- **Stack**: Docker, Hadoop HDFS, PySpark, PostgreSQL, Streamlit.
- **Resource Limits**: Tune Spark/JVM to <12GB total.

## 2. Technology Standards (Strict Compliance Required)

### Python (General)
- **PEP 8**: Follow 4-space indentation, snake_case for methods/vars, CamelCase for classes.
- **Type Hinting**: MANDATORY for all function signatures (e.g., `def process(data: List[Dict]) -> None:`).
- **Docstrings**: Google Style required for all complex functions.
- **Error Handling**: Use `try-except` blocks with specific exceptions. Log stack traces in error logs.

### SQL (PostgreSQL & SparkSQL)
- **Formatting**: Keywords UPPERCASE, identifiers lowercase/snake_case. Indent SQL for readability.
- **Performance (N+1)**: 
    - **CRITICAL**: Never execute SQL queries inside a loop or a Map function. Batch process everything.
    - Avoid `SELECT *`. Always specify columns.
- **CTEs**: Use `WITH` clause instead of nested subqueries.
- **Date Handling**: Always use ISO-8601 format ('YYYY-MM-DD').

### Apache Spark (PySpark)
- **API**: Use DataFrame API (`pyspark.sql`) exclusively. Avoid RDDs.
- **Shuffling**: Minimize shuffles. Use Broadcast Joins (`broadcast(df_small)`) when joining a large table with a small reference table.
- **UDFs**: Avoid Python UDFs if possible; use built-in SQL functions (`pyspark.sql.functions`) for better performance (Catalyst Optimizer).
- **Caching**: Use `.cache()` only if a DataFrame is reused multiple times. Always `.unpersist()` when done.

### Docker
- **Best Practices**:
    - Use specific tags (e.g., `python:3.9-slim`), never `latest`.
    - Minimize layer count (chain `RUN` commands).
    - Cleanup apt-get cache in the same RUN instruction to save space.
- **Security**: Don't run containers as root if possible (use `USER`).

### Streamlit
- **State Management**: Use `st.session_state` to persist data between re-runs.
- **Performance**: Use `@st.cache_data` for data loading functions to avoid reloading large datasets on every interaction.
- **Blocking**: Do not perform heavy Dataframe operations on the UI thread without a progress bar or background worker.

## 3. The 11 Commandments of Senior Data Engineering

### I. Core Principles
1.  **Idempotency (Rule #1)**:
    - Pipelines must be repeatable with identical output.
    - **Implementation**: Always use `mode("overwrite")` on specific partitions or `DELETE` before `INSERT`. Never blindly append.
2.  **Immutability**:
    - NEVER mutate Raw Data.
    - Always write to a new processed location.
3.  **Schema Evolution**:
    - Use Parquet with schema support.
    - Code must handle backward compatibility.

### II. Optimization (Big Data Mindset)
4.  **Vectorization (NO For-Loops)**:
    - **STRICT PROHIBITION**: Never use Python `for` loops to iterate over data rows.
    - Use Native Spark/Pandas vector operations.
5.  **Lazy Evaluation**:
    - Do NOT call `.collect()` on large DataFrames (risk of OOM).
    - Trigger actions only when necessary (write/show/count).
6.  **Partitioning**:
    - Always partition data by time (e.g., `/year=2025/month=01/day=17`) or logically.
    - Enable "Partition Pruning" in queries.

### III. DataOps & Quality
7.  **Data Quality (Fail Fast)**:
    - Validate inputs (Null checks, negative values) *before* processing.
    - If data is garbage, crash the job early.
8.  **Modular & Decoupled**:
    - strictly separate: **Extract** (Get Data) | **Transform** (Logic) | **Load** (Storage).
9.  **Observability**:
    - Logs must answer: *When started? How many rows? Where did it fail?*
    - Use structured logging (JSON preferred), never print().

### IV. SQL Standards (Advanced)
10. **Order of Execution**: Filter early (`WHERE`) before Aggregation (`GROUP BY`).
11. **CTEs over Subqueries**: ALWAYS use Common Table Expressions (`WITH ... as`) instead of nested subqueries.

## 4. Specific Prohibitions
- NEVER suggested cloud-native solutions (S3, Glue).
- NEVER assume infinite RAM.
- NEVER suggest utilizing `pandas` for handling Big Data transformations (Use PySpark).
