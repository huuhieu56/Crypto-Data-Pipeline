# Cinema 360 - Data Intelligence Platform

A local Big Data ETL pipeline for movie analytics using Hadoop, Spark, and PostgreSQL.

## Project Structure

```
cinema360-etl/
├── src/                    # Main source code
│   ├── config/             # Configuration management
│   ├── extract/            # Data ingestion (IMDb, TMDB, MySQL)
│   ├── transform/          # PySpark transformations
│   ├── load/               # Data loading (HDFS, PostgreSQL)
│   ├── quality/            # Data quality validators
│   └── utils/              # Shared utilities
├── jobs/                   # ETL job entry points
├── dashboard/              # Streamlit visualization app
├── docker/                 # Docker configurations
├── sql/                    # SQL schemas
├── tests/                  # Unit and integration tests
└── docs/                   # Documentation
```

## Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    src/ (Core Business Logic)                                   │
│  ┌──────────┐    ┌───────────┐    ┌──────┐    ┌─────────┐    ┌────────┐    ┌────────┐          │
│  │ extract/ │    │transform/ │    │load/ │    │quality/ │    │utils/  │    │config/ │          │
│  └──────────┘    └───────────┘    └──────┘    └─────────┘    └────────┘    └────────┘          │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
        │                 │              │             │
        v                 v              v             v
=====================================================================================================
                                    ETL PIPELINE LAYERS
=====================================================================================================

┌─────────────────┐
│  DATA SOURCES   │
├─────────────────┤         jobs/run_ingestion.py
│ - IMDb Datasets │         calls: src/extract/* -> src/load/hdfs_loader.py
│ - TMDB API      │─────────────────────────────────────────────────────────┐
│ - MySQL Logs    │                                                         │
└─────────────────┘                                                         v
                                              ┌─────────────────────────────────────────────────┐
                                              │              DATA LAKE (HDFS)                   │
                                              │  ┌─────────────────────────────────────────┐    │
                                              │  │  Raw Zone: /data/raw/                   │    │
                                              │  │  - /imdb/*.tsv.gz   (Unprocessed)       │    │
                                              │  │  - /tmdb/*.json     (Unprocessed)       │    │
                                              │  │  - /internal/*.csv  (Unprocessed)       │    │
                                              │  └─────────────────────────────────────────┘    │
                                              │                        │                        │
                                              │  jobs/run_processing.py                        │
                                              │  calls: src/transform/* + src/quality/*        │
                                              │                        v                        │
                                              │  ┌─────────────────────────────────────────┐    │
                                              │  │  Processed Zone: /data/processed/       │    │
                                              │  │  - Cleaned & Joined Parquet files       │    │
                                              │  └─────────────────────────────────────────┘    │
                                              └─────────────────────────────────────────────────┘
                                                                       │
                                                                       │ src/load/postgres_loader.py
                                                                       v
                                              ┌─────────────────────────────────────────────────┐
                                              │            DATA WAREHOUSE (PostgreSQL)          │
                                              │  ┌─────────────────────────────────────────┐    │
                                              │  │  Fact Table: movies_analytics           │    │
                                              │  │  - Aggregated metrics (Profit, ROI)     │    │
                                              │  │  - Joined from all sources              │    │
                                              │  └─────────────────────────────────────────┘    │
                                              └─────────────────────────────────────────────────┘
                                                                       │
                                                                       v
                                              ┌─────────────────────────────────────────────────┐
                                              │              DATA MART (Views/Queries)          │
                                              │  ┌─────────────────────────────────────────┐    │
                                              │  │  - Top Movies by ROI                    │    │
                                              │  │  - Genre Trends by Year                 │    │
                                              │  │  - Overrated Movies (Rating Diff)       │    │
                                              │  └─────────────────────────────────────────┘    │
                                              └─────────────────────────────────────────────────┘
                                                                       │
                                                                       v
                                              ┌─────────────────────────────────────────────────┐
                                              │             VISUALIZATION (Streamlit)           │
                                              │  dashboard/app.py                               │
                                              │  - pages/01_overview.py                         │
                                              │  - pages/02_analysis.py                         │
                                              │  - components/charts.py (Plotly)                │
                                              └─────────────────────────────────────────────────┘
```

### Phase 1: Data Ingestion (`jobs/run_ingestion.py`)

| Step | Module | Function | Output |
|------|--------|----------|--------|
| 1a | `src/extract/imdb_extractor.py` | `download_imdb_datasets()` | TSV files |
| 1b | `src/extract/tmdb_extractor.py` | `download_daily_id_export()` -> `fetch_movie_details()` | JSON files |
| 1c | `src/extract/internal_extractor.py` | `extract_user_reviews()` | CSV files |
| 2 | `src/load/hdfs_loader.py` | `upload_to_hdfs()` | Files in `/data/raw/` |

### Phase 2: Data Processing (`jobs/run_processing.py`)

| Step | Module | Function | Description |
|------|--------|----------|-------------|
| 1 | `src/utils/spark_session.py` | `get_spark_session()` | Initialize Spark |
| 2 | `src/quality/validators.py` | `check_not_empty()`, `check_no_nulls()` | Validate raw data |
| 3 | `src/transform/cleaning.py` | `filter_adult_content()`, `filter_missing_year()` | Clean data |
| 4 | `src/transform/integration.py` | `join_imdb_ratings()`, `join_with_tmdb()` | Merge datasets |
| 5 | `src/transform/metrics.py` | `calculate_profit()`, `calculate_roi()` | Business metrics |
| 6 | `src/load/postgres_loader.py` | `write_to_postgres()` | Load to warehouse |

### Phase 3: Visualization (`dashboard/app.py`)

| Module | Function | Description |
|--------|----------|-------------|
| `dashboard/app.py` | Main entry | Streamlit app |
| `dashboard/pages/01_overview.py` | Overview stats | Summary metrics from PostgreSQL |
| `dashboard/pages/02_analysis.py` | Charts | Plotly visualizations |
| `dashboard/components/charts.py` | Reusable charts | `create_scatter_budget_revenue()` |

### Shared Utilities

| Module | Used By | Purpose |
|--------|---------|---------|
| `src/config/settings.py` | All modules | Environment-based configuration |
| `src/utils/logging_config.py` | All modules | Structured JSON logging |
| `src/utils/spark_session.py` | `transform/`, `load/` | Spark session factory |

## Quick Start

1. **Setup Environment**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Start Infrastructure**
   ```bash
   docker-compose -f docker/docker-compose.yml up -d
   ```

4. **Run ETL Pipeline**
   ```bash
   # Ingestion
   python jobs/run_ingestion.py

   # Processing
   python jobs/run_processing.py
   ```

5. **Launch Dashboard**
   ```bash
   streamlit run dashboard/app.py
   ```

## Data Sources

- **IMDb**: Movie metadata and ratings
- **TMDB**: Financial data (budget, revenue)
- **Internal**: User reviews from MySQL

## Tech Stack

- **Storage**: Hadoop HDFS
- **Processing**: Apache Spark (PySpark)
- **Warehouse**: PostgreSQL
- **Visualization**: Streamlit + Plotly
- **Infrastructure**: Docker

## Documentation

See [docs/ProjectOverview.md](docs/ProjectOverview.md) for detailed project documentation.

## Team

See project documentation for team roles and responsibilities.
