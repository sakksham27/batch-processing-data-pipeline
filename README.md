# Olympic Data Batch Processing Pipeline

A scalable batch processing system designed to efficiently process large Olympic datasets through a medallion architecture data warehouse, utilizing Apache Airflow for orchestration and PostgreSQL for storage.

## 🏗️ Architecture Overview

This project implements a **medallion architecture** data warehouse with three distinct layers:

- **Bronze Layer**: Raw, unprocessed data ingestion
- **Silver Layer**: Cleaned, standardized, and formatted data
- **Gold Layer**: Business intelligence tables with key insights and analytics

The pipeline processes **500,000+ rows** of Olympic data across 6 different datasets, utilizing intelligent file partitioning and scheduled batch loading at 5-minute intervals.

## 📊 Data Sources

The pipeline processes six distinct Olympic datasets:

| Dataset | Table Name | Approximate Rows | Description |
|---------|------------|------------------|-------------|
| Athlete Biography | `olympic_athlete_biography` | ~150,000 | Comprehensive athlete profiles and biographical data |
| Event Results | `olympic_event_results` | ~350,000 | Detailed competition results and outcomes |
| Country Profiles | `olympic_country_profiles` | ~250 | National Olympic committee information |
| Athlete Event Details | `olympic_athlete_event_details` | ~21,000 | Individual athlete performance in specific events |
| Games Summary | `olympic_games_summary` | ~70 | Olympic edition overviews and statistics |
| Medal Tally History | `olympic_medal_tally_history` | ~2000 | Historical medal counts by country and year |

## 🔧 Technology Stack

- **Orchestration**: Apache Airflow (via Astro CLI)
- **Database**: PostgreSQL (Dockerized)
- **Processing**: Python with custom transformation scripts
- **Data Transfer**: Pickle files for inter-task communication
- **Containerization**: Docker
- **Environment**: Astro CLI project structure

## 🏛️ Database Schema Architecture

### Bronze Layer (`bronze` schema)
Raw data tables maintaining original structure and format:
- `bronze.olympic_athlete_biography`
- `bronze.olympic_athlete_event_details`
- `bronze.olympic_country_profiles`
- `bronze.olympic_event_results`
- `bronze.olympic_games_summary`
- `bronze.olympic_medal_tally_history`

### Silver Layer (`silver` schema)
Cleaned and standardized tables with:
- Complex date range conversions using PostgreSQL `daterange` data type
- Standardized text formatting and encoding
- Data type normalization
- Referential integrity enforcement

### Gold Layer (`gold` schema)
Business intelligence tables providing key insights:
1. **Top Medal Countries by Edition**: Leading nations in each Olympic year
2. **Country Performance Trends**: Year-over-year medal analysis
3. **Elite Athletes Leaderboard**: Top gold medal winners across all sports
4. **Sport Medal Distribution**: Medal counts by sport and Olympic edition
5. **Athlete Performance Summary**: Individual medal tallies by type
6. **Comprehensive Analytics Views**: Cross-dimensional analysis tables

## 🔄 Pipeline Architecture

### File Processing Strategy

**Year-based Partitioning** (for datasets with year columns):
- Files are split by year to enable parallel processing
- Maintains temporal data integrity

**Percentage-based Partitioning** (for other datasets):
- Large files split into 20% chunks
- Optimizes memory usage and processing time

### DAG Structure

The pipeline utilizes **multiple specialized DAGs**:

1. **Data Ingestion DAGs** (6 DAGs):
   - One DAG per dataset type
   - Handles subdivided file processing
   - Manages bronze layer data loading

2. **Silver Layer Transformation DAGs**:
   - Complex data standardization
   - Advanced date parsing and conversion
   - Text normalization and encoding fixes

3. **Gold Layer Analytics DAG**:
   - Business intelligence table creation
   - Aggregation and insight generation

4. **Data Partitioning DAG**:
   - Orchestrates file subdivision process
   - Manages temporary pickle file lifecycle

### Key Processing Features

#### Advanced Date Standardization
The pipeline implements sophisticated date parsing logic that handles multiple formats:
- Single dates: `DD Month YYYY`
- Date ranges: `DD - DD Month YYYY`
- Complex patterns with varying separators (–, —, −)
- Edge cases and inconsistent formatting

Example transformation:
```sql
-- Complex date range parsing with multiple fallback patterns
CASE
    WHEN result_date ~ '^\d{1,2} * - *\d{1,2} [A-Za-z]+ \d{4}$' THEN 
        daterange(TO_DATE(start_components, 'DD Month YYYY'),
                 TO_DATE(end_components, 'DD Month YYYY'), '[]')
    -- Additional pattern matching logic...
END AS result_date
```

#### Data Quality Assurance
- Comprehensive logging using Python logger library
- Row count validation across all layers
- Data integrity checks between bronze and silver layers

## 🚀 Setup and Deployment

### Prerequisites
- Docker and Docker Compose
- Astro CLI
- Python 3.8+

### Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/sakksham27/batch-processing-data-pipeline.git
   cd batch-processing-data-pipeline
   ```

2. **Initialize Astro project**:
   ```bash
   astro dev init
   ```

3. **Start the environment**:
   ```bash
   astro dev start
   ```

4. **Configure PostgreSQL connection**:
   - Connection ID: `postgres_initial`
   - Setup database schemas: `bronze`, `silver`, `gold`

### Project Structure
```
batch-processing-data-pipeline/
├── dags/
│   ├── bronze_layer_dags/
│   ├── silver_layer_dags/
│   ├── gold_layer_dag/
│   └── partitioning_dag/
├── include/
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## ⚡ Performance Characteristics

- **Total Data Volume**: 500,000+ rows across 6 datasets
- **Processing Frequency**: 5-minute batch intervals
- **File Partitioning**: Intelligent splitting for optimal memory usage
- **Data Pipeline Stages**: 3-layer medallion architecture
- **Parallel Processing**: Multiple specialized DAGs for concurrent execution

## 📈 Business Intelligence Outputs

The gold layer provides actionable insights including:

- **Olympic Performance Analytics**: Country and athlete rankings
- **Historical Trend Analysis**: Medal performance over time
- **Sport-specific Insights**: Competition patterns and dominance
- **Comparative Dashboards**: Cross-country and cross-athlete analysis

## 🔍 Monitoring and Logging

- Comprehensive logging throughout the pipeline
- Row count validation at each processing stage
- Airflow DAG monitoring and alerting
- Data quality checks and validation queries

## 🛠️ Key Technical Achievements

1. **Complex Date Parsing**: Handles 10+ different date formats with robust fallback logic
2. **Scalable File Processing**: Intelligent partitioning for large datasets
3. **Medallion Architecture**: Industry-standard data warehouse design
4. **Automated Orchestration**: Fully automated pipeline with Airflow
5. **Data Quality Assurance**: Multi-layer validation and monitoring

## 📋 Dependencies

Key Python packages (see `requirements.txt` for complete list):
- `apache-airflow`
- `apache-airflow-providers-postgres`
- `pandas`
- `psycopg2-binary`
- `pickle` (built-in)

## 🔧 Configuration

Database connections and pipeline parameters are configured through:
- Airflow Connections (`postgres_initial`)
- Environment variables
- DAG-specific configuration parameters

---

*This pipeline demonstrates advanced data engineering practices including medallion architecture implementation, complex data transformations, and scalable batch processing design.*
