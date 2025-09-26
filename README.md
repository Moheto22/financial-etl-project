# Financial ETL Pipeline

A production-ready ETL pipeline for extracting and processing financial data from NASDAQ companies using Apache Airflow, Databricks, and AWS S3.

## Technology Stack

- **Orchestration**: Apache Airflow
- **Processing**: Databricks
- **Storage**: AWS S3 + Delta Lake
- **APIs**: Yahoo Finance, Polygon.io

## Key Features

- Market-aware scheduling (only runs when NASDAQ is open)
- Data quality validation and error tracking
- Automated email notifications
- Delta Lake integration with schema evolution
- Cost-effective Databricks job clusters

## Project Structure

```
financial-etl-project/
├── dags/
│   └── dag_financial_etl.py
└── notebooks/
    └── financial_etl.ipynb
```

## Configuration

### Airflow Variables
- `api_key_polygon`: Polygon.io API key
- `job_financial_etl`: Databricks job ID
- `period_financial_etl`: Data period (1d, 1mo, etc.)
- `my_personal_email`: Notification email

### Airflow Connections
- `databricks-connection`: Databricks workspace connection
- `smtp_default`: Email SMTP configuration

## Data Pipeline

1. **Market Validation**: Check if NASDAQ is open
2. **Data Extraction**: Download financial data via Yahoo Finance
3. **Data Quality**: Validate schema and check for nulls
4. **Transformation**: Calculate financial metrics (Range %, Variation %)
5. **Loading**: Store in Delta Lake format on S3
6. **Notification**: Send success/failure email

## Setup

1. Configure Databricks job with notebook
2. Set up S3 bucket and folder structure
3. Add DAG to Airflow dags folder
4. Configure variables and connections

## Monitoring

- Airflow UI for task status
- Databricks UI for processing logs
- Email notifications for execution status

## Scheduling

Runs weekdays at 8 PM UTC (`0 20 * * 1-5`), only when market is open.

---
