from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator,PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from datetime import datetime
import requests
from airflow.models import Variable
from airflow.operators.email import EmailOperator
import logging
from airflow.models import Variable
import json

arguments = {
    'owner': 'airflow',
    'start_date': datetime(2025, 9, 22),
    'databricks_conn_id':'databricks-connection'
}

def check_nasdaq_is_open():
    """
    Check if NASDAQ market is currently open using Polygon.io API.
    
    Returns:
        str: Task ID to execute next ('etl_execution' if market is open, 
             'nasdaq_its_closed' if market is closed)
    """
    api_key = Variable.get("api_key_polygon")
    url = "https://api.polygon.io/v1/marketstatus/now"
    
    params = {'apikey': api_key}
    
    try:
        response = requests.get(url, params=params)
        data = response.json()
        nasdaq_status = data.get('exchanges', {}).get('nasdaq')
        
        if nasdaq_status == 'open' or nasdaq_status == 'after_hours':
            logging.info('Today NASDAQ market is opened')
            return 'etl_execution'
        else:
            logging.info('Today NASDAQ market is closed')
            return 'nasdaq_its_closed'
    except Exception as e:
        logging.error(f"Error checking market status: {str(e)}")
        # Return closed status on API failure to avoid running ETL on errors
        return 'nasdaq_its_closed'
    

def extract_result_databricks(**context):
    """
    Extract and process results from Databricks notebook execution.
    
    Retrieves the notebook output from the Databricks job run and pushes
    the results to XCom for downstream processing. Also handles period
    reset logic for initial 20-year data load.
    """
    # Reset period to daily after initial 20-year historical load
    period = Variable.get("period_financial_etl")
    if period == '20y':
        Variable.set('period_financial_etl', '1d')
        logging.info('Period reset to daily after initial historical load')

    ti = context['ti']
    
    # Get job run ID from previous task
    job_run_id = ti.xcom_pull(task_ids='etl_execution', key='run_id')
    logging.info(f'Processing Databricks job run: {job_run_id}')
    
    hook = DatabricksHook(databricks_conn_id='databricks-connection')
    run_info = hook.get_run(job_run_id)
    
    # Extract task run ID (required for multi-task job format)
    tasks = run_info.get('tasks', [])
    task = tasks[0]  # Single task job - get first task
    task_run_id = task.get('run_id')
    
    task_output = hook.get_run_output(task_run_id)
    
    if 'notebook_output' in task_output and 'result' in task_output['notebook_output']:
        notebook_result = task_output['notebook_output']['result']
        ti.xcom_push(key='etl_results', value=notebook_result)


def analyze_etl_results(**context):
    """
    Analyze ETL execution results and determine email notification type.
    """
    ti = context['ti']
    # Pull ETL results from previous task
    etl_json_results = ti.xcom_pull(task_ids='extract_result_from_databricks', key='etl_results')
    
    etl_results = json.loads(etl_json_results)
    
    if etl_results['status'] == 'success':
        logging.info('ETL executed successfully')
        return 'good_email_report'
    else:
        logging.info('ETL execution failed')
        return 'bad_email_report'


with DAG(
    dag_id='dag-finantial-etl',
    default_args=arguments,
    schedule="0 20 * * 1-5",
    description='Financial ETL pipeline for NASDAQ companies data extraction and processing') as dag:

    nasdaq_status_check = BranchPythonOperator(
        task_id = 'nasdaq_status_check',
        python_callable=check_nasdaq_is_open
    )

    nasdaq_its_closed = EmptyOperator(
        task_id = 'nasdaq_its_closed'
    )

    etl_execution = DatabricksRunNowOperator(
        task_id = 'etl_execution',
        job_id=Variable.get("job_financial_etl"),
        notebook_params={
        'period': Variable.get("period_financial_etl")
        },
        do_xcom_push=True
    )

    extract_result_from_databricks = PythonOperator(
        task_id = 'extract_result_from_databricks',
        python_callable = extract_result_databricks
    )

    analyze_etl_execution_result = BranchPythonOperator(
        task_id = 'analyze_etl_execution_result',
        python_callable = analyze_etl_results
    )

    good_email_report = EmailOperator(
        task_id = 'good_email_report',
        to=[Variable.get("my_personal_email")],           
        subject='ETL Report - {{ ds }} - Succesfull',           
        html_content='<h1>ETL completed succesfully</h1>'
    )

    bad_email_report = EmailOperator(
        task_id = 'bad_email_report',
        to=[Variable.get("my_personal_email")],           
        subject='ETL Report - {{ ds }} - Failed',           
        html_content='<h1>ETL was failed</h1>'
    )


    nasdaq_status_check >> [nasdaq_its_closed,etl_execution]
    etl_execution >> extract_result_from_databricks >> analyze_etl_execution_result >> [good_email_report,bad_email_report]



