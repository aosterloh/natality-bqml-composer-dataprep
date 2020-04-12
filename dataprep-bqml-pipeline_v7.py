import airflow
from airflow.operators import SimpleHttpOperator, HttpSensor
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import datetime, timedelta
import json

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),datetime.min.time())

headers = {
  "Content-Type": "application/json",
  "Authorization": Variable.get("trifacta_bearer")
}

default_args = {
    'owner': 'Alex Osterloh',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['aosterloh@google.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}

recipe_id = Variable.get("recipe_id")

def check_dataprep_run_complete(response):
  return response.json()['status'] == 'Complete'

with airflow.DAG(
        'natality_bq_model_v7',
        default_args=default_args,
        # Not scheduled, trigger only
        schedule_interval=None,
        user_defined_macros={
          'json': json
        }
) as dag:

  run_dataprep_task = SimpleHttpOperator(
    task_id='run_dataprep_job',
    endpoint='/v4/jobGroups',
    data=json.dumps({"wrangledDataset": {"id": int(recipe_id)}}),
    headers=headers,
    xcom_push=True,
    dag=dag,
  )

  wait_for_dataprep_job_to_complete = HttpSensor(
    task_id='wait_for_dataprep_job_to_complete',
    endpoint='/v4/jobGroups/{{ json.loads(ti.xcom_pull(task_ids="run_dataprep_job"))["id"] }}?embed=jobs.errorMessage',
    headers=headers,
    response_check=check_dataprep_run_complete,
    poke_interval=10,
    dag=dag,
    )

  bigquery_run_sql = BigQueryOperator(
    task_id='bq_run_sql',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    sql='''
        CREATE OR REPLACE MODEL `thatistoomuchdata.natality_bqml.natality_model_v7`
        OPTIONS
          (model_type='linear_reg', input_label_cols=['weight_pounds']) AS
        SELECT
          weight_pounds,
          is_male,
          gestation_weeks,
          mother_age,
          CAST(mother_race AS string) AS mother_race
        FROM
          `thatistoomuchdata.natality_data_us.natality_data_clean_model_v7`
        WHERE
          weight_pounds IS NOT NULL
          AND RAND() < 0.001
    ''',
    dag=dag,
    )

run_dataprep_task >> wait_for_dataprep_job_to_complete >> bigquery_run_sql