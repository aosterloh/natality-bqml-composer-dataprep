# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import airflow
from airflow.operators import SimpleHttpOperator, HttpSensor
from airflow.models import Variable
from airflow.contrib.operators import bigquery_operator
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
    'email': ['a....@google.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily'
}

recipe_id = Variable.get("recipe_id")

def check_dataprep_run_complete(response):
  return response.json()['status'] == 'Complete'

with airflow.DAG(
        'natality_bq_model_v8',
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
    xcom_push=True
  )

  wait_for_dataprep_job_to_complete = HttpSensor(
    task_id='wait_for_dataprep_job_to_complete',
    endpoint='/v4/jobGroups/{{ json.loads(ti.xcom_pull(task_ids="run_dataprep_job"))["id"] }}?embed=jobs.errorMessage',
    headers=headers,
    response_check=check_dataprep_run_complete,
    poke_interval=10
  )

  bigquery_run_sql = bigquery_operator.BigQueryOperator(
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
          `thatistoomuchdata.natality_data_us.natality_data_clean_model_v6`
        WHERE
          weight_pounds IS NOT NULL
          AND RAND() < 0.001
    '''
  )

run_dataprep_task >> wait_for_dataprep_job_to_complete >> bigquery_run_sql
