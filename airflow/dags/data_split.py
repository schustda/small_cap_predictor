import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from model.training_data import TrainingData
from datetime import datetime, timedelta

td = TrainingData()
symbols = td.get_dict('symbols')

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 12, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@hourly',
}

dag = DAG(
    dag_id='data_split', default_args=args,
    schedule_interval="@monthly")

model_development_split = PythonOperator(
task_id='model_development_split',
python_callable=td.model_development_split,
dag=dag
)

create_training_data_train = PythonOperator(
task_id='create_training_data',
python_callable=td.create_training_data,
op_args=['model_development_train'],
dag=dag
)

create_training_data_test = PythonOperator(
task_id='create_test_data',
python_callable=td.create_training_data,
op_args=['model_development_test'],
dag=dag
)

for symbol_id,symbol in symbols.items():
    working_split = PythonOperator(
        task_id=f'{symbol_id:03}_{symbol}__split',
        python_callable=td.working_split,
        op_args=[symbol_id],
        dag=dag
    )

    working_split.set_downstream(model_development_split)

model_development_split.set_downstream(create_training_data_train)
model_development_split.set_downstream(create_training_data_test)
