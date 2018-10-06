import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from etl.ihub_data import IhubData
from etl.stock_data import StockData
from model.combined_data import CombineData



ihub = IhubData()
symbol_ids = ihub.get_list('symbol_ids')

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

ihub_dag = DAG(
    dag_id='ihub_etl', default_args=args,
    schedule_interval=None)

for symbol_id in symbol_ids:
    task = PythonOperator(
        task_id='update_posts_{0}'.format(symbol_id),
        python_callable=ihub.update_posts,
        op_args=[symbol_id],
        dag=ihub_dag
    )
