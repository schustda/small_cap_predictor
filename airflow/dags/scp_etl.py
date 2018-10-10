import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from etl.ihub_data import IhubData
from etl.stock_data import StockData
from model.combine_data import CombineData
from model.define_target import DefineTarget
from datetime import datetime, timedelta


ihub = IhubData()
sd = StockData()
cd = CombineData()
dt = DefineTarget()
symbols = ihub.get_dict('symbols')

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2015, 12, 1),
#     'email': ['airflow@example.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
#     'schedule_interval': '@hourly',
# }



dag = DAG(
    dag_id='scp_etl', default_args=args,
    schedule_interval="@daily")

for symbol_id,symbol in symbols.items():
    ihub_etl = PythonOperator(
        task_id=f'{symbol_id:03}_{symbol}_update_posts',
        python_callable=ihub.update_posts,
        op_args=[symbol_id],
        dag=dag
    )

    stock_etl = PythonOperator(
        task_id=f'{symbol_id:03}_{symbol}_update_price_history',
        python_callable=sd.update_stock_data,
        op_args=[symbol_id],
        dag=dag
    )

    combine_data = PythonOperator(
        task_id=f'{symbol_id:03}_{symbol}_combine_data',
        python_callable=cd.compile_data,
        op_args=[symbol_id],
        dag=dag
    )

    define_target = PythonOperator(
        task_id=f'{symbol_id:03}_{symbol}_define_target',
        python_callable=dt.add_target,
        op_args=[symbol_id],
        dag=dag
    )

    stock_etl.set_downstream(combine_data)
    ihub_etl.set_downstream(combine_data)
    combine_data.set_downstream(define_target)
