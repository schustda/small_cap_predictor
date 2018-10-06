import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from etl.ihub_data import IhubData
from etl.stock_data import StockData
from model.combined_data import CombineData



ihub = IhubData()
sd = StockData()
cd = CombineData()
symbols = ihub.get_dict('symbols')

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='scp_etl', default_args=args,
    schedule_interval=None)

for symbol_id,symbol in symbols.items():
    ihub_etl = PythonOperator(
        task_id='{0}_update_posts'.format(symbol),
        python_callable=ihub.update_posts,
        op_args=[symbol_id],
        dag=dag
    )

    stock_etl = PythonOperator(
        task_id='{0}_update_price_history'.format(symbol),
        python_callable=sd.update_stock_data,
        op_args=[symbol_id],
        dag=dag
    )

    combine_data = PythonOperator(
        task_id='{0}_combine_data'.format(symbol),
        python_callable=cd.compile_data,
        op_args=[symbol_id],
        dag=dag
    )

    combine_data.set_upstream(stock_etl)
    combine_data.set_upstream(ihub_etl)
