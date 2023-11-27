from airflow import DAG
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from custom_operators import DownloadCurrencyOperator, DownloadTransactionLogsOperator, MergeDataOperator, LoadToSQLiteOperator

# dag = DAG('dag', schedule_interval=timedelta(days=1), start_date=days_ago(1))
# t1 = DummyOperator(task_id='task_1', dag=dag)
# t2 = DummyOperator(task_id='task_2', dag=dag)
# t3 = DummyOperator(task_id='task_3', dag=dag)
# t4 = DummyOperator(task_id='task_4', dag=dag)
# t5 = DummyOperator(task_id='task_5', dag=dag)
# t6 = DummyOperator(task_id='task_6', dag=dag)
# t7 = DummyOperator(task_id='task_7', dag=dag)

# [t1, t2] >> t5
# t3 >> t6
# [t5, t6] >> t7
# t4

# Задаем аргументы для DAG
default_args = {
    'owner': 'candidate_for_position',
    'start_date': datetime(2021, 1, 1),
    'retries': 1,
}

# Инициализируем DAG
dag = DAG(
    'get_transactions_dag',
    default_args=default_args,  # Передаем аргументы
    description='ETL DAG with CustomOperators',
    # schedule_interval="0 0 * * *", # Настройка интервала
    schedule_interval=None,  # Пока установлен None чтобы он не запускался 900+ раз
)

# Определяем аргументы, которые нужно передать в операторы
date = '2021-01-01'
currency_url = f"https://raw.githubusercontent.com/datanlnja/airflow_course/main/excangerate/{date}.csv"
transaction_logs_url = f"https://raw.githubusercontent.com/datanlnja/airflow_course/main/data/{date}.csv"

# Создаем экземпляры CustomOperator для каждой операции с передачей аргументов
download_currency_task = DownloadCurrencyOperator(
    task_id='download_currency',
    date=date,
    currency_url=currency_url,
    dag=dag,
)

download_transaction_logs_task = DownloadTransactionLogsOperator(
    task_id='download_transaction_logs',
    date=date,
    transaction_logs_url=transaction_logs_url,
    dag=dag,
)

merge_data_task = MergeDataOperator(
    task_id='merge_data',
    date=date,
    dag=dag,
)

load_to_sqlite_task = LoadToSQLiteOperator(
    task_id='load_to_sqlite',
    date=date,
    dag=dag,
)

# Прописываем порядок выполнения задач
download_currency_task >> merge_data_task
download_transaction_logs_task >> merge_data_task
merge_data_task >> load_to_sqlite_task
