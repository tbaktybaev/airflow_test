from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import sqlite3

# Функция для загрузки валюты в CSV


def download_currency_to_csv(date, currency_url, **kwargs):
    """
    Загружает данные о валюте из CSV по указанной дате и сохраняет в файл.

    :param date: Дата в формате 'гггг-мм-дд'.
    :param currency_url: URL для загрузки данных о валюте.
    """
    try:
        currency_data = pd.read_csv(currency_url)
        currency_data.to_csv(f"./data/currency_{date}.csv", index=False)
    except Exception as e:
        print(f"Ошибка при загрузке валюты в CSV: {e}")

# Функция для загрузки логов транзакций в CSV


def download_transaction_logs_to_csv(date, transaction_logs_url, **kwargs):
    """
    Загружает логи транзакций из CSV по указанной дате и сохраняет в файл.

    :param date: Дата в формате 'гггг-мм-дд'.
    :param transaction_logs_url: URL для загрузки логов транзакций.
    """
    try:
        transaction_logs_data = pd.read_csv(transaction_logs_url)
        transaction_logs_data.to_csv(
            f"./data/transaction_logs_{date}.csv", index=False)
    except Exception as e:
        print(f"Ошибка при загрузке логов транзакций в CSV: {e}")
# Функция для объединения данных


def merge_data(date, **kwargs):
    """
    Объединяет данные о валюте и логи транзакций по указанной дате и сохраняет в файл.

    :param date: Дата в формате 'гггг-мм-дд'.
    """
    try:
        currency_data = pd.read_csv(f"./data/currency_{date}.csv")
        transaction_logs_data = pd.read_csv(
            f"./data/transaction_logs_{date}.csv")
        merged_data = pd.merge(currency_data, transaction_logs_data, on='date')
        merged_data.to_csv(f"./data/merged_data_{date}.csv", index=False)
    except Exception as e:
        print(f"Ошибка при объединении данных: {e}")

# Функция для загрузки в SQLite


def load_to_sqlite(date, **kwargs):
    """
    Загружает объединенные данные в базу данных SQLite по указанной дате.

    :param date: Дата в формате 'гггг-мм-дд'.
    """
    try:
        merged_data = pd.read_csv(f"./data/merged_data_{date}.csv")
        conn = sqlite3.connect("./data/transactions_db.db")
        merged_data.to_sql('transactions', conn,
                           index=False, if_exists='replace')
        print("Данные успешно загружены в SQLite.")
    except Exception as e:
        print(f"Ошибка при загрузке данных в SQLite: {e}")
    finally:
        conn.close()

# CustomOperator для выполнения загрузки валюты


class DownloadCurrencyOperator(BaseOperator):
    """
    CustomOperator для выполнения загрузки данных о валюте.

    :param date: Дата в формате 'гггг-мм-дд'.
    :param currency_url: URL для загрузки данных о валюте.
    """
    @apply_defaults
    def __init__(self, date, currency_url, *args, **kwargs):
        super(DownloadCurrencyOperator, self).__init__(*args, **kwargs)
        self.date = date
        self.currency_url = currency_url

    def execute(self, context):
        download_currency_to_csv(self.date, self.currency_url)

# CustomOperator для выполнения загрузки логов транзакций


class DownloadTransactionLogsOperator(BaseOperator):
    """
    CustomOperator для выполнения загрузки логов транзакций.

    :param date: Дата в формате 'гггг-мм-дд'.
    :param transaction_logs_url: URL для загрузки логов транзакций.
    """
    @apply_defaults
    def __init__(self, date, transaction_logs_url, *args, **kwargs):
        super(DownloadTransactionLogsOperator, self).__init__(*args, **kwargs)
        self.date = date
        self.transaction_logs_url = transaction_logs_url

    def execute(self, context):
        download_transaction_logs_to_csv(self.date, self.transaction_logs_url)

# CustomOperator для выполнения объединения данных


class MergeDataOperator(BaseOperator):
    """
    CustomOperator для выполнения объединения данных.

    :param date: Дата в формате 'гггг-мм-дд'.
    """
    @apply_defaults
    def __init__(self, date, *args, **kwargs):
        super(MergeDataOperator, self).__init__(*args, **kwargs)
        self.date = date

    def execute(self, context):
        merge_data(self.date)

# CustomOperator для выполнения загрузки в SQLite


class LoadToSQLiteOperator(BaseOperator):
    """
    CustomOperator для выполнения загрузки данных в SQLite.

    :param date: Дата в формате 'гггг-мм-дд'.
    """
    @apply_defaults
    def __init__(self, date, *args, **kwargs):
        super(LoadToSQLiteOperator, self).__init__(*args, **kwargs)
        self.date = date

    def execute(self, context):
        load_to_sqlite(self.date)
