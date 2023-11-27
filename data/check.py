import sqlite3


def connect_to_database(database_path):
    """
    Подключение к базе данных SQLite.
    """
    try:
        connection = sqlite3.connect(database_path)
        print(f"Подключено к базе данных: {database_path}")
        return connection
    except sqlite3.Error as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None


def close_connection(connection):
    """
    Закрытие подключения к базе данных.
    """
    if connection:
        connection.close()
        print("Подключение закрыто")


def fetch_all_rows(cursor):
    """
    Получение всех строк из результата запроса.
    """
    return cursor.fetchall()


def fetch_transactions(connection):
    """
    Извлечение данных из таблицы transactions.
    """
    if connection:
        try:
            cursor = connection.cursor()

            # Пример запроса: выбор всех строк из таблицы transactions
            cursor.execute("SELECT * FROM transactions")

            # Получение всех строк из результата запроса
            rows = fetch_all_rows(cursor)

            # Вывод результатов
            for row in rows:
                print(row)

        except sqlite3.Error as e:
            print(f"Ошибка выполнения запроса: {e}")
        finally:
            if cursor:
                cursor.close()


# Путь к базе данных SQLite
database_path = "transactions_db.db"

# Подключение к базе данных
connection = connect_to_database(database_path)

# Извлечение данных из таблицы transactions
fetch_transactions(connection)

# Закрытие подключения
close_connection(connection)
