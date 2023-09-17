"""Библиотека для работы с БД"""
from dotenv import load_dotenv
import os
import psycopg2
from ms_training_system.common import lib_logger as log

load_dotenv()


class Connector(log.LoggerMixin):
    """Функции для выполнения запроса, коммита, отключения от БД"""
    def __init__(self):
        super().__init__()
        self.connect = psycopg2.connect(
            host=os.getenv('DbHost'),
            port=os.getenv('DbPort'),
            database=os.getenv('DbDatabase'),
            user=os.getenv('DbUser'),
            password=os.getenv('DbPassword')
        )

    def execute(self, query: str, params: tuple = None, commit: bool = False, result: str = None):
        """Запрос к БД

        :param str query:       Строка с запросом.
        :param tuple params:    Параметры запроса.
        :param bool commit:     Выполнять коммит транзакции? По умолчанию false.
        :param str result:      В каком виде возвращать результат запроса: fetchall, fetchall, dict (данные в словаре,
                                где key - наименование колонки). По умолчанию результат не возвращается.
        :return:                None или полученные строки в заданном виде.
        """
        with self.connect.cursor() as cursor:
            try:
                cursor.execute(query, params)
                if commit:
                    self.connect.commit()
                    res = None
                if result == 'fetchall':
                    res = cursor.fetchall()
                if result == 'fetchone':
                    res = cursor.fetchone()
                if result == 'dict':
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    res = []
                    for row in rows:
                        row_dict = {}
                        for i in range(len(columns)):
                            row_dict[columns[i]] = row[i]
                            res.append(row_dict)
                return res
            except psycopg2.Error as e:
                self.logger.error(f'Ошибка при выполнении SQL запроса: {e}')
                self.connect.rollback()
                raise e

    def commit(self) -> None:
        """Коммит всей транзакции"""
        try:
            conn = self.connect
            conn.commit()
        except psycopg2.Error as e:
            self.logger.error(f'Ошибка при выполнении SQL запроса: {e}')
            self.connect.rollback()
            raise e

    def close(self) -> None:
        """Закрытие соединения с БД"""
        try:
            conn = self.connect
            conn.close()
        except psycopg2.Error as e:
            self.logger.error(f'Ошибка при закрытии соединения {e}')
            raise e

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class DbDataOperations(log.LoggerMixin):
    """Функции для стандартных операций с данными в бд"""
    def __init__(self, connector: Connector):
        super().__init__()
        self.connector = connector

    def data_insert(self, table: str, data: dict):
        """Вставка строки в таблицу БД

        :param str table: Наименование таблицы, куда вставляем данные.
        :param dict data: Данные, которые вставляем в таблицу в словаре формата: {'column': 'value', ...}.
        """
        columns = tuple(data.keys())
        values = tuple(data.values())
        columns_str = ', '.join(columns)
        values_str = ', '.join(['%s'] * len(values))

        query = f'INSERT INTO {table} ({columns_str}) VALUES ({values_str})'
        self.connector.execute(query, values, commit=True)

    def data_update(self, table: str, data: dict, condition: str = None):
        """Обновление строки в таблице БД

        :param str table:       Наименование таблицы в которой обновляем данные.
        :param dict data:       Данные, которые вставляем в таблицу в словаре формата: {'column': 'value', ...}.
        :param str condition:   Условия выборки в синтаксисе SQL, которые указываются после WHERE.
        """
        set_query = ', '.join(f'{column}=%s' for column in data.keys())
        values = tuple(data.values())

        query = f'UPDATE {table} SET {set_query}'
        if condition:
            query = f'{query} WHERE {condition}'

        self.connector.execute(query, values, commit=True)

    def data_select(self, table: str, column: list = None, condition: str = None, result: str = 'fetchall'):
        """Выборка данных в таблице БД

        :param str table:       Наименование таблицы в которой обновляем данные.
        :param list column:     Список наименований столбцов из которых забираем данные.
        :param str condition:   Условия выборки в синтаксисе SQL, которые указываются после WHERE.
        :param str result:      В каком виде возвращать результат запроса: fetchall, fetchall, dict (данные в словаре,
                                где key - наименование колонки). По умолчанию fetchall
        """
        if not column:
            column = '*'
        else:
            column = ''.join(column)
        query = f'SELECT {column} FROM {table}'
        if condition:
            query = f'{query} WHERE {condition}'

        res = self.connector.execute(query, commit=True, result=result)

        return res


class DbAdvancedOperations(DbDataOperations):
    """Дополнительные функции для работы с базой данных"""
    def __int__(self, connector: Connector):
        self.connector = connector

    def insert_new(self, table: str, key: str, data: list, condition: str = None) -> None:
        """Сверка данных с имеющимися в таблице БД и добавление новых записей

        :param str table:       Наименование таблицы в которой обновляем данные.
        :param str key:         Ключевое поле по которому проверяется наличие данных в таблице.
        :param list data:       Данные для сверки и загрузки в базу данных.
        :param str condition:   Условия выборки в синтаксисе SQL, которые указываются после WHERE.
        """
        db_data = self.data_select(table, condition=condition, result='dict')
        income_data_dict = {item[key]: item for item in data}
        db_keys = {item[key] for item in db_data}
        db_columns = set(db_data[0].keys())

        # Добавление новых строк, которых нет в БД
        for income_item in income_data_dict.values():
            if income_item[key] not in db_keys:
                update_data = {key: value for key, value in income_item.items() if key in db_columns}
                self.data_insert(table, update_data)

    def update_table(self, table: str, key: str, data: list, condition: str = None) -> None:
        """Сверка данных с имеющимися в таблице БД и добавление новых записей + обновление изменившихся

        :param str table:       Наименование таблицы в которой обновляем данные.
        :param str key:         Ключевое поле по которому проверяется наличие данных в таблице.
        :param list data:       Данные для сверки и загрузки в базу данных.
        :param str condition:   Условия выборки в синтаксисе SQL, которые указываются после WHERE.
        """
        db_data = self.data_select(table, condition=condition, result='dict')
        income_data_dict = {item[key]: item for item in data}

        # Добавление новых строк, которых нет в БД
        self.insert_new(table, key, data, condition)

        # Обновление данных
        for db_item in db_data:
            db_id = db_item[key]
            if db_id in income_data_dict:
                income_item = income_data_dict[db_id]

                # Проверка на изменения и обновление если они есть
                if db_item != income_item:
                    update_data = {key: value for key, value in income_item.items() if key in db_item}
                    self.data_update(table, update_data, f'id = {db_id}')
