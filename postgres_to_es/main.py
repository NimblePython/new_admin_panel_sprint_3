"""Приложение по регулярному методическому просеиванию (скринингу) данных в БД PostgreSQL для
выявления измененных данных, с целью обновления этих же данных в БД в ElasticSearch (ES)
ES содержит эту же информацию для обеспечения (предоставления) функции полнотекстного поиска в бэкендеы
"""

import psycopg2
import os
import extractor

from dotenv import load_dotenv, find_dotenv
from contextlib import closing
from psycopg2.extras import DictCursor
from backoff_dec import backoff


load_dotenv(find_dotenv())

# При запуске считать состояние
# Генерировать номер подключения сохранить состояние
# Подключиться к БД
# Атомарно выполниить:
# 1. Скачать пачку данных
# 2. Трансформировать данные
# 3. Передать ElasticSearch на включение в индекс
# 4. Пометить выполненное состояние
# В случае сбоя выполнения пп 1-4 (отсутсвие успешного состояния) повторно выполнить эти действия
# Перейти к следующей пачке данных, пока есть данные


@backoff()
def query_exec(cursor, query_to_exec):
    cursor.execute(query_to_exec)
    return cursor


@backoff()
def connect_to_db(params):
    return psycopg2.connect(**params, cursor_factory=DictCursor)


if __name__ == '__main__':
    """ Состояния которые могут быть:
        - Последнее проверенное время
        - Незавершенная транзакция
        - Всё ок
    """

    pg_db = os.environ.get("DB_NAME_PG")
    usr = os.environ.get("DB_USER")
    pwd = os.environ.get("DB_PASSWORD")
    host = os.environ.get("HOST")
    port = int(os.environ.get("PORT"))

    dsl = {'dbname': pg_db, 'user': usr, 'password': pwd, 'host': host, 'port': port}

    try:
        with closing(connect_to_db(dsl)) as connection:
            extract = extractor.Extractor(connection)
            extract.postgres_producer()

    except Exception as e:
        print("%s: %s" % (e.__class__.__name__, e))

