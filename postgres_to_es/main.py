"""Приложение по регулярному методическому просеиванию (скринингу) данных в БД PostgreSQL для
выявления измененных данных, с целью обновления этих же данных в БД в ElasticSearch (ES)
ES содержит эту же информацию для обеспечения (предоставления) функции полнотекстного поиска в бэкендеы
"""

import psycopg2
import os
import extractor
import logging

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
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s')

    pg_db = os.environ.get('DB_NAME_PG')
    usr = os.environ.get('DB_USER')
    pwd = os.environ.get('DB_PASSWORD')
    pg_host = os.environ.get('PG_HOST')
    pg_port = int(os.environ.get('PG_PORT'))
    es_host = os.environ.get('ES_HOST')
    es_port = int(os.environ.get('ES_PORT'))

    pg_dsl = {'dbname': pg_db, 'user': usr, 'password': pwd, 'host': pg_host, 'port': pg_port}
    es_dsl = {'host': es_host, 'port': es_port}

    try:
        with closing(connect_to_db(pg_dsl)) as connection:
            extract = extractor.Extractor(connection, es_dsl)
            extract.postgres_producer()

    except Exception as e:
        print("%s: %s" % (e.__class__.__name__, e))

