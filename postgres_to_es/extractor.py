"""
Модуль для считывания данных из источника.
Источниик - БД в PostgreSQL.
"""
import logging
import time
import psycopg2
import psycopg2.extensions as pg_extensions
import statemanager
import configparser

from models import FilmworkModel, Schema
from transform import Transform
from psycopg2.extensions import connection as _connection
from datetime import datetime
from backoff_dec import backoff


class LoggingCursor(pg_extensions.cursor):
    def execute(self, sql, args=None):
        logger = logging.getLogger('sql_debug.log')
        logger.info(self.mogrify(sql, args))

        try:
            psycopg2.extensions.cursor.execute(self, sql, args)
        except Exception as e:
            logger.exception("%s: %s" % (e.__class__.__name__, e))
            raise


class Extractor:
    PERSON_MODIFIED_KEY = '_pers_modified'
    GENRE_MODIFIED_KEY = '_gen_modified'
    FILM_MODIFIED_KEY = '_film_modified'

    cnt_load = 0
    cnt_part_load = 0
    cnt_successes = 0

    def __init__(self, connection: _connection, dsl: dict):
        self.conn = connection
        self.es_host = dsl['host']
        self.es_port = int(dsl['port'])

        self.films_to_es = []

        json_storage = statemanager.JsonFileStorage('conditions.txt')
        self.manager = statemanager.State(json_storage)

        # значения по умолчанию
        self.chunk = 1000
        self.fetch_size = 100

        # значения из settings.ini если они там заданы, иначе - по умолчанию
        config = configparser.ConfigParser()  # создаём объект парсера конфига
        config.read('settings.ini')
        self.chunk = int(config['Extractor']['chunk_size'])
        self.fetch_size = int(config['Extractor']['fetch_size'])
        self.pause = int(config['Extractor']['pause_between'])

        logging.info(f'Размер кипы: {self.chunk}')
        logging.info(f'Размер порций fetch: {self.fetch_size}')

    def get_query(self, model: Schema):
        query = f"""
                SELECT id, updated_at
                FROM content.{model.table}
                WHERE updated_at > '{model.modified}'
                ORDER BY updated_at
                LIMIT {self.chunk};
                """
        return query

    @backoff()
    def query_exec(self, cursor, query_to_exec):
        logging.debug(query_to_exec)
        cursor.execute(query_to_exec)
        return cursor

    def get_key_value(self, key: str) -> str:
        """ Считываем ключ и возвращаем значение переменной
        Если такого ключа нет то устанавливаем его в дефолтное значение
        :param key:
        :return:
        """
        value = datetime(1895, 12, 28, 0, 0).strftime('%Y-%m-%d %H:%M:%S')  # дата рождения синематографа
        if date := self.manager.get_state(key):
            value = date
            logging.debug(f'Ключ {key} считан из хранилища: {value}')
        else:
            self.manager.set_state(key, value)
            logging.debug(f'Ключ {key} создан и записан в хранилище со значением {value}')

        return value

    @staticmethod
    def get_date_from_chunk_and_cut(chunk: list):
        date = chunk[-1][1].strftime('%Y-%m-%d %H:%M:%S.%f%z')
        # вычищаем все даты, так как сохранили нужную
        chunk = [itm[0] for itm in chunk]
        return date, chunk

    def get_films(self, model: Schema, entities: list):
        query = \
            f"""
            SELECT DISTINCT fw.id
            FROM content.film_work fw
            LEFT JOIN 
                content.{model.table}_film_work tfw ON tfw.film_work_id = fw.id
            WHERE 
                tfw.{model.table}_id in ({', '.join(f"'{el}'" for el in entities)})
            LIMIT {self.chunk};
            """
        if model.table == 'film_work':
            query = \
                f"""
                SELECT DISTINCT fw.id
                FROM content.film_work fw
                WHERE 
                    fw.id in ({', '.join(f"'{el}'" for el in entities)})
                LIMIT {self.chunk};
                """

        with self.conn.cursor() as cur_films:
            # запрашиваем CHUNK фильмов, которое связано с изменениями
            cur_films = self.query_exec(cur_films, query)
            # готовим fetch_size кусок UUIN фильмов для пушинга в ES
            while films := cur_films.fetchmany(self.fetch_size):
                self.films_to_es = [record[0] for record in films]
                logging.debug(f'Вызван для {model.table} --- Фильмы собраны для ES:')
                try:
                    # запуск обогатителя: добавит недостающую информацию и запишет в ES
                    self.postgres_enricher()
                    # Если запись прошла успешно то меняем статус
                    if Extractor.cnt_part_load == Extractor.cnt_successes:
                        # Изменяем сотояние (дату) для параметра от имени которого произошел вызов функции
                        self.manager.set_state(model.key, model.modified)
                        logging.info(f"Изменено сотояние для ключа {model.key} в значение {model.modified}")
                except Exception as e:
                    logging.exception('%s: %s' % (e.__class__.__name__, e))

    def postgres_producer(self):
        # ЗАПУСАЕМ ПРОЦЕСС В БЕСКОНЕЧНОМ ЦИКЛЕ
        is_run = True
        while is_run:
            objects: list[Schema] = []

            logging.info(f"Настраиваемая пауза длительностью {self.pause} сек.")
            time.sleep(self.pause)  # пауза между сессиями сриннинга БД

            data = self.get_key_value(Extractor.PERSON_MODIFIED_KEY)
            objects.append(Schema('person', Extractor.PERSON_MODIFIED_KEY, data))

            data = self.get_key_value(Extractor.GENRE_MODIFIED_KEY)
            objects.append(Schema('genre', Extractor.GENRE_MODIFIED_KEY, data))

            data = self.get_key_value(Extractor.FILM_MODIFIED_KEY)
            objects.append(Schema('film_work', Extractor.FILM_MODIFIED_KEY, data))

            for cur_model in objects:
                # Считывание данных из PG
                with self.conn.cursor() as cur:
                    # запрашиваем CHUNK которые изменились после даты _MODIFIED
                    cur = self.query_exec(cur, self.get_query(cur_model))
                    while records := cur.fetchmany(self.fetch_size):
                        # формируем (кусочек) UUIN персоналий
                        changed_entities = [record for record in records]
                        # запомним дату пследнего из fetch_size для изменения статуса
                        cur_model.modified, changed_entities = self.get_date_from_chunk_and_cut(changed_entities)

                        if changed_entities:
                            # готовим CHUNK фильмов связанных с изменениями и отправляем в ES
                            self.get_films(cur_model, changed_entities)

            Extractor.cnt_part_load = 0
            Extractor.cnt_successes = 0

    @staticmethod
    def make_names(film_work: FilmworkModel) -> FilmworkModel:
        """Уточнение данных, для соответствия
        маппингу индекса в ElasticSearch
        """
        if not film_work.director:
            film_work.director = []
        if film_work.writers:
            film_work.writers_names = [writer.name for writer in film_work.writers]
        if film_work.actors:
            film_work.actors_names = [actor.name for actor in film_work.actors]

        return film_work

    def postgres_enricher(self):
        """Метод работает со списком self.films_to_es,
        по которому дополняет данные из остальных таблиц
        и передает подготовленные данные в Тransform.
        """
        if not self.films_to_es:
            return None

        query = f"""
                SELECT row_to_json(film) as films
                FROM 
                (
                    SELECT 	
                        fw.id, 
                        fw.title,
                        fw.description,
                        fw.rating as imdb_rating,
                        fw.type,
                        fw.created_at,
                        fw.updated_at,
                        (	
                            SELECT json_agg(actors_group)
                            FROM 
                            (
                                SELECT
                                    p.id id, 
                                    p.full_name as name
                                FROM content.person_film_work pfw, content.person p
                                WHERE pfw.film_work_id = fw.id AND pfw.person_id = p.id AND pfw.role='actor'
                            ) actors_group
                        ) as actors,
                        ( 
                            SELECT json_agg(writers_group)
                            FROM 
                            (
                                SELECT
                                    p.id id, 
                                    p.full_name as name
                                FROM 
                                    content.person_film_work pfw, 
                                    content.person p
                                WHERE 
                                    pfw.film_work_id = fw.id
                                AND 
                                    pfw.person_id = p.id 
                                AND 
                                    pfw.role='writer'
                            ) writers_group
                        ) as writers,	
                        (	
                            SELECT 
                                json_agg(p.full_name)                            
                            FROM 
                                content.person_film_work pfw, 
                                content.person p
                            WHERE
                                pfw.film_work_id = fw.id 
                            AND 
                                pfw.person_id = p.id 
                            AND 
                                pfw.role='director'
                        ) as director,                   
                        (
                            SELECT json_agg(g.name)
                            FROM
                                content.genre_film_work gfw, 
                                content.genre g
                            WHERE 
                                gfw.film_work_id = fw.id 
                            AND 
                                gfw.genre_id = g.id
                        ) as genre
                FROM content.film_work as fw
                WHERE fw.id IN ({', '.join(f"'{el}'" for el in self.films_to_es)})
                ) film;
                """

        # Считывание данных из PG и обогащеине
        with self.conn.cursor() as cur:
            cur = self.query_exec(cur, query)

            while records := cur.fetchmany(self.fetch_size):
                raw_records = [FilmworkModel(**record['films']) for record in records]
                film_works_to_elastic = [self.make_names(record) for record in raw_records]

                t = Transform()
                cnt_films = len(film_works_to_elastic)

                Extractor.cnt_load += cnt_films
                Extractor.cnt_part_load = cnt_films
                Extractor.cnt_successes = t.prepare_and_push(film_works_to_elastic,
                                                             chunk_size=cnt_films,
                                                             host_name=self.es_host,
                                                             port=self.es_port)
