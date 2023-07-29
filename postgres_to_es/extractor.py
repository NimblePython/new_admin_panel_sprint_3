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


from models import FilmworkModel
from transform import Transform
from psycopg2.extensions import connection as _connection
from datetime import datetime
from backoff_dec import backoff


class LoggingCursor(pg_extensions.cursor):
    def execute(self, sql, args=None):
        logger = logging.getLogger('sql_debug.txt')
        logger.info(self.mogrify(sql, args))

        try:
            psycopg2.extensions.cursor.execute(self, sql, args)
        except Exception as e:
            logger.error("%s: %s" % (e.__class__.__name__, e))
            raise


class Extractor:
    PERSON_MODIFIED_KEY = '_pers_modified'
    GENRE_MODIFIED_KEY = '_gen_modified'

    cnt_load = 0
    cnt_successes = 0
    docs_count_from_es = 0

    def __init__(self, connection: _connection):
        self.conn = connection

        self.changed_persons = []
        self.changed_genres = []
        self.changed_films = []

        json_storage = statemanager.JsonFileStorage('conditions.txt')
        self.manager = statemanager.State(json_storage)
        # инициализируем дефолтным значением дату изменения отнсительно которой будет происходить скрининг БД
        self.pers_modified = datetime(1895, 12, 28, 0, 0).strftime('%Y-%m-%d %H:%M:%S')  # дата рождения синематографа
        self.gen_modified = self.pers_modified
        self.film_modified = self.pers_modified
        # значения по умолчанию
        self.chunk = 1000
        self.fetch_size = 100

        # значения из settings.ini если они там заданы, иначе - по умолчанию
        config = configparser.ConfigParser()  # создаём объект парсера конфига
        config.read('settings.ini')
        self.chunk = int(config['Extractor']['chunk_size'])
        self.fetch_size = int(config['Extractor']['fetch_size'])

        # log print(f'Размер кипы: {self.chunk}')

    def bake_query(self, key: str):
        queries = {
            'qry_persons_changed':
                f"""
                SELECT id, updated_at
                FROM content.person
                WHERE updated_at > '{self.pers_modified}'
                ORDER BY updated_at
                LIMIT {self.chunk};
                """,
            'qry_genres_changed':
                f"""
                SELECT id, updated_at
                FROM content.genre
                WHERE updated_at > '{self.gen_modified}'
                ORDER BY updated_at
                LIMIT {self.chunk};
                """,
            'qry_fw_basic_changed':
                f"""
                SELECT id
                FROM content.film_work
                WHERE updated_at > '{self.film_modified}'
                ORDER BY updated_at
                LIMIT {self.chunk};
                """,
            'qry_fw_where_genres_changed':
                f"""
                SELECT DISTINCT fw.id
                FROM content.film_work fw
                LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                WHERE 
                    gfw.genre_id in ({', '.join(f"'{el}'" for el in self.changed_genres)})
                LIMIT {self.chunk};
                """,
            'qry_fw_where_persons_changed':
                f"""
                SELECT DISTINCT fw.id
                FROM content.film_work fw
                LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                WHERE 
                    pfw.person_id in ({', '.join(f"'{el}'" for el in self.changed_persons)})
                LIMIT {self.chunk};
                """,
            'qry_all_changes_for_es':
                f"""
                SELECT row_to_json(film) as films
                FROM 
                (
                    SELECT 	
                        fw.id, 
                        fw.title,
                        fw.description,
                        fw.rating,
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
                                FROM content.person_film_work pfw, content.person p
                                WHERE pfw.film_work_id = fw.id AND pfw.person_id = p.id AND pfw.role='writer'
                            ) writers_group
                        ) as writers,	
                        (	
                            SELECT json_agg(directors_group)
                            FROM 
                            (
                                SELECT
                                    p.id id, 
                                    p.full_name as name	                                
                                FROM content.person_film_work pfw, content.person p
                                WHERE pfw.film_work_id = fw.id AND pfw.person_id = p.id AND pfw.role='director'
                            ) directors_group
                        ) as directors, 
                        (	
                            SELECT json_agg(directors_group)
                            FROM 
                            (
                                SELECT
                                    p.id id, 
                                    p.full_name as name	                                
                                FROM content.person_film_work pfw, content.person p
                                WHERE pfw.film_work_id = fw.id AND pfw.person_id = p.id AND pfw.role='director'
                            ) directors_group
                        ) as directors,                        
                        (
                            SELECT json_agg(genres)
                            FROM
                            (	
                                SELECT
                                    g.name as name
                                FROM 
                                    content.genre_film_work gfw, 
                                    content.genre g
                                WHERE 
                                    gfw.film_work_id = fw.id 
                                AND 
                                    gfw.genre_id = g.id
                            ) genres
                        ) as genres
                FROM content.film_work as fw
                WHERE fw.id IN ({', '.join(f"'{el}'" for el in self.changed_films)})
                ) film;
                """,
        }
        if key not in queries:
            print(f"Невозможно подготовить запрос по ключу {key}")
            raise KeyError
        return queries[key]

    @backoff()
    def query_exec(self, cursor, query_to_exec):
        print(query_to_exec)
        cursor.execute(query_to_exec)
        return cursor

    def records_generator(self, cursor):
        while records := cursor.fetchmany(self.fetch_size):
            # loaded_data = [rec for rec in records]
            yield from list(records)

    def get_key_value(self, key: str) -> str:
        """ Считываем ключ и возвращаем значение переменной
        Если такого ключа нет то устанавливаем его в дефолтное значение
        :param key:
        :return:
        """
        value = datetime(1895, 12, 28, 0, 0).strftime('%Y-%m-%d %H:%M:%S')
        if date := self.manager.get_state(key):
            value = date
            # --log print(f'Ключ {key} считан из хранилища: {value}')
        else:
            self.manager.set_state(key, value)
            # --log print(f'Ключ {key} создан и записан в хранилище со значением {value}')

        return value

    def postgres_producer(self):

        # ЗАПУСАЕМ ПРОЦЕСС В БЕСКОНЕЧНОМ ЦИКЛЕ
        is_run = True
        while is_run:
            time.sleep(2)  # пауза для доступа к БД мужду сриннингами
            self.pers_modified = self.get_key_value(Extractor.PERSON_MODIFIED_KEY)
            self.gen_modified = self.get_key_value(Extractor.GENRE_MODIFIED_KEY)

            # Считывание данных о ПЕРСОНАХ из PG
            with self.conn.cursor() as cur_persons, self.conn.cursor() as cur_films:
                # запрашиваем CHUNK актеров, которые изменились после даты PERS_MODIFIED
                cur_persons = self.query_exec(cur_persons, self.bake_query('qry_persons_changed'))
                # готовим (часть - fetch_size) UUIN персоналий
                while persons := cur_persons.fetchmany(self.fetch_size):
                    [self.changed_persons.append(record) for record in persons]

                    # запомним дату персоны пследнего из fetch_size для изменения статуса
                    # сохраняяем именно из fetchmany, так как запрос фиьмов будет происходить отсюда
                    self.pers_modified = self.changed_persons[-1][1].strftime('%Y-%m-%d %H:%M:%S.%f%z')
                    # --log print('Новая дата для изменения статуса персоналий', self.pers_modified)

                    # вычищаем все даты, так как сохранили нужную
                    self.changed_persons = [itm[0] for itm in self.changed_persons]
                    # print('persons', self.changed_persons)
                    if self.changed_persons:
                        # запрашиваем CHUNK фильмов, которое связано с подготовленным кусочком персоналий
                        cur_films = self.query_exec(cur_films, self.bake_query('qry_fw_where_persons_changed'))
                        # готовим fetch_size кусок UUIN фильмов для пушинга в ES
                        while films := cur_films.fetchmany(self.fetch_size):
                            [self.changed_films.append(*record) for record in films]
                            # --log print('films', self.changed_films)

                        try:
                            # запуск обогатителя: добавит недостающую информацию
                            self.postgres_enricher()
                            # Если запись прошла успешно то меняем статус
                            if Extractor.cnt_load == Extractor.cnt_successes:
                                print('Записать дату последнего успешного чанка', self.pers_modified)
                                self.manager.set_state(Extractor.PERSON_MODIFIED_KEY, self.pers_modified)

                            self.changed_films = []
                            self.changed_persons = []
                        except Exception as e:
                            print('Ошибка в функции postgres_enricher()')
                            print('%s: %s' % (e.__class__.__name__, e))

            # Считывание данных о ЖАНРАХ из PG
            with self.conn.cursor() as cur_genres, self.conn.cursor() as cur_films:
                # запрашиваем CHUNK жанров, которые изменились после даты GENRE_MODIFIED
                cur_genres = self.query_exec(cur_genres, self.bake_query('qry_genres_changed'))
                # готовим (часть - fetch_size) UUIN жанров
                while genres := cur_genres.fetchmany(self.fetch_size):
                    [self.changed_genres.append(record) for record in genres]

                    # запомним дату пследнего из fetch_size для изменения статуса
                    # сохраняяем именно из fetchmany, так как запрос фиьмов будет происходить отсюда
                    self.gen_modified = self.changed_genres[-1][1].strftime('%Y-%m-%d %H:%M:%S.%f%z')
                    # --log print('Новая дата для изменения статуса персоналий', self.pers_modified)
                    # вычищаем все даты, так как сохранили нужную
                    self.changed_genres = [itm[0] for itm in self.changed_genres]
                    # print('persons', self.changed_persons)
                    if self.changed_genres:
                        # запрашиваем CHUNK фильмов, которое связано с подготовленным кусочком жанров
                        cur_films = self.query_exec(cur_films, self.bake_query('qry_fw_where_genres_changed'))
                        # готовим fetch_size кусок UUIN фильмов для пушинга в ES
                        while films := cur_films.fetchmany(self.fetch_size):
                            [self.changed_films.append(*record) for record in films]
                            # --log print('films', self.changed_films)
                        try:
                            # запуск обогатителя: добавит недостающую информацию и запишет в ES
                            self.postgres_enricher()
                            # Если запись прошла успешно то меняем статус
                            if Extractor.cnt_load == Extractor.cnt_successes:
                                print('Записать дату последнего успешного чанка', self.gen_modified)
                                self.manager.set_state(Extractor.GENRE_MODIFIED_KEY, self.gen_modified)
                            self.changed_films = []
                            self.changed_genres = []
                        except Exception as e:
                            print('Ошибка в функции postgres_enricher()')
                            print('%s: %s' % (e.__class__.__name__, e))

            print()
            print('Подготовлено фильмов для записи в ES:', Extractor.cnt_load)
            print('Успешно записанных в ES фильмов:', Extractor.cnt_successes)
            Extractor.cnt_load = 0
            Extractor.cnt_successes = 0

    @staticmethod
    def make_names(film_work: FilmworkModel) -> FilmworkModel:
        """Уточнение данных, для соответствия
        маппингу индекса в ElasticSearch
        """
        if film_work.directors:
            film_work.directors_names = [director.name for director in film_work.directors]
        if film_work.writers:
            film_work.writers_names = [writer.name for writer in film_work.writers]
        if film_work.actors:
            film_work.actors_names = [actor.name for actor in film_work.actors]

        return film_work

    def postgres_enricher(self):
        """Метод работает со списком self.changed_films,
        по которому дополняет данные из остальных таблиц
        и передает подготовленные данные в Тransform.
        """
        if len(self.changed_films) == 0:
            return None

        # Считывание данных из PG и обогащеине
        with self.conn.cursor() as cur:
            cur = self.query_exec(cur, self.bake_query('qry_all_changes_for_es'))
            film_works_to_elastic = []

            while records := cur.fetchmany(self.fetch_size):
                raw_records = [FilmworkModel(**record['films']) for record in records]
                film_works_to_elastic = [self.make_names(record) for record in raw_records]

                # [print(film.id) for film in film_works_to_elastic]

                t = Transform()
                cnt_films = len(film_works_to_elastic)
                Extractor.cnt_load += cnt_films
                Extractor.cnt_successes = t.prepare_and_push(film_works_to_elastic,
                                                             chunk_size=cnt_films)

