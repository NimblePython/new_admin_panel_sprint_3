"""
Модуль для считывания данных из источника.
Источниик - БД в PostgreSQL.
"""
import logging
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
    MODIFIED = '_modified'

    def __init__(self, connection: _connection):
        self.conn = connection

        self.changed_persons = []
        self.changed_genres = []
        self.changed_films = []

        json_storage = statemanager.JsonFileStorage('conditions.txt')
        self.manager = statemanager.State(json_storage)
        # инициализируем дефолтным значением дату изменения отнсительно которой будет происходить скрининг БД
        self.modified = datetime(1895, 12, 28, 0, 0).strftime('%Y-%m-%d %H:%M:%S')  # дата рождения синематографа -)

        # значения по умолчанию
        self.chunk = 1000
        self.fetch_size = 100

        # значения из settings.ini если они там заданы, иначе - по умолчанию
        config = configparser.ConfigParser()  # создаём объект парсера конфига
        config.read('settings.ini')
        self.chunk = config['Extractor']['chunk_size']
        self.fetch_size = int(config['Extractor']['fetch_size'])

        # log print(f'Размер кипы: {self.chunk}')

    def bake_query(self, key: str):
        queries = {
            'qry_persons_changed':
                f"""
                SELECT id
                FROM content.person
                WHERE updated_at > '{self.modified}'
                ORDER BY updated_at
                LIMIT {self.chunk};
                """,
            'qry_genres_changed':
                f"""
                SELECT id
                FROM content.genre
                WHERE updated_at > '{self.modified}'
                ORDER BY updated_at
                LIMIT {self.chunk};
                """,
            'qry_fw_basic_changed':
                f"""
                SELECT id
                FROM content.film_work
                WHERE updated_at > '{self.modified}'
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
                AND
                    fw.updated_at > '{self.modified}'
                LIMIT {self.chunk};
                """,
            'qry_fw_where_persons_changed':
                f"""
                SELECT DISTINCT fw.id
                FROM content.film_work fw
                LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                WHERE 
                    pfw.person_id in ({', '.join(f"'{el}'" for el in self.changed_persons)})
                AND
                    fw.updated_at > '{self.modified}'
                LIMIT {self.chunk};
                """,
            'qry_all_changes_for_es':
                f"""
                SELECT row_to_json(film) as films
                FROM (
                    SELECT 	
                        fw.id, 
                        fw.title, 
                        (	SELECT 
                                json_object_agg(role, persons)
                            FROM 
                            (	SELECT 
                                    pfw.role AS ROLE,
                                    json_agg(p.full_name) persons 
                                FROM 
                                    content.person_film_work pfw, 
                                    content.person p
                                WHERE 
                                    pfw.film_work_id = fw.id 
                                AND 
                                    pfw.person_id = p.id
                                GROUP BY 1
                            ) person
                        ) as persons, (
                            SELECT 
                                json_agg(g."name") 
                            FROM 
                                content.genre_film_work gfw, 
                                content.genre g
                            WHERE 
                                gfw.film_work_id = fw.id 
                            AND 
                                gfw.genre_id = g.id
                        ) as genres
                FROM content.film_work as fw
                WHERE fw.id IN ({', '.join(f"'{el}'" for el in self.changed_films)})
                ) film;
                """,
        }
        if key not in queries:
            print(f"Невозможно подготовить запрос по ключу {key}")
            return None
        return queries[key]

    @backoff()
    def query_exec(self, cursor, query_to_exec):
        print(query_to_exec)
        cursor.execute(query_to_exec)
        return cursor

    def postgres_producer(self):
        modif = Extractor.MODIFIED
        if date := self.manager.get_state(modif):
            self.modified = date
            print(f'Ключ {modif} считан из хранилища: {self.modified}')
        else:
            print(f'Ключ {modif} не найден в хранилище')
            self.manager.set_state(modif, self.modified)
            self.manager.set_state('genres', 'planned')
            self.manager.set_state('persons', 'planned')
            self.manager.set_state('films', 'planned')
            print(f'Ключ {modif} создан и записан в хранилище со значением {self.modified}')

        # Считывание данных из PG
        with self.conn.cursor() as cur:
            # запрашиваем актеров, которые изменились после даты MODIFIED
            cur = self.query_exec(cur, self.bake_query('qry_persons_changed'))
            self.manager.set_state('persons', 'loaded')
            # Получим фильмы, информацию о которых надо трансформировать и перекинуть
            self.changed_persons = []
            while records := cur.fetchmany(self.fetch_size):
                # готовим список UUIN персоналий
                [self.changed_persons.append(*record) for record in records]
            # print(self.changed_persons)

            # запрашиваем кино, которое связано с измененными персонами
            if self.changed_persons:
                cur = self.query_exec(cur, self.bake_query('qry_fw_where_persons_changed'))
                self.changed_films = []
                while records := cur.fetchmany(self.fetch_size):
                    # готовим список UUIN фильмов
                    [self.changed_films.append(*record) for record in records]
                # print(self.changed_films)
                # здесь можно self.changed_films сохранять локально в файл,
                # так как во-первых результат запроса (и т.о. размер списка с фильмами) может
                # превысить значение chunk, а во-вторых, сохранив файл, мы гарантированно можем
                # поменять значение MODIFIED
                # идея сохраения файла имет нюанс с возможным разрывом состояния данных во времени -
                # пока будем работать с файлом, состояние одного из фильмов из файла может измениться еще раз
                # поэтому ВАЖНО сохранять текущую дату/время для каждого набора фильмов (в названии файла?!)
                # а при подготовке фильмов к передачи в ES проверять, не произошли ли снова изменения у фильма
                # или свзанных с ним жанров и персоналий

        # запуск обогатителя: добавит недостающую информацию и обновит поля updated_at
        try:
            self.postgres_enricher()
            # to do Изменить cостояние MODIFIED
        except Exception as e:
            print('Ошибка в функции postgres_enricher()')
            print('%s: %s' % (e.__class__.__name__, e))

    def postgres_enricher(self):
        """Метод работает со списком self.changed_films,
        по которому дополняет данные из остальных таблиц
        и передает подготовленные данные в Тransform.
        Важно!При подготовке информации по фильму нужно обновить updated_at
        таблицы М2М ссылающуейся на жанры фильма
        """
        if not self.changed_films:
            return None

        # Считывание данных из PG
        with self.conn.cursor() as cur:
            cur = self.query_exec(cur, self.bake_query('qry_all_changes_for_es'))
            film_works_to_elastic = []

            while records := cur.fetchmany(self.fetch_size):
                # film = FilmworkModel()
                # [print(record['films']) for record in records]
                film_works_to_elastic = [FilmworkModel(**record['films']) for record in records]

            # print(film_works_to_elastic[0])
            # тут можно временно сохранить фильмы в промежуточный файл
            # записать modified в файл
            # изменить статус на in_file

        t = Transform()
        t.prepare_and_push(film_works_to_elastic)
        # logging --- print(len(self.changed_films))