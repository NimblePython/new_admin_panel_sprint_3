import requests

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk
from backoff_dec import backoff
from models import FilmworkModel


class Load:
    successes = 0
    docs_count = 0

    index_settings = {
        "refresh_interval": "1s",
        "analysis": {
            "filter": {
                "english_stop": {
                    "type": "stop",
                    "stopwords": "_english_"
                },
                "english_stemmer": {
                    "type": "stemmer",
                    "language": "english"
                },
                "english_possessive_stemmer": {
                    "type": "stemmer",
                    "language": "possessive_english"
                },
                "russian_stop": {
                    "type": "stop",
                    "stopwords": "_russian_"
                },
                "russian_stemmer": {
                    "type": "stemmer",
                    "language": "russian"
                }
            },
            "analyzer": {
                "ru_en": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "english_stop",
                        "english_stemmer",
                        "english_possessive_stemmer",
                        "russian_stop",
                        "russian_stemmer"
                    ]
                }
            }
        }
    }

    index_mappings = {
        "dynamic": "strict",
        "properties": {
          "id": {"type": "keyword"},
          "imdb_rating": {"type": "float"},
          "genre": {"type": "keyword"},
          "title": {"type": "text", "analyzer": "ru_en", "fields": {"raw": {"type":  "keyword"}}},
          "description": {"type": "text", "analyzer": "ru_en"},
          "director": {"type": "text", "analyzer": "ru_en"},
          "actors_names": {"type": "text", "analyzer": "ru_en"},
          "writers_names": {"type": "text","analyzer": "ru_en"},
          "actors": {"type": "nested", "dynamic": "strict","properties": {"id": {"type": "keyword"}, "name": {"type": "text", "analyzer": "ru_en"}}},
          "writers": {"type": "nested", "dynamic": "strict","properties": {"id": {"type": "keyword"},"name": {"type": "text", "analyzer": "ru_en"}}}
        }
    }

    def __init__(self, data: list[FilmworkModel], host, port):
        self.es_socket = f'http://{host}:{port}/'
        self.es = self.connect_to_es()
        self.data = data

    @backoff()
    def connect_to_es(self):
        return Elasticsearch(self.es_socket)

    @backoff()
    def create_index(self):
        self.es.indices.create(index='movies',
                               settings=Load.index_settings,
                               mappings=Load.index_mappings)

    @backoff()
    def check_index(self):
        url = self.es_socket + 'movies/_mapping'
        message = requests.get(url)
        if message.status_code == 404:
            return False
        return True

    def get_data(self) -> dict:
        for record in self.data:
            doc = dict()
            doc['_id'] = record.id
            doc['_index'] = 'movies'
            doc['_source'] = record.model_dump_json()
            # print(record.updated_at)
            yield doc

    @backoff()
    def insert_films(self, chunk_size: int) -> int:
        """
        Функция для вставки пачки записей о фильмах в ES

        :param chunk_size: размер пачки данных
        :return: число успешно вставленнх записей
        """
        if not self.check_index():
            self.create_index()

        successful_records = 0
        for ok, action in streaming_bulk(self.es,
                                         index='movies',
                                         actions=self.get_data(),
                                         chunk_size=chunk_size):
            # print('Try to insert to ES')
            if ok:
                pass
                # print(">>> Сохранен в ES фильм: ", action['index']['_id'])
            successful_records += ok

        return successful_records
