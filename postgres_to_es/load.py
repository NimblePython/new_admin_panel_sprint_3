import tqdm

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk
from backoff_dec import backoff
from models import FilmworkModel


class Load:
    successes = 0
    docs_count = 0
    def __init__(self, data: list[FilmworkModel]):
        # todo подставить имя хоста - имя контейнера
        # todo загрузить настройки хоста и порта из .env
        self.es = self.connect_to_es()
        self.data = data
        # todo: заменить хост:порт из конфига
        # todo: переделать под модуль Elasticsearch
        self.index = """curl -XPUT http://127.0.0.1:9200/movies -H 'Content-Type: application/json' -d'
        {
          "settings": {
            "refresh_interval": "1s",
            "analysis": {
              "filter": {
                "english_stop": {
                  "type":       "stop",
                  "stopwords":  "_english_"
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
                  "type":       "stop",
                  "stopwords":  "_russian_"
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
          },
          "mappings": {
            "dynamic": "strict",
            "properties": {
              "id": {
                "type": "keyword"
              },
              "rating": {
                "type": "half_float"
              },
              "title": {
                "type": "text",
                "analyzer": "ru_en",
                "fields": {
                  "raw": { 
                    "type":  "keyword"
                  }
                }
              },
              "description": {
                "type": "text",
                "analyzer": "ru_en"
              },
              "directors_names": {
                "type": "text",
                "analyzer": "ru_en"
              },
              "actors_names": {
                "type": "text",
                "analyzer": "ru_en"
              },
              "writers_names": {
                "type": "text",
                "analyzer": "ru_en"
              },
              "actors": {
                "type": "nested",
                "dynamic": "strict",
                "properties": {
                  "id": {
                    "type": "keyword"
                  },
                  "name": {
                    "type": "text",
                    "analyzer": "ru_en"
                  }
                }
              },
              "writers": {
                "type": "nested",
                "dynamic": "strict",
                "properties": {
                  "id": {
                    "type": "keyword"
                  },
                  "name": {
                    "type": "text",
                    "analyzer": "ru_en"
                  }
                }
              },
              "directors": {
                "type": "nested",
                "dynamic": "strict",
                "properties": {
                  "id": {
                    "type": "keyword"
                  },
                  "name": {
                    "type": "text",
                    "analyzer": "ru_en"
                  }
                }
              },
              "genres": {
                "type": "nested",
                "dynamic": "strict",
                "properties": {
                  "name": {
                    "type": "text",
                    "analyzer": "ru_en"
                  }
                }
              }
            }
          }
        }'
        """
        self.host_index = "http://localhost:9200/movies/"
        self.insert = "curl -XPOST " + \
                      self.host_index + \
                      "_doc/ -H 'Content-Type: application/json' -d'"

    @backoff()
    def connect_to_es(self):
        return Elasticsearch("http://localhost:9200")

    def get_data(self) -> dict:
        """
        """
        for record in self.data:
            doc = dict()
            doc['_id'] = record.id
            doc['_index'] = 'movies'
            doc['_source'] = record.model_dump_json()
            # print(record.updated_at)
            yield doc

    def insert_films(self, chunk_size: int):
        """
        for element in self.data:
            self.es.index(
                index='movies',
                document=element.json()
            )
        """
        # progress = tqdm.tqdm(unit="docs", total=chunk_size)
        # Load.docs_count += chunk_size
        for ok, action in streaming_bulk(self.es, index='movies',
                                         actions=self.get_data(),
                                         chunk_size=chunk_size):
            # progress.update(1)
            if ok:
                print(">>> Сохранен в ES фильм: ", action['index']['_id'])
            Load.successes += ok

        return Load.successes

