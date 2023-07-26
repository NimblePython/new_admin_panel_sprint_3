from elasticsearch import Elasticsearch
from backoff_dec import backoff
from models import FilmworkModel
class Load:
    def __init__(self, data: list[FilmworkModel]):
        # to do подставить имя хоста - имя контейнера
        # to do загрузить настройки хоста и порта из .env
        self.es = self.connect_to_es()
        self.data = data

    @backoff()
    def connect_to_es(self):
        return Elasticsearch("http://localhost:9200")

    def insert_films(self):
        print(self.data)
