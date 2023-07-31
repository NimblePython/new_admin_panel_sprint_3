"""Класс будет реализован, в случае необходимости подтягивания фильмов из временного хранилища
Данный класс по сути будет проверять, есть ли что-то в хранилище (напр. в файле),
сравнивать дату, подгружать и пушить в Эластик.
"""
from models import FilmworkModel
from load import Load


class Transform:
    @staticmethod
    def prepare_and_push(data: list[FilmworkModel],
                         host_name: str,
                         port: int,
                         chunk_size: int = 500) -> int:
        """
        :param data: List of films to push to ES
        :param host_name: ElasticSearch server HOST
        :param port: ElasticSearch server PORT
        :param chunk_size: Size of data to push per time
        :return : количество успешно сохраненных в ЭС фильмов
        """

        load_to_es = Load(data, host_name, port)
        ok = load_to_es.insert_films(chunk_size)

        return ok

