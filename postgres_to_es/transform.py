"""Абстрактный класс (пока что).
Будет реализован, в случае необходимости подтягивания фильмов из временного хранилища
Данный класс по сути будет проверять, есть ли что-то в хранилище, сравнивать дату, и если есть,
то подгружать и пушить в Эластик.
"""
from models import FilmworkModel
from load import Load


class Transform:

    def prepare_and_push(self, data: list[FilmworkModel], from_disk=False):
        """
        # проверяем наличие данных в списке, если есть то передаем в Load
        # иначе читаем состояние из хранилища
        # если in_file, то читаем дату файла и сравниваем с датой состояния
        # если в файле актуальная дата, то грузим и передаем в Load
        :param data:
        :param from_disk:
        :return :
        """
        if not data or from_disk:
            pass

        load_to_es = Load(data)
        load_to_es.insert_films()

        return
