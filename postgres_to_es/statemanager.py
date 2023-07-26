import abc
import json

from typing import Any, Dict


class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния.

    Позволяет сохранять и получать состояние.
    Способ хранения состояния может варьироваться в зависимости
    от итоговой реализации. Например, можно хранить информацию
    в базе данных или в распределённом файловом хранилище.
    """

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.
    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        with open(self.file_path, 'w') as sf:
            json.dump(state, sf)

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из файла."""
        results = {}
        try:
            with open(self.file_path, 'r') as sf:
                results = json.load(sf)
        finally:
            return results


class State:
    """Класс для работы с состояниями."""
    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        conditions = self.storage.retrieve_state()
        old_value = None
        if not self.get_state(key):
            print(f'Внимание! Попытка сохранения несуществующего ключа: {key} в set_state()')
            print('Рекомендуем проверять наличие ключа перед вызовом set_state()')
        else:
            old_value = conditions[key]

        conditions[key] = value
        try:
            self.storage.save_state(conditions)
        except Exception as e:
            print("%s: %s" % (e.__class__.__name__, e))
            print(f'Ошибка при сохранении ключа. Значение {value} в ключе {key} не сохранено')
            # Возвращаем значение ключу в списке состояний, так как не удалось сохраниить в хранилище
            if old_value:
                conditions[key] = old_value
                print(f'Актуальным значением ключа осталось: {old_value}')

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        conditions = self.storage.retrieve_state()
        if key not in conditions.keys():
            return None
        return conditions[key]

