"""Параметрический декоратор для осуществления
экспоненциально растущей паузы между повторными
вызовами декорируемой функции.
"""
import time
import logging

from functools import wraps

def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            logging.debug(f"Выполнение функции {func.__name__}")
            n = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.warning(f"Ошибка выполнения функции {func.__name__}")
                    n += 1
                    t = start_sleep_time * factor**n
                    if t >= border_sleep_time:
                        t = border_sleep_time
                    logging.debug(f"Пауза до следующего выполнения функции: {t} сек")
                    time.sleep(t)
        return inner

    return func_wrapper

