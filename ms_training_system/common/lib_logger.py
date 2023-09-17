import datetime
import logging
import os


def create_logger(name, level=logging.INFO):
    """
    Функция для создания объекта логгера с заданным именем и уровнем логирования.
    """
    env = os.environ.get('ENV', '')
    log_dir = './log'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    current_date = str(datetime.date.today())
    filename = f'{current_date}_{env}.log'
    log_file = os.path.join(log_dir, filename)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter(f'%(asctime)s - {env} - %(filename)s - %(name)s - %(levelname)s - %(message)s')
    handler = logging.FileHandler(log_file, mode='a')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def log(func):
    """
    Декоратор для логирования вызова функции.
    """
    def wrapper(*args, **kwargs):
        logger = create_logger(func.__module__)
        logger.info(f"Вызов функции {func.__name__}")
        return func(*args, **kwargs)
    return wrapper


class LoggerMixin:
    """
    Класс-миксин для добавления логирования в классы.
    """
    def __init__(self):
        self.logger = create_logger(self.__class__.__name__)
