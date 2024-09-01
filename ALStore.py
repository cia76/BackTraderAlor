import logging  # Будем вести лог
from collections import deque
from datetime import datetime

from backtrader.metabase import MetaParams
from backtrader.utils.py3 import with_metaclass

from AlorPy import AlorPy


class MetaSingleton(MetaParams):
    """Метакласс для создания Singleton классов"""
    def __init__(cls, *args, **kwargs):
        """Инициализация класса"""
        super(MetaSingleton, cls).__init__(*args, **kwargs)
        cls._singleton = None  # Экземпляра класса еще нет

    def __call__(cls, *args, **kwargs):
        """Вызов класса"""
        if cls._singleton is None:  # Если класса нет в экземплярах класса
            cls._singleton = super(MetaSingleton, cls).__call__(*args, **kwargs)  # то создаем зкземпляр класса
        return cls._singleton  # Возвращаем экземпляр класса


class ALStore(with_metaclass(MetaSingleton, object)):
    """Хранилище Алор"""
    logger = logging.getLogger('ALStore')  # Будем вести лог

    BrokerCls = None  # Класс брокера будет задан из брокера
    DataCls = None  # Класс данных будет задан из данных

    @classmethod
    def getdata(cls, *args, **kwargs):
        """Возвращает новый экземпляр класса данных с заданными параметрами"""
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        """Возвращает новый экземпляр класса брокера с заданными параметрами"""
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self, provider=AlorPy()):
        super(ALStore, self).__init__()
        self.notifs = deque()  # Уведомления хранилища
        self.provider = provider  # Подключаемся к провайдеру AlorPy
        self.new_bars = []  # Новые бары по всем подпискам на тикеры из Алор

    def start(self):
        self.provider.on_entering = lambda: self.logger.debug(f'WebSocket Thread: Запуск')
        self.provider.on_enter = lambda: self.logger.debug(f'WebSocket Thread: Запущен')
        self.provider.on_connect = lambda: self.logger.debug(f'WebSocket Task: Подключен к серверу')
        self.provider.on_resubscribe = lambda: self.logger.debug(f'WebSocket Task: Возобновление подписок ({len(self.provider.subscriptions)})')
        self.provider.on_ready = lambda: self.logger.debug(f'WebSocket Task: Готов')
        self.provider.on_disconnect = lambda: self.logger.debug(f'WebSocket Task: Отключен от сервера')
        self.provider.on_timeout = lambda: self.logger.debug(f'WebSocket Task: Таймаут')
        self.provider.on_error = lambda response: self.logger.debug(f'WebSocket Task: {response}')
        self.provider.on_cancel = lambda: self.logger.debug(f'WebSocket Task: Отмена')
        self.provider.on_exit = lambda: self.logger.debug(f'WebSocket Thread: Завершение')
        self.provider.on_new_bar = lambda response: self.new_bars.append(dict(guid=response['guid'], data=response['data']))  # Обработчик новых баров по подписке из Алор

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        """Выдача уведомлений хранилища"""
        self.notifs.append(None)
        return [x for x in iter(self.notifs.popleft, None)]

    def stop(self):
        self.provider.on_new_bar = self.provider.default_handler  # Возвращаем обработчик по умолчанию
        self.provider.close_web_socket()  # Перед выходом закрываем соединение с WebSocket

    def on_new_candle(self, response):
        guid = response['guid']  # Идентификатор подписки
        if guid not in self.provider.subscriptions:  # Если подписки по такому индентификатору не существует
            return  # то выходим, дальше не продолжаем
        subscription = self.provider.subscriptions[guid]  # Данные подписки
        bar = response['data']  # Данные бара
        intraday = subscription['tf'].isdigit()  # Если время задано в секундах (число), то считаем, что интервал внутридневной
        bar = dict(datetime=self.get_bar_open_date_time(bar['time'], intraday),  # Дата и время открытия бара в зависимости от интервала
                   open=bar['open'], high=bar['high'], low=bar['low'], close=bar['close'],  # Цены Alor
                   volume=int(bar['volume']))  # Объем в лотах. Бар из подписки
        self.new_bars.append(dict(guid=guid, data=bar))

    def get_bar_open_date_time(self, timestamp, intraday) -> datetime:
        """Дата и время открытия бара. Переводим из GMT в MSK для внутридневного интервала . Оставляем в GMT для дневок и выше."""
        return self.provider.utc_timestamp_to_msk_datetime(timestamp) if intraday\
            else datetime.utcfromtimestamp(timestamp)  # Время открытия бара
