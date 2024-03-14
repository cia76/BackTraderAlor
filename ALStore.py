from collections import deque
import logging

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
        self.provider = provider  # Подключаемся ко всем торговым счетам
        self.new_bars = []  # Новые бары по всем подпискам на тикеры из Алор

    def start(self):
        self.provider.on_new_bar = lambda response: self.new_bars.append(dict(guid=response['guid'], data=response['data']))  # Обработчик новых баров по подписке из Алор
        # События WebSocket Thread/Task для понимания, что происходит с провайдером
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

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        """Выдача уведомлений хранилища"""
        self.notifs.append(None)
        return [x for x in iter(self.notifs.popleft, None)]

    def stop(self):
        self.provider.on_new_bar = self.provider.default_handler  # Возвращаем обработчик по умолчанию
        self.provider.close_web_socket()  # Перед выходом закрываем соединение с WebSocket
