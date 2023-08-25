from typing import Union  # Объединение типов
import collections
from datetime import datetime, timedelta, time

from pytz import timezone  # Работаем с временнОй зоной

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
    """Хранилище Алор. Работает с мультисчетами и мультипортфелями

    В параметр providers передавать список счетов в виде словаря с ключами:
    - provider_name - Название провайдера. Должно быть уникальным
    - demo - Режим демо торговли. Можно не указывать. Тогда будет выбран режим реальной торговли
    - username - Имя пользователя из Config
    - refresh_token - Токен из Config

    Пример использования:
    provider1 = dict(provider_name='alor_trade', username=Config.UserName, demo=False, refresh_token=Config.RefreshToken)  # Торговый счет Алор
    provider2 = dict(provider_name='alor_iia', username=ConfigIIA.UserName, demo=False, refresh_token=ConfigIIA.RefreshToken)  # ИИС Алор
    store = ALStore(providers=[provider1, provider2])  # Мультисчет
    """
    params = (
        ('providers', None),  # Список провайдеров счетов в виде словаря
    )

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

    def __init__(self, **kwargs):
        super(ALStore, self).__init__()
        if 'providers' in kwargs:  # Если хранилище создаем из данных/брокера (не рекомендуется)
            self.p.providers = kwargs['providers']  # то список провайдеров берем из переданного ключа providers
        self.notifs = collections.deque()  # Уведомления хранилища
        self.providers = {}  # Справочник провайдеров
        for provider in self.p.providers:  # Пробегаемся по всем провайдерам
            demo = provider['demo'] if 'demo' in provider else False  # Признак демо счета или реальный счет
            provider_name = provider['provider_name'] if 'provider_name' in provider else 'default'  # Название провайдера или название по умолчанию
            self.providers[provider_name] = AlorPy(provider['username'], provider['refresh_token'], demo)  # Работа с Alor OpenAPI V2 из Python https://alor.dev/docs с именем пользователя и токеном
        self.provider = list(self.providers.values())[0]  # Провайдер по умолчанию для работы со справочниками. Первый счет по ключу name
        self.new_bars = []  # Новые бары по всем подпискам на тикеры из Алор

    def start(self):
        for name, provider in self.providers.items():  # Пробегаемся по всем провайдерам
            # События WebSocket Thread/Task для понимания, что происходит с провайдером
            provider.OnEntering = lambda n=name: print(f'- WebSocket Thread({n}): Запуск')
            provider.OnEnter = lambda n=name: print(f'- WebSocket Thread({n}): Запущен')
            provider.OnConnect = lambda n=name: print(f'- WebSocket Task({n}): Подключен к серверу')
            provider.OnResubscribe = lambda n=name: print(f'- WebSocket Task({n}): Возобновление подписок ({len(provider.subscriptions)})')
            provider.OnReady = lambda n=name: print(f'- WebSocket Task({n}): Готов')
            provider.OnDisconnect = lambda n=name: print(f'- WebSocket Task({n}): Отключен от сервера')
            provider.OnTimeout = lambda n=name: print(f'- WebSocket Task({n}): Таймаут')
            provider.OnError = lambda response,  n=name: print(f'- WebSocket Task({n}): {response}')
            provider.OnCancel = lambda n=name: print(f'- WebSocket Task({n}): Отмена')
            provider.OnExit = lambda n=name: print(f'- WebSocket Thread({n}): Завершение')
            provider.OnNewBar = lambda response, n=name: self.new_bars.append(dict(provider_name=n, response=response))  # Обработчик новых баров по подписке из Алор

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        """Выдача уведомлений хранилища"""
        self.notifs.append(None)
        return [x for x in iter(self.notifs.popleft, None)]

    def stop(self):
        for provider in self.providers.values():  # Пробегаемся по всем значениям провайдеров
            provider.OnNewBar = provider.default_handler  # Возвращаем обработчик по умолчанию
            provider.close_web_socket()  # Перед выходом закрываем соединение с WebSocket


class Session:
    """Торговая сессия"""

    def __init__(self, time_begin: time, time_end: time):
        self.time_begin = time_begin  # Время начала сессии
        self.time_end = time_end  # Время окончания сессии


class Schedule:
    """Расписание торгов"""
    market_timezone = timezone('Europe/Moscow')  # ВременнАя зона работы биржи

    def __init__(self, trade_sessions: list[Session]):
        self.trade_sessions = sorted(trade_sessions, key=lambda session: session.time_begin)  # Список торговых сессий сортируем по возрастанию времени начала сессии

    def get_trade_session(self, dt_market: datetime) -> Union[Session, None]:
        """Торговая сессия по дате и времени на бирже

        :param datetime dt_market: Дата и время на бирже
        :return: Дата и время на бирже. None, если торги не идут
        """
        if dt_market.weekday() in (5, 6):  # Если задан выходной день
            return None  # То торги не идут, торговой сессии нет
        t_market = dt_market.time()  # Время на бирже
        for session in self.trade_sessions:  # Пробегаемся по всем торговым сессиям
            if session.time_begin <= t_market <= session.time_end:  # Если время внутри сессии
                return session  # Возвращаем найденную торговую сессию
        return None  # Если время попадает в клиринг/перерыв, то торговой сессии нет

    def time_until_trade(self, dt_market: datetime) -> timedelta:
        """Время, через которое можжно будет торговать

        :param datetime dt_market: Дата и время на бирже
        :return: Время, через которое можжно будет торговать. 0 секунд, если торговать можно прямо сейчас
        """
        session = self.get_trade_session(dt_market)  # Пробуем получить торговую сессию
        if session:  # Если нашли торговую сессию
            return timedelta()  # То ждать не нужно, торговать можно прямо сейчас
        for s in self.trade_sessions:  # Пробегаемся по всем торговым сессиям
            if s.time_begin > dt_market.time():  # Если сессия начинается позже текущего времени на бирже
                session = s  # То это искомая сессия
                break  # Сессию нашли, дальше поиск вести не нужно
        d_market = dt_market.date()  # Дата на бирже
        if not session:  # Сессия не найдена, если время позже окончания последней сессии
            session = self.trade_sessions[0]  # Будет первая торговая сессия
            d_market += timedelta(1)  # Следующего дня
        w_market = d_market.weekday()  # День недели даты на бирже
        if w_market in (5, 6):  # Если биржа на выходных не работает, и задан выходной день
            d_market += timedelta(7 - w_market)  # То будем ждать первой торговой сессии понедельника
        dt_next_session = datetime(d_market.year, d_market.month, d_market.day, session.time_begin.hour, session.time_begin.minute, session.time_begin.second)
        return dt_next_session - dt_market


class MOEXStocks(Schedule):
    """Московская биржа: Фондовый рынок"""

    def __init__(self):
        super(MOEXStocks, self).__init__(
            [Session(time(10, 0, 0), time(18, 39, 59)),  # Основная торговая сессия
             Session(time(19, 5, 0), time(23, 49, 59))])  # Вечерняя торговая сессия


class MOEXFutures(Schedule):
    """Московская биржа: Срочный рынок"""

    def __init__(self):
        super(MOEXFutures, self).__init__(
            [Session(time(9, 0, 0), time(9, 59, 59)),  # Утренняя дополнительная торговая сессия
             Session(time(10, 0, 0), time(13, 59, 59)),  # Основная торговая сессия (Дневной расчетный период)
             Session(time(14, 5, 0), time(18, 49, 59)),  # Основная торговая сессия (Вечерний расчетный период)
             Session(time(19, 5, 0), time(23, 49, 59))])  # Вечерняя дополнительная торговая сессия
