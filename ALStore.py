import collections

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

    def __init__(self):
        super(ALStore, self).__init__()
        self.notifs = collections.deque()  # Уведомления хранилища
        self.providers = {}  # Справочник провайдеров
        for provider in self.p.providers:  # Пробегаемся по всем провайдерам
            demo = provider['demo'] if 'demo' in provider else False  # Признак демо счета или реальный счет
            provider_name = provider['provider_name'] if 'provider_name' in provider else 'default'  # Название провайдера или название по умолчанию
            self.providers[provider_name] = AlorPy(provider['username'], provider['refresh_token'], demo)  # Работа с Alor OpenAPI V2 из Python https://alor.dev/docs с именем пользователя и токеном
        self.provider = list(self.providers.values())[0]  # Провайдер по умолчанию для работы со справочниками. Первый счет по ключу name
        self.symbols = {}  # Информация о тикерах
        self.new_bars = []  # Новые бары по всем подпискам на тикеры из Алор

    def start(self):
        for name, provider in self.providers.items():  # Пробегаемся по всем провайдерам
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
            provider.OnNewBar = lambda response,  n=name: self.new_bars.append(dict(provider_name=n, response=response))  # Обработчик новых баров по подписке из Алор

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        """Выдача уведомлений хранилища"""
        self.notifs.append(None)
        return [x for x in iter(self.notifs.popleft, None)]

    def stop(self):
        for provider in self.providers.values():  # Пробегаемся по всем значениям провайдеров
            provider.OnNewBar = provider.DefaultHandler  # Возвращаем обработчик по умолчанию
            provider.CloseWebSocket()  # Перед выходом закрываем соединение с WebSocket

    # Функции

    def get_symbol_info(self, exchange, symbol, reload=False):
        """Получение информации тикера

        :param str exchange: Биржа 'MOEX' или 'SPBX'
        :param str symbol: Тикер
        :param bool reload: Получить информацию из Алор
        :return: Значение из кэша/Алор или None, если тикер не найден
        """
        if reload or (exchange, symbol) not in self.symbols:  # Если нужно получить информацию из Алор или нет информации о тикере в справочнике
            symbol_info = self.provider.GetSymbol(exchange, symbol)  # Получаем информацию о тикере из Алор
            if not symbol_info:  # Если тикер не найден
                print(f'Информация о {exchange}.{symbol} не найдена')
                return None  # то возвращаем пустое значение
            self.symbols[(exchange, symbol)] = symbol_info  # Заносим информацию о тикере в справочник
        return self.symbols[(exchange, symbol)]  # Возвращаем значение из справочника

    @staticmethod
    def data_name_to_exchange_symbol(dataname):
        """Биржа и код тикера из названия тикера. Если задается без биржи, то по умолчанию ставится MOEX

        :param str dataname: Название тикера
        :return: Код площадки и код тикера
        """
        symbol_parts = dataname.split('.')  # По разделителю пытаемся разбить тикер на части
        if len(symbol_parts) >= 2:  # Если тикер задан в формате <Биржа>.<Код тикера>
            exchange = symbol_parts[0]  # Биржа
            symbol = '.'.join(symbol_parts[1:])  # Код тикера
        else:  # Если тикер задан без биржи
            exchange = 'MOEX'  # Биржа по умолчанию
            symbol = dataname  # Код тикера
        return exchange, symbol  # Возвращаем биржу и код тикера

    @staticmethod
    def exchange_symbol_to_data_name(exchange, symbol):
        """Название тикера из биржи и кода тикера

        :param str exchange: Биржа 'MOEX' или 'SPBX'
        :param str symbol: Тикер
        :return: Название тикера
        """
        return f'{exchange}.{symbol}'

    def bt_to_alor_price(self, exchange, symbol, price: float):
        """Перевод цен из BackTrader в Алор

        :param str exchange: Биржа 'MOEX' или 'SPBX'
        :param str symbol: Тикер
        :param float price: Цена в BackTrader
        :return: Цена в Алор
        """
        si = self.get_symbol_info(exchange, symbol)  # Информация о тикере
        primary_board = si['primary_board']  # Рынок тикера
        if primary_board == 'TQOB':  # Для рынка облигаций
            price /= 10  # цену делим на 10
        min_step = si['minstep']  # Минимальный шаг цены
        decimals = max(0, str(min_step)[::-1].find('.'))  # Из шага цены получаем кол-во знаков после запятой
        return round(price, decimals)  # Округляем цену

    def alor_to_bt_price(self, exchange, symbol, price: float):
        """Перевод цен из Алор в BackTrader

        :param str exchange: Биржа 'MOEX' или 'SPBX'
        :param str symbol: Тикер
        :param float price: Цена в Алор
        :return: Цена в BackTrader
        """
        si = self.get_symbol_info(exchange, symbol)  # Информация о тикере
        primary_board = si['primary_board']  # Рынок тикера
        if primary_board == 'TQOB':  # Для рынка облигаций
            price *= 10  # цену умножаем на 10
        return price
