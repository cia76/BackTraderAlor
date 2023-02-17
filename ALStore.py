import collections

from backtrader.metabase import MetaParams
from backtrader.utils.py3 import with_metaclass
from backtrader import Order
from backtrader.position import Position

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
    """Хранилище Alor"""
    params = (
        ('UserName', None),  # Имя пользователя
        ('RefreshToken', None),  # Токен
        ('Demo', False),  # Режим демо торговли. По умолчанию установлен режим реальной торговли
    )

    BrokerCls = None  # Класс брокера будет задан из брокера
    DataCls = None  # Класс данных будет задан из данных

    @classmethod
    def getdata(cls, *args, **kwargs):
        """Returns DataCls with args, kwargs"""
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        """Returns broker with *args, **kwargs from registered BrokerCls"""
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self):
        super(ALStore, self).__init__()
        self.notifs = collections.deque()  # Уведомления хранилища
        self.apProvider = AlorPy(self.p.UserName, self.p.RefreshToken, self.p.Demo)  # Вызываем конструктор AlorPy с именем пользователя и токеном
        self.symbols = {}  # Информация о тикерах
        self.new_bars = []  # Новые бары по всем подпискам на тикеры из Alor
        self.positions = collections.defaultdict(Position)  # Список позиций
        self.orders = collections.OrderedDict()  # Список заявок, отправленных на биржу

    def start(self):
        self.apProvider.OnNewBar = self.on_new_bar  # Обработчик новых баров по подписке из Alor

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        """Выдача уведомлений хранилища"""
        self.notifs.append(None)
        return [x for x in iter(self.notifs.popleft, None)]

    def stop(self):
        self.apProvider.OnNewBar = self.apProvider.DefaultHandler  # Возвращаем обработчик по умолчанию
        self.apProvider.CloseWebSocket()  # Перед выходом закрываем соединение с WebSocket

    # Функции

    def get_symbol_info(self, exchange, symbol, reload=False):
        """Получение информации тикера

        :param str exchange: Биржа 'MOEX' или 'SPBX'
        :param str symbol: Тикер
        :param bool reload: Получить информацию с Alor
        :return: Значение из кэша/Alor или None, если тикер не найден
        """
        if reload or (exchange, symbol) not in self.symbols:  # Если нужно получить информацию с Alor или нет информации о тикере в справочнике
            symbol_info = self.apProvider.GetSymbol(exchange, symbol)  # Получаем информацию о тикере с Alor
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
        """Перевод цен из BackTrader в Alor

        :param str exchange: Биржа 'MOEX' или 'SPBX'
        :param str symbol: Тикер
        :param float price: Цена в BackTrader
        :return: Цена в Alor
        """
        si = self.get_symbol_info(exchange, symbol)  # Информация о тикере
        primary_board = si['primary_board']  # Рынок тикера
        if primary_board == 'TQOB':  # Для рынка облигаций
            price /= 10  # цену делим на 10
        min_step = si['minstep']  # Минимальный шаг цены
        decimals = max(0, str(min_step)[::-1].find('.'))  # Из шага цены получаем кол-во знаков после запятой
        return round(price, decimals)  # Округляем цену

    def alor_to_bt_price(self, exchange, symbol, price: float):
        """Перевод цен из Alor в BackTrader

        :param str exchange: Биржа 'MOEX' или 'SPBX'
        :param str symbol: Тикер
        :param float price: Цена в Alor
        :return: Цена в BackTrader
        """
        si = self.get_symbol_info(exchange, symbol)  # Информация о тикере
        primary_board = si['primary_board']  # Рынок тикера
        if primary_board == 'TQOB':  # Для рынка облигаций
            price *= 10  # цену умножаем на 10
        return price

    # ALBroker

    def get_positions(self):
        """Все активные позиции по всем клиентски портфелям и биржам"""
        portfolios = self.apProvider.GetPortfolios()  # Портфели: Фондовый рынок / Фьючерсы и опционы / Валютный рынок
        for p in portfolios:  # Пробегаемся по всем портфелям
            for exchange in self.apProvider.exchanges:  # Пробегаемся по всем биржам
                positions = self.apProvider.GetPositions(p, exchange, True)  # Получаем все позиции без денежной позиции
                for position in positions:  # Пробегаемся по всем позициям
                    symbol = position['symbol']  # Тикер
                    dataname = self.exchange_symbol_to_data_name(exchange, symbol)  # Название тикера
                    si = self.get_symbol_info(exchange, symbol)  # Информация о тикере
                    size = position['qty'] * si['lotsize']  # Кол-во в штуках
                    price = round(position['volume'] / size, 2)  # Цена входа
                    self.positions[dataname] = Position(size, price)  # Сохраняем в списке открытых позиций

    def get_order(self, order_number):
        """Заявка BackTrader по номеру заявки на бирже

        :param Order order_number: Номер заявки на бирже
        :return: Заявка BackTrader
        """
        for order in self.orders.values():  # Пробегаемся по всем заявкам на бирже
            if order.info['order_number'] == order_number:  # Если значение совпадает с номером заявки на бирже
                return order  # то возвращаем заявкe BackTrader
        return None  # иначе, ничего не найдено

    # ALData

    def on_new_bar(self, response):
        """Обработка событий получения новых баров"""
        self.new_bars.append(response)  # Добавляем новый бар в список новых баров
