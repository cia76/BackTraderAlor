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
        self.newBars = []  # Новые бары по подписке из Alor
        self.positions = collections.defaultdict(Position)  # Список позиций
        self.orders = collections.OrderedDict()  # Список заявок, отправленных на биржу
        self.order_numbers = {}  # Словарь заявок на бирже. Индекс - номер заявки в BackTrader (order.ref). Значение - номер заявки на бирже (orderNumber)
        self.pcs = collections.defaultdict(collections.deque)  # Очередь всех родительских/дочерних заявок (Parent - Children)
        self.ocos = {}  # Список связанных заявок (One Cancel Others)

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

    def get_order_ref(self, order_no):
        """Номер заявки BackTrader по номеру заявки на бирже

        :param Order order_no: Номер заявки на бирже
        :return: Номер заявки BackTrader
        """
        for ref, order_number in self.order_numbers.items():  # Пробегаемся по всем заявкам на бирже
            if order_number == order_no:  # Если значение совпадает с номером заявки на бирже
                return ref  # то возвращаем номер заявки BackTrader
        return None  # иначе, ничего не найдено

    def cancel_order(self, order):
        """Отмена заявки"""
        portfolio = order.info['portfolio']  # Портфель
        order_number = self.order_numbers[order.ref]  # Номер заявки на бирже
        if order.exectype in (Order.Market, Order.Limit):  # Для рыночных и лимитных заявок
            exchange = order.info['exchange']  # Код биржи
            self.apProvider.DeleteOrder(portfolio, exchange, order_number, False)  # Снятие заявки
        else:  # Для стоп заявок
            server = order.info['server']  # Торговый сервер
            self.apProvider.DeleteStopOrder(server, portfolio, order_number, True)  # Снятие стоп заявки
        order.cancel()  # Отменяем заявку
        return order  # Возвращаем заявку

    def oco_pc_check(self, order):
        """
        Проверка связанных заявок
        Проверка родительской/дочерних заявок
        """
        for order_ref, oco_ref in self.ocos.items():  # Пробегаемся по списку связанных заявок
            if oco_ref == order.ref:  # Если в заявке номер эта заявка указана как связанная (по номеру транзакции)
                self.cancel_order(self.orders[order_ref])  # то отменяем заявку
        if order.ref in self.ocos.keys():  # Если у этой заявки указана связанная заявка
            oco_ref = self.ocos[order.ref]  # то получаем номер транзакции связанной заявки
            self.cancel_order(self.orders[oco_ref])  # отменяем связанную заявку

        if not order.parent and not order.transmit and order.status == Order.Completed:  # Если исполнена родительская заявка
            pcs = self.pcs[order.ref]  # Получаем очередь родительской/дочерних заявок
            for child in pcs:  # Пробегаемся по всем заявкам
                if child.parent:  # Пропускаем первую (родительскую) заявку
                    self.place_order(child)  # Отправляем дочернюю заявку на биржу
        elif order.parent:  # Если исполнена/отменена дочерняя заявка
            pcs = self.pcs[order.parent.ref]  # Получаем очередь родительской/дочерних заявок
            for child in pcs:  # Пробегаемся по всем заявкам
                if child.parent and child.ref != order.ref:  # Пропускаем первую (родительскую) заявку и исполненную заявку
                    self.cancel_order(child)  # Отменяем дочернюю заявку

    # ALData

    def on_new_bar(self, response):
        """Обработка событий получения новых баров"""
        self.newBars.append(response)  # Добавляем новый бар в список новых баров
