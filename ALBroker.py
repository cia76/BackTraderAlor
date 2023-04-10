from typing import Union  # Объединение типов
import collections
from datetime import datetime

from backtrader import BrokerBase, Order, BuyOrder, SellOrder
from backtrader.position import Position
from backtrader.utils.py3 import with_metaclass

from BackTraderAlor import ALStore

from AlorPy import AlorPy


class MetaALBroker(BrokerBase.__class__):
    def __init__(self, name, bases, dct):
        super(MetaALBroker, self).__init__(name, bases, dct)  # Инициализируем класс брокера
        ALStore.BrokerCls = self  # Регистрируем класс брокера в хранилище Алор


class ALBroker(with_metaclass(MetaALBroker, BrokerBase)):
    """Брокер Алор"""
    params = (
        ('provider_name', None),  # Название провайдера. Если не задано, то первое название по ключу name
        ('use_positions', True),  # При запуске брокера подтягиваются текущие позиции с биржи
        ('boards', None),  # Привязка портфелей/серверов для стоп заявок к площадкам из Config
        ('accounts', None),  # Привязка портфелей к биржам из Config
    )

    def __init__(self, **kwargs):
        super(ALBroker, self).__init__()
        self.store = ALStore(**kwargs)  # Хранилище Алор
        self.provider_name = self.p.provider_name if self.p.provider_name else list(self.store.providers.keys())[0]  # Название провайдера, или первое название по ключу name
        self.provider: AlorPy = self.store.providers[self.provider_name]  # Провайдер
        self.portfolios_accounts = {}  # Справочник кодов портфелей/счетов провайдеров
        for market in self.provider.GetPortfolios().values():  # Пробегаемся по всем рынкам: Фондовый рынок / Фьючерсы и опционы / Валютный рынок
            for portfolio in market:  # Пробегаемся по всем портфелям рынка
                p = portfolio['portfolio']  # Номер портфеля
                if p in self.p.accounts:  # Если есть привязка портфеля к бирже
                    self.portfolios_accounts[p] = portfolio['tks']  # то добавляем код портфеля/счета в список
        self.notifs = collections.deque()  # Очередь уведомлений брокера о заявках
        self.startingcash = self.cash = 0  # Стартовые и текущие свободные средства по счету
        self.startingvalue = self.value = 0  # Стартовая и текущая стоимость позиций
        self.cash_value = {}  # Справочник Свободные средства/Стоимость позиций
        self.positions = collections.defaultdict(Position)  # Список позиций
        self.orders = collections.OrderedDict()  # Список заявок, отправленных на биржу
        self.ocos = {}  # Список связанных заявок (One Cancel Others)
        self.pcs = collections.defaultdict(collections.deque)  # Очередь всех родительских/дочерних заявок (Parent - Children)

    def start(self):
        super(ALBroker, self).start()
        self.provider.OnPosition = self.on_position  # Обработка позиций
        self.provider.OnTrade = self.on_trade  # Обработка сделок
        self.provider.OnOrder = self.on_order  # Обработка заявок
        if self.p.use_positions:  # Если нужно при запуске брокера получить текущие позиции на бирже
            self.get_all_active_positions()  # то получаем их
        self.startingcash = self.cash = self.getcash()  # Стартовые и текущие свободные средства по счету. Подписка на позиции для портфеля/биржи
        self.startingvalue = self.value = self.getvalue()  # Стартовая и текущая стоимость позиций

    def getcash(self, portfolio=None):
        """Свободные средства по портфелю/бирже, по всем счетам"""
        if self.store.BrokerCls:  # Если брокер есть в хранилище
            cash = 0  # Будем набирать свободные средства по каждому портфелю на каждой бирже
            portfolios = (portfolio,) if portfolio else self.portfolios_accounts.keys()  # Указанный портфель или все портфели провайдера
            for portfolio in portfolios:  # Пробегаемся по всем заданным портфелям
                for exchange in self.get_exchanges(portfolio):  # Пробегаемся по всем биржам портфеля
                    if not self.is_subscribed(portfolio, exchange):  # Если нет подписок портфеля/биржи
                        self.subscribe(portfolio, exchange)  # то подписываемся на события портфеля/биржи
                        m = self.provider.GetMoney(portfolio, exchange)  # Денежная позиция
                        c = round(m['cash'], 2)  # Округляем до копеек
                        v = round(m['portfolio'] - m['cash'], 2)  # Вычитаем, округляем до копеек
                        self.cash_value[(portfolio, exchange)] = (c, v)  # Свободные средства/Стоимость позиций
                    c, _ = self.cash_value[(portfolio, exchange)]  # Получаем значение из подписки
                    cash += round(c, 2)  # Суммируем, округляем до копеек
            self.cash = cash  # Свободные средства по каждому портфелю на каждой бирже
        return self.cash

    def getvalue(self, datas=None, portfolio=None):
        """Стоимость позиции, позиций по портфелю/бирже, всех позиций"""
        if self.store.BrokerCls:  # Если брокер есть в хранилище
            value = 0  # Будем набирать стоимость позиций
            if datas is not None:  # Если получаем по тикерам
                for data in datas:  # Пробегаемся по всем тикерам
                    exchange, symbol = self.store.data_name_to_exchange_symbol(data._name)  # По тикеру получаем биржу и код тикера
                    si = self.store.get_symbol_info(exchange, symbol)  # Информация о тикере
                    if not si:  # Если тикер не найден
                        continue  # то переходим к следующему тикеру, дальше не продолжаем
                    portfolio = self.get_portfolio(si['primary_board'])  # Площадка, где торгуется тикер
                    if not portfolio:  # Если портфель не найден
                        continue  # то переходим к следующему тикеру, дальше не продолжаем
                    position = self.provider.GetPosition(portfolio, exchange, symbol)  # Пробуем получить позицию
                    if not position:  # Если не получили позицию
                        continue  # то переходим к следующему тикеру, дальше не продолжаем
                    value += round(position['volume'] + position['unrealisedPl'] * si['priceMultiplier'], 2)  # Текущая стоимость позиции по тикеру
                self.value = value  # Стоимость всех позиций по тикерам
            else:  # Если получаем по портфелям/биржам
                portfolios = (portfolio,) if portfolio else self.portfolios_accounts.keys()  # Указанный портфель или все портфели провайдера
                for portfolio in portfolios:  # Пробегаемся по всем портфелям
                    for exchange in self.get_exchanges(portfolio):  # Пробегаемся по всем биржам портфеля
                        if not self.is_subscribed(portfolio, exchange):  # Если нет подписок портфеля/биржи
                            self.subscribe(portfolio, exchange)  # то подписываемся на события портфеля/биржи
                            m = self.provider.GetMoney(portfolio, exchange)  # Денежная позиция
                            c = round(m['cash'], 2)  # Округляем до копеек
                            v = round(m['portfolio'] - m['cash'], 2)  # Вычитаем, округляем до копеек
                            self.cash_value[(portfolio, exchange)] = (c, v)  # Свободные средства/Стоимость позиций
                        _, v = self.cash_value[portfolio, exchange]  # Получаем значение из подписки
                        value += round(v, 2)  # Суммируем, округляем до копеек
                    self.value = value  # Стоимость позиций по портфелю/бирже, всех позиций
        return self.value

    def getposition(self, data):
        """Позиция по тикеру
        Используется в strategy.py для закрытия (close) и ребалансировки (увеличения/уменьшения) позиции:
        - В процентах от портфеля (order_target_percent)
        - До нужного кол-ва (order_target_size)
        - До нужного объема (order_target_value)
        """
        return self.positions[data._name]  # Получаем позицию по тикеру или нулевую позицию, если тикера в списке позиций нет

    def buy(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, oco=None, trailamount=None, trailpercent=None, parent=None, transmit=True, **kwargs):
        """Заявка на покупку"""
        order = self.create_order(owner, data, size, price, plimit, exectype, valid, oco, parent, transmit, True, **kwargs)
        self.notifs.append(order.clone())  # Уведомляем брокера о принятии/отклонении зявки на бирже
        return order

    def sell(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, oco=None, trailamount=None, trailpercent=None, parent=None, transmit=True, **kwargs):
        """Заявка на продажу"""
        order = self.create_order(owner, data, size, price, plimit, exectype, valid, oco, parent, transmit, False, **kwargs)
        self.notifs.append(order.clone())  # Уведомляем брокера о принятии/отклонении зявки на бирже
        return order

    def cancel(self, order):
        """Отмена заявки"""
        return self.cancel_order(order)

    def get_notification(self):
        if not self.notifs:  # Если в списке уведомлений ничего нет
            return None  # то ничего и возвращаем, выходим, дальше не продолжаем
        return self.notifs.popleft()  # Удаляем и возвращаем крайний левый элемент списка уведомлений

    def next(self):
        self.notifs.append(None)  # Добавляем в список уведомлений пустой элемент

    def stop(self):
        super(ALBroker, self).stop()
        self.unsubscribe()  # Отменяем все подписки
        self.provider.OnPosition = self.provider.DefaultHandler  # Обработка позиций
        self.provider.OnTrade = self.provider.DefaultHandler  # Обработка сделок
        self.provider.OnOrder = self.provider.DefaultHandler  # Обработка заявок
        self.store.BrokerCls = None  # Удаляем класс брокера из хранилища

    # Функции

    def get_portfolio(self, primary_board):
        """Получение портфеля

        :param str primary_board: Площадка, где торгуется тикер
        :return: Портфель
        """
        if primary_board not in self.p.boards:  # Если площадка не существует в справочнике площадки
            return None  # то портфель не найден
        return self.p.boards[primary_board][0]  # Возвращаем портфель

    def get_server(self, primary_board):
        """Получение торгового сервера для стоп заявок

        :param str primary_board: Площадка, где торгуется тикер
        :return: Код торгового сервера
        """
        if primary_board not in self.p.boards:  # Если площадка не существует в справочнике площадки
            return None  # то торговый сервер не найден
        return self.p.boards[primary_board][1]  # Возвращаем торговый сервер

    def get_exchanges(self, portfolio):
        """Получение бирж для портфеля

        :param str portfolio: Портфель
        :return: Кортеж бирж
        """
        if portfolio not in self.p.accounts:  # Если портфель не существует в справочнике счетов
            return None  # то биржи не найдены
        return self.p.accounts[portfolio]  # Возвращаем торговый сервер

    def is_subscribed(self, portfolio, exchange):
        """Проверка наличия подписки

        :param str portfolio: Клиентский портфель
        :param str exchange: Биржа 'MOEX' или 'SPBX
        """
        for guid in self.provider.subscriptions.keys():  # Пробегаемся по всем подпискам
            subscription = self.provider.subscriptions[guid]  # Подписка
            if subscription['portfolio'] == portfolio and subscription['exchange'] == exchange:  # Если есть в списке подписок
                return True  # то подписка есть
        return False  # иначе, подписки нет

    def subscribe(self, portfolio, exchange):
        """Подписка на позиции, сделки и заявки

        :param str portfolio: Клиентский портфель
        :param str exchange: Биржа 'MOEX' или 'SPBX'
        """
        self.provider.PositionsGetAndSubscribeV2(portfolio, exchange)  # Подписка на позиции (получение свободных средств и стоимости позиций)
        self.provider.TradesGetAndSubscribeV2(portfolio, exchange)  # Подписка на сделки (изменение статусов заявок)
        self.provider.OrdersGetAndSubscribeV2(portfolio, exchange)  # Подписка на заявки (снятие заявок с биржи)

    def unsubscribe(self):
        """Отмена всех подписок"""
        subscriptions = self.provider.subscriptions.copy()  # Работаем с копией подписок, т.к. будем удалять элементы
        for guid, subscription_request in subscriptions.items():  # Пробегаемся по всем подпискам
            if subscription_request['opcode'] in \
                    ('PositionsGetAndSubscribeV2',  # Если это подписка на позиции (получение свободных средств и стоимости позиций)
                     'TradesGetAndSubscribeV2',  # или подписка на сделки (изменение статусов заявок)
                     'OrdersGetAndSubscribeV2'):  # или подписка на заявки (снятие заявок с биржи)
                self.provider.Unsubscribe(guid)  # то отменяем подписку

    def get_all_active_positions(self):
        """Все активные позиции по всем клиентским портфелям и биржам"""
        for portfolio in self.portfolios_accounts.keys():  # Пробегаемся по всем портфелям провайдера
            for exchange in self.get_exchanges(portfolio):  # Пробегаемся по всем биржам портфеля
                positions = self.provider.GetPositions(portfolio, exchange, True)  # Получаем все позиции без денежной позиции
                for position in positions:  # Пробегаемся по всем позициям
                    symbol = position['symbol']  # Тикер
                    dataname = self.store.exchange_symbol_to_data_name(exchange, symbol)  # Название тикера
                    si = self.store.get_symbol_info(exchange, symbol)  # Информация о тикере
                    size = position['qty'] * si['lotsize']  # Кол-во в штуках. Отрицательное для коротких позиций
                    price = round(position['volume'] / size, 2)  # Цена входа
                    self.positions[dataname] = Position(size, price)  # Сохраняем в списке открытых позиций

    def get_order(self, order_number) -> Union[Order, None]:
        """Заявка BackTrader по номеру заявки на бирже

        :param order_number: Номер заявки на бирже
        :return: Заявка BackTrader или None
        """
        for order in self.orders.values():  # Пробегаемся по всем заявкам на бирже
            if order.info['order_number'] == order_number:  # Если нашли совпадение с номером заявки на бирже
                return order  # то возвращаем заявку BackTrader
        return None  # иначе, ничего не найдено

    def create_order(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, oco=None, parent=None, transmit=True, is_buy=True, **kwargs):
        """Создание заявки. Привязка параметров счета и тикера. Обработка связанных и родительской/дочерних заявок
        Даполнительные параметры передаются через **kwargs:
        - portfolio - Портфель для площадки. Если не задан, то берется из Config.Boards
        - server - Торговый сервер для стоп заявок. Если не задан, то берется из Config.Boards
        """
        order = BuyOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, valid=valid, oco=oco, parent=parent, transmit=transmit) if is_buy \
            else SellOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, valid=valid, oco=oco, parent=parent, transmit=transmit)  # Заявка на покупку/продажу
        order.addcomminfo(self.getcommissioninfo(data))  # По тикеру выставляем комиссии в заявку. Нужно для исполнения заявки в BackTrader
        order.addinfo(**kwargs)  # Передаем в заявку все дополнительные свойства из брокера, в т.ч. portfolio, server
        exchange, symbol = self.store.data_name_to_exchange_symbol(data._name)  # По тикеру получаем биржу и тикера
        order.addinfo(exchange=exchange, symbol=symbol)  # В заявку заносим код биржи exchange и тикер symbol
        if order.exectype in (Order.Close, Order.StopTrail, Order.StopTrailLimit, Order.Historical):  # Эти типы заявок не реализованы
            print(f'Постановка заявки {order.ref} по тикеру {exchange}.{symbol} отклонена. Работа с заявками {order.exectype} не реализована')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        si = self.store.get_symbol_info(exchange, symbol)  # Информация о тикере
        if not si:  # Если тикер не найден
            print(f'Постановка заявки {order.ref} по тикеру {exchange}.{symbol} отклонена. Тикер не найден')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        if 'portfolio' not in order.info:  # Если при постановке заявки не указали портфель
            portfolio = self.get_portfolio(si['primary_board'])  # Площадка, где торгуется тикер
            if not portfolio:  # Если портфель не найден
                print(f'Постановка заявки {order.ref} по тикеру {exchange}.{symbol} отклонена. Портфель (portfolio) не найден')
                order.reject(self)  # то отклоняем заявку
                self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
                return order  # Возвращаем отклоненную заявку
            order.addinfo(portfolio=portfolio)  # то ставим портфель из брокера
        else:  # Если при постановке заявки портфель был указан
            portfolio = order.info['portfolio']  # то получаем его
        if not self.is_subscribed(portfolio, exchange):  # Если нет подписок портфеля/биржи
            self.subscribe(portfolio, exchange)  # то подписываемся на события портфеля/биржи
        if order.exectype != Order.Market and not order.price:  # Если цена заявки не указана для всех заявок, кроме рыночной
            price_type = 'Лимитная' if order.exectype == Order.Limit else 'Стоп'  # Для стоп заявок это будет триггерная (стоп) цена
            print(f'Постановка заявки {order.ref} по тикеру {exchange}.{symbol} отклонена. {price_type} цена (price) не указана для заявки типа {order.exectype}')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        if order.exectype == Order.StopLimit and not order.pricelimit:  # Если лимитная цена не указана для стоп-лимитной заявки
            print(f'Постановка заявки {order.ref} по тикеру {exchange}.{symbol} отклонена. Лимитная цена (pricelimit) не указана для заявки типа {order.exectype}')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        if order.exectype in (Order.Stop, Order.StopLimit):  # Для стоп/стоп-лимитных заявок
            if 'server' not in order.info:  # Если для стоп заявки не указан торговый сервер
                server = self.get_server(si['primary_board'])  # то ищем его в справочнике площадки
                if not server:  # Если торговый сервер не найден
                    print(f'Постановка заявки {order.ref} по тикеру {exchange}.{symbol} отклонена. Торговый сервер (server) не найден для заявки типа {order.exectype}')
                    order.reject(self)  # то отклоняем заявку
                    self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
                    return order  # Возвращаем отклоненную заявку
                order.addinfo(server=server)  # Указываем торговый сервер для стоп заявок
        if oco:  # Если есть связанная заявка
            self.ocos[order.ref] = oco.ref  # то заносим в список связанных заявок
        if not transmit or parent:  # Для родительской/дочерних заявок
            parent_ref = getattr(order.parent, 'ref', order.ref)  # Номер транзакции родительской заявки или номер заявки, если родительской заявки нет
            if order.ref != parent_ref and parent_ref not in self.pcs:  # Если есть родительская заявка, но она не найдена в очереди родительских/дочерних заявок
                print(f'Постановка заявки {order.ref} по тикеру {exchange}.{symbol} отклонена. Родительская заявка не найдена')
                order.reject(self)  # то отклоняем заявку
                self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
                return order  # Возвращаем отклоненную заявку
            pcs = self.pcs[parent_ref]  # В очередь к родительской заявке
            pcs.append(order)  # добавляем заявку (родительскую или дочернюю)
        if transmit:  # Если обычная заявка или последняя дочерняя заявка
            if not parent:  # Для обычных заявок
                return self.place_order(order)  # Отправляем заявку на биржу
            else:  # Если последняя заявка в цепочке родительской/дочерних заявок
                self.notifs.append(order.clone())  # Удедомляем брокера о создании новой заявки
                return self.place_order(order.parent)  # Отправляем родительскую заявку на биржу
        # Если не последняя заявка в цепочке родительской/дочерних заявок (transmit=False)
        return order  # то возвращаем созданную заявку со статусом Created. На биржу ее пока не ставим

    def place_order(self, order: Order):
        """Отправка заявки на биржу"""
        side = 'buy' if order.isbuy() else 'sell'  # Покупка/продажа
        portfolio = order.info['portfolio']  # Портфель
        account = self.portfolios_accounts[portfolio]  # Счет портфеля провайдера
        exchange = order.info['exchange']  # Код биржи
        symbol = order.info['symbol']  # Код тикера
        si = self.store.get_symbol_info(exchange, symbol)  # Информация о тикере
        quantity = abs(order.size // si['lotsize'])  # Размер позиции в лотах. В Алор всегда передается положительный размер лота
        response = None  # Результат запроса
        if order.exectype == Order.Market:  # Рыночная заявка
            response = self.provider.CreateMarketOrder(portfolio, exchange, symbol, side, quantity)
        elif order.exectype == Order.Limit:  # Лимитная заявка
            limit_price = self.store.bt_to_alor_price(exchange, symbol, order.price)  # Лимитная цена
            response = self.provider.CreateLimitOrder(portfolio, exchange, symbol, side, quantity, limit_price)
        elif order.exectype == Order.Stop:  # Стоп заявка
            server = order.info['server']  # Торговый сервер для стоп заявок
            stop_price = self.store.bt_to_alor_price(exchange, symbol, order.price)  # Стоп цена
            response = self.provider.CreateStopLossOrder(server, account, portfolio, exchange, symbol, side, quantity, stop_price)
        elif order.exectype == Order.StopLimit:  # Стоп-лимитная заявка
            server = order.info['server']  # Торговый сервер для стоп заявок
            stop_price = self.store.bt_to_alor_price(exchange, symbol, order.price)  # Стоп цена
            limit_price = self.store.bt_to_alor_price(exchange, symbol, order.pricelimit)  # Лимитная цена
            response = self.provider.CreateStopLossLimitOrder(server, account, portfolio, exchange, symbol, side, quantity, stop_price, limit_price)
        order.submit(self)  # Отправляем заявку на биржу
        self.notifs.append(order.clone())  # Уведомляем брокера об отправке заявки на биржу
        if not response:  # Если при отправке заявки на биржу произошла веб ошибка
            print(f'Постановка заявки по тикеру {exchange}.{symbol} отклонена. Ошибка веб сервиса')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        order.accept(self)  # Заявка принята на бирже
        self.orders[order.ref] = order  # Сохраняем в списке заявок, отправленных на биржу
        order.addinfo(order_number=response['orderNumber'])  # Сохраняем пришедший номер заявки на бирже
        return order  # Возвращаем заявку

    def cancel_order(self, order):
        """Отмена заявки"""
        if not order.alive():  # Если заявка уже была завершена
            return  # то выходим, дальше не продолжаем
        portfolio = order.info['portfolio']  # Портфель
        order_number = order.info['order_number']  # Номер заявки на бирже
        if order.exectype in (Order.Market, Order.Limit):  # Для рыночных и лимитных заявок
            exchange = order.info['exchange']  # Код биржи
            self.provider.DeleteOrder(portfolio, exchange, order_number, False)  # Снятие заявки
        else:  # Для стоп заявок
            server = order.info['server']  # Торговый сервер
            self.provider.DeleteStopOrder(server, portfolio, order_number, True)  # Снятие стоп заявки
        return order  # В список уведомлений ничего не добавляем. Ждем события on_order

    def oco_pc_check(self, order):
        """
        Проверка связанных заявок
        Проверка родительской/дочерних заявок
        """
        ocos = self.ocos.copy()  # Пока ищем связанные заявки, они могут измениться. Поэтому, работаем с копией
        for order_ref, oco_ref in ocos.items():  # Пробегаемся по списку связанных заявок
            if oco_ref == order.ref:  # Если в заявке номер эта заявка указана как связанная (по номеру транзакции)
                self.cancel_order(self.orders[order_ref])  # то отменяем заявку
        if order.ref in ocos.keys():  # Если у этой заявки указана связанная заявка
            oco_ref = ocos[order.ref]  # то получаем номер транзакции связанной заявки
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

    def on_position(self, response):
        """Обработка денежных позиций"""
        data = response['data']  # Данные позиции
        if not data['isCurrency']:  # Если пришли не валютные остатки (деньги)
            return  # то выходим, дальше не продолжаем
        c = round(data['volume'], 2)  # Свободные средства округляем до копеек
        portfolio = data['portfolio']  # Портфель
        exchange = data['exchange']  # Биржа
        m = self.provider.GetMoney(portfolio, exchange)  # Денежная позиция
        v = round(m['portfolio'] - data['volume'], 2)  # Суммируем, округляем до копеек
        self.cash_value[(portfolio, exchange)] = (c, v)  # Свободные средства/Стоимость позиций

    def on_order(self, response):
        """Обработка заявок на отмену (canceled). Статусы working, filled, rejected обрабатываются в place_order и on_trade"""
        data = response['data']  # Данные заявки
        if data['status'] != 'canceled':  # Нас интересует только отмена заявки
            return  # иначе, выходим, дальше не продолжаем
        order_number = data['id']  # Номер заявки из сделки
        order: Order = self.get_order(order_number)  # Заявка BackTrader
        if not order:  # Если заявки нет в BackTrader (не из автоторговли)
            return  # то выходим, дальше не продолжаем
        order.cancel()  # Отменяем существующую заявку
        self.notifs.append(order.clone())  # Уведомляем брокера об отмене заявки
        self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки (Canceled)

    def on_trade(self, response):
        """Обработка сделок"""
        data = response['data']  # Данные сделки
        order_no = data['orderno']  # Номер заявки из сделки
        order = self.get_order(order_no)  # Заявка BackTrader
        if not order:  # Если заявки нет в BackTrader (не из автоторговли)
            return  # то выходим, дальше не продолжаем
        size = data['qtyUnits']  # Кол-во в штуках. Всегда положительное
        if data['side'] == 'sell':  # Если сделка на продажу
            size *= -1  # то кол-во ставим отрицательным
        price = abs(data['price'] / size)  # Цена исполнения за штуку
        str_utc = data['date'][0:19]  # Возвращается зн-ие типа: '2023-02-16T09:25:01.4335364Z'. Берем первые 20 символов
        dt_utc = datetime.strptime(str_utc, '%Y-%m-%dT%H:%M:%S')  # Переводим в дату/время UTC
        dt = self.provider.UTCToMskDateTime(dt_utc)  # Дата/время сделки по времени биржи (МСК)
        pos = self.getposition(order.data)  # Получаем позицию по тикеру или нулевую позицию если тикера в списке позиций нет
        psize, pprice, opened, closed = pos.update(size, price)  # Обновляем размер/цену позиции на размер/цену сделки
        order.execute(dt, size, price, closed, 0, 0, opened, 0, 0, 0, 0, psize, pprice)  # Исполняем заявку в BackTrader
        if order.executed.remsize:  # Если осталось что-то к исполнению
            if order.status != order.Partial:  # Если заявка переходит в статус частичного исполнения (может исполняться несколькими частями)
                order.partial()  # то заявка частично исполнена
                self.notifs.append(order.clone())  # Уведомляем брокера о частичном исполнении заявки
        else:  # Если зничего нет к исполнению
            order.completed()  # то заявка полностью исполнена
            self.notifs.append(order.clone())  # Уведомляем брокера о полном исполнении заявки
            # Снимаем oco-заявку только после полного исполнения заявки
            # Если нужно снять oco-заявку на частичном исполнении, то прописываем это правило в ТС
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки (Completed)
