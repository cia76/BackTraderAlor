from typing import Union  # Объединение типов
import collections
from datetime import datetime
import logging

from backtrader import BrokerBase, Order, BuyOrder, SellOrder
from backtrader.position import Position
from backtrader.utils.py3 import with_metaclass

from BackTraderAlor import ALStore, ALData


# noinspection PyArgumentList
class MetaALBroker(BrokerBase.__class__):
    def __init__(self, name, bases, dct):
        super(MetaALBroker, self).__init__(name, bases, dct)  # Инициализируем класс брокера
        ALStore.BrokerCls = self  # Регистрируем класс брокера в хранилище Алор


# noinspection PyProtectedMember,PyArgumentList,PyUnusedLocal
class ALBroker(with_metaclass(MetaALBroker, BrokerBase)):
    """Брокер Алор"""
    logger = logging.getLogger(f'ALBroker')  # Будем вести лог

    def __init__(self):
        super(ALBroker, self).__init__()
        self.store = ALStore()  # Хранилище Алор
        self.notifs = collections.deque()  # Очередь уведомлений брокера о заявках
        self.startingcash = self.cash = 0  # Стартовые и текущие свободные средства
        self.startingvalue = self.value = 0  # Стартовая и текущая стоимость позиций
        self.positions = collections.defaultdict(Position)  # Список позиций
        self.orders = collections.OrderedDict()  # Список заявок, отправленных на биржу
        self.ocos = {}  # Список связанных заявок (One Cancel Others)
        self.pcs = collections.defaultdict(collections.deque)  # Очередь всех родительских/дочерних заявок (Parent - Children)

        self.store.provider.on_position = self.on_position  # Обработка позиций
        self.store.provider.on_trade = self.on_trade  # Обработка сделок
        self.store.provider.on_order = self.on_order  # Обработка заявок
        self.store.provider.on_stop_order_v2 = self.on_stop_order_v2  # Обработка стоп-заявок

    def start(self):
        super(ALBroker, self).start()
        self.get_all_active_positions()  # Получаем все активные позиции, в т.ч. денежные

    def getcash(self, portfolio=None, exchange=None):
        """Свободные средства по портфелю/бирже, по всем счетам"""
        cash = 0  # Будем набирать свободные средства
        if self.store.BrokerCls:  # Если брокер есть в хранилище
            if portfolio and exchange:  # Если считаем свободные средства по портфелю/бирже
                cash = next((position.price for key, position in self.positions.items() if key[0] == portfolio and key[1] == exchange and not key[2]), None)  # Денежная позиция по портфелю/рынку
            else:  # Если считаем свободные средства по всем счетам
                cash = sum([position.price for key, position in self.positions.items() if not key[2]])  # Сумма всех денежных позиций
                self.cash = cash  # Сохраняем текущие свободные средства
        return cash

    def getvalue(self, datas=None, portfolio=None, exchange=None):
        """Стоимость позиции, позиций по портфелю/бирже, всех позиций"""
        value = 0  # Будем набирать стоимость позиций
        if self.store.BrokerCls:  # Если брокер есть в хранилище
            if datas:  # Если считаем стоимость позиции/позиций
                data: ALData  # Данные Алор
                for data in datas:  # Пробегаемся по всем тикерам
                    position = self.positions[(data.portfolio, data.exchange, data.board, data.symbol)]  # Позиция по тикеру
                    value += position.price * position.size  # Добавляем стоимость позиции по тикеру
            elif portfolio and exchange:  # Если считаем стоимость позиций по портфелю/бирже
                value = sum([position.price * position.size for key, position in self.positions.items() if key[0] == portfolio and key[1] == exchange and key[2]])  # Стоимость позиций по портфелю/бирже
            else:  # Если считаем стоимость всех позиций
                value = sum([position.price * position.size for key, position in self.positions.items() if key[2]])  # Стоимость всех позиций
                self.value = value  # Сохраняем текущую стоимость позиций
        return value

    def getposition(self, data: ALData):
        """Позиция по тикеру
        Используется в strategy.py для закрытия (close) и ребалансировки (увеличения/уменьшения) позиции:
        - В процентах от портфеля (order_target_percent)
        - До нужного кол-ва (order_target_size)
        - До нужного объема (order_target_value)
        """
        return self.positions[(data.portfolio, data.exchange, data.board, data.symbol)]  # Получаем позицию по тикеру или нулевую позицию, если тикера в списке позиций нет

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
        return self.notifs.popleft() if self.notifs else None  # Удаляем и возвращаем крайний левый элемент списка уведомлений или ничего

    def next(self):
        self.notifs.append(None)  # Добавляем в список уведомлений пустой элемент

    def stop(self):
        super(ALBroker, self).stop()
        self.unsubscribe()  # Отменяем все подписки
        self.store.provider.on_position = self.store.provider.default_handler  # Обработка позиций
        self.store.provider.on_trade = self.store.provider.default_handler  # Обработка сделок
        self.store.provider.on_order = self.store.provider.default_handler  # Обработка заявок
        self.store.provider.on_stop_order_v2 = self.store.provider.default_handler  # Обработка стоп-заявок
        self.store.BrokerCls = None  # Удаляем класс брокера из хранилища

    # Функции

    def is_subscribed(self, portfolio, exchange):
        """Проверка наличия подписки

        :param str portfolio: Клиентский портфель
        :param str exchange: Биржа 'MOEX' или 'SPBX
        """
        for guid in self.store.provider.subscriptions.keys():  # Пробегаемся по всем подпискам
            subscription = self.store.provider.subscriptions[guid]  # Подписка
            if 'portfolio' not in subscription or 'exchange' not in subscription:  # Если подписка не по портфелю/бирже (например, на бары)
                continue  # то переходим к следующей подписке
            if subscription['portfolio'] == portfolio and subscription['exchange'] == exchange:  # Если есть в списке подписок
                return True  # то подписка есть
        return False  # иначе, подписки нет

    def subscribe(self, portfolio, exchange):
        """Подписка на позиции, сделки, заявки и стоп заявки

        :param str portfolio: Клиентский портфель
        :param str exchange: Биржа 'MOEX' или 'SPBX'
        """
        self.store.provider.positions_get_and_subscribe_v2(portfolio, exchange)  # Подписка на позиции (получение свободных средств и стоимости позиций)
        self.store.provider.trades_get_and_subscribe_v2(portfolio, exchange)  # Подписка на сделки (изменение статусов заявок)
        self.store.provider.orders_get_and_subscribe_v2(portfolio, exchange)  # Подписка на заявки (снятие заявок с биржи)
        self.store.provider.stop_orders_get_and_subscribe_v2(portfolio, exchange)  # Подписка на стоп-заявки (исполнение или снятие заявок с биржи)

    def unsubscribe(self):
        """Отмена всех подписок"""
        subscriptions = self.store.provider.subscriptions.copy()  # Работаем с копией подписок, т.к. будем удалять элементы
        for guid, subscription_request in subscriptions.items():  # Пробегаемся по всем подпискам
            if subscription_request['opcode'] in \
                    ('PositionsGetAndSubscribeV2',  # Если это подписка на позиции (получение свободных средств и стоимости позиций)
                     'TradesGetAndSubscribeV2',  # или подписка на сделки (изменение статусов заявок)
                     'OrdersGetAndSubscribeV2',  # или подписка на заявки (снятие заявок с биржи)
                     'StopOrdersGetAndSubscribeV2'):  # или подписка на стоп-заявки (исполнение или снятие заявок с биржи)
                self.store.provider.unsubscribe(guid)  # то отменяем подписку

    def get_all_active_positions(self):
        """Все активные позиции в т.ч. денежные по всем клиентским портфелям и биржам"""
        cash = 0  # Будем набирать свободные средства
        value = 0  # Будем набирать стоимость позиций
        for account in self.store.provider.accounts:  # Пробегаемся по всем счетам
            portfolio = account['portfolio']  # Портфель
            for exchange in account['exchanges']:  # Пробегаемся по всем биржам
                positions = self.store.provider.get_positions(portfolio, exchange, False)  # Позиции с денежной позицией
                for position in positions:  # Пробегаемся по всем позициям
                    symbol = position['symbol']  # Тикер
                    if position['isCurrency']:  # Если пришли валютные остатки (деньги)
                        board = ''  # Для свободных средств нет кода режима торгов
                        size = 1  # Кол-во
                        price = position['volume']  # Размер свободных средств
                        cash += price  # Увеличиваем общий размер свободных средств
                    else:  # Если пришла позиция
                        si = self.store.provider.get_symbol_info(exchange, symbol)  # Информация о тикере
                        board = si['board']  # Код режима торгов
                        size = position['qty'] * si['lotsize']  # Кол-во в штуках
                        price = self.store.provider.alor_price_to_price(exchange, symbol, position['avgPrice'])  # Цена входа
                        value += price * size  # Увеличиваем общий размер стоимости позиций
                    self.positions[(portfolio, exchange, board, symbol)] = Position(size, price)  # Сохраняем в списке открытых позиций
        self.cash = cash  # Сохраняем текущие свободные средства
        self.value = value  # Сохраняем текущую стоимость позиций

    def get_order(self, order_number) -> Union[Order, None]:
        """Заявка BackTrader по номеру заявки на бирже
        Пробегаемся по всем заявкам на бирже. Если нашли совпадение с номером заявки на бирже, то возвращаем заявку BackTrader. Иначе, ничего не найдено

        :param order_number: Номер заявки на бирже
        :return: Заявка BackTrader или None
        """
        return next((order for order in self.orders.values() if order.info['order_number'] == order_number), None)

    def create_order(self, owner, data: ALData, size, price=None, plimit=None, exectype=None, valid=None, oco=None, parent=None, transmit=True, is_buy=True, **kwargs):
        """Создание заявки. Привязка параметров счета и тикера. Обработка связанных и родительской/дочерних заявок
        Даполнительные параметры передаются через **kwargs:
        - account_id - Порядковый номер счета
        """
        order = BuyOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, valid=valid, oco=oco, parent=parent, transmit=transmit) if is_buy \
            else SellOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, valid=valid, oco=oco, parent=parent, transmit=transmit)  # Заявка на покупку/продажу
        order.addcomminfo(self.getcommissioninfo(data))  # По тикеру выставляем комиссии в заявку. Нужно для исполнения заявки в BackTrader
        order.addinfo(**kwargs)  # Передаем в заявку все дополнительные параметры, в т.ч. account_id
        if order.exectype in (Order.Close, Order.StopTrail, Order.StopTrailLimit, Order.Historical):  # Эти типы заявок не реализованы
            self.logger.warning(f'Постановка заявки {order.ref} по тикеру {data.board}.{data.symbol} на бирже {data.exchange} отклонена. Работа с заявками {order.exectype} не реализована')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        portfolio = self.store.provider.get_account(data.board, order.info['account_id'])['portfolio'] if 'account_id' in order.info else data.portfolio  # Портфель из заявки/тикера
        order.addinfo(portfolio=portfolio)  # Сохраняем в заявке
        if not self.is_subscribed(portfolio, data.exchange):  # Если нет подписок портфеля/биржи
            self.subscribe(portfolio, data.exchange)  # то подписываемся на события портфеля/биржи
        if order.exectype != Order.Market and not order.price:  # Если цена заявки не указана для всех заявок, кроме рыночной
            price_type = 'Лимитная' if order.exectype == Order.Limit else 'Стоп'  # Для стоп заявок это будет триггерная (стоп) цена
            self.logger.warning(f'Постановка заявки {order.ref} по тикеру {data.board}.{data.symbol} на бирже {data.exchange} отклонена. {price_type} цена (price) не указана для заявки типа {order.exectype}')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        if order.exectype == Order.StopLimit and not order.pricelimit:  # Если лимитная цена не указана для стоп-лимитной заявки
            self.logger.warning(f'Постановка заявки {order.ref} по тикеру {data.board}.{data.symbol} на бирже {data.exchange} отклонена. Лимитная цена (pricelimit) не указана для заявки типа {order.exectype}')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        if oco:  # Если есть связанная заявка
            self.ocos[order.ref] = oco.ref  # то заносим в список связанных заявок
        if not transmit or parent:  # Для родительской/дочерних заявок
            parent_ref = getattr(order.parent, 'ref', order.ref)  # Номер транзакции родительской заявки или номер заявки, если родительской заявки нет
            if order.ref != parent_ref and parent_ref not in self.pcs:  # Если есть родительская заявка, но она не найдена в очереди родительских/дочерних заявок
                self.logger.warning(f'Постановка заявки {order.ref} по тикеру {data.board}.{data.symbol} на бирже {data.exchange} отклонена. Родительская заявка не найдена')
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
        return order  # то возвращаем созданную заявку со статусом Created. На биржу ее пока не отправляем

    def place_order(self, order: Order):
        """Отправка заявки на биржу"""
        portfolio = order.info['portfolio']  # Портфель
        exchange = order.data.exchange  # Биржа тикера
        class_code = order.data.board  # Код режима торгов
        symbol = order.data.symbol  # Тикер
        side = 'buy' if order.isbuy() else 'sell'  # Покупка/продажа
        si = self.store.provider.get_symbol_info(exchange, symbol)  # Информация о тикере
        quantity = abs(order.size // si['lotsize'])  # Размер позиции в лотах. В Алор всегда передается положительный размер лота
        response = None  # Результат запроса
        if order.exectype == Order.Market:  # Рыночная заявка
            response = self.store.provider.create_market_order(portfolio, exchange, symbol, side, quantity)
        elif order.exectype == Order.Limit:  # Лимитная заявка
            limit_price = self.store.provider.price_to_alor_price(exchange, symbol, order.price)  # Лимитная цена
            response = self.store.provider.create_limit_order(portfolio, exchange, symbol, side, quantity, limit_price)
        elif order.exectype == Order.Stop:  # Стоп заявка
            stop_price = self.store.provider.price_to_alor_price(exchange, symbol, order.price)  # Стоп цена
            condition = 'MoreOrEqual' if order.isbuy() else 'LessOrEqual'  # Условие срабатывания стоп цены
            response = self.store.provider.create_stop_order(portfolio, exchange, symbol, class_code, side, quantity, stop_price, condition)
        elif order.exectype == Order.StopLimit:  # Стоп-лимитная заявка
            stop_price = self.store.provider.price_to_alor_price(exchange, symbol, order.price)  # Стоп цена
            limit_price = self.store.provider.price_to_alor_price(exchange, symbol, order.pricelimit)  # Лимитная цена
            condition = 'MoreOrEqual' if order.isbuy() else 'LessOrEqual'  # Условие срабатывания стоп цены
            response = self.store.provider.create_stop_limit_order(portfolio, exchange, symbol, class_code, side, quantity, stop_price, limit_price, condition)
        order.submit(self)  # Отправляем заявку на биржу (Order.Submitted)
        self.notifs.append(order.clone())  # Уведомляем брокера об отправке заявки на биржу
        if not response:  # Если при отправке заявки на биржу произошла веб ошибка
            board = order.info['board']  # Режим торгов
            self.logger.warning(f'Постановка заявки по тикеру {board}.{symbol} на бирже {exchange} отклонена. Ошибка веб сервиса')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        order.addinfo(order_number=response['orderNumber'])  # Сохраняем пришедший номер заявки на бирже
        order.accept(self)  # Заявка принята на бирже (Order.Accepted)
        self.orders[order.ref] = order  # Сохраняем заявку в списке заявок, отправленных на биржу
        return order  # Возвращаем заявку

    def cancel_order(self, order):
        """Отмена заявки"""
        if not order.alive():  # Если заявка уже была завершена
            return  # то выходим, дальше не продолжаем
        portfolio = order.info['portfolio']  # Портфель
        exchange = order.data.exchange  # Код биржи
        order_number = order.info['order_number']  # Номер заявки на бирже
        if order.exectype in (Order.Market, Order.Limit):  # Для рыночных и лимитных заявок
            self.store.provider.delete_order(portfolio, exchange, order_number, False)  # Снятие заявки
        else:  # Для стоп заявок
            self.store.provider.delete_order(portfolio, exchange, order_number, True)  # Снятие стоп заявки
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
        """Обработка позиций"""
        position = response['data']  # Данные позиции
        portfolio = position['portfolio']  # Портфель
        exchange = position['exchange']  # Биржа
        symbol = position['symbol']  # Тикер
        if position['isCurrency']:  # Если пришли валютные остатки (деньги)
            board = None  # Для свободных средств нет кода режима торгов
            size = 1  # Кол-во свободных средств
            price = position['volume']  # Размер свободных средств
        else:  # Если пришла позиция
            si = self.store.provider.get_symbol_info(exchange, symbol)  # Информация о тикере
            board = si['board']  # Код режима торгов
            size = position['qty'] * si['lotsize']  # Кол-во в штуках
            price = self.store.provider.alor_price_to_price(exchange, symbol, position['avgPrice'])  # Цена входа
        self.positions[(portfolio, exchange, board, symbol)] = Position(size, price)  # Сохраняем в списке открытых позиций

    def on_order(self, response):
        """Обработка рыночных и лимитных заявок на отмену (canceled). Статусы working, filled, rejected обрабатываются в place_order и on_trade"""
        data = response['data']  # Данные заявки
        status = data['status']  # Статус заявки: working - на исполнении, filled - исполнена, canceled - отменена, rejected - отклонена
        if status != 'canceled':  # Для рыночной или лимитной заявки интересует только отмена заявки
            return  # иначе, выходим, дальше не продолжаем
        order_number = data['id']  # Номер заявки из сделки
        order: Order = self.get_order(order_number)  # Заявка BackTrader
        if not order:  # Если заявки нет в BackTrader (не из автоторговли)
            return  # то выходим, дальше не продолжаем
        order.cancel()  # Отменяем существующую заявку
        self.notifs.append(order.clone())  # Уведомляем брокера об отмене заявки
        self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки (Canceled)

    def on_stop_order_v2(self, response):
        """Обработка стоп-заявок на отмену (canceled) и исполнение (filled). Статусы working и rejected обрабатываются в place_order и on_trade"""
        data = response['data']  # Данные заявки
        status = data['status']  # Статус заявки: working - на исполнении, filled - исполнена, canceled - отменена, rejected - отклонена
        if status not in ('filled', 'canceled'):  # Для стоп-заявки интересует отмена и исполнение, которое не приводит к сделке
            return  # иначе, выходим, дальше не продолжаем
        order_number = data['id']  # Номер заявки из сделки
        order: Order = self.get_order(order_number)  # Заявка BackTrader
        if not order:  # Если заявки нет в BackTrader (не из автоторговли)
            return  # то выходим, дальше не продолжаем
        if status == 'filled':  # Для исполненной стоп-заявки
            order.completed()  # Заявка полностью исполнена
        else:  # Для отмененной рыночной, лимитной, стоп-заявки
            order.cancel()  # Отменяем существующую заявку
        self.notifs.append(order.clone())  # Уведомляем брокера об отмене заявки
        self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки (Canceled)

    def on_trade(self, response):
        """Обработка сделок"""
        data = response['data']  # Данные сделки
        if data['existing']:  # При (пере)подключении к серверу передаются сделки как из истории, так и новые. Если сделка из истории
            return  # то выходим, дальше не продолжаем
        order_no = data['orderno']  # Номер заявки из сделки
        order = self.get_order(order_no)  # Заявка BackTrader
        if not order:  # Если заявки нет в BackTrader (не из автоторговли)
            return  # то выходим, дальше не продолжаем
        size = data['qtyUnits']  # Кол-во в штуках. Всегда положительное
        if data['side'] == 'sell':  # Если сделка на продажу
            size *= -1  # то кол-во ставим отрицательным
        price = abs(data['price'])  # Цена исполнения за штуку
        str_utc = data['date'][:19]  # Возвращается значение типа: '2023-02-16T09:25:01.4335364Z'. Берем первые 20 символов до точки перед наносекундами
        dt_utc = datetime.strptime(str_utc, '%Y-%m-%dT%H:%M:%S')  # Переводим в дату/время UTC
        dt = self.store.provider.utc_to_msk_datetime(dt_utc)  # Дата и время сделки по времени биржи (МСК)
        pos = self.getposition(order.data)  # Получаем позицию по тикеру или нулевую позицию если тикера в списке позиций нет
        psize, pprice, opened, closed = pos.update(size, price)  # Обновляем размер/цену позиции на размер/цену сделки
        order.execute(dt, size, price, closed, 0, 0, opened, 0, 0, 0, 0, psize, pprice)  # Исполняем заявку в BackTrader
        if order.executed.remsize:  # Если осталось что-то к исполнению
            if order.status != order.Partial:  # Если заявка переходит в статус частичного исполнения (может исполняться несколькими частями)
                order.partial()  # то заявка частично исполнена
                self.notifs.append(order.clone())  # Уведомляем брокера о частичном исполнении заявки
        else:  # Если ничего нет к исполнению
            order.completed()  # то заявка полностью исполнена
            self.notifs.append(order.clone())  # Уведомляем брокера о полном исполнении заявки
            # Снимаем oco-заявку только после полного исполнения заявки
            # Если нужно снять oco-заявку на частичном исполнении, то прописываем это правило в ТС
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки (Completed)
