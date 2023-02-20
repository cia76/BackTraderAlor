from datetime import datetime, time
import backtrader as bt
from BackTraderAlor.ALStore import ALStore  # Хранилище Alor
from AlorPy.Config import Config  # Файл конфигурации


class MultiPortfolio(bt.Strategy):
    """Работа со множеством портфелей:
    - Свободные средства/баланс по всем портфелям
    - Свободные средства/баланс по указанному портфелю
    - Баланс по тикеру
    - Постановка/отмена заявок по разным портфелям
    """
    params = (  # Параметры торговой системы
        ('LimitPct', 1),  # Заявка на покупку на n% ниже цены закрытия
    )

    def log(self, txt, dt=None):
        """Вывод строки с датой на консоль"""
        dt = bt.num2date(self.datas[0].datetime[0]) if not dt else dt  # Заданная дата или дата текущего бара
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}, {txt}')  # Выводим дату и время с заданным текстом на консоль

    def __init__(self):
        """Инициализация торговой системы"""
        self.orders = {}  # Организовываем заявки в виде справочника, т.к. у каждого тикера может быть только одна активная заявка
        for d in self.datas:  # Пробегаемся по всем тикерам
            self.orders[d._name] = None  # Заявки по тикеру пока нет
        self.isLive = False  # Сначала будут приходить исторические данные, затем перейдем в режим реальной торговли

    def start(self):
        """Запуск торговой системы"""
        print('\nВсе рынки')
        print('- Свободные средства:', '%.2f' % self.broker.getcash())
        print('- Стоимость позиций :', '%.2f' % self.broker.getvalue())

        print(f'\nФондовый рынок ({Config.PortfolioStocks})')
        print('- Свободные средства:', '%.2f' % self.broker.getcash(portfolio=Config.PortfolioStocks))
        print('- Стоимость позиций :', '%.2f' % self.broker.getvalue(portfolio=Config.PortfolioStocks))

        print(f'\nСрочный рынок ({Config.PortfolioFutures})')
        print('- Свободные средства:', '%.2f' % self.broker.getcash(portfolio=Config.PortfolioFutures))
        print('- Стоимость позиций :', '%.2f' % self.broker.getvalue(portfolio=Config.PortfolioFutures))

        for d in self.datas:  # Пробегаемся по всем тикерам
            # Если по тикеру нет позиции, то будет Ошибка сервера: 404 Symbol <symbol> does not exist! <PreparedRequest [GET]>
            print('\nБаланс по тикеру', d._name, ':', '%.2f\n' % self.broker.getvalue((d,)))

    def next(self):
        """Получение следующего исторического/нового бара"""
        if not self.isLive:  # Если не в режиме реальной торговли
            return  # то выходим, дальше не продолжаем
        if not all(d.datetime[0] == self.data.datetime[0] for d in self.datas):  # Если пришли бары не по всем тикерам
            return  # то выходим, дальше не продолжаем
        for d in self.datas:  # По этим тикерам будет работать с заявками
            order = self.orders[d._name]  # Заявка тикера
            if order and order.status == bt.Order.Submitted:  # Если заявка не на бирже (отправлена брокеру)
                return  # то ждем постановки заявки на бирже, выходим, дальше не продолжаем
            if not self.getposition(d):  # Если позиции нет
                if order and order.status == bt.Order.Accepted:  # Если заявка на бирже (принята брокером)
                    self.cancel(order)  # то снимаем ее
                size = 1 if d._name == self.datas[0]._name else 10  # Выставляем 1 фьючерс / 10 акций
                limit_price = d.close[0] * (1 - self.p.LimitPct / 100)  # На n% ниже цены закрытия
                # portfolio = Config.PortfolioStocks if d == self.datas[1] else Config.PortfolioFutures  # Первый тикер торгуем по портфелю фондового рынка, второй - по портфеллю срочного рынка
                # self.orders[d._name] = self.buy(data=d, exectype=bt.Order.Limit, size=size, price=limit_price, portfolio=portfolio)  # Лимитная заявка на покупку для заданного портфеля
                self.orders[d._name] = self.buy(data=d, exectype=bt.Order.Limit, size=size, price=limit_price)  # Если не указать портфель, то он поставится по умолчанию для площадки тикера
            else:  # Если позиция есть
                self.orders[d._name] = self.close()  # Заявка на закрытие позиции по рыночной цене

    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статуса приходящих баров"""
        data_status = data._getstatusname(status)  # Получаем статус (только при LiveBars=True)
        self.log(f'{data._name} - {data_status}', datetime.now())  # Не можем вывести в лог, т.к. первый статус DELAYED получаем до первого бара (и его даты)
        self.isLive = data_status == 'LIVE'  # Режим реальной торговли

    def notify_order(self, order: bt.Order):
        """Изменение статуса заявки"""
        order_data_name = order.data._name  # Тикер заявки
        self.log(f'Заявка номер {order.ref} {order.info["order_number"]} {order.getstatusname()} {"Покупка" if order.isbuy() else "Продажа"} {order_data_name} {order.size} @ {order.price}')
        if order.status == bt.Order.Completed:  # Если заявка полностью исполнена
            if order.isbuy():  # Заявка на покупку
                self.log(f'Покупка {order_data_name} @{order.executed.price:.2f}, Цена {order.executed.value:.2f}, Комиссия {order.executed.comm:.2f}')
            else:  # Заявка на продажу
                self.log(f'Продажа {order_data_name} @{order.executed.price:.2f}, Цена {order.executed.value:.2f}, Комиссия {order.executed.comm:.2f}')
            self.orders[order_data_name] = None  # Сбрасываем заявку на вход в позицию

    def notify_trade(self, trade):
        """Изменение статуса позиции"""
        if trade.isclosed:  # Если позиция закрыта
            self.log(f'Прибыль по закрытой позиции {trade.getdataname()} Общая={trade.pnl:.2f}, Без комиссии={trade.pnlcomm:.2f}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbol_stocks = 'MOEX.SBER'  # Тикер
    symbol_futures = 'MOEX.SI-3.23'  # Для фьючерсов: <Код тикера заглавными буквами>-<Месяц экспирации: 3, 6, 9, 12>.<Последние 2 цифры года>
    store = ALStore(UserName=Config.UserName, RefreshToken=Config.RefreshToken, Boards=Config.Boards, Accounts=Config.Accounts)  # Хранилище Alor
    cerebro = bt.Cerebro(stdstats=False, quicknotify=True)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    broker = store.getbroker(use_positions=False)  # Брокер Alor без привязки к портфелю/бирже
    cerebro.setbroker(broker)  # Устанавливаем брокера
    for symbol in (symbol_futures, symbol_stocks):  # Пробегаемся по всем тикерам
        data = store.getdata(dataname=symbol, timeframe=bt.TimeFrame.Minutes, compression=1, LiveBars=True,  # Исторические и новые минутные бары
                             fromdate=datetime(2023, 2, 13), sessionstart=time(10, 5), sessionend=time(18, 35))  # Для тикеров, которые торгуются в разное время обязательно ставим начало/окончание торгов
        cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(MultiPortfolio, LimitPct=1)  # Добавляем торговую систему с лимитным входом в n%
    cerebro.run()  # Запуск торговой системы
