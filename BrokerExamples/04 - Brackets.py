from datetime import datetime, time
import backtrader as bt
from BackTraderAlor.ALStore import ALStore  # Хранилище Alor
from AlorPy.Config import Config  # Файл конфигурации


class Brackets(bt.Strategy):
    """
    Выставляем родительскую заявку на покупку на n% ниже цены закрытия
    Вместе с ней выставляем дочерние заявки на выход с n% убытком/прибылью
    При исполнении родительской заявки выставляем все дочерние
    При исполнении дочерней заявки отменяем все остальные неисполненные дочерние
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
        self.order = None  # Заявка на вход в позицию
        self.isLive = False  # Сначала будут приходить исторические данные, затем перейдем в режим реальной торговли

    def next(self):
        """Получение следующего исторического/нового бара"""
        if not self.isLive:  # Если не в режиме реальной торговли
            return  # то выходим, дальше не продолжаем
        if self.order and self.order.status == bt.Order.Submitted:  # Если заявка не исполнена (отправлена брокеру)
            return  # то ждем исполнения, выходим, дальше не продолжаем
        if not self.position:  # Если позиции нет
            if self.order and self.order.status == bt.Order.Accepted:  # Если заявка не исполнена (принята брокером)
                self.cancel(self.order)  # то снимаем заявку на вход
            close_minus_n = self.data.close[0] * (1 - self.p.LimitPct / 100)  # Цена на n% ниже цены закрытия
            close_minus_2n = self.data.close[0] * (1 - self.p.LimitPct / 100 * 2)  # Цена на 2n% ниже цены закрытия
            # self.order = self.buy(exectype=bt.Order.Limit, price=close_minus_n, transmit=False)  # Родительская лимитная заявка на покупку
            # orderStop = self.sell(exectype=bt.Order.Stop, price=close_minus_2n, size=self.order.size, parent=self.order, transmit=False)  # Дочерняя стоп заявка на продажу с убытком n%
            # orderLimit = self.sell(exectype=bt.Order.Limit, price=self.close[0], size=self.order.size, parent=self.order, transmit=True)  # Дочерняя лимитная заявка на продажу с прибылью n%
            # self.order, orderStop, orderLimit = self.buy_bracket(limitprice=self.data.close[0], price=close_minus_n, stopprice=close_minus_2n, server=Config.TradeServerCode)  # Bracket заявка в BT
            self.order, orderStop, orderLimit = self.buy_bracket(limitprice=self.data.close[0], price=close_minus_n, stopprice=close_minus_2n)  # Если не указать сервер для стоп заявки, то он поставится по умолчанию для площадки тикера

    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статуса приходящих баров"""
        data_status = data._getstatusname(status)  # Получаем статус (только при LiveBars=True)
        print(data._name, '-', data_status)  # Не можем вывести в лог, т.к. первый статус DELAYED получаем до первого бара (и его даты)
        self.isLive = data_status == 'LIVE'  # Режим реальной торговли

    def notify_order(self, order):
        """Изменение статуса заявки"""
        order_data_name = order.data._name  # Тикер заявки
        self.log(f'Заявка номер {order.ref} {order.info["order_number"]} {order.getstatusname()} {"Покупка" if order.isbuy() else "Продажа"} {order_data_name} {order.size} @ {order.price}')
        if order.status == bt.Order.Completed:  # Если заявка полностью исполнена
            if order.isbuy():  # Заявка на покупку
                self.log(f'Покупка {order_data_name} @{order.executed.price:.2f}, Цена {order.executed.value:.2f}, Комиссия {order.executed.comm:.2f}')
            else:  # Заявка на продажу
                self.log(f'Продажа {order_data_name} @{order.executed.price:.2f}, Цена {order.executed.value:.2f}, Комиссия {order.executed.comm:.2f}')
            self.order = None  # Сбрасываем заявку на вход в позицию

    def notify_trade(self, trade):
        """Изменение статуса позиции"""
        if trade.isclosed:  # Если позиция закрыта
            self.log(f'Прибыль по закрытой позиции {trade.getdataname()} Общая={trade.pnl:.2f}, Без комиссии={trade.pnlcomm:.2f}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    exchange = 'MOEX'  # Биржа
    portfolio = Config.PortfolioStocks  # Портфель фондового рынка
    symbol = 'MOEX.SBER'  # Тикер
    # portfolio = Config.PortfolioFutures  # Портфель срочного рынка
    # symbol = 'MOEX.SI-3.23'  # Для фьючерсов: <Код тикера заглавными буквами>-<Месяц экспирации: 3, 6, 9, 12>.<Последние 2 цифры года>
    cerebro = bt.Cerebro(stdstats=False, quicknotify=True)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    store = ALStore(UserName=Config.UserName, RefreshToken=Config.RefreshToken)  # Хранилище Alor
    broker = store.getbroker(use_positions=False, portfolio=portfolio, exchange=exchange)  # Брокер Alor
    cerebro.setbroker(broker)  # Устанавливаем брокера
    data = store.getdata(dataname=symbol, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=datetime(2023, 2, 13), LiveBars=True)  # Исторические и новые минутные бары за все время
    cerebro.adddata(data)  # Добавляем данные
    cerebro.addsizer(bt.sizers.FixedSize, stake=10)  # Кол-во акций для покупки/продажи
    # cerebro.addsizer(bt.sizers.FixedSize, stake=1)  # Кол-во фьючерсов для покупки/продажи
    cerebro.addstrategy(Brackets, LimitPct=1)  # Добавляем торговую систему с лимитным входом в n%
    cerebro.run()  # Запуск торговой системы
