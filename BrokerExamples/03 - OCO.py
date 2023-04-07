from datetime import date, timedelta
import backtrader as bt
from BackTraderAlor.ALStore import ALStore  # Хранилище Alor
from AlorPy.Config import Config  # Файл конфигурации


class OCO(bt.Strategy):
    """
    Выставляем заявку на покупку на n% ниже цены закрытия
    Выставляем зависимую заявку на продажу на n% выше цены закрытия
    Если за 1 бар ни одна заявка не срабатывает, то закрываем их
    Если заявка срабатывает, то закрываем позицию. Неважно, с прибылью или убытком
    """
    params = (  # Параметры торговой системы
        ('LimitPct', 1),  # Заявка на покупку/продажу на n% ниже/выше цены закрытия
    )

    def log(self, txt, dt=None):
        """Вывод строки с датой на консоль"""
        dt = bt.num2date(self.datas[0].datetime[0]) if not dt else dt  # Заданная дата или дата текущего бара
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}, {txt}')  # Выводим дату и время с заданным текстом на консоль

    def __init__(self):
        """Инициализация торговой системы"""
        self.isLive = False  # Сначала будут приходить исторические данные, затем перейдем в режим реальной торговли
        self.order = None  # Заявка на вход в позицию

    def next(self):
        """Получение следующего исторического/нового бара"""
        if not self.isLive:  # Если не в режиме реальной торговли
            return  # то выходим, дальше не продолжаем
        if self.order and self.order.status == bt.Order.Submitted:  # Если заявка не исполнена (отправлена брокеру)
            return  # то ждем исполнения, выходим, дальше не продолжаем
        if not self.position:  # Если позиции нет
            if self.order and self.order.status == bt.Order.Accepted:  # Если заявка не исполнена (принята брокером)
                self.cancel(self.order)  # то снимаем ее
            close_minus_n = self.data.close[0] * (1 - self.p.LimitPct / 100)  # На n% ниже цены закрытия
            self.order = self.buy(exectype=bt.Order.Limit, price=close_minus_n)  # Лимитная заявка на покупку
            close_plus_n = self.data.close[0] * (1 + self.p.LimitPct / 100)  # На n% выше цены закрытия
            self.sell(exectype=bt.Order.Limit, price=close_plus_n, oco=self.order)  # Зависимая лимитная заявка на короткую продажу
        else:  # Если позиция есть
            self.order = self.close()  # Заявка на закрытие позиции по рыночной цене

    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статуса приходящих баров"""
        data_status = data._getstatusname(status)  # Получаем статус (только при LiveBars=True)
        print(data_status)  # Не можем вывести в лог, т.к. первый статус DELAYED получаем до первого бара (и его даты)
        self.isLive = data_status == 'LIVE'  # Режим реальной торговли

    def notify_order(self, order):
        """Изменение статуса заявки"""
        if order.status in (bt.Order.Created, bt.Order.Submitted, bt.Order.Accepted):  # Если заявка создана, отправлена брокеру, принята брокером (не исполнена)
            self.log(f'Alive Status: {order.getstatusname()}. TransId={order.ref}')
        elif order.status in (bt.Order.Canceled, bt.Order.Margin, bt.Order.Rejected, bt.Order.Expired):  # Если заявка отменена, нет средств, заявка отклонена брокером, снята по времени (снята)
            self.log(f'Cancel Status: {order.getstatusname()}. TransId={order.ref}')
        elif order.status == bt.Order.Partial:  # Если заявка частично исполнена
            self.log(f'Part Status: {order.getstatusname()}. TransId={order.ref}')
        elif order.status == bt.Order.Completed:  # Если заявка полностью исполнена
            if order.isbuy():  # Заявка на покупку
                self.log(f'Bought @{order.executed.price:.2f}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
            elif order.issell():  # Заявка на продажу
                self.log(f'Sold @{order.executed.price:.2f}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
            self.order = None  # Сбрасываем заявку на вход в позицию

    def notify_trade(self, trade):
        """Изменение статуса позиции"""
        if trade.isclosed:  # Если позиция закрыта
            self.log(f'Trade Profit, Gross={trade.pnl:.2f}, NET={trade.pnlcomm:.2f}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbol = 'MOEX.SBER'  # Тикер
    # symbol = 'MOEX.SI-6.23'  # Для фьючерсов: <Код тикера заглавными буквами>-<Месяц экспирации: 3, 6, 9, 12>.<Последние 2 цифры года>
    # symbol = 'MOEX.RTS-6.23'
    cerebro = bt.Cerebro(stdstats=False, quicknotify=True)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    today = date.today()  # Сегодняшняя дата без времени
    week_ago = today - timedelta(days=7)  # Дата неделю назад без времени
    store = ALStore(providers=[dict(provider_name='alor_trade', username=Config.UserName, demo=False, refresh_token=Config.RefreshToken)])  # Хранилище Alor
    broker = store.getbroker(use_positions=False, boards=Config.Boards, accounts=Config.Accounts)  # Брокер Alor
    cerebro.setbroker(broker)  # Устанавливаем брокера
    data = store.getdata(dataname=symbol, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=week_ago, live_bars=True)  # Исторические и новые минутные бары за все время
    cerebro.adddata(data)  # Добавляем данные
    cerebro.addsizer(bt.sizers.FixedSize, stake=10)  # Кол-во акций в штуках для покупки/продажи
    # cerebro.addsizer(bt.sizers.FixedSize, stake=1)  # Кол-во фьючерсов в штуках для покупки/продажи
    cerebro.addstrategy(OCO, LimitPct=1)  # Добавляем торговую систему с лимитным входом в n%
    cerebro.run()  # Запуск торговой системы
