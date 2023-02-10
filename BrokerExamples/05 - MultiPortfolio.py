from datetime import datetime
import backtrader as bt
from BackTraderAlor.ALStore import ALStore  # Хранилище Alor
from AlorPy.Config import Config  # Файл конфигурации


class MultiPortfolio(bt.Strategy):
    """Работа со множеством портфелей"""
    # TODO Сделать постановку/снятие заявок по разным портфелям
    params = (  # Параметры торговой системы
        ('portfolio', None),  # Портфель
    )

    def __init__(self):
        """Инициализация торговой системы"""
        print('\nВсе рынки')
        print('- Свободные средства:', self.broker.getcash())
        print('- Баланс:', self.broker.getvalue())

        self.broker.p.exchange = 'MOEX'  # Биржа
        self.broker.p.portfolio = Config.PortfolioStocks  # Портфель фондового рынка
        print(f'\nФондовый рынок ({self.broker.p.portfolio})')
        print('- Свободные средства:', self.broker.getcash())
        print('- Баланс:', self.broker.getvalue())

        self.broker.p.portfolio = Config.PortfolioFutures  # Портфель срочного рынка
        print(f'\nСрочный рынок ({self.broker.p.portfolio})')
        print('- Свободные средства:', self.broker.getcash())
        print('- Баланс:', self.broker.getvalue())

        if self.p.portfolio:  # Если в ТС пришел портфель тикера
            self.broker.p.portfolio = self.p.portfolio  # То устанавливаем портфель тикера
            print('\nБаланс по тикеру', self.data._name, ':', self.broker.getvalue((self.data,)))


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbol = 'MOEX.SU29006RMFS2'  # Тикер, позиция по которому у вас есть
    store = ALStore(UserName=Config.UserName, RefreshToken=Config.RefreshToken)  # Хранилище Alor
    cerebro = bt.Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    broker = store.getbroker(use_positions=False)  # Брокер Alor без привязки к портфелю/бирже
    cerebro.setbroker(broker)  # Устанавливаем брокера
    data = store.getdata(dataname=symbol, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=datetime(2023, 2, 10), LiveBars=False)  # Исторические бары
    cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(MultiPortfolio, portfolio=Config.PortfolioStocks)  # Добавляем торговую систему с портфелем
    cerebro.run()  # Запуск торговой системы
