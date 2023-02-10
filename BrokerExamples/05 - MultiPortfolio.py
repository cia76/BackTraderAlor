from datetime import datetime
import backtrader as bt
from BackTraderAlor.ALStore import ALStore  # Хранилище Alor
from AlorPy.Config import Config  # Файл конфигурации


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    exchange = 'MOEX'  # Биржа
    store = ALStore(UserName=Config.UserName, RefreshToken=Config.RefreshToken)  # Хранилище Alor

    print('Фондовый рынок')
    portfolio = Config.PortfolioStocks  # Портфель фондового рынка
    symbol = 'SU29006RMFS2'  # Тикер
    data = store.getdata(dataname=symbol, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=datetime(2023, 2, 10), LiveBars=False)  # Исторические бары
    broker = store.getbroker(use_positions=False, portfolio=portfolio, exchange=exchange)  # Брокер Alor
    print('Свободные средства по счету:', broker.getcash())
    print('Баланс счета:', broker.getvalue())
    print('Баланс', symbol, ':', broker.getvalue((data,)))

    # portfolio = Config.PortfolioFutures  # Портфель срочного рынка
    # symbol = 'MOEX.SiH3'  # Для фьючерсов: <Код тикера><Месяц экспирации: 3-H, 6-M, 9-U, 12-Z><Последняя цифра года>

