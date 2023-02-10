from datetime import datetime
import backtrader as bt
from BackTraderAlor.ALStore import ALStore  # Хранилище Alor
from AlorPy.Config import Config  # Файл конфигурации


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    store = ALStore(UserName=Config.UserName, RefreshToken=Config.RefreshToken)  # Хранилище Alor
    broker = store.getbroker(use_positions=False)  # Брокер Alor без привязки к портфелю/бирже
    broker.p.exchange = 'MOEX'  # Биржа

    print('\nВсе рынки')
    print('- Свободные средства:', broker.getcash())
    print('- Баланс:', broker.getvalue())

    broker.p.portfolio = Config.PortfolioStocks  # Портфель фондового рынка
    print(f'\nФондовый рынок ({broker.p.portfolio})')
    print('- Свободные средства:', broker.getcash())
    print('- Баланс:', broker.getvalue())
    symbol = 'SU29006RMFS2'  # Тикер
    data = store.getdata(dataname=symbol, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=datetime(2023, 2, 10), LiveBars=False)  # Исторические бары
    print('- Баланс', symbol, ':', broker.getvalue((data,)))

    broker.p.portfolio = Config.PortfolioFutures  # Портфель срочного рынка
    print(f'\nСрочный рынок ({broker.p.portfolio})')
    print('- Свободные средства:', broker.getcash())
    print('- Баланс:', broker.getvalue())
    symbol = 'MOEX.SiH3'  # Для фьючерсов: <Код тикера><Месяц экспирации: 3-H, 6-M, 9-U, 12-Z><Последняя цифра года>
    data = store.getdata(dataname=symbol, timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=datetime(2023, 2, 10), LiveBars=False)  # Исторические бары
    print('- Баланс', symbol, ':', broker.getvalue((data,)))
