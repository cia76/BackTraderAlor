from backtrader import Cerebro, TimeFrame
from BackTraderAlor.ALStore import ALStore  # Хранилище Alor
from AlorPy.Config import Config  # Файл конфигурации
import Strategy as ts  # Торговые системы

# Использование меньшего временнОго интервала (Replay)
if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbol = 'MOEX.SBER'  # Тикер
    store = ALStore(UserName=Config.UserName, RefreshToken=Config.RefreshToken, Boards=Config.Boards, Accounts=Config.Accounts)  # Хранилище Alor
    cerebro = Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=5)  # Исторические данные по меньшему временному интервалу
    cerebro.replaydata(data, timeframe=TimeFrame.Days)  # На графике видим большой интервал, прогоняем ТС на меньшем
    cerebro.addstrategy(ts.PrintStatusAndBars)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
    cerebro.plot()  # Рисуем график
