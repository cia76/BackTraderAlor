from datetime import time, datetime
from backtrader import Cerebro, feeds, TimeFrame
from BackTraderAlor.ALStore import ALStore  # Хранилище Alor
from AlorPy.Config import Config  # Файл конфигурации
import Strategy  # Торговые системы

# Склейка истории тикера из файла и Alor (Rollover)
if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbol = 'MOEX.SBER'  # Тикер истории Alor
    d1 = feeds.GenericCSVData(  # Получаем историю из файла
        dataname=f'..\\..\\DataAlor\\{symbol}_D.txt',  # Файл для импорта из Alor. Создается из примера AlorPy 04 - Bars.py
        separator='\t',  # Колонки разделены табуляцией
        dtformat='%d.%m.%Y %H:%M',  # Формат даты/времени DD.MM.YYYY HH:MI
        openinterest=-1,  # Открытого интереса в файле нет
        sessionend=time(0, 0),  # Для дневных данных и выше подставляется время окончания сессии. Чтобы совпадало с историей, нужно поставить закрытие на 00:00
        fromdate=datetime(2020, 1, 1))  # Начальная дата и время приема исторических данных (Входит)
    store = ALStore(providers=[dict(provider_name='alor_trade', username=Config.UserName, demo=False, refresh_token=Config.RefreshToken)])  # Хранилище Alor
    d2 = store.getdata(dataname=symbol, timeframe=TimeFrame.Days, fromdate=datetime(2022, 12, 1), LiveBars=False)  # Получаем историю из Alor
    cerebro = Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    cerebro.rolloverdata(d1, d2, name=symbol)  # Склеенный тикер
    cerebro.addstrategy(Strategy.PrintStatusAndBars)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
    cerebro.plot()  # Рисуем график
