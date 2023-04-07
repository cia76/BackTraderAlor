from datetime import date, timedelta
from backtrader import Cerebro, TimeFrame
from BackTraderAlor.ALStore import ALStore  # Хранилище Alor
from AlorPy.Config import Config  # Файл конфигурации
import Strategy  # Торговые системы

# Несколько временнЫх интервалов по одному тикеру: Получение большего временнОго интервала из меньшего (Resample)
if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbol = 'MOEX.SBER'  # Тикер
    store = ALStore(providers=[dict(provider_name='alor_trade', username=Config.UserName, demo=False, refresh_token=Config.RefreshToken)])  # Хранилище Alor
    cerebro = Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    today = date.today()  # Сегодняшняя дата без времени
    week_ago = today - timedelta(days=7)  # Дата неделю назад без времени
    data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=1, fromdate=week_ago)  # Исторические данные по самому меньшему временному интервалу
    cerebro.adddata(data)  # Добавляем данные из QUIK для проверки исходников
    cerebro.resampledata(data, timeframe=TimeFrame.Minutes, compression=5, boundoff=1)  # Можно добавить больший временной интервал кратный меньшему (добавляется автоматом)
    data1 = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=5, fromdate=week_ago)
    cerebro.adddata(data1)  # Добавляем данные из QUIK для проверки правильности работы Resample
    cerebro.addstrategy(Strategy.PrintStatusAndBars)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
    cerebro.plot()  # Рисуем график
