from datetime import datetime, date, timedelta, time
from backtrader import Cerebro, TimeFrame
from BackTraderAlor.ALStore import ALStore, MOEXStocks, MOEXFutures  # Хранилище Alor. Расписания торгов фондового/срочного рынков
from AlorPy.Config import Config  # Файл конфигурации
import Strategy  # Торговые системы

# Исторические/новые бары тикера
if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbol = 'MOEX.SBER'  # Тикер в формате <Код биржи MOEX/SPBX>.<Код тикера>
    schedule = MOEXStocks()  # Расписание торгов фондового рынка
    # symbol = 'MOEX.Si-6.23'  # Для фьючерсов: <Код тикера>-<Месяц экспирации: 3, 6, 9, 12>.<Две последнии цифры года>
    # symbol = 'MOEX.SiM3'  # или <Код тикера><Месяц экспирации: 3-H, 6-M, 9-U, 12-Z><Последняя цифра года>
    # schedule = MOEXFutures()  # Расписание торгов срочного рынка
    store = ALStore(providers=[dict(provider_name='alor_trade', username=Config.UserName, demo=False, refresh_token=Config.RefreshToken)])  # Хранилище Alor
    cerebro = Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    today = date.today()  # Сегодняшняя дата без времени
    week_ago = today - timedelta(days=7)  # Дата неделю назад без времени

    # 1. Все исторические дневные бары
    # data = store.getdata(dataname=symbol, timeframe=TimeFrame.Days)

    # 2. Исторические часовые бары с дожи 4-х цен (four_price_doji) за текущий год
    # data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=60, fromdate=datetime(today.year, 1, 1), todate=datetime(today.year, 12, 31), four_price_doji=True)

    # 3. Исторические 30-и минутные бары с заданной даты неделю назад до последнего бара
    # data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=30, fromdate=week_ago)

    # 4. Исторические 5-и минутные бары первого часа текущей сессиИ без первой 5-и минутки
    # data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=5, fromdate=datetime(today.year, today.month, today.day, 10, 5), todate=datetime(today.year, today.month, today.day, 10, 55))

    # 5. Исторические 5-и минутные бары первого часа сессиЙ за неделю без первой 5-и минутки
    # data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=5, fromdate=week_ago, todate=today, sessionstart=time(10, 5), sessionend=time(11, 0))

    # 6. Исторические и новые минутные бары с начала сегодняшней сессии
    data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=1, fromdate=today, schedule=schedule, live_bars=True)

    cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(Strategy.PrintStatusAndBars)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
    cerebro.plot()  # Рисуем график
