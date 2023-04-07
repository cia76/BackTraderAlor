from datetime import date, timedelta
from backtrader import Cerebro, TimeFrame
from BackTraderAlor.ALStore import ALStore  # Хранилище Alor
from AlorPy.Config import Config  # Файл конфигурации
import Strategy  # Торговые системы

# Несколько тикеров для нескольких торговых систем по одному временнОму интервалу
if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbols = ('MOEX.SBER', 'MOEX.GAZP', 'MOEX.LKOH', 'MOEX.GMKN',)  # Кортеж тикеров
    store = ALStore(providers=[dict(provider_name='alor_trade', username=Config.UserName, demo=False, refresh_token=Config.RefreshToken)])  # Хранилище Alor
    cerebro = Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    today = date.today()  # Сегодняшняя дата без времени
    week_ago = today - timedelta(days=7)  # Дата неделю назад без времени
    for symbol in symbols:  # Пробегаемся по всем тикерам
        data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=1, fromdate=week_ago, live_bars=True)  # Исторические и новые бары тикера с начала сессии
        cerebro.adddata(data)  # Добавляем тикер
    cerebro.addstrategy(Strategy.PrintStatusAndBars, name="One Ticker", symbols=('MOEX.SBER',))  # Добавляем торговую систему по одному тикеру
    cerebro.addstrategy(Strategy.PrintStatusAndBars, name="Two Tickers", symbols=('MOEX.GAZP', 'MOEX.LKOH',))  # Добавляем торговую систему по двум тикерам
    cerebro.addstrategy(Strategy.PrintStatusAndBars, name="All Tickers")  # Добавляем торговую систему по всем тикерам
    cerebro.run()  # Запуск торговой системы
