from datetime import datetime, timedelta, time
from time import sleep

from backtrader.feed import AbstractDataBase
from backtrader.utils.py3 import with_metaclass
from backtrader import TimeFrame, date2num

from BackTraderAlor import ALStore

from AlorPy import AlorPy


class MetaALData(AbstractDataBase.__class__):
    def __init__(self, name, bases, dct):
        super(MetaALData, self).__init__(name, bases, dct)  # Инициализируем класс данных
        ALStore.DataCls = self  # Регистрируем класс данных в хранилище Алор


class ALData(with_metaclass(MetaALData, AbstractDataBase)):
    """Данные Алор"""
    params = (
        ('provider_name', None),  # Название провайдера. Если не задано, то первое название по ключу name
        ('four_price_doji', False),  # False - не пропускать дожи 4-х цен, True - пропускать
        ('schedule', None),  # Расписание работы биржи
        ('live_bars', False),  # False - только история, True - история и новые бары
    )

    def islive(self):
        """Если подаем новые бары, то Cerebro не будет запускать preload и runonce, т.к. новые бары должны идти один за другим"""
        return self.p.live_bars

    def __init__(self, **kwargs):
        if self.p.timeframe == TimeFrame.Days:  # Дневной временной интервал (по умолчанию)
            self.timeFrame = 'D'
        elif self.p.timeframe == TimeFrame.Weeks:  # Недельный временной интервал
            self.timeFrame = 'W'
        elif self.p.timeframe == TimeFrame.Months:  # Месячный временной интервал
            self.timeFrame = 'M'
        elif self.p.timeframe == TimeFrame.Years:  # Годовой временной интервал
            self.timeFrame = 'Y'
        elif self.p.timeframe == TimeFrame.Minutes:  # Минутный временной интервал
            self.timeFrame = self.p.compression * 60  # Переводим в секунды
        elif self.p.timeframe == TimeFrame.Seconds:  # Секундный временной интервал
            self.timeFrame = self.p.compression
        self.store = ALStore(**kwargs)  # Передаем параметры в хранилище Алор. Может работать самостоятельно, не через хранилище
        self.provider_name = self.p.provider_name if self.p.provider_name else list(self.store.providers.keys())[0]  # Название провайдера, или первое название по ключу name
        self.provider: AlorPy = self.store.providers[self.provider_name]  # Провайдер
        self.exchange, self.symbol = self.provider.dataname_to_exchange_symbol(self.p.dataname)  # По тикеру получаем биржу и код тикера
        self.history_bars = []  # Исторические бары после применения фильтров
        self.guid = None  # Идентификатор подписки на историю цен
        self.dt_last_open = datetime.min  # Дата/время открытия последнего полученного бара в BackTrader
        self.last_bar_received = False  # Получен последний бар
        self.live_mode = False  # Режим получения баров. False = История, True = Новые бары

    def setenvironment(self, env):
        """Добавление хранилища Алор в cerebro"""
        super(ALData, self).setenvironment(env)
        env.addstore(self.store)  # Добавление хранилища Алор в cerebro

    def start(self):
        super(ALData, self).start()
        self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) баров
        seconds_from = self.provider.msk_datetime_to_utc_time_stamp(self.p.fromdate) if self.p.fromdate else 0  # Дата и время начала выборки
        if not self.p.live_bars:  # Если получаем только историю
            seconds_to = self.provider.msk_datetime_to_utc_time_stamp(self.p.todate) if self.p.todate else 32536799999  # Дата и время окончания выборки
            history_bars = self.provider.get_history(self.exchange, self.symbol, self.timeFrame, seconds_from, seconds_to)['history']  # Получаем бары из Алор
            for bar in history_bars:  # Пробегаемся по всем полученным барам
                if self.is_bar_valid(bar):  # Если исторический бар соответствует всем условиям выборки
                    self.history_bars.append(bar)  # то добавляем бар
            if len(self.history_bars) > 0:  # Если был получен хотя бы 1 бар
                self.put_notification(self.CONNECTED)  # то отправляем уведомление о подключении и начале получения исторических баров
        else:  # Если получаем историю и новые бары
            self.guid = self.provider.bars_get_and_subscribe(self.exchange, self.symbol, self.timeFrame, seconds_from)  # Подписываемся на бары, получаем guid подписки
            self.put_notification(self.CONNECTED)  # Отправляем уведомление о подключении и начале получения исторических баров

    def _load(self):
        """Загружаем бар из истории или новый бар в BackTrader"""
        if not self.p.live_bars:  # Если получаем только историю (self.history_bars)
            if len(self.history_bars) == 0:  # Если исторических данных нет / Все исторические данные получены
                self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения исторических баров
                return False  # Больше сюда заходить не будем
            bar = self.history_bars[0]  # Берем первый бар из выборки, с ним будем работать
            self.history_bars.remove(bar)  # Убираем его из хранилища новых баров
        else:  # Если получаем историю и новые бары (self.store.new_bars)
            if len(self.store.new_bars) == 0:  # Если в хранилище никаких новых баров нет
                return None  # то нового бара нет, будем заходить еще
            new_bars = [b for b in self.store.new_bars  # Смотрим в хранилище новых баров
                        if b['provider_name'] == self.provider_name and b['response']['guid'] == self.guid]  # бары провайдера с guid подписки
            if len(new_bars) == 0:  # Если новый бар еще не появился
                return None  # то нового бара нет, будем заходить еще
            self.last_bar_received = len(new_bars) == 1  # Если в хранилище остался 1 бар, то мы будем получать последний возможный бар
            new_bar = new_bars[0]  # Берем первый бар из хранилища
            self.store.new_bars.remove(new_bar)  # Убираем его из хранилища
            bar = new_bar['response']['data']  # С данными этого бара будем работать
            if not self.is_bar_valid(bar):  # Если бар не соответствует всем условиям выборки
                return None  # то пропускаем бар, будем заходить еще
            dt_open = self.get_bar_open_date_time(bar)  # Дата/время открытия бара
            if dt_open <= self.dt_last_open:  # Если пришел бар из прошлого (дата открытия меньше последней даты открытия)
                return None  # то пропускаем бар, будем заходить еще
            self.dt_last_open = dt_open  # Запоминаем дату/время открытия пришедшего бара для будущих сравнений
            time_market_now = self.get_alor_date_time_now()  # Текущее биржевое время из Алор
            if self.last_bar_received and self.p.schedule:  # Если получили последний бар, и задано расписание биржи
                delay_seconds = self.p.schedule.time_until_trade(time_market_now).total_seconds()  # Сколько секунд нужно подождать до начала сессии биржи
                if delay_seconds > 0:  # Если нужно подождать
                    sleep(delay_seconds)  # то ждем
            if self.last_bar_received and not self.live_mode:  # Если получили последний бар и еще не находимся в режиме получения новых баров (LIVE)
                self.put_notification(self.LIVE)  # Отправляем уведомление о получении новых баров
                self.live_mode = True  # Переходим в режим получения новых баров (LIVE)
            elif self.live_mode and not self.last_bar_received:  # Если находимся в режиме получения новых баров (LIVE)
                self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) баров
                self.live_mode = False  # Переходим в режим получения истории
        # Все проверки пройдены. Записываем полученный исторический/новый бар
        self.lines.datetime[0] = date2num(self.get_bar_open_date_time(bar))  # DateTime
        self.lines.open[0] = self.provider.alor_price_to_price(self.exchange, self.symbol, bar['open'])  # Open
        self.lines.high[0] = self.provider.alor_price_to_price(self.exchange, self.symbol, bar['high'])  # High
        self.lines.low[0] = self.provider.alor_price_to_price(self.exchange, self.symbol, bar['low'])  # Low
        self.lines.close[0] = self.provider.alor_price_to_price(self.exchange, self.symbol, bar['close'])  # Close
        self.lines.volume[0] = bar['volume']  # Volume
        self.lines.openinterest[0] = 0  # Открытый интерес в Алор не учитывается
        return True  # Будем заходить сюда еще

    def stop(self):
        super(ALData, self).stop()
        if self.guid is not None:  # Если была подписка на бары
            self.provider.unsubscribe(self.guid)  # Отменяем подписку на новые бары
            self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения новых баров
        self.store.DataCls = None  # Удаляем класс данных в хранилище

    # Функции

    def is_bar_valid(self, bar):
        """Проверка бара на соответствие условиям выборки"""
        dt_open = self.get_bar_open_date_time(bar)  # Дата/время открытия бара
        if self.p.sessionstart != time.min and dt_open.time() < self.p.sessionstart:  # Если задано время начала сессии и открытие бара до этого времени
            return False  # то бар не соответствует условиям выборки
        dt_close = self.get_bar_close_date_time(dt_open)  # Дата/время закрытия бара
        if self.p.sessionend != time(23, 59, 59, 999990) and dt_close.time() > self.p.sessionend:  # Если задано время окончания сессии и закрытие бара после этого времени
            return False  # то бар не соответствует условиям выборки
        high = self.provider.alor_price_to_price(self.exchange, self.symbol, bar['high'])  # High
        low = self.provider.alor_price_to_price(self.exchange, self.symbol, bar['low'])  # Low
        if not self.p.four_price_doji and high == low:  # Если не пропускаем дожи 4-х цен, но такой бар пришел
            return False  # то бар не соответствует условиям выборки
        time_market_now = self.get_alor_date_time_now()  # Текущее биржевое время
        if dt_close > time_market_now and time_market_now.time() < self.p.sessionend:  # Если время закрытия бара еще не наступило на бирже, и сессия еще не закончилась
            return False  # то бар не соответствует условиям выборки
        return True  # В остальных случаях бар соответствуем условиям выборки

    def get_bar_open_date_time(self, bar):
        """Дата/время открытия бара. Переводим из GMT в MSK для интрадея. Оставляем в GMT для дневок и выше."""
        return self.provider.utc_time_stamp_to_msk_datetime(bar['time'])\
            if self.p.timeframe in (TimeFrame.Minutes, TimeFrame.Seconds)\
            else datetime.utcfromtimestamp(bar['time'])  # Время открытия бара

    def get_bar_close_date_time(self, dt_open, period=1):
        """Дата/время закрытия бара"""
        if self.p.timeframe == TimeFrame.Days:  # Дневной временной интервал (по умолчанию)
            return dt_open + timedelta(days=period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Weeks:  # Недельный временной интервал
            return dt_open + timedelta(weeks=period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Months:  # Месячный временной интервал
            year = dt_open.year  # Год
            next_month = dt_open.month + period  # Добавляем месяцы
            if next_month > 12:  # Если произошло переполнение месяцев
                next_month -= 12  # то вычитаем год из месяцев
                year += 1  # ставим следующий год
            return datetime(year, next_month, 1)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Years:  # Годовой временной интервал
            return dt_open.replace(year=dt_open.year + period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Minutes:  # Минутный временной интервал
            return dt_open + timedelta(minutes=self.p.compression * period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Seconds:  # Секундный временной интервал
            return dt_open + timedelta(seconds=self.p.compression * period)  # Время закрытия бара

    def get_alor_date_time_now(self):
        """Текущая дата и время
        - Если получили последний бар истории, то запрашием текущие дату и время с сервера Алор
        - Если находимся в режиме получения истории, то переводим текущие дату и время с компьютера в МСК
        """
        return self.provider.utc_time_stamp_to_msk_datetime(self.provider.get_time())\
            if self.last_bar_received\
            else datetime.now(self.provider.tz_msk).replace(tzinfo=None)
