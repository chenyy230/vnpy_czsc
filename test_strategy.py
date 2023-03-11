# -*- coding: utf-8 -*-

"""
author: chenyiyong
email: 8665254@qq.com
create_dt:2023/3/11 13:00
describe:
"""
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
from typing import List
from czsc.traders.base import CzscTrader
from czsc.objects import Freq, RawBar

from czsc.connectors.vnpy_connector import params
from czsc.fsa.im import IM
from vnpy.trader.engine import MainEngine, EventEngine

import pymongo
from vnpy.trader.object import Exchange
from vnpy_mongodb.mongodb_database import MongodbDatabase
from vnpy_ctastrategy import CtaTemplate
from vnpy.trader.object import TickData, OrderData, TradeData, PositionData, BarData
from vnpy.trader.constant import Direction, Offset,Interval
from vnpy.trader.utility import BarGenerator
from czsc.connectors.vnpy_connector import get_exchange


dt_fmt = "%Y-%m-%d %H:%M:%S"

# vnpy同czsc转换字典
freq_czsc_vnpy = {"1分钟": Interval.MINUTE, "5分钟": Interval.MINUTE5, "15分钟": Interval.MINUTE15,
                  "30分钟": Interval.MINUTE30, "60分钟": Interval.HOUR, "日线": Interval.DAILY, "周线": Interval.WEEKLY}

exchange_ft = {"SHFE": Exchange.SHFE, "CZCE": Exchange.CZCE, "DCE": Exchange.DCE, "INE": Exchange.INE}

def get_kline_from_db1(symbol: str, period, start_time, end_time, **kwargs):
    """
    从VNPY数据库中提取数据
    get_kline_from_db1('APL8','1m','20211125','20211225',df=True)
    get_kline_from_db1('APL8','1m','2022-1-25',datetime.now(),df=False)
    :param symbol: symbol ='RM205'  合约名称
    :param period: 1m   默认
    :param start_time: 数据库中K线起始时间
    :param end_time: 数据库中K线结束时间
    **kwargs: df=True or False   设置True：输出Dataframe格式，设置False:直接转换成CZSC类RawBar对象列表
    :return:df
     需保证VNPY配置选项中已设置正确
    database.name= mongodb
    database.host= localhost
    database.port= 27017
    database.database=vnpy
    """
    start_time = pd.to_datetime(start_time).strftime('%Y%m%d%H%M%S')
    end_time = pd.to_datetime(end_time).strftime('%Y%m%d%H%M%S')
    # 连接数据库
    client = pymongo.MongoClient('localhost', 27017)
    db = client['vnpy']
    table = db['bar_data']
    # 读取数据
    data = pd.DataFrame(list(table.find({'symbol': symbol})))  # 读取数据
    # 选择需要显示的字段
    data = data[
        ['datetime', 'symbol', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'interval', 'exchange',
         'turnover']]
    # 打印输出
    df = data[data['interval'].isin([period])]
    df = df.loc[(df['datetime'] > start_time) & (df['datetime'] < end_time), :]
    p_col = ['time', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'interval', 'exchange', 'amount']
    df.columns = p_col
    df.reset_index(inplace=True, drop=True)  # 重新设置排序

    if kwargs.get("df", True):
        return df
    else:
        freq_map = {"1m": Freq.F1, "5m": Freq.F5, "1d": Freq.D}
        return format_qh_kline(df, freq=freq_map[period])


def get_kline_from_db2(symbol: str, exchange, interval, start_time, end_time):
    """
    通过VNPY中mongodb数据接口提取数据库数据, 本函数不提供DataFrame格式数据，纯输出Bars，通过转换成CZSC库需要的RawBar数据
    示范：get_kline_from_db2('APL8', Exchange.CZCE, Interval.MINUTE, datetime(2022, 1, 25), datetime.now())
    :param symbol:  symbol ='RM205'  合约名称
    :param exchange: Exchange.CZCE 或者 Exchange.SHFE  或者 Exchange.DCE 等
    :param interval: Interval.MINUTE
    :param start_time: datetime(2022, 1, 25)
    :param end_time: datetime(2022, 2, 25) 可使用 datetime.now()，输出到最近数据
    :return: bars  通过format_vnpy_qh_kline函数转换成CZSC要求的Bar数据（RawBar)
    """

    # 连接数据库
    db = MongodbDatabase()
    # 读取数据
    if isinstance(start_time, str):
        start_time = datetime.strptime(start_time, '%Y%m%d')
    if isinstance(end_time, str):
        end_time = datetime.strptime(end_time, '%Y%m%d')
    data = db.load_bar_data(symbol, exchange, interval, start_time, end_time)
    bars = format_vnpy_qh_kline(data, interval)
    return bars


# 将DataFrame格式数据转换成czsc库RawBar对象
def format_qh_kline(kline: pd.DataFrame, freq: Freq) -> List[RawBar]:
    """vnpy K线数据转换
    :param kline: VNPY 从数据库返回的K线数据（dataframe格式）
                           time symbol    open    high     low   close  volume  \
    0   2021-08-05 01:01:00  RM205  2810.0  2815.0  2810.0  2812.0    62.0
    1   2021-08-05 01:02:00  RM205  2812.0  2814.0  2812.0  2813.0    30.0
    2   2021-08-05 01:03:00  RM205  2812.0  2812.0  2810.0  2811.0    23.0
    3   2021-08-05 01:04:00  RM205  2811.0  2811.0  2809.0  2810.0    16.0
    4   2021-08-05 01:05:00  RM205  2810.0  2810.0  2809.0  2810.0    11.0
    ..                  ...    ...     ...     ...     ...     ...     ...
    673 2021-08-06 14:56:00  RM205  2828.0  2828.0  2827.0  2827.0     4.0
    674 2021-08-06 14:57:00  RM205  2827.0  2828.0  2827.0  2827.0    11.0
    675 2021-08-06 14:58:00  RM205  2826.0  2828.0  2826.0  2827.0     6.0
    676 2021-08-06 14:59:00  RM205  2826.0  2828.0  2825.0  2825.0    26.0
    677 2021-08-06 15:00:00  RM205  2826.0  2827.0  2826.0  2827.0    29.0

        interval exchange     amount
    0         1m     CZCE  1746540.0
    1         1m     CZCE   845100.0
    2         1m     CZCE   647910.0
    3         1m     CZCE   450720.0
    4         1m     CZCE   309870.0
    ..       ...      ...        ...
    673       1m     CZCE   113240.0
    674       1m     CZCE   311410.0
    675       1m     CZCE   169860.0
    676       1m     CZCE   736060.0
    677       1m     CZCE   820990.0
    :return: 转换好的K线数据
    """
    bars = []
    dt_key = 'time'
    kline = kline.sort_values(dt_key, ascending=True, ignore_index=True)
    records = kline.to_dict('records')

    for i, record in enumerate(records):
        # 将每一根K线转换成 RawBar 对象
        bar = RawBar(symbol=record['symbol'],
                     dt=pd.to_datetime(record['time'], unit='ms') + pd.to_timedelta('8H'), id=i,
                     freq=freq,
                     open=record['open'], close=record['close'], high=record['high'], low=record['low'],
                     vol=record['volume'] * 100 if record['volume'] else 0,  # 成交量，单位：股
                     amount=record['amount'] if record['amount'] > 0 else 0,  # 成交额，单位：元
                     )
        bars.append(bar)
    return bars


def format_vnpy_qh_kline(bars: List[BarData], interval) -> List[RawBar]:
    """
    将VNPY中默认的BarData数据格式转换成CZSC库要求的RawBar数据。
    :param bars: VNPY中默认的BarData数据
    :return: Rawbars  CZSC库要示的RawBar数据

    数据类型：
    BarData(gateway_name='DB', extra=None, symbol='APL8', exchange=<Exchange.CZCE: 'CZCE'>,
    datetime=datetime.datetime(2023, 2, 10, 15, 0, tzinfo=backports.zoneinfo.ZoneInfo(key='Asia/Shanghai')),
    interval=<Interval.MINUTE: '1m'>,
    volume=1080.0,
    turnover=92545200.0,
    open_interest=0,
    open_price=8571.0,
    high_price=8576.0,
    low_price=8569.0,
    close_price=8575.0)

    RawBar数据
    RawBar(symbol='APL8', id=56472,
    dt=datetime.datetime(2023, 2, 10, 15, 0, tzinfo=backports.zoneinfo.ZoneInfo(key='Asia/Shanghai')),
    freq=<Freq.F1: '1分钟'>,
    open=8571.0,
    close=8575.0,
    high=8576.0,
    low=8569.0,
    vol=108000.0,
    amount=92545200.0,
    cache=None)
    """
    Rawbars = []
    freq_map = {"1m": Freq.F1, "5m": Freq.F5, "15m": Freq.F15, "30m": Freq.F30, "1h": Freq.F60, "d": Freq.D}
    for i, BarData in enumerate(bars):
        # 将每一根K线转换成 RawBar 对象
        bar = RawBar(symbol=BarData.symbol,
                     dt=pd.to_datetime(BarData.datetime).astimezone(tz=None) + pd.to_timedelta('8H'), id=i,
                     freq=freq_map[interval.value],
                     open=BarData.open_price, close=BarData.close_price, high=BarData.high_price, low=BarData.low_price,
                     vol=BarData.volume * 100 if BarData.volume else 0,  # 成交量，单位：股
                     amount=BarData.turnover if BarData.turnover > 0 else 0,  # 成交额，单位：元
                     )
        Rawbars.append(bar)
    return Rawbars


def format_single_kline(BarData: BarData, last_id:int) -> RawBar:
    """单根K线转换
    将VNPY中默认的BarData数据格式转换成CZSC库要求的RawBar数据。
    :param bars: VNPY中默认的BarData数据
    :return: Rawbars  CZSC库要示的RawBar数据

    数据类型：
    BarData(gateway_name='DB', extra=None, symbol='APL8', exchange=<Exchange.CZCE: 'CZCE'>,
    datetime=datetime.datetime(2023, 2, 10, 15, 0, tzinfo=backports.zoneinfo.ZoneInfo(key='Asia/Shanghai')),
    interval=<Interval.MINUTE: '1m'>,
    volume=1080.0,
    turnover=92545200.0,
    open_interest=0,
    open_price=8571.0,
    high_price=8576.0,
    low_price=8569.0,
    close_price=8575.0)

    RawBar数据
    RawBar(symbol='APL8', id=56472,
    dt=datetime.datetime(2023, 2, 10, 15, 0, tzinfo=backports.zoneinfo.ZoneInfo(key='Asia/Shanghai')),
    freq=<Freq.F1: '1分钟'>,
    open=8571.0,
    close=8575.0,
    high=8576.0,
    low=8569.0,
    vol=108000.0,
    amount=92545200.0,
    cache=None)
    """

    freq_map = {"1m": Freq.F1, "5m": Freq.F5, "15m": Freq.F15, "30m": Freq.F30, "1h": Freq.F60, "d": Freq.D}
    bar = RawBar(symbol=BarData.symbol, dt=pd.to_datetime(BarData.datetime).astimezone(tz=None) + pd.to_timedelta('8H'),
                 id=last_id, freq=freq_map[BarData.interval.value],
                 open=BarData.open_price, close=BarData.close_price, high=BarData.high_price, low=BarData.low_price,
                 vol=BarData.volume * 100 if BarData.volume else 0,  # 成交量，单位：股
                 amount=BarData.turnover if BarData.turnover > 0 else 0,  # 成交额，单位：元
                 )
    return bar


class VnpyTradeManager(CtaTemplate):
    """VNPY交易管理器"""
    author = ""
    
    last_id=0

    parameters = [
    ]
    long_price= 0
    short_price= 0
    variables = [
    ]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        """

        """
        self.vt_symbol=vt_symbol
        self.main_engine :MainEngine= cta_engine.main_engine
        self.event_engine: EventEngine = cta_engine.event_engine
        self.symbol = vt_symbol.split('.')[0]
        self.exchange = exchange_ft[get_exchange(self.symbol)]
        self.strategy = params['strategy']
        self.symbol_max_pos = params['symbol_max_pos']  # 每个标的最大持仓比例
        self.trade_sdt = params['train_sdt']  # 交易跟踪开始日期
        self.base_freq = self.strategy(symbol='symbol').sorted_freqs[0]

        self.delta_days = params['delta_days']  # 定时执行获取的K线天数

        self.bg = BarGenerator(self.on_bar)
        self.bg5 = BarGenerator(self.on_bar, 5, on_window_bar=self.on_min5_bar, interval=Interval.MINUTE5)
        self.bg15 = BarGenerator(self.on_bar, 15, on_window_bar=self.on_min15_bar, interval=Interval.MINUTE15)
        self.bg30 = BarGenerator(self.on_bar, 30, on_window_bar=self.on_min30_bar, interval=Interval.MINUTE30)
        self.bg60 = BarGenerator(self.on_bar, 1, on_window_bar=self.on_1hour_bar, interval=Interval.HOUR)

        if params['callback_params']['feishu_app_id'][1] and params['callback_params']['feishu_app_id'][2]:
            self.im = IM(app_id=params['callback_params']['feishu_app_id'][1], app_secret=params['callback_params']['feishu_app_id'][2])
            self.members =params['callback_params']['feishu_app_id'][3]
        else:
            self.im = None
            self.members = None

        # 推送模式：detail-详细模式，summary-汇总模式
        self.feishu_push_mode = params['callback_params']['feishu_app_id'][0]

        file_log =params['callback_params']['file_log']
        if file_log:
            logger.add(file_log, rotation='1 day', encoding='utf-8', enqueue=True)
        self.file_log = file_log
        logger.info(f"TraderCallback init: {params['callback_params']}")

    def push_message(self, msg: str, msg_type='text'):
        """批量推送消息"""
        if self.im and self.members:
            for member in self.members:
                try:
                    if msg_type == 'text':
                        self.im.send_text(msg, member)
                    elif msg_type == 'image':
                        self.im.send_image(msg, member)
                    elif msg_type == 'file':
                        self.im.send_file(msg, member)
                    else:
                        logger.error(f"不支持的消息类型：{msg_type}")
                except Exception as e:
                    logger.error(f"推送消息失败：{e}")

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        symbol = self.symbol
        self.traders = {}
        bars = get_kline_from_db2(symbol, self.exchange, interval=freq_czsc_vnpy[self.base_freq],
                                  start_time=self.trade_sdt,end_time=datetime.now())
        
        try:
            trader: CzscTrader = self.strategy(symbol=symbol).init_trader(bars, sdt='20220301')
            self.traders[symbol] = trader
            pos_info = {x.name: x.pos for x in trader.positions}
            logger.info(f"{symbol} trader pos：{pos_info} | ensemble_pos: {trader.get_ensemble_pos('mean')}")
        except Exception as e:
            logger.exception(f'创建交易对象失败，symbol={symbol}, e={e}')
        self.write_log("策略初始化")
        self.bars = bars
        self.last_id:int = self.bars[-1].id

    def on_start(self):
        """
        Callback when strategy is started.
        """
        self.write_log("策略启动")

    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        self.write_log("策略停止")

    def on_tick(self, tick: TickData):
        """
        Callback of new tick data update.
        """
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        """
        Callback of new bar data update.
        """
       
        if self.base_freq == '1分钟':
            self.update_traders(bar)
        elif self.base_freq == '5分钟':
            self.bg5.update_bar(bar)
        elif self.base_freq == '15分钟':
            self.bg15.update_bar(bar)
        elif self.base_freq == '30分钟':
            self.bg30.update_bar(bar)
        elif self.base_freq == '60分钟':
            self.bg60.update_bar(bar)

    def on_min5_bar(self,bar: BarData):
        self.update_traders(bar)

    def on_min15_bar(self,bar: BarData):
        self.update_traders(bar)

    def on_min30_bar(self,bar: BarData):
        self.update_traders(bar)

    def on_1hour_bar(self, bar: BarData):
        self.update_traders(bar)

    def update_traders(self, bar: BarData):
        """更新交易策略"""
        self.last_id = self.last_id+1
        bar = format_single_kline(bar, self.last_id)

        for symbol in self.traders.keys():
            try:
                trader = self.traders[symbol]
                trader.on_bar(bar)

                # 根据策略的交易信号，下单【期货多空都可】
                # 根据vnpy的交易信号，下单
                if self.pos==0:
                    if trader.get_ensemble_pos(method='vote') == 1 and self.pos==0 and trader.pos_changed:
                        self.buy(bar.close_price, 1)
                        self.write_log(f"买入开仓：{bar.close_price}")

                    elif trader.get_ensemble_pos(method='vote') == -1 and self.pos==0 and trader.pos_changed:
                        self.short(bar.close_price, 1)
                        self.write_log(f"卖出开仓：{bar.close_price}")
                elif self.pos>0:
                    if trader.get_ensemble_pos(method='vote') == -1 and trader.pos_changed:
                        self.sell(bar.close_price, abs(self.pos))
                        self.short(bar.close_price, 1)
                        self.write_log(f"买入平仓：{bar.close_price}_卖出开仓：{bar.close_price}")
                elif self.pos<0:
                    if trader.get_ensemble_pos(method='vote') == 1 and trader.pos_changed:
                        self.cover(bar.close_price, abs(self.pos))
                        self.buy(bar.close_price, 1)
                        self.write_log(f"卖出平仓：{bar.close_price}_买入开仓：{bar.close_price}")

                # 更新交易对象
                self.traders[symbol] = trader
                
                pos_info = {x.name: x.pos for x in trader.positions}
                logger.info(f"{symbol} trader pos：{pos_info} | ensemble_pos: {trader.get_ensemble_pos('mean')}")

            except Exception as e:
                logger.error(f"{symbol} 更新交易策略失败，原因是 {e}")

            self.sync_data()


    def on_order(self, order: OrderData):
        """
        Callback of new order data update.
        """
        logger.info(f"on order callback: {order.vt_symbol} {order.status} {order.vt_orderid}")

        if self.feishu_push_mode == 'detail':
            msg = f"委托回报通知：\n{'*' * 31}\n" \
                  f"时间：{order.datetime.strftime(dt_fmt)}\n" \
                  f"标的：{order.vt_symbol}\n" \
                  f"订单号：{order.vt_orderid}\n" \
                  f"方向：f'方向：{order.direction}—开平：{order.offset}\n" \
                  f"委托数量：{int(order.volume)}\n" \
                  f"委托价格：{order.price}\n" \
                  f"状态：{order.status}\n"
            self.push_message(msg, msg_type='text')

    def on_trade(self, trade: TradeData):
        """
        Callback of new trade data update.
        """
        logger.info(f"on trade callback:代码:{trade.vt_symbol}_{trade.direction}_{trade.offset}_{trade.price}_{trade.volume}")

        if self.pos !=0:
            if trade.direction == Direction.LONG and trade.offset == Offset.OPEN:
                self.long_price = trade.price   # 记录开仓价格
                self.long_time = trade.datetime # 记录开仓时间
            elif trade.direction == Direction.SHORT and trade.offset == Offset.OPEN:
                self.short_price = trade.price  # 记录开仓价格
                self.short_time = trade.datetime    # 记录开仓时间

        if self.feishu_push_mode == 'detail':
            msg = f"成交变动通知：\n{'*' * 31}\n" \
                  f"时间：{datetime.now().strftime(dt_fmt)}\n" \
                  f"标的：{trade.vt_symbol}\n" \
                  f"方向：f'方向：{trade.direction}—开平：{trade.offset}\n" \
                  f"成交量：{int(trade.volume)}\n" \
                  f"成交价：{round(trade.price, 2)}"
            self.push_message(msg, msg_type='text')
            
    def on_position(self, position: PositionData):
        """
        Callback of new position data update.
        """
        logger.info(f"on position callback: {position.vt_symbol} {position.direction} {position.volume}")

        if self.feishu_push_mode == 'detail':
            msg = f"成交变动通知：\n{'*' * 31}\n" \
                  f"时间：{datetime.now().strftime(dt_fmt)}\n" \
                  f"标的：{position.vt_symbol}\n" \
                  f"id：{position.vt_positionid}\n" \
                  f"持仓量：{int(position.volume)}\n" \
                  f"昨持仓量：{int(position.yd_volume)}\n"
            self.push_message(msg, msg_type='text')
        

