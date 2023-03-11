# -*- coding: utf-8 -*-

"""
author: chenyiyong
email: 8665254@qq.com
create_dt:2023/3/11 13:00
describe:
"""

import csv
import json
import os
import subprocess
import time
from datetime import datetime
from os import listdir
import pandas as pd
import pytz
from czsc.enum import Freq
from gm.api import *
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData
from vnpy_mongodb.mongodb_database import MongodbDatabase

pd.set_option('expand_frame_repr', False)  # 当列太多时不需要换行
# 在这里设置你的掘金token，用于获取数据
set_token("9c4f58eb3ef18b291d90ad76e1640b5c1b5588ca")

# 保存下载数据的位置
# targetpath = os.path.abspath('.') + '\\JJdata\\'
targetpath = 'D:\\JJdata\\down_data_temp\\'

QHdict_code = [
    # 'CFFEX.IC',  # 中证期货主力连续合约
    # 'CFFEX.IF',  # 沪深期货主力连续合约
    # 'CFFEX.IH',  # 上证期货主力连续合约
    # 'CFFEX.T',  # 年期国债期货主力连续合约
    # 'CFFEX.TF',  # 年期国债期货主力连续合约
    # 'CFFEX.TS',  # 年期国债期货主力连续合约
    'CZCE.AP',  # 苹果主力连续合约
    'CZCE.CF',  # 棉花主力连续合约
    'CZCE.CJ',  # 红枣主力连续合约
    'CZCE.CY',  # 棉纱主力连续合约
    'CZCE.FG',  # 玻璃主力连续合约
    'CZCE.MA',  # 甲醇主力连续合约
    'CZCE.OI',  # 菜油主力连续合约
    'CZCE.RM',  # 菜粕主力连续合约
    'CZCE.RS',  # 菜籽主力连续合约
    'CZCE.SA',  # 纯碱主力连续合约
    'CZCE.SF',  # 硅铁主力连续合约
    'CZCE.SM',  # 锰硅主力连续合约
    'CZCE.SR',  # 白糖主力连续合约
    'CZCE.TA',  # PTA主力连续合约
    'CZCE.UR',  # 尿素主力连续合约
    # 'CZCE.ZC',  # 动力煤主力连续合约
    'DCE.A',  # 豆一主力连续合约
    'DCE.B',  # 豆二主力连续合约
    'DCE.CS',  # 玉米淀粉主力连续合约
    'DCE.EB',  # 苯乙烯主力连续合约
    'DCE.EG',  # 乙二醇主力连续合约
    'DCE.I',  # 铁矿石主力连续合约
    'DCE.J',  # 焦炭主力连续合约
    'DCE.JD',  # 鸡蛋主力连续合约
    'DCE.JM',  # 焦煤主力连续合约
    'DCE.L',  # 塑料主力连续合约
    'DCE.M',  # 豆粕主力连续合约
    'DCE.P',  # 棕榈油主力连续合约
    'DCE.PP',  # 聚丙烯主力连续合约
    'DCE.V',  # PVC主力连续合约
    'DCE.Y',  # 豆油主力连续合约
    'INE.LU',  # 低硫燃料油连续合约
    'INE.NR',  # 号胶主力连续合约
    'INE.SC',  # 原油主力连续合约
    'SHFE.AG',  # 白银主力连续合约
    'SHFE.AL',  # 铝主力连续合约
    'SHFE.AU',  # 黄金主力连续合约
    'SHFE.BU',  # 沥青主力连续合约
    'SHFE.CU',  # 铜主力连续合约
    'SHFE.FU',  # 燃油主力连续合约
    'SHFE.HC',  # 热轧卷板主力连续合约
    'SHFE.NI',  # 镍主力连续合约
    'SHFE.PB',  # 铅主力连续合约
    'SHFE.RB',  # 螺纹钢主力连续合约
    'SHFE.RU',  # 橡胶主力连续合约
    'SHFE.SN',  # 锡主力连续合约
    'SHFE.SP',  # 纸浆主力连续合约
    'SHFE.SS',  # 不锈钢主力连续合约
    'SHFE.WR',  # 线材主力连续合约
    'SHFE.ZN',  # 锌主力连续合约
]


# 下载掘金数据函数
def get_kline(symbol, end_date=None, freq='1d', k_count=1000):
    """从掘金获取历史K线数据

    参考： https://www.myquant.cn/docs/python/python_select_api#6fb030ec42984aff

    :param symbol:
    :param end_date: str
        交易日期，如 2019-12-31
    :param freq: str
        K线级别，如 1d
    :param k_count: int
    :return: pd.DataFrame
    """
    if end_date == None:
        end_date = datetime.now()

    df = history_n(symbol=symbol, frequency=freq, end_time=end_date,
                   fields='symbol,eob,open,close,high,low,volume,amount',
                   count=k_count, df=True)
    if freq == '1d':
        df = df.iloc[:-1]
    df['date'] = df['eob']
    df['vtSymbol'] = df['symbol']
    str1 = df.iloc[-1]['vtSymbol']
    list1 = str1.split(".")
    if list1[0] == 'CZCE':
        if len(list1[1]) > 2:
            str2 = list1[1]
        else:
            str2 = list1[1] + 'L8'
    else:
        if len(list1[1]) > 2:
            str2 = list1[1].lower()
        else:
            str2 = list1[1].lower() + 'l8'

    df.loc[df['vtSymbol'] != list1[1], 'vtSymbol'] = str2

    freq_a = int(int(freq.split("s")[0]) / 60)
    if freq_a == 1:
        df['freq']: Freq = Freq.F1.value
    if freq_a == 5:
        df['freq']: Freq = Freq.F5.value
    if freq_a == 15:
        df['freq']: Freq = Freq.F15.value
    if freq_a == 30:
        df['freq']: Freq = Freq.F30.value
    if freq_a == 60:
        df['freq']: Freq = Freq.F60.value

    df.sort_values(by='date', ascending=True, inplace=True)
    df['id'] = df.index
    df['turnover'] = df['amount']
    df = df[['id', 'vtSymbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'turnover', 'freq']]
    df['date'] = df.date.apply(lambda x: x.strftime(r"%Y-%m-%d %H:%M:%S"))
    df.reset_index(drop=True, inplace=True)
    for col in ['open', 'close', 'high', 'low']:
        df[col] = df[col].apply(round, args=(2,))
    return df


# 下载分钟数据
def use_kline_an(dickcode, pathtarget, frequency_n):
    for d in dickcode:
        vtSymbol = d
        freq_n = str(frequency_n * 60) + 's'
        print('下载数据', vtSymbol, frequency_n)
        kline = get_kline(symbol=vtSymbol, freq=freq_n, k_count=1000, end_date=None)
        df = pd.DataFrame(kline)

        df.to_csv(pathtarget + vtSymbol + '.' + str(frequency_n) + '.csv')


def csv_load(filepath, file, exchange, interval):
    """
    读取csv文件内容，并写入到数据库中
    """
    with open(filepath + file, "r") as f:
        reader = csv.DictReader(f)
        bars = []
        start = None
        count = 0
        utc = pytz.timezone('Asia/Shanghai')
        for item in reader:
            dt = item["date"]
            dt = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
            dt = utc.localize(dt)

            bar = BarData(
                symbol=item['vtSymbol'],
                exchange=exchange,
                datetime=dt,
                interval=interval,
                volume=float(item['volume']),
                turnover=float(item['turnover']),
                open_price=float(item['open']),
                high_price=float(item['high']),
                low_price=float(item['low']),
                close_price=float(item['close']),
                gateway_name="vnpy",
            )
            bars.append(bar)
            # do some statistics
            count += 1
            if not start:
                start = bar.datetime
        end = bar.datetime
        # insert into database
        bd = MongodbDatabase()
        bd.save_bar_data(bars=bars)
        # database_manager.save_bar_data(bars)
        print("插入数据", start, "-", end, "总数量：", count)


def run_load_csv(filepath):
    """
        遍历同一文件夹内所有csv文件，并且载入到数据库中
    """
    for file in os.listdir(filepath):
        list1 = file.split(".")

        if not file.endswith(".csv"):
            continue
        print("载入文件：", file)

        if list1[0] == 'CZCE':
            _exchange = Exchange.CZCE
        if list1[0] == 'DCE':
            _exchange = Exchange.DCE
        if list1[0] == 'SHFE':
            _exchange = Exchange.SHFE
        if list1[2] == '1':
            _interval = Interval.MINUTE
        if list1[2] == '5':
            _interval = Interval.MINUTE5
        if list1[2] == '15':
            _interval = Interval.MINUTE15
        if list1[2] == '30':
            _interval = Interval.MINUTE30
        csv_load(filepath, file, _exchange, _interval)


# 存储主力合约到本地文件夹,名称为main_contract.json
def save_main_contract(QHdict_code, filename):
    """
    保存主力合约代码到
    """
    main_contract = {}
    main_list = []
    for i in QHdict_code:
        zlhy = get_continuous_contracts(csymbol=i)
        if zlhy is None:
            print('主力合约为空', i)
        else:

            zl_symbol = zlhy[0]['symbol'].split('.')[1]
            print(zl_symbol)
            main_list.append(zl_symbol)
    if os.path.exists(filename):
        os.remove(filename)
    main_contract['main_contract'] = main_list
    json.dump(main_contract, open(filename, 'w'))
    return main_contract


def main_down():
    # 存储K线数据的文件夹
    targetpath = 'D:\\JJdata\\down_data_temp\\'


    # 存储主力合约的文件
    main_contract_file = 'D:\\JJdata\\main_contract.json'


    print('准备下载掘金期货数据，并存储到VNPY数据库，请稍候。。。')
    print('存储K线数据的文件夹为D:\JJdata\down_data_temp\ ')
    print('存储主力合约的文件为D:\JJdata\main_contract.json')
    # 创建文件夹
    if not os.path.exists(targetpath):
        os.makedirs(targetpath)

    # print('清空本地D:\\JJdata\\down_data_temp\\的数据')
    for file_name in listdir(targetpath):
        if file_name.endswith('.csv'):
            os.remove(targetpath + file_name)

    # 运行掘金客户端，地址为掘金客户端的安装路径
    myPopenObj = subprocess.Popen("D:\\Program Files (x86)\\Hongshu Goldminer3\\goldminer3.exe")
    print('***等待掘金终端启动***')
    print('掘金客户端的安装路径默认为"D:\Program Files (x86)\Hongshu Goldminer3\goldminer3.exe"')
    time.sleep(20)
    newlist = []
    for i in QHdict_code:
        zlhy = get_continuous_contracts(csymbol=i)
        # print(zlhy[0]['symbol'])
        zl_symbol = zlhy[0]['symbol']
        newlist.append(zl_symbol)

    # 存储主力合约
    save_main_contract(QHdict_code, filename=main_contract_file)
    print('主力合约名称已存储到D:\\JJdata\\main_contract.json')

    # 修改下列方框内的数字为下载周期，单位为分钟
    # 下载主力合约分钟数据
    zlhylist = [1,5]
    print(f'本次下载主力合约分钟数据为{zlhylist}分钟')
    for i in zlhylist:
        # 下载主力合约分钟数据
        use_kline_an(newlist, targetpath, i)

    zllist = [1,5]
    print(f'本次下载主连数据为{zllist}分钟')
    for i in zllist:
        # 下载主连数据
        use_kline_an(QHdict_code, targetpath, i)
    print('***下载完成***')
    # 关闭掘金客户端
    myPopenObj.kill()

    # 写入数据库
    print('***开始写入数据库***')
    run_load_csv(targetpath)
    print('***写入数据库完成***')

if __name__ == '__main__':
    main_down()
