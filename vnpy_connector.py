import json
from time import sleep
from logging import INFO

from vnpy.event import EventEngine
from vnpy.trader.setting import SETTINGS
from vnpy.trader.engine import MainEngine
# from czsc.fsa.im import IM

from vnpy_tts import TtsGateway
from vnpy_ctp import CtpGateway
from loguru import logger
from vnpy_ctastrategy.base import EVENT_CTA_LOG
from vnpy_ctastrategy.engine import CtaEngine
from vnpy.trader.ui import MainWindow, create_qapp
from vnpy_ctastrategy import CtaStrategyApp
from vnpy_paperaccount import PaperAccountApp
from VnpyTraderManager import params

dt_fmt = "%Y-%m-%d %H:%M:%S"

ctp_setting = {
    "用户名": "",
    "密码": "",
    "经纪商代码": "",
    "交易服务器": "",
    "行情服务器": "",
    "产品名称": "",
    "授权编码": ""
}

# 模拟7*24小时交易
TTS_setting1 = {
    "用户名": "5917",
    "密码": "123456",
    "经纪商代码": "",
    "交易服务器": "tcp://121.37.80.177:20002",
    "行情服务器": "tcp://121.37.80.177:20004",
    "产品名称": "",
    "授权编码": ""
}

# 模拟7*24小时交易
TTS_setting3 = {
    "用户名": "5918",
    "密码": "123456",
    "经纪商代码": "",
    "交易服务器": "tcp://121.37.80.177:20002",
    "行情服务器": "tcp://121.37.80.177:20004",
    "产品名称": "",
    "授权编码": ""
}

# 模拟仿真交易
TTS_setting2 = {
    "用户名": "5708",
    "密码": "123456",
    "经纪商代码": "",
    "交易服务器": "tcp://121.37.80.177:20002",
    "行情服务器": "tcp://121.37.80.177:20004",
    "产品名称": "",
    "授权编码": ""
}
# 模拟仿真交易
TTS_setting4 = {
    "用户名": "5709",
    "密码": "123456",
    "经纪商代码": "",
    "交易服务器": "tcp://121.37.80.177:20002",
    "行情服务器": "tcp://121.37.80.177:20004",
    "产品名称": "",
    "授权编码": ""
}


def get_exchange(symbol):
    import re
    contract_exchange_dict = {'AP': 'CZCE', 'CF': 'CZCE', 'CJ': 'CZCE', 'CY': 'CZCE', 'FG': 'CZCE', 'MA': 'CZCE',
                              'OI': 'CZCE',
                              'RM': 'CZCE', 'RS': 'CZCE', 'SA': 'CZCE', 'SF': 'CZCE', 'SM': 'CZCE', 'SR': 'CZCE',
                              'TA': 'CZCE',
                              'UR': 'CZCE', 'a': 'DCE', 'b': 'DCE', 'cs': 'DCE', 'eb': 'DCE', 'eg': 'DCE', 'i': 'DCE',
                              'j': 'DCE',
                              'jd': 'DCE', 'jm': 'DCE', 'l': 'DCE', 'm': 'DCE', 'p': 'DCE', 'pp': 'DCE', 'v': 'DCE',
                              'y': 'DCE',
                              'lu': 'INE', 'nr': 'INE', 'sc': 'INE', 'ag': 'SHFE', 'al': 'SHFE', 'au': 'SHFE',
                              'bu': 'SHFE',
                              'cu': 'SHFE', 'fu': 'SHFE', 'hc': 'SHFE', 'ni': 'SHFE', 'pb': 'SHFE', 'rb': 'SHFE',
                              'ru': 'SHFE',
                              'sn': 'SHFE', 'sp': 'SHFE', 'ss': 'SHFE', 'wr': 'SHFE', 'zn': 'SHFE'}

    def split_contract_code(contract_code):
        """将合约代码字符串拆分为代码和月份"""
        # 使用正则表达式从字符串中提取字母部分和数字部分
        pattern = r'([a-zA-Z]+)(\d+)'
        match = re.match(pattern, contract_code)
        if match:
            code = match.group(1)  # 获取字母部分
            month = match.group(2)  # 获取数字部分
        else:
            # 如果无法匹配正则表达式，则默认将字符串前两个字符作为代码，剩余部分作为月份
            code = contract_code[:2]
            month = contract_code[2:]
        return code

    code = split_contract_code(symbol)
    exchange_code = contract_exchange_dict.get(code)
    return exchange_code if exchange_code else None


SETTINGS["log.active"] = True
SETTINGS["log.level"] = INFO
SETTINGS["log.console"] = False


def vnpy_run():
    """
    Running in the child process.
    """
    SETTINGS["log.file"] = True

    qapp = create_qapp()
    # 初始化事件引擎
    event_engine = EventEngine()
    # 初始化主引擎
    main_engine = MainEngine(event_engine)
    # 加载网关
    main_engine.add_gateway(CtpGateway)
    main_engine.add_gateway(TtsGateway)
    # 加载CTA应用模块
    cta_engine: CtaEngine = main_engine.add_app(CtaStrategyApp)
    main_engine.write_log("CTA应用模块创建成功")
    # 加载仿真帐户应用模块
    main_engine.add_app(PaperAccountApp)
    # 加载日志引擎
    log_engine = main_engine.get_engine("log")
    event_engine.register(EVENT_CTA_LOG, log_engine.process_log_event)
    main_engine.write_log("注册日志事件监听")
    # 网关连接
    num = input("选TTS接口输入1,选CTP接口直接回车")
    if num == '1':
        main_engine.connect(TTS_setting2, "TTS")
        main_engine.write_log("连接TTS接口")
    else:
        main_engine.connect(ctp_setting, "CTP")
        main_engine.write_log("连接CTP接口")

    sleep(10)
    # 初始化策略引擎
    cta_engine.init_engine()
    main_engine.write_log("CTA策略初始化完成")

    # 策略名称列表（一个策略对应一个合约）
    cta_strategy_list = ['VnpyTradeManager']

    # load回来主力合约的参照字典
    data = json.load(open('D:\\JJdata\\main_contract.json', 'r', encoding='utf-8'))

    # 判断params['symbols']中是否有不在主力合约列表中的合约，有则删除
    for symbol in params['symbols']:
        if symbol not in data['main_contract']:
            main_engine.write_log(f"{symbol}不在主力合约列表中,已删除,不参与交易,请检查")
            params['symbols'].remove(symbol)
    main_engine.write_log(f"已更新，当前交易合约列表为{params['symbols']}")

    # 这里是删除params['symbols']中不在策略组中的策略
    del_strategy_list = []
    for strategy in cta_engine.strategies.keys():
        if strategy.split('_')[0] not in params['symbols']:
            del_strategy_list.append(strategy)
    for strategy in del_strategy_list:
        cta_engine.remove_strategy(strategy)

    # 判断symbol是否在策略组中，不存在则加入策略组
    for symbol in params['symbols']:
        if f"{symbol}_{cta_strategy_list[0]}" not in cta_engine.strategies.keys():
            cta_engine.add_strategy(cta_strategy_list[0], f"{symbol}_{cta_strategy_list[0]}",
                                    f"{symbol}.{get_exchange(symbol)}", setting={})
            main_engine.write_log(f"{symbol}_{cta_strategy_list[0]}加入策略组")

    # 初始化策略
    cta_engine.init_all_strategies()
    # 留足够的时间初始化策略（每个合约预留10秒）
    time_ = 5 * len(params['symbols'])
    sleep(time_)
    main_engine.write_log("CTA策略全部初始化")
    sleep(10)
    # 启动策略
    cta_engine.start_all_strategies()
    main_engine.write_log("CTA策略全部启动")

    # # 启动定时任务
    # scheduler = BlockingScheduler()
    # for symbol in params['symbols']:
    #     scheduler.add_job(cta_engine.get_position, args=symbol, trigger='cron', minute='*/5')
    #
    # scheduler.start()

    # 启动ui主界面，这里是为了方便查看策略状态（如果不用，以下四行可以注释掉）
    sleep(20)
    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()
    qapp.exec()
