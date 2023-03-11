# -*- coding: utf-8 -*-

"""
author: chenyiyong
email: 8665254@qq.com
create_dt:2023/3/11 13:00
describe:
"""

from czsc.connectors import vnpy_connector as manager

#策略模板
from vnpy_ctastrategy.strategies.src.czsc_stocks import CzscStocksV230218
#掘金数据下载
import vnpy_downdata
from czsc.fsa.im import IM

def get_feishu_members_by_mobiles(mobiles, **kwargs):
    """根据手机号获取飞书用户id

    :param mobiles: 手机号列表
    :param kwargs:
    :return:
    """
    app_id = kwargs.get('app_id', 'cli_a30****39500e')
    app_secret = kwargs.get('app_secret', 'jVoMf6*****0fkR2HhoVbZ7fiTkTkgg')
    im = IM(app_id, app_secret)
    res = im.get_user_id({"mobiles": mobiles})['data']['user_list']
    return [x['user_id'] for x in res]


#策略参数字典
params = {
    # trader 缓存目录
    'cache_path': "D:\\czsc_qh_beta_cache",
    # 设定实盘交易的期货池
    'symbols': ['ru2305','rb2305'],
    # 单个期货的最大持仓比例
    'symbol_max_pos': 0.24,
    # CzscTrader初始交易开始的时间，这个时间之后的交易都会被缓存在对象中
    'train_sdt': '20230225',
    # update trader时，K线获取的天数
    'delta_days': 1,
    #交易策略
    'strategy': CzscStocksV230218,
    # TraderCallback 回调类的参数
    'callback_params': {
        'feishu_push_mode': 'detail',
        'feishu_app_id': 'cli_a30****39500e',
        'feishu_app_secret': 'jVoMf6*****0fkR2HhoVbZ7fiTkTkgg',
        'feishu_members': ['ou_63fc******8b084130c14aec42b8'],
        'file_log': 'D:\\czsc_qh_beta_cache\\czsc_qh_beta.log',
    },
}

if __name__ == '__main__':
    #下载数据到vnpy(建议早上和晚上开盘前运行)
    # vnpy_downdata.main_down()
    #运行策略
    manager.vnpy_run()