# vnpy_czsc
czsc库与VNPY对接项目
本次版本新增功能：
1、实时存取基础周期K线数据到数据库。
2、策略文件和配置文件可以不用放在VNPY系统目录下，可随放在VnpyTraderManager.py同级工作目录下即可

VnpyTradeManager交易管理器

一、主入口
1、VnpyTraderManager.py 此为引导入口文件，配置环境为vnpy内置解释器python3.10版本。
2、vnpy_downdata.py 此为掘金数据下载程序，可在引导程序中运行，也可单独运行。

二、vnpy_connector
1、 vnpy_connector.py 此为VNPY主要引擎和策略加载处理程序，此文件需放置于c:\veighna_studio\Lib\site-packages\czsc\connectors目录下。
（连接CTP接口，需自行设置CTP接口参数，在输入项直接回车,如果想使用TTS终端服务，则不需要设置CTP接口参数，直接在输入项内输入1回车即可自行连接。）
三、VNPY交易管理类以及信号仓位策略类
1. test_strategy.py 放在与VnpyTraderManager同目录的\strategies下即可。
2. czsc_stocks.py 放在与VnpyTraderManager同目录的\strategies\src下即可.

四、需替换原VNPY程序中的策略引擎文件和部分模块
1、engine.py 此文件替换掉c:\veighna_studio\Lib\site-packages\vnpy_ctastrategy目录下的同名文件。
2、constant.py 此文件替换掉c:\veighna_studio\Lib\site-packages\vnpy\trader目录下的同名文件。
3、utility.py此文件替换掉C:\veighna_studio\Lib\site-packages\vnpy\trader目录下的同名文件。






