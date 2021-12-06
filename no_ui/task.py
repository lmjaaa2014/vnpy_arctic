"""
自动任务时的规则
"""
import os
import sys
import logging
import datetime
import pandas as pd
from time import sleep
from typing import Callable
from vnpy_datarecorder import DataRecorderApp
from vnpy.trader.app import BaseApp
from vnpy.event import EventEngine
from vnpy.trader.constant import Product
from vnpy.trader.engine import MainEngine
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.utility import load_json

from .support import vn_contract_df, save_vn_contracts
from vnpy.trader.setting import SETTINGS
SLEEP_WAITE = 10
from logging import INFO, DEBUG
# 设为100，队列qsize()未处理超此数量时，log记下warning; 代码中30s检查一次
# vnpyredis.app.data_redis.engine.DataRedisEngine#run
QUEUE_WARNING = 100
def qcut_rank(series, n, rank_method='average') -> pd.Series:
    """
    对有重复边界的qcut，使用排名进行分箱。[参考解决办法，未完全解决](https://stackoverflow.com/a/36883735/2336654)
    :param series: 可分割序列
    :param n: 切成n份，标识从1开始！！
    :param rank_method: series.rank的排名方法
    :return:
    """
    edges = pd.Series([float(i) / n for i in range(n + 1)])
    f = lambda x: (edges >= x).values.argmax()
    return series.rank(pct=1, method=rank_method).apply(f).astype("category")


def run_task(task_n: int, total_n,
             gateway: BaseGateway, gateway_name: str, connect_list: list,
              reboot_func: Callable,
             save_contract=False, vn_product_filter: list = []
             ):

    # 基本设置

    SETTINGS["log.active"] = True
    SETTINGS["log.level"] = DEBUG
    SETTINGS["log.console"] = True
    SETTINGS["log.file"] = True

    if not vn_product_filter:
        vn_product_filter = [x.value for x in Product]

    # 创建Engine
    task_msg = f'【{task_n}/{total_n} pid {os.getpid()} ppid {os.getppid()}】'
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(gateway)
    app_engine = main_engine.add_app(DataRecorderApp)
    main_engine.write_log(task_msg + f"创建成功:gateway:{main_engine.get_all_gateway_names()}, app:{list(main_engine.apps.keys())}")

    # 连接
    connect_setting = connect_list[task_n % len(connect_list)]
    app_engine.engine_start(connect_setting.get("用户名"))
    main_engine.connect(connect_setting, gateway_name)
    sleep(SLEEP_WAITE)  # 等待gateway的初始化耗时

    # 合约信息
    contracts_list = main_engine.get_all_contracts()
    if save_contract and task_n == 0:
        _ = save_vn_contracts(gateway_name, contracts_list, vn_product_filter)
        app_engine.write_log(f'task=0 保存合约信息{_}')
    # 分配任务
    # total_df = vn_contract_df(contracts_list)
    # total_df = total_df[total_df['product'].isin(vn_product_filter)]
    #
    # ##若无指定symbol则保存所有合约,若有指定标的则保存指定合约
    # trade_symbol_list = load_json("data_recorder_setting.json")
    # # trade_symbol_list = ["cu2203.SHFE","au2204.SHFE","au2112.SHFE","MA207.CZCE","AP203.CZCE"]
    # if trade_symbol_list is not None and len(trade_symbol_list) != 0:
    #     total_df = total_df[total_df['vt_symbol'].isin(trade_symbol_list.get("tick", {}).keys())]
    #
    #
    #
    #
    #
    # total_df = total_df.sort_values('vt_symbol')
    # total_df['task_n'] = qcut_rank(total_df['vt_symbol'], total_n)
    # task_df = total_df[total_df['task_n'] == task_n+1]
    # for vt_symbol in list(task_df['vt_symbol']):
    #     app_engine.add_tick_recording(vt_symbol)
    #     print(f"{task_n}/{total_n},add symbol = {vt_symbol}")


    _ = list(app_engine.tick_recordings.keys())
    app_engine.write_log(f"接口{gateway_name}任务{task_n}任务量{len(_)}:{_}")

    # 保持线程持续
    while True:
        sleep(10)
        trading = reboot_func()
        if not trading:
            if save_contract and task_n == 0:
                contracts_list = main_engine.get_all_contracts()
                _ = save_vn_contracts(gateway_name, contracts_list, vn_product_filter)
                app_engine.write_log(f'task=0 保存合约信息{_}', level=logging.DEBUG)

            app_engine.close()
            main_engine.close()
            main_engine.write_log(task_msg + "关闭退出")
            sys.exit(0)
        else:
            # todo alive检测：通过哪些对象属性，检测存活main_engine和gateway
            now = datetime.datetime.now()
            if now.second % 30 == 0 and str(now.microsecond).startswith('9'):
                pass
                # app_engine.write_log(f"{task_msg} keep alive")



