import multiprocessing
import sys
from time import sleep
from datetime import datetime, time


from vnpy.event import EventEngine
from vnpy.trader.setting import SETTINGS
from vnpy.trader.engine import MainEngine
from vnpy.trader.utility import load_json
from vnpy_ctp import CtpGateway
from vnpy_datarecorder import DataRecorderApp
from logging import INFO, DEBUG

SETTINGS["log.active"] = True
SETTINGS["log.level"] = DEBUG
SETTINGS["log.console"] = True
SETTINGS["log.file"] = True




# Chinese futures market trading period (day/night)
DAY_START = time(8, 45)
DAY_END = time(15, 0)

NIGHT_START = time(20, 45)
NIGHT_END = time(2, 45)


def check_trading_period():
    """"""
    current_time = datetime.now().time()

    trading = False
    if (
        (current_time >= DAY_START and current_time <= DAY_END)
        or (current_time >= NIGHT_START)
        or (current_time <= NIGHT_END)
    ):
        trading = True

    return trading


def run_child():
    """
    Running in the child process.
    """


    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(CtpGateway)
    app_engine = main_engine.add_app(DataRecorderApp)
    main_engine.write_log(f"主引擎创建成功:gateway:{main_engine.get_all_gateway_names()}, app:{main_engine.get_all_apps()}")

    # 连接
    connect_ctp_list = load_json('connect_ctp_list.json')
    ctp_setting = connect_ctp_list[0]
    main_engine.connect(ctp_setting, "CTP")
    main_engine.write_log("连接CTP接口")
    sleep(10)  # 等待初始化查询


    app_engine.add_tick_recording('au2012.SHFE')
    # app_engine.add_bar_recording(vt_symbol)

    while True:
        sleep(1)


def run_parent():
    """
    Running in the parent process.
    """
    print("启动CTA策略守护父进程")

    child_process = None

    while True:
        trading = check_trading_period()

        # Start child process in trading period
        if trading and child_process is None:
            print("启动子进程")
            child_process = multiprocessing.Process(target=run_child)
            child_process.start()
            print("子进程启动成功")

        # 非记录时间则退出子进程
        if not trading and child_process is not None:
            if not child_process.is_alive():
                child_process = None
                print("子进程关闭成功")

        sleep(5)


if __name__ == "__main__":
    run_parent()
