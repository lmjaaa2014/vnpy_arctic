"""
ctp实时行情
1、在市合约接近500个。模拟账号没有期权行情，而且经常出现脏数据
2、账号可多连，json列表2个，分配到2个任务。

"""
import datetime
import multiprocessing

from vnpy.gateway.ctp import CtpGateway
from vnpy.trader.constant import Product
from vnpy.trader.utility import load_json
from no_ui.support import trading_reboot_rule
from no_ui.task import run_task


def run_parent(total_n):
    print(f"启动父进程{__file__}")
    # 设置
    reboot_func = trading_reboot_rule
    vn_product_filter = [Product.FUTURES.value, Product.OPTION.value]

    child_task = {n: None for n in range(total_n)}
    while True:
        trading = reboot_func()
        # Start child process in trading period
        if trading:
            for n in range(total_n):
                if child_task[n] is None:  # 此处判断是否有子进程在运行
                    child_task[n] = multiprocessing.Process(
                        target=run_task, args=(n, total_n,
                                               CtpGateway, 'CTP', load_json('connect_ctp_list.json'), reboot_func,
                                               True, vn_product_filter
                                               )
                    )
                    child_task[n].start()
                    print(f"{datetime.datetime.now()}【{n}/{total_n}】子进程启动")

        # 非记录时间则退出子进程
        if not trading:
            for k, v in child_task.items():
                if v is not None:
                    if not v.is_alive():
                        child_task[k] = None
                        print(f"{datetime.datetime.now()}【{k}/{total_n}】子进程关闭")


if __name__ == '__main__':
    n_default = len(load_json('connect_ctp_list.json'))  # 单个账号可能存在连接上限

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', type=int, default=n_default, help='multiprocess total n')
    run_parent(parser.parse_args().n)
