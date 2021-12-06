import multiprocessing
from datetime import datetime

from vnpy_ctp import CtpGateway

from no_ui.support import trading_reboot_rule
from ui.run import main
from vnpy.trader.constant import Product
from vnpy.trader.utility import load_json


def run_parent(total_n):
    print(f"启动父进程{__file__}")
    # 设置
    reboot_func = trading_reboot_rule
    vn_product_filter = [Product.FUTURES.value, Product.OPTION.value]

    child_task = {n: None for n in range(total_n)}
    for n in range(total_n):
        child_task[n] = multiprocessing.Process(
            target=main, args=())
        child_task[n].start()
        print(f"{datetime.now()}【{n}/{total_n}】子进程启动")







if __name__ == '__main__':
    n_default = len(load_json('connect_ctp_list.json'))  # 单个账号可能存在连接上限

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', type=int, default=n_default, help='multiprocess total n')
    run_parent(parser.parse_args().n)