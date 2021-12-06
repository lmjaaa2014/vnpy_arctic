"""
辅助功能

"""
import datetime
import threading
from copy import deepcopy
import pandas as pd
from typing import List


def trading_reboot_xtp():
    """
    证券类的时段重启规则
    """
    DAY_START = datetime.time(9, 5)  # 最早的>>>9：15
    DAY_END = datetime.time(15, 31)  # 最晚的>>>盘后固定价格交易15:30
    current_time = datetime.datetime.now().time()
    trading = False
    if (
            (DAY_START <= current_time <= DAY_END)
    ):
        trading = True

    return trading


def trading_reboot_rule():
    """判断控制 `特定接口` 的任务重启规则
    针对每个接口设定判断规则，注意：
    - 开盘类的时间：多加10min，程序启动需要耗时；收盘类的时间：多加1min，因为可能有推送数据延迟
    - 此函数是用于任务的重启控制，没有加入小节休息时间过滤，不希望程序在此时段重启（因为重启中有sleep耗时将近1min）
    - 小节时间、交易时段、午间休息 这类的tick时间过滤放在每个symbol的timerange中做过滤处理

    """
    # # 测试用
    # current_time = datetime.datetime.now()
    # if current_time.minute % 3 == 0:
    #     return True
    # else:
    #     return False

    # CTP的时段
    DAY_START = datetime.time(8, 45)  # 最早的 jd开盘集合竞价8:55
    DAY_END = datetime.time(15, 31)  # 最晚的 TF 15:15
    NIGHT_START = datetime.time(20, 45)  # 最早的 20:55
    NIGHT_END = datetime.time(2, 31)  # 最晚的 au 02:30

    current_time = datetime.datetime.now().time()
    trading = False
    if (
            (DAY_START <= current_time <= DAY_END)
            or (current_time >= NIGHT_START)
            or (current_time <= NIGHT_END)
    ):
        trading = True

    return trading


def vn_contract_df(all_contracts: List["ContractData"]) -> pd.DataFrame:
    """vnpy合约信息转化为DataFrame。并按`vt_symbol`升序排列

    :param all_contracts: 获取可以来自`main_engine.get_all_contracts()`
    :return:
    """
    if not all_contracts:
        return pd.DataFrame()
    # 特定数据类型转换
    _all_contracts = deepcopy(all_contracts)  # deepcopy出来，否则会改变原始数据，其他engine再用会出问题
    for x in _all_contracts:
        x.exchange = x.exchange.value
        x.product = x.product.value
        if x.option_type:
            x.option_type = x.option_type.value

    df = pd.DataFrame([x.__dict__ for x in _all_contracts])
    if not df.empty:
        return df.sort_values('vt_symbol')
    else:
        return pd.DataFrame()


def save_vn_contracts(gw_name: str, all_contracts: List["ContractData"], product_filter: list) -> int:
    """
    保存vnpy合约信息到数据库
    :param gw_name: 接口名称
    :param all_contracts: 通常获取函数 main_engine.get_all_contracts()
    :param product_filter: 筛选需要的product类型。例如 [Product.FUTURES.value, Product.OPTION.value, Product.INDEX.value, Product.FUND.value]
    :return:
    """
    df = vn_contract_df(all_contracts)
    if product_filter:
        df = df[df['product'].isin(product_filter)]
    if df.empty:
        return 0

    print(f'todo 增量保存合约信息{df.shape}')  # todo
    return len(df)
