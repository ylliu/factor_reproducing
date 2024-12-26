import tushare as ts
import pandas as pd
import time

from tqdm import tqdm

# 设置 Tushare 的 API Token，替换为你的 Tushare token
ts.set_token("c632fdfffc18c3b68ba65351dde6638ff8ff147d6033b12da8d2be86")  # 在此处填写你的 Tushare API Token
pro = ts.pro_api()


# 获取沪深300成分股列表
def get_hs300_constituents(start_date, end_date):
    """获取当前沪深300成分股"""
    hs300 = pro.index_weight(index_code='399300.SZ', start_date=start_date, end_date=end_date)
    return hs300[['trade_date', 'con_code']].rename(columns={'trade_date': 'date', 'con_code': 'instrument'})


# 获取个股的价格数据
def get_stock_price(stock_code, start_date, end_date):
    """
    获取单只股票的收盘价数据
    :param stock_code: 股票代码
    :param start_date: 开始日期 (格式: YYYYMMDD)
    :param end_date: 结束日期 (格式: YYYYMMDD)
    :return: 收盘价 DataFrame
    """
    df = pro.daily(ts_code=stock_code, start_date=start_date, end_date=end_date, fields='trade_date,ts_code,close')
    if df.empty:
        return pd.DataFrame()
    df = df.rename(columns={'trade_date': 'date', 'ts_code': 'instrument', 'close': 'close_price'})
    return df[['date', 'instrument', 'close_price']]


# 获取沪深300成分股的历史价格和收益率数据
def get_hs300_returns(start_date, end_date):
    """
    获取沪深300成分股的历史收益率数据
    :param start_date: 开始日期 (格式: YYYYMMDD)
    :param end_date: 结束日期 (格式: YYYYMMDD)
    :return: 含未来收益的沪深300成分股 DataFrame
    """
    hs300 = get_hs300_constituents(start_date, end_date)
    hs300['date'] = pd.to_datetime(hs300['date']).dt.strftime('%Y%m%d')
    hs300 = hs300[(hs300['date'] >= start_date) & (hs300['date'] <= end_date)]

    all_data = []
    stock_codes = hs300['instrument'].unique()
    for code in tqdm(stock_codes):
        print(f"正在处理股票: {code}")
        try:
            stock_data = get_stock_price(code, start_date, end_date)
            # stock_data['future_return'] = stock_data['close_price'].pct_change(1).shift(-1)  # 计算未来1日收益率
            stock_data['future_return'] = stock_data['close_price'].shift(-1) / stock_data['close_price'] - 1
            all_data.append(stock_data)
            time.sleep(0.5)  # 避免频繁请求导致 API 限制
        except Exception as e:
            print(f"获取 {code} 数据失败: {e}")

    # 合并所有数据
    result = pd.concat(all_data, axis=0, ignore_index=True)
    result['date'] = pd.to_datetime(result['date'], format='%Y%m%d')
    return result


# 设置时间范围
start_date = "20090123"  # 开始日期
end_date = "20190430"  # 结束日期

# 获取沪深300收益率数据
hs300_returns_data = get_hs300_returns(start_date, end_date)

# 保存为 CSV 文件
hs300_returns_data.to_csv("hs300_returns.csv", index=False)
print("沪深300收益率数据已保存到 hs300_returns.csv 文件中！")
