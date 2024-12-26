import tushare as ts
import pandas as pd
import time

# 设置 Tushare 的 API Token，替换为你的 Tushare token
ts.set_token("c632fdfffc18c3b68ba65351dde6638ff8ff147d6033b12da8d2be86")  # 在此处填写你的 Tushare API Token
pro = ts.pro_api()


# 获取沪深300成分股列表
def get_hs300_constituents(start_date, end_date):
    """获取当前沪深300成分股"""
    hs300 = pro.index_weight(index_code='399300.SZ', start_date=start_date, end_date=end_date)
    return hs300[['trade_date', 'con_code']].rename(columns={'trade_date': 'date', 'con_code': 'instrument'})


# 获取个股的流通市值数据
def get_stock_mkt_cap(stock_code, start_date, end_date):
    """
    获取单只股票的流通市值（mkt_cap_float）
    :param stock_code: 股票代码
    :param start_date: 开始日期 (格式: YYYYMMDD)
    :param end_date: 结束日期 (格式: YYYYMMDD)
    :return: 流通市值 DataFrame
    """
    df = pro.daily_basic(ts_code=stock_code, start_date=start_date, end_date=end_date,
                         fields='trade_date,ts_code,free_share')
    if df.empty:
        return pd.DataFrame()
    df = df.rename(columns={'trade_date': 'date', 'ts_code': 'instrument', 'free_share': 'mkt_cap_float'})
    return df[['date', 'instrument', 'mkt_cap_float']]


# 获取沪深300成分股在指定时间区间的流通市值数据
def get_hs300_mkt_cap(start_date, end_date):
    """
    获取沪深300成分股的历史流通市值
    :param start_date: 开始日期 (格式: YYYYMMDD)
    :param end_date: 结束日期 (格式: YYYYMMDD)
    :return: 沪深300成分股流通市值的 DataFrame
    """
    hs300 = get_hs300_constituents(start_date, end_date)
    hs300['date'] = pd.to_datetime(hs300['date']).dt.strftime('%Y%m%d')
    hs300 = hs300[(hs300['date'] >= start_date) & (hs300['date'] <= end_date)]

    all_data = []
    stock_codes = hs300['instrument'].unique()
    for code in stock_codes:
        print(f"正在处理股票: {code}")
        try:
            stock_data = get_stock_mkt_cap(code, start_date, end_date)
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

# 获取沪深300流通市值数据
hs300_mkt_cap_data = get_hs300_mkt_cap(start_date, end_date)

# 保存为 CSV 文件
hs300_mkt_cap_data.to_csv("hs300_mkt_cap.csv", index=False)
print("沪深300流通市值数据已保存到 hs300_mkt_cap.csv 文件中！")
