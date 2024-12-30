import tushare as ts
import pandas as pd
import time
import statsmodels.api as sm
import re


class StockDataProcessor:
    def __init__(self, token, start_date, end_date):
        # 初始化Tushare Token和日期范围
        ts.set_token(token)
        self.pro = ts.pro_api()
        self.start_date = start_date
        self.end_date = end_date

    # 获取沪深300成分股列表
    def get_hs300_constituents(self):
        """获取当前沪深300成分股"""
        hs300 = self.pro.index_weight(index_code='399300.SZ', start_date=self.start_date, end_date=self.end_date)
        return hs300[['trade_date', 'con_code']].rename(columns={'trade_date': 'date', 'con_code': 'instrument'})

    # 获取单只股票的流通市值数据
    def get_stock_mkt_cap(self, stock_code):
        """
        获取单只股票的流通市值（mkt_cap_float）及其行业信息
        :param stock_code: 股票代码
        :return: 流通市值和行业数据的 DataFrame
        """
        # 获取股票的流通市值数据
        df = self.pro.daily_basic(ts_code=stock_code, start_date=self.start_date, end_date=self.end_date,
                                  fields='trade_date,ts_code,circ_mv')
        if df.empty:
            return pd.DataFrame()
        df = df.rename(columns={'trade_date': 'date', 'ts_code': 'instrument', 'circ_mv': 'mkt_cap_float'})

        # 获取股票的行业数据
        stock_info = self.pro.stock_basic(ts_code=stock_code, fields='ts_code,industry')
        if stock_info.empty:
            industry = None  # 如果没有行业数据，则返回 None
        else:
            industry = stock_info['industry'].iloc[0]  # 提取行业信息

        # 将行业信息添加到流通市值数据中
        df['industry'] = industry

        return df[['date', 'instrument', 'mkt_cap_float', 'industry']]

    # 获取沪深300成分股在指定时间区间的流通市值数据
    def get_hs300_mkt_cap(self):
        """
        获取沪深300成分股的历史流通市值
        :return: 沪深300成分股流通市值的 DataFrame
        """
        hs300 = self.get_hs300_constituents()
        hs300['date'] = pd.to_datetime(hs300['date']).dt.strftime('%Y%m%d')
        hs300 = hs300[(hs300['date'] >= self.start_date) & (hs300['date'] <= self.end_date)]

        all_data = []
        stock_codes = hs300['instrument'].unique()
        for code in stock_codes:
            print(f"正在处理股票: {code}")
            try:
                stock_data = self.get_stock_mkt_cap(code)
                all_data.append(stock_data)
                time.sleep(0.5)  # 避免频繁请求导致 API 限制
            except Exception as e:
                print(f"获取 {code} 数据失败: {e}")

        # 合并所有数据
        result = pd.concat(all_data, axis=0, ignore_index=True)
        result['date'] = pd.to_datetime(result['date'], format='%Y%m%d')
        return result

    # 获取每只股票的行业数据
    def get_industry_data(self, stock_codes):
        """
        获取沪深300成分股的行业数据
        :param stock_codes: 股票代码列表
        :return: 行业数据 DataFrame
        """
        industry_data = {}
        for stock in stock_codes:
            print(stock)
            df = self.pro.stock_basic(ts_code=stock, fields='ts_code,industry')
            industry_data[stock] = df['industry'].values[0]  # 获取行业名称
        industry_df = pd.DataFrame(list(industry_data.items()), columns=['ts_code', 'industry'])
        return industry_df.set_index('ts_code')

    # 行业市值中性化

    def neutralize_by_industry_and_market_cap(self, data):
        """
        对因子进行行业和市值中性化
        :param data: 包含日期、股票代码、流通市值和行业数据的 DataFrame
        :return: 中性化后的因子数据
        """
        # 行业数据转为哑变量
        industry_dummies = pd.get_dummies(data['industry'], drop_first=True)

        # 将市值和行业哑变量作为自变量，mkt_cap_float 作为因变量进行回归
        X = pd.concat([data['mkt_cap_float'], industry_dummies], axis=1)
        X = sm.add_constant(X)  # 加上常数项
        y = data['mkt_cap_float']

        # 使用 OLS 回归模型进行回归
        model = sm.OLS(y, X).fit()

        # 提取残差作为新的中性化因子值
        residual = model.resid

        # 将中性化后的数据添加到原始数据中
        data['neutralized_factor'] = residual

        return data

    # 运行整个数据处理流程
    def process_data(self):
        hs300_mkt_cap_data = self.get_hs300_mkt_cap()

        # 对数据进行行业市值中性化
        # neutralized_data = self.neutralize_by_industry_and_market_cap(hs300_mkt_cap_data)

        # 保存中性化后的数据为 CSV 文件
        hs300_mkt_cap_data.to_csv("hs300_mkt_cap.csv", index=False)
        print("沪深300流通市值中性化数据已保存到 hs300_mkt_cap_neutralized.csv 文件中！")


# 用法示例：

if __name__ == '__main__':
    # 实例化并运行数据处理
    token = "c632fdfffc18c3b68ba65351dde6638ff8ff147d6033b12da8d2be86"  # 替换为你的Tushare token
    start_date = "20090123"
    end_date = "20190430"

    processor = StockDataProcessor(token, start_date, end_date)
    processor.process_data()
