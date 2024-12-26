# lncap 流通市值的自然对数 目标沪深300 时间2009 年 1 月 23 日至 2019 年 4 月 30 日
# 提取的因子数据需经过数据对齐、去极值、标准化、缺失值处理等步骤，才可进
# 入下一步的选股模型。 取极值 用5倍的xmad
import os

import pandas as pd
import numpy as np
from scipy.stats import median_abs_deviation
import statsmodels.api as sm

# 生成示例数据（在实际操作中，替换成你从数据源提取的真实数据）
# 示例数据需要包含以下列：date（日期）、instrument（股票代码）、mkt_cap_float（流通市值）
# 假设沪深300成分股的流通市值数据已存储在 CSV 文件中
print(os.getcwd())
data = pd.read_csv("./hs300_mkt_cap.csv")  # 你的真实数据文件
data['date'] = pd.to_datetime(data['date'])

# 过滤时间范围
start_date = "2009-01-23"
end_date = "2019-04-30"
data = data[(data['date'] >= start_date) & (data['date'] <= end_date)]

# 计算因子：流通市值的自然对数（lncap）
data['lncap'] = np.log(data['mkt_cap_float'])
print(data['lncap'].isnull().sum())  # 查看是否存在空值
data = data.dropna(subset=['lncap'])  # 删除 lncap 列中有缺失值的行


# 去极值：使用 5 倍 xmad 方法
def outlier_removal(series):
    """
    去极值：5 倍 MAD 方法
    """
    median = series.median()
    mad = median_abs_deviation(series, scale=1)  # Median Absolute Deviation
    upper_limit = median + 5 * mad
    lower_limit = median - 5 * mad
    return np.clip(series, lower_limit, upper_limit)


data['lncap'] = data.groupby('date')['lncap'].apply(outlier_removal).reset_index(level=0, drop=True)


# 标准化：去均值归一化
def standardize(series):
    """
    因子标准化：去均值、归一化
    """
    return (series - series.mean()) / series.std()


data['lncap'] = data.groupby('date')['lncap'].apply(standardize).reset_index(level=0, drop=True)

# 缺失值处理：填充缺失值
data['lncap'] = data['lncap'].fillna(0)  # 缺失值填充为 0

# 数据对齐：确保每个日期的因子数据完整（避免部分股票数据缺失导致的对齐问题）
aligned_data = data.pivot(index='date', columns='instrument', values='lncap').dropna(how='all', axis=1).reset_index()
aligned_data = aligned_data.melt(id_vars='date', var_name='instrument', value_name='lncap')

# 单因子检测：计算因子与未来收益的相关性
# 示例：假设已知未来收益数据在 future_return 列
returns_data = pd.read_csv("hs300_returns.csv")  # 你的收益率数据文件
returns_data['date'] = pd.to_datetime(returns_data['date'])

# 合并因子和收益率数据
merged_data = pd.merge(aligned_data, returns_data, on=['date', 'instrument'], how='inner')
# 处理缺失值或无穷大
merged_data['lncap'] = merged_data['lncap'].replace([np.inf, -np.inf], np.nan)  # 替换无穷大为 NaN
merged_data = merged_data.dropna(subset=['lncap'])  # 删除包含 NaN 的行

# 计算每天的 IC（因子与未来收益的 Spearman 相关性）
def calculate_ic(group):
    return group['lncap'].corr(group['future_return'], method='spearman')


ic_series = merged_data.groupby('date').apply(calculate_ic)

# 输出累计 IC 曲线
ic_cumulative = ic_series.cumsum()

# 可视化累计 IC 曲线
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 6))
plt.plot(ic_cumulative, label='Cumulative IC')
plt.axhline(0, color='red', linestyle='--', linewidth=0.8)
plt.title('Cumulative IC Curve')
plt.xlabel('Date')
plt.ylabel('Cumulative IC')
plt.legend()
plt.grid()
plt.show()


# 计算 t 值：使用线性回归的标准误差进行计算
def calculate_t_values(series):
    # 对每个日期的因子收益进行回归分析
    X = sm.add_constant(series['lncap'])  # 自变量（因子）
    y = series['future_return']  # 因变量（未来收益）
    model = sm.OLS(y, X).fit()  # 线性回归模型
    t_values = model.tvalues[1]  # 获取因子的 t 值（常数项的 t 值不需要）
    return t_values


# 计算 t 值的绝对值平均值
t_values_abs = merged_data.groupby('date').apply(calculate_t_values)
t_values_abs_mean = np.mean(np.abs(t_values_abs))

# 计算 t 值绝对值 > 2 的概率
t_values_abs_gt_2_prob = np.mean(np.abs(t_values_abs) > 2)

# 计算因子收益的平均值
factor_returns_mean = merged_data.groupby('date')['future_return'].mean()

# 计算因子收益的标准差
factor_returns_std = merged_data.groupby('date')['future_return'].std()

# 计算因子收益 t 值（因子收益 / 标准误差）
factor_returns_t_value = factor_returns_mean / factor_returns_std

# 计算因子收益 > 0 的概率
factor_returns_gt_0_prob = np.mean(factor_returns_mean > 0)

# 计算 IC 平均值
ic_mean = ic_series.mean()

# 计算 IC 标准差
ic_std = ic_series.std()

# 计算 IRIC（信息比率）
iric = ic_mean / ic_std

# 计算 IC > 0 的概率
ic_gt_0_prob = np.mean(ic_series > 0)

# 汇总所有结果
results = {
    't 值绝对值平均值': t_values_abs_mean,
    't 值绝对值 > 2 概率': t_values_abs_gt_2_prob,
    '因子收益平均值': factor_returns_mean.mean(),
    '因子收益标准差': factor_returns_std.mean(),
    '因子收益 t 值': factor_returns_t_value.mean(),
    '因子收益 > 0 概率': factor_returns_gt_0_prob,
    'IC 平均值': ic_mean,
    'IC 标准差': ic_std,
    'IRIC': iric,
    'IC > 0 概率': ic_gt_0_prob
}

# 转换为 DataFrame 显示
results_df = pd.DataFrame(list(results.items()), columns=['指标', '值'])
print(results_df)
