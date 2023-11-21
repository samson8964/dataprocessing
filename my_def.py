#%%
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from iFinDPy import *
import time
import talib as ta
#%%
def process_and_save_notuse(df_in):
    # 找到所有的NaN列
    nan_cols = df_in.columns[df_in.isna().all()]

    # 初始化一个空的DataFrame来存储当前的品种数据
    df_current = pd.DataFrame()

    # 初始化一个空的列表来存储所有的品种数据
    dfs = []

    # 遍历所有的列
    for col in df_in.columns:
        # 如果当前列是NaN列，那么我们已经找到了一个新的品种
        if col in nan_cols:
            # 如果当前的品种数据不为空，那么我们将其添加到列表中
            if not df_current.empty:
                dfs.append(df_current)
            # 然后我们初始化一个新的DataFrame来存储新的品种数据
            df_current = pd.DataFrame()

        else:
            # 如果当前列不是NaN列，那么我们将其添加到当前的品种数据中
            df_current[col] = df_in[col]

        # 如果最后一个品种数据没有被添加到列表中，那么我们需要手动添加
        if not df_current.empty:
            dfs.append(df_current)

    # 现在我们有了一个包含所有品种数据的列表，我们可以将每个品种数据保存为一个单独的文件
    for df in dfs:
        # 去除表的列索引的符号和数字
        df.columns = df.columns.str.replace('\.\d+', '', regex=True)
        unique_types = df['时间'].apply(type).unique()
        if len(unique_types) > 1 or unique_types[0] != pd._libs.tslibs.timestamps.Timestamp:
            print(f"品种 {df[df.columns[1]][0]} 的 '时间' 列存在非日期格式的数据，需要进行数据清洗。")
            continue  # 如果'时间'列存在非日期格式的数据，跳过这个品种的处理
        # 把时间列设为index，只保留日期部分
        df['时间'] = pd.to_datetime(df['时间']).dt.normalize()
        df.set_index('时间', inplace=True)

        # 构造文件名

        # 获取品种名
        variety_name = df[df.columns[0]][0]
        # 构造文件名
        file_name = f'../数据/各品种主连数据/{variety_name}_{df.index.min().strftime("%Y%m%d")}_to_{df.index.max().strftime("%Y%m%d")}.xlsx'

        # 保存文件
        df.to_excel(file_name, index=True)
#%%
#后复权计算
def adjust_and_save(file_path):
    df = pd.read_excel(file_path, index_col=0, parse_dates=[0])
    df['月合约代码_shift'] = df['月合约代码'].shift(1)
    df['前一日收盘价'] = df['收盘价'].shift(1)
    df['切换日'] = df['月合约代码'] != df['月合约代码_shift']
    df.iloc[0:1, df.columns.get_loc('切换日')] = False
    df.loc[df['切换日'], '复权系数'] = df.loc[df['切换日'], '前一日收盘价'] / df.loc[df['切换日'], '收盘价']
    df['复权系数'] = df['复权系数'].fillna(method='ffill')

    df2 = df[df['切换日']].copy()
    df2 = df2.sort_values(by='月合约代码')
    df2['复权系数'] = df2['复权系数'].cumprod()
    df = df.reset_index().merge(df2[['月合约代码', '复权系数']], on='月合约代码',
                                how='left').set_index('时间')
    df.index = pd.to_datetime(df.index)

    df['复权开盘价'] = df['开盘价'] * df['复权系数_y']
    df['复权最高价'] = df['最高价'] * df['复权系数_y']
    df['复权最低价'] = df['最低价'] * df['复权系数_y']
    df['复权收盘价'] = df['收盘价'] * df['复权系数_y']
    df.index = pd.to_datetime(df.index)
    variety_name = df['期货简称'][0]  # 从文件路径中获取品种名
    # 构造文件名
    file_name = f'../数据/复权后品种行情数据/{variety_name}_{str(df.index.min().date())}_to_{str(df.index.max().date())}后复权数据.xlsx'
    print(f'{file_name}保存成功')
    print(type(df.index[0]))

    df.to_excel(file_name)

#%%
import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import Kline


def draw_kline_chart(file_path, output_path):
    """
    从指定的 Excel 文件中读取数据，绘制 K 线图，并将图表渲染为一个 HTML 文件。

    参数:
    file_path -- Excel 文件的路径
    output_path -- 输出 HTML 文件的路径
    """
    # 读取数据
    datebase = pd.read_excel(file_path, index_col=[0], usecols=[
        '时间', '期货简称', '复权开盘价', '复权最高价', '复权最低价', '复权收盘价', '成交量', '复权系数_y'])
    datebase.rename(columns={'期货简称': 'name', '复权开盘价': 'open', '复权最高价': 'high', '复权最低价': 'low',
                             '复权收盘价': 'close',
                             '成交量': 'volume', '复权系数_y': 'adjust'}, inplace=True)
    datebase.dropna(inplace=True)
    if datebase.empty:
        print(f'File {file_path} is empty.')
        return

    # 构造 K 线图数据
    kline_data = datebase[['open', 'close', 'low', 'high']].values.tolist()

    # 创建 K 线图
    kline = (
        Kline(init_opts=opts.InitOpts(width='100%', height='600px'))
        .add_xaxis(list(datebase.index))
        .add_yaxis("kline", kline_data)
        .set_global_opts(
            yaxis_opts=opts.AxisOpts(is_scale=True),
            xaxis_opts=opts.AxisOpts(is_scale=True),
            title_opts=opts.TitleOpts(title="K线图示例"),
            datazoom_opts=[
                opts.DataZoomOpts(
                    is_show=True,
                    type_="slider",
                    xaxis_index=[0],
                    range_start=10,
                    range_end=60,
                    orient="horizontal"
                ),
            ],
        )
    )

    # 渲染图表为 HTML 文件
    kline.render(
        f'{output_path}/{datebase["name"][0]}_{str(datebase.index[0].date())}_至_{str(datebase.index[-1].date())}_主连后复权行情.html')
    print(str(datebase['name'][0]) + "保存完毕")

#%%

#%%
#从Ifind 调取原始数据-设置时间为index
def ifind_process(data):
    data.rename(columns={'time': '时间', 'thscode': '同花顺代码', 'ths_future_short_name_future': '期货简称',
                         'ths_future_code_future': '期货代码', 'ths_month_contract_code_future': '月合约代码',
                         'ths_contract_multiplier_product_future': '合约乘数',
                         'ths_pricing_unit_future': '报价单位', 'ths_mini_chg_price_future': '最小变动价位',
                         'ths_open_price_future': '开盘价', 'ths_high_price_future': '最高价',
                         'ths_low_future': '最低价', 'ths_close_price_future': '收盘价',
                         'ths_settle_future': '结算价', 'ths_vol_future': '成交量',
                         'ths_open_interest_future': '持仓量'}, inplace=True)
    data.set_index(keys='时间', inplace=True)
    data.index = pd.to_datetime(data.index)
    return data
#%%

#手动获取原始数据
def fetch_raw_data(start='', end=''):
    # 使用 iFind 或其他方法获取原始数据
    engine = create_engine(
        'mysql+pymysql://zc99617224:44263203@database-1.c1n6uzso3si0.rds.cn-north-1.amazonaws.com.cn:3306/trendfollow')
    THS_iFinDLogin('dmzb026', '923035')
    data = THS_DS(
        'AGZL.SHF,ALZL.SHF,APZL.CZC,BUZL.SHF,CFZL.CZC,CZL.DCE,EBZL.DCE,EGZL.DCE,FGZL.CZC,FUZL.SHF,HCZL.SHF,LZL.DCE,MAZL.CZC,MZL.DCE,OIZL.CZC,PGZL.DCE,PPZL.DCE,PZL.DCE,RBZL.SHF,RMZL.CZC,SAZL.CZC,SFZL.CZC,SMZL.CZC,SPZL.SHF,TAZL.CZC,URZL.CZC,VZL.DCE,YZL.DCE,ZNZL.SHF',
        'ths_future_short_name_future;ths_future_code_future;ths_month_contract_code_future;ths_contract_multiplier_product_future;ths_pricing_unit_future;ths_mini_chg_price_future;ths_open_price_future;ths_high_price_future;ths_low_future;ths_close_price_future;ths_settle_future;ths_vol_future;ths_open_interest_future',
        ';;;;;;;;;;;;', 'Days:Alldays', '{}'.format(start), '{}'.format(end), 'format:dataframe')
    raw_data = data.data
    print(type(data.data))
    raw_data = ifind_process(raw_data)
    if raw_data.isnull().sum().sum() > 0:
        print('表中有空值需要处理')
    raw_data.to_sql(name='{}'.format('监控品种主连原始数据'), con=engine, if_exists='append', chunksize=20000)
    THS_iFinDLogout()
    engine.dispose()

    return raw_data
#%%
#检查原始数据是否需要更新
def data_update():
    engine = create_engine(
        'mysql+pymysql://zc99617224:44263203@database-1.c1n6uzso3si0.rds.cn-north-1.amazonaws.com.cn:3306/trendfollow')
    THS_iFinDLogin('dmzb026', '923035')
    con = pymysql.connect(host='database-1.c1n6uzso3si0.rds.cn-north-1.amazonaws.com.cn', user='zc99617224',
                          password='44263203', database='trendfollow', port=3306)
    cur = con.cursor()
    #查找数据中最后一天的日期
    sql_datenow = 'SELECT MAX(时间) FROM 监控品种主连原始数据'
    cur.execute(sql_datenow)
    last_date = cur.fetchone()[0].date()
    con.close()
    cur.close()
    if last_date < datetime.today().date():
        print('数据库最新日期为{}'.format(last_date) + '需要更新。')
        data = THS_DS(
            'AGZL.SHF,ALZL.SHF,APZL.CZC,BUZL.SHF,CFZL.CZC,CZL.DCE,EBZL.DCE,EGZL.DCE,FGZL.CZC,FUZL.SHF,HCZL.SHF,LZL.DCE,MAZL.CZC,MZL.DCE,OIZL.CZC,PGZL.DCE,PPZL.DCE,PZL.DCE,RBZL.SHF,RMZL.CZC,SAZL.CZC,SFZL.CZC,SMZL.CZC,SPZL.SHF,TAZL.CZC,URZL.CZC,VZL.DCE,YZL.DCE,ZNZL.SHF',
            'ths_future_short_name_future;ths_future_code_future;ths_month_contract_code_future;ths_contract_multiplier_product_future;ths_pricing_unit_future;ths_mini_chg_price_future;ths_open_price_future;ths_high_price_future;ths_low_future;ths_close_price_future;ths_settle_future;ths_vol_future;ths_open_interest_future',
            ';;;;;;;;;;;;', 'Days:Alldays', '{}'.format(last_date + timedelta(days=1)),
            '{}'.format(datetime.now().date()), 'format:dataframe')
        raw_data = data.data
        print(type(data.data))
        raw_data.rename(columns={'time': '时间', 'thscode': '同花顺代码', 'ths_future_short_name_future': '期货简称',
                                 'ths_future_code_future': '期货代码', 'ths_month_contract_code_future': '月合约代码',
                                 'ths_contract_multiplier_product_future': '合约乘数',
                                 'ths_pricing_unit_future': '报价单位', 'ths_mini_chg_price_future': '最小变动价位',
                                 'ths_open_price_future': '开盘价', 'ths_high_price_future': '最高价',
                                 'ths_low_future': '最低价', 'ths_close_price_future': '收盘价',
                                 'ths_settle_future': '结算价', 'ths_vol_future': '成交量',
                                 'ths_open_interest_future': '持仓量'}, inplace=True)
        print(raw_data['期货简称'].unique())
        raw_data.set_index(keys='时间', inplace=True)
        raw_data.index = pd.to_datetime(raw_data.index)
        raw_data.to_sql(name='{}'.format('监控品种主连原始数据'), con=engine, if_exists='append', chunksize=20000)
        THS_iFinDLogout()
        engine.dispose()

    else:
        print('未简称到数据日期差异。')
#%%
def cut_mian():
    engine = create_engine(
        'mysql+pymysql://zc99617224:44263203@database-1.c1n6uzso3si0.rds.cn-north-1.amazonaws.com.cn:3306/trendfollow')
    con = pymysql.connect(host='database-1.c1n6uzso3si0.rds.cn-north-1.amazonaws.com.cn', user='zc99617224',
                          password='44263203', database='trendfollow', port=3306)
    df = pd.read_sql(sql='SELECT * FROM 监控品种主连原始数据', con=con, index_col='时间')
    df.index = df.index.date
    short_name = df['期货简称'].unique()
    for i in short_name:
        data = df[df['期货简称'] == i].dropna(axis='index', how='any')
        data.drop_duplicates(inplace=True)
        data.to_sql(name='{}数据'.format(i), con=engine,
                    if_exists='replace', chunksize=20000, index=True, index_label='时间')
#%%
#后复权计算
def adjust_sql(symbol=''):
    engine = create_engine(
        'mysql+pymysql://zc99617224:44263203@database-1.c1n6uzso3si0.rds.cn-north-1.amazonaws.com.cn:3306/后复权行情')
    con_in = pymysql.connect(host='database-1.c1n6uzso3si0.rds.cn-north-1.amazonaws.com.cn', user='zc99617224',
                             password='44263203', database='trendfollow', port=3306)
    # con_out = pymysql.connect(host='database-1.c1n6uzso3si0.rds.cn-north-1.amazonaws.com.cn', user='zc99617224',
    #                           password='44263203', database='后复权行情', port=3306)
    symbol = symbol
    sql = 'SELECT * FROM {}主连数据'.format(symbol)
    df = pd.read_sql(sql=sql, con=con_in, index_col='时间')
    df['月合约代码_shift'] = df['月合约代码'].shift(1)
    df['前一日收盘价'] = df['收盘价'].shift(1)
    df['切换日'] = df['月合约代码'] != df['月合约代码_shift']
    df.iloc[0:1, df.columns.get_loc('切换日')] = False
    df.loc[df['切换日'], '复权系数'] = df.loc[df['切换日'], '前一日收盘价'] / df.loc[df['切换日'], '收盘价']
    df['复权系数'] = df['复权系数'].fillna(method='ffill')

    df2 = df[df['切换日']].copy()
    df2 = df2.sort_values(by='月合约代码')
    df2['复权系数'] = df2['复权系数'].cumprod()
    df = df.reset_index().merge(df2[['月合约代码', '复权系数']], on='月合约代码',
                                how='left').set_index('时间')
    df.index = pd.to_datetime(df.index)

    df['复权开盘价'] = df['开盘价'] * df['复权系数_y']
    df['复权最高价'] = df['最高价'] * df['复权系数_y']
    df['复权最低价'] = df['最低价'] * df['复权系数_y']
    df['复权收盘价'] = df['收盘价'] * df['复权系数_y']
    df.index = pd.to_datetime(df.index)
    df.dropna(how='any', inplace=True)
    variety_name = df['期货简称'][0] + '后复权数据'
    # 构造文件名
    print(variety_name + '计算完毕')

    df.to_sql(name=variety_name, con=engine, if_exists='replace', chunksize=20000)
    print(variety_name + '保存成功')
    con_in.close()
#%%
def mark_point(database='后复权行情', symbol='螺纹钢'):
    con = pymysql.connect(host='database-1.c1n6uzso3si0.rds.cn-north-1.amazonaws.com.cn', user='zc99617224',
                          password='44263203', database='{}'.format(database), port=3306)
    sql = 'SELECT * FROM {}主连后复权数据'.format(symbol)
    df = pd.read_sql(sql=sql, con=con, index_col='时间')
    marked_df = df.copy()
    marked_df['Resistance'] = False
    marked_df['Support'] = False
    marked_df['SMA_5'] = ta.SMA(marked_df['复权收盘价'], 5)
    marked_df['SMA_30'] = ta.SMA(marked_df['复权收盘价'], 30)

    for i in range(4, len(df) - 4):  # 保证能够查看前后4根K线
        high = df['复权最高价'].iloc[i]
        low = df['复权最低价'].iloc[i]

        # 检查是否为前后4根K线中的最高点和最低点
        if high == df['复权最高价'].iloc[i - 4:i + 5].max() and marked_df['SMA_5'].iloc[i] >= marked_df['SMA_30'].iloc[
            i]:
            marked_df['Resistance'].iloc[i] = True
        if low == df['复权最低价'].iloc[i - 4:i + 5].min() and marked_df['SMA_5'].iloc[i] <= marked_df['SMA_30'].iloc[
            i]:
            marked_df['Support'].iloc[i] = True

    return marked_df
#%%
