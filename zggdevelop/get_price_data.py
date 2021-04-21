import pandas as pd
from solve_database import *
import re
from datetime import datetime
import yfinance as yf
import requests
from lxml import etree
import ast

def get_companyinfo(stock_name='AAPL', start='2015-01-01', end='2015-12-25', date='5y', interval='1d'):
    df = yf.Ticker(stock_name).history(period=date, interval=interval)
    return df

def get_stockprice(stock_names):
    """
    通过yfinance批量下载股票数据,下载从2015-01-01到今天的数据
    :param stock_names: 股票列表
    :return: 返回所有股票数据 以及下载失败的list
    """
    try:
        # 修改源代码，在multi.py里添加 return data,[v[0] for v in list(shared._ERRORS.items())]
        data,error_list = yf.download(stock_names,start='2015-01-01',interval="1d",proxy='127.0.0.1:10809')
    #  data = yf.download(stock_name, period="max", interval="1d", proxy='127.0.0.1:10809')
    #     print(error_list)
    except Exception as e:
        print(e)
    finally:
        return data,error_list

def ex_stockdata(data,stock_name):
    """
    传入通过yfinance下载的所有股票数据data,通过传入stock_name.获取个股股票数据
    :param data: yfinance下载的所有股票数据
    :param stock_name: 股票代码
    :return: 股票数据dataframe[date open high low close volume]
    """
    date = [datetime.strftime(x,"%F") for x in data["Close"][stock_name].dropna().index]
    close = data["Close"][stock_name].dropna()
    open = data["Open"][stock_name].dropna()
    high = data["High"][stock_name].dropna()
    low = data["Low"][stock_name].dropna()
    volume = data["Volume"][stock_name].dropna()
    res = []
    for i in range(len(date)):
        col_date = (date[i],str(close.values[i]),str(open.values[i]),str(high.values[i]),str(low.values[i]),str(volume.values[i]))
        res.append(col_date)
    return res

def get_cncon_stock():
    """
    获取今天所有中概股的列表代码,排除_U的股票以及退市的股票
    :return: 返回中概股代码列表
    """
    url = "http://17.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112408633" \
          "316197092606_1618407776222&pn=1&pz=500&po=1&np=1&ut=bd1d9ddb04089700" \
          "cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=b:MK0201&fields=f1,f2,f3,f" \
          "4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f2" \
          "5,f26,f22,f33,f11,f62,f128,f136,f115,f152&_=1618407776223"
    res = requests.get(url)
    html = etree.HTML(res.content, etree.HTMLParser(encoding='utf-8'))
    data = etree.tostring(html, pretty_print=True, encoding='utf-8', method="html").decode('utf-8')
    res_data = re.sub(r'<(/)?[a-z]+(>)?(\n)?', "", data)
    data_dic = ast.literal_eval(res_data[res_data.index("("): len(res_data) - 1])["data"]["diff"]
    all_stocks = []

    for stocks in data_dic:
        if stocks["f2"] != "-":
            # 删除带_U的股票数据，因为yfinance下载不到
            # stocks["f12"] = stocks["f12"].split("_")[0]
            stocks["f12"] = stocks["f12"].replace("_U","-UN")
            all_stocks.append(stocks["f12"])
    return all_stocks

def run(lit):
    # 判断lit中是否有重复的元素
    if [lit.count(x) for x in lit if lit.count(x) > 1]:
        lit1 = []
        for i in lit:

            if lit.count(i) > 1:
                lit1.append(i)

        # 将lit2去重并保持原顺序
        lit2 = list(set(lit1))

        lit2.sort(key=lit1.index)

        # lit中有重复的元素,则返回重复的元素列表lit2
        return lit2

    # lit中没有重复的元素,则返回None
    return None



if __name__=="__main__":
    # 连接数据库
    db = conn_sql("139.9.222.28",'chiwang','wc1998wc0322wc..')
    cursor = db.cursor()
    # 获取股票列表
    cncp_stocks = list(set(get_cncon_stock()))
    print(cncp_stocks)
    stock_indexs = ["^IXIC","^SPX","^DJI"]
    #
    # 获取股票价格数据
    cn_df,error_list1 = get_stockprice(cncp_stocks)
    si_df,error_list2 = get_stockprice(stock_indexs)
    #
    #
    # 创建股票db
    for stock in cncp_stocks:
        stock = stock.replace("-","")
        creat_cncpstock_db(db=db, cursor=cursor, cncpstock_name=stock)

    for stock in stock_indexs:
        stock = stock.replace("^", "")
        creat_cncpstock_db(db=db, cursor=cursor, cncpstock_name=stock)
    # 创建价格数据表
    for stock in cncp_stocks:
        stock = stock.replace("-","")
        creat_stockdata_table(db=db, cursor=cursor, stock_name=stock)
    for stock in stock_indexs:
        stock = stock.replace("^", "")
        creat_stockdata_table(db=db, cursor=cursor, stock_name=stock)
    # # 插入数据
    # # 插入股票数据
    for stock_name in cncp_stocks:
        cn_stock = ex_stockdata(cn_df, stock_name)
        stock_name = stock_name.replace("-", "")
        print(cn_stock)
        insert_stockdata_table(db, cursor, cn_stock, stock_name)
    # 插入股指数据
    for stock_name in stock_indexs:
        si_stock = ex_stockdata(si_df,stock_name)
        stock_name = stock_name.replace("^", "")
        insert_stockdata_table(db, cursor, si_df, stock_name)
    # # 插入股票信息数据




    # 插入数据
    # for stock_name in cncp_stocks[1:10]:
    #     table = read_stockdata_table(db,cursor,stock_name)
    #     print(table)
    #     insert_stockdata_table(db, cursor, qf, stock_name)

    # chongfu = run(get_cncon_stock())

    """
    停止连接
    """
    # 停止连接
    cursor.close()
    db.close()

