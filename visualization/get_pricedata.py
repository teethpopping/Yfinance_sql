import pandas as pd
from creat_database import *
import re
from datetime import datetime
import yfinance as yf
import requests
from lxml import etree
import ast

def get_companyinfo(stock_name='AAPL', start='2015-01-01', end='2015-12-25', date='5y', interval='1d'):
    df = yf.Ticker(stock_name).history(period=date, interval=interval)
    return df

def get_stockprice(stock_name):
    # data = yf.download(stock_name,start='2015-01-01',interval="1d",proxy='127.0.0.1:10809')
    data = yf.download(stock_name, period="max", interval="1d", proxy='127.0.0.1:10809')
    return data

def ex_stockdata(data,stock_name):
    date = [datetime.strftime(x,"%F") for x in df["Close"][stock_name].dropna().index]
    close = data["Close"][stock_name].dropna()
    open = data["Open"][stock_name].dropna()
    high = data["High"][stock_name].dropna()
    low = data["Low"][stock_name].dropna()
    volume = data["Volume"][stock_name].dropna()

    res = []
    for i in range(len(date)):
        col_date = (date[i],close.values[i],open.values[i],high.values[i],low.values[i],volume.values[i])
        res.append(col_date)
    return res


def get_cncon_stock():

    url = "http://17.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112408633316197092606_1618407776222&pn=1&pz=500&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=b:MK0201&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152&_=1618407776223"
    res = requests.get(url)
    html = etree.HTML(res.content, etree.HTMLParser(encoding='utf-8'))
    data = etree.tostring(html,pretty_print=True,encoding='utf-8',method="html").decode('utf-8')
    res_data = re.sub(r'<(/)?[a-z]+(>)?(\n)?',"", data)
    data_dic = ast.literal_eval(res_data[res_data.index("("): len(res_data) -1])["data"]["diff"]
    all_stocks = []

    for stocks in data_dic:
        if stocks["f2"] != "-":
            # stocks["f12"].split("_")[0]
            all_stocks.append(stocks["f12"].split("_")[0])
    print(all_stocks)
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
    db = conn_sql("139.9.222.28",'lulu','lulu666')
    cursor = db.cursor()
    # 获取股票列表
    cncp_stocks = list(set(get_cncon_stock()))
#     df = get_stockprice(cncp_stocks)

    for stock_name in cncp_stocks:
        table = read_stockdata_table(db,cursor,stock_name)
        
#         qf = ex_stockdata(df,stock_name)
        # insert_stockdata_table(db, cursor, qf, stock_name)

    # chongfu = run(get_cncon_stock())
    # 创建股票db
#     [creat_cncpstock_db(db=db, cursor=cursor, cncpstock_name=stock) for stock in cncp_stocks]
#     # 创建价格数据表
#     [creat_stockdata_table(db=db, cursor=cursor, stock_name=stock) for stock in cncp_stocks]
#     read_stockdata_table(db, cursor, "baba")
    """
    停止连接
    """
    # 停止连接
    cursor.close()
    db.close()