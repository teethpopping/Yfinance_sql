import pymysql
import pandas as pd
import numpy as np

# pymysql.converters.encoders[np.float64] = pymysql.converters.escape_float()
# pymysql.converters.conversions = pymysql.converters.encoders.copy()
# pymysql.converters.conversions.update(pymysql.converters.decoders)

def conn_sql(host,user,password):
    connection = pymysql.connect(host=host,
                                port=3306,
                                user=user,
                                password=password,
                                 charset='utf8')
    return connection

# 创建数据库
def creat_companylist_db(db,cursor):
    """
    创建公司列表数据库
    :param db:数据库连接
    :param cursor: 游标
    :return:
    """
    db_name = "companylist"
    cursor.execute("drop database if exists {}".format(db_name))
    sql = "create database {}".format(db_name)
    try:
        cursor.execute(sql)
        db.commit()
        print(db_name+"数据库创建成功")
    except Exception as e:
        db.rollback()
        print(e)

def creat_stockindex_db(db,cursor,stockindex_name):
    """
    创建股指数据库
    :param db:数据库连接
    :param cursor: 游标
    :param stockindex_name: 股指名称字符串
    :return:
    """
    cursor.execute("drop database if exists {}".format(stockindex_name))
    sql = "create database {}".format(stockindex_name)
    try:
        cursor.execute(sql)
        db.commit()
        print(stockindex_name+"数据库创建成功")
    except Exception as e:
        db.rollback()
        print(e)
    except pymysql.err.ProgrammingError as pe:
        print(stockindex_name,"不合规,创建失败")

def creat_cncpstock_db(db,cursor,cncpstock_name):
    """
    创建股指数据库
    :param db:数据库连接
    :param cursor: 游标
    :param stockindex_name: 股指名称字符串
    :return:
    """
    cursor.execute("drop database if exists {}".format(cncpstock_name))
    sql = "create database {}".format(cncpstock_name)
    try:
        cursor.execute(sql)
        db.commit()
        print(cncpstock_name+"数据库创建成功")
    except Exception as e:
        db.rollback()
        print(e)

# 创建表
def creat_companylist_table(db,cursor,table_name='companylist'):
    # 切换数据库
    cursor.execute("use {}".format(table_name))
    cursor.execute("drop database if exists {}".format(table_name))
    # 创建表
    sql = """
    CREATE TABLE {}(
        company_name_en varchar(255) primary key,
        company_name_cn varchar(255)
    )
    """.format(table_name)
    try:
        cursor.execute(sql)
        db.commit()
        print(table_name+"数据表创建成功")
    except Exception as e:
        db.rollback()
        print(e)

def creat_stockdata_table(db,cursor,stock_name):
    # 切换数据库
    cursor.execute("use {}".format(stock_name))
    cursor.execute("drop table if exists {}PRICE".format(stock_name))
    # 创建表
    sql = """
    CREATE TABLE {}PRICE(
        date DATE primary key,
        OPEN VARCHAR(255),
        HIGH VARCHAR(255),
        LOW VARCHAR(255),
        CLOSE VARCHAR(255),
        VOLUME VARCHAR(255))""".format(stock_name)
    try:
        cursor.execute(sql)
        db.commit()
        print(stock_name+"数据表创建成功")
    except Exception as e:
        db.rollback()
        print(e)

def creat_hxbd_table(db,cursor,form,stock_name):
    """
    存储核心必读的表
    :param db: 连接的数据库
    :param cursor: 游标
    :param form: 分为 "zyzbnb"->主要指标年报 "gsgknb"->公司概况年报
    :param stock_name: 公司名称
    :return:
    """
    if form == "zyzbnb":
        # 切换数据库
        cursor.execute("use {}".format(stock_name))
        cursor.execute("drop table if exists {}ZYZBNB".format(stock_name))
        # 创建表
        # 人民币要改为rmb
        # petem->pettm
        sql = """
         CREATE TABLE {}ZYZBNB(
             REPORTDATE DATE primary key,
            CURRENCY VARCHAR(255),
            STARTDATE DATE,
            ENDDATE DATE,
            PETTM DOUBLE,
            PBMRQ DOUBLE ,
            EPSTTM DOUBLE ,
            BPS DOUBLE ,
            DPS DOUBLE ,
            DTOP DOUBLE ,
            OPERATEREVE DOUBLE ,
            OPERATEREVEYOY DOUBLE ,
            PARENTNETPROFIT DOUBLE ,
            PARENTNETPROFITYOY DOUBLE ,
            SPECIALINDEX1 DOUBLE ,
            SPECIALINDEX2 DOUBLE ,
            ADSNUM DOUBLE ,
            TOTALVALUE DOUBLE ,
            )""".format(stock_name)
    elif form == 'gsgknb':
        cursor.execute("use {}".format(stock_name))
        cursor.execute("drop table if exists {}GSKGNB".format(stock_name))
        sql = """
        CREATE TABLE {}GSGKNB(
        checkdate DATETIME PRIMARY KEY;
        SECURITYCODE VARCHAR(255),
        SECURITYSHORTNAME VARCHAR(255),
        COMPNAME VARCHAR(255),
        COMPNAMECN VARCHAR(255),
        SECURITYTYPE VARCHAR(255),
        TRADEMARKET VARCHAR(255),
        CHAIRMAN VARCHAR(255),
        INDUSTRY VARCHAR(255),
        FOUNDDATE DATE,
        LISTEDDATE DATE,
        OFFICEADDRESS VARCHAR(255),
        WEBSITE VARCHAR(255),
        )""".format(stock_name)
    try:
        cursor.execute(sql)
        db.commit()
        print(stock_name + "数据表创建成功")
    except Exception as e:
        db.rollback()
        print(e)

def creat_gsgk_table(db,cursor,stock_name):
    # 切换数据库
    cursor.execute("use {}".format(stock_name))
    cursor.execute("drop table if exists {}PRICE".format(stock_name))
    # 创建表
    sql = """
     CREATE TABLE {}PRICE(
         date DATE primary key,
        )""".format(stock_name)
    try:
        cursor.execute(sql)
        db.commit()
        print(stock_name + "数据表创建成功")
    except Exception as e:
        db.rollback()
        print(e)

def creat_cwfx_table(db,cursor,form,stock_name):
    if form == 'zyzbnb':
        # 切换数据库
        cursor.execute("use {}".format(stock_name))
        cursor.execute("drop table if exists {}ZYZBNB".format(stock_name))
        # 创建表
        sql = """
         CREATE TABLE {}ZYZBNB(
             SECURITYCODE VARCHAR(255),
             STARTDATE DATE,
             REPORTDATE DATE PRIMARY KEY,
             TIMETYPECODE VARCHAR(255),
             TIMETYPE VARCHAR(5),
             ENDDATE DATE ,
             CURRENCY VARCHAR(255),
             STANDARD VARCHAR(255),
             REVENUE DOUBLE ,
             REVENUEYOY DOUBLE ,
             GROSSMARGIN DOUBLE ,
             GROSSMARGINYOY DOUBLE ,
             PARENTPROFIT DOUBLE ,
             PARENTPROFITYOY DOUBLE ,
             EBPS DOUBLE ,
             DBPS DOUBLE ,
             GROSSPROFITMARGIN DOUBLE ,
             NETPROFIT DOUBLE ,
             CCOUNTRATE DOUBLE ,
             TURNOVERRATE DOUBLE ,
             TOTALRATE DOUBLE ,
             ACCOUNTDAYS DOUBLE ,
             TURNOVERDAYS DOUBLE ,
             TOTALDAYS DOUBLE ,
             EQUALITYRATE DOUBLE ,
             NETASSETRATE DOUBLE ,
             LIQUALITYRATE DOUBLE ,
             QUICKRATIO DOUBLE ,
             CASHRATIO DOUBLE ,
             ASSETRATIO DOUBLE ,
             PROPERTYRATIO DOUBLE ,
             RANK INT)""".format(stock_name)
    elif form == 'zcfzbnb':
        pass
    elif form == 'zhsybnb':
        pass
    elif form == 'xjllb':
        pass

    try:
        cursor.execute(sql)
        db.commit()
        print(stock_name + "数据表创建成功")
    except Exception as e:
        db.rollback()
        print(e)

def creat_gbgd_table(db,cursor,stock_name):
    # 切换数据库
    cursor.execute("use {}".format(stock_name))
    cursor.execute("drop table if exists {}PRICE".format(stock_name))
    # 创建表
    sql = """
     CREATE TABLE {}PRICE(
         date DATE primary key,
        )""".format(stock_name)
    try:
        cursor.execute(sql)
        db.commit()
        print(stock_name + "数据表创建成功")
    except Exception as e:
        db.rollback()
        print(e)

def creat_fhpx_table(db,cursor,stock_name):
    # 切换数据库
    cursor.execute("use {}".format(stock_name))
    cursor.execute("drop table if exists {}PRICE".format(stock_name))
    # 创建表
    sql = """
     CREATE TABLE {}PRICE(
         date DATE primary key,
        )""".format(stock_name)
    try:
        cursor.execute(sql)
        db.commit()
        print(stock_name + "数据表创建成功")
    except Exception as e:
        db.rollback()
        print(e)

# 插入数据
def insert_companylist_table(db,cursor,data,table_name='companylist'):
    """
    向公司列表中插入数据
    :param db: 数据库连接
    :param cursor: 数据库游标
    :param table_name: 公司总表名称 定为companylist
    :param data: 传入数据，数据为元组列表[(company_name_en,company_name_cn)]
    :return:
    """
    cursor.execute("use {}".format(table_name))
    sql = """
    INSERT INTO {}
    (company_name_en,company_name_cn)
    VALUES (%S,%S)
    """.format(table_name)
    try:
        cursor.executemany(sql,data)
        db.commit()
        print(table_name+"数据插入成功")
    except Exception as e:
        db.rollback()
        print(e)

def insert_stockdata_table(db,cursor,data,table_name):
    """
    向公司列表中插入数据
    :param db: 数据库连接
    :param cursor: 数据库游标
    :param table_name: 股票名称
    :param data: 传入数据，数据为元组列表[(date,OPEN,HIGH,LOW,CLOSE,VOLUME)]
    :return:
    """
    cursor.execute("use {}".format(table_name))
    sql = """
    INSERT INTO {}PRICE
    (date,OPEN,HIGH,LOW,CLOSE,VOLUME)
    VALUES (%s,%s,%s,%s,%s,%s)
    """.format(table_name)
    try:
        cursor.executemany(sql,data)
        db.commit()
        print(table_name+"数据插入成功")
    except Exception as e:
        db.rollback()
        print(e)

def insert_stockindex_table(db,cursor,data,table_name):
    """
    向公司列表中插入数据
    :param db: 数据库连接
    :param cursor: 数据库游标
    :param table_name: 股票名称
    :param data: 传入数据，数据为元组列表[(date,OPEN,HIGH,LOW,CLOSE,VOLUME)]
    :return:
    """
    cursor.execute("use {}".format(table_name))
    sql = """
    INSERT INTO {}PRICE
    (date,OPEN,HIGH,LOW,CLOSE,VOLUME)
    VALUES (%s,%s,%s,%s,%s,%s)
    """.format(table_name)
    try:
        cursor.executemany(sql,data)
        db.commit()
        print(table_name+"数据插入成功")
    except Exception as e:
        db.rollback()
        print(e)

def insert_hxbd_table(db,cursor,form,data,table_name):
    """
    向公司列表中插入数据
    :param db: 数据库连接
    :param cursor: 数据库游标
    :param form: 分为 "zyzbnb"->主要指标年报 "gsgknb"->公司概况年报
    :param table_name: 股票名称
    :param data: 传入数据，数据为元组列表[]
    :return:
    """
    if form == "zyzbnb":
        cursor.execute("use {}".format(table_name))
        sql = """
        INSERT INTO {}ZYZBNB
        (REPORTDATE,CURRENCY,STARTDATE,ENDDATE,
        PETTM,PBMRQ,EPSTTM,BPS,DPS,DTOP,OPERATEREVE,
        OPERATEREVEYOY,PARENTNETPROFIT,PARENTNETPROFITYOY,
        SPECIALINDEX1,SPECIALINDEX2,ADSNUM,TOTALVALUE)
        VALUES (%s,%s,%s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f)
        """.format(table_name)
    elif form == 'gsgknb':
        cursor.execute("use {}".format(table_name))
        sql = """
        INSERT INTO {}GSGKNB
        (checkdate,SECURITYCODE,SECURITYSHORTNAME,COMPNAME,
        COMPNAMECN,SECURITYTYPE,TRADEMARKET,CHAIRMAN,
        INDUSTRY,FOUNDDATE,LISTEDDATE,OFFICEADDRESS,WEBSITE)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """.format(table_name)
    try:
        cursor.executemany(sql,data)
        db.commit()
        print(table_name+"数据插入成功")
    except Exception as e:
        db.rollback()
        print(e)

def insert_cwfx_table(db,cursor,form,data,table_name):
    """
    向公司列表中插入数据
    :param db: 数据库连接
    :param cursor: 数据库游标
    :param form: 分为 "zyzbnb"->主要指标年报 "acfzbnb"->资产负债表年报
    "zhsybnb"->综合损益表 "xjllbnb"->现金流量表年报
    :param table_name: 股票名称
    :param data: 传入数据，数据为元组列表[]
    :return:
    """
    if form == "zyzbnb":
        cursor.execute("use {}".format(table_name))
        sql = """
        INSERT INTO {}ZYZBNB
        (REPORTDATE,CURRENCY,STARTDATE,ENDDATE,
        PETTM,PBMRQ,EPSTTM,BPS,DPS,DTOP,OPERATEREVE,
        OPERATEREVEYOY,PARENTNETPROFIT,PARENTNETPROFITYOY,
        SPECIALINDEX1,SPECIALINDEX2,ADSNUM,TOTALVALUE)
        VALUES (%s,%s,%s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f)
        """.format(table_name)
    elif form == 'gsgknb':
        cursor.execute("use {}".format(table_name))
        sql = """
        INSERT INTO {}GSGKNB
        (checkdate,SECURITYCODE,SECURITYSHORTNAME,COMPNAME,
        COMPNAMECN,SECURITYTYPE,TRADEMARKET,CHAIRMAN,
        INDUSTRY,FOUNDDATE,LISTEDDATE,OFFICEADDRESS,WEBSITE)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """.format(table_name)
    try:
        cursor.executemany(sql,data)
        db.commit()
        print(table_name+"数据插入成功")
    except Exception as e:
        db.rollback()
        print(e)

# 读取数据
def read_stockdata_table(db,cursor,table_name,sql=""):
    """
    读取股票价格数据,如果传入sql则执行sql，否则为读取表格全部数据
    :param db: db的连接
    :param cursor: 游标
    :param table_name: 数据库的名字
    :return: 返回读取的数据Dataframe
    """
    cursor.execute("use {}".format(table_name))
    if sql != "":
        df = pd.read_sql(sql,db)
        return df
    else:
        insql = "SELECT * FROM {}PRICE".format(table_name)
        df = pd.read_sql(insql,db)
        return df


# 删除数据
def drop_database(db,cursor,db_name):
    cursor.execute("drop database if exists {}".format(db_name))
    db.commit()
    print(db_name+"数据库删除成功")

def drop_table(db,cursor,table_name):
    cursor.execute("drop table if exists {}".format(table_name))
    db.commit()
    print(table_name+"数据表删除成功")

if __name__ == '__main__':
    # 连接数据库
    db = conn_sql("139.9.222.28",'chiwang','wc1998wc0322wc')
    # 创建游标
    cursor = db.cursor()
    """
    创建数据库
    """
    # # 创建股指数据库
    # stockindex = ['NASDAQ','NYSE']
    # [creat_stockindex_db(db=db,cursor=cursor,stockindex_name=stock) for stock in stockindex]
    # # 创建个股数据库
    # cncp_stocks = ['AAPL','BLBL','ALBB']
    # [creat_cncpstock_db(db=db,cursor=cursor,cncpstock_name=stock) for stock in cncp_stocks]
    # # 创建价格数据表
    # cncp_stocks = ['AAPL', 'BLBL', 'ALBB']
    # [creat_stockdata_table(db=db, cursor=cursor, stock_name=stock) for stock in cncp_stocks]

    """
    创建表
    """
    # 创建股票行情数据表
    data = [('2021-4-12',1.2,1.3,1.4,1.5,1.6),
            ('2021-4-13',1.2,1.3,1.4,1.5,1.6)]
    # insert_stockdata_table(db,cursor,data,'AAPL')

    # 创建股票财务数据表


    """
    停止连接
    """
    # 停止连接
    cursor.close()
    db.close()


