import pandas as pd
import pymysql
from get_pricedata import *
from creat_database import *
import datetime
from datetime import datetime
import numpy as np
from change_table import *
if __name__ == "__main__":
    db = conn_sql("139.9.222.28",'lulu','lulu666')
    cursor = db.cursor()
    # periods = ["year","quarter", "month", "week", "day"]
    # for period in periods:
    #     table = read_change_table(db, cursor, period)
    #     pd.DataFrame.to_csv(table,"{}_change_table.csv".format(period))

    # 对增幅表排序
    periods = ["year","quarter", "month", "week", "day"]
    for period in periods:
        df = sort_change_table(db, cursor, period, 10, True)
        print(df)
        pd.DataFrame.to_csv(df,"{}_change_sort.csv".format(period))