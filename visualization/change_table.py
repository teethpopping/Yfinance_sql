import pandas as pd
import pymysql
from get_pricedata import *
from creat_database import *
import datetime
from datetime import datetime
import numpy as np

if __name__ == "__main__":
  db = conn_sql("139.9.222.28",'lulu','lulu666')
  cursor = db.cursor()
  
  cncp_stocks = list(set(get_cncon_stock()))
  for stock in cncp_stocks:
      table = read_stockdata_table(db, cursor, stock)
    
      
  create_change_db(db,cursor)
  periods = ["day","week", "month","quarter","year"]
  for period in periods:
    create_change_table(db, cursor,period)
  
        
  for stock in cncp_stocks:
      day_change = []
      week_change = []
      month_change = []
      quarter_change = []
      year_change = []
      table = read_stockdata_table(db, cursor, stock)
      table['date'] = pd.to_datetime(table['date'])
      table = table.set_index(table["date"])
      pd.DataFrame.to_csv(table,"stocks/{}.csv".format(stock))
      table.CLOSE = table.CLOSE.astype(float)
      table["daily_change"] = 100*table["CLOSE"].diff()/table["CLOSE"].shift()
      for i in range(table.shape[0]):
        # insert daily change 
        # print(table["date"][i].date())
        data = (stock, table["date"][i].date(), str(table["daily_change"][i]))
        day_change.append(data)
      # print(day_change)
        # calculate week_change
      # 插入每日涨跌幅
      insert_change_table(db, cursor, day_change, "day")
      
      week = table.resample("W-MON")["CLOSE"].last()
      week["week_change"] = 100 * week.diff()/week.shift()
      for date, change in week["week_change"].items():
        week_data = (stock, date.strftime("%Y-Week-%V"), str(change))
        week_change.append(week_data)
      # print(week_change)
      insert_change_table(db, cursor, week_change, "week")

      month = table.resample("M")["CLOSE"].last()
    
      month["month_change"] = 100 * month.diff()/ month.shift()
      for date, change in month["month_change"].items():
        month_data = (stock, date.strftime("%Y-Month-%m"), str(change))
        month_change.append(month_data)
      # print(month_change)
      insert_change_table(db, cursor, month_change, "month")


      quarter = table.resample("Q")["CLOSE"].last()

      quarter["quarter_change"] = 100 * quarter.diff()/quarter.shift()
      for date, change in quarter["quarter_change"].items():
        quarter_data = (stock, str(date.year) + "-Quarter-"+ str(date.quarter), str(change))
        quarter_change.append(quarter_data)
      # print(quarter_change)
      insert_change_table(db, cursor, quarter_change, "quarter")


      year = table.resample("Y")["CLOSE"].first()
      year["year_change"] = 100 *year.diff()/ year.shift()
      for date, change in year["year_change"].items():
        year_data = (stock, date.strftime("%Y"), str(change))
        year_change.append(year_data)
      # print(year_change)
      insert_change_table(db, cursor, year_change, "year")
      # print("week")
      # print(week)
      # print(type(week))
      # print("*****************************")
      # print("month")
      # print(month)
      # print("*****************************")
      # print("quarter")
      # print(quarter)
      # print("*****************************")
      # print("year")
      # print(year)

    
  cursor.close()
  db.close()