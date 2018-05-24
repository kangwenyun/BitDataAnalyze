# -*- coding: utf-8 -*-
# author: wenyun
# 单独周五跌\周六涨\周日涨\周五跌，然后周六涨\周五跌，周日涨

import pymongo
import time

class DBConnection: # 连接数据库，获取数据
    def __init__(self):
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client['klinedata']
        self.data = []

    def get_data(self, symbol):
        if(not self.db.tradepair.find({"pair":symbol})):
            print("[DBConnection] unkonwn symbol " + symbol)
            return
        symbol += '_1min'
        cur = self.db[symbol].find()
        for ele in cur:
            self.data.append(ele)
        return self.data

class Statistics: # 数据处理
    def __init__(self, data):
        self.original_data = data
        self.processed_data = {}
        self.detail_data = {}
        self.init_processed_data()
        self.init_detail_data()
        self.get_total_day()

    def init_processed_data(self): # 初始化处理过的数据
        self.processed_data["single_day"] = {} # 单独涨
        self.processed_data["total_day"] = {} # 各有多少个周五六日
        self.processed_data["single_day"]["rise"] = {}
        self.processed_data["single_day"]["rise"]["Sat"] = 0 # 周六涨天数
        self.processed_data["single_day"]["rise"]["Sun"] = 0 # 周日涨天数
        self.processed_data["single_day"]["rise"]["Fri"] = 0 # 周五涨天数
        self.processed_data["total_day"]["Sat"] = 0 # 周六总天数
        self.processed_data["total_day"]["Sun"] = 0 # 周日总天数
        self.processed_data["total_day"]["Fri"] = 0 # 周五总天数
        self.processed_data["after_friday_fall"] = {}
        self.processed_data["after_friday_fall"]["Sat"] = {}
        self.processed_data["after_friday_fall"]["Sun"] = {}
        self.processed_data["after_friday_fall"]["Sat"] = 0 # 周六涨
        self.processed_data["after_friday_fall"]["Sun"] = 0 # 周日涨
    
    def init_detail_data(self): # 初始化详细数据(将符合要求的数据的详细信息存储起来)
        self.detail_data["single_day"] = {}
        self.detail_data["single_day"]["rise"] = {}
        self.detail_data["single_day"]["rise"]["Sat"] = {} # 周六涨天数
        self.detail_data["single_day"]["rise"]["Sun"] = {} # 周日涨天数
        self.detail_data["single_day"]["rise"]["Fri"] = {} # 周五涨天数
        self.detail_data["after_friday_fall"] = {}
        self.detail_data["after_friday_fall"]["Sat"] = {}
        self.detail_data["after_friday_fall"]["Sun"] = {}
        self.detail_data["after_friday_fall"]["Sat"] = {}
        self.detail_data["after_friday_fall"]["Sun"] = {}

    def get_total_day(self): # 周五六日总天数
        for index in range(0, len(self.original_data)):
            if time.ctime(self.original_data[index]["id"]).split(" ")[0] == "Fri":
                self.processed_data["total_day"]["Fri"] += 1
            elif time.ctime(self.original_data[index]["id"]).split(" ")[0] == "Sat":
                self.processed_data["total_day"]["Sat"] += 1
            elif time.ctime(self.original_data[index]["id"]).split(" ")[0] == "Sun":
                self.processed_data["total_day"]["Sun"] += 1
    
    def single_day_count(self, data): # 统计符合要求的天数
        for index in range(0, len(self.original_data)): # 对原始数据进行处理
            open = self.original_data[index]["open"]
            close = self.original_data[index]["close"]
            tm = time.ctime(self.original_data[index]["id"])
            self.original_data[index]["tm"] = tm
            day = tm.split(" ")[0]
            if day == "Fri" and int(open) < int(close): # 周五涨天数
                self.processed_data["single_day"]["rise"]["Fri"] += 1
                self.detail_data["single_day"]["rise"]["Fri"][tm] = self.original_data[index]
            elif day == "Sat" and int(open) < int(close): # 周六涨天数
                self.processed_data["single_day"]["rise"]["Sat"] += 1
                self.detail_data["single_day"]["rise"]["Sat"][tm] = self.original_data[index]
            elif day == "Sun" and int(open) < int(close): # 周日涨天数
                self.processed_data["single_day"]["rise"]["Sun"] += 1
                self.detail_data["single_day"]["rise"]["Sun"][tm] = self.original_data[index]

    def single_day_print(self): # 打印比例
        self.single_day_count()
        print("单独周五跌：" + str(self.processed_data["total_day"]["Fri"] - self.processed_data["single_day"]["rise"]["Fri"]) + "/" + str(self.processed_data["total_day"]["Fri"])
             + "=" + str(1 - self.processed_data["single_day"]["rise"]["Fri"]/self.processed_data["total_day"]["Fri"]))
        print("单独周六涨：" + str(self.processed_data["single_day"]["rise"]["Sat"]) + "/" + str(self.processed_data["total_day"]["Sat"])
             + "=" + str(self.processed_data["single_day"]["rise"]["Sat"]/self.processed_data["total_day"]["Sat"]))
        print("单独周日涨：" + str(self.processed_data["single_day"]["rise"]["Sun"]) + "/" + str(self.processed_data["total_day"]["Sun"])
             + "=" + str(self.processed_data["single_day"]["rise"]["Sun"]/self.processed_data["total_day"]["Sun"]))
        print("周五：")
        for key in self.detail_data["single_day"]["rise"]["Fri"]:
            print(key)
        print("周六：")
        for key in self.detail_data["single_day"]["rise"]["Sat"]:
            print(key)
        print("周日：")
        for key in self.detail_data["single_day"]["rise"]["Sun"]:
            print(key)

    def after_friday_count(self): # 统计周五跌周六周日涨的天数
        for index in range(0, len(self.original_data)): # 对原始数据进行处理
            open = self.original_data[index]["open"]
            close = self.original_data[index]["close"]
            tm = time.ctime(self.original_data[index]["id"])
            day = tm.split(" ")[0]
            if day == "Fri" and int(open) > int(close): # 周五跌
                sat_index = index-1 # 跟这个周五同一周的周六
                if sat_index > 0:
                    sat_open = self.original_data[sat_index]["open"]
                    sat_close = self.original_data[sat_index]["close"]
                    sat_tm = time.ctime(self.original_data[sat_index]["id"])
                    if int(sat_open) < int(sat_close): # 周六涨
                        self.processed_data["after_friday_fall"]["Sat"] += 1
                        self.detail_data["after_friday_fall"]["Sat"][tm] = self.original_data[index]
                        self.detail_data["after_friday_fall"]["Sat"][sat_tm] = self.original_data[sat_index]
                sun_index = index-2 # 跟这个周五同一周的周日
                if sun_index > 0:
                    sun_open = self.original_data[sun_index]["open"]
                    sun_close = self.original_data[sun_index]["close"]
                    sun_tm = time.ctime(self.original_data[sun_index]["id"])
                    if int(sun_open) < int(sun_close): # 周日涨
                        self.processed_data["after_friday_fall"]["Sun"] += 1
                        self.detail_data["after_friday_fall"]["Sun"][tm] = self.original_data[index]
                        self.detail_data["after_friday_fall"]["Sun"][sun_tm] = self.original_data[sun_index]
    
    def after_friday_print(self): # 打印比例
        self.after_friday_count()
        print("周五跌周六涨：" + str(self.processed_data["after_friday_fall"]["Sat"]) + "/" + str(self.processed_data["total_day"]["Sat"])
             + "=" + str(self.processed_data["after_friday_fall"]["Sat"]/self.processed_data["total_day"]["Sat"]))
        print("周五跌周日涨：" + str(self.processed_data["after_friday_fall"]["Sun"]) + "/" + str(self.processed_data["total_day"]["Sun"])
             + "=" + str(self.processed_data["after_friday_fall"]["Sun"]/self.processed_data["total_day"]["Sun"]))
        print("周五周六：")
        for key in self.detail_data["after_friday_fall"]["Sat"]:
            print(key)
        print("周五周日：")
        for key in self.detail_data["after_friday_fall"]["Sun"]:
            print(key)

class Main:
    def __init__(self, symbol):
        self.symbol = symbol
        self.db_conn = DBConnection()
        self.statistics = Statistics(self.db_conn.get_data(self.symbol))
        
    def start(self):
        print(self.symbol)
        # self.statistics.single_day_print(data)
        self.statistics.after_friday_print()


if __name__ == "__main__":
    main = Main("btcusdt")
    main.start()