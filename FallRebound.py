# -*- coding: utf-8 -*-
# author: wenyun
# 跟跌反弹就单线分析，不需要其他盘面数据
# 跌幅超过一定程度之后，接下来的一个时间段内，能往回反弹一定百分比的概率
# 跌幅跟反弹的幅度也是相对于当天开盘价
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
        symbol += '_1day'
        cur = self.db[symbol].find()
        for ele in cur:
            self.data.append(ele)
        return self.data

class Statistics: # 数据处理
    def __init__(self, data):
        self.original_data = data
        self.processed_data = {}
        self.detail_data = {}

class Main:
    def __init__(self, symbol):
        self.symbol = symbol
        self.db_conn = DBConnection()
        self.statistics = Statistics(self.db_conn.get_data(self.symbol))
        
    def start(self):
        print(self.symbol)
        self.statistics.after_friday_print()


if __name__ == "__main__":
    main = Main("iostusdt")
    main.start()