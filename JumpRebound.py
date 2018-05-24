# -*- coding: utf-8 -*-
# author: wenyun
# 跳跌反弹就单线分析，不需要其他盘面数据
# 跌幅超过一定程度之后，接下来的一个时间段内，能往回反弹一定百分比的概率
# 跳跌就是时间更短，幅度更大的跌幅，比如一分钟就跌了30个点
# 比如，当前点跌5%,然后往前找5分钟看有没有跌15%的瞬间
# 跳跌的比例 = (往前一定时间段内最低值 - 最高值)/最高值   (15%)
# 反弹的比例 = 跳跌的比例 - 当前的比例
import pymongo
import time
from datetime import datetime
import pytz

class DBConnection: # 连接数据库，获取数据
    def __init__(self):
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client['klinedata']
        self.open_data = {}
        self.data = {}

    def deter_data(self, minute, left): #确定拉取哪些数据， 当前时间分钟数，jump_period剩余数
        if minute <= 0 or minute > 60:
            print("[db] unkonwn minute " + str(minute))
            raise Exception('unkonwn minute ' + str(minute))
        elif left >= 60 and 0 == minute % 60:
            self.dur = 60
        elif left >= 30 and 0 == minute % 30:
            self.dur = 30
        elif left >= 15 and 0 == minute % 15:
            self.dur = 15
        elif left >= 5 and 0 == minute % 5:
            self.dur = 5
        else:
            self.dur = 1

    def get_data(self, symbol, minute):
        if(not self.db.tradepair.find({"pair":symbol})):
            print("[DBConnection] unkonwn symbol " + symbol)
            return
        self.data = {}
        symbol += "_" + str(minute) + "min"
        cur = self.db[symbol].find()
        for ele in cur:
            self.data[ele["id"]] = ele
        return self.data
    
class Statistics: # 数据处理
    def __init__(self, symbol, jump_period, jump_percent, rebound_percent):
        self.symbol = symbol
        self.jump_period = jump_period
        self.jump_percent = jump_percent
        self.rebound_percent = rebound_percent
        self.db = DBConnection()
        self.check_data = self.db.get_data(symbol, 1)
        self.jump_data = {}
        self.init_processed_data()
        self.init_detail_data()
    
    def init_processed_data(self): # 初始化用于统计天数的字典
        self.processed_data = {}
        self.processed_data["jump"] = 0 # 跌下去的天数
        self.processed_data["rebound"] = 0 # 弹回来的天数

    def init_detail_data(self): # 初始化详细数据(将符合要求的数据的详细信息存储起来)
        self.detail_data = {}
        self.detail_data["high"] = {} # 一定时间段内的最高点
        self.detail_data["low"] = {} # 一定时间段内的最低点
        self.detail_data["close"] = {} # 当前点
    
    def jump_rebound_count(self):
        ele = max(list(self.check_data.keys()))
        while ele in self.check_data:
            high = self.find_high(ele)
            if self.jump(ele, high):
                self.cur(ele, high)
            ele -= 60
        if self.processed_data["jump"] > 0:
            print(str(self.processed_data["rebound"]) + "/" + str(self.processed_data["jump"]) + "="
                + str(self.processed_data["rebound"]/self.processed_data["jump"]))

    def jump(self, id, high):
        low = self.find_low(id)
        # 算比例，跳跌的比例 = (往前一定时间段内最低值 - 最高值)/最高值
        self.jump_per = (low["low"] - high["high"])/high["high"]
        if self.jump_per <= 0 - self.jump_percent:
            self.processed_data["jump"] += 1
            self.detail_data["high"][high["id"]] = high
            self.detail_data["low"][low["id"]] = low
            print("[HIGH] " + str(high["id"]) + " " + time.ctime(high["id"]) + ":" + "high_" + str(high["high"]))
            print("[YES_JUMP_LOW] " + str(low["id"]) + " " + time.ctime(low["id"]) + ":" + "low_" + str(low["low"]) + "_per" + str(self.jump_per))
            return True
        return False

    def cur(self, id, high): # 当前
        cur_val = self.check_data[id] # 当前时间点的值
        # 算当前涨幅比例
        self.cur_per = (cur_val["close"] - high["high"])/high["high"]
        if self.cur_per - self.jump_per >= self.rebound_percent:
            self.processed_data["rebound"] += 1
            self.detail_data["close"][cur_val["id"]] = cur_val # 反弹到的值
            print("[YES_RBND_CLS] " + str(cur_val["id"]) + " " + time.ctime(cur_val["id"]) + ":" + "close_" + str(cur_val["close"]) + "_per" + str(self.cur_per - self.jump_per))
        else:
            print("[NO_REBND_CLS] " + str(cur_val["id"]) + " " + time.ctime(cur_val["id"]) + ":" + "close_" + str(cur_val["close"]) + "per" + str(self.cur_per - self.jump_per))

    def find_low(self, id):
        minute = id % 86400 % 3600 / 60 # 剩余分钟数
        left = self.jump_period  # 剩余时长
        # 时间往前推,找到最低值
        low = self.check_data[id]
        low_val = low["high"]
        while left > 0:
            if 0 == minute:
                minute = 60
            self.db.deter_data(minute, left)
            period = str(self.db.dur) + "min"
            if not period in self.jump_data:
                self.jump_data[period] = self.db.get_data(self.symbol, self.db.dur) # 获取该阶段原始数据
            minute -= self.db.dur
            left -= self.db.dur
            # print(self.db.dur)
            if id in self.check_data and low_val > self.jump_data[str(self.db.dur) + "min"][id]["low"]: # 这个时间段的最低值低于0点开值
                low = self.jump_data[str(self.db.dur) + "min"][id]
                low_val = low["low"]
            id -= self.db.dur * 60
        return low

    def find_high(self, id):
        minute = id % 86400 % 3600 / 60 # 剩余分钟数
        left = self.jump_period  # 剩余时长
        # 时间往前推,找到最低值
        high = self.check_data[id]
        high_val = high["low"]
        while left > 0:
            if 0 == minute:
                minute = 60
            self.db.deter_data(minute, left)
            period = str(self.db.dur) + "min"
            if not period in self.jump_data:
                self.jump_data[period] = self.db.get_data(self.symbol, self.db.dur) # 获取该阶段原始数据
            minute -= self.db.dur
            left -= self.db.dur
            # print(self.db.dur)
            if id in self.check_data and high_val < self.jump_data[str(self.db.dur) + "min"][id]["high"]:
                high = self.jump_data[str(self.db.dur) + "min"][id]
                high_val = high["high"]
            id -= self.db.dur * 60
        return high

class Main:
    def __init__(self, symbol, jump_period, jump_percent, rebound_percent):
        self.symbol = symbol
        self.jump_period = jump_period
        self.jump_percent = jump_percent
        self.rebound_percent = rebound_percent
        self.db_conn = DBConnection()
        # 用于遍历的数据，0点开盘价，所请求查找的跌幅时间段，跌幅，反弹幅
        self.statistics = Statistics(symbol, jump_period, jump_percent, rebound_percent)
        
    def start(self):
        print(self.symbol + "_jump_" + str(self.jump_period) + "min_per" + str(self.jump_percent) + "_rebound_per" + str(self.rebound_percent))
        self.statistics.jump_rebound_count()

if __name__ == "__main__":
    main = Main("socusdt", 7, 0.1, 0.1) # 是几分钟就输入几，跌幅，回弹幅度
    main.start()