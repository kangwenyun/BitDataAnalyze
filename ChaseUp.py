# -*- coding: utf-8 -*-
# author: wenyun
# 追涨：基本面大盘普涨的情况下，哪个币之前跌的多，
# 且刚冒出来涨了1-2个百分点的币，这个是追涨的策略。
# ① 大盘普涨：相对于开价,btc涨(涨幅要超过2个点),且其他一定比例的币也涨,比例设为可调整的参数（80%）
#   计算量就大了，每分钟都要计算，每分钟要用到所有数据
# ② 之前跌得多的：
#   之前：设为参数，看之前几天的日线走势
#   幅度：设为参数，= (这几天的最高价 - 当前价)/这几天的最高价
# ③ 刚冒出来涨了1-2个百分点：相对于当天开盘价格，(当前价 - 今天开盘价)/今天开盘价>=1.5%
#   输出识别有多少个这样的情况，跟这种情况下当天后期达到最高的收益是多少
# 每个币的参数可能不一样，算法一样，然后可能得每个币单独跑
import pymongo
import time
from datetime import datetime
import pytz
import json
import csv

class DBConnection: # 连接数据库，获取数据
    def __init__(self):
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client['klinedata']
        self.db.authenticate('klineapp','klineapp')

    def get_data(self, symbol, unit, k_id):
        if(not self.db.tradepair.find({"pair":symbol})):
            print("[DBConnection] unkonwn symbol " + symbol)
            return
        symbol += '_' + unit
        return self.db[symbol].find_one({'id': k_id})

    def get_all_time(self, symbol):
        if(not self.db.tradepair1.find({"pair":symbol})):
            print("[DBConnection] unkonwn symbol " + symbol)
            return
        time = []
        symbol += "_1min"
        cur = self.db[symbol].find()
        for ele in cur:
            time.append(ele["id"])
        return time

class Statistics: # 数据处理
    def __init__(self, currency):
        self.currency = currency
        self.db = DBConnection()
        self.coin_king = "btcusdt"
        self.data = {}
        self.init_per()

    def init_per(self):
        self.per = {}
        self.per['up'] = 0
        self.per['up_[0,5)'] = 0
        self.per['up_[5,10)'] = 0
        self.per['up_[10,20)'] = 0
        self.per['up_[20,30)'] = 0
        self.per['up_[30,40)'] = 0
        self.per['up_[40,50)'] = 0
        self.per['up_[50,60)'] = 0
        self.per['up_[60,70)'] = 0
        self.per['up_[70,80)'] = 0
        self.per['up_[80,90)'] = 0
        self.per['up_[90,100)'] = 0
        self.per['up_[100+)'] = 0

        self.per['fall'] = 0
        self.per['fall_[0,5)'] = 0
        self.per['fall_[5,10)'] = 0
        self.per['fall_[10,20)'] = 0
        self.per['fall_[20,30)'] = 0
        self.per['fall_[30,40)'] = 0
        self.per['fall_[40,50)'] = 0
        self.per['fall_[50,60)'] = 0
        self.per['fall_[60,70)'] = 0
        self.per['fall_[70,80)'] = 0
        self.per['fall_[80,90)'] = 0
        self.per['fall_[90,100)'] = 0
        self.per['fall_[100+)'] = 0
        
    def get_open_time(self, k_id): # 获取当前时间戳所在天数0点的时间戳
        dt = time.strftime("%Y-%m-%d", time.localtime(k_id))
        tz = pytz.timezone('Asia/Shanghai')
        y = int(dt.split("-")[0])
        m = int(dt.split("-")[1])
        d = int(dt.split("-")[2])
        dt = tz.localize(datetime(y, m, d))
        tm = time.mktime(dt.timetuple())
        return tm

    def get_data(self, coin, unit, k_id):
        if '1day' == unit:
            key = coin + '_' + unit + '_' + str(self.get_open_time(k_id))
        else:
            key = coin + '_' + unit + '_' + str(k_id)
        if not key in self.data:
            if '1day' == unit:
                self.data[key] = self.db.get_data(coin, unit, self.get_open_time(k_id))
            else:
                self.data[key] = self.db.get_data(coin, unit, k_id)
            # f = open('/home/wencloud/txt/key.txt', 'a')
            # if '1day' == unit:
            #     f.writelines("self.data[" + key + ']:' + str(self.data[key]) + '\n')
            # else:
            #     f.writelines("self.data[" + key + ']:' + str(self.data[key]) + '\n')
            # f.close()
        return self.data[key]
    
    def open_up_percent(self, coin, k_id): # 给定时间戳，跟开价比，算币的涨幅
        open_item = self.get_data(coin, '1day', self.get_open_time(k_id)) # 先获取0点开盘价
        cur_item = self.get_data(coin, '1min', k_id) # 当前价
        if open_item and cur_item:
            return (cur_item['close']-open_item['open'])/open_item['open']
        else:
            return -1
    
    def other_up(self, k_id, king_up_percent, other_up_percent): # 跟币王一起涨起来的币的比例
        per = self.open_up_percent(self.coin_king, k_id)
        if per >= king_up_percent:
            up_data = 0
            total_data = 0
            self.detail_data = {}
            # 对每一个币进行计算，看是不是涨
            self.detail_data[str(k_id) + '_total'] = []
            self.detail_data[str(k_id) + '_up'] = []
            for coin in self.currency:
                total_data += 1
                self.detail_data[str(k_id) + '_total'].append(coin)
                up_percent = self.open_up_percent(coin, k_id)
                if up_percent >= other_up_percent:
                    up_data += 1
                    self.detail_data[str(k_id) + '_up'].append(coin)
            up = up_data
            total = total_data
            if total > 0:
                return float(up)/total
        return -1
        
    def get_high_before(self, coin, k_id, period): # 找id所代表日期前几天该币的最高价（包括当天）
        open_id = self.get_open_time(k_id) # 0点时间戳
        high = 0
        high_time = 0
        while period >= 0:
            tmp = self.get_data(coin, '1day', open_id)
            if tmp and high < tmp['high']:
                high = tmp['high']
                high_time = open_id
            period -= 1
            open_id -= 86400
        return high

    def fall_percent_to_high(self, coin, k_id, period): # 在之前给定时间段内的跌幅 = (这几天的最高价 - 当前价)/这几天的最高价
        self.cur_item = self.get_data(coin, '1min', k_id)
        self.high = self.get_high_before(coin, k_id, period)
        if self.cur_item and 0 != self.high:
            return (self.high - self.cur_item['close'])/self.high
        return -1

    def get_highest_ret(self, coin, k_id): # 当天后期达到的最高收益
        end_id = self.get_open_time(k_id) + 86400 # 后一天0点时间戳
        tmp_id = k_id + 60
        highest = 0
        high_time = 0
        while tmp_id < end_id:
            tmp = self.get_data(coin, '1min', tmp_id)
            if tmp and highest < tmp['high']:
                highest = tmp['high']
                high_time = tmp_id
            tmp_id += 60
            h = self.get_data(coin, '1day', k_id)
            if h and highest == h['high']:
                break
        return highest

    def up_now(self, coin, k_id): # 当前涨幅 = (当前价 - 今天开盘价)/今天开盘价
        self.open_item = self.get_data(coin, '1day', self.get_open_time(k_id))
        if self.cur_item and self.open_item:
            return (self.cur_item['close'] - self.open_item['open'])/self.open_item['open']
        return -1

    # 对每一个时间戳进行扫描,跟btc一起涨起来的币的比例,算跌幅的时间段，之前跌得多的比例，当前涨起来的比例的范围
    def scan(self, up_percent_follow_king, period, fall_percent, low_up_percent, high_up_percent):
        lst = self.db.get_all_time(self.coin_king) # 所有id
        res = []
        per = {}
        _id = 1
        for item in lst:
            print(item)
            ufk = self.other_up(item, 0.02, 0.02)
            if ufk >= up_percent_follow_king:
                for coin in self.currency:
                    fall = self.fall_percent_to_high(coin, item, period) 
                    up = self.up_now(coin, item)
                    ret = self.get_highest_ret(coin, item)
                    if fall >= fall_percent and up >= low_up_percent and up <= high_up_percent: # 之前跌得多的,涨幅在范围内的
                        # 如果确定买入了，这个币后边区间的数据要去掉
                        flag = True # 这个币不在任何已加入集合中的数据的区间内
                        for it in res:
                            if it['find_coin'] == coin and item < it['_0_id'] + 86400:
                                flag = False
                                break
                        if flag:
                            ele = {}
                            ele['_id'] = _id
                            ele['follow_btc'] = ufk
                            ele['follow_coins'] = self.detail_data[str(item) + '_up']
                            ele['datetime'] = time.ctime(item)
                            ele['id'] = item
                            ele['_0_id'] = self.get_open_time(item)
                            ele['find_coin'] = coin
                            ele['fall_percent'] = fall
                            ele['current_up'] = up
                            ele['current_close'] = self.cur_item['close']
                            ele['history_high'] = self.high
                            ele['history_period'] = period
                            ele['today_open'] = self.open_item['open']
                            ele['future_high'] = ret
                            ele['future_ratio'] = (ret - self.cur_item['close'])/self.cur_item['close']
                            res.append(ele)
                            _id += 1
                            if ele['future_ratio'] < 0:
                                self.per["fall_" + self.classify(ele['future_ratio'])] += 1
                                self.per['fall'] += 1
                            else:
                                self.per["up_" + self.classify(ele['future_ratio'])] += 1
                                self.per['up'] += 1
        if len(res) > 0:
            with open('/home/wencloud/txt/ChaseUp.txt', 'a') as f:
                for ele in res:
                    ele_json = json.dumps(ele, indent = 4)
                    f.writelines(ele_json + '\n')
        f = open('/home/wencloud/txt/ChaseUp.txt', 'a')
        f.writelines("[ChaseUp]scan ok" + '\n')
        f.close()
        if len(res) > 0:
            self.convert_to_csv(res)
            self.write_table()
    
    def convert_to_csv(self, data): # data = [{'time':,'coin':, 'future_ratio':},{},...]
        with open('/home/wencloud/txt/ChaseUp.csv','a') as csvfile:
            writer = csv.writer(csvfile)
            #先写入columns_name
            writer.writerow(["time","coin","future_ratio"])
            for ele in data:
                writer.writerow([ele['datetime'], ele['find_coin'], ele['future_ratio']])
        f = open('/home/wencloud/txt/ChaseUp.csv', 'a')
        f.writelines("[ChaseUp]convert ok" + '\n')
        f.close()

    def classify(self, ratio):
        ret = '0'
        if ratio <= -1 or ratio >= 1:
            ret = '[100+)'
        elif (ratio > -1 and ratio <= -0.9) or (ratio >= 0.9 and ratio < 1):
            ret = '[90,100)'
        elif (ratio > -0.9 and ratio <= -0.8) or (ratio >= 0.8 and ratio < 0.9):
            ret = '[80,90)'
        elif (ratio > -0.8 and ratio <= -0.7) or (ratio >= 0.7 and ratio < 0.8):
            ret = '[70,80)'
        elif (ratio > -0.7 and ratio <= -0.6) or (ratio >= 0.6 and ratio < 0.7):
            ret = '[60,70)'
        elif (ratio > -0.6 and ratio <= -0.5) or (ratio >= 0.5 and ratio < 0.6):
            ret = '[50,60)'
        elif (ratio > -0.5 and ratio <= -0.4) or (ratio >= 0.4 and ratio < 0.5):
            ret = '[40,50)'
        elif (ratio > -0.4 and ratio <= -0.3) or (ratio >= 0.3 and ratio < 0.4):
            ret = '[30,40)'
        elif (ratio > -0.3 and ratio <= -0.2) or (ratio >= 0.2 and ratio < 0.3):
            ret = '[20,30)'
        elif (ratio > -0.2 and ratio <= -0.1) or (ratio >= 0.1 and ratio < 0.2):
            ret = '[10,20)'
        elif (ratio > -0.1 and ratio <= -0.05) or (ratio >= 0.05 and ratio < 0.1):
            ret = '[5,10)'
        elif ratio > -0.05 and ratio < 0.05:
            ret = '[0,5)'
        return ret

    def write_table(self): # per = [{'fall_[80 90)':,'up_[80 90)':},{},...]
        with open('/home/wencloud/txt/percent.csv','a') as csvfile:
            writer = csv.writer(csvfile)
            #先写入columns_name
            writer.writerow(["涨幅区间","比例"])
            if self.per['up'] > 0:
                writer.writerows([["[0,5)", str(self.per['up_[0,5)']) + '/ ' + str(self.per['up']) + '=' + str(float(self.per['up_[0,5)'])/self.per['up'])], 
                                ["[5,10)", str(self.per['up_[5,10)']) + '/ ' + str(self.per['up']) + '=' + str(float(self.per['up_[5,10)'])/self.per['up'])], 
                                ["[10,20)", str(self.per['up_[10,20)']) + '/ ' + str(self.per['up']) + '=' + str(float(self.per['up_[10,20)'])/self.per['up'])], 
                                ["[20,30)", str(self.per['up_[20,30)']) + '/ ' + str(self.per['up']) + '=' + str(float(self.per['up_[20,30)'])/self.per['up'])], 
                                ["[30,40)", str(self.per['up_[30,40)']) + '/ ' + str(self.per['up']) + '=' + str(float(self.per['up_[30,40)'])/self.per['up'])], 
                                ["[40,50)", str(self.per['up_[40,50)']) + '/ ' + str(self.per['up']) + '=' + str(float(self.per['up_[40,50)'])/self.per['up'])],
                                ["[50,60)", str(self.per['up_[50,60)']) + '/ ' + str(self.per['up']) + '=' + str(float(self.per['up_[50,60)'])/self.per['up'])], 
                                ["[60,70)", str(self.per['up_[60,70)']) + '/ ' + str(self.per['up']) + '=' + str(float(self.per['up_[60,70)'])/self.per['up'])], 
                                ["[70,80)", str(self.per['up_[70,80)']) + '/ ' + str(self.per['up']) + '=' + str(float(self.per['up_[70,80)'])/self.per['up'])], 
                                ["[80,90)", str(self.per['up_[80,90)']) + '/ ' + str(self.per['up']) + '=' + str(float(self.per['up_[80,90)'])/self.per['up'])], 
                                ["[90,100)", str(self.per['up_[90,100)']) + '/ ' + str(self.per['up']) + '=' + str(float(self.per['up_[90,100)'])/self.per['up'])], 
                                ["[100+)", str(self.per['up_[100+)']) + '/ ' + str(self.per['up']) + '=' + str(float(self.per['up_[100+)'])/self.per['up'])]])
            else:
                writer.writerow("self.per['up'] == 0")
            writer.writerow(["跌幅区间","比例"])
            if self.per['fall'] > 0:
                writer.writerows([["[0,5)", str(self.per['fall_[0,5)']) + '/ ' + str(self.per['fall']) + '=' + str(float(self.per['fall_[0,5)'])/self.per['fall'])], 
                                    ["[5,10)", str(self.per['fall_[5,10)']) + '/ ' + str(self.per['fall']) + '=' + str(float(self.per['fall_[5,10)'])/self.per['fall'])], 
                                    ["[10,20)", str(self.per['fall_[10,20)']) + '/ ' + str(self.per['fall']) + '=' + str(float(self.per['fall_[10,20)'])/self.per['fall'])], 
                                    ["[20,30)", str(self.per['fall_[20,30)']) + '/ ' + str(self.per['fall']) + '=' + str(float(self.per['fall_[20,30)'])/self.per['fall'])], 
                                    ["[30,40)", str(self.per['fall_[30,40)']) + '/ ' + str(self.per['fall']) + '=' + str(float(self.per['fall_[30,40)'])/self.per['fall'])], 
                                    ["[40,50)", str(self.per['fall_[40,50)']) + '/ ' + str(self.per['fall']) + '=' + str(float(self.per['fall_[40,50)'])/self.per['fall'])],
                                    ["[50,60)", str(self.per['fall_[50,60)']) + '/ ' + str(self.per['fall']) + '=' + str(float(self.per['fall_[50,60)'])/self.per['fall'])], 
                                    ["[60,70)", str(self.per['fall_[60,70)']) + '/ ' + str(self.per['fall']) + '=' + str(float(self.per['fall_[60,70)'])/self.per['fall'])], 
                                    ["[70,80)", str(self.per['fall_[70,80)']) + '/ ' + str(self.per['fall']) + '=' + str(float(self.per['fall_[70,80)'])/self.per['fall'])], 
                                    ["[80,90)", str(self.per['fall_[80,90)']) + '/ ' + str(self.per['fall']) + '=' + str(float(self.per['fall_[80,90)'])/self.per['fall'])], 
                                    ["[90,100)", str(self.per['fall_[90,100)']) + '/ ' + str(self.per['fall']) + '=' + str(float(self.per['fall_[90,100)'])/self.per['fall'])], 
                                    ["[100+)", str(self.per['fall_[100+)']) + '/ ' + str(self.per['fall']) + '=' + str(float(self.per['fall_[100+)'])/self.per['fall'])]])
            else:
                writer.writerow('self.per[fall] == 0')
        f = open('/home/wencloud/txt/percent.csv', 'a')
        f.writelines("[ChaseUp]percent ok" + '\n')
        f.close()
                        
class Main:
    # 币种,跟btc一起涨起来的币的比例,算跌幅的时间段，之前跌得多的比例，当前涨起来的比例的范围
    def __init__(self, currency, percent, period, fall_percent, low_up_percent, high_up_percent):
        self.statistics = Statistics(currency)
        self.percent = percent
        self.period = period
        self.fall_percent = fall_percent
        self.low_up_percent = low_up_percent
        self.high_up_percent = high_up_percent
        
    def start(self):
        self.statistics.scan(self.percent, self.period, self.fall_percent, self.low_up_percent, self.high_up_percent)

if __name__ == "__main__":
    usdt = ["eosusdt","ethusdt","etcusdt","ltcusdt","bchusdt","xrpusdt","omgusdt","dashusdt","zecusdt","iotausdt","adausdt","steemusdt","wiccusdt",
            "socusdt","ctxcusdt","actusdt","btmusdt","btsusdt","ontusdt","iostusdt","htusdt","trxusdt","dtausdt","neousdt","qtumusdt",
            "smtusdt","elausdt","venusdt","thetausdt","sntusdt","zilusdt","xemusdt","nasusdt","ruffusdt","hsrusdt","letusdt","mdsusdt","storjusdt",
            "elfusdt","itcusdt","cvcusdt","gntusdt"]
    btc = ["bchbtc","ethbtc","ltcbtc","etcbtc","eosbtc","omgbtc","xrpbtc","dashbtc","zecbtc","iotabtc","adabtc","steembtc","polybtc","edubtc","kanbtc",
            "lbabtc","wanbtc","bftbtc","btmbtc","ontbtc","iostbtc","htbtc","trxbtc","smtbtc","elabtc","wiccbtc","ocnbtc","zlabtc","abtbtc","mtxbtc",
            "nasbtc","venbtc","dtabtc","neobtc","waxbtc","btsbtc","zilbtc","thetabtc","ctxcbtc","srnbtc","xembtc","icxbtc","dgdbtc","chatbtc","wprbtc",
            "lunbtc","swftcbtc","sntbtc","meetbtc","yeebtc","elfbtc","letbtc","qtumbtc","lskbtc","itcbtc","socbtc","qashbtc","mdsbtc","ekobtc","topcbtc",
            "mtnbtc","actbtc","hsrbtc","stkbtc","storjbtc","gnxbtc","dbcbtc","sncbtc","cmtbtc","tnbbtc","ruffbtc","qunbtc","zrxbtc","kncbtc","blzbtc",
            "propybtc","rpxbtc","appcbtc","aidocbtc","powr","cvcbtc","paybtc","qspbtc","datbtc","rdnbtc","mcobtc","rcnbtc","manabtc","utkbtc","tntbtc",
            "gasbtc","batbtc","ostbtc","linkbtc","gntbtc","mtlbtc","evxbtc","reqbtc","adxbtc","astbtc","engbtc","saltbtc","bifibtc","bcxbtc","bcdbtc",
            "sbtcbtc","btgbtc"]
    main = Main(usdt, 0.8, 3, 0.2, 0.02, 0.03) # usdt区跟btc一起涨起来的币大于0.8,之前3天跌幅超过0.2，目前涨幅在0.02和0.03之间
    main.start()
