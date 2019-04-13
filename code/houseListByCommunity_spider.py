# -*- encoding: utf-8 -*-
import random
import threading
import requests
from urllib.parse import urlencode
import hashlib
import base64
import pymongo
import json
import sys
from importlib import reload
import queue
import datetime

import time
from config import *

reload(sys)


client = pymongo.MongoClient(MONGO_URL)
db = client[MONGO_DB]

urlQueue = queue.Queue()

for communityId_str in db[MONGO_COLLECTION_COMMUNITY_ID].find():
    if len(communityId_str) < 2: continue
    for offset in range(0, 101):
        urlQueue.put([communityId_str, offset*20])
        # community_id = communityId_str['community_id']
        # getHouseListByCommunity(community_id, offset * 20)

houseInfoList = []

baseUrl = 'https://app.api.lianjia.com/house/rented/search?'

data = {
    'priceRequest': '',
    'sugQueryStr': '',
    'isFromMap': 'false',
    'condition': '',
    'limit_offset': 0,
    'communityRequset': '',
    'is_history': 0,
    'is_suggestion': 0,
    'moreRequest': '',
    'areaRequest': '',
    'roomRequest': '',
    'comunityIdRequest': '',
    'limit_count': 20,
    'city_id': CITYID
}

def saveToMongo(result):
    try:
        if db[MONGO_COLLECTION_HOUSE_ID].insert(result):
            print('存储到MongoDB成功')
    except Exception as e:
        print(e)
        print('存储到Mongo失败')


def dictSort(d):
    return {k: d[k] for k in sorted(d)}


def getAuthorizationCode(data):
    l = ""

    # 1 将URL中的参数取出，排序
    data_sort = dictSort(data)

    # 2 使用“=”连接key-value, 将所有元素连接成为一个字符串，并添加前缀AppSecret的内容
    l += AUTHORIZATION_SIFFOX
    l += ''.join([key + '=' + str(data_sort[key]) for key in data_sort.keys()])
    l_sha1 = hashlib.sha1(l.encode()).hexdigest()
    authorization_source = AUTHORIZATION_PREFIX + ':' + l_sha1
    authorization = base64.b64encode(authorization_source.encode())

    return authorization.decode()


def getHouseListByCommunity(community_id, offset):
    data['comunityIdRequest'] = 'c' + str(community_id)
    data['condition'] = 'c' + str(community_id)
    data['limit_offset'] = offset

    url = baseUrl + urlencode(data)
    authorizationCode = getAuthorizationCode(data)

    headers['Authorization'] = authorizationCode
    headers['Lianjia-City-Id'] = str(CITYID)

    try:
        response = requests.get(url, headers=headers)
        time.sleep(random.random() * 0.8)

        if response.status_code == 200:
            str_json = json.loads(response.text)
            str_list = str_json['data']['list']
            if str_list:
                for house in str_json['data']['list']:
                    house_id_name = {
                        'house_id': house['house_code'],
                        'house_name': house['title']
                    }

                    houseInfoList.append(house_id_name)

                    saveToMongo(house_id_name)
                    print("Now is writing house_id_name:", house_id_name)


    except requests.ConnectionError as e:
        print("getCommunityList Error", e.args)
    except KeyError as e:
        print("KeyError", e)


def getResult(urlQueue):
    while True:
        try:
            # 不阻塞的读取队列数据
            item = urlQueue.get_nowait()

            community_id_name = item[0]
            community_id = community_id_name['community_id']
            offset_true = item[1]

            getHouseListByCommunity(community_id, offset_true)

            i = urlQueue.qsize()

        except Exception as e:
            print(e)
        print('Current Thread Name %s, Url: %s ' % (threading.currentThread().name, offset))

def main():
    threadingNum = 10
    threads = []
    for i in range(0, threadingNum):
        t = threading.Thread(target=getResult, args=(urlQueue,))
        threads.append(t)
    for t in threads:
        t.start()
        time.sleep(random.random() * 1.3)

    for t in threads:
        # 多线程多join的情况下，依次执行各线程的join方法, 这样可以确保主线程最后退出， 且各个线程间没有阻塞
        t.join()


if __name__ == "__main__":
    stime = datetime.datetime.now()
    print(stime)
    main()
    etime = datetime.datetime.now()
    print(etime)
    print(etime - stime)

