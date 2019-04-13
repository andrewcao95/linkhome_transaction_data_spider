# -*- encoding: utf-8 -*-

import random
import threading
import datetime
import requests
from urllib.parse import urlencode
import hashlib
import base64
import json
import sys
from importlib import reload
import pymongo
import queue
import time
from config import *

reload(sys)

client = pymongo.MongoClient(MONGO_URL)
db = client[MONGO_DB]

houseIdList = []
urlQueue = queue.Queue()


for houseId_str in db[MONGO_COLLECTION_HOUSE_ID].find():
    if len(houseId_str) < 2: continue
    houseIdList.append(houseId_str)

for houseId in houseIdList[684870:]:
    urlQueue.put([houseId])


def saveToMongo(result, dbName):
    try:
        if db[dbName].insert(result):
            print('存储到MongoDB成功')
    except Exception:
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


def getHouseInfo(flag_code, house_code, part_code):
    if house_code and flag_code:
        if part_code == 'dp1':
            baseUrl = 'https://app.api.lianjia.com/house/zufang/detailpart1?'
            dbName = MONGO_COLLECTION_HOUSE_INFO_dp1
        if part_code == 'dp2':
            baseUrl = 'https://app.api.lianjia.com/house/zufang/detailpart2?'
            dbName = MONGO_COLLECTION_HOUSE_INFO_dp2
        if part_code == 'mo':
            baseUrl = 'https://app.api.lianjia.com/house/zufang/moreInfo?'
            dbName = MONGO_COLLECTION_HOUSE_INFO_mo

        data = {'house_code': house_code}
        authorizationStr = getAuthorizationCode(data)
        url = baseUrl + urlencode(data)
        headers['Authorization'] = authorizationStr

        try:
            response = requests.get(url, headers=headers)
            time.sleep(random.random() * 1.5)

            if (response.status_code == 200):
                jsonOrigin = json.loads(response.text)


                needHouseInfo = {
                    'flag_code': str(flag_code),
                    'part_code': part_code,
                    'jsonOrigin': jsonOrigin
                }

                saveToMongo(needHouseInfo, dbName)
                print("Now is writing needHouseInfo:", needHouseInfo)

        except requests.ConnectionError as e:
            print("getDetailPage Error", e.args)
        except KeyError as e:
            print("KeyError", e.args)


def getResult(urlQueue):
    while True:
        try:
            # 不阻塞的读取队列数据
            item = urlQueue.get_nowait()

            flagId = item[0]['_id']
            houseId = item[0]['house_id']

            getHouseInfo(flagId, houseId, 'dp1')
            time.sleep(random.random() * 1.5)
            getHouseInfo(flagId, houseId, 'dp2')
            time.sleep(random.random() * 1.5)
            getHouseInfo(flagId, houseId, 'mo')
            time.sleep(random.random() * 1.5)

            i = urlQueue.qsize()

        except Exception as e:
            print(e)


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
