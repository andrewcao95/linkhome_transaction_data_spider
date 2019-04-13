# -*- encoding: utf-8 -*-

import datetime
import threading
import requests
from urllib.parse import urlencode
import hashlib
import base64
import json
import sys
import pymongo
from importlib import reload
import random
import time
import queue
from config import *

reload(sys)

client = pymongo.MongoClient(MONGO_URL)
db = client[MONGO_DB]

urlQueue = queue.Queue()
district_id = [23008614, 23008626, 23008613, 23008618, 23008617, 23008623, 23008625, 23008611, 23008615, 23008629,
               23008624, 23008616, 23008620, 23008622, 23008619, 23008621, 23008628]

for districtId in district_id:
    for offset in range(0, 101):
        urlQueue.put([districtId, offset * 20])

communityInfoList = []

baseUrl = 'https://app.api.lianjia.com/house/community/search?'

data = {
    'limit_offset': 0,
    'district_id': 23008614,
    'group_type': 'bizcircle',
    'limit_count': 20,
    'city_id': 110000
}


def saveToMongo(result):
    try:
        if db[MONGO_COLLECTION_COMMUNITY_ID].insert(result):
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


def getCommunityList(districtId, offset):
    data['limit_offset'] = offset
    data['district_id'] = districtId

    url = baseUrl + urlencode(data)
    authorizationCode = getAuthorizationCode(data)

    headers['Authorization'] = authorizationCode
    headers['Lianjia-City-Id'] = str(CITYID)

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            str_json = json.loads(response.text)
            str_list = str_json['data']['list']
            if str_list:
                for community in str_json['data']['list']:
                    community_id_name = {
                        'community_id': community['community_id'],
                        'community_name': community['community_name']
                    }

                    communityInfoList.append(community_id_name)

                    saveToMongo(community_id_name)
                    print("Now is writing community_id_name:", community_id_name)

    except requests.ConnectionError as e:
        print("getCommunityList Error", e.args)
    except KeyError as e:
        print("KeyError", e)


def getResult(urlQueue):
    while True:
        try:
            # 不阻塞的读取队列数据
            item = urlQueue.get_nowait()

            districtId = item[0]
            offsetId = item[1]

            getCommunityList(districtId, offsetId)
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
