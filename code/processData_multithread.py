# -*- encoding: utf-8 -*-

import sys
import threading
from importlib import reload
import pymongo
import re
from config import *
import xlsxwriter
import json
import queue

reload(sys)
import ast

RESULT_FILE_PATH = './data/resultInfo.xlsx'

client = pymongo.MongoClient(MONGO_URL)
db = client[MONGO_DB]
urlQueue = queue.Queue()

count = 0
flagCodeArray = []
rawDataList = []
washedDataList = []


def loadDatabase():
    with open('./data/flag_code.json') as file_flagCode:
        flagCodes = file_flagCode.readlines()
        for line_flagCode in flagCodes:
            line_flagCode_json = json.loads(line_flagCode)
            line_flagCode_dict = {'flag_code': line_flagCode_json['flag_code']}
            flagCodeArray.append(line_flagCode_dict)

    for flagCode in flagCodeArray[0:1000]:
        urlQueue.put([flagCode])


def extractDataFromDB(result):
    try:
        global count
        count += 1
        print('[' + str(count) + ']')

        dp1_result = {}
        dp2_result = {}
        mo_result = {}

        for dp1_result_instance in db[MONGO_COLLECTION_HOUSE_INFO_dp1].find(result).limit(1):
            dp1_result = dp1_result_instance

        for dp2_result_instance in db[MONGO_COLLECTION_HOUSE_INFO_dp2].find(result).limit(1):
            dp2_result = dp2_result_instance

        for mo_result_instance in db[MONGO_COLLECTION_HOUSE_INFO_mo].find(result).limit(1):
            mo_result = mo_result_instance

        if dp1_result['flag_code'] == dp2_result['flag_code'] == mo_result['flag_code']:
            needData = {
                'rent_price': dp1_result['jsonOrigin']['data']['basic_info']['price'],
                'room_num': dp1_result['jsonOrigin']['data']['basic_info']['blueprint_bedroom_num'],
                'hall_num': dp1_result['jsonOrigin']['data']['basic_info']['blueprint_hall_num'],
                'wc_num':
                    [item['value'] for item in dp1_result['jsonOrigin']['data']['basic_list'] if
                     item['name'] == '房型'][
                        0].strip()[-2:-1],
                'area': dp1_result['jsonOrigin']['data']['basic_info']['area'],
                'rent_type':
                    [item['value'] for item in dp1_result['jsonOrigin']['data']['info_list'] if
                     item['name'] == '方式：'][
                        0].strip(),
                'deal_year':
                    [item['value'] for item in dp1_result['jsonOrigin']['data']['info_list'] if
                     item['name'] == '成交：'][
                        0].split('.')[0].strip(),
                'deal_month':
                    [item['value'] for item in dp1_result['jsonOrigin']['data']['info_list'] if
                     item['name'] == '成交：'][
                        0].split('.')[1].strip(),
                'deal_day':
                    [item['value'] for item in dp1_result['jsonOrigin']['data']['info_list'] if
                     item['name'] == '成交：'][
                        0].split('.')[
                        2].strip(),
                'orientation': dp1_result['jsonOrigin']['data']['basic_info']['orientation'].strip(),
                'floor_type': dp1_result['jsonOrigin']['data']['basic_info']['floor_state'].split('/')[0],
                'floor': re.sub("\D", "",
                                dp1_result['jsonOrigin']['data']['basic_info']['floor_state'].split('/')[1]),
                'lift':
                    [item['value'] for item in dp1_result['jsonOrigin']['data']['info_list'] if
                     item['name'] == '电梯：'][
                        0].strip(),
                'heat_type':
                    [item['value'] for item in dp1_result['jsonOrigin']['data']['info_list'] if
                     item['name'] == '供暖：'][
                        0].strip(),
                'year':
                    re.sub('\D', '', [item['value'] for item in dp1_result['jsonOrigin']['data']['info_list'] if
                                      item['name'] == '年代：'][0].strip()),

                'car':
                    [item['value'] for item in dp1_result['jsonOrigin']['data']['info_list'] if
                     item['name'] == '车位：'][
                        0].strip(),
                'decoration':
                    [item['value'] for item in dp1_result['jsonOrigin']['data']['info_list'] if
                     item['name'] == '装修：'][
                        0].strip(),
                'duration':
                    [item['value'] for item in dp1_result['jsonOrigin']['data']['info_list'] if
                     item['name'] == '租期：'][
                        0].strip(),
                'community_name': dp1_result['jsonOrigin']['data']['info_jump_list'][0]['value'],
                'inner_area': re.sub("[^-?\d+\.?\d*e?-?\d*?]", "",
                                     [item['value'] for item in mo_result['jsonOrigin']['data']['list'][0]['list']
                                      if
                                      item['name'] == '套内面积：'][0].strip()),
                'hid': [item['value'] for item in mo_result['jsonOrigin']['data']['list'][0]['list'] if
                        item['name'] == '链家编号：'][0].strip(),
                'category': [item['value'] for item in mo_result['jsonOrigin']['data']['list'][1]['list'] if
                             item['name'] == '建筑类型：'][0].strip(),
                'first_price':
                    [item['value'] for item in dp1_result['jsonOrigin']['data']['deal_info']['review']['list'] if
                     item['name'] == '挂牌价格（元/月）'][0],
                'deal_duration':
                    [item['value'] for item in dp1_result['jsonOrigin']['data']['deal_info']['review']['list'] if
                     item['name'] == '成交周期（天）'][0],
                'price_adjustment_times':
                    [item['value'] for item in dp1_result['jsonOrigin']['data']['deal_info']['review']['list'] if
                     item['name'] == '调价（次）'][0],
                'introduce_times':
                    [item['value'] for item in dp1_result['jsonOrigin']['data']['deal_info']['review']['list'] if
                     item['name'] == '带看（次）'][0],
                'save_pop':
                    [item['value'] for item in dp1_result['jsonOrigin']['data']['deal_info']['review']['list'] if
                     item['name'] == '关注（人）'][0],
                'search_times':
                    [item['value'] for item in dp1_result['jsonOrigin']['data']['deal_info']['review']['list'] if
                     item['name'] == '浏览（次）'][0]
            }

            rawDataList.append(needData)

    except Exception as e:
        print("Error", e)
    except KeyError as e:
        print("KeyError", e)


def saveData(rawDataList):
    try:
        with open('./data/resultList.dat', 'a', encoding='utf-8') as file:
            for result_line in rawDataList:
                print("正在写入" + str(result_line))
                file.write(str(result_line) + "\n")
    except IOError as e:
        print(e)


def generate_excel(rec_datas):
    try:
        workbook = xlsxwriter.Workbook(RESULT_FILE_PATH)
        worksheet = workbook.add_worksheet()
        worksheet.write('A1', 'rent_price')
        worksheet.write('B1', 'room_num')
        worksheet.write('C1', 'hall_num')
        worksheet.write('D1', 'wc_num')
        worksheet.write('E1', 'area')
        worksheet.write('F1', 'rent_type')
        worksheet.write('G1', 'deal_year')
        worksheet.write('H1', 'deal_month')
        worksheet.write('I1', 'deal_day')
        worksheet.write('J1', 'orientation')
        worksheet.write('K1', 'floor_type')
        worksheet.write('L1', 'floor')
        worksheet.write('M1', 'lift')
        worksheet.write('N1', 'heat_type')
        worksheet.write('O1', 'year')
        worksheet.write('P1', 'car')
        worksheet.write('Q1', 'decoration')
        worksheet.write('R1', 'duration')
        worksheet.write('S1', 'community_name')
        worksheet.write('T1', 'inner_area')
        worksheet.write('U1', 'hid')
        worksheet.write('V1', 'category')
        worksheet.write('W1', 'first_price')
        worksheet.write('X1', 'deal_duration')
        worksheet.write('Y1', 'price_adjustment_times')
        worksheet.write('Z1', 'introduce_times')
        worksheet.write('AA1', 'save_pop')
        worksheet.write('AB1', 'search_times')

        row = 1
        col = 0

        linecount = 0

        for rec_data in rec_datas:
            rec_data_dict = ast.literal_eval(rec_data)
            linecount += 1
            print(linecount)

            for key in rec_data_dict:
                worksheet.write_string(row, col, str(rec_data_dict[key]))
                col += 1
            col = 0
            row += 1

        workbook.close()

    except TypeError as e:
        print('TypeError', e)


def getResult(urlQueue, ):
    while True:
        try:
            # 不阻塞的读取队列数据
            item = urlQueue.get_nowait()

            result = item[0]
            extractDataFromDB(result)
            i = urlQueue.qsize()

        except Exception as e:
            print(e)
            return


def main():
    threadingNum = 1000
    threads = []
    for i in range(0, threadingNum):
        t = threading.Thread(target=getResult, args=(urlQueue,))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    loadDatabase()
    main()
    saveData(rawDataList)
