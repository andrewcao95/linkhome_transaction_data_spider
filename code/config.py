# -*- encoding: utf-8 -*-

# APP配置信息
AUTHORIZATION_SIFFOX = '93273ef46a0b880faf4466c48f74878f'  # app密码
AUTHORIZATION_PREFIX = '20170324_android'  # app账号

# 城市ID
CITYID = 110000

# APP请求头
headers = {
    'Host': 'app.api.lianjia.com',
    'Authorization': '',
    'Page-Schema': 'chengjiaorent%2Flist',
    'Cookie': 'lianjia_udid=7d4f658511cb1ba7;lianjia_token=2.00422958c03c5329ce538471f1cac4358a;lianjia_ssid=35359c58-d108-4559-9085-a44170f57c17;lianjia_uuid=7cf19555-ff5d-484a-b1ea-ee5c9b21a24d',
    'User-Agent': 'HomeLink9.1.5;google Nexus+5; Android 6.0.1',
    'Lianjia-Channel': 'Android_seo_google_m',
    'Lianjia-Device-Id': '7d4f658511cb1ba7',
    'Lianjia-Access-Token': '2.00422958c03c5329ce538471f1cac4358a',
    'Lianjia-Version': '9.1.5',
    'Lianjia-City-Id': '',
    'Lianjia-Im-Version': '2.27.0'
}

#MONGODB数据库配置
MONGO_URL = 'localhost'
MONGO_DB = 'linkhome'
MONGO_COLLECTION_COMMUNITY_ID = 'community_id_name'
MONGO_COLLECTION_HOUSE_ID = 'house_id_name'
MONGO_COLLECTION_HOUSE_INFO_dp1 = 'houseInfo_dp1'
MONGO_COLLECTION_HOUSE_INFO_dp2 = 'houseInfo_dp2'
MONGO_COLLECTION_HOUSE_INFO_mo = 'houseInfo_mo'
