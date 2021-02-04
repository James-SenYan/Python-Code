import pymongo
import pymysql
from datetime import datetime, timedelta

mongoClient = pymongo.MongoClient("mongodb://nocturne:123456@dds-bp13398b2d79e3642555-pub.mongodb.rds.aliyuncs.com:3717/nocturne")
mongodb = mongoClient.nocturne

mysql_conn = pymysql.connect(host='rr-bp1276ko13dklfko98o.mysql.rds.aliyuncs.com', port=3306, user='nocturne', password='nocturne123', database='nocturne-course')
cur = mysql_conn.cursor()

mysql_conn_dac = pymysql.connect(host='rr-bp1276ko13dklfko98o.mysql.rds.aliyuncs.com', port=3306,user='nocturne',
password='nocturne123', database='nocturne-dac')

cur_dac = mysql_conn_dac.cursor()

inner_user_ids = ['733338888797559017', '741669550222872576', '733338888797566867', '733338888797556737', '733338888797562955','733338888797557209', '782241660582105088', '765511981368217600', '781458304323948544']

def user_ids_from_designated_channel(start_date, end_date, channel):
	#设备和时间	    
    deviceids_createtime = list(mongodb.logs.find({"type": "app_install_channel", "device": channel, "create_time": {'$gte': start_date, '$lte': end_date}}, {"extra.deviceId": 1, "create_time":1}))
    device_createtime_map = {}
    for item in deviceids_createtime:
    	deviceid = item['extra']['deviceId']
    	if deviceid not in device_createtime_map.keys():
    		device_createtime_map[deviceid] = item['create_time']
    devices = list(device_createtime_map.keys())

    #用户和设备
    userids_deviceid = list(mongodb.logs.find({"extra.deviceId": {'$in':devices}, "create_time": {'$gte': start_date, '$lte': end_date}},{"username": 1, "extra.deviceId":1}))
    user_device_map = {}
    for item in userids_deviceid:
    	username = item['username']
    	if username != '' and username not in user_device_map.keys():
    		user_device_map[username] = item['extra']['deviceId']

    #用户和时间
    user_createtime_map = {}
    for name in user_device_map:
    	user_createtime_map[name] = device_createtime_map[user_device_map[name]]       
    result = {}
    for user in user_createtime_map:
        date = user_createtime_map[user].strftime('%Y-%m-%d')
        if date not in result.keys():
            result[date] = set()
        result[date].add(user)
    return result


def print_receive_free_course(user_ids, date):
    user_ids_str = str(user_ids)[1:-1] if len(user_ids) > 0 else "''"
 
    sql = """
    select user_id from trainee where course_id = '12' and user_id in (%s) and create_time between '%s' and '%s'
    """ % (user_ids_str, date, date + timedelta(days=1))# timedelta plus one day


    cur.execute(sql)
    elems = cur.fetchall()

    print("当日领取免费课数：", len(elems))


def print_total_sell_page_uv(user_ids):
    user_ids_str = str(user_ids)[1:-1] if len(user_ids) > 0 else "''"

    sql = '''
    select count(distinct uuid) from dac_log where uuid in (%s) and course_id = 1
    '''%user_ids_str
    cur_dac.execute(sql)
    elm = cur_dac.fetchone()
    print("购买页面总用户浏览量:", elm[0])


def print_total_purchase(user_ids):
    user_ids_str = str(user_ids)[1:-1] if len(user_ids) > 0 else "''"
 
    sql = """
    select t.schedule_class, count(1) from trainee t where t.course_id = '1' and t.user_id in (%s) group by t.schedule_class
    """ % (user_ids_str) 

    cur.execute(sql)
    elems = cur.fetchall()

    for ele in elems:
        print(ele[0], "购买数：", ele[1])


def count_designated_channel_free_convert_fee(start_date, end_date, channel):
    # fetch designated channel user's register time(group by register date)
    register_user = user_ids_from_designated_channel(start_date, end_date, channel)
    total_user_ids = []

    date = start_date
    while date < end_date:
        date_str = date.strftime('%Y-%m-%d')
        print("日期:", date_str)

        user_ids = register_user.get(date_str, set())
        total_user_ids += user_ids

        print("当日注册数：", len(user_ids))
        print_receive_free_course(user_ids, date)

        date += timedelta(days=1)

    print_total_sell_page_uv(total_user_ids)
    print_total_purchase(total_user_ids)


if __name__ == "__main__":
    # 用户注册的时间区间
    # 打印注册时间区间内注册的用户之后的领取免费课-浏览入门课售卖页-入门课购买的行为
    regiter_start_date = datetime(2021, 1, 22)
    registerend_date = datetime(2021, 1, 27)
    count_designated_channel_free_convert_fee(regiter_start_date, registerend_date, "nocturne_app_android/huawei_appstore")
