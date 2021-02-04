import pymongo
import pymysql
from datetime import datetime, timedelta

mongoClient = pymongo.MongoClient("mongodb://nocturne:123456@dds-bp13398b2d79e3642555-pub.mongodb.rds.aliyuncs.com:3717/nocturne")
mongodb = mongoClient.nocturne

mysql_conn = pymysql.connect(host='rr-bp1276ko13dklfko98o.mysql.rds.aliyuncs.com', port=3306, user='nocturne', password='nocturne123', database='nocturne-course')
cur = mysql_conn.cursor()


inner_user_ids = ['733338888797559017', '741669550222872576', '733338888797566867', '733338888797556737', '733338888797562955','733338888797557209', '782241660582105088', '765511981368217600', '781458304323948544']

def user_ids_from_designated_channel(start_date, end_date, channel):
	#设备和时间	    
    deviceids_createtime = list(mongodb.logs.find({"type": "app_install_channel", "extra.installChannel": channel, "create_time": {'$gte': start_date, '$lte': end_date}}, {"extra.deviceId": 1, "create_time":1}))
    device_createtime_map = {}
    for item in deviceids_createtime:
    	deviceid = item['extra']['deviceId']
    	if deviceid not in device_createtime_map.keys():
    		device_createtime_map[deviceid] = item['create_time']
    devices = list(device_createtime_map.keys())
    #用户和设备
    userids_deviceid = list(mongodb.logs.find({ "type": "app_install_channel", "extra.installChannel": channel,"extra.deviceId": {'$in':devices}, "create_time": {'$gte': start_date, '$lte': end_date}},{"username": 1, "extra.deviceId":1}))
    user_device_map = {}
    for item in userids_deviceid:
    	username = item['username']
    	if username not in user_device_map.keys():
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


def print_purchase_in(user_ids, date, days):#指定天数内购买的人数和总营收
    user_ids_str = str(user_ids)[1:-1] if len(user_ids) > 0 else "''"
 
    sql = """
    select o.user_id, p.course_id from `order` o, sku s, course_product p where o.sku_id = s.id and s.product_id = p.id and p.course_id != '5' and o.create_time BETWEEN '%s' AND '%s' and o.user_id in (%s)
    """ % (date, date + timedelta(days=days), user_ids_str) 

    cur.execute(sql)
    elems = cur.fetchall()

    print(days, "日内购买数:", len(elems))
    
    total = 0
    for ele in elems:# ele:(user_id,course_id)
        course_id = ele[1]
        if course_id == '1':
            total += 398
        else:
            total += 498
    
    print(days, "日营收:", total)
 

def print_convert_date(user_ids, date):
    print_receive_free_course(user_ids, date)
    print_purchase_in(user_ids, date, 7)
    print_purchase_in(user_ids, date, 23)


def count_designated_channel_free_convert_fee(start_date, end_date, channel):
    # fetch designated channel user's register time(group by register date)
    register_user = user_ids_from_designated_channel(start_date, end_date, channel)
    date = start_date
    while date < end_date:
        date_str = date.strftime('%Y-%m-%d')
        print("日期:", date_str)

        user_ids = register_user.get(date_str, set())
        print("当日注册数：", len(user_ids))
        print_convert_date(user_ids, date)

        date += timedelta(days=1)


if __name__ == "__main__":
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2021, 1, 12)
    count_designated_channel_free_convert_fee(start_date, end_date, "sxs002")
