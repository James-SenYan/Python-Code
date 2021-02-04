#encoding: utf-8


import pymysql
import pymongo
from datetime import datetime, timedelta

mongoClient = pymongo.MongoClient(
    "mongodb://nocturne:123456@dds-bp13398b2d79e3642555-pub.mongodb.rds.aliyuncs.com:3717/nocturne")
mongodb = mongoClient.nocturne

mysql_conn_courses = pymysql.connect(host='rr-bp1276ko13dklfko98o.mysql.rds.aliyuncs.com', port=3306, user='nocturne',
                                     password='nocturne123', database='nocturne-course')
cur_courses = mysql_conn_courses.cursor()


def find_designated_course_trainees(course_id, schedule_number):
    sql = '''
    SELECT user_id, trainee.course_id, schedule_number
    FROM trainee
    INNER JOIN SCHEDULE ON trainee.`schedule_id`= schedule.`schedule_id`
    AND trainee.`course_id` = schedule.`course_id`
    WHERE trainee.course_id = %s AND schedule_number = %s AND user_id NOT in (
    select distinct user_id from activation_chance where create_time between '2020-11-08' and '2020-11-14'
    )
    ''' % (course_id, schedule_number)
    cur_courses.execute(sql)
    elms = cur_courses.fetchall()
    result = {}
    for elm in elms:
        course_id = elm[1]
        schedule_number = elm[2]
        tag = (course_id, schedule_number)
        if tag not in result.keys():
            result[tag] = []
        result[tag].append(elm[0])

    users = result.get((course_id, schedule_number))
    return users


def purchase_behavior_info(start_date, end_date, course_id, schedule_number):
    users_wanted = find_designated_course_trainees(course_id, schedule_number)
    date = start_date
    users_viewd_already = set()
    while date < end_date:
        date_str = date.strftime('%Y-%m-%d')
        print("日期:", date_str)
        date_plus_one_day = date + timedelta(days=1)
        logs_sell_package = list(mongodb.logs.find({"type": "sell-package", "username": {'$in': users_wanted},
                                                    "create_time": {'$gte': date, '$lte': date_plus_one_day}}))

        logs_sell_package_distinct = list(
            mongodb.logs.distinct("username", {"type": "sell-package", "username": {'$in': users_wanted},
                                               "create_time": {'$gte': date, '$lte': date_plus_one_day}}))

        logs_sell_package_pay_click = list(
            mongodb.logs.find({"type": "sell-package_pay_click", "username": {'$in': users_wanted},
                               "create_time": {'$gte': date, '$lte': date_plus_one_day}}))

        logs_sell_package_pay_click_distinct = list(
            mongodb.logs.distinct("username", {"type": "sell-package_pay_click", "username": {'$in': users_wanted},
                                               "create_time": {'$gte': date, '$lte': date_plus_one_day}}))

        logs_sell_package_course_click = list(mongodb.logs.find({"type": "sell-package_course_click", "username": {'$in': users_wanted},
                               "create_time": {'$gte': date, '$lte': date_plus_one_day}}))

        # 进入售卖页uv userview
        result_sell_package_users = [log for log in logs_sell_package_distinct]
        result_sell_package_uv = len(result_sell_package_users)
        print("进入售卖页uv: " + str(result_sell_package_uv))
        # 进入售卖页面uv增量
        increment_uv = 0
        for user in result_sell_package_users:
            if user not in users_viewd_already:
                increment_uv += 1
        print("进入售卖页uv增量: " + str(increment_uv))
        for user in result_sell_package_users:
            if user not in users_viewd_already:
                users_viewd_already.add(user)
        # 进入售卖页pv pageview
        result_sell_package_pv = len([log['username'] for log in logs_sell_package])
        print("进入售卖页pv: " + str(result_sell_package_pv))
        # 点击立即购买uv
        result_sell_package_pay_click_uv = len([log for log in logs_sell_package_pay_click_distinct])
        print("点击立即购买uv: " + str(result_sell_package_pay_click_uv))
        # 点击立即购买pv
        result_sell_package_pay_click_pv = len([log['username'] for log in logs_sell_package_pay_click])
        print("点击立即购买pv: " + str(result_sell_package_pay_click_pv))
        #点击某个课程的pv
        result={}
        for log in logs_sell_package_course_click:
            courseId = log['extra']['courseId']
            if courseId not in result.keys():
                result[courseId] = []
            if log['username'] != None:
                result[courseId].append(log['username'])
        #点击python办公效率化
        print("点击python办公效率化pv: "+str(len(result.get("2",''))))
        #点击python数据分析
        print("点击python数据分析pv: "+str(len(result.get("3",''))))
        #点击python数据可视化
        print("点击python数据可视化pv: "+str(len(result.get("6",''))))
        #点击python网络爬虫
        print("点击python网络爬虫pv: "+str(len(result.get("4",''))))
        #点击某个课程的uv
        result={}
        for log in logs_sell_package_course_click:
            courseId = log['extra']['courseId']
            if courseId not in result.keys():
                result[courseId] = set()
            if log['username'] != None:
                result[courseId].add(log['username'])
        #点击python办公效率化
        print("点击python办公效率化uv: "+str(len(result.get("2",''))))
        #点击python数据分析
        print("点击python数据分析uv: "+str(len(result.get("3",''))))
        #点击python数据可视化
        print("点击python数据可视化uv: "+str(len(result.get("6",''))))
        #点击python网络爬虫
        print("点击python网络爬虫uv: "+str(len(result.get("4",''))))
        print("***********")
        date += timedelta(days=1)


if __name__ == "__main__":
    start_date = datetime(2020, 12, 17)
    end_date = datetime(2021, 1, 3)
    course_id = 1
    schedule_number = 48
    purchase_behavior_info(start_date, end_date, str(course_id), schedule_number)
