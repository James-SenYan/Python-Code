#encoding=utf8
import pymysql
import pymongo
from datetime import datetime, timedelta

mysql_conn_courses = pymysql.connect(host='rr-bp1276ko13dklfko98o.mysql.rds.aliyuncs.com', port=3306,user='nocturne',
password='nocturne123', database='nocturne-course')

cur_courses = mysql_conn_courses.cursor()

mongoClient = pymongo.MongoClient("mongodb://nocturne:123456@dds-bp13398b2d79e3642555-pub.mongodb.rds.aliyuncs.com:3717/nocturne")
mongodb = mongoClient.nocturne


def trainees_from(couser_id, schedule_no):
    # 返回user_id跟lesson的上课关系（trainee 跟 trainee_calendar表）
    # select user_id from trainee t, schedule s where t.schedule_id = s.schedule_id and t.course_id = '%s' and s.schedule_no = '%s'
    sql = '''
    SELECT t.user_id, tc.lesson_open_date, tc.lesson_number
    from trainee t inner join trainee_calendar tc
    on t.trainee_id = tc.trainee_id
    where t.course_id = 1 and t.schedule_id = 
    (select schedule.schedule_id
    from schedule 
    where course_id = %s and schedule_number = %s)
    '''%(course_id, schedule_no)
    cur_courses.execute(sql)
    elms = cur_courses.fetchall()
    result = {}
    for elm in elms:
        if elm[0] not in result.keys():
            result[elm[0]] = {}
        if elm[2] not in result[elm[0]].keys():
            result[elm[0]][elm[2]] = ''
        result[elm[0]][elm[2]] = elm[1]
    return result


def print_lesson_attend_for_each_lesson_group_by_ios_android(trainees):
    # logs = list(mongodb.logs.find({"username": {'$in': user_ids}, "type": "page_enter", "extra.page_id": 1}))
    # 针对返回里extra下面的lesson_id进行分组，按课时的维度打印ios跟android的学习数据
    # 根据时间升序排序，每个lesson只看用户的第一个数据即可
    # extra字段下的platform来区分ios跟android，分别对应"iOS"/"Android",如果没有platform字段说明是web端，可以忽略
    # 遍历logs，判断是否是当日学习

    # 打印结果形式： 第xx课: ios当日学习数:xx   ios总学习数:xx    android当日学习数:xx  android总学习数
    trainee_uids = list(trainees.keys())
    logs = list(mongodb.logs.find({"username": {'$in': trainee_uids}, "type": "page_enter", "extra.page_id": 1, "extra.platform": {'$in':['iOS', 'Android']}}))
    result = {}
    for log in logs:
        user_id = log['username']
        lesson = log['extra']['lesson_id']
        if lesson == None or lesson == 0:
            continue
        platform = log['extra']['platform']
        create_time = log['create_time']
        #open_time = datetime.combine(trainees[user_id][lesson], datetime.min.time())
        open_time_str = trainees[user_id][lesson].strftime('%Y-%m-%d')
        open_time = datetime.strptime(open_time_str, '%Y-%m-%d')
        if lesson not in result.keys():
            result[lesson] = {}
        if platform not in result[lesson].keys():
            result[lesson][platform] = {'learning_inday': set(), 'learning_total': set()}
        if create_time > open_time and create_time < open_time + timedelta(days = 1):
            result[lesson][platform]['learning_inday'].add(user_id)
        result[lesson][platform]['learning_total'].add(user_id)
    count = len(result.keys())
    for i in range(1, count+1):
        print("第{}课: ios当日学习数:{}   ios总学习数:{}    android当日学习数:{}  android总学习数{}".format(
             i, len(result[i]['iOS']['learning_inday']) if 'iOS' in result[i].keys() else 0,
             len(result[i]['iOS']['learning_total']) if 'iOS' in result[i].keys() else 0, 
             len(result[i]['Android']['learning_inday']) if 'Android' in result[i].keys() else 0, 
             len(result[i]['Android']['learning_total']) if 'Android' in result[i].keys() else 0))
    return result
def print_lesson_finish_for_each_lesson_group_by_ios_android(trainees):
    # logs = list(mongodb.logs.find({"username": {'$in': user_ids}, "type": "finish"}))
    # 针对返回里extra下面的lesson_id进行分组，按课时的维度打印ios跟android的完成
    # 根据时间升序排序，每个lesson只看用户的第一个数据即可
    # extra字段下的platform来区分ios跟android，分别对应"iOS"/"Android",如果没有platform字段说明是web端，可以忽略
    # 遍历logs，判断是否是当日完成

    # 打印结果形式： 第xx课: ios当日完成数:xx   ios总完成数:xx    android当日完成数:xx  android总完成数
    trainee_uids = list(trainees.keys())
    logs = list(mongodb.logs.find({"username": {'$in': trainee_uids}, "type": "finish", "extra.platform": {'$in':['iOS', 'Android']}}))
    result = {}
    for log in logs:
        user_id = log['username']
        lesson = log['extra']['lesson_id']
        if lesson == None or lesson == 0:
            continue
        platform = log['extra']['platform']
        create_time = log['create_time']
        open_time = datetime.combine(trainees[user_id][lesson], datetime.min.time())
        if lesson not in result.keys():
            result[lesson] = {}
        if platform not in result[lesson].keys():
            result[lesson][platform] = {'finish_inday': set(), 'finish_total': set()}
        if create_time > open_time and create_time < open_time + timedelta(days = 1):
            result[lesson][platform]['finish_inday'].add(user_id)
        result[lesson][platform]['finish_total'].add(user_id)
    count = len(result.keys())
    for i in range(1, count+1):
        print("第{}课: ios当日完成数:{}   ios总完成数:{}    android当日完成数:{}  android总完成数{}".format(
             i, len(result[i]['iOS']['finish_inday']) if 'iOS' in result[i].keys() else 0, 
             len(result[i]['iOS']['finish_total']) if 'iOS' in result[i].keys() else 0, 
             len(result[i]['Android']['finish_inday']) if 'Android' in result[i].keys() else 0
             , len(result[i]['Android']['finish_total']) if 'Android' in result[i].keys() else 0))
    return result

def print_study_data_group_by_ios_anddroid(couser_id, schedule_no):
    # 获取当期学员
    trainees = trainees_from(course_id, schedule_no)
    result_attend = print_lesson_attend_for_each_lesson_group_by_ios_android(trainees)
    print("***********************************************************")
    result_finish = print_lesson_finish_for_each_lesson_group_by_ios_android(trainees)
    print("***********************************************************")
    count = len(result_attend.keys())
    for i in range(1, count+1):
        denominator1 = len(result_attend[i]['iOS']['learning_inday']) if ('iOS' in result_finish[i].keys() and 'iOS' in result_attend[i].keys()) else 0
        val1 = len(result_finish[i]['iOS']['finish_inday'])/denominator1 if denominator1 != 0 else 0
        denominator2 = len(result_attend[i]['iOS']['learning_total']) if ('iOS' in result_finish[i].keys() and 'iOS' in result_attend[i].keys()) else 0
        val2 = len(result_finish[i]['iOS']['finish_total'])/denominator2 if denominator2 != 0 else 0
        denominator3 = len(result_attend[i]['Android']['learning_inday']) if ('Android' in result_finish[i].keys() and 'Android' in result_attend[i].keys()) else 0
        val3 = len(result_finish[i]['Android']['finish_inday'])/denominator3 if denominator3 != 0 else 0
        denominator4 = len(result_attend[i]['Android']['learning_total']) if ('Android' in result_finish[i].keys() and 'Android' in result_attend[i].keys()) else 0
        val4 = len(result_finish[i]['Android']['finish_total'])/denominator4 if denominator4 != 0 else 0
        print("第{}课: ios当日完登率:{:.3f}%   ios总完登率:{:.3f}%    android当日完登率:{:.3f}%  android总完登率{:.3f}%".format(
             i, val1*100, val2*100, val3*100, val4*100))


if __name__ == "__main__":
    # 指定courseId跟scheduleNo，分开打印ios跟android的学习数据
    course_id = 1
    schedule_no = 56
    print_study_data_group_by_ios_anddroid(course_id, schedule_no)
