import pymysql
import pymongo
from datetime import datetime

mongoClient = pymongo.MongoClient(
    "mongodb://nocturne:123456@dds-bp13398b2d79e3642555-pub.mongodb.rds.aliyuncs.com:3717/nocturne")
mongodb = mongoClient.nocturne

mysql_conn_courses = pymysql.connect(host='rr-bp1276ko13dklfko98o.mysql.rds.aliyuncs.com', port=3306, user='nocturne',
                                     password='nocturne123', database='nocturne-course')
cur_courses = mysql_conn_courses.cursor()


def users_click_sellpage_without_buying12(begin_date, end_date):
	users = []
	begin_date_str = begin_date.strftime('%Y-%m-%d')
	end_date_str = end_date.strftime('%Y-%m-%d')
	sql = '''
	SELECT user_id, create_time
	from trainee
	where course_id = 12 and create_time between '%s' and '%s';
	'''%(begin_date_str, end_date_str)
	cur_courses.execute(sql)
	elms = cur_courses.fetchall()
	for elm in elms:
		users.append(elm[0])

	logs_sell_distinct = list(mongodb.logs.distinct("username", {"type": {'$in': ["mobile_sell_free_camp",'sell_free_camp']}, "username": {'$in': users}}))
	users_click_sell_distinct = []
	for log in logs_sell_distinct:
		users_click_sell_distinct.append(log)
	#uv
	print('uv: {}'.format(len(users_click_sell_distinct)))

	user_ids_str = str(users_click_sell_distinct)[1:-1] if len(users_click_sell_distinct) > 0 else "''"
	
	sql2 = '''
	SELECT count(distinct t.user_id)as num
	from trainee t
	where t.user_id in (%s) and t.course_id <>12
	'''%user_ids_str
	cur_courses.execute(sql2)
	nums = cur_courses.fetchone()
	#总购买人数
	print("购买总人数: {}".format(nums[0]))
	#按照课程分组，每门课的购买人数
	sql3 = '''
	SELECT course_id, count(*) as nums
	from trainee
	where user_id in (%s) and course_id <>12
	group by course_id
	'''%user_ids_str
	cur_courses.execute(sql3)
	elms = cur_courses.fetchall()
	for elm in elms:
		print("course_id:{}    人数:{}".format(elm[0],elm[1]))


if __name__ == "__main__":
	begin_date = datetime(2020, 12, 1)
	end_date = datetime(2021, 1, 13)
	users_click_sellpage_without_buying12(begin_date, end_date)