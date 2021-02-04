import pymysql
import json
from datetime import datetime, timedelta

mysql_conn_courses = pymysql.connect(host='rr-bp1276ko13dklfko98o.mysql.rds.aliyuncs.com', port=3306,user='nocturne',
password='nocturne123', database='nocturne-course')

mysql_conn_dac = pymysql.connect(host='rr-bp1276ko13dklfko98o.mysql.rds.aliyuncs.com', port=3306,user='nocturne',
password='nocturne123', database='nocturne-dac')

cur_courses = mysql_conn_courses.cursor()

cur_dac = mysql_conn_dac.cursor()

def daclog(courseid, schedule_number):
	sql = '''
	SELECT log.uuid as userid, log.os_type as channel, log.extra as lessonNo, log.client_time, log.page
	from dac_log log
	where log.event_type = 1 and course_id = %s and schedule_no = %s and (page = 'lessonContent' or page = 'lessonPunch');
	'''%(courseid, schedule_number)
	cur_dac.execute(sql)
	elms = cur_dac.fetchall()
	daclog = {}
	for elm in elms:
		userid = elm[0]
		os_type = elm[1]
		if elm[2] != None:
			dic = json.loads(elm[2])
			lesson_no = dic["lessonNo"] if "lessonNo" in dic.keys() else -1
		else:
			lesson_no = -1
		time = elm[3]
		status = elm[4]
		if lesson_no not in daclog.keys():
			daclog[lesson_no] = {}
		if os_type not in daclog[lesson_no].keys():
			daclog[lesson_no][os_type] = []
		daclog[lesson_no][os_type].append([userid, time, status])
	return daclog

def find_designated_opentime(courseid, schedule_no, lesson_no):
	#gain scheduleid from table schedule
	sql1 = '''
	SELECT sku.course_id, sku.schedule_number, sku.schedule_id, sku.schedule_arrange_days
	from `schedule` as sku
	where course_id = %s and schedule_number = %s
	'''%(courseid, schedule_no)
	cur_courses.execute(sql1)
	elm = cur_courses.fetchone()
	
	scheduleid = elm[2]
	arrange_days = elm[3]
	#gain traineeid via scheduleid and userid(table trainee)学员课程信息
	sql2 = '''
	SELECT t.trainee_id, t.schedule_id, t.user_id, tc.lesson_open_date, tc.lesson_number
	from trainee t
	inner join trainee_calendar tc on t.trainee_id = tc.trainee_id 
	where t.schedule_id = "%s" and tc.lesson_number = %s;
	'''%(scheduleid, lesson_no)
	cur_courses.execute(sql2)
	elms = cur_courses.fetchall()
	
	return [elms[0][3], arrange_days]

	



if __name__ == "__main__":
	courseid = 1
	schedule_number = 53
	daclogs = daclog(courseid, schedule_number)
	for lesson_number in range(1, 31):
		arrange_days = find_designated_opentime(courseid, schedule_number, 1)[1]
		if lesson_number > arrange_days:
			break;
		for os_type in ['iOS', 'Android']:
			opentime = datetime.strptime(str(find_designated_opentime(courseid, schedule_number, lesson_number)[0]), '%Y-%m-%d')
			items = daclogs[lesson_number][os_type]
			users_finished = 0
			users_logging  = 0
			users_finished_inday = 0
			users_logging_inday = 0
			for item in items:
				if item[2] == 'lessonContent':
					users_logging += 1
					finish_time = datetime.strptime(item[1], '%Y-%m-%d %H:%M:%S')
					if opentime < finish_time and finish_time < opentime + timedelta(days = 1):
						users_logging_inday += 1
			items = daclogs[-1][os_type]
			for item in items:			
				if item[2] == 'lessonPunch':
					users_finished += 1
					finish_time = datetime.strptime(item[1], '%Y-%m-%d %H:%M:%S')
					if opentime < finish_time and finish_time < opentime + timedelta(days = 1):
						users_finished_inday += 1
			print("the following are statistics of course {}, schedule_no {}, lessonNo {} on channel->{}".format(courseid,schedule_number,lesson_number,os_type))
			print("当日登陆总人数:{}  当日登陆率:{:.3f}%	 当日完成总人数:{}	  当日完成率:{:.3f}%".format(users_logging_inday,users_logging_inday/users_logging*100, users_finished_inday, users_finished_inday/users_finished*100))	
			print("*********************************************************************************")
