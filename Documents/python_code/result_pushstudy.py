import pymysql

from datetime import datetime, timedelta

mysql_conn_courses = pymysql.connect(host='rr-bp1276ko13dklfko98o.mysql.rds.aliyuncs.com', port=3306,user='nocturne',
password='nocturne123', database='nocturne-course')

cur_courses = mysql_conn_courses.cursor()

result = {}
def judge_finish_inday(user_id):
	sql = '''
	SELECT l.trainee_id, l.lesson_number, l.finish_learning_time, tc.lesson_open_date,
	case  when l.finish_learning_time < DATE_ADD(tc.lesson_open_date,interval 1 day) and 
	l.finish_learning_time > tc.lesson_open_date then 1
	else 0
	end inday
	from learning l inner join
	trainee_calendar tc on l.trainee_id = tc.trainee_id and l.lesson_number = tc.lesson_number
	where l.course_id = 1 and l.trainee_id in(
	select trainee_id from trainee where user_id = %s and course_id=1
	)
	;
	'''%(user_id)
	cur_courses.execute(sql)
	elms = cur_courses.fetchall()
	for elm in elms:
		if elm[1] not in result.keys():
			result[elm[1]] = set()
		if elm[4]:	
			result[elm[1]].add(elm[0])
	

def count_amount_trainees_finish_inday_bylesson_number(lesson_number):
 	users = [786359155916410880, 748826076616200192, 787413132800036864, 788380617548369920]
 	for user in users:
 		judge_finish_inday(user)	
 	print("lesson-number({}): {}".format(lesson_number, len(result.get(lesson_number))))


if __name__ == "__main__":
	for lesson_number in range(1,31):
		count_amount_trainees_finish_inday_bylesson_number(lesson_number)