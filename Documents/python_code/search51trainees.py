import pymysql	
import re
import os

mysql_conn_courses = pymysql.connect(host='rr-bp1276ko13dklfko98o.mysql.rds.aliyuncs.com', port=3306,user='nocturne',
password='nocturne123', database='nocturne-course')

cur_courses = mysql_conn_courses.cursor()
def find_uid51():#return uid list of schedule51
	global group
	global groupA
	global groupB
	global groupC
	group = []
	groupA = []
	groupC = []
	file_path = "51.txt"
	with open(file_path, encoding = "utf8") as file:
		lines = file.readlines()
		for line in lines:
			if(re.findall(r'\d{17}(?=.+)', line)):
				for i in range(len(line)):
					if(line[i] in ['0','1','2','3','4','5','6','7','8','9']):
						group.append(line[i:].strip("\n "))
						break
	for i in range(len(group)):
		if group[i] == '786662911929356288':
			index = i
			break
		else:
			groupA.append(group[i])
	
	groupB = group[index:]

	sql = '''
	select t.user_id
	from schedule sche inner join
	trainee t on t.schedule_id = sche.schedule_id
	where sche.schedule_number = 51
	'''
	cur_courses.execute(sql)
	elms = cur_courses.fetchall()
	for elm in elms:
		if elm[0] not in group:
			groupC.append(elm[0])

find_uid51()

print(groupA)
print(groupB)
print(groupC)