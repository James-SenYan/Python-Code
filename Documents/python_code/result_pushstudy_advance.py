import pymysql	
import re
import os

mysql_conn_courses = pymysql.connect(host='rr-bp1276ko13dklfko98o.mysql.rds.aliyuncs.com', port=3306,user='nocturne',
password='nocturne123', database='nocturne-course')

cur_courses = mysql_conn_courses.cursor()
	

def count_amount_trainees_finish_inday_bylesson_number(user_ids):
	user_ids_str = str(user_ids)[1:-1] if len(user_ids) > 0 else "''"
	sql = '''
	select temp.ln, count(temp.inday)
	from (
	select l.trainee_id, l.lesson_number ln, l.finish_learning_time, tc.lesson_open_date,
	case  when l.finish_learning_time < DATE_ADD(tc.lesson_open_date,interval 1 day) and 
	l.finish_learning_time > tc.lesson_open_date then 1
	else NULL
	end inday
	from learning l inner join
	trainee_calendar tc on l.trainee_id = tc.trainee_id and l.lesson_number = tc.lesson_number
	where l.course_id = 1 and l.trainee_id in(
	select trainee_id from trainee where user_id in (%s) and course_id=1) 
	) temp
	group by temp.ln;
	'''%user_ids_str
 	
	cur_courses.execute(sql)
	elms = cur_courses.fetchall()
	for elm in elms:
		print("lesson-number({}): {}".format(elm[0], elm[1]))
	





if __name__ == "__main__":
	groupA = ['786359155916410880', '748826076616200192', '787413132800036864', '788380617548369920', '787993380612608000', '785115002242994176', '787967280549335040', '786611751268782080', '788367133502738432', '788320469844889600', '788020696608804864', '787202243132002304', '756202206037938176', '787660699764920320', '775686906083151872', '785086240205967360', '781560093387919360', '786256531795873792', '783283800665886720', '755755389773025280', '787827191223554048', '788217142683504640', '772072294943166464', '751098682069356544', '786258413352259584', '788433780980715520', '788412042473508864', '788355044243542016', '787307664152858624', '784167434075901952', '783474353802186752', '787808887817768960', '786145086605234176', '788422203963478016', '787621888615911424', '787501954602766336', '771096767826628608', '787813151009017856', '789513971928797184', '787474430736863232', '783643260257177600', '787135559591399424', '779342024523517952', '754421759914151936', '783496334350094336', '768558393010032640', '788103668754157568', '785991533127405568', '787686858649112576', '787641428850642944', '765315823630946304', '786186248028295168', '786694682754551808', '761683011191312384', '787851297386008576', '786224394296233984', '769879498421309440', '786239857420013568', '786316807308185600', '786080781998362624', '763501359218692096', '785989674396422144', '789427303813091328', '786566529683951616', '787635767299411968', '787291005442723840', '788137689626316800', '786953921087541248', '742276673910738944', '786239353767989248', '742273485149179904', '786002536225443840', '773606111705174016', '788542313592131584', '786167281746317312', '788255292281720832', '778684639584849920', '788330795013378048', '787415351138062336', '786505180710834176', '786251441026502656', '783026006054277120', '786017188690923520', '770443882948333568', '745023088860729344', '764867003197558784', '786901303959097344', '758642623866081280']
	groupB = ['786662911929356288', '786278862241009664', '787725720767303680', '750829407714807808', '786868627755634688', '788372063449911296', '786518208890146816', '786397083208126464', '748871909478895616', '786739331502313472', '782445634534903808', '785984233809514496', '787296349644525568', '786254340481748992', '785915523748990976', '785385226561261568', '787841445033021440', '786405523112464384', '787645887567761408', '786863739537592320', '788523464587087872', '786370637982994432', '786311832742006784', '776731765929807872', '788401449322549248', '787678919058132992', '787128748872175616', '742377514978119680', '786185836491575296', '741976992995479552', '773619638146830336', '784448760138043392', '783838498259341312', '787681410751533056', '786535519600644096', '786662208263557120', '787988736024907776', '786998024978436096', '750809421399068672', '784372544919965696', '786356456512688128', '785297695064854528', '764071967253991424', '785975225652482048', '786854150293295104', '786119884378869760', '766635643475464192', '749045979952582656', '787391985068937216', '787996732838776832', '772420430106595328', '788025245675425792', '788349985921306624', '787961928734871552', '787469224632520704', '786131111742148608', '769958214044356608', '759257213809332224', '773578961555951616', '763698043357892608', '752735034523389952', '788332389826170880', '787958962577608704', '786512277548699648', '786376848988049408', '786373003834691584', '756478144327847936', '787309296764719104', '788377385962967040', '787636359157649408', '783833447503630336', '774574153029259264', '786011117389746176', '785078078354624512', '784215964089978880', '786251079922094080', '784490182664916992', '787468873477001216', '788384153623465984', '785983673198841856', '787441663705747456', '788175362114064384', '786646688621531136', '785073535273275392', '787734815326998528']
	groupC = ['745046537415036928', '772583651992014848', '786020788326502400', '786126025360019456', '786139347409047552', '786147915340320768', '786154547810013184', '786143842864074752', '786142295740518400', '785266217006272512', '767397418915467264', '786187272428326912', '762247804247740416', '786193160585220096', '786181269850820608', '786215757125390336', '778330126592118784', '771347990643347456', '786241904223588352', '774694201433657344', '774644913827614720', '783265705020755968', '786311008590630912', '781560093387919360', '785090275063500800', '786381274092015616', '786429724598865920', '786521370409701376', '786156752520744960', '786544321163038720', '747422000150089728', '771293488271724544', '33338888797558322', '785846045757542400', '781310021123313664', '786649185838829568', '786686152454574080', '786732711137382400', '755171421218541568', '786762842165874688', '781296111397769216', '786868241707700224', '786880961643417600', '786891302246944768', '751408544481415168', '782930056551993344', '785136760874471424', '786865933745786880', '770298891106127872', '785073088047222784', '787062018351173632', '785952979768709120', '787249750067253248', '787266711417393152', '785655302518607872', '780406671934623744', '786132634710708224', '787091758134988800', '776766175429398528', '786250637959892992', '787469593240539136', '787476594318249984', '787450672236072960', '786708779277619200', '787639979898966016', '787655543132393472', '787661799989579776', '786206243508588544', '784742889019543552', '787680496254849024', '782949204866764800', '787680819727962112', '787811240742621184', '787818470279876608', '786863385966153728', '787932009535508480', '787278183530893312', '787953173108494336', '777583590140678144', '787996196043362304', '787272680801177600', '787839676970307584', '788053853320122368', '788073140839649280', '788137800926367744', '774758050945961984', '765293717945978880', '788118913258229760', '769576112123678720', '780990922946121728', '776438614224670720', '757174985025523712', '782697995090137088', '785074193820946432', '751833936912584704', '788344917113311232', '779621195325771776', '788347548837744640', '784543665766141952', '788390183556222976', '786620749510479872', '788409430063845376', '785565471109156864', '742071172597944320', '786541515400155136', '788520409732878336', '788457769744601088', '788531106529284096', '760301148505772032', '777608992221433856', '789199922972200960', '800127628916166656']
	print("51A:(total number is {})".format(len(groupA)))
	count_amount_trainees_finish_inday_bylesson_number(groupA)
	print("***************************\n51B:(total number is {})".format(len(groupB)))
	count_amount_trainees_finish_inday_bylesson_number(groupB)
	print("***************************\n51C:(total number is {})".format(len(groupC)))
	count_amount_trainees_finish_inday_bylesson_number(groupC)