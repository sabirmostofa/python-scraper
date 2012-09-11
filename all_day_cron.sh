#! /bin/sh

command="python /home/s/delete/all_day.py > /home/s/delete/all_day_logger.log 2>&1 &"
check="ps -ef|grep all_day.py|grep -v grep|wc -l"
val=$(eval $check)
if [  $val = 1 ];then
	echo "running"
else 
	#start job
	eval $command
	#~ python all_day.py
fi




