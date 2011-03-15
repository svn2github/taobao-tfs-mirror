#!/bin/bash

REAL_FILE=`readlink -f $0`
BASE_HOME=`dirname $REAL_FILE`


warn_echo()
{
  printf  "\033[36m $* \033[0m\n"
}

print_usage()
{
  warn_echo "Usage: $0 source_ns_ip dest_ns_ip [day]"
  warn_echo "          source_ns_ip: source ns ip"
  warn_echo "          dest_ns_ip:   dest ns ip"
  warn_echo "          day:          the date of log to be sync, default is yesterday, ex, 20110303, optional"
}
#$1 day
next_day()
{
  oneday=`date -d $1 +%s`
  diff=`echo $oneday+86400 | bc`
  next=`date -d "1970-01-01 UTC $diff seconds" +"%Y%m%d"`
  echo $next
}
#$1: log time
#$2: day
check_date()
{
  next=`next_day $2`
  if [ $1 -gt $2'000100' ] && [ $1 -lt $next'000100' ]
  then
    echo 0
  else
    echo 1
  fi

}
#$1: day
get_log_name()
{
  all_log=`ls dataserver_*_write_stat.log.*`
  for one in $all_log
  do
    log_time=`echo $one | awk -F '.' '{print $NF}'`
    ret=`check_date $log_time $1`
    if [ $ret -eq 0 ]
    then
      echo $one
    fi
  done
}

if [ $# -lt 2 ]
then
  print_usage
  exit 1
fi
if [ -z $3 ]
then
  DAY=`date -d yesterday +%Y%m%d`
else
  DAY=$3
fi
FILE_NAME='total_write.log'
if [ -e "$FILE_NAME" ]
then
  mv $FILE_NAME $FILE_NAME'.'$DAY
fi

LOG_NAME=`get_log_name $DAY`
for log in $LOG_NAME
do
  cat $log >> $FILE_NAME
done

$BASE_HOME/sync_log -s $1 -d $2 -f $FILE_NAME
if [ -e "$FILE_NAME" ]
then
  rm $FILE_NAME
fi

