#!/bin/bash

if [ $# -lt 5 ];then
  echo "usage: $0 mysql_host mysql_port mysql_user mysql_pwd mysql_dbname"
  exit 1
else
  HOST=$1
  DB_PORT=$2
  DB_USER=$3
  DB_PASS=$4
  DB_NAME=$5
fi

mysql_op()
{
  sql_express=$1
  #printf "$sql_express\n"
  #/usr/bin/mysql -h$HOST -u$DB_USER -p$DB_PASS -D$DB_NAME << EOF
  /usr/bin/mysql -h$HOST -P$DB_PORT -u$DB_USER -p$DB_PASS -D$DB_NAME << EOF
  $sql_express;
  QUIT
EOF
}

ROW_LIMIT=100
start_appid=0
start_uid=0
out_file="appid_uid.txt"

if [ -e "$out_file" ]; then
  rm -rf $out_file
fi
touch $out_file


while true
do
  data=`mysql_op "select distinct app_id from t_meta_info where app_id > $start_appid order by app_id asc limit $ROW_LIMIT;" | awk 'NR>1{print $0}'`
  if [ -n "$data" ]; then
    #get last appid as next_start_appid
    start_appid=`echo $data | awk '{print $NF}'`
  else
    break
  fi

  for app_id in $data
  do
    #get uid by spec appid
    start_uid=0
    while true
    do
      res=`mysql_op "select distinct uid from t_meta_info where app_id = $app_id and uid > $start_uid order by uid asc limit $ROW_LIMIT;" | awk 'NR > 1{print $0}'`
      if [ -n "$res" ]; then
        start_uid=`echo $res | awk '{print $NF}'`
        for uid in $res
        do
          echo "$app_id, $uid" >> $out_file
        done
      fi

      if [ `echo $res | wc -w` -lt $ROW_LIMIT ];then
        break;
      fi
    done
  done

  if [ `echo $data | wc -w` -lt $ROW_LIMIT ];then
    break;
  fi

done

echo "dump appid, uid per line successful"
