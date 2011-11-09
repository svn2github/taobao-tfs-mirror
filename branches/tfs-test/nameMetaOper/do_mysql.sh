#!/bin/sh

HOST="10.232.31.33"
DB_USER="tfs_name"
DB_PASS="name_app"
#HOST="10.232.36.208"
#DB_USER="root"
#DB_PASS="123"
DB_NAME="tfs_name_db"

mysql_op()
{
  sql_express=$1
  #printf "$sql_express\n"
  mysql -h$HOST -u$DB_USER -p$DB_PASS << EOF
  use $DB_NAME;
  $sql_express;
  QUIT
EOF
}

#$1 file_list
query()
{
  file_list=$1
  cat $file_list | while read line
  do 
    app_id=$(echo $line | awk '{print $1}')
    user_id=$(echo $line | awk '{print $2}')
    name=$(echo $line | awk '{print $4}')
    result=$(mysql_op "select name from t_meta_info where app_id=$app_id and uid=$user_id and name like \"%$name%\"")
    if [ -n "$result" ]
    then
      continue 
    else
      echo 0
      break
    fi
  done
}

#$2 file_list
clear()
{
  file_list=$1
  cat $file_list | while read line
  do 
    app_id=$(echo $line | awk '{print $1}')
    user_id=$(echo $line | awk '{print $2}')
    name=$(echo $line | awk '{print $4}')
    result=$(mysql_op "delete from t_meta_info where app_id=$app_id and uid=$user_id and name like \"%$name%\"")
  done
}

#$2 app_id
clear_app_id()
{
  app_id=$1
  result=$(mysql_op "delete from t_meta_info where app_id=$app_id")
}

#---------------------------------------------------------
#------------------------- main --------------------------
#---------------------------------------------------------
case $1 in
  query)
  query $2
  ;;
  clear)
  clear $2
  ;;
  clear_app_id)
  clear_app_id $2
  ;;
  *)
  ;;
esac

