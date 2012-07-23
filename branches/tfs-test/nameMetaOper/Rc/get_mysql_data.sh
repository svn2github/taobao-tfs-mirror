#!/bin/bash

REAL_FILE=`readlink -f $0`
BASE_HOME=`dirname $REAL_FILE`
CLUSTER_LIST=$BASE_HOME/ns_list
TMP_FILE=$BASE_HOME/tmp_file

HOST="10.232.36.204"
DB_USER="root"
DB_PASS="tfs"
DB_NAME="tfs_stat"

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

#$1 app_key
get_max_quote()
{
 max_quote=`mysql_op "select QUTO from t_app_info where APP_KEY=\"$1\""`
 echo $max_quote | awk '{print $NF}'
}

#$1 app_key
get_cur_quote()
{
 cur_quote=`mysql_op "select USED_CAPACITY from t_app_stat, t_app_info where t_app_stat.APP_ID=t_app_info.ID and APP_KEY=\"$1\""`
 if [ -n "$cur_quote" ]
 then
   echo $cur_quote | awk '{print $NF}'
 else
   echo 0
 fi
}

#$1 app_key
reset()
{
  ret=`mysql_op "select ID from t_app_info where APP_KEY=\"$1\""`
  app_id=`echo $ret | awk '{print $NF}'`
  mysql_op "update t_app_stat set USED_CAPACITY=0, FILE_COUNT=0 where APP_ID=$app_id"
  mysql_op "delete from t_session_stat where SESSION_ID like \"$app_id-%\"";
}

#$1 app_key
#$2 new_quote 
mod_max_quote()
{
  mysql_op "update t_app_info set quto=$2 where APP_KEY=\"$1\""
  mysql_op "update t_base_info_update_time set APP_LAST_UPDATE_TIME=now(), BASE_LAST_UPDATE_TIME=now()"
}

#$1 app_key
#$2 mode
set_group_mode()
{
  mysql_op "update t_cluster_rack_group set CLUSTER_RACK_ACCESS_TYPE= $2, modify_time=now() where CLUSTER_GROUP_ID=(select CLUSTER_GROUP_ID from t_app_info where APP_KEY=\"$1\")"
  mysql_op "update t_base_info_update_time set BASE_LAST_UPDATE_TIME=now()"
}

#$1 app_key
#$2 mode
#$3 cluster_addr
set_cluster_mode()
{
  if [ -z "$3" ]
  then
    mysql_op "update t_cluster_rack_info set CLUSTER_STAT=$2, modify_time=now() where CLUSTER_RACK_ID in (select CLUSTER_RACK_ID from t_cluster_rack_group where CLUSTER_GROUP_ID=(select CLUSTER_GROUP_ID from t_app_info where APP_KEY=\"$1\"))"
    mysql_op "update t_base_info_update_time set BASE_LAST_UPDATE_TIME=now()"
  else
    mysql_op "update t_cluster_rack_info set CLUSTER_STAT=$2, modify_time=now() where NS_VIP = \"$3\""
    mysql_op "update t_base_info_update_time set BASE_LAST_UPDATE_TIME=now()"
  fi
}


#$1 app_key
get_group_mode()
{
  mysql_op "select CLUSTER_RACK_ACCESS_TYPE from t_cluster_rack_group where CLUSTER_GROUP_ID=(select CLUSTER_GROUP_ID from t_app_info where APP_KEY=\"$1\")"
}

# cluster list should be the same to test project conf
#$1 tfsname
get_addr()
{
  cid=-1
  #ret=`mysql_op "select NS_VIP from t_cluster_rack_info"`
  #ns_ip=`echo $ret | awk '{for(i=2; i<=NF; i++) print $i}'`
  for i in `cat $CLUSTER_LIST` 
  do
    $BASE_HOME/tfstool -s $i -i "stat $1" &>$TMP_FILE
    ret=`cat $TMP_FILE | grep "FILE_NAME"`; 
    if [ -n "$ret" ]
    then
      addr=$i
      break
    fi
  done
  if [ -e $TMPF_FILE ]
  then
    rm $TMP_FILE -f
  fi
  echo $addr
}

#$1 app_key
#$2 clusterName
#$3 ns_addr 
add_cluster()
{
  ret=`mysql_op "select CLUSTER_GROUP_ID from t_app_info where APP_KEY=\"$1\""`
  cluster_group_id=`echo $ret | awk '{print $NF}'`
  ret=`mysql_op "select max(CLUSTER_RACK_ID) from t_cluster_rack_info"`
  cluster_rack_id=`echo $ret | awk '{print $NF}'`
  cluster_rack_id=$(($cluster_rack_id + 1))
  mysql_op "insert into t_cluster_rack_info values($cluster_rack_id, \"$2\", \"$3\", 2, \"\", now(), now())"
  mysql_op "insert into t_cluster_rack_group values($cluster_group_id, $cluster_rack_id, 2, \"\", now(), now())"
  mysql_op "update t_base_info_update_time set BASE_LAST_UPDATE_TIME=now()"
}

#$1 app_key
#$2 cluster_addr
rm_cluster()
{
  ret=`mysql_op "select CLUSTER_GROUP_ID from t_app_info where APP_KEY=\"$1\""`
  cluster_group_id=`echo $ret | awk '{print $NF}'`
  ret=`mysql_op "select CLUSTER_RACK_ID from t_cluster_rack_info where NS_VIP = \"$2\""`
  cluster_rack_id=`echo $ret | awk '{print $NF}'`
  if [ -z "$cluster_rack_id" ]
  then
    echo "ERROR"
    exit
  fi
  mysql_op "delete from t_cluster_rack_group where CLUSTER_GROUP_ID=$cluster_group_id and CLUSTER_RACK_ID = $cluster_rack_id"
  mysql_op "delete from t_cluster_rack_info where CLUSTER_RACK_ID = $cluster_rack_id"
  mysql_op "update t_base_info_update_time set BASE_LAST_UPDATE_TIME=now()"
}

#$1 rc addr
#$2 status
add_rc()
{
  mysql_op "insert into t_resource_server_info values(\"$1\", $2, \"\", now(), now())"
  mysql_op "update t_base_info_update_time set BASE_LAST_UPDATE_TIME=now()"
}

#$1 rc addr
rm_rc()
{
  mysql_op "delete from t_resource_server_info where ADDR_INFO=\"$1\""
  mysql_op "update t_base_info_update_time set BASE_LAST_UPDATE_TIME=now()"
}

#$1 new_mode
#$2 rc addr
set_rc_mode()
{
  if [ -z "$2" ]
  then
    mysql_op "update t_resource_server_info set STAT=$1, MODIFY_TIME=now()"
  else
    mysql_op "update t_resource_server_info set STAT=$1, MODIFY_TIME=now() where ADDR_INFO=\"$2\""
  fi
  mysql_op "update t_base_info_update_time set BASE_LAST_UPDATE_TIME=now()"
}

#$1 old rc addr 
#$2 new rc addr
mod_rc()
{
  mysql_op "update t_resource_server_info set ADDR_INFO=\"$2\", MODIFY_TIME=now() where ADDR_INFO=\"$1\""
  mysql_op "update t_base_info_update_time set BASE_LAST_UPDATE_TIME=now()"
}

#$1 session_id
get_cache_size()
{
  used_capacity=`mysql_op "select CACHE_SIZE from t_session_info where SESSION_ID=\"$1\""`
  echo $used_capacity | awk '{print $NF}'
}

#$1 appKey
#$2 status
set_duplicate_status()
{
  mysql_op "update t_app_info set NEED_DUPLICATE=$2, MODIFY_TIME=now() where APP_KEY=\"$1\""
  mysql_op "update t_base_info_update_time set APP_LAST_UPDATE_TIME=now(), BASE_LAST_UPDATE_TIME=now()"
}

#$1 cluster_rack_id
#$2 new duplicate_server_addr
change_duplicate_server_addr()
{
  mysql_op "update t_cluster_rack_duplicate_server set dupliate_server_addr=\"$2\", modify_time=now() where cluster_rack_id=$1"
  mysql_op "update t_base_info_update_time set base_last_update_time = now()"
}

#$1 APP_KEY
get_used_capacity()
{
  used_capacity=`mysql_op "select USED_CAPACITY from t_app_stat where APP_ID=(select ID from t_app_info where APP_KEY=\"$1\")"`
  echo $used_capacity | awk '{print $NF}'
}

#$1 APP_KEY
get_file_count()
{
  used_capacity=`mysql_op "select FILE_COUNT from t_app_stat where APP_ID=(select ID from t_app_info where APP_KEY=\"$1\")"`
  echo $used_capacity | awk '{print $NF}'
}

#$1 session_id
#$2 oper_type
get_oper_times()
{
  used_capacity=`mysql_op "select OPER_TIMES from t_session_stat where SESSION_ID=\"$1\" and OPER_TYPE=$2"`
  echo $used_capacity | awk '{print $NF}'
}

#$1 session_id
#$2 oper_type
get_succ_times()
{
  used_capacity=`mysql_op "select SUCC_TIMES from t_session_stat where SESSION_ID=\"$1\" and OPER_TYPE=$2"`
  echo $used_capacity | awk '{print $NF}'
}

#$1 session_id
#$2 oper_type
get_file_size()
{
  used_capacity=`mysql_op "select FILE_SIZE from t_session_stat where SESSION_ID=\"$1\" and OPER_TYPE=$2"`
  echo $used_capacity | awk '{print $NF}'
}

#---------------------------------------------------------
#------------------------- main --------------------------
#---------------------------------------------------------
case $1 in
  max_quote)
  get_max_quote $2
  ;;
  cur_quote)
  get_cur_quote $2
  ;;
  reset)
  reset $2
  ;;
  mod_quote)
  mod_max_quote $2 $3
  ;;
  set_group_mode)
  set_group_mode $2 $3
  ;;
  set_cluster_mode)
  set_cluster_mode $2 $3 $4
  ;;
  set_one_cmode)
  set_one_cmode $2 $3 $4
  ;;
  get_addr)
  get_addr $2
  ;;
  add_cluster)
  add_cluster $2 $3 $4
  ;;
  rm_cluster)
  rm_cluster $2 $3
  ;;
  add_rc)
  add_rc $2 $3
  ;;
  rm_rc)
  rm_rc $2
  ;;
  set_rc_mode)
  set_rc_mode $2 $3
  ;;
  mod_rc)
  mod_rc $2 $3
  ;;
  get_cache_size)
  get_cache_size $2
  ;;
  set_duplicate_status)
  set_duplicate_status $2 $3
  ;;
  change_duplicate_server_addr)
  change_duplicate_server_addr $2 $3
  ;;
  get_used_capacity)
  get_used_capacity $2
  ;;
  get_file_count)
  get_file_count $2
  ;;
  get_oper_times)
  get_oper_times $2 $3
  ;;
  get_succ_times)
  get_succ_times $2 $3
  ;;
  get_file_size)
  get_file_size $2 $3
  ;;
  *)
  ;;
esac

