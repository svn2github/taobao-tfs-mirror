
USER=admin

LOG_NAME=sync_log
SCRIPT_NAME=sync.sh
CMD_HOME=`dirname $(readlink -f $0)`
SOURCE_SYNC_LOG=${CMD_HOME}/${LOG_NAME}
SOURCE_SYNC_SCRIPT=${CMD_HOME}/${SCRIPT_NAME}
DISPATCH_DS_FILE=$CMD_HOME/ds_list


print_usage()
{
  echo "$0 source_ns_ip dest_ns_ip log_path [thread_count] [sync day] [modify time]"
}

start_sync()
{
  for ds in `cat $DISPATCH_DS_FILE`
  do
    echo -n "$ds: "
    scp -oStrictHostKeyChecking=no ${SOURCE_SYNC_LOG} $USER@$ds:${DEST_SYNC_LOG}
    scp -oStrictHostKeyChecking=no ${SOURCE_SYNC_SCRIPT} $USER@$ds:${DEST_SYNC_SCRIPT}
    ssh -o ConnectTimeout=3 -oStrictHostKeyChecking=no $USER@$ds \
    "cd $6;" \
    "chmod +x ${DEST_SYNC_LOG};" \
    "chmod +x ${DEST_SYNC_SCRIPT};" \
    "nohup sh $DEST_SYNC_SCRIPT $1 $2 $3 $4 $5 >tmp_start_log 2>&1 &"
    echo "start sync done"
  done
}

if ! [ -f $DISPATCH_DS_FILE ]
then
  echo "ds list file not found . exit"
  exit 1
fi

if [ $# -lt 3 ]
then
  print_usage
  exit 1
fi

DEST_SYNC_LOG=$3/${LOG_NAME}
DEST_SYNC_SCRIPT=$3/${SCRIPT_NAME}

if [ -z $4 ]
then
  THREAD_COUNT=2
else
  THREAD_COUNT=$4
fi

if [ -z $5 ]
then
  DAY=`date -d yesterday +%Y%m%d`
else
  DAY=$5
fi

if [ -z $6 ]
then
  MODIFY=`date +%Y%m%d`
else
  MODIFY=$6
fi

echo "start_sync $1 $2 $THREAD_COUNT $DAY $MODIFY $3"
start_sync $1 $2 $THREAD_COUNT $DAY $MODIFY $3
