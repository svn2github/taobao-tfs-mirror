
USER=admin

LOG_NAME=sync_log
SCRIPT_NAME=sync.sh
CMD_HOME=`dirname $(readlink -f $0)`
SOURCE_SYNC_LOG=${CMD_HOME}/${LOG_NAME}
SOURCE_SYNC_SCRIPT=${CMD_HOME}/${SCRIPT_NAME}
DISPATCH_DS_FILE=$CMD_HOME/ds_list


print_usage()
{
  echo "$0 source_ns_ip dest_ns_ip log_path [sync day] [modify time]"
}

start_sync()
{
  for ds in `cat $DISPATCH_DS_FILE`
  do
    echo -n "$ds: "
    scp ${SOURCE_SYNC_LOG} $USER@$ds:${DEST_SYNC_LOG}
    scp ${SOURCE_SYNC_SCRIPT} $USER@$ds:${DEST_SYNC_SCRIPT}
    ssh -o ConnectTimeout=3 $USER@$ds \
    "cd $5;" \
    "chmod +x ${DEST_SYNC_LOG};" \
    "chmod +x ${DEST_SYNC_SCRIPT};" \
    "sh $DEST_SYNC_SCRIPT $1 $2 $3 $4"
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
  DAY=`date -d yesterday +%Y%m%d`
else
  DAY=$4
fi

if [ -z $5 ]
then
  MODIFY=`date +%Y%m%d`
else
  MODIFY=$5
fi

start_sync $1 $2 $DAY $MODIFY $3
