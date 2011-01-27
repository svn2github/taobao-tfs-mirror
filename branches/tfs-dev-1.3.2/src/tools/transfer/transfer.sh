#!/bin/sh

USER=admin

BLK_ID_FILE=input_block
DEST_DS_FILE=dest_ds
IO_THRESHOLD=512
THREAD_CNT=4

# maybe argument
SOURCE_NS_IP=172.23.90.230:3100
DEST_NS_IP=172.26.90.230:3100

SIG_STOP=15
SIG_SLOW=16
SIG_HIGH=17
SIG_KILL=9

# main server
CMD_HOME=`dirname $(readlink -f $0)`
OP_LOG_FILE=${CMD_HOME}/transfer_op_log
DISPATCH_DS_FILE=$CMD_HOME/ds_list
SAVE_FILE_PATH=${CMD_HOME}/save
SAVE_BLK_FILE=$SAVE_FILE_PATH/${BLK_ID_FILE}
SAVE_DEST_DS_FILE=${SAVE_FILE_PATH}/${DEST_DS_FILE}

# dispatch server
DEST_CMD_HOME=/home/admin/transfer
BIN_NAME=transfer_block
MIN_PORT=3200
DS_PROCESS_COUNT=12
TRANSFER_BIN=${DEST_CMD_HOME}/${BIN_NAME}
INPUT_BLK_ID=${DEST_CMD_HOME}/${BLK_ID_FILE}
INPUT_DEST_DS=${DEST_CMD_HOME}/${DEST_DS_FILE}
SUC_BLK_ID=${INPUT_BLK_ID}.succ
FAIl_BLK_ID=${INPUT_BLK_ID}.fail


op_log ()
{
    echo "[ `date +"%F %T"` ] $*" >> $OP_LOG_FILE
}

print_usage ()
{
    echo "Usage: $0 [dispatch | start | slow | check | stop ]"
}

get_status ()
{
    if [ $1 -eq 0 ]
    then
        echo -n "SUCCESS"
    else
        echo -n "FAIL"
    fi
}

dispatch ()
{
    if [ -n "$1" ] && [ -f $1 ]
    then
        blk_file=$1
        ds_cnt=`wc -l $DISPATCH_DS_FILE | awk '{print $1}'`
        blk_cnt=`wc -l $blk_file | awk '{print $1}'`

        rm -rf ${SAVE_FILE_PATH}
        mkdir -p ${SAVE_FILE_PATH}

        start_line=1
        for ds in `cat $DISPATCH_DS_FILE`
        do
            rm ${SAVE_BLK_FILE}_${ds}
            # a little waste io
            for line in `seq $start_line $ds_cnt $blk_cnt`
            do
                sed -n "${line}p" $blk_file >> ${SAVE_BLK_FILE}_${ds}
            done
            start_line=$(($start_line+1))

            for port in `seq $MIN_PORT 2 $(($MIN_PORT+$DS_PROCESS_COUNT*2))`
            do
                echo "$ds:$port" >> ${SAVE_DEST_DS_FILE}_$ds
            done

            ssh $USER@$ds "if ! [ -e $DEST_CMD_HOME ];then mkdir -p $DEST_CMD_HOME;fi"
            scp ${SAVE_BLK_FILE}_${ds} $USER@$ds:${DEST_CMD_HOME}/${BLK_ID_FILE} && \
                scp ${SAVE_DEST_DS_FILE}_${ds} $USER@$ds:${DEST_CMD_HOME}/${DEST_DS_FILE}

            op_log "dispatch block id to server $ds `get_status $?`, block count: `wc -l ${SAVE_BLK_FILE}_$ds | awk '{print $1}'`"
        done
    else
        echo "invalid dipatch block list file"
    fi
}

# $0 start thread_cnt io
start_transfer()
{
    if [ -n "$1" ] && [ $1 -gt 1 ] >/dev/null
    then
        THREAD_CNT=$1
    fi

    if [ -n "$2" ] &&  [ $2 -gt 1 ] >/dev/null
    then
        IO_THRESHOLD=$2
    fi

    for ds in `cat $DISPATCH_DS_FILE`
    do
        ssh -o ConnectTimeout=3 $USER@$ds \
            "$TRANSFER_BIN -s $SOURCE_NS_IP -n $DEST_NS_IP -f $INPUT_DEST_DS -b $INPUT_BLK_ID -w $IO_THRESHOLD -t $THREAD_CNT -d && ps -C $TRANSFER_BIN -o pid="
        op_log "start transfer on server $ds `get_status $?`"
    done
}

slow_transfer()
{
    if [ -n "$1" ]
    then
        sig=SIG_HIGH
    else
        sig=SIG_SLOW
    fi
    for ds in `cat $DISPATCH_DS_FILE`
    do
        ssh -o ConnectTimeout=3 $USER@$ds 'kill -s '$sig' `ps -C '${BIN_NAME}' -o pid=` 2>/dev/null'
        op_log "$sig transfer on $ds `get_status $?`"
    done
}

stop_transfer()
{
    for ds in `cat $DISPATCH_DS_FILE`
    do
        ssh -o ConnectTimeout=3 $USER@$ds 'kill -s '$SIG_STOP' `ps -C '$BIN_NAME' -o pid=` 2>/dev/null'
        op_log "stop tranfer on $ds `get_status $?`"
    done
}

check_transfer()
{
    for ds in `cat $DISPATCH_DS_FILE`
    do
        pid=`ssh -o ConnectTimeout=3 $USER@$ds "ps -C $BIN_NAME -o pid="`
        pid_ret=$?
        block_id=`ssh -o ConnectTimeout=3 $USER@$ds "tail -1 $SUC_BLK_ID"`
        op_log "check transfer on $ds `get_status $pid_ret`, current success block id: $block_id"
    done
}


####################
if ! [ -f $DISPATCH_DS_FILE ]
then
    echo "ds list file not found . exit"
    exit 1
fi

case "$1" in
    dispatch)
        dispatch $2
        ;;
    start)
        start_transfer $2 $3
        ;;
    slow)
        slow_transfer $2
        ;;
    check)
        check_transfer
        ;;
    stop)
        stop_transfer
        ;;
    *)
        print_usage
esac
####################