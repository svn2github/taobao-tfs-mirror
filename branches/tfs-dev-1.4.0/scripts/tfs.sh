#!/bin/sh

TFS_HOME="`cd ..;pwd`"
TFS_NS_CONF=${TFS_HOME}/conf/ns.conf
TFS_DS_CONF=${TFS_HOME}/conf/ds.conf
TFS_MOCK_DS_CONF=${TFS_HOME}/conf/mock_ds.conf
TFS_ADMIN_CONF=${TFS_HOME}/conf/ads.conf
BIN_DIR=${TFS_HOME}/bin
NS_BIN=${BIN_DIR}/nameserver
DS_BIN=${BIN_DIR}/dataserver
ADMIN_BIN=${BIN_DIR}/adminserver
MOCK_DS_BIN=${BIN_DIR}/mock_data_server
NS_CMD="${NS_BIN} -f ${TFS_NS_CONF} -d"
DS_CMD="${DS_BIN} -f ${TFS_DS_CONF} -d -i"
ADMIN_CMD="${ADMIN_BIN} -f ${TFS_ADMIN_CONF} -d -s"
MOCK_DS_CMD="${MOCK_DS_BIN} -f ${TFS_MOCK_DS_CONF} -d -i"
UP_TIME=4
DOWN_TIME=8

ulimit -c unlimited

warn_echo()
{
    printf  "\033[36m $* \033[0m\n"
}

fail_echo()
{
    printf "\033[31m $* ... CHECK IT\033[0m\n"
}

succ_echo()
{
    printf "\033[32m $* \033[0m\n"
}

print_usage()
{
    warn_echo "Usage: $0 [start_ns | stop_ns | start_ds ds_index | stop_ds ds_index | stop_ds_all | check_ns | check_ds | admin_ns | admin_ds | check_admin | stop_admin]"
    warn_echo "ds_index format : 2-4 OR 2,4,3 OR 2-4,6,7 OR '2-4 5,7,8'"
}

# get command or name infomation dynamically
# format: index for ds, capacity for mock
# get_info bin type [index capacity]
get_info()
{
    case $1 in
        ds)
            if [ $2 -gt 0 ]
            then
                echo "${DS_CMD} $3"
            else
                echo "dataserver $3"
            fi
            ;;
        ns)
            if [ $2 -gt 0 ]
            then
                echo "${NS_CMD}"
            else
                echo "nameserver"
            fi
            ;;
        admin)
            if [ $3 -eq 1 ]
            then
                service="ns"
            elif [ $3 -eq 2 ]
            then
                service="ds"
            fi

            if [ $2 -gt 0 ]
            then
                echo "${ADMIN_CMD} $service"
            else
                run_service=`ps -C adminserver -o cmd |egrep -o ' -s +(ns|ds)' |awk '{print $2}'`
                if [ "$run_service" ]
                then
                    service=$run_service
                fi
                echo "adminserver [ $service ]"
            fi
            ;;
        mock_ds)
            if [ $2 -gt 0 ]
            then
                echo "${MOCK_DS_CMD} $3 -c $4 -s $5"
            else
                echo "mock ds $3"
            fi
            ;;
        *)
            exit 1
    esac
}

# get specified index
get_index()
{
    local ds_index=""
  # range index type
    range_index=`echo "$1" | egrep -o '[0-9]+-[0-9]+'` # ignore non-digit and '-' signal chars
    for index in $range_index
    do
        start_index=`echo $index | awk -F '-' '{print $1}'`
        end_index=`echo $index | awk -F '-' '{print $2}'`

        if [ $start_index -gt $end_index ]
        then
            echo ""
            exit 1;
        fi
        ds_index="$ds_index `seq $start_index $end_index`"
    done

  # individual index type
    in_index=`echo " $1 " | tr ',' ' '| sed 's/ /  /g' | egrep -o ' [0-9]+ '`
    ds_index="$ds_index $in_index"
    if [ -z "$range_index" ] && [ -z "$in_index" ]
    then
        echo ""
    else
        echo "$ds_index"
    fi
}

# check if only one instance is running
# format:
# check_run bin [index]
check_run()
{
    case $1 in
        ds)
            grep_cmd="${DS_CMD} +$2\b"
            ;;
        ns)
            grep_cmd="${NS_CMD}"
            ;;
        admin)
            grep_cmd="${ADMIN_CMD}"
            ;;
        mock_ds)
            grep_cmd="${MOCK_DS_CMD} +$2\b"
            ;;
        *)
            exit 1
    esac

    run_pid=`ps -ef | egrep "$grep_cmd" | egrep -v 'egrep' | awk '{print $2}'`

    if [ `echo "$run_pid" | wc -l` -gt 1 ]
    then
        echo -1
    elif [ -z "$run_pid" ]
    then
        echo 0
    else
        echo $run_pid
    fi
}

do_start()
{
    if [ $1 = "ds" ] && [ -z "$2" ]
    then
        warn_echo "invalid range"
        print_usage
        exit 1
    fi

    if [ $1 = "mock_ds" ]
    then
        if [ -z "$2" ]
        then
            warn_echo "invalid range"
            print_usage
            exit 1
        fi
        if [ -z "$3" ]
        then
            warn_echo "invalid capacity for mock ds"
            print_usage
            exit 1
        fi
	if [ -z "$4" ]
	then
	    warn_echo "invalid size for mock ds"
	    print_usage
	    exit 1
	fi
    fi

    local start_index=""
    for i in $2
    do
        # a little ugly
        start_name=`get_info $1 0 $i`
        cmd=`get_info $1 1 $i $3 $4` # only mock_ds use $3 as capacity, $4 as size

        ret_pid=`check_run $1 $i`

        if [ $ret_pid -gt 0 ]
        then
            fail_echo "$start_name is already running pid: $ret_pid"
        elif [ $ret_pid -eq 0 ]
        then
            $cmd &
            start_index="$start_index $i"
        else
            fail_echo "more than one same $start_name is running"
        fi
    done

    # check if ns/ds is up
    if [ "$start_index" ]
    then
        sleep ${UP_TIME}
    fi

    for i in $start_index
    do
        start_name=`get_info $1 0 $i`
        ret_pid=`check_run $1 $i`

        if [ $ret_pid -gt 0 ]
        then
            succ_echo "$start_name is up SUCCESSFULLY pid: $ret_pid"
        elif [ $ret_pid -eq 0 ]
        then
            fail_echo "$start_name FAIL to up"
        else
            fail_echo "more than one same $start_name is running"
        fi
    done
}

do_stop()
{
    if ( [ $1 = "ds" ] || [ $1 = "mock_ds" ] ) && [ -z "$2" ]
    then
        warn_echo "invalid range"
        print_usage
        exit 1
    fi

    local stop_index=""
    for i in $2
    do
        stop_name=`get_info $1 0 $i`
        ret_pid=`check_run $1 $i`

        if [ $ret_pid -gt 0 ]
        then
            kill -15 $ret_pid
            stop_index="$stop_index $i"
        elif [ $ret_pid -eq 0 ]
        then
            fail_echo "$stop_name is NOT running"
        else
            fail_echo "more than one same $stop_name is running"
        fi
    done

    if [ "$stop_index" ]
    then
        sleep ${DOWN_TIME}
    fi

    # check if ns/ds is down
    for i in $stop_index
    do
        stop_name=`get_info $1 0 $i`
        ret_pid=`check_run $1 $i`

        if [ $ret_pid -gt 0 ]
        then
            fail_echo "$stop_name FAIL to stop pid: $ret_pid"
        elif [ $ret_pid -eq 0 ]
        then
            succ_echo "$stop_name exit SUCCESSFULLY"
        else
            fail_echo "more than one same $stop_name is running"
        fi
    done
}

start_ns()
{
    do_start "ns" 0
}

stop_ns()
{
    do_stop "ns" 0
}

start_ds()
{
    do_start "ds" "`get_index "$1"`"
}

stop_ds()
{
    do_stop "ds" "`get_index "$1"`"
}

start_admin()
{
    do_start "admin" $1
}

stop_admin()
{
    do_stop "admin" $1
}

stop_ds_all()
{
    run_index=`ps -ef | egrep "${DS_CMD}" | egrep -o " -i +[0-9]+" | awk '{print $2}' | sort -n`
    if [ -z "$run_index" ]
    then
        fail_echo "NO dataserver is running"
        exit 1
    fi

    dup_run_index=`echo "$run_index" | uniq -d`
    uniq_run_index=`echo "$run_index" | uniq -u`

    if [ "$dup_run_index" ]
    then
        fail_echo "more than one same dataserver [ "$dup_run_index" ] is running"
    fi

    if [ "$uniq_run_index" ]
    then
        stop_ds "`echo $uniq_run_index`"
    fi
}

check_ns()
{
    ret_pid=`check_run ns`
    if [ $ret_pid -gt 0 ]
    then
        succ_echo "nameserver is running pid: $ret_pid"
    elif [ $ret_pid -eq 0 ]
    then
        fail_echo "nameserver is NOT running"
    else
        fail_echo "more than one same nameserver is running"
    fi
}

check_ds()
{
    run_index=`ps -ef | egrep "${DS_CMD}" | egrep -o " -i +[0-9]+" | awk '{print $2}' | sort -n`

    if [ -z "$run_index" ]
    then
        fail_echo "NO dataserver is running"
        exit 1
    fi

    dup_run_index=`echo "$run_index" | uniq -d`
    uniq_run_index=`echo "$run_index" | uniq -u`

    if [ "$dup_run_index" ]
    then
        fail_echo "more than one same dataserver [ "$dup_run_index" ] is running"
    fi
    if [ "$uniq_run_index" ]
    then
        succ_echo "dataserver [ "$uniq_run_index" ] is running"
    fi
}

check_admin()
{
    ret_pid=`check_run admin`
    if [ $ret_pid -gt 0 ]
    then
        succ_echo "adminserver [ `ps -p $ret_pid -o cmd=| egrep -o ' -s +(ns|ds)' |awk '{print $2}'` ] is running pid: $ret_pid"
    elif [ $ret_pid -eq 0 ]
    then
        fail_echo "NO adminserver is running"
    else
        fail_echo "more than one same adminserver is running"
    fi
}

#### for test mock ####
start_mock_ds()
{
    do_start "mock_ds" "`get_index "$1"`" "`get_index "$2"`" "`get_index "$3"`"
}

stop_mock_ds()
{
    do_stop "mock_ds" "`get_index "$1"`"
}
#### for test mock end ####

########################
case "$1" in
    start_ns)
        start_ns
        ;;
    check_ns)
        check_ns
        ;;
    stop_ns)
        stop_ns
        ;;
    start_ds)
        start_ds "$2"
        ;;
    check_ds)
        check_ds
        ;;
    stop_ds)
        stop_ds "$2"
        ;;
    stop_ds_all)
        stop_ds_all
        ;;
    admin_ns)
        start_admin 1
        ;;
    admin_ds)
        start_admin 2
        ;;
    stop_admin)
        run_service=`ps -C adminserver -o cmd=|egrep -o ' -s +(ns|ds)'|awk '{print $2}'`
        if [ -z "$run_service" ]
        then
            stop_admin 0
        elif [ $run_service = "ns" ]
        then
            stop_admin 1
        else
            stop_admin 2
        fi
        ;;
    check_admin)
        check_admin
        ;;
    # for test mock
    start_mock_ds)
        start_mock_ds "$2" "$3" "$4"                 # start_mock_ds index capacity size
        ;;
    stop_mock_ds)
        stop_mock_ds "$2"
        ;;
    *)
        print_usage
esac

########################
