#!/bin/sh

cd ../
TFS_HOME="`pwd`"
TFS_CONF=${TFS_HOME}/conf/tfs.conf
BIN_DIR=${TFS_HOME}/bin
NS_BIN=${BIN_DIR}/nameserver
DS_BIN=${BIN_DIR}/dataserver
ADMIN_BIN=${BIN_DIR}/adminserver

warn_echo()
{
  echo -e "\033[36m $@ \033[0m"
}

fail_echo()
{
    echo  -e "\033[31m $@ \033[0m"
}

succ_echo()
{
    echo  -e "\033[32m $@ \033[0m"
}

print_usage()
{
    warn_echo "Usage: $0 [start_ns | stop_ns | start_ds ds_index | stop_ds ds_index | stop_ds_all | check_ns | check_ds | admin_ns | admin_ds | check_admin | stop_admin]"
    warn_echo "ds_index format : 2-4 OR 2,4,3 OR 2-4,6,7 OR '2-4 5,7,8'"
}

# get_info bin type index
get_info()
{
    case $1 in
	ds)
	    if [ $2 -gt 0 ]
	    then
		echo "${DS_BIN} -f ${TFS_CONF} -d -i $3"
	    else
		echo "dataserver $3"
	    fi
	    ;;
	ns)
	    if [ $2 -gt 0 ]
	    then
		echo "$NS_BIN -f $TFS_CONF -d"
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
		echo "$ADMIN_BIN -f $TFS_CONF -d -s $service"
	    else
		run_service=`ps -C adminserver -o cmd |egrep -o ' -s +(ns|ds)' |awk '{print $2}'`
		if [ "$run_service" ]
		then
		    service=$run_service
		fi
		echo "adminserver [ $service ]"
	    fi
	    ;;
	*)
	    warn_echo "wrong argument for get_name"
    esac
}

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

check_run()
{
    case $1 in
	ds)
	    run_pid=`ps -ef | egrep "dataserver.*?-i +$2\b" | egrep -v 'egrep' | awk '{print $2}'`
	    ;;
	ns)
	    run_pid=`ps -C "nameserver" -o pid=`
	    ;;
	admin)
	    run_pid=`ps -C "adminserver" -o pid=`
	    ;;
	*)
	    warn_echo "wrong argument for check_run"
	    exit 1
    esac

    if [ -z "$run_pid" ]
    then
	echo 0
    else
	echo $run_pid
    fi
}

do_start()
{
    if [ $1 == "ds" ] &&  [ -z "$2" ]
    then
	warn_echo "invalid range"
	print_usage
	exit 1
    fi
    local start_index=""
    for i in $2
    do
	# a little ugly
	start_name=`get_info $1 0 $i`
	cmd=`get_info $1 1 $i`
	
	ret_pid=`check_run $1 $i`

	if [ $ret_pid -gt 0 ]
	then
	    fail_echo "$start_name is already running pid: $ret_pid ... CHECK IT"
	else
	    $cmd &
	    start_index="$start_index $i"
	fi
    done

    # check if ns/ds is up
    if [ "$start_index" ]
    then
	sleep 1
    fi

    for i in $start_index
    do
	start_name=`get_info $1 0 $i`
	ret_pid=`check_run $1 $i`

	if [ $ret_pid -gt 0 ]
	then
	    succ_echo "$start_name is up SUCCESSFULLY pid: $ret_pid"
	else
	    fail_echo "$start_name FAIL to up  ... CHECK IT"
	fi
    done
}

do_stop()
{
    if [ $1 == "ds" ] && [ -z "$2" ]
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
	else
	    fail_echo "$stop_name is NOT running ... CHECK IT"
	fi
    done

    if [ "$stop_index" ]
    then
	sleep 5
    fi

    # check if ns/ds is down . necessary ?
    for i in $stop_index
    do
	stop_name=`get_info $1 0 $i`
	ret_pid=`check_run $1 $i`

	if [ $ret_pid -gt 0 ]
	then
	    fail_echo "$stop_name FAIL to stop pid: $ret_pid ... CHECK IT"
	else
	    succ_echo "$stop_name exit SUCCESSFULLY"
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
    # can't exit from ` `
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
    run_index=`ps -C dataserver -o cmd= |egrep -o " -i +[0-9]+" | awk '{print $2}' | sort -n`
    if [ -z "$run_index" ]
    then
	fail_echo "NO dataserver is running ... CHECK IT"
	exit 1
    fi

    killall -15 "dataserver"

  # check if all is down. necessary ?
    sleep 5
    pid=`ps -C dataserver -o pid=`
    index=`ps -C dataserver -o cmd= |egrep -o " -i +[0-9]+" | awk '{print $2}'`

    if [ "$index" ]
    then
	k=1
	for i in $pid
	do
	    fail_echo "dataserver `echo $index | awk '{print $'$k'}'` FAIL to stop pid: $i ... CHECK IT"
	    k=`expr $k + 1`
	done
    else
	succ_echo "all dataservers [ "$run_index" ] stop SUCCESSFULLY"
    fi
}

check_ns()
{
    ret_pid=`check_run ns`
    if [ $ret_pid -gt 0 ]
    then
	succ_echo "nameserver is running pid: $ret_pid"
    else
	fail_echo "nameserver is NOT running ... CHECK IT"
    fi
}

check_ds()
{
    run_index=`ps -C dataserver -o cmd= |egrep -o " -i +[0-9]+" | awk '{print $2}' | sort -n`
    if [ -z "$run_index" ]
    then
	fail_echo "NO dataserver is running ... CHECK IT"
    else
	succ_echo "dataserver [ "$run_index" ] is running"
    fi
}

check_admin()
{
    ret_pid=`check_run admin`
    if [ $ret_pid -gt 0 ]
    then
	succ_echo "adminserver [ `ps -p $ret_pid -o cmd=| egrep -o ' -s +(ns|ds)' |awk '{print $2}'` ] is running pid: $ret_pid"
    else
	fail_echo "NO adminserver is running ... CHECK IT"
    fi
}

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
	elif [ $run_service == "ns" ]
	then
	    stop_admin 1
	else
	    stop_admin 2
	fi
	;;
    check_admin)
	check_admin
	;;
    *)
	print_usage
esac

########################
