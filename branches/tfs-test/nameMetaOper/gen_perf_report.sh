#!/bin/bash

log_name=$1
user_id=$2
log_count=$3
total_tps=0


for i in $(seq $log_count):
do
  echo ${log_name}.${user_id}
  per_stat=$(grep "TPS" ${log_name}.${user_id}|tail -1)
  echo $per_stat
  per_tps=$(echo $per_stat|awk '{print $17}'|sed 's/\..*//g')
  ((user_id += 1))
  ((total_tps += per_tps))
done
echo "succTPS of $log_count process: ${total_tps}"
