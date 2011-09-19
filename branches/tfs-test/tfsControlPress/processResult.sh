#!/bin/bash
#author mingyan.zc
#used to batch process test log

seed_file_list=$(ls |grep tfsSeed.|grep -v temp)
for file_name in $seed_file_list
do
   echo ">>>>>>>>>>>>>>>>>>>>>"$file_name"<<<<<<<<<<<<<<<<<<<<<"
   write_statis=$(grep "write statis:" $file_name)
   read_statis=$(grep "read statis:" $file_name)
   unlink_statis=$(grep "unlink statis:" $file_name)
   write_failcnt=$(echo $write_statis|awk '{print$9}'|sed 's\[a-zA-Z:,]\\g')
   read_failcnt=$(echo $read_statis|awk '{print$9}'|sed 's\[a-zA-Z:,]\\g')
   unlink_failcnt=$(echo $unlink_statis|awk '{print$9}'|sed 's\[a-zA-Z:,]\\g')
   if [ "$write_failcnt" -eq 0 -a "$read_failcnt" -eq 0 -a "$unlink_failcnt" -eq 0 ] 
     then
       echo "All test success!" 
       rm $file_name 
     else
       mv $file_name $file_name".old"
       echo $write_statis 
       echo $read_statis 
       echo $unlink_statis 
   fi
done

