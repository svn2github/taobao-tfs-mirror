#!/bin/bash

user_id=$1
max_sub_dirs_count=$2
max_sub_files_count=$3
max_sub_dirs_deep=$4

index=0
dir_prefix=/dir_
file_prefix=/file_
first_dir=/dir_0
dir_list=oper_create_dir.fileList.$user_id
file_list=oper_save_file.fileList.$user_id

# get the already created deepest dir if exist
if [ -f $dir_list ]
then
  first_dir=$(tail -1 $dir_list)
fi

rm -f $dir_list
rm -f $file_list

# dirs width
# can make max_sub_dirs_count+1 dirs, reserve one for firstdir
for i in $(seq 1 $max_sub_dirs_count):
do
  time=$(date |awk '{print $4}'|sed 's/:/_/g')
  curr_dir=${dir_prefix}${time}_${index} 
  echo $curr_dir>>$dir_list
  ((index += 1))
done

# files width
index=0
for i in $(seq 0 $max_sub_files_count):
do
  time=$(date |awk '{print $4}'|sed 's/:/_/g')
  curr_file=${file_prefix}${time}_${index} 
  echo $curr_file>>$file_list
  ((index += 1))
done

# dirs depth
index=0
# first time this dir will success, then will failed because already existed
echo $first_dir>>$dir_list
curr_dir=$first_dir
for i in $(seq 4 $max_sub_dirs_deep):
do
  time=$(date |awk '{print $4}'|sed 's/:/_/g')
  curr_dir=${curr_dir}${dir_prefix}${time}_${index} 
  echo $curr_dir>>$dir_list
  ((index += 1))
done

