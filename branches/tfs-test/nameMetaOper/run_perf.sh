#!/bin/bash

# dir
create_dir()
{
  # set oper type
  replace_conf operType 1
  # reset user id
  tmp_user_id=$user_id
  for i in $(seq $pro_count):
  do
    replace_conf userId $tmp_user_id;
    /bin/bash meta_oper.sh start_oper $conf 
    ((tmp_user_id += 1))
  done
}

ls_dir()
{
  # set oper type
  replace_conf operType 2
  # reset user id
  tmp_user_id=$user_id
  for i in $(seq $pro_count):
  do
    replace_conf userId $tmp_user_id;
    /bin/bash meta_oper.sh start_oper $conf 
    ((tmp_user_id += 1))
  done
}

mv_dir()
{
  # set oper type
  replace_conf operType 4
  # reset user id
  tmp_user_id=$user_id
  for i in $(seq $pro_count):
  do
    replace_conf userId $tmp_user_id;
    /bin/bash meta_oper.sh start_oper $conf 
    ((tmp_user_id += 1))
  done
}

rm_dir()
{
  # set oper type
  replace_conf operType 8
  # reset user id
  tmp_user_id=$user_id
  for i in $(seq $pro_count):
  do
    replace_conf userId $tmp_user_id;
    /bin/bash meta_oper.sh start_oper $conf 
    ((tmp_user_id += 1))
  done
}

# small file 
create_file()
{
  # set oper type
  replace_conf operType 16
  # reset user id
  tmp_user_id=$user_id
  for i in $(seq $pro_count):
  do
    replace_conf userId $tmp_user_id;
    /bin/bash meta_oper.sh start_oper $conf 
    ((tmp_user_id += 1))
  done
}

save_small_file()
{
  # set oper type
  replace_conf operType 64
  # reset user id
  tmp_user_id=$user_id
  for i in $(seq $pro_count):
  do
    replace_conf userId $tmp_user_id;
    /bin/bash meta_oper.sh start_oper $conf 
    ((tmp_user_id += 1))
  done
}

ls_file()
{
  # set oper type
  replace_conf operType 32
  # reset user id
  tmp_user_id=$user_id
  for i in $(seq $pro_count):
  do
    replace_conf userId $tmp_user_id;
    /bin/bash meta_oper.sh start_oper $conf 
    ((tmp_user_id += 1))
  done
}

mv_file()
{
  # set oper type
  replace_conf operType 2048 
  # reset user id
  tmp_user_id=$user_id
  for i in $(seq $pro_count):
  do
    replace_conf userId $tmp_user_id;
    /bin/bash meta_oper.sh start_oper $conf 
    ((tmp_user_id += 1))
  done
}

fetch_file()
{
  # set oper type
  replace_conf operType 256 
  # reset user id
  tmp_user_id=$user_id
  for i in $(seq $pro_count):
  do
    replace_conf userId $tmp_user_id;
    /bin/bash meta_oper.sh start_oper $conf 
    ((tmp_user_id += 1))
  done
}

rm_file()
{
  # set oper type
  replace_conf operType 4096 
  # reset user id
  tmp_user_id=$user_id
  for i in $(seq $pro_count):
  do
    replace_conf userId $tmp_user_id;
    /bin/bash meta_oper.sh start_oper $conf 
    ((tmp_user_id += 1))
  done
}

test_dir()
{
  create_dir
  #ls_dir
  #mv_dir
  #rm_dir
}

test_small_file()
{
  save_small_file
  ls_file
  mv_file
  fetch_file
  rm_file
}

replace_conf()
{
  key=$1
  value=$2
  line_no=$(cat -n $conf |grep "\b${key}\b" |grep -v "#"|awk '{print $1}') 
  sed -i "${line_no}s/${key}.*\$/${key} = ${value}/" $conf
}

#---------------------------------------------------------
#------------------------- main --------------------------
#---------------------------------------------------------

if [ $# -lt 4 ]
then
  echo "usage: ./run_perf.sh testcase(dir, small_file, large_file) metaOper.conf pro_count user_id"
  exit 1
fi

testcase=$1
conf=$2
pro_count=$3
user_id=$4

case $testcase in
  dir)
  test_dir
  ;;
  small_file)
  test_small_file
  ;;
  large_file)
  test_large_file
  ;;
  *)
  ;;
esac


