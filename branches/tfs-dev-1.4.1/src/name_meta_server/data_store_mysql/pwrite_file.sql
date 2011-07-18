drop procedure pwrite_file;
delimiter $$
create procedure
pwrite_file(in i_app_id bigint, in i_uid bigint,
  in i_pid bigint unsigned,
  in i_name varbinary(512),
  in i_size bigint, in i_ver_no smallint, in i_meta_info VARBINARY(65536),
  out o_ret int)
begin
  declare aff_row int;
  declare next_ver_no smallint;
  declare real_pid bigint unsigned;
  declare exit handler for sqlexception
  begin
    set o_ret = 0;
    rollback;
  end;
  select 0 into aff_row;
  select 0 into o_ret;
  select i_pid | (1 << 63) into real_pid;
  if i_ver_no = 0 then
    set next_ver_no = 1;
  else
    set next_ver_no = i_ver_no + 1;
  end if;
  if next_ver_no = 9999 then
    set next_ver_no = 1;
  end if;
  start transaction;
  /* make sure this file had been created */
  select count(1) into aff_row from t_meta_info where
  app_id = i_app_id and uid = i_uid and pid = real_pid and name = substring(i_name, 1, ascii(substring(i_name, 1, 1)) + 1);

  if aff_row >= 1 then
    if i_ver_no = 0 then
      insert into t_meta_info (app_id, uid, pid , name, id, create_time, modify_time, size, ver_no, meta_info)
      values (i_app_id, i_uid, i_pid | (1 << 63), i_name, 0, now(), now(), i_size, 1, i_meta_info);
    else
      update t_meta_info set modify_time = now(), size = i_size, ver_no = next_ver_no, meta_info = i_meta_info
      where app_id = i_app_id and uid = i_uid and pid = real_pid
      and name = i_name and ver_no = i_ver_no;
    end if;
    select row_count() into aff_row;
  end if;
  set o_ret = aff_row;
  if o_ret = 0 then
    rollback;
  else
    commit;
  end if;
end $$
delimiter ;
