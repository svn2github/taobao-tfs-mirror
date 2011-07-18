drop procedure create_file;
delimiter $$
create procedure
create_file(in i_app_id bigint, in i_uid bigint,
  in i_ppid bigint unsigned, in i_pid bigint unsigned, in i_pname varbinary(512),
  in i_name varbinary(512), out o_ret int)
begin
    declare aff_row int;
    declare exit handler for sqlexception
    begin
        set o_ret = 0;
        rollback;
    end;
    select 0 into aff_row;
    select 0 into o_ret;
    start transaction;
    update t_meta_info set modify_time = now()
    where app_id = i_app_id and uid = i_uid and pid = i_ppid and name = i_pname;
    select row_count() into aff_row;
    if aff_row = 1 then
      insert into t_meta_info (app_id, uid, pid , name, id, create_time, modify_time, size, ver_no)
      values (i_app_id, i_uid, i_pid | (1 << 63), i_name, 0, now(), now(), 0, 1);
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
