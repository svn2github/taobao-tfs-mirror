drop procedure rm_dir;
delimiter $$
create procedure
rm_dir(in i_app_id bigint, in i_uid bigint, in i_ppid bigint unsigned,
      in i_pname varbinary(512), in i_pid bigint unsigned, in i_id bigint,
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
        delete from t_meta_info
        where app_id = i_app_id and uid = i_uid and pid = i_pid and name = i_name and id = i_id;
        select row_count() into aff_row;
        if aff_row = 1 then
            select count(1) into aff_row from t_meta_info
            where app_id = i_app_id and uid = i_uid and (pid = i_id or pid = (i_id | (1 << 63)));
                if aff_row = 0 then
                  set o_ret = 1;
                end if;
        end if;
    end if;
    if o_ret = 1 then
      commit;
    else
      rollback;
    end if;
end $$
delimiter ;
