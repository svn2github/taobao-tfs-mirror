drop procedure create_family;
delimiter $$
create procedure
create_family(in i_family_aid_info int,in members_info varbinary(1024))                                                                   
begin
    declare aff_row int;
    declare id bigint;
    declare o_family_id bigint;
    declare ret int;
    declare exit handler for sqlexception
    set id      = 0;
    set aff_row = 0;
    set ret     = -14000;
    set o_family_id = 0;

    CALL  erasurecode_seq_nextval(id);

    if id > 0 then
      start transaction;
      select count(1) into aff_row from t_family_info where family_id = id;
      if aff_row = 0 then
        insert into t_family_info(family_id, family_aid_info, member_infos, create_time)
          values(id, i_family_aid_info, members_info, now());
        set ret = 0;
      else 
        set ret = -16001;
      end if;
      if ret < 0 then
        rollback;
      else
        commit;
        set o_family_id = id;
      end if;
    else
      set ret = -16000;
    end if;
    select o_family_id, ret;
end $$
delimiter ;  
