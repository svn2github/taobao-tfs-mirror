drop procedure del_family;
delimiter $$
create procedure
del_family( in i_family_id bigint)                                                                   
begin
    declare aff_row int;
    declare ret int;
    declare exit handler for sqlexception
    set ret     = -14000;
    set aff_row = 0;
    start transaction;
    select count(1) into aff_row from t_family_info where family_id = i_family_id;
    if aff_row = 1 then
      delete from t_family_info where family_id = i_family_id;
      select row_count() into aff_row;
      if aff_row >= 1 then
          set ret = 0;
      else
        set ret = -14002;
      end if;
    else 
      set ret = -16001;
    end if;
    if ret < 0 then
      rollback;
    else
      commit;
    end if;
    select ret;
end $$
delimiter ;  
