CREATE TABLE  tfs_sequence (
  `name` varchar(50) NOT NULL,
  `current_value` bigint NOT NULL,
  `increment` int NOT NULL DEFAULT '1',
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
insert into tfs_sequence (name, current_value) values ('seq_pid', 1);
delimiter $$
CREATE PROCEDURE  pid_seq_nextval(out cur_val bigint)
BEGIN
  declare aff_row bigint ;
  START TRANSACTION;
  select current_value into cur_val from tfs_sequence where name = 'seq_pid';
  UPDATE tfs_sequence
  SET current_value = current_value + increment where name = 'seq_pid' and current_value = cur_val;
  select row_count() into aff_row;
  commit;
  if aff_row <>1 then
    select 0 into cur_val;
  end if;
END $$
delimiter ;
