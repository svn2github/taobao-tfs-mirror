drop procedure erasurecode_seq_nextval;
delimiter $$
CREATE PROCEDURE  erasurecode_seq_nextval(out next_seq bigint)
BEGIN
  START TRANSACTION;                                                                                                         
  UPDATE t_erasurecode_sequence
  SET current_value = current_value + increment where name = 'erasurecode_sequence';
  select current_value - increment into next_seq from t_erasurecode_sequence where name = 'erasurecode_sequence';
  commit;
END $$
delimiter ;


