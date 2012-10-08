drop table t_family_info;
drop table t_erasurecode_sequence;

CREATE TABLE  t_erasurecode_sequence (
  `name` varchar(50) NOT NULL,
  `current_value` bigint NOT NULL,
  `increment` int NOT NULL DEFAULT '1',
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
insert into t_erasurecode_sequence (name, current_value) values ('erasurecode_sequence', 1);

CREATE TABLE t_family_info (
  family_id bigint not null,
  family_aid_info int not null,
  create_time datetime not null,
  member_infos blob not null,
  PRIMARY KEY (family_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8; 


