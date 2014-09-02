drop table t_family_info;
drop table t_erasurecode_sequence;

CREATE TABLE  t_erasurecode_sequence (
  `name` varchar(50) NOT NULL comment "序列号名称",
  `current_value` bigint NOT NULL comment "当前值",
  `increment` int NOT NULL DEFAULT '1' comment "默认增加值",
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
insert into t_erasurecode_sequence (name, current_value) values ('erasurecode_sequence', 1);

CREATE TABLE t_family_info (
  family_id bigint not null comment " 编组ID",
  family_aid_info int not null comment "编组扩展信息",
  create_time datetime not null comment "编组创建时间",
  member_infos blob not null comment "编组成员信息",
  PRIMARY KEY (family_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8; 


