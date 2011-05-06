CREATE TABLE T_RESOURCE_SERVER_INFO (
  ADDR_INFO CHAR(64) NOT NULL,
  STAT INT NOT NULL DEFAULT 1,
  REM CHAR(255),
  CREATE_TIME DATETIME,
  MODIFY_TIME DATETIME,
  PRIMARY KEY (ADDR_INFO)
);

CREATE TABLE T_CLUSTER_RACK_INFO (
  CLUSTER_RACK_ID INT,
  CLUSTER_ID   CHAR(8),
  NS_VIP CHAR(64),
  CLUSTER_STAT  INT,
  REM CHAR(255),
  CREATE_TIME DATETIME,
  MODIFY_TIME DATETIME,
  PRIMARY KEY (CLUSTER_RACK_ID, CLUSTER_ID)
);
ALTER TABLE T_CLUSTER_RACK_INFO ADD UNIQUE KEY UN_CLUSTER_ID (CLUSTER_ID);
ALTER TABLE T_CLUSTER_RACK_INFO ADD UNIQUE KEY UN_NS_VIP  (NS_VIP);

CREATE TABLE T_CLUSTER_RACK_GROUP (
  CLUSTER_GROUP_ID INT,
  CLUSTER_RACK_ID INT,
  CLUSTER_RACK_ACCESS_TYPE INT,
  REM CHAR(255),
  CREATE_TIME DATETIME,
  MODIFY_TIME DATETIME,
  PRIMARY KEY (CLUSTER_GROUP_ID, CLUSTER_RACK_ID)
);

CREATE TABLE T_CLUSTER_RACK_DUPLICATE_SERVER (
  CLUSTER_RACK_ID INT,
  DUPLIATE_SERVER_ADDR CHAR(64),
  CREATE_TIME DATETIME,
  MODIFY_TIME DATETIME,
  PRIMARY KEY (CLUSTER_RACK_ID)
);

CREATE TABLE T_BASE_INFO_UPDATE_TIME (
  BASE_LAST_UPDATE_TIME DATETIME,
  APP_LAST_UPDATE_TIME  DATETIME
);

CREATE TABLE T_APP_INFO (
  APP_KEY CHAR(255),
  ID INT,
  QUTO BIGINT,
  CLUSTER_GROUP_ID INT,
  APP_NAME CHAR(255),
  APP_OWNER CHAR(255),
  REPORT_INTERVAL INT,
  NEED_DUPLICATE INT,
  REM CHAR(255),
  CREATE_TIME DATETIME,
  MODIFY_TIME DATETIME,
  PRIMARY KEY (APP_KEY)
);

ALTER TABLE T_APP_INFO ADD UNIQUE KEY APP_INFO_UN_ID (ID);


CREATE TABLE T_SESSION_INFO (
  SESSION_ID CHAR(255),
  CACHE_SIZE BIGINT,
  CLIENT_VERSION CHAR(64),
  LOG_OUT_TIME DATETIME,
  CREATE_TIME DATETIME,
  MODIFY_TIME DATETIME,
  PRIMARY KEY (SESSION_ID)
);

CREATE TABLE T_SESSION_STAT (
  SESSION_ID CHAR(255),
  OPER_TYPE  INT,
  OPER_TIMES BIGINT,
  FILE_SIZE BIGINT,
  RESPONSE_TIME INT,
  CREATE_TIME DATETIME,
  MODIFY_TIME DATETIME,
  PRIMARY KEY (SESSION_ID, OPER_TYPE)
);

CREATE TABLE T_APP_STAT (
  APP_ID INT,
  USED_CAPACITY  BIGINT,
  FILE_COUNT INT,
  CREATE_TIME DATETIME,
  MODIFY_TIME DATETIME,
  PRIMARY KEY (APP_ID)
);


insert into t_app_info (app_key, id, quto, cluster_group_id, app_name, 
  app_owner, report_interval, need_duplicate, rem, create_time, modify_time)
values ('tappkey00001', 1, 1024*1024*1024, 1, 'picture_space',
  'mutang', 5, 1, '', now(), now());

insert into t_app_info (app_key, id, quto, cluster_group_id, app_name, 
  app_owner, report_interval, need_duplicate, rem, create_time, modify_time)
values ('tappkey00002', 2, 1024*1024*1024, 1, 'ic',
  'xxxx', 5, 1, '', now(), now());

insert into t_base_info_update_time (base_last_update_time, app_last_update_time) 
values (now(), now());

insert into t_cluster_rack_duplicate_server
(cluster_rack_id, dupliate_server_addr, create_time, modify_time)
values
(1, '10.232.35.40:81000', now(), now());

insert into t_cluster_rack_group 
(cluster_group_id, cluster_rack_id, cluster_rack_access_type, rem, create_time, modify_time)
values
(1, 1, 1, '', now(), now());

insert into t_cluster_rack_group 
(cluster_group_id, cluster_rack_id, cluster_rack_access_type, rem, create_time, modify_time)
values
(1, 2, 1, '', now(), now());

insert into t_cluster_rack_info
(cluster_rack_id, cluster_id, Ns_vip, cluster_stat, rem, create_time, modify_time)
values
(1, 'T1Mxxxx', '10.232.35.41:1123', 1, '', now(), now());
insert into t_cluster_rack_info
(cluster_rack_id, cluster_id, Ns_vip, cluster_stat, rem, create_time, modify_time)
values
(1, 'T5Mxxxx', '10.232.35.42:1123', 1, '', now(), now());

insert into t_cluster_rack_info
(cluster_rack_id, cluster_id, Ns_vip, cluster_stat, rem, create_time, modify_time)
values
(2, 'T1Bxxxx', '10.232.35.41:2123', 1, '', now(), now());
insert into t_cluster_rack_info
(cluster_rack_id, cluster_id, Ns_vip, cluster_stat, rem, create_time, modify_time)
values
(2, 'T5Bxxxx', '10.232.35.42:2123', 1, '', now(), now());

insert into t_resource_server_info (addr_info, stat, rem, create_time, modify_time)
values ('10.232.35.40:90000', 1, 'test first', now(), now());
insert into t_resource_server_info (addr_info, stat, rem, create_time, modify_time)
values ('10.232.35.40:91000', 1, 'test_second', now(), now());

