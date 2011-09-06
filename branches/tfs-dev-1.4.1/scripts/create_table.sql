CREATE TABLE t_resource_server_info (
  addr_info VARCHAR(64) NOT NULL,
  stat INT NOT NULL DEFAULT 1,
  rem VARCHAR(255),
  create_time DATETIME,
  modify_time DATETIME,
  PRIMARY KEY (addr_info)
);

CREATE TABLE t_cluster_rack_info (
  cluster_rack_id INT,
  cluster_id   VARCHAR(8),
  ns_vip VARCHAR(64),
  cluster_stat  INT,
  rem VARCHAR(255),
  create_time DATETIME,
  modify_time DATETIME,
  PRIMARY KEY (cluster_rack_id, cluster_id)
);
ALTER TABLE t_cluster_rack_info ADD UNIQUE KEY un_cluster_id (cluster_id);
ALTER TABLE t_cluster_rack_info ADD UNIQUE KEY un_ns_vip  (ns_vip);

CREATE TABLE t_cluster_rack_group (
  cluster_group_id INT,
  cluster_rack_id INT,
  cluster_rack_access_type INT,
  rem VARCHAR(255),
  create_time DATETIME,
  modify_time DATETIME,
  PRIMARY KEY (cluster_group_id, cluster_rack_id)
);

CREATE TABLE t_cluster_rack_duplicate_server (
  cluster_rack_id INT,
  dupliate_server_addr VARCHAR(64),
  create_time DATETIME,
  modify_time DATETIME,
  PRIMARY KEY (cluster_rack_id)
);

CREATE TABLE t_base_info_update_time (
  base_last_update_time DATETIME,
  app_last_update_time  DATETIME
);

CREATE TABLE t_app_info (
  app_key VARCHAR(255),
  id INT,
  quto BIGINT,
  cluster_group_id INT,
  app_name VARCHAR(255) NOT NULL,
  app_owner VARCHAR(255),
  report_interval INT,
  need_duplicate INT,
  rem VARCHAR(255),
  create_time DATETIME,
  modify_time DATETIME,
  PRIMARY KEY (app_key)
);

ALTER TABLE t_app_info ADD UNIQUE KEY app_info_un_id (id);
ALTER TABLE t_app_info ADD UNIQUE KEY app_info_un_name (app_name);


CREATE TABLE t_session_info (
  session_id VARCHAR(255),
  cache_size BIGINT,
  cache_time BIGINT,
  client_version VARCHAR(64),
  log_out_time DATETIME,
  create_time DATETIME,
  modify_time DATETIME,
  PRIMARY KEY (session_id)
);

CREATE TABLE t_session_stat (
  session_id VARCHAR(255),
  oper_type  INT,
  oper_times BIGINT,
  file_size BIGINT,
  response_time INT,
  succ_times BIGINT,
  create_time DATETIME,
  modify_time DATETIME,
  PRIMARY KEY (session_id, oper_type)
);

CREATE TABLE t_app_stat (
  app_id INT,
  used_capacity BIGINT,
  file_count BIGINT,
  create_time DATETIME,
  modify_time DATETIME,
  PRIMARY KEY (app_id)
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

