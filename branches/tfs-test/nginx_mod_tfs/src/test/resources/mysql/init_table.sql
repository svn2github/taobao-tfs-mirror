USE tfs_stat;

/* t_resource_server_info */
insert into t_resource_server_info (addr_info, stat, rem, create_time, modify_time) values ("10.232.36.202:9202", 1, " ", NOW(), NOW()); 


/* t_cluster_rack_info */
insert into t_cluster_rack_info (cluster_rack_id, cluster_id, ns_vip, cluster_stat, rem, create_time, modify_time) values (1, "T1M", "10.232.36.202:5202", 2, " ", NOW(), NOW());

insert into t_cluster_rack_info (cluster_rack_id, cluster_id, ns_vip, cluster_stat, rem, create_time, modify_time) values (1, "T1B", "10.232.36.209:6209", 1, " ", NOW(), NOW());

insert into t_cluster_rack_info (cluster_rack_id, cluster_id, ns_vip, cluster_stat, rem, create_time, modify_time) values (1, "T2M", "10.232.36.202:7202", 1, " ", NOW(), NOW());

insert into t_cluster_rack_info (cluster_rack_id, cluster_id, ns_vip, cluster_stat, rem, create_time, modify_time) values (2, "T3M", "10.232.36.210:8210", 2, " ", NOW(), NOW());
/*
insert into t_cluster_rack_info (cluster_rack_id, cluster_id, ns_vip, cluster_stat, rem, create_time, modify_time) values (3, "T2M", "10.232.36.202:7202", 2, " ", NOW(), NOW());

insert into t_cluster_rack_info (cluster_rack_id, cluster_id, ns_vip, cluster_stat, rem, create_time, modify_time) values (4, "T1B", "10.232.36.209:6209", 2, " ", NOW(), NOW());
*/
insert into t_cluster_rack_group (cluster_group_id, cluster_rack_id, cluster_rack_access_type, rem, create_time, modify_time) values (1, 1, 2, " ", NOW(), NOW());

insert into t_cluster_rack_group (cluster_group_id, cluster_rack_id, cluster_rack_access_type, rem, create_time, modify_time) values (2, 2, 2, " ", NOW(), NOW());

insert into t_cluster_rack_group (cluster_group_id, cluster_rack_id, cluster_rack_access_type, rem, create_time, modify_time) values (3, 3, 2, " ", NOW(), NOW());

insert into t_cluster_rack_group (cluster_group_id, cluster_rack_id, cluster_rack_access_type, rem, create_time, modify_time) values (4, 4, 2, " ", NOW(), NOW());


/* t_app_info */
insert into t_app_info (app_key, id, quto, cluster_group_id, app_name, app_owner, report_interval, need_duplicate, rem, create_time, modify_time) values ("tfsNginxA01", 1, 30, 1, "tfsNginxA01", "tfsNginxA01", 10, 1, " ", NOW(), NOW());

insert into t_app_info (app_key, id, quto, cluster_group_id, app_name, app_owner, report_interval, need_duplicate, rem, create_time, modify_time) values ("tfsNginxB01", 2, 30, 2, "tfsNginxB01", "tfsNginxB01", 10, 0, " ", NOW(), NOW());

insert into t_app_info (app_key, id, quto, cluster_group_id, app_name, app_owner, report_interval, need_duplicate, rem, create_time, modify_time) values ("tfsNginxB02", 3, 30, 2, "tfsNginxB02", "tfsNginxB02", 10, 0, " ", NOW(), NOW());

insert into t_app_info (app_key, id, quto, cluster_group_id, app_name, app_owner, report_interval, need_duplicate, rem, create_time, modify_time) values ("tfsNginxA02", 4, 30, 1, "tfsNginxA02", "tfsNginxA02", 10, 0, " ", NOW(), NOW());

insert into t_app_info (app_key, id, quto, cluster_group_id, app_name, app_owner, report_interval, need_duplicate, rem, create_time, modify_time) values ("tfsNginxB03", 5, 30, 2, "tfsNginxB03", "tfsNginxB03", 10, 0, " ", NOW(), NOW());

insert into t_app_info (app_key, id, quto, cluster_group_id, app_name, app_owner, report_interval, need_duplicate, rem, create_time, modify_time) values ("tfsNginxA03", 6, 30, 1, "tfsNginxA03", "tfsNginxA03", 10, 0, " ", NOW(), NOW());

insert into t_app_info (app_key, id, quto, cluster_group_id, app_name, app_owner, report_interval, need_duplicate, rem, create_time, modify_time) values ("tfsNginxIPMap01", 7, 30, 1, "tfsNginxIPMap01", "tfsNginxIPMap01", 10, 0, " ", NOW(), NOW());

insert into t_app_info (app_key, id, quto, cluster_group_id, app_name, app_owner, report_interval, need_duplicate, rem, create_time, modify_time) values ("tfsNginxIPMap02", 8, 30, 1, "tfsNginxIPMap02", "tfsNginxIPMap02", 10, 0, " ", NOW(), NOW());

insert into t_app_info (app_key, id, quto, cluster_group_id, app_name, app_owner, report_interval, need_duplicate, rem, create_time, modify_time) values ("tfsNginxC01", 9, 30, 3, "tfsNginxC01", "tfsNginxC01", 10, 0, " ", NOW(), NOW());

insert into t_app_info (app_key, id, quto, cluster_group_id, app_name, app_owner, report_interval, need_duplicate, rem, create_time, modify_time) values ("tfsNginxD01", 10, 30, 4, "tfsNginxD01", "tfsNginxD01", 10, 0, " ", NOW(), NOW());


/* t_app_ip_replace */
insert into t_app_ip_replace (app_id, source_ip, turn_ip) values (7, "10.*.*.*", "10.232.36.203");

insert into t_app_ip_replace (app_id, source_ip, turn_ip) values (8, "10.*.*.*", "10.232.36.210");

/* t_base_info_update_time */
insert into t_base_info_update_time (base_last_update_time, app_last_update_time) values (NOW(), NOW());

/* t_cluster_rack_duplicate_server */
insert into t_cluster_rack_duplicate_server (cluster_rack_id, dupliate_server_addr, create_time, modify_time) values (1, '10.232.4.9:5168;10.232.4.9:5168;group_1;5', NOW(), NOW());


