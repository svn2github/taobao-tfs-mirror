USE tfs_stat;

/* t_cluster_rack_info */
update t_cluster_rack_group set cluster_rack_access_type = 0, create_time = NOW(), modify_time = NOW() where cluster_group_id = 1;

/* t_base_info_update_time */
update t_base_info_update_time set base_last_update_time = NOW(), app_last_update_time = NOW();
