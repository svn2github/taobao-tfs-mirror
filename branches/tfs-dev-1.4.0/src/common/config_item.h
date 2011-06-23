/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_CONFIG_ITEM_H_
#define TFS_COMMON_CONFIG_ITEM_H_

namespace tfs
{
  namespace common
  {
  //public
#define CONF_SN_PUBLIC                                "public"
#define CONF_SN_NAMESERVER                            "nameserver"
#define CONF_SN_DATASERVER                            "dataserver"
#define CONF_SN_TFSCLIENT                             "tfsclient"
#define CONF_SN_ADMINSERVER                           "adminserver"
#define CONF_SN_MOCK_DATASERVER                       "mockdataserver"
#define CONF_SN_RCSERVER                              "rcserver"

#define CONF_CLUSTER_ID                               "cluster_id"
#define CONF_LOCK_FILE                                "lock_file"
#define CONF_PID_FILE                                 "pid_file"
#define CONF_LOG_FILE                                 "log_file"
#define CONF_READ_LOG_FILE                            "read_log_file"
#define CONF_WRITE_LOG_FILE                           "write_log_file"
#define CONF_LOG_SIZE                                 "log_size"
#define CONF_LOG_NUM                                  "log_num"
#define CONF_LOG_LEVEL                                "log_level"
#define CONF_WORK_DIR                                 "work_dir"
#define CONF_OBJECT_DEAD_MAX_TIME                     "object_dead_max_time"
#define CONF_OBJECT_CLEAR_MAX_TIME                    "object_clear_max_time"

#define CONF_PORT                                     "port"
#define CONF_THREAD_COUNT                             "thread_count"
#define CONF_IP_ADDR                                  "ip_addr"
#define CONF_DEV_NAME                                 "dev_name"
#define CONF_BLOCK_MAX_SIZE                           "block_max_size"
#define CONF_BLOCK_USE_RATIO                          "block_max_use_ratio"
#define CONF_HEART_INTERVAL                           "heart_interval"
#define CONF_HEART_MAX_QUEUE_SIZE                     "heart_max_queue_size"
#define CONF_HEART_THREAD_COUNT												"heart_thread_count"
#define CONF_MAX_REPLICATION                          "max_replication"
#define CONF_MIN_REPLICATION                          "min_replication"
#define CONF_USE_CAPACITY_RATIO                       "use_capacity_ratio"


  //adminserver, only monitor ds
#define CONF_DS_SCRIPT                                "ds_script"
#define CONF_DS_FKILL_WAITTIME                        "ds_fkill_waittime"
#define CONF_CHECK_INTERVAL                           "check_interval"
#define CONF_CHECK_COUNT                              "check_count"
#define CONF_DS_INDEX_LIST                            "ds_index_list"
#define CONF_WARN_DEAD_COUNT                          "warn_dead_count"

  //nameserver
#define CONF_IP_ADDR_LIST                             "ip_addr_list"
#define CONF_GROUP_MASK                               "group_mask"
#define CONF_OWNER_CHECK_INTERVAL                     "owner_check_interval"
#define CONF_BLOCK_CHUNK_NUM                          "block_chunk_num"
#define CONF_TASK_MAX_QUEUE_SIZE                      "task_max_queue_size"
#define CONF_TASK_PRECENT_SEC_SIZE                    "task_percent_sec_size"
#define CONF_MAX_WRITE_FILECOUNT                      "max_write_filecount"
#define CONF_ADD_PRIMARY_BLOCK_COUNT                  "add_primary_block_count"
#define CONF_SAFE_MODE_TIME														"safe_mode_time"
#define CONF_BUILD_PLAN_INTERVAL                      "build_plan_interval"
#define CONF_RUN_PLAN_EXPIRE_INTERVAL                 "run_plan_expire_interval"
#define CONF_RUN_PLAN_RATIO                           "run_plan_ratio"
#define CONF_COMPACT_DELETE_RATIO                     "compact_delete_ratio"
#define CONF_COMPACT_MAX_LOAD                         "compact_max_load"
#define CONF_REPLICATE_RATIO                          "replicate_ratio"
#define CONF_REPL_WAIT_TIME                           "repl_wait_time"

#define CONF_OPLOG_SYSNC_MAX_SLOTS_NUM                "oplog_sync_max_slots_num"
#define CONF_OPLOGSYNC_THREAD_NUM                     "oplog_sync_thread_num"

#define CONF_MAX_WAIT_WRITE_LEASE                     "max_wait_write_lease"
#define CONF_MAX_LEASE_TIMEOUT                        "max_lease_timeout"
#define CONF_LEASE_EXPIRED_TIME                       "lease_expired_time"//hour_
#define CONF_CLEANUP_LEASE_THRESHOLD                  "cleanup_lease_threshold"
#define CONF_DUMP_STAT_INFO_INTERVAL                  "dump_stat_info_interval"
#define CONF_BUILD_PLAN_DEFAULT_WAIT_TIME             "build_plan_default_wait_time"
#define CONF_BALANCE_MAX_DIFF_BLOCK_NUM               "balance_max_diff_block_num"

  //dataserver
#define CONF_DATA_THREAD_COUNT                        "data_thread_count"
#define CONF_EXPIRE_COMPACTBLOCK_TIME                 "expire_compactblock_time"
#define CONF_DS_DEAD_TIME                             "ds_dead_time"
#define CONF_DS_THREAD_COUNT                          "ds_thread_count"
#define CONF_EXPIRE_DATAFILE_TIME                     "expire_datafile_time"
#define CONF_EXPIRE_CLONEDBLOCK_TIME                  "expire_clonedblock_time"
#define CONF_VISIT_STAT_INTERVAL                      "dump_visit_stat_interval"
#define CONF_IO_WARN_TIME                             "max_io_warning_time"
#define CONF_REMOVE_PRESERVE_TIME                     "remove_preserve_time"
#define CONF_SLAVE_NSIP                               "slave_nsip"
#define CONF_SLAVE_NSPORT                             "slave_nsport"
#define CONF_SYNC_RETRY_COUNT                         "sync_retry_count"
#define CONF_MOUNT_POINT_NAME                         "mount_name"             //mount point name
#define CONF_MOUNT_MAX_USESIZE                        "mount_maxsize"
#define CONF_BASE_FS_TYPE                             "base_filesystem_type"
#define CONF_AVG_SEGMENT_SIZE                         "avg_file_size"
#define CONF_SUPERBLOCK_START                         "superblock_reserve"     //"0"
#define CONF_MAINBLOCK_SIZE                           "mainblock_size"         //"67108864"
#define CONF_EXTBLOCK_SIZE                            "extblock_size"          //"33554432"
#define CONF_BLOCKTYPE_RATIO                          "block_ratio"            //"2"
#define CONF_BLOCK_VERSION                            "fs_version"             //"1"
#define CONF_HASH_SLOT_RATIO                          "hash_slot_ratio"
#define CONF_WRITE_SYNC_FLAG                          "write_sync_flag"
#define CONF_DATA_FILE_NUMS                           "max_data_file_nums"
#define CONF_MAX_CRCERROR_NUMS                        "max_crc_error_nums"
#define CONF_MAX_EIOERROR_NUMS                        "max_eio_error_nums_"
#define CONF_READ_CACHE_SIZE                          "read_cache_size"
#define CONF_BACKUP_PATH                              "backup_path"
#define CONF_BACKUP_TYPE                              "backup_type"
#define CONF_EXPIRE_CHECKBLOCK_TIME                   "expire_checkblock_time"
#define CONF_MAX_CPU_USAGE                            "max_cpu_usage"
#define CONF_REPLICATE_THREADCOUNT                    "replicate_threadcount"

//rc
#define CONF_RC_MONITOR_INTERVAL                      "rc_monitor_interval"
#define CONF_RC_STAT_INTERVAL                         "rc_stat_interval"
#define CONF_RC_UPDATE_INTERVAL                       "rc_update_interval"
#define CONF_RC_DB_INFO                               "rc_db_info"
#define CONF_RC_DB_USER                               "rc_db_user"
#define CONF_RC_DB_PWD                                "rc_db_pwd"
  }
}
#endif //TFS_COMMON_CONFDEFINE_H_
