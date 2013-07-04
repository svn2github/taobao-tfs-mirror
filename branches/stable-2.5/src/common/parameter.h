/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: parameter.h 983 2011-10-31 09:59:33Z duanfei $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_PARAMETER_H_
#define TFS_COMMON_PARAMETER_H_

#include "internal.h"

namespace tfs
{
  namespace common
  {
    struct NameServerParameter
    {
      int initialize(void);
      int32_t max_replication_;
      int32_t max_block_size_;
      int32_t max_write_file_count_;
      int32_t max_use_capacity_ratio_;
      uint32_t group_mask_;
      int32_t heart_interval_;
      int32_t replicate_ratio_;
      int32_t replicate_wait_time_;
      int32_t compact_delete_ratio_;
      int32_t compact_update_ratio_;
      int32_t compact_max_load_;
      int32_t compact_time_lower_;
      int32_t compact_time_upper_;
      int32_t cluster_index_;
      int32_t add_primary_block_count_;
      int32_t safe_mode_time_;
      int32_t task_expired_time_;
      int32_t object_dead_max_time_;
      int32_t object_clear_max_time_;
      int32_t dump_stat_info_interval_;
      int32_t group_seq_;
      int32_t group_count_;
      int32_t report_block_expired_time_;
      int32_t discard_newblk_safe_mode_time_;
      int32_t discard_max_count_;
      int32_t report_block_queue_size_;
      int32_t report_block_time_lower_;
      int32_t report_block_time_upper_;
      int32_t report_block_time_interval_;//day
      int32_t report_block_time_interval_min_;//min(debug)
      int32_t max_task_in_machine_nums_;
      int32_t max_write_timeout_;
      int32_t cleanup_write_timeout_threshold_;
      int32_t choose_target_server_random_max_nums_;
      int32_t keepalive_queue_size_;
      int32_t marshalling_delete_ratio_;
      int32_t marshalling_time_lower_;
      int32_t marshalling_time_upper_;
      int32_t marshalling_type_;
      int32_t max_data_member_num_;
      int32_t max_check_member_num_;
      int32_t max_marshalling_queue_timeout_;
      int32_t move_task_expired_time_;
      int32_t compact_task_expired_time_;
      int32_t marshalling_task_expired_time_;
      int32_t reinstate_task_expired_time_;
      int32_t dissolve_task_expired_time_;
      int32_t max_mr_network_bandwith_ratio_;
      int32_t max_rw_network_bandwith_ratio_;
      int32_t compact_family_member_ratio_;
      int32_t max_single_machine_network_bandwith_;
      int32_t adjust_copies_location_time_lower_;
      int32_t adjust_copies_location_time_upper_;
      double  balance_percent_;

      static NameServerParameter ns_parameter_;
      static NameServerParameter& instance()
      {
        return ns_parameter_;
      }
    };

    struct FileSystemParameter
    {
      int initialize(const std::string& index);
      std::string mount_name_; // name of mount point
      uint64_t max_mount_size_; // the max space of the mount point
      int base_fs_type_;
      int32_t super_block_reserve_offset_;
      int32_t avg_segment_size_;
      int32_t main_block_size_;
      int32_t extend_block_size_;
      float block_type_ratio_;
      int32_t file_system_version_;
      float hash_slot_ratio_; // 0.5
      int32_t max_init_index_element_nums_;
      int32_t max_extend_index_element_nums_;
      static FileSystemParameter fs_parameter_;
      static std::string get_real_mount_name(const std::string& mount_name, const std::string& index);
      static FileSystemParameter& instance()
      {
        return fs_parameter_;
      }
    };

    struct DataServerParameter
    {
      int initialize(const std::string& config_file, const std::string& index);
      int32_t heart_interval_;
      int32_t check_interval_;
      int32_t expire_data_file_time_;
      int32_t expire_cloned_block_time_;
      int32_t expire_compact_time_;
      int32_t replicate_thread_count_;
      int32_t max_block_size_;
      int32_t sync_flag_;
      int32_t dump_vs_interval_;
      int64_t max_io_warn_time_;
      int32_t tfs_backup_type_;
      std::string local_ns_ip_;
      int32_t local_ns_port_;
      std::string slave_ns_ip_;
      std::string ns_addr_list_;
      int32_t max_datafile_nums_;
      int32_t max_crc_error_nums_;
      int32_t max_eio_error_nums_;
      int32_t expire_check_block_time_;
      int32_t max_cpu_usage_;
      int32_t object_dead_max_time_;
      int32_t object_clear_max_time_;
      int32_t dump_stat_info_interval_;
      int32_t max_sync_retry_count_;
      int32_t max_sync_retry_interval_;
      int32_t sync_fail_retry_interval_;
      static std::string get_real_file_name(const std::string& src_file,
          const std::string& index, const std::string& suffix);
      static int get_real_ds_port(const int ds_port, const std::string& index);
      static DataServerParameter ds_parameter_;
      static DataServerParameter& instance()
      {
        return ds_parameter_;
      }
    };

    struct RcServerParameter
    {
      int initialize(void);

      std::string db_info_;
      std::string db_user_;
      std::string db_pwd_;
      int64_t monitor_interval_;
      int64_t stat_interval_;
      int64_t update_interval_;

      static RcServerParameter rc_parameter_;
      static RcServerParameter& instance()
      {
        return rc_parameter_;
      }
    };

    struct NameMetaServerParameter
    {
      struct DbInfo
      {
        DbInfo():hash_value_(0)
        {
        }
        std::string conn_str_;
        std::string user_;
        std::string passwd_;
        int32_t hash_value_;
      };
      int initialize(void);
      std::vector<DbInfo> db_infos_;
      uint64_t rs_ip_port_;
      double gc_ratio_;
      int32_t max_pool_size_;
      int32_t max_cache_size_;
      int32_t max_mutex_size_;
      int32_t free_list_count_;
      int32_t max_sub_files_count_;
      int32_t max_sub_dirs_count_;
      int32_t max_sub_dirs_deep_;

      static NameMetaServerParameter meta_parameter_;
      static NameMetaServerParameter& instance()
      {
        return meta_parameter_;
      }
    };

    struct RtServerParameter
    {
      int32_t mts_rts_lease_expired_time_;
      int32_t mts_rts_renew_lease_interval_;
      int32_t rts_rts_lease_expired_time_;
      int32_t rts_rts_renew_lease_interval_;
      int32_t safe_mode_time_;

      int initialize(void);

      static RtServerParameter rt_parameter_;
      static RtServerParameter& instance()
      {
        return rt_parameter_;
      }
    };

    struct CheckServerParameter
    {
      uint64_t self_id_;
      uint64_t ns_id_;
      int32_t cluster_id_;
      int32_t check_interval_;
      int32_t check_span_;
      int32_t thread_count_;
      int32_t check_retry_turns_;
      int32_t turn_interval_;

      int initialize(const std::string& config_file);

      static CheckServerParameter cs_parameter_;
      static CheckServerParameter& instance()
      {
        return cs_parameter_;
      }
    };

    struct KvMetaParameter
    {
      std::string tair_master_;
      std::string tair_slave_;
      std::string tair_group_;
      int tair_object_area_;
      int32_t dump_stat_info_interval_;
      uint64_t rs_ip_port_;
      uint64_t ms_ip_port_;

      int initialize(const std::string& config_file);

      static KvMetaParameter kv_meta_parameter_;
      static KvMetaParameter& instance()
      {
        return kv_meta_parameter_;
      }
    };

    struct KvRtServerParameter
    {
      int32_t kv_mts_rts_lease_expired_time_;//4s
      int32_t kv_rts_check_lease_interval_;//1s
      int32_t kv_mts_rts_heart_interval_;//2s
      int32_t safe_mode_time_;

      int initialize(void);

      static KvRtServerParameter kv_rt_parameter_;
      static KvRtServerParameter& instance()
      {
        return kv_rt_parameter_;
      }
    };

#define SYSPARAM_NAMESERVER NameServerParameter::instance()
#define SYSPARAM_DATASERVER DataServerParameter::instance()
#define SYSPARAM_FILESYSPARAM FileSystemParameter::instance()
#define SYSPARAM_RCSERVER RcServerParameter::instance()
#define SYSPARAM_NAMEMETASERVER NameMetaServerParameter::instance()
#define SYSPARAM_RTSERVER RtServerParameter::instance()
#define SYSPARAM_CHECKSERVER CheckServerParameter::instance()
#define SYSPARAM_KVMETA KvMetaParameter::instance()
#define SYSPARAM_KVRTSERVER KvRtServerParameter::instance()
  }/** common **/
}/** tfs **/
#endif //TFS_COMMON_SYSPARAM_H_
