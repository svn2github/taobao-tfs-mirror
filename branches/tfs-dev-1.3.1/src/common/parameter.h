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
#ifndef TFS_COMMON_PARAMETER_H_
#define TFS_COMMON_PARAMETER_H_

#include "define.h"

namespace tfs
{
  namespace common
  {
    class SysParam
    {
    public:
      struct NameServer
      {
        int32_t min_replication_;
        int32_t max_replication_;
        int32_t max_block_size_;
        int32_t max_write_file_count_;
        int32_t max_use_capacity_ratio_;
        uint32_t group_mask_;
        int32_t ds_dead_time_;
        int32_t heart_interval_;
        int32_t replicate_max_time_;
        int32_t replicate_wait_time_;
        int32_t replicate_check_interval_;
        int32_t replicate_max_count_per_server_;
        int32_t redundant_check_interval_;
        int32_t balance_check_interval_;
        int32_t compact_time_lower_;
        int32_t compact_time_upper_;
        int32_t compact_delete_ratio_;
        int32_t compact_max_load_;
        int32_t compact_preserve_time_;
        int32_t compact_check_interval_;
        int32_t cluster_index_;
        int32_t max_wait_write_lease_;
        int32_t cleanup_lease_time_lower_;
        int32_t cleanup_lease_time_upper_;
        int32_t cleanup_lease_threshold_;
        int32_t cleanup_lease_count_;
        int32_t add_primary_block_count_;
        int32_t safe_mode_time_;
        char* config_log_file_;
      };

      struct FileSystemParam
      {
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
      };

      struct DataServer
      {
        std::string work_dir_;
        std::string log_file_;
        std::string pid_file_;
        int32_t local_ds_port_;
        char* dev_name_;
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
        int32_t client_thread_client_;
        int32_t server_thread_client_;
        int32_t tfs_backup_type_;
        char* local_ns_ip_;
        int32_t local_ns_port_;
        char* slave_ns_ip_;
        char* ns_addr_list_;
        int32_t max_datafile_nums_;
        int32_t max_crc_error_nums_;
        int32_t max_eio_error_nums_;
        int32_t expire_check_block_time_;
        int32_t max_cpu_usage_;
      };

    public:
      const NameServer& nameserver() const
      {
        return nameserver_;
      }
      const DataServer& dataserver() const
      {
        return dataserver_;
      }

      const FileSystemParam& filesystem_param() const
      {
        return filesystem_param_;
      }
      NameServer& nameserver()
      {
        return nameserver_;
      }
      DataServer& dataserver()
      {
        return dataserver_;
      }
      FileSystemParam& filesystem_param()
      {
        return filesystem_param_;
      }
      static SysParam& instance()
      {
        return instance_;
      }

      int load(const std::string& tfsfile);
      int load_data_server(const std::string& tfsfile, const std::string& index);

    private:
      static const int32_t PARAM_BUF_LEN = 64;
    private:
      int load_filesystem_param(const std::string& index);

    private:
      NameServer nameserver_;
      DataServer dataserver_;
      FileSystemParam filesystem_param_;
      static SysParam instance_;

      SysParam();
      void set_hour_range(char* str, int32_t& min, int32_t& max);
    };

#define SYSPARAM_NAMESERVER SysParam::instance().nameserver()
#define SYSPARAM_DATASERVER SysParam::instance().dataserver()
#define SYSPARAM_FILESYSPARAM SysParam::instance().filesystem_param()

  }
}
#endif //TFS_COMMON_SYSPARAM_H_
