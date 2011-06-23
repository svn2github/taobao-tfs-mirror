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
#include "parameter.h"

#include <tbsys.h>
#include "config.h"
#include "config_item.h"
#include "error_msg.h"
#include "func.h"
#include "internal.h"
namespace
{
  const int PORT_PER_PROCESS = 2;
}
namespace tfs
{
  namespace common
  {
    NameServerParameter NameServerParameter::ns_parameter_;
    FileSystemParameter FileSystemParameter::fs_parameter_;
    DataServerParameter DataServerParameter::ds_parameter_;
    RcServerParameter RcServerParameter::rc_parameter_;

    int NameServerParameter::initialize(void)
    {
      const char* index = TBSYS_CONFIG.getString(CONF_SN_NAMESERVER, CONF_CLUSTER_ID);
      if (index == NULL 
          || strlen(index) < 1
          || !isdigit(index[0]))
      {
        TBSYS_LOG(ERROR, "%s in [%s] is invalid", CONF_CLUSTER_ID, CONF_SN_NAMESERVER);
        return EXIT_SYSTEM_PARAMETER_ERROR;
      }
      cluster_index_ = index[0];

      int32_t block_use_ratio = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_BLOCK_USE_RATIO, 95);
      if (block_use_ratio <= 0)
        block_use_ratio = 95;
      block_use_ratio = std::min(100, block_use_ratio);
      int32_t max_block_size = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_BLOCK_MAX_SIZE);
      if (max_block_size <= 0)
      {
        TBSYS_LOG(ERROR, "%s in [%s] is invalid", CONF_BLOCK_MAX_SIZE, CONF_SN_NAMESERVER);
        return EXIT_SYSTEM_PARAMETER_ERROR;
      }
      // roundup to 1M
      int32_t writeBlockSize = (int32_t)(((double) max_block_size * block_use_ratio) / 100);
      max_block_size_ = (writeBlockSize & 0xFFF00000) + 1024 * 1024;
      max_block_size_ = std::max(max_block_size_, max_block_size);

      min_replication_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_MIN_REPLICATION, 2);
      max_replication_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_MAX_REPLICATION, 2);
      if (min_replication_ <= 0)
         min_replication_ = 2;
      max_replication_ = std::max(min_replication_, max_replication_);

      replicate_ratio_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_REPLICATE_RATIO, 50);
      if (replicate_ratio_ <= 0)
        replicate_ratio_ = 50;
      replicate_ratio_ = std::min(replicate_ratio_, 100);

      max_write_file_count_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_MAX_WRITE_FILECOUNT, 16);
      max_write_file_count_ = std::min(max_write_file_count_, 128);

      max_use_capacity_ratio_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_USE_CAPACITY_RATIO, 98);
      max_use_capacity_ratio_ = std::min(max_use_capacity_ratio_, 100);

      TBSYS_LOG(INFO, "load configure::max_block_size_:%u, min_replication_:%u,"
        "max_replication_:%u,max_write_file_count_:%u,max_use_capacity_ratio_:%u\n", max_block_size_,
          min_replication_, max_replication_, max_write_file_count_,
          max_use_capacity_ratio_);

      const char* group_mask_str = TBSYS_CONFIG.getString(CONF_SN_NAMESERVER, CONF_GROUP_MASK, "255.255.255.255");
      if (group_mask_str == NULL)
          group_mask_str = "255.255.255.255";
      group_mask_ = Func::get_addr(group_mask_str);

      heart_interval_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_HEART_INTERVAL, 2);
      if (heart_interval_ <= 0)
        heart_interval_ = 2;

      replicate_wait_time_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_REPL_WAIT_TIME, 240);
      if (replicate_wait_time_ <= 0)
        replicate_wait_time_ = 240;
      compact_delete_ratio_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_COMPACT_DELETE_RATIO, 15);
      if (compact_delete_ratio_ <= 0)
        compact_delete_ratio_ = 15;
      compact_delete_ratio_ = std::min(compact_delete_ratio_, 100);

      compact_max_load_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_COMPACT_MAX_LOAD, 100);
      object_dead_max_time_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_OBJECT_DEAD_MAX_TIME, 86400);
      if (object_dead_max_time_ <=  0)
        object_dead_max_time_ = 86400;
      object_clear_max_time_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_OBJECT_CLEAR_MAX_TIME, 300);
      if (object_clear_max_time_ <= 0)
        object_clear_max_time_ = 300;

     int32_t thread_count = TBSYS_CONFIG.getInt(CONF_SN_PUBLIC, CONF_THREAD_COUNT, 8);
      max_wait_write_lease_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_MAX_WAIT_WRITE_LEASE, 5);
      if (max_wait_write_lease_ >= thread_count)
        max_wait_write_lease_ = thread_count / 2;
        
      add_primary_block_count_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_ADD_PRIMARY_BLOCK_COUNT, 3);
      if (add_primary_block_count_ <= 0)
        add_primary_block_count_ = 3;
      add_primary_block_count_ = std::min(add_primary_block_count_, max_write_file_count_);

      safe_mode_time_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_SAFE_MODE_TIME, 300);
      if (safe_mode_time_ <= 0)
        safe_mode_time_ = 300;

      build_plan_interval_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_BUILD_PLAN_INTERVAL, 30);
      if (build_plan_interval_ <= 30)
        build_plan_interval_ = 30;
      run_plan_expire_interval_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_RUN_PLAN_EXPIRE_INTERVAL, 120);
      if (run_plan_expire_interval_ <= 0)
        run_plan_expire_interval_ = 120;
      run_plan_ratio_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_BUILD_PLAN_RATIO,25);
      if (run_plan_ratio_ <= 25)
        run_plan_ratio_ = 25;
      run_plan_ratio_ = std::min(run_plan_ratio_, 100);
      dump_stat_info_interval_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_DUMP_STAT_INFO_INTERVAL, 10000000);
      if (dump_stat_info_interval_ <= 60000000)
        dump_stat_info_interval_ = 60000000;
      build_plan_default_wait_time_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_BUILD_PLAN_DEFAULT_WAIT_TIME, 2);//s
      if (build_plan_default_wait_time_ <= 0)
        build_plan_default_wait_time_ = 2;
      balance_max_diff_block_num_ = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_BALANCE_MAX_DIFF_BLOCK_NUM, 5);//s
      if (balance_max_diff_block_num_ <= 0)
        balance_max_diff_block_num_ = 5;
      return TFS_SUCCESS;
    }

    int DataServerParameter::initialize(const std::string& index)
    {
      heart_interval_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_HEART_INTERVAL, 5);
      check_interval_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_CHECK_INTERVAL, 2);
      expire_data_file_time_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_EXPIRE_DATAFILE_TIME, 20);
      expire_cloned_block_time_
        = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_EXPIRE_CLONEDBLOCK_TIME, 300);
      expire_compact_time_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_EXPIRE_COMPACTBLOCK_TIME, 86400);
      replicate_thread_count_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_REPLICATE_THREADCOUNT, 3);
      //default use O_SYNC
      sync_flag_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_WRITE_SYNC_FLAG, 1);
      max_block_size_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_BLOCK_MAX_SIZE);
      dump_vs_interval_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_VISIT_STAT_INTERVAL, -1);

      const char* max_io_time = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, CONF_IO_WARN_TIME, "0");
      max_io_warn_time_ = strtoll(max_io_time, NULL, 10);
      if (max_io_warn_time_ < 200000 || max_io_warn_time_ > 2000000)
        max_io_warn_time_ = 1000000;

      tfs_backup_type_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_BACKUP_TYPE, 1);
      local_ns_ip_ = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, CONF_IP_ADDR);
      if (NULL == local_ns_ip_)
      {
        TBSYS_LOG(ERROR, "can not find %s in [%s]", CONF_IP_ADDR, CONF_SN_DATASERVER);
        return EXIT_SYSTEM_PARAMETER_ERROR;
      }
      local_ns_port_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_PORT);
      ns_addr_list_ = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, CONF_IP_ADDR_LIST);
      if (NULL == ns_addr_list_)
      {
        TBSYS_LOG(ERROR, "can not find %s in [%s]", CONF_IP_ADDR_LIST, CONF_SN_DATASERVER);
        return EXIT_SYSTEM_PARAMETER_ERROR;
      }
      slave_ns_ip_ = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, CONF_SLAVE_NSIP);
      /*if (NULL == slave_ns_ip_)
      {
        TBSYS_LOG(ERROR, "can not find %s in [%s]", CONF_SLAVE_NSIP, CONF_SN_DATASERVER);
        return EXIT_SYSTEM_PARAMETER_ERROR;
      }*/
      //config_log_file_ = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, CONF_LOG_FILE);
      max_datafile_nums_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_DATA_FILE_NUMS, 50);
      max_crc_error_nums_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_MAX_CRCERROR_NUMS, 4);
      max_eio_error_nums_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_MAX_EIOERROR_NUMS, 6);
      expire_check_block_time_
        = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_EXPIRE_CHECKBLOCK_TIME, 86400);
      max_cpu_usage_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_MAX_CPU_USAGE, 60);
      dump_stat_info_interval_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_DUMP_STAT_INFO_INTERVAL, 60000000);
      return SYSPARAM_FILESYSPARAM.initialize(index);
    }

    std::string DataServerParameter::get_real_file_name(const std::string& src_file, 
        const std::string& index, const std::string& suffix)
    {
      return src_file + "_" + index + "." + suffix;
    }

    int DataServerParameter::get_real_ds_port(const int ds_port, const std::string& index)
    {
      return ds_port + ((atoi((index.c_str())) - 1) * PORT_PER_PROCESS);
    }

    int FileSystemParameter::initialize(const std::string& index)
    {
      if (TBSYS_CONFIG.getString(CONF_SN_DATASERVER, CONF_MOUNT_POINT_NAME) == NULL || strlen(TBSYS_CONFIG.getString(
              CONF_SN_DATASERVER, CONF_MOUNT_POINT_NAME)) >= static_cast<uint32_t> (MAX_DEV_NAME_LEN))
      {
        TBSYS_LOG(ERROR, "can not find %s in [%s]", CONF_MOUNT_POINT_NAME, CONF_SN_DATASERVER);
        return EXIT_SYSTEM_PARAMETER_ERROR;
      }

      mount_name_ = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, CONF_MOUNT_POINT_NAME);
      mount_name_ = get_real_mount_name(mount_name_, index);

      const char* tmp_max_size = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, CONF_MOUNT_MAX_USESIZE);
      if (tmp_max_size == NULL)
      {
        TBSYS_LOG(ERROR, "can not find %s in [%s]", CONF_MOUNT_MAX_USESIZE, CONF_SN_DATASERVER);
        return EXIT_SYSTEM_PARAMETER_ERROR;
      }

      max_mount_size_ = strtoull(tmp_max_size, NULL, 10);
      base_fs_type_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_BASE_FS_TYPE);
      super_block_reserve_offset_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_SUPERBLOCK_START, 0);
      avg_segment_size_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_AVG_SEGMENT_SIZE);
      main_block_size_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_MAINBLOCK_SIZE);
      extend_block_size_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_EXTBLOCK_SIZE);

      const char* tmp_ratio = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, CONF_BLOCKTYPE_RATIO);
      if (tmp_ratio == NULL)
      {
        TBSYS_LOG(ERROR, "can not find %s in [%s]", CONF_BLOCKTYPE_RATIO, CONF_SN_DATASERVER);
        return EXIT_SYSTEM_PARAMETER_ERROR;
      }

      block_type_ratio_ = strtof(tmp_ratio, NULL);
      if (block_type_ratio_ == 0)
      {
        TBSYS_LOG(ERROR, "%s error :%s", CONF_BLOCKTYPE_RATIO, tmp_ratio);
        return EXIT_SYSTEM_PARAMETER_ERROR;
      }

      file_system_version_ = TBSYS_CONFIG.getInt(CONF_SN_DATASERVER, CONF_BLOCK_VERSION, 1);

      const char* tmp_hash_ratio = TBSYS_CONFIG.getString(CONF_SN_DATASERVER, CONF_HASH_SLOT_RATIO);
      if (tmp_hash_ratio == NULL)
      {
        TBSYS_LOG(ERROR, "can not find %s in [%s]", CONF_HASH_SLOT_RATIO, CONF_SN_DATASERVER);
        return EXIT_SYSTEM_PARAMETER_ERROR;
      }

      hash_slot_ratio_ = strtof(tmp_hash_ratio, NULL);
      if (hash_slot_ratio_ == 0)
      {
        TBSYS_LOG(ERROR, "%s error :%s", CONF_HASH_SLOT_RATIO, tmp_hash_ratio);
        return EXIT_SYSTEM_PARAMETER_ERROR;
      }

      return TFS_SUCCESS;
    }
    std::string FileSystemParameter::get_real_mount_name(const std::string& mount_name, const std::string& index)
    {
      return mount_name + index;
    }

    int RcServerParameter::initialize(void)
    {
      db_info_ = TBSYS_CONFIG.getString(CONF_SN_RCSERVER, CONF_RC_DB_INFO, "");
      db_user_ = TBSYS_CONFIG.getString(CONF_SN_RCSERVER, CONF_RC_DB_USER, "");
      db_pwd_ = TBSYS_CONFIG.getString(CONF_SN_RCSERVER, CONF_RC_DB_PWD, "");

      monitor_interval_ = TBSYS_CONFIG.getInt(CONF_SN_RCSERVER, CONF_RC_MONITOR_INTERVAL, 60);
      stat_interval_ = TBSYS_CONFIG.getInt(CONF_SN_RCSERVER, CONF_RC_STAT_INTERVAL, 120);
      update_interval_ = TBSYS_CONFIG.getInt(CONF_SN_RCSERVER, CONF_RC_UPDATE_INTERVAL, 30);
      return TFS_SUCCESS;
    }
  }/** common **/
}/** tfs **/

