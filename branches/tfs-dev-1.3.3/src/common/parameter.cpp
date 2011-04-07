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
#include "config.h"
#include "config_item.h"
#include "error_msg.h"
#include <tbsys.h>
using namespace std;

namespace tfs
{
  namespace common
  {
    inline string change_index(char* base, const std::string& suffix, const std::string& index)
    {
      if (base == NULL)
      {
        return NULL;
      }

      string name(base);
      if (suffix.size() == 0)
      {
        name.append(index);
        return (char*) name.c_str();
      }

      size_t pos = name.rfind(suffix);
      if (pos == string::npos)
      {
        return NULL; // too rough
      }
      name.replace(pos, suffix.size(), index + suffix);
      return name;
    }

    SysParam SysParam::instance_;

    SysParam::SysParam()
    {
    }

    void SysParam::set_hour_range(char* str, int32_t& min, int32_t& max)
    {
      if (str == NULL)
        return;

      char *p1, *p2, buffer[PARAM_BUF_LEN];
      strncpy(buffer, str, 63);
      p1 = buffer;
      p2 = strsep(&p1, "-~ ");
      if (p2 != NULL && p2[0] != '\0')
      {
        min = atoi(p2);
      }
      if (p1 != NULL && p1[0] != '\0')
      {
        max = atoi(p1);
      }
    }

    int SysParam::load(const string& tfsfile)
    {
      int32_t ret = CONFIG.load(tfsfile.c_str());
      if (ret != TFS_SUCCESS)
        return ret;

      char* index = CONFIG.get_string_value(CONFIG_PUBLIC, CONF_CLUSTER_ID);
      if (index == NULL 
          || strlen(index) < 1
          || !isdigit(index[0]))
      {
        fprintf(stderr, "%s(%s) is invalid\n", CONF_CLUSTER_ID, index == NULL ? "null" : index);
        return EXIT_SYSTEM_PARAMETER_ERROR; 
      }
      nameserver_.cluster_index_ = index[0];

      int32_t block_use_ratio = CONFIG.get_int_value(CONFIG_PUBLIC, CONF_BLOCK_USE_RATIO, 95);
      if (block_use_ratio <= 0)
        block_use_ratio = 95;
      block_use_ratio = std::min(100, block_use_ratio);
      int32_t max_block_size = CONFIG.get_int_value(CONFIG_PUBLIC, CONF_BLOCK_MAX_SIZE);
      if (max_block_size <= 0)
      {
        fprintf(stderr, "%s(%d) is invalid\n", CONF_BLOCK_MAX_SIZE, max_block_size);
        return EXIT_SYSTEM_PARAMETER_ERROR; 
      }
      // roundup to 1M
      int32_t writeBlockSize = (int32_t)(((double) max_block_size * block_use_ratio) / 100);
      nameserver_.max_block_size_ = (writeBlockSize & 0xFFF00000) + 1024 * 1024;
      nameserver_.max_block_size_ = std::max(nameserver_.max_block_size_, max_block_size);


      nameserver_.min_replication_ = CONFIG.get_int_value(CONFIG_PUBLIC, CONF_MIN_REPLICATION, 2);
      nameserver_.max_replication_ = CONFIG.get_int_value(CONFIG_PUBLIC, CONF_MAX_REPLICATION, 2);
      if (nameserver_.min_replication_ <= 0)
         nameserver_.min_replication_ = 2;
      nameserver_.max_replication_ = std::max(nameserver_.min_replication_, nameserver_.max_replication_);

      nameserver_.replicate_ratio_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_REPLICATE_RATIO, 50);
      if (nameserver_.replicate_ratio_ <= 0)
        nameserver_.replicate_ratio_ = 50;
      nameserver_.replicate_ratio_ = std::min(nameserver_.replicate_ratio_, 100);

      nameserver_.max_write_file_count_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_MAX_WRITE_FILECOUNT, 16);
      nameserver_.max_write_file_count_ = std::min(nameserver_.max_write_file_count_, 128);

      nameserver_.max_use_capacity_ratio_ = CONFIG.get_int_value(CONFIG_PUBLIC, CONF_USE_CAPACITY_RATIO, 98);
      nameserver_.max_use_capacity_ratio_ = std::min(nameserver_.max_use_capacity_ratio_, 100);

      TBSYS_LOG(INFO, "load configure::max_block_size_:%u, min_replication_:%u,"
        "max_replication_:%u,max_write_file_count_:%u,max_use_capacity_ratio_:%u\n", nameserver_.max_block_size_,
          nameserver_.min_replication_, nameserver_.max_replication_, nameserver_.max_write_file_count_,
          nameserver_.max_use_capacity_ratio_);

      char* group_mask_str = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_GROUP_MASK, "255.255.255.255");
      if (group_mask_str == NULL)
          group_mask_str = "255.255.255.255";
      nameserver_.group_mask_ = Func::get_addr(group_mask_str);

      nameserver_.heart_interval_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_HEART_INTERVAL, 2);
      if (nameserver_.heart_interval_ <= 0)
        nameserver_.heart_interval_ = 2;

      nameserver_.config_log_file_ = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_LOG_FILE);
      nameserver_.replicate_wait_time_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_REPL_WAIT_TIME, 240);
      if (nameserver_.replicate_wait_time_ <= 0)
        nameserver_.replicate_wait_time_ = 240;
      nameserver_.compact_delete_ratio_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_COMPACT_DELETE_RATIO, 15);
      if (nameserver_.compact_delete_ratio_ <= 0)
        nameserver_.compact_delete_ratio_ = 15;
      nameserver_.compact_delete_ratio_ = std::min(nameserver_.compact_delete_ratio_, 100);

      nameserver_.compact_max_load_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_COMPACT_MAX_LOAD, 100);
      nameserver_.object_dead_max_time_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_OBJECT_DEAD_MAX_TIME, 86400);
      if (nameserver_.object_dead_max_time_ <=  0)
        nameserver_.object_dead_max_time_ = 86400;
      nameserver_.object_clear_max_time_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_OBJECT_CLEAR_MAX_TIME, 300);
      if (nameserver_.object_clear_max_time_ <= 0)
        nameserver_.object_clear_max_time_ = 300;

     int32_t thread_count = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_THREAD_COUNT, 1);
      nameserver_.max_wait_write_lease_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_MAX_WAIT_WRITE_LEASE, 5);
      if (nameserver_.max_wait_write_lease_ >= thread_count)
        nameserver_.max_wait_write_lease_ = thread_count / 2;
        
      nameserver_.add_primary_block_count_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_ADD_PRIMARY_BLOCK_COUNT, 3);
      if (nameserver_.add_primary_block_count_ <= 0)
        nameserver_.add_primary_block_count_ = 3;
      nameserver_.add_primary_block_count_ = std::min(nameserver_.add_primary_block_count_, nameserver_.max_write_file_count_);

      nameserver_.safe_mode_time_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_SAFE_MODE_TIME, 300);
      if (nameserver_.safe_mode_time_ <= 0)
        nameserver_.safe_mode_time_ = 300;

      nameserver_.build_plan_interval_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_BUILD_PLAN_INTERVAL, 30);
      if (nameserver_.build_plan_interval_ <= 30)
        nameserver_.build_plan_interval_ = 30;
      nameserver_.run_plan_expire_interval_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_RUN_PLAN_EXPIRE_INTERVAL, 120);
      if (nameserver_.run_plan_expire_interval_ <= 0)
        nameserver_.run_plan_expire_interval_ = 120;
      nameserver_.run_plan_ratio_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_RUN_PLAN_RATIO,25);
      if (nameserver_.run_plan_ratio_ <= 25)
        nameserver_.run_plan_ratio_ = 25;
      nameserver_.run_plan_ratio_ = std::min(nameserver_.run_plan_ratio_, 100);
      nameserver_.dump_stat_info_interval_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_DUMP_STAT_INFO_INTERVAL, 10000000);
      if (nameserver_.dump_stat_info_interval_ <= 60000000)
        nameserver_.dump_stat_info_interval_ = 60000000;
      nameserver_.build_plan_default_wait_time_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_BUILD_PLAN_DEFAULT_WAIT_TIME, 2);//s
      if (nameserver_.build_plan_default_wait_time_ <= 0)
        nameserver_.build_plan_default_wait_time_ = 2;
      nameserver_.balance_max_diff_block_num_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_BALANCE_MAX_DIFF_BLOCK_NUM, 5);//s
      if (nameserver_.balance_max_diff_block_num_ <= 0)
        nameserver_.balance_max_diff_block_num_ = 5;
      return TFS_SUCCESS;
    }

    int SysParam::load_data_server(const string& tfsfile, const string& index)
    {
      int32_t ret = CONFIG.load(tfsfile.c_str());
      if (ret != TFS_SUCCESS)
        return ret;

      std::string server_index;
      server_index.replace(0, server_index.size(), "_" + index);
      char* top_work_dir = CONFIG.get_string_value(CONFIG_PUBLIC, CONF_WORK_DIR);
      if (NULL == top_work_dir)
      {
        TBSYS_LOG(ERROR, "work directory config not found");
        return TFS_ERROR;
      }

      char default_work_dir[MAX_PATH_LENGTH], default_log_file[MAX_PATH_LENGTH], default_pid_file[MAX_PATH_LENGTH];
      char default_read_log_file[MAX_PATH_LENGTH], default_write_log_file[MAX_PATH_LENGTH];
      snprintf(default_work_dir, MAX_PATH_LENGTH-1, "%s/dataserver", top_work_dir);
      snprintf(default_log_file, MAX_PATH_LENGTH-1, "%s/logs/dataserver.log", top_work_dir);
      snprintf(default_pid_file, MAX_PATH_LENGTH-1, "%s/logs/dataserver.pid", top_work_dir);
      snprintf(default_read_log_file, MAX_PATH_LENGTH-1, "%s/logs/dataserver_read_stat.log", top_work_dir);
      snprintf(default_write_log_file, MAX_PATH_LENGTH-1, "%s/logs/dataserver_write_stat.log", top_work_dir);

      dataserver_.work_dir_ = change_index(CONFIG.get_string_value(CONFIG_DATASERVER, CONF_WORK_DIR, default_work_dir),
                                           string(""), server_index);
      dataserver_.log_file_ = change_index(CONFIG.get_string_value(CONFIG_DATASERVER, CONF_LOG_FILE, default_log_file),
                                           string(".log"), server_index);
      dataserver_.pid_file_ = change_index(CONFIG.get_string_value(CONFIG_DATASERVER, CONF_LOCK_FILE, default_pid_file),
                                           string(".pid"), server_index);
      dataserver_.read_stat_log_file_ = change_index(CONFIG.get_string_value(CONFIG_DATASERVER, CONF_READ_LOG_FILE, default_read_log_file), string("_read_stat.log"), server_index);
      dataserver_.write_stat_log_file_ = change_index(CONFIG.get_string_value(CONFIG_DATASERVER, CONF_WRITE_LOG_FILE, default_write_log_file), string("_write_stat.log"), server_index);

      int base_port = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_PORT);
      dataserver_.local_ds_port_ = base_port + atoi(index.c_str()) * PORT_PER_PROCESS - PORT_PER_PROCESS;
      dataserver_.dev_name_ = CONFIG.get_string_value(CONFIG_DATASERVER, CONF_DEV_NAME);

      dataserver_.heart_interval_ = CONFIG.get_int_value(CONFIG_PUBLIC, CONF_HEART_INTERVAL, 5);
      dataserver_.check_interval_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_CHECK_INTERVAL, 2);
      dataserver_.expire_data_file_time_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_EXPIRE_DATAFILE_TIME, 20);
      dataserver_.expire_cloned_block_time_
        = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_EXPIRE_CLONEDBLOCK_TIME, 300);
      dataserver_.expire_compact_time_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_EXPIRE_COMPACTBLOCK_TIME, 86400);
      dataserver_.replicate_thread_count_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_REPLICATE_THREADCOUNT, 3);
      //default use O_SYNC
      dataserver_.sync_flag_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_WRITE_SYNC_FLAG, 1);
      dataserver_.max_block_size_ = CONFIG.get_int_value(CONFIG_PUBLIC, CONF_BLOCK_MAX_SIZE);
      dataserver_.dump_vs_interval_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_VISIT_STAT_INTERVAL, -1);

      char* max_io_time = CONFIG.get_string_value(CONFIG_DATASERVER, "CONF_IO_WARN_TIME", "0");
      dataserver_.max_io_warn_time_ = strtoll(max_io_time, NULL, 10);
      if (dataserver_.max_io_warn_time_ < 200000 || dataserver_.max_io_warn_time_ > 2000000)
        dataserver_.max_io_warn_time_ = 1000000;

      dataserver_.client_thread_client_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_THREAD_COUNT);
      dataserver_.server_thread_client_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_DS_THREAD_COUNT);

      dataserver_.tfs_backup_type_ = CONFIG.get_int_value(CONFIG_PUBLIC, CONF_BACKUP_TYPE, 1);
      dataserver_.local_ns_ip_ = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_IP_ADDR);
      dataserver_.local_ns_port_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_PORT);
      dataserver_.ns_addr_list_ = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_IP_ADDR_LIST);
      dataserver_.slave_ns_ip_ = CONFIG.get_string_value(CONFIG_PUBLIC, CONF_SLAVE_NSIP, NULL);
      //dataserver_.config_log_file_ = CONFIG.get_string_value(CONFIG_DATASERVER, CONF_LOG_FILE);
      dataserver_.max_datafile_nums_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_DATA_FILE_NUMS, 50);
      dataserver_.max_crc_error_nums_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_MAX_CRCERROR_NUMS, 4);
      dataserver_.max_eio_error_nums_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_MAX_EIOERROR_NUMS, 6);
      dataserver_.expire_check_block_time_
        = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_EXPIRE_CHECKBLOCK_TIME, 86400);
      dataserver_.max_cpu_usage_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_MAX_CPU_USAGE, 60);
      return load_filesystem_param(index);
    }

    int SysParam::load_filesystem_param(const string& index)
    {
      if (CONFIG.get_string_value(CONFIG_DATASERVER, CONF_MOUNT_POINT_NAME) == NULL || strlen(CONFIG.get_string_value(
          CONFIG_DATASERVER, CONF_MOUNT_POINT_NAME)) >= static_cast<uint32_t> (MAX_DEV_NAME_LEN))
      {
        std::cerr << "mount name is invalid" << std::endl;
        return TFS_ERROR;
      }
      filesystem_param_.mount_name_ = string(CONFIG.get_string_value(CONFIG_DATASERVER, CONF_MOUNT_POINT_NAME)).append(
          index);

      char* tmp_max_size = CONFIG.get_string_value(CONFIG_DATASERVER, CONF_MOUNT_MAX_USESIZE);
      if (tmp_max_size == NULL)
      {
        std::cerr << "mount max size is null" << std::endl;
        return TFS_ERROR;
      }

      filesystem_param_.max_mount_size_ = strtoull(tmp_max_size, NULL, 10);
      filesystem_param_.base_fs_type_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_BASE_FS_TYPE);
      filesystem_param_.super_block_reserve_offset_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_SUPERBLOCK_START, 0);
      filesystem_param_.avg_segment_size_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_AVG_SEGMENT_SIZE);
      filesystem_param_.main_block_size_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_MAINBLOCK_SIZE);
      filesystem_param_.extend_block_size_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_EXTBLOCK_SIZE);

      char* tmp_ratio = CONFIG.get_string_value(CONFIG_DATASERVER, CONF_BLOCKTYPE_RATIO);
      if (tmp_ratio == NULL)
      {
        std::cerr << "block ratio is null" << std::endl;
        return TFS_ERROR;
      }

      filesystem_param_.block_type_ratio_ = strtof(tmp_ratio, NULL);
      if (filesystem_param_.block_type_ratio_ == 0)
      {
        std::cerr << "block ratio is invalid" << std::endl;
        return TFS_ERROR;
      }

      filesystem_param_.file_system_version_ = CONFIG.get_int_value(CONFIG_DATASERVER, CONF_BLOCK_VERSION, 1);

      char* tmp_hash_ratio = CONFIG.get_string_value(CONFIG_DATASERVER, CONF_HASH_SLOT_RATIO);
      if (tmp_hash_ratio == NULL)
      {
        std::cerr << "hash slot ratio is null" << std::endl;
        return TFS_ERROR;
      }

      filesystem_param_.hash_slot_ratio_ = strtof(tmp_hash_ratio, NULL);
      if (filesystem_param_.hash_slot_ratio_ == 0)
      {
        std::cerr << "block ratio is invalid" << std::endl;
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    int SysParam::load_mock_dataserver(const std::string& conf)
    {
      int32_t ret = CONFIG.load(conf.c_str());
      if (ret != TFS_SUCCESS)
        return ret;

      mock_dataserver_.port_ = CONFIG.get_int_value(CONFIG_MOCK_DATASERVER, CONF_PORT, 3200);

      // get top work directory
      const char *top_work_dir = CONFIG.get_string_value(CONFIG_PUBLIC, CONF_WORK_DIR);
      if (top_work_dir == NULL)
      {
        TBSYS_LOG(ERROR, "work directory config not found");
        return EXIT_CONFIG_ERROR;
      }
      char default_work_dir[MAX_PATH_LENGTH], default_log_file[MAX_PATH_LENGTH];
      snprintf(default_work_dir, MAX_PATH_LENGTH-1, "%s/mock_dataserver", top_work_dir);
      snprintf(default_log_file, MAX_PATH_LENGTH-1, "%s/logs/mock_dataserver.log", top_work_dir);
      mock_dataserver_.work_dir_ = CONFIG.get_string_value(CONFIG_MOCK_DATASERVER, CONF_WORK_DIR, default_work_dir);
      mock_dataserver_.log_file_ = CONFIG.get_string_value(CONFIG_MOCK_DATASERVER, CONF_LOG_FILE, default_log_file);

      mock_dataserver_.dev_name_ = CONFIG.get_string_value(CONFIG_MOCK_DATASERVER, CONF_DEV_NAME);
      return TFS_SUCCESS;
    }

  }
}

