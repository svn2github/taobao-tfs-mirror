/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: checkserver.cpp 746 2012-04-13 13:09:59Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */

#include <unistd.h>
#include <iostream>
#include <string>


#include "common/config_item.h"
#include "common/parameter.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "common/base_packet_factory.h"
#include "common/base_packet_streamer.h"
#include "message/checkserver_message.h"
#include "common/client_manager.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tbutil;
using namespace tbsys;

#include "checkserver.h"

namespace tfs
{
  namespace checkserver
  {
    void CheckThread::run()
    {
      check_ds_list();
    }

    int CheckThread::check_ds_list()
    {
      int ret = TFS_SUCCESS;
      for (uint32_t i = 0; i < ds_list_.size(); i++)
      {
        CheckBlockInfoVec check_ds_result;
        ret = ServerHelper::get_instance()->check_ds(
            ds_list_[i], check_time_, last_check_time_, check_ds_result);
        if (TFS_SUCCESS == ret)  // error handled in ServerHelper
        {
          add_result_map(check_ds_result);
        }
      }
      return ret;
    }

    void CheckThread::add_result_map(const CheckBlockInfoVec& result_vec)
    {
      // add into result map
      result_map_lock_->lock();
      CheckBlockInfoVecConstIter iter = result_vec.begin();
      for ( ; iter != result_vec.end(); iter++)
      {
        CheckBlockInfoMapIter mit = result_map_->find(iter->block_id_);
         if (mit == result_map_->end())
         {
           CheckBlockInfoVec tmp_vec;
           tmp_vec.push_back(*iter);
           result_map_->insert(CheckBlockInfoMap::value_type(iter->block_id_, tmp_vec));
         }
         else
         {
           mit->second.push_back(*iter);
         }
      }
      result_map_lock_->unlock();
    }

    int CheckServer::init_meta()
    {
      int ret = TFS_SUCCESS;

       // create log dir if not exist
      if (0 != access(meta_dir_.c_str(), F_OK))
      {
        CFileUtil::mkdirs(const_cast<char*>(meta_dir_.c_str()));
      }

      string meta_path = meta_dir_ + "cs.meta";
      meta_fd_ = open(meta_path.c_str(), O_CREAT | O_RDWR, 0644);
      if (meta_fd_ < 0)
      {
        TBSYS_LOG(ERROR, "open meta file %s error, ret: %d",
            meta_path.c_str(), -errno);
        ret = TFS_ERROR;
      }
      else
      {
        // maybe an empty file, but it doesn't matter
        int rlen = read(meta_fd_, &last_check_time_, sizeof(uint32_t));
        if (rlen < 0)
        {
          TBSYS_LOG(ERROR, "read mete file %s error", meta_path.c_str());
          ret = TFS_ERROR;
        }
      }

      return ret;
    }

    int CheckServer::update_meta()
    {
      pwrite(meta_fd_, &last_check_time_, sizeof(uint32_t),  0);
      fsync(meta_fd_);
      return TFS_SUCCESS;
    }

    int CheckServer::init_log(const int index)
    {
      // create log dir if not exist
      if (0 != access(log_dir_.c_str(), F_OK))
      {
        CFileUtil::mkdirs(const_cast<char*>(log_dir_.c_str()));
      }

      int log_num = TBSYS_CONFIG.getInt(CONF_SN_PUBLIC, CONF_LOG_NUM, 16);
      int log_size = TBSYS_CONFIG.getInt(CONF_SN_PUBLIC, CONF_LOG_SIZE, 1024 * 1024 * 1024);
      string log_level = TBSYS_CONFIG.getString(CONF_SN_PUBLIC, CONF_LOG_LEVEL, "info");
      string log_file;
      stringstream tmp_stream;
      tmp_stream << log_dir_ + "checkserver_" << index << ".log";
      tmp_stream >> log_file;

      TBSYS_LOGGER.setLogLevel(log_level.c_str());
      TBSYS_LOGGER.setFileName(log_file.c_str());
      TBSYS_LOGGER.setMaxFileSize(log_size);
      TBSYS_LOGGER.setMaxFileIndex(log_num);

      return TFS_SUCCESS;
    }

    int CheckServer::init_pid(const int index)
    {
      int ret = TFS_SUCCESS;

      // create log dir if not exist
      if (0 != access(log_dir_.c_str(), F_OK))
      {
        CFileUtil::mkdirs(const_cast<char*>(log_dir_.c_str()));
      }

      string pid_file;
      stringstream tmp_stream;
      tmp_stream << log_dir_ + "checkserver_" << index << ".pid";
      tmp_stream >> pid_file;

      if (0 != CProcess::existPid(pid_file.c_str()))
      {
        TBSYS_LOG(ERROR, "checkserver %d already running.", index);
        ret = TFS_ERROR;
      }
      else
      {
        CProcess::writePidFile(pid_file.c_str());
      }
      return ret;
    }

    int CheckServer::open_file(const uint32_t check_time)
    {
      int ret = TFS_SUCCESS;
      string m_blk, s_blk;
      char time_str[64];
      CTimeUtil::timeToStr(check_time, time_str);
      stringstream tmp_stream;
      tmp_stream << meta_dir_ << "sync_to_slave.blk." << time_str;
      tmp_stream >> m_blk;
      tmp_stream.clear();
      tmp_stream << meta_dir_ << "sync_to_master.blk." << time_str;
      tmp_stream >> s_blk;
      tmp_stream.clear();

      master_fp_ = fopen(m_blk.c_str(), "w+");
      if (NULL == master_fp_)
      {
        ret = TFS_ERROR;
        TBSYS_LOG(ERROR, "open %s error, ret: %d", m_blk.c_str(), ret);
      }
      else
      {
        slave_fp_ = fopen(s_blk.c_str(), "w+");
       if (NULL == slave_fp_)
       {
        ret = TFS_ERROR;
        TBSYS_LOG(ERROR, "open %s error, ret: %d", s_blk.c_str(), ret);
       }
      }

      return ret;
    }

    int CheckServer::close_file()
    {
      if (NULL != master_fp_)
      {
        fclose(master_fp_);
      }
      if (NULL != slave_fp_)
      {
        fclose(slave_fp_);
      }
      return TFS_SUCCESS;
    }

    int CheckServer::init(const char* config_file, const int index)
    {
      // load public config
      int ret = TBSYS_CONFIG.load(config_file);
      if (TFS_SUCCESS == ret)
      {
        string work_dir = TBSYS_CONFIG.getString(CONF_SN_PUBLIC, CONF_WORK_DIR, "./");
        stringstream tmp_stream;
        tmp_stream << work_dir << "/logs/";
        tmp_stream >> log_dir_;
        tmp_stream.clear();
        tmp_stream << work_dir << "/checkserver_" << index << "/";
        tmp_stream >> meta_dir_;
        tmp_stream.clear();

        // init pid
        ret = init_pid(index);
        if (TFS_SUCCESS == ret)
        {
          // init log
          ret = init_log(index);
        }

        if (TFS_SUCCESS == ret)
        {
          // load meta
          ret = init_meta();
        }

        if (TFS_SUCCESS == ret)
        {
          // init log issues
          // checkserver's config
          ret = SYSPARAM_CHECKSERVER.initialize(config_file);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "init config item fail.");
          }
          else
          {
            master_ns_id_ = SYSPARAM_CHECKSERVER.master_ns_id_;
            slave_ns_id_ = SYSPARAM_CHECKSERVER.slave_ns_id_;
            thread_count_ = SYSPARAM_CHECKSERVER.thread_count_;
            check_interval_ = SYSPARAM_CHECKSERVER.check_interval_ * 60;

            ret = ServerHelper::get_instance()->init();
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "init server helper error, ret: %d", ret);
            }
          }
        }
      }
      return ret;
    }

    void CheckServer::run_check()
    {
      while (false == stop_)
      {
        check_logic_cluster();
        sleep(check_interval_);
      }
    }

    int CheckServer::check_logic_cluster()
    {
      int ret = 0;
      uint32_t check_time = time(NULL);
      CheckBlockInfoMap master_check_result;
      CheckBlockInfoMap slave_check_result;

      TBSYS_LOG(DEBUG, "[check cluster begin]");

      ret = open_file(check_time);
      if (TFS_SUCCESS == ret)
      {
        // check master
        if (0 != master_ns_id_)
        {
          ret = check_physical_cluster(master_ns_id_, check_time, master_check_result);
          // seletable
          if (TFS_SUCCESS == ret)
          {
            diff_blocks_in_cluster(master_ns_id_, master_check_result);
          }
        }

        // check slave
        if (0 != slave_ns_id_)
        {
          ret = check_physical_cluster(slave_ns_id_, check_time, slave_check_result);
          // seletable
          if (TFS_SUCCESS == ret)
          {
            diff_blocks_in_cluster(slave_ns_id_, slave_check_result);
          }
        }

        // diff master & slave
        if (TFS_SUCCESS == ret && 0 != master_ns_id_ && 0 != slave_ns_id_)
        {
          common::VUINT recheck_list;
          diff_blocks_between_cluster(master_ns_id_, master_check_result,
              slave_ns_id_, slave_check_result, recheck_list);
          recheck_block(recheck_list);
        }
      }

      TBSYS_LOG(DEBUG, "[check cluster end]");

      // update and save last check time
      last_check_time_ = check_time;
      update_meta();
      close_file();

      return TFS_SUCCESS;
    }

    int CheckServer::check_physical_cluster(const uint64_t ns_id, const uint32_t check_time,
        CheckBlockInfoMap& cluster_result)
    {
      int ret = TFS_SUCCESS;
      VUINT64 ds_list;
      ret = ServerHelper::get_instance()->get_ds_list(ns_id, ds_list);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "get dataserver list error, ret: %d", ret);
      }
      else
      {
        tbutil::Mutex result_map_lock;
        CheckThreadPtr* work_threads = new CheckThreadPtr[thread_count_];
        for (int i = 0; i < thread_count_; i++)
        {
          work_threads[i] = new CheckThread(&cluster_result, &result_map_lock);
          work_threads[i]->set_check_time(check_time);
          work_threads[i]->set_last_check_time(last_check_time_);
        }

        // dispatch task to multi thread
        for (uint32_t i = 0; i < ds_list.size(); i++)
        {
          int idx = i % thread_count_;
          work_threads[idx]->add_ds(ds_list[i]);
        }

        for (int i = 0; i < thread_count_; i++)
        {
          try
          {
            work_threads[i]->start();
          }
          catch (exception e)
          {
            TBSYS_LOG(ERROR, "start thread exception: %s", e.what());
          }
        }

        for (int i = 0; i < thread_count_; i++)
        {
          try
          {
            work_threads[i]->join();
          }
          catch (exception e)
          {
            TBSYS_LOG(ERROR, "join thread exception: %s", e.what());
          }
        }

        tbsys::gDeleteA(work_threads);
      }
      return TFS_SUCCESS;
    }

    int CheckServer::recheck_block(const VUINT& recheck_block)
    {
      int ret = TFS_SUCCESS;
      for (uint32_t i = 0; i < recheck_block.size(); i++)
      {
        CheckBlockInfoVec check_result;
        CheckBlockInfo info;
        ret = ServerHelper::get_instance()->check_block(master_ns_id_,
            recheck_block[i], info);
        if (TFS_SUCCESS == ret)
        {
          check_result.push_back(info);
          ret = ServerHelper::get_instance()->check_block(slave_ns_id_,
              recheck_block[i], info);
          if (TFS_SUCCESS == ret)
          {
            check_result.push_back(info);
            diff_logic_block(master_ns_id_, check_result,
                false, slave_ns_id_);
          }
        }
      }
      return TFS_SUCCESS;
    }

    void CheckServer::diff_blocks_in_cluster(const uint64_t ns_id, const CheckBlockInfoMap& block_result)
    {
      (void)ns_id;
      CheckBlockInfoMapConstIter iter = block_result.begin();
      for ( ; iter != block_result.end(); iter++)
      {
        diff_logic_block(ns_id, iter->second);
      }
    }

    void CheckServer::diff_logic_block(const uint64_t master_ns_id, const CheckBlockInfoVec& block_result,
        const bool in_cluster, const uint64_t slave_ns_id)
    {
      bool same_flag = true;
      uint32_t block_id = block_result.begin()->block_id_;
      std::string master_ns_ip = CNetUtil::addrToString(master_ns_id);
      std::string slave_ns_ip = CNetUtil::addrToString(slave_ns_id);
      std::string magic = "";
      if (true == in_cluster)
      {
        magic = "IN-CLUSTER: " + master_ns_ip;
      }
      else
      {
        magic = "BE-CLUSTER: " + master_ns_ip + " " +  slave_ns_ip;
      }

      // ignore one replicate and error sutuation
      if (block_result.size() > 1)
      {
        CheckBlockInfoVecConstIter first = block_result.begin();
        CheckBlockInfoVecConstIter iter = block_result.begin();
        for ( ; iter != block_result.end(); iter++)
        {
          TBSYS_LOG(DEBUG, "block_id: %u, file_count: %u, total_size: %u",
              magic.c_str(), iter->block_id_, iter->file_count_, iter->total_size_);
          if (iter != block_result.begin())
          {
            if( (first->file_count_ != iter->file_count_) ||
                (first->total_size_ != iter->total_size_))
            {
              same_flag = false;
            }
          }
        }

        if (true == same_flag)
        {
          TBSYS_LOG(INFO, "%s block_id: %u, SAME",
              magic.c_str(), block_id);
        }
        else
        {
          TBSYS_LOG(WARN, "%s block_id: %u, NOTSAME",
              magic.c_str(), block_id);

          if (false == in_cluster)
          {
            fprintf(master_fp_, "%u\n", block_id);
          }
        }
      }
    }

    void CheckServer::diff_blocks_between_cluster(
        const uint64_t master_ns_id, CheckBlockInfoMap& master_result,
        const uint64_t slave_ns_id, CheckBlockInfoMap& slave_result,
        common::VUINT& recheck_list)
    {
      std::string master_ns_ip = CNetUtil::addrToString(master_ns_id);
      std::string slave_ns_ip = CNetUtil::addrToString(slave_ns_id);
      std::string magic = "BE-CLUSTER: " + master_ns_ip + " " +  slave_ns_ip;

      CheckBlockInfoMapIter iter = master_result.begin();
      for ( ; iter != master_result.end(); iter++)
      {
        CheckBlockInfo& m_result = *(iter->second.begin());
        CheckBlockInfoMapIter target = slave_result.find(m_result.block_id_);
        if (target == slave_result.end())
        {
          TBSYS_LOG(WARN, "%s block_id: %u, NOT-IN-SLAVE",
              magic.c_str(), m_result.block_id_);
          fprintf(master_fp_, "%u\n", m_result.block_id_);
        }
        else
        {
          CheckBlockInfo& s_result = *(target->second.begin());
          if ((m_result.file_count_ == s_result.file_count_) &&
              (m_result.total_size_ == s_result.total_size_))
          {
            TBSYS_LOG(INFO, "%s block_id: %u, SAME",
                magic.c_str(), m_result.block_id_);
          }
          else if(m_result.file_count_ != s_result.file_count_) // compact ??
          {
            TBSYS_LOG(DEBUG, "[BE-CLUSTER] block_id: %u,  add to recheck list",
                m_result.block_id_);
            recheck_list.push_back(m_result.block_id_);
          }
          else
          {
            TBSYS_LOG(WARN, "[%s block_id: %u, NOTSAME",
                magic.c_str(), m_result.block_id_);
          }
          slave_result.erase(target);
        }
      }

      // not exist in master
      iter = slave_result.begin();
      for ( ; iter != slave_result.end(); iter++)
      {
        TBSYS_LOG(WARN, "%s block_id: %u, NOT-IN-MASTER",
              magic.c_str(), iter->first);
        fprintf(slave_fp_, "%u\n", iter->first);
      }
    }
  }
}

using namespace tfs::checkserver;

void usage(const char* app_name)
{
  fprintf(stderr, "Usage: %s -f -i [-d] [-h]\n", app_name);
  fprintf(stderr, "       -f config file path\n");
  fprintf(stderr, "       -f cluster index\n");
  fprintf(stderr, "       -d daemonize\n");
  fprintf(stderr, "       -h help\n");
  exit(TFS_ERROR);
}

int main(int argc, char** argv)
{
  int ret = TFS_SUCCESS;
  std::string config_file;
  bool daemon_flag = false;
  int index = 0;

  // analyze arguments
  int ch = 0;
  while ((ch = getopt(argc, argv, "i:f:dh")) != EOF)
  {
    switch (ch)
    {
      case 'i':
        index = atoi(optarg);
        break;
      case 'f':
        config_file = optarg;
        break;
      case 'd':
        daemon_flag = true;
        break;
      case 'h':
      default:
        usage(argv[0]);
    }
  }

  if (0 == config_file.length() || 0 >= index)
  {
    usage(argv[0]);
  }

  if (true == daemon_flag)
  {
      daemon(1, 1);  // will close all the fd
  }

  // do init after daemon
  CheckServer checkserver;
  ret = checkserver.init(config_file.c_str(), index);
  if (TFS_SUCCESS == ret)
  {
    checkserver.run_check();
  }

  return 0;
}


