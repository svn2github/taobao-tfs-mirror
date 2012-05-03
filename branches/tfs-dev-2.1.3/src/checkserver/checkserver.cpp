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
 *   linqing <linqing.zyd@taobao.com>
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
        ret = ServerHelper::check_ds(ds_list_[i],
          check_time_, last_check_time_, check_ds_result);
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
        char tmp[MAX_FILE_NAME_LEN];
        if (meta_dir_.length() >= (unsigned)MAX_FILE_NAME_LEN)
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "meta directory length exceed.");
        }
        else
        {
          snprintf(tmp, MAX_FILE_NAME_LEN, "%s", meta_dir_.c_str());
          CFileUtil::mkdirs(tmp);
        }
      }

      if (TFS_SUCCESS == ret)
      {
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
      }

      return ret;
    }

    void CheckServer::update_meta()
    {
      // it doesn't matter if fail
      if (meta_fd_ >= 0)
      {
        pwrite(meta_fd_, &last_check_time_, sizeof(uint32_t),  0);
        fsync(meta_fd_);
      }
    }

    int CheckServer::init_log(const int index)
    {
      int ret = TFS_SUCCESS;

      // create log dir if not exist
      if (0 != access(log_dir_.c_str(), F_OK))
      {
        if (log_dir_.length() >= (unsigned)MAX_FILE_NAME_LEN)
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "log directory %s length exceed.", log_dir_.c_str());
        }
        else
        {
          char tmp[MAX_FILE_NAME_LEN];
          snprintf(tmp, MAX_FILE_NAME_LEN, "%s", log_dir_.c_str());
          // don't check return val, tbsys bug with trailing slash
          CFileUtil::mkdirs(tmp);
        }
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

      return ret;
    }

    int CheckServer::init_pid(const int index)
    {
      int ret = TFS_SUCCESS;

      // create log dir if not exist
      if (0 != access(log_dir_.c_str(), F_OK))
      {
        if (log_dir_.length() >= (unsigned)MAX_FILE_NAME_LEN)
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "log directory %s length exceed.", log_dir_.c_str());
        }
        else
        {
          char tmp[MAX_FILE_NAME_LEN];
          snprintf(tmp, MAX_FILE_NAME_LEN, "%s", log_dir_.c_str());
          CFileUtil::mkdirs(tmp);
        }
      }

      if (TFS_SUCCESS == ret)
      {
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

      if (NULL == master_fp_)
      {
        master_fp_ = fopen(m_blk.c_str(), "w+");
        if (NULL == master_fp_)
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "open %s error, ret: %d", m_blk.c_str(), ret);
        }
      }

      if (ret == TFS_SUCCESS && NULL == slave_fp_)
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

    void CheckServer::close_file()
    {
      if (NULL != master_fp_)
      {
        fclose(master_fp_);
        master_fp_ = NULL;
      }
      if (NULL != slave_fp_)
      {
        fclose(slave_fp_);
        slave_fp_ = NULL;
      }
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
            overlap_check_time_ = SYSPARAM_CHECKSERVER.overlap_check_time_ * 60;
            block_stable_time_ = SYSPARAM_CHECKSERVER.block_stable_time_ * 60;
          }
        }
      }
      return ret;
    }

    void CheckServer::run_check()
    {
      while (false == stop_)
      {
        if (TFS_SUCCESS != check_logic_cluster())
        {
          TBSYS_LOG(ERROR, "check cluster fail.");
        }
        else
        {
          TBSYS_LOG(INFO, "check cluster success.");
        }
        sleep(check_interval_);
      }
    }

    int CheckServer::check_logic_cluster()
    {
      TBSYS_LOG(DEBUG, "[check cluster begin]");

      int ret = 0;
      uint32_t check_time = time(NULL);
      CheckBlockInfoMap master_check_result;
      CheckBlockInfoMap slave_check_result;

      ret = open_file(check_time);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "open file fail");
      }
      else
      {
        // check master
        if (TFS_SUCCESS == ret && 0 != master_ns_id_)
        {
          ret = check_physical_cluster(master_ns_id_, check_time, master_check_result);
        }

        // check slave
        if (TFS_SUCCESS == ret && 0 != slave_ns_id_)
        {
          ret = check_physical_cluster(slave_ns_id_, check_time, slave_check_result);
        }

        // diff master & slave
        if (TFS_SUCCESS == ret && 0 != master_ns_id_ && 0 != slave_ns_id_)
        {
          common::VUINT recheck_list;
          compare_cluster(master_check_result, slave_check_result, recheck_list);
          recheck_block(recheck_list);
        }
      }

      // update and save last check time
      last_check_time_ = check_time;
      update_meta();
      close_file();

      TBSYS_LOG(DEBUG, "[check cluster end]");
      return ret;
    }

    int CheckServer::check_physical_cluster(const uint64_t ns_id, const uint32_t check_time,
        CheckBlockInfoMap& cluster_result)
    {
      int ret = TFS_SUCCESS;
      VUINT64 ds_list;
      ret = ServerHelper::get_ds_list(ns_id, ds_list);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "get dataserver list error, ret: %d", ret);
      }
      else
      {
        // compute check range for ds
        uint32_t low_bound = 0;
        uint32_t high_bound = check_time - block_stable_time_;
        if (0 != last_check_time_)
        {
          low_bound = last_check_time_ - block_stable_time_ - overlap_check_time_;
        }

        tbutil::Mutex result_map_lock;
        CheckThreadPtr* work_threads = new CheckThreadPtr[thread_count_];
        for (int i = 0; i < thread_count_; i++)
        {
          work_threads[i] = new CheckThread(&cluster_result, &result_map_lock);
          work_threads[i]->set_check_time(high_bound);
          work_threads[i]->set_last_check_time(low_bound);
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

    return ret;
  }

  void CheckServer::recheck_block(const VUINT& recheck_block)
  {
    for (uint32_t i = 0; i < recheck_block.size(); i++)
    {
      int m_ret = TFS_SUCCESS;
      int s_ret = TFS_SUCCESS;
      CheckBlockInfo m_info;
      CheckBlockInfo s_info;
      m_ret = ServerHelper::check_block(master_ns_id_, recheck_block[i], m_info);
      s_ret = ServerHelper::check_block(slave_ns_id_, recheck_block[i], s_info);
      if (TFS_SUCCESS == m_ret && TFS_SUCCESS == s_ret)
      {
        CompType cmp = compare_block(m_info, s_info);
        if (BLK_SAME == cmp)
        {
          TBSYS_LOG(INFO, "block %u SAME", recheck_block[i]);
        }
        else if(BLK_SIZE == cmp || BLK_DIFF == cmp)
        {
          // add to sync_to_slave list, TODO: decide who is master
          fprintf(master_fp_, "%u\n", recheck_block[i]);
          TBSYS_LOG(WARN, "block %u DIFF", recheck_block[i]);
        }
      }
      else if (TFS_SUCCESS == m_ret && TFS_SUCCESS != s_ret)
      {
        // add to sync_to_slave list
        fprintf(master_fp_, "%u\n", recheck_block[i]);
        TBSYS_LOG(WARN, "block %u NOT_IN_SLAVE", recheck_block[i]);
      }
      else if (TFS_SUCCESS != m_ret && TFS_SUCCESS == s_ret)
      {
        // add to sync_to_master list
        fprintf(slave_fp_, "%u\n", recheck_block[i]);
        TBSYS_LOG(WARN, "block %u NOT_IN_MASTER", recheck_block[i]);
      }
      else
      {
        TBSYS_LOG(WARN, "block %u NOT_IN_CLUSTER", recheck_block[i]);
      }
    }
  }

  CompType CheckServer::compare_block(const CheckBlockInfo& left, const CheckBlockInfo& right)
  {
    CompType result = BLK_SAME;
    if (left.block_id_ != right.block_id_)
    {
      TBSYS_LOG(WARN, "different block id, won't compare, %d %d",
          left.block_id_, right.block_id_);
      result = BLK_ERROR;
    }
    else
    {
      TBSYS_LOG(DEBUG, "block %u, file count %u, total size %u",
          left.block_id_, left.file_count_, left.total_size_);
      TBSYS_LOG(DEBUG, "block %u, file count %u, total size %u",
          right.block_id_, right.file_count_, right.total_size_);
      if (left.file_count_ == right.file_count_ &&
          left.total_size_ == right.total_size_)
      {
        result = BLK_SAME;
      }
      else if (left.file_count_ != right.file_count_)
      {
        result = BLK_SIZE;
      }
      else
      {
        result = BLK_DIFF;
      }
    }
    return result;
  }

  void CheckServer::compare_cluster(CheckBlockInfoMap& master_result,
      CheckBlockInfoMap& slave_result, common::VUINT& recheck_list)
  {
    CheckBlockInfoMapIter iter = master_result.begin();
    for ( ; iter != master_result.end(); iter++)
    {
      // TODO, select master block
      CheckBlockInfo& m_result = *(iter->second.begin());
      CheckBlockInfoMapIter target = slave_result.find(m_result.block_id_);
      if (target == slave_result.end())
      {
        // may not in slave, recheck
        recheck_list.push_back(iter->first);
        TBSYS_LOG(DEBUG, "block %u, file count %u, total size %u",
            m_result.block_id_, m_result.file_count_, m_result.total_size_);
        TBSYS_LOG(DEBUG, "block %u may not in slave, recheck", iter->first);
      }
      else
      {
        CheckBlockInfo& s_result = *(target->second.begin());
        CompType cmp = compare_block(m_result, s_result);
        if (BLK_SAME == cmp)
        {
          // ok, same
          TBSYS_LOG(INFO, "block %u SAME", iter->first);
        }
        else if(BLK_SIZE == cmp)  // different size
        {
          // compact may happen, recheck
          recheck_list.push_back(m_result.block_id_);
          TBSYS_LOG(DEBUG, "block %u may be compacted, recheck", iter->first);
        }
        else if(BLK_DIFF == cmp)
        {
          // must not same
          TBSYS_LOG(WARN, "block %u DIFF", iter->first);
          fprintf(master_fp_, "%u\n", iter->first);
        }
        slave_result.erase(target);
      }
    }

    // may not exist in master, recheck
    iter = slave_result.begin();
    for ( ; iter != slave_result.end(); iter++)
    {
      recheck_list.push_back(iter->first);
      // debug info
      CheckBlockInfo& item = *(iter->second.begin());
      TBSYS_LOG(DEBUG, "block %u, file count %u, total size %u",
            item.block_id_, item.file_count_, item.total_size_);
      TBSYS_LOG(DEBUG, "block %u may not in master, recheck", iter->first);
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
    ret = daemon(1, 1);  // will close all the fd
    if (0 != ret)
    {
      ret = TFS_ERROR;
      TBSYS_LOG(ERROR, "daemonize checkserver error, ret = %d. ", -errno);
    }
  }

  if (TFS_SUCCESS == ret)
  {
    // init client manager
    MessageFactory factory;
    BasePacketStreamer streamer(&factory);
    ret = NewClientManager::get_instance().initialize(&factory, &streamer);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "init client manager error.");
    }
    else
    {
      CheckServer checkserver;
      ret = checkserver.init(config_file.c_str(), index);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "init check server error, ret: %d.", ret);
      }
      else
      {
        checkserver.run_check();
      }
      NewClientManager::get_instance().destroy();
    }
  }

  return 0;
}

