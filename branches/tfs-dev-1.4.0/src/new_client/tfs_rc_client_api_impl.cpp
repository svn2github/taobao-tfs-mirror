/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_client_api.cpp 49 2010-11-16 09:58:57Z zongdai@taobao.com $
 *
 * Authors:
 *    daoan<aoan@taobao.com>
 *      - initial release
 *
 */
#include "tfs_rc_client_api_impl.h"

#include "common/func.h"
#include "tfs_client_api.h"
#include "tfs_rc_helper.h"
#include "fsname.h"

#define RC_CLIENT_VERSION "rc_1.0.0_c++"
namespace
{
  const int INIT_INVALID = 0;
  const int INIT_LOGINED = 1;

  const int CLUSTER_ACCESS_TYPE_READ_ONLY = 1;
  const int CLUSTER_ACCESS_TYPE_READ_WRITE = 2;

}

namespace tfs
{
  namespace client
  {
    using namespace tfs::common;
    using namespace std;
    StatUpdateTask::StatUpdateTask(RcClientImpl& rc_client):rc_client_(rc_client)
    {
    }
    void StatUpdateTask::runTimerTask()
    {
      uint64_t rc_ip = 0;
      KeepAliveInfo ka_info;
      {
        tbsys::CThreadGuard mutex_guard(&rc_client_.mutex_);
        rc_client_.get_ka_info(ka_info);
        rc_ip = rc_client_.active_rc_ip_;
      }
      ka_info.s_stat_.cache_hit_ratio_ = TfsClient::Instance()->get_cache_hit_ratio();
      bool update_flag = false;
      BaseInfo new_base_info;
      int ret = RcHelper::keep_alive(rc_ip, ka_info, update_flag, new_base_info);
      if (TFS_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "keep alive ok");
        {
          tbsys::CThreadGuard mutex_guard(&rc_client_.mutex_);
          rc_client_.next_rc_index_ = 0;
        }
        int last_report_interval = 0;
        if (update_flag)
        {
          tbsys::CThreadGuard mutex_guard(&rc_client_.mutex_);
          last_report_interval = rc_client_.base_info_.report_interval_;
          rc_client_.base_info_ = new_base_info;
          rc_client_.calculate_ns_info(new_base_info, rc_client_.local_addr_);
        }
        if (update_flag && last_report_interval != new_base_info.report_interval_)
        {
          TBSYS_LOG(DEBUG, "reschedule update stat task :old interval is %d new is %d",
              last_report_interval, new_base_info.report_interval_);

          rc_client_.keepalive_timer_->cancel(rc_client_.stat_update_task_);
          rc_client_.keepalive_timer_->scheduleRepeated(rc_client_.stat_update_task_,
            tbutil::Time::seconds(new_base_info.report_interval_));
        }
      }
      else
      {
        TBSYS_LOG(DEBUG, "keep alive error will roll back");
        uint64_t next_rc_ip;
        tbsys::CThreadGuard mutex_guard(&rc_client_.mutex_);
        next_rc_ip = rc_client_.get_active_rc_ip(rc_client_.next_rc_index_);
        if (0 == next_rc_ip)
        {
          rc_client_.next_rc_index_ = 0;
        }
        else
        {
          rc_client_.active_rc_ip_ = next_rc_ip;
        }
        // roll back stat info;
        rc_client_.stat_ += ka_info.s_stat_;
      }
    }

    RcClientImpl::RcClientImpl()
      :need_use_unique_(false), local_addr_(0),
      init_stat_(INIT_INVALID), active_rc_ip_(0), next_rc_index_(0)
    {
      stat_update_task_ = new StatUpdateTask(*this);
      keepalive_timer_ = new tbutil::Timer();
    }

    RcClientImpl::~RcClientImpl()
    {
      keepalive_timer_->cancel(stat_update_task_);
      keepalive_timer_->destroy();
      stat_update_task_ = 0;
      keepalive_timer_ = 0;
      logout();
    }
    TfsRetType RcClientImpl::initialize(const char* str_rc_ip, const char* app_key, const char* str_app_ip,
        const int32_t cache_times, const int32_t cache_items, const char* dev_name)
    {
      uint64_t rc_ip = Func::get_host_ip(str_rc_ip);
      uint64_t app_ip = Func::str_to_addr(str_app_ip, 0);
      return initialize(rc_ip, app_key, app_ip, cache_times, cache_items, dev_name);
    }

    TfsRetType RcClientImpl::initialize(const uint64_t rc_ip, const char* app_key, const uint64_t app_ip,
        const int32_t cache_times, const int32_t cache_items, const char* dev_name)
    {
      int ret = TFS_SUCCESS;
      tbsys::CThreadGuard mutex_guard(&mutex_);
      if (INIT_INVALID != init_stat_)
      {
        TBSYS_LOG(ERROR, "initialize error stat %d", init_stat_);
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = TfsClient::Instance()->initialize(NULL, cache_times, cache_items);
      }
      TBSYS_LOG(DEBUG, "TfsClient::Instance()->initialize ret %d", ret);
      if (TFS_SUCCESS == ret)
      {
        local_addr_ = Func::get_local_addr(dev_name);
        TBSYS_LOG(DEBUG, "local_addr_ = %d", local_addr_);
        ret = login(rc_ip, app_key, app_ip);
      }
      TBSYS_LOG(DEBUG, "login ret %d", ret);
      if (TFS_SUCCESS == ret)
      {
        session_base_info_.client_version_ = RC_CLIENT_VERSION;
        session_base_info_.cache_size_ = cache_items;
        session_base_info_.cache_time_ = cache_times;
        session_base_info_.modify_time_ = tbsys::CTimeUtil::getTime();
        session_base_info_.is_logout_ = false;

        active_rc_ip_ = rc_ip;
        init_stat_ = INIT_LOGINED;

        keepalive_timer_->scheduleRepeated(stat_update_task_,
            tbutil::Time::seconds(base_info_.report_interval_));
      }
      return ret;
    }


    TfsRetType RcClientImpl::logout()
    {
      int ret = TFS_ERROR;
      size_t retry = 0;
      uint64_t rc_ip = 0;
      tbsys::CThreadGuard mutex_guard(&mutex_);
      ret = check_init_stat();
      if (TFS_SUCCESS == ret)
      {
        KeepAliveInfo ka_info;
        get_ka_info(ka_info);
        ka_info.s_base_info_.is_logout_ = true;
        while(0 != (rc_ip = get_active_rc_ip(retry)))
        {
          ret = RcHelper::logout(rc_ip, ka_info);
          if (TFS_SUCCESS == ret)
          {
            break;
          }
        }
      }
      if (TFS_SUCCESS == ret)
      {
        init_stat_ = INIT_INVALID;
      }
      return ret;
    }

    void RcClientImpl::set_wait_timeout(const int64_t timeout_ms)
    {
      TfsClient::Instance()->set_wait_timeout(timeout_ms);
      return;
    }

    void RcClientImpl::set_log_level(const char* level)
    {
      TBSYS_LOGGER.setLogLevel(level);
    }

    void RcClientImpl::set_log_file(const char* log_file)
    {
      TBSYS_LOGGER.setFileName(log_file);
    }

    int RcClientImpl::open(const char* file_name, const char* suffix, const RcClient::RC_MODE mode,
          const bool large, const char* local_key)
    {
      int ret = check_init_stat();
      if (TFS_SUCCESS == ret)
      {
        if (RcClient::CREATE == mode && need_use_unique_)
        {
          TBSYS_LOG(ERROR, "should use save_file");
          ret = -1;
        }
        else
        {
          int ns_get_index = 0;
          string ns_addr;
          do
          {
            ns_addr = get_ns_addr(file_name, mode, ns_get_index++);
            if (ns_addr.empty())
            {
              break;
            }
            ret = open(ns_addr.c_str(), file_name, suffix, mode, large, local_key);
          } while(ret <= 0);
        }
      }
      else
      {
        ret = -1;
      }
      return ret;
    }

    int RcClientImpl::open(const char* ns_addr, const char* file_name, const char* suffix, const RcClient::RC_MODE mode,
          const bool large, const char* local_key)
    {
        int flag = 0;
        int ret = -1;
        if (RcClient::CREATE == mode)
        {
          flag = common::T_WRITE;
        }
        else if (RcClient::STAT == mode)
        {
          flag = common::T_STAT;
        }
        else
        {
          flag = common::T_READ;
        }
        if (large)
        {
          flag |= common::T_LARGE;
          ret = TfsClient::Instance()->open(file_name, suffix, ns_addr, flag, local_key);
        }
        else
        {
          ret = TfsClient::Instance()->open(file_name, suffix, ns_addr, flag);
        }
        return ret;
    }
    TfsRetType RcClientImpl::close(const int fd, char* tfs_name_buff, const int32_t buff_len)
    {
      int ret = check_init_stat();
      if (TFS_SUCCESS == ret)
      {
        ret = TfsClient::Instance()->close(fd, tfs_name_buff, buff_len);
      }
      return ret;
    }

    int64_t RcClientImpl::read(const int fd, void* buf, const int64_t count)
    {
      int64_t read_count = -1;
      int ret = check_init_stat();
      if (TFS_SUCCESS == ret)
      {
        int64_t start_time = tbsys::CTimeUtil::getTime();
        read_count = TfsClient::Instance()->read(fd, buf, count);
        int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
        add_stat_info(OPER_READ, read_count, response_time, read_count >= 0);
      }
      return read_count;
    }

    int64_t RcClientImpl::readv2(const int fd, void* buf, const int64_t count, TfsFileStat* tfs_stat_buf)
    {
      int64_t read_count = -1;
      int ret = check_init_stat();
      if (TFS_SUCCESS == ret)
      {
        int64_t start_time = tbsys::CTimeUtil::getTime();
        read_count = TfsClient::Instance()->readv2(fd, buf, count, tfs_stat_buf);
        int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
        add_stat_info(OPER_READ, read_count, response_time, read_count >= 0);
      }
      return read_count;
    }
    int64_t RcClientImpl::pread(const int fd, void* buf, const int64_t count, const int64_t offset)
    {
      int64_t read_count = -1;
      int ret = check_init_stat();
      if (TFS_SUCCESS == ret)
      {
        int64_t start_time = tbsys::CTimeUtil::getTime();
        read_count = TfsClient::Instance()->pread(fd, buf, count, offset);
        int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
        add_stat_info(OPER_READ, read_count, response_time, read_count >= 0);
      }
      return read_count;
    }

    int64_t RcClientImpl::write(const int fd, const void* buf, const int64_t count)
    {
      int64_t write_count = -1;
      int ret = check_init_stat();
      if (TFS_SUCCESS == ret)
      {
        int64_t start_time = tbsys::CTimeUtil::getTime();
        write_count = TfsClient::Instance()->write(fd, buf, count);
        int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
        add_stat_info(OPER_WRITE, write_count, response_time, write_count >= 0);
      }
      return write_count;
    }
    int64_t RcClientImpl::pwrite(const int fd, const void* buf, const int64_t count, const int64_t offset)
    {
      int64_t write_count = -1;
      int ret = check_init_stat();
      if (TFS_SUCCESS == ret)
      {
        int64_t start_time = tbsys::CTimeUtil::getTime();
        write_count = TfsClient::Instance()->pwrite(fd, buf, count, offset);
        int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
        add_stat_info(OPER_WRITE, write_count, response_time, write_count >= 0);
      }
      return write_count;
    }

    int64_t RcClientImpl::lseek(const int fd, const int64_t offset, const int whence)
    {
      return TfsClient::Instance()->lseek(fd, offset, whence);
    }
    TfsRetType RcClientImpl::fstat(const int fd, common::TfsFileStat* buf)
    {
      return TfsClient::Instance()->fstat(fd, buf);
    }

    TfsRetType RcClientImpl::unlink(const char* file_name, const char* suffix, const common::TfsUnlinkType action)
    {
      int ret = check_init_stat();
      if (TFS_SUCCESS == ret)
      {
          int ns_get_index = 0;
          string ns_addr;
          do
          {
            ns_addr = get_ns_addr(file_name, RcClient::CREATE, ns_get_index++);
            if (ns_addr.empty())
            {
              break;
            }
            ret = unlink(ns_addr.c_str(), file_name, suffix, action);
          } while(TFS_SUCCESS != ret);
      }
      return ret;
    }
    TfsRetType RcClientImpl::unlink(const char* ns_addr, const char* file_name,
        const char* suffix, const common::TfsUnlinkType action)
    {
      int ret = TFS_SUCCESS;
      if (need_use_unique_)
      {
#ifdef WITH_UNIQUE_STORE
        ret = TfsClient::Instance()->init_unique_store(duplicate_server_master_.c_str(),
            duplicate_server_slave_.c_str(),
            duplicate_server_group_.c_str(),
            duplicate_server_area_, ns_addr);
        if (TFS_SUCCESS == ret)
        {
          int64_t start_time = tbsys::CTimeUtil::getTime();
          int64_t data_size = 0;
          int32_t ref_count = TfsClient::Instance()->unlink_unique(file_name, suffix, data_size, 1, ns_addr);
          int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
          add_stat_info(OPER_UNIQUE_UNLINK, data_size, response_time, ref_count >= 0);
          if (ref_count < 0)
          {
            ret = TFS_ERROR;
          }
        }
#else
        TBSYS_LOG(ERROR, "you should compile client with --enable-uniquestore");
        ret = TFS_ERROR;
#endif
      }
      else
      {
        int64_t start_time = tbsys::CTimeUtil::getTime();
        int64_t data_size = 0;
        int32_t ref_count = TfsClient::Instance()->unlink(file_name, suffix,
            ns_addr, data_size, action);
        int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
        switch (action)
        {
          case DELETE:
            break;
          case UNDELETE:
            data_size = 0 - data_size;
            break;
          default:
            data_size = 0;
            break;
        }
        add_stat_info(OPER_UNLINK, data_size, response_time, ref_count >= 0);
        if (ref_count < 0)
        {
          ret = TFS_ERROR;
        }
      }
      return ret;
    }

    int64_t RcClientImpl::save_file(const char* local_file, char* tfs_name_buff, const int32_t buff_len,
        const bool is_large_file)
    {
      int ret = check_init_stat();
      int64_t saved_size = -1;
      if (TFS_SUCCESS == ret)
      {
          int ns_get_index = 0;
          string ns_addr;
          do
          {
            ns_addr = get_ns_addr(NULL, RcClient::CREATE, ns_get_index++);
            if (ns_addr.empty())
            {
              break;
            }
            saved_size = save_file(ns_addr.c_str(), local_file, tfs_name_buff, buff_len, is_large_file);
          } while(saved_size < 0);
      }
      return saved_size;
    }
    int64_t RcClientImpl::save_file(const char* source_data, const int32_t data_len,
        char* tfs_name_buff, const int32_t buff_len)
    {
      int ret = check_init_stat();
      int64_t saved_size = -1;
      if (TFS_SUCCESS == ret)
      {
          int ns_get_index = 0;
          string ns_addr;
          do
          {
            ns_addr = get_ns_addr(NULL, RcClient::CREATE, ns_get_index++);
            if (ns_addr.empty())
            {
              break;
            }
            saved_size = save_file(ns_addr.c_str(), source_data, data_len,
                tfs_name_buff, buff_len);
          } while(saved_size < 0);
      }
      return saved_size;
    }
    int64_t RcClientImpl::save_file(const char* ns_addr, const char* local_file, char* tfs_name_buff,
        const int32_t buff_len, const bool is_large_file)
    {
      int flag = T_DEFAULT;
      if (is_large_file)
      {
        flag = T_LARGE;
      }
      int64_t saved_size = -1;
      if (need_use_unique_)
      {
#ifdef WITH_UNIQUE_STORE
        int ret = TfsClient::Instance()->init_unique_store(duplicate_server_master_.c_str(),
            duplicate_server_slave_.c_str(),
            duplicate_server_group_.c_str(),
            duplicate_server_area_, ns_addr);
        if (TFS_SUCCESS == ret)
        {
          int64_t start_time = tbsys::CTimeUtil::getTime();
          saved_size = TfsClient::Instance()->save_unique(local_file,
              NULL, NULL, tfs_name_buff, buff_len, ns_addr);
          int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
          add_stat_info(OPER_UNIQUE_WRITE, saved_size, response_time, saved_size >= 0);
        }
#else
        TBSYS_LOG(ERROR, "you should compile client with --enable-uniquestore");
        saved_size = -1;
#endif
      }
      else
      {
        int64_t start_time = tbsys::CTimeUtil::getTime();
        saved_size = TfsClient::Instance()->save_file(local_file,
              NULL, NULL, tfs_name_buff, buff_len, ns_addr, flag);
        int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
        add_stat_info(OPER_WRITE, saved_size, response_time, saved_size >= 0);
      }
      return saved_size;
    }
    int64_t RcClientImpl::save_file(const char* ns_addr, const char* source_data, const int32_t data_len,
        char* tfs_name_buff, const int32_t buff_len)
    {
      int64_t saved_size = -1;
      if (need_use_unique_)
      {
#ifdef WITH_UNIQUE_STORE
        int ret = TfsClient::Instance()->init_unique_store(duplicate_server_master_.c_str(),
            duplicate_server_slave_.c_str(),
            duplicate_server_group_.c_str(),
            duplicate_server_area_, ns_addr);
        if (TFS_SUCCESS == ret)
        {
          int64_t start_time = tbsys::CTimeUtil::getTime();
          saved_size = TfsClient::Instance()->save_unique(source_data, data_len,
              NULL, NULL, tfs_name_buff, buff_len, ns_addr);
          int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
          add_stat_info(OPER_UNIQUE_WRITE, saved_size, response_time, saved_size >= 0);
        }
#else
        TBSYS_LOG(ERROR, "you should compile client with --enable-uniquestore");
#endif
      }
      else
      {
        int64_t start_time = tbsys::CTimeUtil::getTime();
        saved_size = TfsClient::Instance()->save_file(source_data, data_len,
              NULL, NULL, tfs_name_buff, buff_len, ns_addr);
        int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
        add_stat_info(OPER_WRITE, saved_size, response_time, saved_size >= 0);
      }
      return saved_size;
    }

    TfsRetType RcClientImpl::login(const uint64_t rc_ip, const char* app_key, const uint64_t app_ip)
    {
      int ret = TFS_SUCCESS;
      if (TFS_SUCCESS == (ret = RcHelper::login(rc_ip, app_key, app_ip,
              session_base_info_.session_id_, base_info_)))
      {
        calculate_ns_info(base_info_, local_addr_);
      }
      return ret;
    }

    TfsRetType RcClientImpl::check_init_stat() const
    {
      int ret = TFS_SUCCESS;
      if (init_stat_ != INIT_LOGINED)
      {
        TBSYS_LOG(ERROR, "not inited");
        ret = TFS_ERROR;
      }
      return ret;
    }
    uint64_t RcClientImpl::get_active_rc_ip(size_t& retry_index) const
    {
      uint64_t active_rc_ip = 0;
      if (retry_index <= base_info_.rc_server_infos_.size())
      {
        if (0 == retry_index)
        {
          active_rc_ip = active_rc_ip_;
        }
        else
        {
          active_rc_ip = base_info_.rc_server_infos_[retry_index - 1];
        }
        retry_index++;
      }
      return active_rc_ip;
    }
    void RcClientImpl::get_ka_info(common::KeepAliveInfo& kainfo)
    {
      kainfo.last_report_time_ = tbsys::CTimeUtil::getTime();
      kainfo.s_base_info_ = session_base_info_;
      kainfo.s_stat_.app_oper_info_.swap(stat_.app_oper_info_);
    }

    void RcClientImpl::add_stat_info(const OperType& oper_type, const int64_t size,
        const int64_t response_time, const bool is_success)
    {
      AppOperInfo appinfo;
      appinfo.oper_type_ = oper_type;
      appinfo.oper_times_ = 1;
      appinfo.oper_rt_ =  response_time;
      if (is_success)
      {
        appinfo.oper_size_ = size;
        appinfo.oper_succ_= 1;
      }
      tbsys::CThreadGuard mutex_guard(&mutex_);
      stat_.app_oper_info_[oper_type] += appinfo;
    }

    string RcClientImpl::get_ns_addr(const char* file_name, const RcClient::RC_MODE mode, const int index) const
    {
      string ns_addr;
      int32_t cluster_id = get_cluster_id(file_name);
      if (index > 1 || (RcClient::CREATE != mode && 0 == cluster_id))
      {
        //null ;
      }
      else
      {
        tbsys::CThreadGuard mutex_guard(&mutex_);
        if (RcClient::CREATE == mode)
        {
          ns_addr = write_ns_[index];
        }
        else
        {
          ClusterNsType::const_iterator it = choice[index].find(cluster_id);
          if (it != choice[index].end())
          {
            ns_addr = it->second;
          }
        }
      }
      if (ns_addr.empty())
      {
        //TBSYS_LOG(INFO, "can not get ns_addr maybe you do not have access permition");
      }
      return ns_addr;
    }

    int32_t RcClientImpl::get_cluster_id(const char* file_name)
    {
      FSName fs(file_name);
      return fs.get_cluster_id();
    }

    void RcClientImpl::calculate_ns_info(const common::BaseInfo& base_info, const uint32_t local_addr)
    {
      write_ns_[0].clear();
      write_ns_[1].clear();
      need_use_unique_ = false;
      choice[0].clear();
      choice[1].clear();
      std::vector<ClusterRackData>::const_iterator it = base_info.cluster_infos_.begin();
      std::vector<ClusterData>::const_iterator cluster_data_it;
      for (; it != base_info.cluster_infos_.end(); it++)
      {
        bool can_write = false;
        cluster_data_it = it->cluster_data_.begin();
        for (; cluster_data_it != it->cluster_data_.end(); cluster_data_it++)
        {
          assert(0 != cluster_data_it->cluster_stat_);
          //rc server should not give the cluster which stat is 0
          assert(0 != cluster_data_it->access_type_);
          if (CLUSTER_ACCESS_TYPE_READ_WRITE == cluster_data_it->access_type_)
          {
            can_write = true;
          }
          int32_t cluster_id = 0;
          bool is_master = false;
          parse_cluster_id(cluster_data_it->cluster_id_, cluster_id, is_master);

          if (CLUSTER_ACCESS_TYPE_READ_WRITE == cluster_data_it->access_type_)
          {
            if (write_ns_[0].empty())
            {
              write_ns_[0] = cluster_data_it->ns_vip_;
            }
            else
            {
              //calculate who will be the first choice;
              uint32_t distance1 = calculate_distance(write_ns_[0], local_addr);
              uint32_t distance2 = calculate_distance(cluster_data_it->ns_vip_, local_addr);
              if (distance1 > distance2)
              {
                write_ns_[1] = write_ns_[0];
                write_ns_[0] = cluster_data_it->ns_vip_;
              }
              else
              {
                write_ns_[1] = cluster_data_it->ns_vip_;
              }

            }
          }
          if (choice[0][cluster_id].empty())
          {
            choice[0][cluster_id] = cluster_data_it->ns_vip_;
          }
          else
          {
            //calculate who will be the first choice;
            uint32_t distance1 = calculate_distance(choice[0][cluster_id], local_addr);
            uint32_t distance2 = calculate_distance(cluster_data_it->ns_vip_, local_addr);
            if (distance1 > distance2)
            {
              choice[1][cluster_id] = choice[0][cluster_id];
              choice[0][cluster_id] = cluster_data_it->ns_vip_;
            }
            else
            {
              choice[1][cluster_id] = cluster_data_it->ns_vip_;
            }

          }

        }
        if (can_write)
        {
          need_use_unique_ = it->need_duplicate_;
          if (need_use_unique_)
          {
            parse_duplicate_info(it->dupliate_server_addr_);
          }
        }
      }
      TBSYS_LOG(INFO, "need_use_unique_:%d local_addr_:%u", need_use_unique_, local_addr);
      for (int i = 0; i < 2; i++)
      {
        TBSYS_LOG(INFO, "%d write_ns %s", i, write_ns_[i].c_str());
        ClusterNsType::const_iterator it = choice[i].begin();
        for (; it != choice[i].end(); it++)
        {
          TBSYS_LOG(INFO, "cluster_id :%d ns :%s", it->first, it->second.c_str());
        }
      }
      return;
    }

    void RcClientImpl::parse_cluster_id(const std::string& cluster_id_str, int32_t& id, bool& is_master)
    {
      //cluster_id_str will be like 'T1M'  'T1B'
      id = 0;
      is_master = false;
      if (cluster_id_str.length() < 3)
      {
        TBSYS_LOG(ERROR, "cluster_id_str error %s", cluster_id_str.c_str());
      }
      else
      {
        id = cluster_id_str[1] - '0';
        is_master = (cluster_id_str[2] == 'M' || cluster_id_str[2] == 'm');
      }
    }
    uint32_t RcClientImpl::calculate_distance(const std::string& ip_str, const uint32_t addr)
    {
      uint32_t ip1 = Func::get_addr(ip_str.c_str());
      uint32_t ip2 = addr;
      uint32_t mask = 0xff;
      uint32_t n1 = 0;
      uint32_t n2 = 0;
      for (int i = 0; i < 4; i++)
      {
        n1 <<=  8;
        n2 <<= 8;
        n1 |= ip1 & mask;
        n2 |= ip2 & mask;
        ip1 >>= 8;
        ip2 >>= 8;
      }
      uint32_t result = 0;
      if (n1 > n2)
      {
        result = n1 - n2;
      }
      else
      {
        result = n2 - n1;
      }

      return result;
    }
    void RcClientImpl::parse_duplicate_info(const std::string& duplicate_info)
    {
      char tmp[512];
      snprintf(tmp, 512, "%s", duplicate_info.c_str());
      vector<char*> list;
      tbsys::CStringUtil::split(tmp, ";", list);
      if (list.size() < 4)
      {
        TBSYS_LOG(ERROR, "parse_duplicate_info error: %s", duplicate_info.c_str());
      }
      else
      {
        duplicate_server_master_ = list[0];
        duplicate_server_slave_ = list[1];
        duplicate_server_group_ = list[2];
        duplicate_server_area_ = atoi(list[3]);
      }
      TBSYS_LOG(DEBUG, "master = %s slave = %s group= %s area = %d",
                duplicate_server_master_.c_str(), duplicate_server_slave_.c_str(),
                duplicate_server_group_.c_str(), duplicate_server_area_);
    }
  }
}
