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
 *    daoan<daoan@taobao.com>
 *      - initial release
 *
 */
#include "tfs_rc_client_api_impl.h"

#include "common/func.h"
#include "common/session_util.h"
#include "tfs_client_api.h"
#include "tfs_rc_helper.h"
#include "fsname.h"
#include "tfs_meta_client_api.h"

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
      init_stat_(INIT_INVALID), active_rc_ip_(0), next_rc_index_(0), app_id_(0), my_fd_(1)
    {
      stat_update_task_ = new StatUpdateTask(*this);
      keepalive_timer_ = new tbutil::Timer();
      name_meta_client_ = new NameMetaClient();
    }

    RcClientImpl::~RcClientImpl()
    {
      keepalive_timer_->cancel(stat_update_task_);
      keepalive_timer_->destroy();
      stat_update_task_ = 0;
      keepalive_timer_ = 0;
      delete name_meta_client_;
      logout();
    }
    TfsRetType RcClientImpl::initialize(const char* str_rc_ip, const char* app_key, const char* str_app_ip,
        const int32_t cache_times, const int32_t cache_items, const char* dev_name, const char* rs_addr)
    {
      if (str_rc_ip == NULL || app_key == NULL)
      {
        TBSYS_LOG(WARN, "input parameter is invalid. rc_ip: %s, app_key: %s, app_ip: %s",
            str_rc_ip == NULL ? "null":str_rc_ip,
            app_key == NULL ? "null":app_key,
            str_app_ip == NULL ? "null":str_app_ip);
        return TFS_ERROR;
      }
      if (cache_times < 0 || cache_items < 0)
      {
        TBSYS_LOG(WARN, "invalid cache setting. cache_times: %d, cache_items: %d", cache_times, cache_items);
        return TFS_ERROR;
      }
      uint64_t rc_ip = Func::get_host_ip(str_rc_ip);
      uint64_t app_ip = 0;
      if (NULL != str_app_ip)
      {
        app_ip = Func::str_to_addr(str_app_ip, 0);
      }
      return initialize(rc_ip, app_key, app_ip, cache_times, cache_items, dev_name, rs_addr);
    }

    TfsRetType RcClientImpl::initialize(const uint64_t rc_ip, const char* app_key, const uint64_t app_ip,
        const int32_t cache_times, const int32_t cache_items, const char* dev_name, const char* rs_addr)
    {
      int ret = TFS_SUCCESS;
      tbsys::CThreadGuard mutex_guard(&mutex_);
      if (init_stat_ != INIT_LOGINED)
      {
        if (TFS_SUCCESS == ret)
        {
          ret = TfsClient::Instance()->initialize(NULL, cache_times, cache_items);
        }
        TBSYS_LOG(DEBUG, "TfsClient::Instance()->initialize ret %d", ret);
        if (TFS_SUCCESS == ret)
        {
          if (app_ip != 0)
          {
            local_addr_ = app_ip & 0xffffffff;
          }
          else
          {
            local_addr_ = Func::get_local_addr(dev_name);
          }
          TBSYS_LOG(DEBUG, "local_addr_ = %s", tbsys::CNetUtil::addrToString(local_addr_).c_str());
          ret = login(rc_ip, app_key, local_addr_);
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
          if (NULL != rs_addr)
          {
            name_meta_client_->initialize(rs_addr);
          }
          else
          {
            name_meta_client_->initialize(base_info_.meta_root_server_);
          }
          keepalive_timer_->scheduleRepeated(stat_update_task_,
              tbutil::Time::seconds(base_info_.report_interval_));
        }
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
      int fd = -1;
      int ret = check_init_stat();
      if (TFS_SUCCESS == ret)
      {
        int flag = -1;
        ret = (RcClient::CREATE == mode && need_use_unique_) ? TFS_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "should use save_file");
        }
        else//check mode
        {
          flag = -1;
          if (RcClient::CREATE == mode)
          {
            flag = common::T_WRITE;
          }
          else if (RcClient::READ == mode)
          {
            flag = common::T_READ | common::T_STAT;
          }

        }
        ret = flag != -1 ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "mode %d not support", mode);
        }
        else
        {
          int ns_get_index = 0;
          int raw_tfs_fd = -1;
          string ns_addr;
          do
          {
            ns_addr = get_ns_addr(file_name, mode, ns_get_index++);
            if (ns_addr.empty())
            {
              break;
            }
            raw_tfs_fd = open(ns_addr.c_str(), file_name, suffix, mode, large, local_key);
          } while(raw_tfs_fd < 0);
          if (raw_tfs_fd >= 0)
          {
            fdInfo fd_info(raw_tfs_fd, 0, 0);
            fd = gen_fdinfo(fd_info);
            if (fd < 0)
            {
              TfsClient::Instance()->close(raw_tfs_fd, NULL, 0);
            }
          }
        }
      }
      return fd;
    }

    TfsRetType RcClientImpl::close(const int fd, char* tfs_name_buff, const int32_t buff_len)
    {
      int ret = check_init_stat();
      if (TFS_SUCCESS == ret)
      {
        fdInfo fd_info;
        remove_fdinfo(fd, fd_info);
        if (fd_info.raw_tfs_fd_ >= 0)
        {
          ret = TfsClient::Instance()->close(fd_info.raw_tfs_fd_, tfs_name_buff, buff_len);
        }
      }
      return ret;
    }

    int64_t RcClientImpl::read(const int fd, void* buf, const int64_t count)
    {
      int64_t read_count = -1;
      int ret = check_init_stat();
      if (TFS_SUCCESS == ret)
      {
        fdInfo fd_info;
        ret = get_fdinfo(fd, fd_info);
        if (TFS_SUCCESS == ret)
        {
          if (fd_info.raw_tfs_fd_ >= 0)
          {
            int64_t start_time = tbsys::CTimeUtil::getTime();
            read_count = TfsClient::Instance()->read(fd_info.raw_tfs_fd_, buf, count);
            int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
            add_stat_info(OPER_READ, read_count, response_time, read_count >= 0);
          }
          else
          {
            TBSYS_LOG(ERROR, "name meta file not support read");

          }
        }
      }
      return read_count;
    }

    int64_t RcClientImpl::readv2(const int fd, void* buf, const int64_t count, TfsFileStat* tfs_stat_buf)
    {
      int64_t read_count = -1;
      int ret = check_init_stat();
      if (TFS_SUCCESS == ret)
      {
        fdInfo fd_info;
        ret = get_fdinfo(fd, fd_info);
        if (TFS_SUCCESS == ret)
        {
          if (fd_info.raw_tfs_fd_ >= 0)
          {
            int64_t start_time = tbsys::CTimeUtil::getTime();
            read_count = TfsClient::Instance()->readv2(fd_info.raw_tfs_fd_, buf, count, tfs_stat_buf);
            int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
            add_stat_info(OPER_READ, read_count, response_time, read_count >= 0);
          }
          else
          {
            TBSYS_LOG(ERROR, "name meta file not support readv2");
          }
        }
      }
      return read_count;
    }

    int64_t RcClientImpl::write(const int fd, const void* buf, const int64_t count)
    {
      int64_t write_count = -1;
      int ret = check_init_stat();
      if (TFS_SUCCESS == ret)
      {
        fdInfo fd_info;
        ret = get_fdinfo(fd, fd_info);
        if (TFS_SUCCESS == ret)
        {
          int64_t start_time = tbsys::CTimeUtil::getTime();
          if (fd_info.raw_tfs_fd_ >= 0)
          {
            write_count = TfsClient::Instance()->write(fd_info.raw_tfs_fd_, buf, count);
          }
          else
          {
            TBSYS_LOG(ERROR, "name meta file not support write");
          }
          int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
          add_stat_info(OPER_WRITE, write_count, response_time, write_count >= 0);
        }
      }
      return write_count;
    }

    int64_t RcClientImpl::lseek(const int fd, const int64_t offset, const int whence)
    {
      int64_t off_set = -1;
      fdInfo fd_info;
      int ret = get_fdinfo(fd, fd_info);
      if (TFS_SUCCESS == ret)
      {
        if (fd_info.raw_tfs_fd_ >= 0)
        {
          off_set = TfsClient::Instance()->lseek(fd_info.raw_tfs_fd_, offset, whence);
        }
        else
        {
          TBSYS_LOG(ERROR, "name meta file not support lseek");
        }
      }
      return off_set;
    }

    TfsRetType RcClientImpl::fstat(const int fd, common::TfsFileStat* buf)
    {
      fdInfo fd_info;
      int ret = get_fdinfo(fd, fd_info);
      if (TFS_SUCCESS == ret)
      {
        if (fd_info.raw_tfs_fd_ >= 0)
        {
          ret = TfsClient::Instance()->fstat(fd_info.raw_tfs_fd_, buf);
        }
        else
        {
          TBSYS_LOG(ERROR, "name meta file not support fstat");
        }
      }
      return ret;
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
          int32_t ref_count = TfsClient::Instance()->unlink_unique(data_size, file_name, suffix, ns_addr, 1);
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
        ret = TfsClient::Instance()->unlink(data_size, file_name, suffix,
            ns_addr,action);
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
        add_stat_info(OPER_UNLINK, data_size, response_time, ret == TFS_SUCCESS);
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

    int64_t RcClientImpl::save_buf(const char* source_data, const int32_t data_len,
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
          saved_size = save_buf(ns_addr.c_str(), source_data, data_len,
              tfs_name_buff, buff_len);
        } while(saved_size < 0);
      }
      return saved_size;
    }

    int RcClientImpl::fetch_file(const char* local_file,
                       const char* file_name, const char* suffix)
    {
      int ret = check_init_stat();
      if (TFS_SUCCESS == ret)
      {
        int ns_get_index = 0;
        string ns_addr;
        do
        {
          ns_addr = get_ns_addr(NULL, RcClient::READ, ns_get_index++);
          if (ns_addr.empty())
          {
            break;
          }
          ret = fetch_file(ns_addr.c_str(), local_file, file_name, suffix);
        } while(ret != TFS_SUCCESS);
      }
      return ret;
    }

    int RcClientImpl::fetch_buf(int64_t& ret_count, char* buf, const int64_t count,
                     const char* file_name, const char* suffix)
    {
      int ret = check_init_stat();
      if (TFS_SUCCESS == ret)
      {
        int ns_get_index = 0;
        string ns_addr;
        do
        {
          ns_addr = get_ns_addr(NULL, RcClient::READ, ns_get_index++);
          if (ns_addr.empty())
          {
            break;
          }
          ret = fetch_buf(ns_addr.c_str(), ret_count, buf, count, file_name, suffix);
        } while(ret != TFS_SUCCESS);
      }
      return ret;
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
          saved_size = TfsClient::Instance()->save_file_unique(tfs_name_buff, buff_len, local_file,
              NULL, ns_addr);
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
        saved_size = TfsClient::Instance()->save_file(tfs_name_buff, buff_len, local_file,
            flag, NULL, ns_addr);
        int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
        add_stat_info(OPER_WRITE, saved_size, response_time, saved_size >= 0);
      }
      return saved_size;
    }
    int64_t RcClientImpl::save_buf(const char* ns_addr, const char* source_data, const int32_t data_len,
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
          saved_size = TfsClient::Instance()->save_buf_unique(tfs_name_buff, buff_len, source_data, data_len,
              NULL, ns_addr);
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
        saved_size = TfsClient::Instance()->save_buf(tfs_name_buff, buff_len, source_data, data_len,
            T_DEFAULT, NULL, ns_addr);
        int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
        add_stat_info(OPER_WRITE, saved_size, response_time, saved_size >= 0);
      }
      return saved_size;
    }
    int RcClientImpl::fetch_file(const char* ns_addr, const char* local_file,
        const char* file_name, const char* suffix)
    {
      int ret = TFS_SUCCESS;
      int64_t start_time = tbsys::CTimeUtil::getTime();
      ret = TfsClient::Instance()->fetch_file(local_file,
          file_name, suffix, ns_addr);
      int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
      int64_t file_size = 0;
      //TODO get file_size
      add_stat_info(OPER_READ, file_size, response_time, TFS_SUCCESS == ret);
      return ret;
    }

    int RcClientImpl::fetch_buf(const char* ns_addr, int64_t& ret_count, char* buf, const int64_t count,
        const char* file_name, const char* suffix)
    {
      int ret = TFS_SUCCESS;
      int64_t start_time = tbsys::CTimeUtil::getTime();
      ret = TfsClient::Instance()->fetch_file(ret_count, buf, count,
          file_name, suffix, ns_addr);
      int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
      add_stat_info(OPER_READ, ret_count, response_time, TFS_SUCCESS == ret);
      return ret;
    }

    TfsRetType RcClientImpl::login(const uint64_t rc_ip, const char* app_key, const uint64_t app_ip)
    {
      int ret = TFS_SUCCESS;
      if (TFS_SUCCESS == (ret = RcHelper::login(rc_ip, app_key, app_ip,
              session_base_info_.session_id_, base_info_)))
      {
        calculate_ns_info(base_info_, local_addr_);
        int32_t app_id = 0;
        int64_t session_ip = 0;
        common::SessionUtil::parse_session_id(session_base_info_.session_id_, app_id, session_ip);
        app_id_ = app_id;
      }
      return ret;
    }

    TfsRetType RcClientImpl::check_init_stat(const bool check_app_id) const
    {
      int ret = TFS_SUCCESS;
      if (init_stat_ != INIT_LOGINED)
      {
        TBSYS_LOG(ERROR, "not inited");
        ret = TFS_ERROR;
      }
      if (check_app_id)
      {
        if (app_id_ <= 0)
        {
          TBSYS_LOG(ERROR, "app_id error");
          ret = TFS_ERROR;
        }
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

    int RcClientImpl::open(const char* ns_addr, const char* file_name, const char* suffix,
        const int flag, const bool large, const char* local_key)
    {
      int ret = NULL == ns_addr || NULL == file_name ? -1 : 0;
      if (0 == ret)
      {
        int tfs_flag = large ? flag | common::T_LARGE : flag;
        ret = large ? TfsClient::Instance()->open(file_name, suffix, ns_addr, tfs_flag, local_key)
                    : TfsClient::Instance()->open(file_name, suffix, ns_addr, tfs_flag);
      }
      return ret;
    }

    string RcClientImpl::get_ns_addr(const char* file_name, const RcClient::RC_MODE mode, const int index) const
    {
      int32_t cluster_id = get_cluster_id(file_name);
      return get_ns_addr_by_cluster_id(cluster_id, mode, index);

    }
    string RcClientImpl::get_ns_addr_by_cluster_id(int32_t cluster_id, const RcClient::RC_MODE mode, const int index) const
    {
      string ns_addr;
      if ((index >= CHOICE_CLUSTER_NS_TYPE_LENGTH)
          || -1 == cluster_id)
      {
        TBSYS_LOG(DEBUG, "wrong index or file not exist, index: %d, cluster_id: %d", index, cluster_id);
        //null ;
      }
      else
      {
        tbsys::CThreadGuard mutex_guard(&mutex_);
        if ((RcClient::READ == mode) && cluster_id == 0)
        {
          ns_addr = choice[index].begin()->second;
        }
        else if (RcClient::CREATE == mode || RcClient::WRITE == mode)
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
      for (int8_t i = 0; i < CHOICE_CLUSTER_NS_TYPE_LENGTH; ++i)
      {
        write_ns_[i].clear();
        choice[i].clear();
      }
      need_use_unique_ = false;
      std::vector<ClusterRackData>::const_iterator it = base_info.cluster_infos_.begin();
      std::vector<ClusterData>::const_iterator cluster_data_it;
      for (; it != base_info.cluster_infos_.end(); it++)
      {
        //every cluster rack
        bool can_write = false;
        cluster_data_it = it->cluster_data_.begin();
        for (; cluster_data_it != it->cluster_data_.end(); cluster_data_it++)
        {
          //every cluster
          assert(0 != cluster_data_it->cluster_stat_);
          //rc server should not give the cluster which stat is 0
          assert(0 != cluster_data_it->access_type_);
          int32_t cluster_id = 0;
          bool is_master = false;
          parse_cluster_id(cluster_data_it->cluster_id_, cluster_id, is_master);

          if (CLUSTER_ACCESS_TYPE_READ_WRITE == cluster_data_it->access_type_)
          {
            can_write = true;
            add_ns_into_write_ns(cluster_data_it->ns_vip_, local_addr);
          }
          add_ns_into_choice(cluster_data_it->ns_vip_, local_addr, cluster_id);
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
      for (int i = 0; i < CHOICE_CLUSTER_NS_TYPE_LENGTH; i++)
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

    int RcClientImpl::add_ns_into_write_ns(const std::string& ip_str, const uint32_t addr)
    {
      int32_t iret = !ip_str.empty() || addr > 0 ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        int8_t index = 0;
        //calculate who will be the first choice;
        //uint32_t distance2 = calculate_distance(ip_str, addr);
        for (; index < CHOICE_CLUSTER_NS_TYPE_LENGTH; index++)
        {
          if (write_ns_[index].empty())
          {
            break;
          }
        }
        if (index < CHOICE_CLUSTER_NS_TYPE_LENGTH)
        {
          write_ns_[index] = ip_str;
        }
      }
      return iret;
    }

    int RcClientImpl::add_ns_into_choice(const std::string& ip_str, const uint32_t addr, const int32_t cluster_id)
    {
      int32_t iret = !ip_str.empty() || addr > 0  || cluster_id <= 48 || cluster_id >= 58 ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        int8_t index = 0;
        for (; index < CHOICE_CLUSTER_NS_TYPE_LENGTH; index++)
        {
          if (choice[index].empty())
          {
            break;
          }
          else
          {
            ClusterNsType::const_iterator it = choice[index].find(cluster_id);
            if (it == choice[index].end())
            {
              break;
            }
          }
        }

        if (index < CHOICE_CLUSTER_NS_TYPE_LENGTH)
        {
          choice[index][cluster_id] = ip_str;
        }
      }
      return iret;
    }
    // for name meta
    TfsRetType RcClientImpl::create_dir(const int64_t uid, const char* dir_path)
    {
      int ret = check_init_stat(true);
      if (TFS_SUCCESS == ret)
      {
        ret = name_meta_client_->create_dir(app_id_, uid, dir_path);
      }
      return ret;
    }

    TfsRetType RcClientImpl::create_file(const int64_t uid, const char* file_path)
    {
      int ret = check_init_stat(true);
      if (TFS_SUCCESS == ret)
      {
        ret = name_meta_client_->create_file(app_id_, uid, file_path);
      }
      return ret;
    }

    TfsRetType RcClientImpl::rm_dir(const int64_t uid, const char* dir_path)
    {
      int ret = check_init_stat(true);
      if (TFS_SUCCESS == ret)
      {
        ret = name_meta_client_->rm_dir(app_id_, uid, dir_path);
      }
      return ret;
    }
    TfsRetType RcClientImpl::rm_file(const int64_t uid, const char* file_path)
    {
      int ret = check_init_stat(true);
      if (TFS_SUCCESS == ret)
      {
        int cluster_id = name_meta_client_->get_cluster_id(app_id_, uid, file_path);
        if (-1 == cluster_id)
        {
          TBSYS_LOG(DEBUG, "file not exsit, file_path: ", file_path);
          ret = EXIT_TARGET_EXIST_ERROR;
        }
        else
        {
          // treat as write oper
          string ns_addr = get_ns_addr_by_cluster_id(cluster_id, RcClient::WRITE, 0);
          if (ns_addr.empty())
          {
            TBSYS_LOG(ERROR, "can not do this operator in cluster %d", cluster_id);
          }
          else
          {
            ret = name_meta_client_->rm_file(ns_addr.c_str(), app_id_, uid, file_path);
          }
        }
      }
      return ret;
    }

    TfsRetType RcClientImpl::mv_dir(const int64_t uid, const char* src_dir_path,
        const char* dest_dir_path)
    {
      int ret = check_init_stat(true);
      if (TFS_SUCCESS == ret)
      {
        ret = name_meta_client_->mv_dir(app_id_, uid, src_dir_path, dest_dir_path);
      }
      return ret;
    }
    TfsRetType RcClientImpl::mv_file(const int64_t uid, const char* src_file_path,
        const char* dest_file_path)
    {
      int ret = check_init_stat(true);
      if (TFS_SUCCESS == ret)
      {
        ret = name_meta_client_->mv_file(app_id_, uid, src_file_path, dest_file_path);
      }
      return ret;
    }

      bool RcClientImpl::is_raw_tfsname(const char* name)
      {
        bool ret = false;
        if (NULL != name)
        {
          //TODO
          ret = (*name == 'T' || *name == 'L');
        }
        return ret;
      }

      TfsRetType RcClientImpl::ls_dir(const int64_t app_id, const int64_t uid, const char* dir_path,
          std::vector<common::FileMetaInfo>& v_file_meta_info)
      {
        int ret = check_init_stat(true);
        if (TFS_SUCCESS == ret)
        {
          ret = name_meta_client_->ls_dir(app_id, uid, dir_path, v_file_meta_info);
        }
        return ret;
      }

      TfsRetType RcClientImpl::ls_file(const int64_t app_id, const int64_t uid,
          const char* file_path,
          common::FileMetaInfo& file_meta_info)
      {
        int ret = check_init_stat(true);
        if (TFS_SUCCESS == ret)
        {
          ret = name_meta_client_->ls_file(app_id, uid, file_path, file_meta_info);
        }
        return ret;
      }

      int RcClientImpl::gen_fdinfo(const fdInfo& fdinfo)
      {
        int gen_fd = -1;
        map<int, fdInfo>::iterator it;
        tbsys::CThreadGuard mutex_guard(&fd_info_mutex_);
        if (static_cast<int32_t>(fd_infos_.size()) >= MAX_OPEN_FD_COUNT - 1)
        {
          TBSYS_LOG(ERROR, "too much open files");
        }
        else
        {
          while(1)
          {
            if (MAX_FILE_FD == my_fd_)
            {
              my_fd_ = 1;
            }
            gen_fd = my_fd_++;
            it = fd_infos_.find(gen_fd);
            if (it == fd_infos_.end())
            {
              fd_infos_.insert(make_pair(gen_fd, fdinfo));
              break;
            }
          }
        }
        return gen_fd;
      }

      int RcClientImpl::open(const int64_t app_id, const int64_t uid, const char* name, const RcClient::RC_MODE mode)
      {
        int tfs_ret = check_init_stat(true);
        int fd = -1;
        if (TFS_SUCCESS == tfs_ret)
        {
          if (RcClient::CREATE == mode && need_use_unique_)
          {
            TBSYS_LOG(ERROR, "should use save_file");
            fd = -1;
          }
          else if ((RcClient::CREATE == mode || RcClient::WRITE == mode) && (app_id_ != app_id))
          {
            TBSYS_LOG(ERROR, "can not write other app_id");
            fd = -1;
          }
          else
          {
            fdInfo fd_info(-1, app_id, uid, name);
            int32_t cluster_id = 0;
            cluster_id = name_meta_client_->get_cluster_id(app_id, uid, name);
            fd_info.ns_addr_ = get_ns_addr_by_cluster_id(cluster_id, mode, 0);
            if (fd_info.ns_addr_.empty())
            {
              TBSYS_LOG(ERROR, "can not do this operator in cluster %d", cluster_id);
            }
            else
            {
              fd = gen_fdinfo(fd_info);
              if (fd >= 0)
              {
                if (RcClient::CREATE == mode)
                {
                  tfs_ret = name_meta_client_->create_file(app_id, uid, name);
                  if (TFS_SUCCESS != tfs_ret)
                  {
                    remove_fdinfo(fd, fd_info);
                    fd = -1;
                  }
                }
              }
            }
          }
        }
        return fd;
      }

      int64_t RcClientImpl::pwrite(const int fd, const void* buf, const int64_t count, const int64_t offset)
      {
        int64_t write_count = -1;
        int ret = check_init_stat();
        if (TFS_SUCCESS == ret)
        {
          fdInfo fd_info;
          ret = get_fdinfo(fd, fd_info);
          if (TFS_SUCCESS == ret)
          {
            int64_t start_time = tbsys::CTimeUtil::getTime();
            if (fd_info.raw_tfs_fd_ < 0)
            {
              write_count = name_meta_client_->write(fd_info.ns_addr_.c_str(), fd_info.app_id_, fd_info.uid_,
                  fd_info.name_.c_str(), buf, offset, count);
            }
            else
            {
              TBSYS_LOG(WARN, "sorry, raw tfs client not support pwrite.");
              //write_count = TfsClient::Instance()->pwrite(fd_info.raw_tfs_fd_, buf, count, offset);
            }
            int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
            add_stat_info(OPER_WRITE, write_count, response_time, write_count >= 0);
          }
        }
        return write_count;
      }

      int64_t RcClientImpl::pread(const int fd, void* buf, const int64_t count, const int64_t offset)
      {
        int64_t read_count = -1;
        int ret = check_init_stat();
        if (TFS_SUCCESS == ret)
        {
          fdInfo fd_info;
          ret = get_fdinfo(fd, fd_info);
          if (TFS_SUCCESS == ret)
          {
            int64_t start_time = tbsys::CTimeUtil::getTime();
            if (fd_info.raw_tfs_fd_ < 0)
            {
              read_count = name_meta_client_->read(fd_info.ns_addr_.c_str(), fd_info.app_id_, fd_info.uid_,
                  fd_info.name_.c_str(), buf, offset, count);
            }
            else
            {
              TBSYS_LOG(WARN, "sorry, raw tfs client not support pread.");
              //read_count = TfsClient::Instance()->pread(fd_info.raw_tfs_fd_, buf, count, offset);
            }
            int64_t response_time = tbsys::CTimeUtil::getTime() - start_time;
            add_stat_info(OPER_READ, read_count, response_time, read_count >= 0);
          }
        }
        return read_count;
      }

      //int64_t RcClientImpl::save_file(const int64_t app_id, const int64_t uid,
      //    const char* local_file, const char* file_path)
      //{
      //  int64_t saved_size = -1;
      //  if (app_id_ != app_id)
      //  {
      //    saved_size = EXIT_APPID_PERMISSION_DENY;
      //    TBSYS_LOG(ERROR, "can not write other app_id");
      //  }
      //  else
      //  {
      //    int ret = check_init_stat();
      //    if (TFS_SUCCESS == ret)
      //    {
      //      int ns_get_index = 0;
      //      string ns_addr;
      //      do
      //      {
      //        ns_addr = get_ns_addr(NULL, RcClient::WRITE, ns_get_index++);
      //        if (ns_addr.empty())
      //        {
      //          break;
      //        }
      //        saved_size = name_meta_client_->save_file(ns_addr.c_str(), app_id, uid, local_file, file_path);
      //      } while(saved_size < 0);
      //    }
      //  }
      //  return saved_size;
      //}

      //int RcClientImpl::fetch_file(const int64_t app_id, const int64_t uid,
      //    const char* local_file, const char* file_path)
      //{
      //  int ret = check_init_stat();
      //  if (TFS_SUCCESS == ret)
      //  {
      //    int ns_get_index = 0;
      //    string ns_addr;
      //    do
      //    {
      //      ns_addr = get_ns_addr(NULL, RcClient::READ, ns_get_index++);
      //      if (ns_addr.empty())
      //      {
      //        break;
      //      }
      //      ret = name_meta_client_->fetch_file(ns_addr.c_str(), app_id, uid, local_file, file_path);
      //    } while(ret != TFS_SUCCESS);
      //  }
      //  return ret;
      //}

      TfsRetType RcClientImpl::remove_fdinfo(const int fd, fdInfo& fdinfo)
      {
        TfsRetType ret = TFS_ERROR;
        map<int, fdInfo>::iterator it;
        tbsys::CThreadGuard mutex_guard(&fd_info_mutex_);
        it = fd_infos_.find(fd);
        if (it != fd_infos_.end())
        {
          fdinfo = it->second;
          fd_infos_.erase(it);
          ret = TFS_SUCCESS;
        }
        return ret;
      }
      TfsRetType RcClientImpl::get_fdinfo(const int fd, fdInfo& fdinfo) const
      {
        TfsRetType ret = TFS_ERROR;
        map<int, fdInfo>::const_iterator it;
        tbsys::CThreadGuard mutex_guard(&fd_info_mutex_);
        it = fd_infos_.find(fd);
        if (it != fd_infos_.end())
        {
          fdinfo = it->second;
          ret = TFS_SUCCESS;
        }
        return ret;
      }
    }
  }
