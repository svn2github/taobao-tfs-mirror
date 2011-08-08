/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_client_api.h 19 2010-10-18 05:48:09Z nayan@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_RC_CLIENTAPI_IMPL_H_
#define TFS_CLIENT_RC_CLIENTAPI_IMPL_H_

#include <stdio.h>
#include <tbsys.h>
#include <Timer.h>
#include "common/define.h"
#include "common/cdefine.h"
#include "common/rc_define.h"
#include "tfs_rc_client_api.h"

namespace tfs
{
  namespace client
  {
    class RcClientImpl;
    class StatUpdateTask : public tbutil::TimerTask
    {
      public:
        explicit StatUpdateTask(RcClientImpl& rc_client);
        virtual void runTimerTask();
      private:
        RcClientImpl& rc_client_;
    };
    typedef tbutil::Handle<StatUpdateTask> StatUpdateTaskPtr;
    class RcClientImpl
    {
      friend class StatUpdateTask;
      public:
      public:
        RcClientImpl();
        ~RcClientImpl();

        TfsRetType initialize(const char* str_rc_ip, const char* app_key, const char* str_app_ip,
            const int32_t cache_times = common::DEFAULT_BLOCK_CACHE_TIME,
            const int32_t cache_items = common::DEFAULT_BLOCK_CACHE_ITEMS,
            const char* dev_name = NULL);
        //return value :TFS_SUCCESS/TFS_ERROR;
        TfsRetType initialize(const uint64_t rc_ip, const char* app_key, const uint64_t app_ip,
            const int32_t cache_times = common::DEFAULT_BLOCK_CACHE_TIME,
            const int32_t cache_items = common::DEFAULT_BLOCK_CACHE_ITEMS,
            const char* dev_name = NULL);

        void set_wait_timeout(const int64_t timeout_ms);
        void set_log_level(const char* level);
        void set_log_file(const char* log_file);

        //return value fd
        int open(const char* file_name, const char* suffix, const RcClient::RC_MODE mode,
            const bool large = false, const char* local_key = NULL);
        TfsRetType close(const int fd, char* tfs_name_buff = NULL, const int32_t buff_len = 0);

        int64_t read(const int fd, void* buf, const int64_t count);
        int64_t readv2(const int fd, void* buf, const int64_t count, common::TfsFileStat* tfs_stat_buf);
        int64_t pread(const int fd, void* buf, const int64_t count, const int64_t offset);

        int64_t write(const int fd, const void* buf, const int64_t count);
        int64_t pwrite(const int fd, const void* buf, const int64_t count, const int64_t offset);

        int64_t lseek(const int fd, const int64_t offset, const int whence);
        TfsRetType fstat(const int fd, common::TfsFileStat* buf);

        TfsRetType unlink(const char* file_name, const char* suffix = NULL,
            const common::TfsUnlinkType action = common::DELETE);
        int64_t save_file(const char* local_file, char* tfs_name_buff, const int32_t buff_len,
            const bool is_large_file = false);

        int64_t save_file(const char* source_data, const int32_t data_len,
            char* tfs_name_buff, const int32_t buff_len);

        TfsRetType logout();
      private:
        DISALLOW_COPY_AND_ASSIGN(RcClientImpl);

      private:
        TfsRetType login(const uint64_t rc_ip, const char* app_key, const uint64_t app_ip);

        TfsRetType check_init_stat() const;

        uint64_t get_active_rc_ip(size_t& retry_index) const;
        void get_ka_info(common::KeepAliveInfo& kainfo);

        void add_stat_info(const common::OperType& oper_type, const int64_t size,
            const int64_t response_time, const bool is_success);

        int open(const char* ns_addr, const char* file_name, const char* suffix,
            const RcClient::RC_MODE mode, const bool large, const char* local_key);

        TfsRetType unlink(const char* ns_addr, const char* file_name,
            const char* suffix, const common::TfsUnlinkType action);

        int64_t save_file(const char* ns_addr, const char* local_file, char* tfs_name_buff,
            const int32_t buff_len, const bool is_large_file = false);

        int64_t save_file(const char* ns_addr, const char* source_data, const int32_t data_len,
            char* tfs_name_buff, const int32_t buff_len);

        std::string get_ns_addr(const char* file_name, const RcClient::RC_MODE mode, const int index) const;

        static int32_t get_cluster_id(const char* file_name);
        static void parse_cluster_id(const std::string& cluster_id_str, int32_t& id, bool& is_master);
        static uint32_t calculate_distance(const std::string& ip_str, const uint32_t addr);

        void calculate_ns_info(const common::BaseInfo& base_info, const uint32_t local_addr);

        void parse_duplicate_info(const std::string& duplicate_info);

      private:
        typedef std::map<int32_t, std::string> ClusterNsType; //<cluster_id, ns>
        ClusterNsType choice[2];
        std::string write_ns_[2];
        std::string duplicate_server_master_;
        std::string duplicate_server_slave_;
        std::string duplicate_server_group_;
        int32_t duplicate_server_area_;
        bool need_use_unique_;
        uint32_t local_addr_;

      private:
        int32_t init_stat_;
        uint64_t active_rc_ip_;
        size_t next_rc_index_;

        common::BaseInfo base_info_;
        common::SessionBaseInfo session_base_info_;
        common::SessionStat stat_;

        tbutil::TimerPtr keepalive_timer_;
        StatUpdateTaskPtr stat_update_task_;

      private:
        mutable tbsys::CThreadMutex mutex_;
    };
  }
}

#endif
