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
#include "common/rc_define.h"
#include "tfs_rc_client_api.h"

namespace tfs
{
  namespace common
  {
    class FileMetaInf;
  }
  namespace client
  {
    class RcClientImpl;
    class NameMetaClient;
    class KvMetaClientImpl;
    class TfsClusterManager;
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
        RcClientImpl();
        ~RcClientImpl();

        TfsRetType initialize(const char* str_rc_ip, const char* app_key, const char* str_app_ip,
            const int32_t cache_times = common::DEFAULT_BLOCK_CACHE_TIME,
            const int32_t cache_items = common::DEFAULT_BLOCK_CACHE_ITEMS,
            const char* dev_name = NULL,
            const char* rs_addr = NULL);
        //return value :TFS_SUCCESS/TFS_ERROR;
        TfsRetType initialize(const uint64_t rc_ip, const char* app_key, const uint64_t app_ip,
            const int32_t cache_times = common::DEFAULT_BLOCK_CACHE_TIME,
            const int32_t cache_items = common::DEFAULT_BLOCK_CACHE_ITEMS,
            const char* dev_name = NULL,
            const char* rs_addr = NULL);

        int64_t get_app_id() const { return app_id_;}
        void set_client_retry_count(const int64_t count);
        int64_t get_client_retry_count() const;
        void set_client_retry_flag(bool retry_flag);

#ifdef WITH_TAIR_CACHE
        void set_remote_cache_info(const char * remote_cache_info);
#endif
        void set_wait_timeout(const int64_t timeout_ms);
        void set_log_level(const char* level);
        void set_log_file(const char* log_file);

        TfsRetType logout();

        // for raw tfs
        int open(const char* file_name, const char* suffix, RcClient::RC_MODE mode,
            const bool large = false, const char* local_key = NULL);
        TfsRetType close(const int fd, char* tfs_name_buff = NULL, const int32_t buff_len = 0);

        int64_t read(const int fd, void* buf, const int64_t count);
        int64_t readv2(const int fd, void* buf, const int64_t count, common::TfsFileStat* tfs_stat_buf);

        int64_t write(const int fd, const void* buf, const int64_t count);

        int64_t lseek(const int fd, const int64_t offset, const int whence);
        TfsRetType fstat(const int fd, common::TfsFileStat* buf, const common::TfsStatType fmode = common::NORMAL_STAT);

        TfsRetType unlink(const char* file_name, const char* suffix = NULL,
           const common::TfsUnlinkType action = common::DELETE);

        int64_t save_file(const char* local_file, char* tfs_name_buff, const int32_t buff_len,
            const char* suffix = NULL, const bool is_large_file = false);
        int64_t save_buf(const char* source_data, const int32_t data_len,
            char* tfs_name_buff, const int32_t buff_len, const char* suffix = NULL);

        int fetch_file(const char* local_file,
                       const char* file_name, const char* suffix = NULL);
        int fetch_buf(int64_t& ret_count, char* buf, const int64_t count,
                     const char* file_name, const char* suffix = NULL);

        bool is_hit_local_cache(const char* tfs_name);

#ifdef WITH_TAIR_CACHE
        bool is_hit_remote_cache(const char* tfs_name);
#endif

        // for kv meta
        void set_kv_rs_addr(const char *rs_addr); // tmp use

        TfsRetType put_bucket(const char *bucket_name,
            const common::UserInfo &user_info);
        TfsRetType get_bucket(const char *bucket_name, const char *prefix,
            const char *start_key, const char delimiter, const int32_t limit,
            std::vector<common::ObjectMetaInfo> *v_object_meta_info,
            std::vector<std::string> *v_object_name, std::set<std::string> *s_common_prefix,
            int8_t *is_truncated, const common::UserInfo &user_info);
        TfsRetType del_bucket(const char *bucket_name,
            const common::UserInfo &user_info);
        TfsRetType head_bucket(const char *bucket_name,
            common::BucketMetaInfo *bucket_meta_info,
            const common::UserInfo &user_info);

        TfsRetType put_object(const char *bucket_name, const char *object_name,
            const char* local_file, const common::UserInfo &user_info);
        int64_t pwrite_object(const char *bucket_name, const char *object_name,
            const void *buf, const int64_t offset, const int64_t length,
            const common::UserInfo &user_info);
        int64_t pread_object(const char *bucket_name, const char *object_name,
            void *buf, const int64_t offset, const int64_t length,
            common::ObjectMetaInfo *object_meta_info,
            common::CustomizeInfo *customize_info,
            const common::UserInfo &user_info);
        TfsRetType get_object(const char *bucket_name, const char *object_name,
            const char* local_file, const common::UserInfo &user_info);
        TfsRetType del_object(const char *bucket_name, const char *object_name,
            const common::UserInfo &user_info);
        TfsRetType head_object(const char *bucket_name, const char *object_name,
            common::ObjectInfo *object_info, const common::UserInfo &user_info);

        TfsRetType set_life_cycle(const int32_t file_type, const char *file_name,
                                  const int32_t invalid_time_s, const char *app_key);
        TfsRetType get_life_cycle(const int32_t file_type, const char *file_name,
                                        int32_t *invalid_time_s);
        TfsRetType rm_life_cycle(const int32_t file_type, const char *file_name);

        // for name meta
        TfsRetType create_dir(const int64_t uid, const char* dir_path);
        TfsRetType create_dir_with_parents(const int64_t uid, const char* dir_path);
        TfsRetType create_file(const int64_t uid, const char* file_path);

        TfsRetType rm_dir(const int64_t uid, const char* dir_path);
        TfsRetType rm_file(const int64_t uid, const char* file_path);

        TfsRetType mv_dir(const int64_t uid, const char* src_dir_path, const char* dest_dir_path);
        TfsRetType mv_file(const int64_t uid, const char* src_file_path, const char* dest_file_path);

        TfsRetType ls_dir(const int64_t app_id, const int64_t uid, const char* dir_path,
            std::vector<common::FileMetaInfo>& v_file_meta_info);
        TfsRetType ls_file(const int64_t app_id, const int64_t uid,
            const char* file_path,
            common::FileMetaInfo& file_meta_info);

        bool is_dir_exist(const int64_t app_id, const int64_t uid, const char* dir_path);
        bool is_file_exist(const int64_t app_id, const int64_t uid, const char* file_path);

        int open(const int64_t app_id, const int64_t uid, const char* name, const RcClient::RC_MODE mode);
        int64_t pread(const int fd, void* buf, const int64_t count, const int64_t offset);
        int64_t pwrite(const int fd, const void* buf, const int64_t count, const int64_t offset);
        //use the same close func as raw tfs

        int64_t save_file(const int64_t app_id, const int64_t uid,
            const char* local_file, const char* tfs_file_name);

        int64_t save_buf(const int64_t app_id, const int64_t uid,
          const char* buf, const int64_t buf_len, const char* tfs_file_name);

        int64_t fetch_file(const int64_t app_id, const int64_t uid,
            const char* local_file, const char* tfs_file_name);

        int64_t fetch_buf(const int64_t app_id, const int64_t uid,
          char* buffer, const int64_t offset, const int64_t length, const char* tfs_file_name);

      private:
        DISALLOW_COPY_AND_ASSIGN(RcClientImpl);

      private:
        TfsRetType login(const uint64_t rc_ip, const char* app_key, const uint64_t app_ip);

        TfsRetType check_init_stat(const bool check_app_id = false) const;

        void destory();

        uint64_t get_active_rc_ip(size_t& retry_index) const;
        void get_ka_info(common::KeepAliveInfo& kainfo);

        void add_stat_info(const common::OperType& oper_type, const int64_t size,
            const int64_t response_time, const bool is_success);

        int open(const char* ns_addr, const char* file_name, const char* suffix,
            const int flag, const bool large, const char* local_key);

        TfsRetType unlink(const char* ns_addr, const char* file_name,
            const char* suffix, const common::TfsUnlinkType action);

        int64_t save_file(const char* ns_addr, const char* local_file, char* tfs_name_buff,
            const int32_t buff_len, const char* suffix = NULL, const bool is_large_file = false);

        int64_t save_buf(const char* ns_addr, const char* source_data, const int32_t data_len,
            char* tfs_name_buff, const int32_t buff_len, const char* suffix = NULL);

        int fetch_file(const char* ns_addr, const char* local_file,
                       const char* file_name, const char* suffix);

        int fetch_buf(const char* ns_addr, int64_t& ret_count, char* buf, const int64_t count,
                     const char* file_name, const char* suffix);

        static void parse_cluster_id(const std::string& cluster_id_str, int32_t& id, bool& is_master);

        void calculate_ns_info(const common::BaseInfo& base_info);

        void parse_duplicate_info(const std::string& duplicate_info);

      private:
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
        bool ignore_rc_remote_cache_info_;

        common::BaseInfo base_info_;
        common::SessionBaseInfo session_base_info_;
        common::SessionStat stat_;

        tbutil::TimerPtr keepalive_timer_;
        StatUpdateTaskPtr stat_update_task_;

      private:
        mutable tbsys::CThreadMutex mutex_;

      private:
        enum {
          INVALID_RAW_TFS_FD = -1,
          NAME_TFS_FD = -2,
        };
        struct fdInfo
        {
          fdInfo()
            :raw_tfs_fd_(INVALID_RAW_TFS_FD), flag_(0),
            app_id_(0), uid_(0), offset_(0), is_large_(false), cluster_id_(0)
          {
          }
          fdInfo(const char* file_name, const char* suffix, const int flag,
              const bool large, const char* local_key)
            :raw_tfs_fd_(INVALID_RAW_TFS_FD), flag_(flag), app_id_(0), uid_(0),
            offset_(0), is_large_(large), cluster_id_(0)
          {
            if (NULL != file_name)
            {
              name_ = file_name;
            }
            if (NULL != suffix)
            {
              suffix_ = suffix;
            }
            if (NULL != local_key)
            {
              local_key_ = local_key;
            }
          }
          fdInfo(const int64_t app_id, const int64_t uid, const int32_t cluster_id, const int flag,
              const char* name = NULL)
            :raw_tfs_fd_(NAME_TFS_FD), flag_(flag), app_id_(app_id), uid_(uid), offset_(0), is_large_(false),cluster_id_(cluster_id)
          {
            if (NULL != name)
            {
              name_ = name;
            }
          }
          int raw_tfs_fd_;
          int flag_;
          int64_t app_id_;
          int64_t uid_;
          int64_t offset_;
          bool is_large_;
          int cluster_id_;
          std::string local_key_;
          std::string name_;
          std::string suffix_;
          std::string ns_addr_;
        };
        std::map<int, fdInfo> fd_infos_;
        mutable tbsys::CThreadMutex fd_info_mutex_;
        NameMetaClient *name_meta_client_;
        KvMetaClientImpl *kv_meta_client_;
        TfsClusterManager *tfs_cluster_manager_;
        int64_t app_id_;
        char kv_rs_addr_[128];
        int my_fd_;
      private:
        bool have_permission(const char* file_name, const RcClient::RC_MODE mode);
        bool have_permission(const int32_t cluster_id, const uint32_t block_id, const RcClient::RC_MODE mode);
        static bool is_raw_tfsname(const char* name);
        int gen_fdinfo(const fdInfo& fdinfo);
        TfsRetType remove_fdinfo(const int fd, fdInfo& fdinfo);
        TfsRetType get_fdinfo(const int fd, fdInfo& fdinfo) const;
        TfsRetType update_fdinfo_offset(const int fd, const int64_t offset);
        TfsRetType update_fdinfo_rawfd(const int fd, const int raw_fd);
        int64_t real_read(const int fd, const int raw_tfs_fd, void* buf, const int64_t count,
            fdInfo& fd_info, common::TfsFileStat* tfs_stat_buf);
        int64_t read_ex(const int fd, void* buf, const int64_t count, common::TfsFileStat* tfs_stat_buf);

    };
  }
}

#endif
