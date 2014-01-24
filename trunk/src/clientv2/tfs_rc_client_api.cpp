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
//#include "local_key.h"
#include "tfs_rc_client_api.h"
#include "common/define.h"
#include "tfs_rc_client_api_impl.h"

namespace tfs
{
  namespace clientv2
  {
    using namespace tfs::common;
    using namespace std;

    RcClient::RcClient():impl_(NULL)
    {
      impl_ = new RcClientImpl();
    }

    RcClient::~RcClient()
    {
      delete impl_;
      impl_ = NULL;
    }
    TfsRetType RcClient::initialize(const char* str_rc_ip, const char* app_key, const char* str_app_ip,
        const int32_t cache_times, const int32_t cache_items, const char* dev_name)
    {
      int32_t real_cache_times = (cache_times >= 0) ? cache_times : common::DEFAULT_BLOCK_CACHE_TIME;
      int32_t real_cache_items = (cache_items >= 0) ? cache_items : common::DEFAULT_BLOCK_CACHE_ITEMS;
      return impl_->initialize(str_rc_ip, app_key, str_app_ip, real_cache_times, real_cache_items, dev_name);
    }
    TfsRetType RcClient::initialize(const uint64_t rc_ip, const char* app_key, const uint64_t app_ip,
        const int32_t cache_times, const int32_t cache_items, const char* dev_name)
    {
      int32_t real_cache_times = (cache_times >= 0) ? cache_times : common::DEFAULT_BLOCK_CACHE_TIME;
      int32_t real_cache_items = (cache_items >= 0) ? cache_items : common::DEFAULT_BLOCK_CACHE_ITEMS;
      return impl_->initialize(rc_ip, app_key, app_ip, real_cache_times, real_cache_items, dev_name);
    }

    int64_t RcClient::get_app_id() const
    {
      return impl_->get_app_id();
    }

#ifdef WITH_TAIR_CACHE
    void RcClient::set_remote_cache_info(const char * remote_cache_info)
    {
      impl_->set_remote_cache_info(remote_cache_info);
    }
#endif

    void RcClient::set_wait_timeout(const int64_t timeout_ms)
    {
      impl_->set_wait_timeout(timeout_ms);
    }

    void RcClient::set_log_level(const char* level)
    {
      impl_->set_log_level(level);
    }

    void RcClient::set_log_file(const char* log_file)
    {
      impl_->set_log_file(log_file);
    }

    TfsRetType RcClient::logout()
    {
      return impl_->logout();
    }

    // for raw tfs
    int RcClient::open(const char* file_name, const char* suffix, const RC_MODE mode)
    {
      return impl_->open(file_name, suffix, mode);
    }

    TfsRetType RcClient::close(const int fd, char* tfs_name_buff, const int32_t buff_len)
    {
      return impl_->close(fd, tfs_name_buff, buff_len);
    }

    int64_t RcClient::read(const int fd, void* buf, const int64_t count)
    {
      return impl_->read(fd, buf, count);
    }

    int64_t RcClient::readv2(const int fd, void* buf, const int64_t count, TfsFileStat* tfs_stat_buf)
    {
      return impl_->readv2(fd, buf, count, tfs_stat_buf);
    }

    int64_t RcClient::write(const int fd, const void* buf, const int64_t count)
    {
      return impl_->write(fd, buf, count);
    }

    int64_t RcClient::lseek(const int fd, const int64_t offset, const int whence)
    {
      return impl_->lseek(fd, offset, whence);
    }

    TfsRetType RcClient::fstat(const int fd, common::TfsFileStat* buf, const common::TfsStatType fmode)
    {
      return impl_->fstat(fd, buf, fmode);
    }

    TfsRetType RcClient::unlink(const char* file_name, const char* suffix, const common::TfsUnlinkType action)
    {
      return impl_->unlink(file_name, suffix, action);
    }

    int64_t RcClient::save_file(const char* local_file, char* tfs_name_buff, const int32_t buff_len,
        const char *suffix)
    {
      return impl_->save_file(local_file, tfs_name_buff, buff_len, suffix);
    }

    int64_t RcClient::save_buf(const char* source_data, const int32_t data_len,
        char* tfs_name_buff, const int32_t buff_len, const char* suffix)
    {
      return impl_->save_buf(source_data, data_len, tfs_name_buff, buff_len, suffix);
    }

    int RcClient::fetch_file(const char* local_file,
        const char* file_name, const char* suffix)
    {
      return impl_->fetch_file(local_file, file_name, suffix);
    }

    // for kv meta
    void RcClient::set_kv_rs_addr(const char *rs_addr)
    {
      impl_->set_kv_rs_addr(rs_addr);
    }

    TfsRetType RcClient::put_bucket(const char *bucket_name,
        const UserInfo &user_info)
    {
      return impl_->put_bucket(bucket_name, user_info);
    }

    TfsRetType RcClient::get_bucket(const char *bucket_name, const char *prefix,
        const char *start_key, const char delimiter, const int32_t limit,
        vector<ObjectMetaInfo> *v_object_meta_info,
        vector<string> *v_object_name,
        set<string> *s_common_prefix,
        int8_t *is_truncated, const UserInfo &user_info)
    {
      return impl_->get_bucket(bucket_name, prefix, start_key, delimiter,
          limit, v_object_meta_info, v_object_name, s_common_prefix,
          is_truncated, user_info);
    }

    TfsRetType RcClient::del_bucket(const char *bucket_name, const UserInfo &user_info)
    {
      return impl_->del_bucket(bucket_name, user_info);
    }

    TfsRetType RcClient::head_bucket(const char *bucket_name,
        BucketMetaInfo *bucket_meta_info, const UserInfo &user_info)
    {
      return impl_->head_bucket(bucket_name, bucket_meta_info, user_info);
    }

    TfsRetType RcClient::put_object(const char *bucket_name, const char *object_name,
        const char* local_file, const UserInfo &user_info)
    {
      return impl_->put_object(bucket_name, object_name, local_file, user_info);
    }

    int64_t RcClient::pwrite_object(const char *bucket_name, const char *object_name,
        const void *buf, const int64_t offset, const int64_t length,
        const UserInfo &user_info)
    {
      return impl_->pwrite_object(bucket_name, object_name, buf, offset,
          length, user_info);
    }

    int64_t RcClient::pread_object(const char *bucket_name, const char *object_name,
        void *buf, const int64_t offset, const int64_t length,
        ObjectMetaInfo *object_meta_info, CustomizeInfo *customize_info,
        const UserInfo &user_info)
    {
      return impl_->pread_object(bucket_name, object_name, buf, offset,
          length, object_meta_info, customize_info, user_info);
    }

    TfsRetType RcClient::get_object(const char *bucket_name, const char *object_name,
        const char* local_file, const UserInfo &user_info)
    {
      return impl_->get_object(bucket_name, object_name, local_file,
          user_info);
    }

    TfsRetType RcClient::del_object(const char *bucket_name, const char *object_name,
        const UserInfo &user_info)
    {
      return impl_->del_object(bucket_name, object_name, user_info);
    }

    TfsRetType RcClient::head_object(const char *bucket_name, const char *object_name,
        ObjectInfo *object_info, const UserInfo &user_info)
    {
      return impl_->head_object(bucket_name, object_name, object_info, user_info);
    }

    TfsRetType RcClient::set_life_cycle(const int32_t file_type, const char *file_name,
                                        const int32_t invalid_time_s, const char *app_key)
    {
      return impl_->set_life_cycle(file_type, file_name, invalid_time_s, app_key);
    }

    TfsRetType RcClient::get_life_cycle(const int32_t file_type, const char *file_name,
                                              int32_t *invalid_time_s)
    {
      return impl_->get_life_cycle(file_type, file_name, invalid_time_s);
    }

    TfsRetType RcClient::rm_life_cycle(const int32_t file_type, const char *file_name)
    {
      return impl_->rm_life_cycle(file_type, file_name);
    }

  }
}
