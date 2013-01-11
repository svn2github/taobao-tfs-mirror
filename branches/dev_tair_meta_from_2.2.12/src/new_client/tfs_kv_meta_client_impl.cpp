/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *      mingyan(mingyan.zc@taobao.com)
 *      - initial release
 *
 */

#include "tfs_kv_meta_client_impl.h"

#include <algorithm>
#include "common/client_manager.h"
#include "message/message_factory.h"
#include "tfs_kv_meta_helper.h"


namespace tfs
{
  namespace client
  {
    using namespace tfs::common;
    using namespace std;

    KvMetaClientImpl::KvMetaClientImpl()
    :kms_id_(0)
    {
      packet_factory_ = new message::MessageFactory();
      packet_streamer_ = new common::BasePacketStreamer(packet_factory_);
    }

    KvMetaClientImpl::~KvMetaClientImpl()
    {
      tbsys::gDelete(packet_factory_);
      tbsys::gDelete(packet_streamer_);
    }

    int KvMetaClientImpl::initialize(const char *kms_addr, const char *ns_addr)
    {
      int ret = TFS_SUCCESS;
      if (NULL == kms_addr || NULL == ns_addr)
      {
        TBSYS_LOG(WARN, "kms_addr or ns_addr is null");
        ret = EXIT_INVALID_ARGU_ERROR;
      }
      else
      {
        ret = initialize(Func::get_host_ip(kms_addr), ns_addr);
      }
      return ret;
    }

    int KvMetaClientImpl::initialize(const int64_t kms_addr, const char *ns_addr)
    {
      int ret = TFS_SUCCESS;
      ret = NewClientManager::get_instance().initialize(packet_factory_, packet_streamer_);
      if (TFS_SUCCESS == ret)
      {
        kms_id_ = kms_addr;
        ns_addr_ = ns_addr;
        ret = tfs_meta_manager_.initialize();
      }
      return ret;
    }

    TfsRetType KvMetaClientImpl::put_bucket(const char *bucket_name)
    {
       TfsRetType ret = TFS_ERROR;

       if (!is_valid_bucket_name(bucket_name))
       {
         TBSYS_LOG(ERROR, "bucket name is invalid");
       }
       else
       {
         ret = TFS_SUCCESS;
       }

       if (TFS_SUCCESS == ret)
       {
         ret = do_put_bucket(bucket_name);
       }

       return ret;
    }

    TfsRetType KvMetaClientImpl::get_bucket(const char *bucket_name, const char *prefix,
                                            const char *start_key, const char delimiter, const int32_t limit,
                                            vector<ObjectMetaInfo> *v_object_meta_info,
                                            vector<string> *v_object_name, set<string> *s_common_prefix,
                                            int8_t *is_truncated)
    {
       TfsRetType ret = TFS_ERROR;

       if (!is_valid_bucket_name(bucket_name))
       {
         TBSYS_LOG(ERROR, "bucket name is invalid");
       }
       else
       {
         ret = TFS_SUCCESS;
       }

       if (TFS_SUCCESS == ret)
       {
         ret = do_get_bucket(bucket_name, prefix, start_key, delimiter, limit,
             v_object_meta_info, v_object_name, s_common_prefix, is_truncated);
       }

       return ret;
    }

    TfsRetType KvMetaClientImpl::del_bucket(const char *bucket_name)
    {
       TfsRetType ret = TFS_ERROR;

       if (!is_valid_bucket_name(bucket_name))
       {
         TBSYS_LOG(ERROR, "bucket name is invalid");
       }
       else
       {
         ret = TFS_SUCCESS;
       }

       if (TFS_SUCCESS == ret)
       {
         ret = do_del_bucket(bucket_name);
       }

       return ret;
    }

    // copy from NameMetaClientImpl::write_data
    int64_t KvMetaClientImpl::write_data(const char *ns_addr,
        const void *buffer, int64_t offset, int64_t length,
        vector<FragMeta> *v_frag_meta)
    {
      int64_t ret = TFS_SUCCESS;
      int64_t write_length = 0;
      int64_t left_length = length;
      int64_t cur_offset = offset;
      int64_t cur_pos = 0;

      do
      {
        // write to tfs, and get frag meta
        write_length = min(left_length, MAX_SEGMENT_SIZE);
        FragMeta frag_meta;
        int64_t real_length = tfs_meta_manager_.write_data(ns_addr,
          reinterpret_cast<const char*>(buffer) + cur_pos, cur_offset, write_length, frag_meta);

        if (real_length != write_length)
        {
          TBSYS_LOG(ERROR, "write fragment failed, cur_pos : %"PRI64_PREFIX"d, "
            "write_length : %"PRI64_PREFIX"d, ret_length: %"PRI64_PREFIX"d, ret: %"PRI64_PREFIX"d",
            cur_pos, write_length, real_length, ret);
          if (real_length < 0)
          {
            ret = real_length;
          }
          break;
        }
        else
        {
          v_frag_meta->push_back(frag_meta);
          cur_offset += (offset == -1 ? 0 : real_length);
          cur_pos += real_length;
          left_length -= real_length;
        }
      }
      while(left_length > 0);
      if (TFS_SUCCESS == ret)
      {
        ret = length - left_length;
      }
      return ret;
    }

    int64_t KvMetaClientImpl::read_data(const char* ns_addr,
        const vector<FragMeta> &v_frag_meta,
        void *buffer, int64_t offset, int64_t length)
    {
      int64_t ret = TFS_SUCCESS;
      int64_t cur_offset = offset;
      int64_t cur_length = 0;
      int64_t left_length = length;
      int64_t cur_pos = 0;

      vector<FragMeta>::const_iterator iter = v_frag_meta.begin();
      for(; iter != v_frag_meta.end(); iter++)
      {
        if (cur_offset > iter->offset_ + iter->size_)
        {
          TBSYS_LOG(ERROR, "fatal error wrong pos, cur_offset: %"PRI64_PREFIX"d, total: %"PRI64_PREFIX"d",
              cur_offset, (iter->offset_ + iter->size_));
          break;
        }

        // deal with the hole
        if (cur_offset < iter->offset_)
        {
          int32_t diff = min(offset + length - cur_offset, iter->offset_ - cur_offset);
          left_length -= diff;
          memset(reinterpret_cast<char*>(buffer) + cur_pos, 0, diff);
          if (left_length <= 0)
          {
            left_length = 0;
            break;
          }
          cur_offset += diff;
          cur_pos += diff;
        }

        cur_length = min(iter->size_ - (cur_offset - iter->offset_), left_length);
        int64_t read_length = tfs_meta_manager_.read_data(ns_addr, iter->block_id_, iter->file_id_,
            reinterpret_cast<char*>(buffer) + cur_pos, (cur_offset - iter->offset_), cur_length);

        if (read_length < 0)
        {
          TBSYS_LOG(ERROR, "read tfs data failed, block_id: %u, file_id: %"PRI64_PREFIX"u",
              iter->block_id_, iter->file_id_);
          ret = read_length;
          break;
        }

        if (read_length != cur_length)
        {
          left_length -= read_length;
          TBSYS_LOG(WARN, "read tfs data return wrong length,"
              " cur_offset: %"PRI64_PREFIX"d, cur_length: %"PRI64_PREFIX"d, "
              "read_length(%"PRI64_PREFIX"d) => cur_length(%"PRI64_PREFIX"d), left_length: %"PRI64_PREFIX"d",
              cur_offset, cur_length, read_length, cur_length, left_length);
          break;
        }
        cur_pos += read_length;
        cur_offset += read_length;
        left_length -= read_length;

        if (left_length <= 0)
        {
          break;
        }
      }
      return (TFS_SUCCESS == ret) ? (length - left_length) : ret;
    }

    int64_t KvMetaClientImpl::put_object(const char *bucket_name, const char *object_name,
        const void *buffer, int64_t offset, int64_t length)
    {
      int64_t ret = EXIT_GENERAL_ERROR;
      if (!is_valid_bucket_name(bucket_name) || !is_valid_object_name(object_name))
      {
        TBSYS_LOG(ERROR, "bucket name of object name is invalid ");
        ret = EXIT_INVALID_FILE_NAME;
      }
      else if (buffer == NULL || length < 0)
      {
        TBSYS_LOG(ERROR, "invalid buffer, length: %"PRI64_PREFIX"d", length);
        ret = EXIT_INVALID_ARGU_ERROR;
      }
      else
      {
        int64_t left_length = length;
        int64_t cur_offset = offset;
        int64_t cur_pos = 0;
        // TODO cluster id
        int32_t cluster_id = tfs_meta_manager_.get_cluster_id(ns_addr_.c_str());
        do
        {
          // write MAX_BATCH_DATA_LENGTH(8M) to tfs cluster
          int64_t write_length = min(left_length, MAX_BATCH_DATA_LENGTH);
          vector<FragMeta> v_frag_meta;
          int64_t real_write_length = write_data(ns_addr_.c_str(), reinterpret_cast<const char*>(buffer) + cur_pos,
              cur_offset, write_length, &v_frag_meta);
          if (real_write_length != write_length)
          {
            TBSYS_LOG(ERROR, "write tfs data error, cur_pos: %"PRI64_PREFIX"d"
                "write_length(%"PRI64_PREFIX"d) => real_length(%"PRI64_PREFIX"d)",
                cur_pos, write_length, real_write_length);
            break;
          }
          TBSYS_LOG(DEBUG, "write tfs data, cluster_id: %d, cur_offset: %"PRI64_PREFIX"d, write_length: %"PRI64_PREFIX"d",
              cluster_id, cur_offset, write_length);

          vector<FragMeta>::iterator iter = v_frag_meta.begin();
          for (; v_frag_meta.end() != iter; iter++)
          {
            ObjectInfo object_info;
            if (0 == iter->offset_)
            {
              object_info.has_meta_info_ = true;
              object_info.has_customize_info_ = true;
              object_info.meta_info_.max_tfs_file_size_ = MAX_SEGMENT_SIZE;
              TBSYS_LOG(DEBUG, "first object info, will put meta info.");
              object_info.meta_info_.dump();
            }
            object_info.offset_ = iter->offset_;

            object_info.tfs_file_info_.block_id_ = iter->block_id_;
            object_info.tfs_file_info_.file_id_ = iter->file_id_;
            object_info.tfs_file_info_.cluster_id_ = cluster_id;

            ret = do_put_object(bucket_name, object_name, object_info);
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "do put object fail, bucket: %s, object: %s, offset: %"PRI64_PREFIX"d, ret: %d",
                  bucket_name, object_name, iter->offset_, ret);
              if (TFS_ERROR == ret)
              {
                ret = EXIT_GENERAL_ERROR;
              }
              break;
            }
          }
          left_length -= real_write_length;
          cur_pos += real_write_length;
          cur_offset += real_write_length;
        }
        while (left_length > 0 && TFS_SUCCESS == ret);
        if (TFS_SUCCESS == ret)
        {
          ret = length - left_length;
        }
      }

      return ret;
    }

    int64_t KvMetaClientImpl::get_object(const char *bucket_name, const char *object_name,
        void *buffer, int64_t offset, int64_t length,
        ObjectMetaInfo *object_meta_info, CustomizeInfo *customize_info)
    {
      int64_t ret = EXIT_GENERAL_ERROR;
      if (!is_valid_bucket_name(bucket_name) || !is_valid_object_name(object_name))
      {
        TBSYS_LOG(ERROR, "bucket name of object name is invalid ");
        ret = EXIT_INVALID_FILE_NAME;
      }
      else if (buffer == NULL || offset < 0 || length < 0)
      {
        TBSYS_LOG(ERROR, "invalid buffer, offset: %"PRI64_PREFIX"d,"
                  " length: %"PRI64_PREFIX"d", offset, length);
        ret = EXIT_INVALID_ARGU_ERROR;
      }
      else
      {
        bool still_have = false;
        int64_t left_length = length;
        int64_t read_length = 0;
        int64_t cur_offset = offset;
        int64_t cur_length = 0;
        int64_t cur_pos = 0;

        do
        {
          // get object
          ObjectInfo object_info;
          object_info.offset_ = cur_offset;
          ret = do_get_object(bucket_name, object_name, &object_info, &still_have);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "do get object fail, bucket: %s, object: %s, offset: %"PRI64_PREFIX"d, ret: %d",
                bucket_name, object_name, cur_offset, ret);
            if (TFS_ERROR == ret)
            {
              ret = EXIT_GENERAL_ERROR;
            }
            break;
          }
          if (0 == cur_offset)
          {
            if (!object_info.has_meta_info_ || !object_info.has_customize_info_)
            {
              TBSYS_LOG(ERROR, "invalid object, no meta info or customize info, bucket: %s, object: %s",
                        bucket_name, object_name);
              ret = EXIT_INVALID_OBJECT;
              break;
            }

            if (NULL != object_meta_info)
            {
              *object_meta_info = object_info.meta_info_;
              *customize_info = object_info.customize_info_;
            }
          }
          vector<FragMeta> v_frag_meta;
          FragMeta frag_meta(object_info.tfs_file_info_.block_id_,
              object_info.tfs_file_info_.file_id_,
              cur_offset,
              object_info.meta_info_.max_tfs_file_size_);
          v_frag_meta.push_back(frag_meta);

          cur_length = min(static_cast<int64_t>(object_info.meta_info_.max_tfs_file_size_), left_length);

          // read tfs
          read_length = read_data(ns_addr_.c_str(), v_frag_meta,
              reinterpret_cast<char*>(buffer) + cur_pos, cur_offset, cur_length);
          // tfs error occurs
          if (read_length < 0)
          {
            TBSYS_LOG(ERROR, "read data from tfs failed, read_length: %"PRI64_PREFIX"d", read_length);
            ret = read_length;
            break;
          }
          // one tfs file read data error, should break
          if (0 == read_length)
          {
            break;
          }

          left_length -= read_length;
          TBSYS_LOG(DEBUG, "@@ read once, offset: %ld, length %ld, read_length: %ld, left: %ld",
              cur_offset, cur_length, read_length, left_length);
          cur_offset += read_length;
          cur_pos += read_length;
        }
        while(left_length > 0 && still_have);
        if (TFS_SUCCESS == ret)
        {
          ret = (length - left_length);
        }
      }

      return ret;
    }

    TfsRetType KvMetaClientImpl::put_object(const char *bucket_name, const char *object_name,
        const char* local_file)
    {
      TfsRetType ret = TFS_SUCCESS;
      int fd = -1;
      int64_t read_len = 0, write_len = 0, offset = 0;
      if (!is_valid_bucket_name(bucket_name) || !is_valid_object_name(object_name))
      {
        TBSYS_LOG(ERROR, "bucket name or object name is invalid ");
        ret = EXIT_INVALID_FILE_NAME;
      }
      else if (NULL == local_file)
      {
        TBSYS_LOG(ERROR, "local file is null");
        ret = EXIT_INVALID_ARGU_ERROR;
      }
      else if ((fd = ::open(local_file, O_RDONLY)) < 0)
      {
        TBSYS_LOG(ERROR, "open local file %s fail: %s", local_file, strerror(errno));
        ret = EXIT_INVALID_ARGU_ERROR;
      }
      else
      {
        char* buf = new char[MAX_BATCH_DATA_LENGTH];

        while (TFS_SUCCESS == ret)
        {
          if ((read_len = ::read(fd, buf, MAX_BATCH_DATA_LENGTH)) < 0)
          {
            ret = EXIT_INVALID_ARGU_ERROR;
            TBSYS_LOG(ERROR, "read local file %s fail, error: %s", local_file, strerror(errno));
            break;
          }

          if (0 == read_len)
          {
            break;
          }
          while (read_len > 0)
          {
            write_len = put_object(bucket_name, object_name, buf, offset, read_len);
            if (write_len <= 0)
            {
              TBSYS_LOG(ERROR, "put object fail. bucket: %s, object: %s", bucket_name, object_name);
              ret = TFS_ERROR;
              break;
            }

            offset += write_len;
            read_len -= write_len;
          }
        }

        tbsys::gDeleteA(buf);
        ::close(fd);
      }

      return ret;
    }

    TfsRetType KvMetaClientImpl::get_object(const char *bucket_name, const char *object_name,
        const char* local_file,
        ObjectMetaInfo *object_meta_info, CustomizeInfo *customize_info)
    {
      TfsRetType ret = TFS_SUCCESS;
      int fd = -1;
      int64_t offset = 0;
      if (!is_valid_bucket_name(bucket_name) || !is_valid_object_name(object_name))
      {
        TBSYS_LOG(ERROR, "bucket name or object name is invalid ");
        ret = EXIT_INVALID_FILE_NAME;
      }
      else if (NULL == local_file)
      {
        TBSYS_LOG(ERROR, "local file is null");
        ret = EXIT_INVALID_ARGU_ERROR;
      }
      else if ((fd = ::open(local_file, O_WRONLY|O_CREAT, 0644)) < 0)
      {
        TBSYS_LOG(ERROR, "open local file %s to write fail: %s", local_file, strerror(errno));
        ret = EXIT_INVALID_ARGU_ERROR;
      }
      else
      {
        int32_t io_size = MAX_READ_SIZE;

        char* buf = new char[io_size];
        int64_t read_len = 0, write_len = 0;
        while (1)
        {
          read_len = get_object(bucket_name, object_name, buf, offset, io_size, object_meta_info, customize_info);
          if (read_len < 0)
          {
            ret = read_len;
            TBSYS_LOG(ERROR, "get object fail. bucket: %s, object: %s, ret: %d", bucket_name, object_name, ret);
            break;
          }

          if (0 == read_len)
          {
            break;
          }

          if ((write_len = ::write(fd, buf, read_len)) != read_len)
          {
            TBSYS_LOG(ERROR, "write local file %s fail, write len: %"PRI64_PREFIX"d, ret: %"PRI64_PREFIX"d, error: %s",
                local_file, read_len, write_len, strerror(errno));
            ret = TFS_ERROR;
            break;
          }
          offset += read_len;
        }
        tbsys::gDeleteA(buf);
        ::close(fd);
      }

      return ret;
    }

    TfsRetType KvMetaClientImpl::del_object(const char *bucket_name, const char *object_name)
    {
      TfsRetType ret = TFS_ERROR;
      if (!is_valid_bucket_name(bucket_name) || !is_valid_object_name(object_name))
      {
        TBSYS_LOG(ERROR, "bucket name or object name is invalid ");
        ret = EXIT_INVALID_FILE_NAME;
      }
      else
      {
        ret = do_del_object(bucket_name, object_name);

        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "del object failed. bucket: %s, object: %s", bucket_name, object_name);
        }
      }

      return ret;
    }

    int KvMetaClientImpl::do_put_bucket(const char *bucket_name)
    {
      return KvMetaHelper::do_put_bucket(kms_id_, bucket_name);
    }

    // TODO
    int KvMetaClientImpl::do_get_bucket(const char *bucket_name, const char *prefix,
                                        const char *start_key, const char delimiter, const int32_t limit,
                                        vector<ObjectMetaInfo> *v_object_meta_info,
                                        vector<string> *v_object_name, set<string> *s_common_prefix,
                                        int8_t *is_truncated)
    {
      return KvMetaHelper::do_get_bucket(kms_id_, bucket_name, prefix, start_key, delimiter, limit,
          v_object_meta_info, v_object_name, s_common_prefix, is_truncated);
    }

    int KvMetaClientImpl::do_del_bucket(const char *bucket_name)
    {
      return KvMetaHelper::do_del_bucket(kms_id_, bucket_name);
    }

    int KvMetaClientImpl::do_put_object(const char *bucket_name,
        const char *object_name, const ObjectInfo &object_info)
    {
      return KvMetaHelper::do_put_object(kms_id_, bucket_name, object_name, object_info);
    }

    int KvMetaClientImpl::do_get_object(const char *bucket_name,
        const char *object_name, ObjectInfo *object_info, bool *still_have)
    {
      return KvMetaHelper::do_get_object(kms_id_, bucket_name, object_name, object_info, still_have);
    }

    int KvMetaClientImpl::do_del_object(const char *bucket_name, const char *object_name)
    {
      return KvMetaHelper::do_del_object(kms_id_, bucket_name, object_name);
    }

    // TODO
    bool KvMetaClientImpl::is_valid_bucket_name(const char *bucket_name)
    {
      bool is_valid = true;

      if (NULL == bucket_name)
      {
        is_valid = false;
      }

      /*
      int32_t len = -1;
      // len > 3 && len < 256
      if (is_valid)
      {
        len = static_cast<int32_t>(strlen(bucket_name));

        if (len < MIN_FILE_PATH_LEN || len > MAX_FILE_PATH_LEN)
        {
          is_valid = false;
        }
      }

      // start & end loweralpha or digit, other is loweralpha or digit or '.'
      // my..aws is not permit
      if (is_valid)
      {
        //handle the string like my..aws
        bool conjoin = false;
        for (int32_t i = 0; i < len; i++)
        {
          if (i == 0 || i == len - 1)
          {
            if (!islower(bucket_name[i]) && !isdigit(bucket_name[i]))
            {
              is_valid = false;
              break;
            }
          }
          else
          {
            if (!islower(bucket_name[i]) && !isdigit(bucket_name[i])
                && PERIOD != bucket_name[i] && DASH != bucket_name[i])
            {
              is_valid = false;
              break;
            }
            else if (PERIOD == bucket_name[i])
            {
              if (conjoin)
              {
                is_valid = false;
                break;
              }
              else
              {
                conjoin = true;
              }
            }
            else
            {
              conjoin = false;
            }
          }
        }
      }

      // check the form 192.234.34.45
      if (is_valid)
      {
        int32_t period_size = 0;
        int32_t digit_size = 0;
        for (int i = 0; i < len; i++)
        {
          if (isdigit(bucket_name[i]))
          {
            digit_size++;
          }
          else if (PERIOD == bucket_name[i])
          {
            period_size++;
          }
        }

        if (MIN_FILE_PATH_LEN == period_size && period_size + digit_size == len)
        {
          is_valid = false;
        }
      }
      */

      return is_valid;
    }

    // TODO
    bool KvMetaClientImpl::is_valid_object_name(const char *object_name)
    {
      bool is_valid = true;

      if (NULL == object_name)
      {
        is_valid = false;
      }
      return is_valid;
    }

  }
}

