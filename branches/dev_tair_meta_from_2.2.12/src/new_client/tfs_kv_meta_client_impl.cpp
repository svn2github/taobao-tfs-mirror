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
    {
      packet_factory_ = new message::MessageFactory();
      packet_streamer_ = new common::BasePacketStreamer(packet_factory_);
    }

    KvMetaClientImpl::~KvMetaClientImpl()
    {
      tbsys::gDelete(packet_factory_);
      tbsys::gDelete(packet_streamer_);
    }

    int KvMetaClientImpl::initialize(const char* kms_addr)
    {
      int ret = TFS_SUCCESS;
      if (kms_addr == NULL)
      {
        TBSYS_LOG(WARN, "kms_addr is null");
        ret = EXIT_INVALID_ARGU_ERROR;
      }
      else
      {
        ret = initialize(Func::get_host_ip(kms_addr));
      }
      return ret;
    }

    int KvMetaClientImpl::initialize(const int64_t kms_addr)
    {
      int ret = TFS_SUCCESS;
      ret = common::NewClientManager::get_instance().initialize(packet_factory_, packet_streamer_);
      if (TFS_SUCCESS == ret)
      {
        kms_id_ = kms_addr;
        ret = tfs_meta_manager_.initialize();
      }
      return ret;
    }

    TfsRetType KvMetaClientImpl::put_bucket(const char *bucket_name)
    {
       TfsRetType ret = TFS_ERROR;

       if (!is_valid_bucket_name(bucket_name))
       {
         TBSYS_LOG(ERROR, "bucket: %s is invalid", bucket_name);
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

    TfsRetType KvMetaClientImpl::get_bucket(const char *bucket_name, const char* prefix,
                                            const char* start_key, const int32_t limit,
                                            vector<string>& v_object_name)
    {
       TfsRetType ret = TFS_ERROR;

       if (!is_valid_file_path(bucket_name))
       {
         //TBSYS_LOG();
       }
       else
       {
         ret = TFS_SUCCESS;
       }

       if (TFS_SUCCESS == ret)
       {
         ret = do_get_bucket(bucket_name, prefix, start_key, limit, v_object_name);
       }

       return ret;
    }

    TfsRetType KvMetaClientImpl::del_bucket(const char *bucket_name)
    {
       TfsRetType ret = TFS_ERROR;

       if (!is_valid_file_path(bucket_name))
       {
         //TBSYS_LOG();
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

    TfsRetType KvMetaClientImpl::put_object(const char *bucket_name, const char *object_name,
        const char* local_file)
    {
      TfsRetType ret = TFS_ERROR;
      UNUSED(local_file);
      if (!is_valid_file_path(bucket_name) || !is_valid_file_path(object_name))
      {
        TBSYS_LOG(ERROR, "bucket name or object name is invalid ");
        ret = EXIT_INVALID_FILE_NAME;
      }
      else
      {
        TfsFileInfo tfs_file_info;
        tfs_file_info.block_id_ = 1;
        tfs_file_info.file_id_ = 1;
        tfs_file_info.cluster_id_ = 1;
        ObjectMetaInfo object_meta_info;
        CustomizeInfo customize_info;
        ret = do_put_object(bucket_name, object_name, tfs_file_info, object_meta_info, customize_info);

        if (TFS_SUCCESS != ret)
        {
            TBSYS_LOG(ERROR, "put object failed");
        }
      }
      return ret;
    }

    /*TfsRetType KvMetaClientImpl::put_object(const char *bucket_name, const char *object_name,
        const char* local_file)
    {
      int ret = TFS_ERROR;
      int fd = -1;
      int64_t read_len = 0, write_len = 0, pos = 0, off_set = 0;
      if (!is_valid_file_path(object_name))
      {
        TBSYS_LOG(ERROR, "file name is invalid ");
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
          pos = 0;
          while (read_len > 0)
          {
            // TODO: save raw tfs and get tfs_file_info
            //write_len = write(ns_addr, app_id, uid, tfs_name, buf, off_set, read_len);
            //if (write_len <= 0)
            //{
            //  TBSYS_LOG(ERROR, "write to tfs fail");
            //  ret = write_len;
            //  break;
            //}

            // TODO: then write to kv_meta server

            //off_set += write_len;
            //pos += write_len;
            //read_len -= write_len;
          }

        }

        tbsys::gDeleteA(buf);
      }

      if (fd > 0)
      {
        ::close(fd);
      }

      return ret != TFS_SUCCESS ? ret : off_set;
    }*/

    TfsRetType KvMetaClientImpl::get_object(const char *bucket_name, const char *object_name,
        const char* local_file)
    {
      TfsRetType ret = TFS_ERROR;
      UNUSED(bucket_name);
      UNUSED(object_name);
      UNUSED(local_file);
      return ret;
    }

    TfsRetType KvMetaClientImpl::del_object(const char *bucket_name, const char *object_name)
    {
      TfsRetType ret = TFS_ERROR;
      UNUSED(bucket_name);
      UNUSED(object_name);
      return ret;
    }

    int KvMetaClientImpl::do_put_bucket(const char *bucket_name)
    {
      return KvMetaHelper::do_put_bucket(kms_id_, bucket_name);
    }

    // TODO
    int KvMetaClientImpl::do_get_bucket(const char *bucket_name, const char* prefix,
                                        const char* start_key, const int32_t limit,
                                        vector<string>& v_object_name)
    {
      return KvMetaHelper::do_get_bucket(kms_id_, bucket_name, prefix, start_key, limit, v_object_name);
    }

    int KvMetaClientImpl::do_del_bucket(const char *bucket_name)
    {
      return KvMetaHelper::do_del_bucket(kms_id_, bucket_name);
    }

    int KvMetaClientImpl::do_put_object(const char *bucket_name,
        const char *object_name, const TfsFileInfo &tfs_file_info,
        const common::ObjectMetaInfo &object_meta_info, const common::CustomizeInfo &customize_info)
    {
      return KvMetaHelper::do_put_object(kms_id_, bucket_name, object_name, tfs_file_info, object_meta_info, customize_info);
    }

    int KvMetaClientImpl::do_get_object(const char *bucket_name,
        const char *object_name, TfsFileInfo &tfs_file_info, ObjectMetaInfo &object_meta_info, CustomizeInfo &customize_info)
    {
      return KvMetaHelper::do_get_object(kms_id_, bucket_name, object_name, tfs_file_info, object_meta_info, customize_info);
    }

    int KvMetaClientImpl::do_del_object(const char *bucket_name, const char *object_name)
    {
      return KvMetaHelper::do_del_object(kms_id_, bucket_name, object_name);
    }

    bool KvMetaClientImpl::is_valid_file_path(const char* file_path)
    {
      return ((file_path != NULL) && (strlen(file_path) > 0) && (static_cast<int32_t>(strlen(file_path)) < MAX_FILE_PATH_LEN) && (file_path[0] == '/') && (strstr(file_path, " ") == NULL));
    }

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

    // copy from NameMetaClientImpl::write_data
    /*int64_t KvMetaClientImpl::write_data(const char *ns_addr, int32_t cluster_id,
        const void *buffer, int64_t offset, int64_t length,
        FragInfo &frag_info)
    {
      int64_t ret = TFS_SUCCESS;
      int64_t write_length = 0;
      int64_t left_length = length;
      int64_t cur_offset = offset;
      int64_t cur_pos = 0;

      if (cluster_id != 0)
      {
        tfs_file_info.cluster_id_ = cluster_id;
      }
      else
      {
        tfs_file_info.cluster_id_ = tfs_meta_manager_.get_cluster_id(ns_addr);
      }
      do
      {
        // write to tfs, and get frag meta
        write_length = min(left_length, MAX_SEGMENT_LENGTH);
        FragMeta frag_meta;
        int64_t real_length = tfs_meta_manager_.write_data(ns_addr, reinterpret_cast<const char*>(buffer) + cur_pos, cur_offset, write_length, frag_meta);

        if (real_length != write_length)
        {
          TBSYS_LOG(ERROR, "write segment data failed, cur_pos : %"PRI64_PREFIX"d, write_length : %"PRI64_PREFIX"d, ret_length: %"PRI64_PREFIX"d, ret: %"PRI64_PREFIX"d",
              cur_pos, write_length, real_length, ret);
          if (real_length < 0)
          {
            ret = real_length;
          }
          break;
        }
        else
        {
          frag_info.v_frag_meta_.push_back(frag_meta);
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
    }*/
  }
}

