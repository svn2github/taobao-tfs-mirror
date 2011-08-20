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
 *    daoan<aoan@taobao.com>
 *      - initial release
 *
 */
#include <algorithm>
#include "tfs_meta_client_api_impl.h"
#include "common/func.h"
#include "tfs_client_api.h"
#include "tfs_meta_helper.h"
#include "tfs_meta_manager.h"
#include "fsname.h"

namespace tfs
{
  namespace client
  {
    using namespace tfs::common;
    using namespace std;

    NameMetaClientImpl::NameMetaClientImpl()
      :app_id_(0), user_id_(0)
    {
      memset(ns_server_ip_, 0, 64);
      v_meta_server_.clear();
    }

    NameMetaClientImpl::~NameMetaClientImpl()
    {
    }

    void NameMetaClientImpl::set_meta_servers(const char* meta_server_str)
    {
      vector<std::string> fields;
      Func::split_string(meta_server_str, ',', fields);
      vector<std::string>::iterator iter = fields.begin();
      for (; iter != fields.end(); iter++)
      {
        v_meta_server_.push_back(Func::get_host_ip((*iter).c_str()));
      }
      
    }

    void NameMetaClientImpl::set_server(const char* server_ip_port)
    {
      strcpy(ns_server_ip_, server_ip_port);
    }

    TfsRetType NameMetaClientImpl::create_dir(const char* dir_path)
    {
      return do_file_action(CREATE_DIR, dir_path);
    }

    TfsRetType NameMetaClientImpl::create_file(const char* file_path)
    {
      return do_file_action(CREATE_FILE, file_path);
    }

    TfsRetType NameMetaClientImpl::mv_dir(const char* src_dir_path, const char* dest_dir_path)
    {
      return do_file_action(MOVE_DIR, src_dir_path, dest_dir_path);
    }

    TfsRetType NameMetaClientImpl::mv_file(const char* src_file_path, const char* dest_file_path)
    {
      return do_file_action(MOVE_FILE, src_file_path, dest_file_path);
    }

    TfsRetType NameMetaClientImpl::rm_dir(const char* dir_path)
    {
      return do_file_action(REMOVE_DIR, dir_path);
    }

    TfsRetType NameMetaClientImpl::rm_file(const char* file_path)
    {
      int ret = TFS_ERROR;
      FragInfo frag_info;
      if ((ret = read_frag_info(file_path, frag_info)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "read frag info error, ret: %d", ret);
      }
      else if ((ret = do_file_action(REMOVE_FILE, file_path)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "remove file failed, file_path: %s, ret: %d", file_path, ret);
      }
      unlink_file(frag_info);
      return ret;
    }

    TfsRetType NameMetaClientImpl::ls_dir(const char* dir_path, int64_t pid, vector<FileMetaInfo>& v_file_meta_info, bool is_recursive)
    {
      int ret = TFS_SUCCESS;
      if ((ret = do_ls_ex(dir_path, DIRECTORY, pid, v_file_meta_info)) != TFS_SUCCESS)
      {
        TBSYS_LOG(WARN, "ls directory failed, dir_path: %s", dir_path);
      }

      if (is_recursive)
      {
        vector<FileMetaInfo>::iterator iter = v_file_meta_info.begin();
        vector<FileMetaInfo> sub_v_file_meta_info;
        for (; iter != v_file_meta_info.end(); iter++)
        {
          if (!(iter->is_file()))
          {
            vector<FileMetaInfo> tmp_v_file_meta_info;
            if ((ret = ls_dir(NULL, iter->pid_, tmp_v_file_meta_info, is_recursive)) != TFS_SUCCESS)
            {
              TBSYS_LOG(WARN, "ls sub directory failed, pid: %"PRI64_PREFIX"d", iter->pid_);
            }
            if (static_cast<int32_t>(tmp_v_file_meta_info.size()) <= 0)
            {
              break;
            }
            vector<FileMetaInfo>::iterator tmp_iter = tmp_v_file_meta_info.begin();
            for (; tmp_iter != tmp_v_file_meta_info.end(); tmp_iter++)
            {
              sub_v_file_meta_info.push_back(*tmp_iter);
            }
          }
        }
        for (; iter != sub_v_file_meta_info.end(); iter++)
        {
          v_file_meta_info.push_back(*iter);
        }
      }
      return ret;
    }

    TfsRetType NameMetaClientImpl::ls_file(const char* file_path, FileMetaInfo& file_meta_info)
    {
      int ret = TFS_SUCCESS;
      vector<FileMetaInfo> v_file_meta_info;
      if ((ret = do_ls_ex(file_path, NORMAL_FILE, -1, v_file_meta_info)) != TFS_SUCCESS)
      {
        TBSYS_LOG(WARN, "ls file failed, file_path: %s", file_path);
      }
      else
      {
        file_meta_info = v_file_meta_info[0];
      }
      return ret;
    }

    int64_t NameMetaClientImpl::read(const char* file_path, const int64_t offset, const int64_t length, void* buffer)
    {
      int ret = TFS_SUCCESS;
      bool still_have = false;
      int64_t left_length = length;
      int64_t cur_offset = offset;
      int64_t cur_length = 0;
      do
      {
        FragInfo frag_info;
        if ((ret = do_read(file_path, cur_offset, left_length, frag_info, still_have)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "read file failed, path: %s, ret: %d", file_path, ret);
          break;
        }

        if (frag_info.get_length() <= 0)
        {
          TBSYS_LOG(ERROR, "get frag info failed");
          break;
        }
        
        if (frag_info.get_last_offset() <= (offset + length))
        {
          break;
        }
        
        cur_length = min(frag_info.get_last_offset(), left_length);

        int64_t read_length = read_data(frag_info, buffer, cur_offset, cur_length);
        if (read_length != cur_length)
        {
          TBSYS_LOG(ERROR, "read data from tfs failed, read_length: %"PRI64_PREFIX"d", read_length);
        }
        cur_offset += read_length;
        left_length -= read_length;
      }
      while(still_have);
      return (length - left_length);
    }

    int64_t NameMetaClientImpl::write(const char* file_path, const int64_t offset, void* buffer, const int64_t pos, const int64_t length)
    {
      int ret = TFS_SUCCESS;
      int32_t cluster_id = -1;
      int64_t left_length = length;
      int64_t cur_pos = pos;
      if (!is_valid_file_path(file_path) || offset < 0)
      {
        TBSYS_LOG(ERROR, "file path is invalid or offset less then 0, offset: %"PRI64_PREFIX"d", offset);
      }
      else if (buffer == NULL || pos < 0 || length < 0)
      {
        TBSYS_LOG(ERROR, "invalid buffer, pos: %"PRI64_PREFIX"d, length %"PRI64_PREFIX"d", pos, length);
      }
      else if((cluster_id = get_cluster_id(file_path)) == -1)
      {
        TBSYS_LOG(ERROR, "get cluster id error, cluster_id: %d", cluster_id);
      }
      else
      {
        do
        {
          // write MAX_BATCH_DATA_LENGTH(8M) to tfs cluster
          int64_t write_length = min(left_length, MAX_BATCH_DATA_LENGTH);
          FragInfo frag_info;
          int64_t real_write_length = write_data(cluster_id, buffer, cur_pos, write_length, frag_info);
          if (real_write_length != write_length)
          {
            TBSYS_LOG(ERROR, "write tfs data error, cur_pos: %"PRI64_PREFIX"d" 
                "write_length(%"PRI64_PREFIX"d) => real_length(%"PRI64_PREFIX"d)", 
                cur_pos, write_length, real_write_length);
            unlink_file(frag_info);
            break;
          }
          TBSYS_LOG(DEBUG, "write tfs data, cluster_id, cur_pos: %"PRI64_PREFIX"d, write_length: %"PRI64_PREFIX"d",
              cluster_id, cur_pos, write_length);
          frag_info.dump();

          // then write to meta server
          if ((ret = write_meta_info(file_path, frag_info)) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "write meta info error, cur_pos: %"PRI64_PREFIX"d"
                "write_length(%"PRI64_PREFIX"d) => real_length(%"PRI64_PREFIX"d), ret: %d", 
                cur_pos, write_length, real_write_length, ret);
            unlink_file(frag_info);
          }
          left_length -= real_write_length;
        }
        while(left_length > 0);
      }
      return (length - left_length);
    }

    bool NameMetaClientImpl::is_valid_file_path(const char* file_path)
    {
      return ((file_path != NULL) && (strlen(file_path) > 0) && (static_cast<int32_t>(strlen(file_path)) < MAX_FILE_PATH_LEN) && (file_path[0] == '/'));
    }

    TfsRetType NameMetaClientImpl::do_file_action(MetaActionOp action, const char* path, const char* new_path)
    {
      int ret = TFS_ERROR;
      uint64_t meta_server_id = v_meta_server_[0];
      if (!is_valid_file_path(path) || ((new_path != NULL) && !is_valid_file_path(new_path)))
      {
        TBSYS_LOG(WARN, "path is invalid, old_path: %s, new_path: %s", path == NULL? "null":path, new_path == NULL? "null":new_path);
      }
      else if ((new_path != NULL) && (strcmp(path, new_path) == 0))
      {
        TBSYS_LOG(WARN, "source file path equals to destination file path: %s == %s", path, new_path);
      }
      else if ((ret = NameMetaHelper::do_file_action(meta_server_id, app_id_, user_id_, action, path, new_path)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "do file action error occured, path: %s, new_path: %s, action: %d, ret: %d", path, (new_path == NULL? "null":new_path), action, ret);
      }
      return ret;
    }
    
    int NameMetaClientImpl::read_frag_info(const char* file_path, FragInfo& frag_info)
    {
      int ret = TFS_SUCCESS;
      bool still_have = true;
      FragInfo tmp_frag_info;
      int64_t offset = 0;
      int64_t size = MAX_OUT_INFO_COUNT * sizeof(FragMeta);
      do 
      {
        tmp_frag_info.v_frag_meta_.clear();
        uint64_t meta_server_id = v_meta_server_[0];
        if ((ret = NameMetaHelper::do_read_file(meta_server_id, app_id_, user_id_, file_path, 
            offset, size, tmp_frag_info, still_have)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "read frag info error, ret: %d", ret);
        }
        offset = tmp_frag_info.get_last_offset();
        frag_info.cluster_id_ = tmp_frag_info.cluster_id_;
        frag_info.push_back(tmp_frag_info);
      }
      while ((offset != 0) && still_have);
      return ret;
    }

    int NameMetaClientImpl::unlink_file(FragInfo& frag_info)
    {
      int ret = TFS_SUCCESS;
      int tmp_ret = TFS_ERROR;
      int64_t file_size = 0;
      std::vector<FragMeta>::iterator iter = frag_info.v_frag_meta_.begin();
      for(; iter != frag_info.v_frag_meta_.end(); iter++)
      {
        FSName fsname(iter->block_id_, iter->file_id_, frag_info.cluster_id_);
        if ((tmp_ret = TfsClient::Instance()->unlink(fsname.get_name(), NULL, file_size)) != TFS_SUCCESS)
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "unlink tfs file failed, file: %s, ret: %d", fsname.get_name(), tmp_ret);
        }
      }
      return ret;
    }
    
    int NameMetaClientImpl::do_ls_ex(const char* file_path, const FileType file_type, const int64_t pid, std::vector<FileMetaInfo>& v_file_meta_info)
    {
      int ret = TFS_ERROR;
      int32_t meta_size = 0;
      bool still_have = false;

      int64_t last_pid = pid;
      FileType last_file_type = file_type;
      char last_file_path[MAX_FILE_PATH_LEN];
      strncpy(last_file_path, file_path, strlen(file_path));

      do
      {
        std::vector<MetaInfo> tmp_v_meta_info;
        std::vector<MetaInfo>::iterator iter;

        uint64_t meta_server_id = v_meta_server_[0];
        if ((ret = NameMetaHelper::do_ls(meta_server_id, app_id_, user_id_, last_file_path, last_file_type, last_pid, 
              tmp_v_meta_info, still_have)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "do ls info failed, file_path: %s, file_type: %d, ret: %d", 
              last_file_path, last_file_type, ret);
          break;
        }

        if (file_type == NORMAL_FILE)
        {
          break;
        }
        else
        {
          meta_size = static_cast<int32_t>(tmp_v_meta_info.size());

          FileMetaInfo last_file_meta_info;
          last_file_meta_info = v_file_meta_info[meta_size - 1];
          last_pid = last_file_meta_info.pid_;
          last_file_type = last_file_meta_info.is_file() ? NORMAL_FILE:DIRECTORY;
          strcpy(last_file_path, v_file_meta_info[meta_size - 1].name_.c_str());
        }
        for (; iter != tmp_v_meta_info.end(); iter++)
        {
          v_file_meta_info.push_back(iter->file_info_);
        }
      }
      while (meta_size > 0 && still_have);
      return ret;
    }

    int NameMetaClientImpl::do_read(const char* path, const int64_t offset, const int64_t size, 
        FragInfo& frag_info, bool& still_have)
    {
        uint64_t meta_server_id = v_meta_server_[0];
        return NameMetaHelper::do_read_file(meta_server_id, app_id_, user_id_,
            path, offset, size, frag_info, still_have);
    }

    int64_t NameMetaClientImpl::read_data(FragInfo& frag_info, void* buffer, int64_t pos, int64_t length)
    {
      int64_t cur_pos = pos;
      int64_t cur_length = 0;
      int64_t left_length = length;

      TfsMetaManager tfs_meta_manager;
      tfs_meta_manager.initialize(ns_server_ip_);

      vector<FragMeta> v_frag_meta = frag_info.v_frag_meta_;
      vector<FragMeta>::iterator iter = v_frag_meta.begin();
      for(; iter != v_frag_meta.end(); iter++)
      {
        if (cur_pos > iter->offset_ + iter->size_ || cur_pos < iter->offset_)
        {
          TBSYS_LOG(ERROR, "fatal error, wrong pos: %"PRI64_PREFIX"d", cur_pos);
          break;
        }
        cur_pos = max(pos, iter->offset_); // only the first time
        cur_length = min(iter->size_ - (cur_pos - iter->offset_), left_length);
        int64_t read_length = tfs_meta_manager.read_data(iter->block_id_, iter->file_id_, buffer, cur_pos, cur_length);
        if (read_length != cur_length)
        {
          left_length -= read_length;
          TBSYS_LOG(ERROR, "read tfs data failed, cur_pos: %"PRI64_PREFIX"d, cur_length: %"PRI64_PREFIX"d, "
              "read_length(%"PRI64_PREFIX"d) => cur_length(%"PRI64_PREFIX"d), left_length: %"PRI64_PREFIX"d", 
             cur_pos, cur_length, read_length, cur_length, left_length);
          break;
        }
        left_length -= read_length;

        if (left_length <= 0)
        {
          break;
        }
      }
      return (length - left_length);
    }

    int64_t NameMetaClientImpl::write_data(int32_t cluster_id, void* buffer, int64_t pos, int64_t length, 
        FragInfo& frag_info)
    {
      int ret = TFS_SUCCESS;
      int64_t write_length = 0;
      int64_t left_length = length;
      int64_t cur_pos = pos;

      TfsMetaManager tfs_meta_manager;
      tfs_meta_manager.initialize(ns_server_ip_);

      frag_info.cluster_id_ = cluster_id;
      do
      {
        // write to tfs, and get frag meta
        write_length = min(left_length, MAX_SEGMENT_LENGTH);
        FragMeta frag_meta;
        int64_t real_length = tfs_meta_manager.write_data(buffer, cur_pos, write_length, frag_meta);
        if (real_length != write_length)
        {
          TBSYS_LOG(ERROR, "write segment data failed, cur_pos, write_length, ret_length: %d, ret: %d", 
              cur_pos, write_length, real_length, ret);
          break;
        }

        if (real_length != write_length)
        {
          TBSYS_LOG(ERROR, "write segment data failed, cur_pos, write_length, ret_length: %d, ret: %d", 
              cur_pos, write_length, real_length, ret);
          break;
        }
        else
        {
          frag_info.v_frag_meta_.push_back(frag_meta);
          cur_pos += real_length;
          left_length -= real_length;
        }
      }
      while(left_length > 0);

      return length - left_length;
    }

    int NameMetaClientImpl::write_meta_info(const char* path, FragInfo& frag_info)
    {
      uint64_t meta_server_id = v_meta_server_[0];
      return NameMetaHelper::do_write_file(meta_server_id, app_id_, user_id_, path, frag_info);
    }
    int32_t NameMetaClientImpl::get_cluster_id(const char* path)
    {
      FragInfo frag_info;
      bool still_have = false;
      do_read(path, 0, 0, frag_info, still_have);
      return frag_info.cluster_id_;
    }

  }
}
