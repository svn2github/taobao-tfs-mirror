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
*   chuyu <chuyu@taobao.com>
*      - initial release
*
*/
#include "meta_store_manager.h"

#include "common/parameter.h"
#include "common/error_msg.h"
#include "database_helper.h"
#include "meta_cache_helper.h"
#include "meta_server_service.h"

using namespace tfs::common;
namespace tfs
{
  namespace namemetaserver
  {
    using namespace std;
    MetaStoreManager::MetaStoreManager():
      mutex_count_(5), app_id_uid_mutex_(NULL)
    {
       database_pool_ = new DataBasePool();
       top_dir_name_[0] = 1; 
       top_dir_name_[1] = '/'; 
       top_dir_size_ = 2;
    }
    int MetaStoreManager::init(const int32_t pool_size)
    {
      char* conn_str[DataBasePool::MAX_POOL_SIZE];
      char* user_name[DataBasePool::MAX_POOL_SIZE];
      char* passwd[DataBasePool::MAX_POOL_SIZE];
      int32_t hash_flag[DataBasePool::MAX_POOL_SIZE];

      int32_t my_pool_size = pool_size;
      int ret = TFS_SUCCESS;

      if (pool_size > DataBasePool::MAX_POOL_SIZE)
      {
        TBSYS_LOG(INFO, "pool size is too lage set it to %d",
            DataBasePool::MAX_POOL_SIZE);
        my_pool_size = DataBasePool::MAX_POOL_SIZE;
      }
      for (int i = 0; i < pool_size; i++)
      {
        int data_base_index = i % SYSPARAM_NAMEMETASERVER.db_infos_.size();
        const NameMeatServerParameter::DbInfo& dbinfo = SYSPARAM_NAMEMETASERVER.db_infos_[data_base_index];

        conn_str[i] = (char*)malloc(100);
        snprintf(conn_str[i], 100, "%s", dbinfo.conn_str_.c_str());
        user_name[i] = (char*)malloc(100);
        snprintf(user_name[i], 100, "%s", dbinfo.user_.c_str());
        passwd[i] = (char*)malloc(100);
        snprintf(passwd[i], 100, "%s", dbinfo.passwd_.c_str());
        hash_flag[i] = dbinfo.hash_value_;
      }
      bool pool_ret = database_pool_->init_pool(pool_size,
          conn_str, user_name, passwd, hash_flag);

      if(!pool_ret)
      {
        TBSYS_LOG(ERROR, "database pool init error");
        ret = TFS_ERROR;
      }
      for (int i = 0; i < pool_size; i++)
      {
        ::free(conn_str[i]);
        ::free(user_name[i]);
        ::free(passwd[i]);
      }
      app_id_uid_mutex_ = new tbsys::CThreadMutex[mutex_count_];
      return ret;
    }

    MetaStoreManager::~MetaStoreManager()
    {
      tbsys::gDelete(database_pool_);
      tbsys::gDelete(app_id_uid_mutex_);
    }
    tbsys::CThreadMutex* MetaStoreManager::get_mutex(const int64_t app_id, const int64_t uid)
    {
      assert(NULL != app_id_uid_mutex_);
      assert(mutex_count_ > 0);
      HashHelper helper(app_id, uid);
      int32_t hash_value = tbsys::CStringUtil::murMurHash((const void*)&helper, sizeof(HashHelper))
        % mutex_count_;
      return app_id_uid_mutex_ + hash_value;
    }

    CacheRootNode* MetaStoreManager::get_root_node(const int64_t app_id, const int64_t uid)
    {
      //TODO get_root_node from lru
      //if root not exit in lru, we need ls '/' and put the result ro lru
      UNUSED(app_id);
      UNUSED(uid);
      CacheRootNode* ret = NULL;
      return ret;
    }
    int MetaStoreManager::create_top_dir(const int64_t app_id, const int64_t uid, CacheRootNode* root_node)
    {
      int ret = TFS_SUCCESS;
      assert(NULL == root_node->dir_meta_);
      ret = insert(app_id, uid, 0, NULL, 0, 0, top_dir_name_, top_dir_size_, DIRECTORY);
      if (TFS_SUCCESS == ret)
      {
        vector<MetaInfo> out_v_meta_info;
        bool still_have;
        ret = ls(app_id, uid, 0, top_dir_name_, top_dir_size_,
            NULL, 0, false, out_v_meta_info, still_have);
        if (TFS_SUCCESS == ret && !out_v_meta_info.empty())
        {
          root_node->dir_meta_ = (CacheDirMetaNode*)malloc(sizeof(CacheDirMetaNode));
          assert(NULL != root_node->dir_meta_);
          FileMetaInfo& file_info = out_v_meta_info[0].file_info_;
          root_node->dir_meta_->id_ = file_info.id_;
          root_node->dir_meta_->create_time_ = file_info.create_time_;
          root_node->dir_meta_->modify_time_ = file_info.modify_time_;
          root_node->dir_meta_->name_ = top_dir_name_; //do not free top_dir name
          root_node->dir_meta_->version_ = file_info.ver_no_;
          root_node->dir_meta_->child_dir_infos_ = NULL;
          root_node->dir_meta_->child_file_infos_ = NULL;
          root_node->dir_meta_->flag_ = 0;
        }
      }
      return ret;
    }

    int MetaStoreManager::free_root_node(CacheRootNode* root_node)
    {
      int ret = TFS_SUCCESS;
      assert(NULL != root_node);
      if (NULL != root_node->dir_meta_)
      {
        MetaCacheHelper::free(root_node->dir_meta_);
      }
      free(root_node);
      return ret;
    }


    int MetaStoreManager::select(const int64_t app_id, const int64_t uid, CacheDirMetaNode* p_dir_node,
                                 const char* name, const bool is_file, void*& ret_node)
    {
      int ret = TFS_SUCCESS;
      ret_node = NULL;
      if (NULL == name || NULL == p_dir_node)
      {
        TBSYS_LOG(ERROR, "parameter error");
        ret = TFS_ERROR;
      }
      bool got_all = false;
      if (TFS_SUCCESS == ret)
      {
        if (is_file)
        {
          CacheFileMetaNode* ret_file_node = NULL;
          ret = MetaCacheHelper::find_file(p_dir_node, name, ret_file_node);
          ret_node = ret_file_node;
        }
        else
        {
          CacheDirMetaNode* ret_dir_node = NULL;
          ret = MetaCacheHelper::find_dir(p_dir_node, name, ret_dir_node);
          ret_node = ret_dir_node;
        }
        got_all = p_dir_node->is_got_all_children();
      }
      if (TFS_SUCCESS == ret)
      {
        if (NULL == ret_node && !got_all)
        {
          assert (app_id != 0);
          assert (uid != 0);
          ret = get_all_children_from_db(app_id, uid, p_dir_node);
          if (TFS_SUCCESS == ret)
          {
            ret = select(0, 0, p_dir_node, name, is_file, ret_node);
          }
        }
      }
      return ret;
    }


    //always return all children 
    int MetaStoreManager::ls(const int64_t app_id, const int64_t uid, CacheDirMetaNode* p_dir_node,
                             const char* name, const bool is_file,
                             std::vector<common::MetaInfo>& out_v_meta_info, bool& still_have)
    {
      int ret = TFS_SUCCESS;
      //TODO if we had not got p_dir_node's children we should got all children and put them into cache
      //out_v_meta_info's size should be limited in MAX_OUT_INFO_COUNT
      //out_v_meta_info will get the infos, which one >= name
      UNUSED(app_id);
      UNUSED(uid);
      UNUSED(p_dir_node);
      UNUSED(name);
      UNUSED(is_file);
      UNUSED(out_v_meta_info);
      UNUSED(still_have);
      return ret;

    }
    int MetaStoreManager::insert(const int64_t app_id, const int64_t uid,
                                 CacheDirMetaNode* p_p_dir_node, CacheDirMetaNode* p_dir_node,
                                 const char* name, const common::FileType type, common::MetaInfo* meta_info)
    {
      int ret = TFS_SUCCESS;
      int32_t p_name_len = 0, name_len = 0;
      //int64_t dir_id = 0;
      int64_t pp_id = 0;
      //we only cache the first line for file
      if (NULL == p_dir_node)
      {
        TBSYS_LOG(ERROR, "prameters err");
        ret = TFS_ERROR;
      }
      if (NULL != p_p_dir_node)
      {
        pp_id = p_p_dir_node->id_;
      }
      if (TFS_SUCCESS == ret)
      {
        p_name_len = FileName::length(p_dir_node->name_);
        name_len = FileName::length(name);
        ret = insert(app_id, uid, pp_id, p_dir_node->name_,
                     p_name_len, p_dir_node->id_,
                     name, name_len, type, meta_info);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "insert into db error");
        }
        else
        {
          vector<MetaInfo> out_v_meta_info;
          bool still_have;
          char name_end[MAX_META_FILE_NAME_LEN+16];
          int32_t name_end_len = 0;
          MetaServerService::next_file_name_base_on(name_end, name_end_len, name, name_len);
          ret = ls(app_id, uid, p_dir_node->id_, name, name_len, 
              name_end, name_end_len,
              type != DIRECTORY, out_v_meta_info, still_have);
          if (TFS_SUCCESS == ret )
          {
            assert(!out_v_meta_info.empty());
            FileMetaInfo& file_info = out_v_meta_info[0].file_info_;
            if (DIRECTORY == type)
            {
              CacheDirMetaNode* dir_node =
                static_cast<CacheDirMetaNode*>(MemHelper::malloc(sizeof(CacheDirMetaNode), CACHE_DIR_META_NODE));
              dir_node->id_ = file_info.id_;
              dir_node->create_time_ = file_info.create_time_;
              dir_node->modify_time_ = file_info.modify_time_;
              dir_node->version_ = file_info.ver_no_;
              dir_node->flag_ = 0;
              dir_node->name_ = static_cast<char*>(MemHelper::malloc(name_len));
              memcpy(dir_node->name_, name, name_len);
              if ((ret == MetaCacheHelper::insert_dir(p_dir_node, dir_node)) != TFS_SUCCESS)
              {
                TBSYS_LOG(ERROR, "insert dir error");
                MemHelper::free(dir_node->name_);
                MemHelper::free(dir_node, CACHE_DIR_META_NODE);
              }
            }
            else
            {
              int64_t buff_len = 0;
              //find file info
              CacheFileMetaNode* file_node;
              ret = MetaCacheHelper::find_file(p_dir_node, name, file_node);
              if (TFS_SUCCESS == ret && NULL != file_node)
              {
                file_node->size_ = -1;
                //replace info if cur is the first line
                if (*((unsigned char*)name) + 1 == name_len)
                {
                  file_node->create_time_ = file_info.create_time_;
                  file_node->modify_time_ = file_info.modify_time_;
                  file_node->version_ = file_info.ver_no_;
                  buff_len = out_v_meta_info[0].frag_info_.get_length();
                  ::free(file_node->meta_info_);
                  file_node->meta_info_ = (char*)MemHelper::malloc(buff_len);
                  assert(NULL != file_node->meta_info_);
                  int64_t pos = 0;
                  assert(TFS_SUCCESS == out_v_meta_info[0].frag_info_.serialize(
                        file_node->meta_info_, buff_len, pos));
                }
              }
              else
              {
                file_node =
                  static_cast<CacheFileMetaNode*>(MemHelper::malloc(sizeof(CacheFileMetaNode), CACHE_FILE_META_NODE));
                file_node->size_ = -1;
                file_node->create_time_ = file_info.create_time_;
                file_node->modify_time_ = file_info.modify_time_;
                buff_len = out_v_meta_info[0].frag_info_.get_length();
                file_node->meta_info_ = (char*)MemHelper::malloc(buff_len);
                assert(NULL != file_node->meta_info_);
                int64_t pos = 0;
                assert(TFS_SUCCESS == out_v_meta_info[0].frag_info_.serialize(
                      file_node->meta_info_, buff_len, pos));
                file_node->version_ = file_info.ver_no_;
                file_node->name_ = static_cast<char*>(MemHelper::malloc(name_len));
                memcpy(file_node->name_, name, name_len);
                if ((ret == MetaCacheHelper::insert_file(p_dir_node, file_node)) != TFS_SUCCESS)
                {
                  TBSYS_LOG(ERROR, "insert file error");
                  MemHelper::free(file_node->meta_info_);
                  MemHelper::free(file_node->name_);
                  MemHelper::free(file_node, CACHE_FILE_META_NODE);
                }
              }
            }
          }
        }
      }
      return ret;
    }

    //this will replace MetaServerService::get_meta_info func
    int MetaStoreManager::get_file_frag_info(const int64_t app_id, const int64_t uid, 
                                             CacheDirMetaNode* p_dir_node, CacheFileMetaNode* file_node,
                                             const int64_t offset, std::vector<common::MetaInfo>& out_v_meta_info,
                                             int32_t& cluster_id, int64_t& last_offset)
    {
      int ret = TFS_SUCCESS;
      assert (NULL != p_dir_node);
      assert (NULL != file_node);
      cluster_id = -1;
      last_offset = 0;
      assert(NULL != file_node->meta_info_);
      assert(-1 != file_node->size_);
      if (0 == file_node->size_)
      {
        //this means the file have no frag info, file size is 0
      }
      else
      {
        MetaInfo meta_info;
        int64_t pos = 0;
        meta_info.frag_info_.deserialize(file_node->meta_info_, MAX_FRAG_INFO_SIZE, pos);
        if (offset < meta_info.frag_info_.get_last_offset())
        {
          //TODO meta_info.file_info_
          cluster_id = meta_info.frag_info_.cluster_id_;
          last_offset = meta_info.frag_info_.get_last_offset();
          out_v_meta_info.push_back(meta_info);
        }
        else if (meta_info.frag_info_.had_been_split_)
        {
          int32_t name_len = *((unsigned char*)file_node->name_) + 1;
          ret = get_meta_info_from_db(app_id, uid, p_dir_node->id_, 
              file_node->name_, name_len, offset, out_v_meta_info, cluster_id, last_offset);
        }
      }
      return ret;
    }

    int MetaStoreManager::get_meta_info_from_db(const int64_t app_id, const int64_t uid, const int64_t pid,
        const char* name, const int32_t name_len,
        const int64_t offset, std::vector<MetaInfo>& tmp_v_meta_info,
        int32_t& cluster_id, int64_t& last_offset)
    {
      int ret = TFS_ERROR;
      char search_name[MAX_META_FILE_NAME_LEN + 8];
      int32_t search_name_len = name_len;
      assert(name_len <= MAX_META_FILE_NAME_LEN);
      memcpy(search_name, name, search_name_len);
      bool still_have = false;
      last_offset = 0;
      cluster_id = -1;
      do
      {
        tmp_v_meta_info.clear();
        still_have = false;
        ret = select(app_id, uid, pid,
            search_name, search_name_len, true, tmp_v_meta_info);
        TBSYS_LOG(DEBUG, "select size: %zd", tmp_v_meta_info.size());
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get read meta info fail, name: %s, ret: %d", search_name, ret);
          break;
        }
        if (!tmp_v_meta_info.empty())
        {
          const MetaInfo& last_metaInfo = tmp_v_meta_info[tmp_v_meta_info.size() - 1];
          cluster_id = last_metaInfo.frag_info_.cluster_id_;
          last_offset = last_metaInfo.get_last_offset();

          // offset is -1 means file's max offset
          if ((-1 == offset || last_offset <= offset) &&
              last_metaInfo.frag_info_.had_been_split_)
          {
            still_have = true;
            MetaServerService::next_file_name_base_on(search_name, search_name_len,
                last_metaInfo.get_name(), last_metaInfo.get_name_len());
          }
        }
      } while(TFS_SUCCESS == ret && still_have);

      return ret;
    }

    ////////////////////////////////////////
    //database about
    int MetaStoreManager::select(const int64_t app_id, const int64_t uid, const int64_t pid, const char* name, const int32_t name_len, const bool is_file, std::vector<MetaInfo>& out_v_meta_info)
    {
      int ret = TFS_ERROR;
      out_v_meta_info.clear();
      bool still_have = false;
      char name_end[MAX_META_FILE_NAME_LEN+16];
      if (NULL == name || (unsigned char)name[0] + 1 > MAX_META_FILE_NAME_LEN)
      {
        TBSYS_LOG(ERROR, "parameters error");
      }
      else
      {
        //make name_end [len]xxxxxxxx[max_offset] 
        memcpy(name_end, name, name_len);
        char* p = name_end + (unsigned char)name[0] + 1;
        int32_t name_end_len = name_len;
        name_end_len = (unsigned char)name[0] + 1 + 8;
        MetaServerService::int64_to_char(p, 8, -1L);
        ret = ls(app_id, uid, pid, name, name_len, name_end, name_end_len, 
            is_file, out_v_meta_info, still_have);
      }
      return ret;
    }

    int MetaStoreManager::ls(const int64_t app_id, const int64_t uid, const int64_t pid, 
        const char* name, const int32_t name_len, 
        const char* name_end, const int32_t name_end_len,
        const bool is_file, std::vector<MetaInfo>& out_v_meta_info, bool& still_have)
    {
      int ret = TFS_ERROR;
      still_have = false;
      out_v_meta_info.clear();
      int64_t real_pid = 0;
      if (is_file)
      {
        real_pid = pid | (1L<<63);
      }
      else
      {
        real_pid = pid & ~(1L<<63);
      }

      DatabaseHelper* database_helper = NULL;
      database_helper = database_pool_->get(database_pool_->get_hash_flag(app_id, uid));
      if (NULL != database_helper)
      {
        ret = database_helper->ls_meta_info(out_v_meta_info, app_id, uid, real_pid, 
            name, name_len, name_end, name_end_len);
        database_pool_->release(database_helper);
      }

      if (static_cast<int32_t>(out_v_meta_info.size()) >= ROW_LIMIT)
      {
        still_have = true;
      }
      return ret;
    }

    int MetaStoreManager::insert(const int64_t app_id, const int64_t uid,
        const int64_t ppid, const char* pname, const int32_t pname_len,
        const int64_t pid, const char* name, const int32_t name_len, const FileType type, MetaInfo* meta_info)
    {
      int ret = TFS_ERROR;
      int status = TFS_ERROR;
      int64_t proc_ret = EXIT_UNKNOWN_SQL_ERROR;
      int64_t id = 0;
      DatabaseHelper* database_helper = NULL;
      if (type == DIRECTORY)
      {
        database_helper = database_pool_->get(0);
        if (NULL != database_helper)
        {
          database_helper->get_nex_val(id);
          database_pool_->release(database_helper);
          database_helper = NULL;
        }
      }
      database_helper = database_pool_->get(database_pool_->get_hash_flag(app_id, uid));
      if (NULL != database_helper)
      {
        if (type == NORMAL_FILE)
        {
          status = database_helper->create_file(app_id, uid, ppid, pid, pname, pname_len, name, name_len, proc_ret);
          if (TFS_SUCCESS != status)
          {
            TBSYS_LOG(DEBUG, "database helper create file, status: %d", status);
          }
        }
        else if (type == DIRECTORY)
        {
          if (id != 0)
          {
            PROFILER_BEGIN("create_dir");
            status = database_helper->create_dir(app_id, uid, ppid, pname, pname_len, pid, id, name, name_len, proc_ret);
            PROFILER_END();
            if (TFS_SUCCESS != status)
            {
              TBSYS_LOG(DEBUG, "database helper create dir, status: %d", status);
            }
          }
        }
        else if (type == PWRITE_FILE)
        {
          if (NULL == meta_info)
          {
            TBSYS_LOG(ERROR, "meta_info should not be NULL");
          }
          else
          {
            int64_t frag_len = meta_info->frag_info_.get_length();
            if (frag_len > MAX_FRAG_INFO_SIZE)
            {
              TBSYS_LOG(ERROR, "meta info is too long(%d > %d)", frag_len, MAX_FRAG_INFO_SIZE);
              ret = TFS_ERROR;
            }
            else
            {
              int64_t pos = 0;
              char* frag_info = (char*) malloc(frag_len);
              if (NULL == frag_info)
              {
                TBSYS_LOG(ERROR, "mem not enough");
                ret = TFS_ERROR;
              }
              else
              {
                status = meta_info->frag_info_.serialize(frag_info, frag_len, pos);
                if (TFS_SUCCESS != status)
                {
                  TBSYS_LOG(ERROR, "get meta info failed, status: %d ", status);
                }
                else
                {
                  status = database_helper->pwrite_file(app_id, uid, pid, name, name_len,
                      meta_info->file_info_.size_, meta_info->file_info_.ver_no_, frag_info, frag_len, proc_ret);
                  if (TFS_SUCCESS != status)
                  {
                    TBSYS_LOG(DEBUG, "database helper pwrite file, status: %d", status);
                  }
                }
                free (frag_info);
              }
            }
          }
        }

        ret = get_return_status(status, proc_ret);
        database_pool_->release(database_helper);
      }

      return ret;
    }

    int MetaStoreManager::update(const int64_t app_id, const int64_t uid,
        const int64_t s_ppid, const int64_t s_pid, const char* s_pname, const int32_t s_pname_len,
        const int64_t d_ppid, const int64_t d_pid, const char* d_pname, const int32_t d_pname_len,
        const char* s_name, const int32_t s_name_len,
        const char* d_name, const int32_t d_name_len,
        const FileType type)
    {
      int ret = TFS_ERROR;
      int status = TFS_ERROR;
      int64_t proc_ret = EXIT_UNKNOWN_SQL_ERROR;

      DatabaseHelper* database_helper = NULL;
      database_helper = database_pool_->get(database_pool_->get_hash_flag(app_id, uid));
      if (NULL != database_helper)
      {
        if (type & NORMAL_FILE)
        {
          status = database_helper->mv_file(app_id, uid, s_ppid, s_pid, s_pname, s_pname_len,
              d_ppid, d_pid, d_pname, d_pname_len, s_name, s_name_len, d_name, d_name_len, proc_ret);
          if (TFS_SUCCESS != status)
          {
            TBSYS_LOG(DEBUG, "database helper mv file, status: %d", status);
          }
        }
        else if (type & DIRECTORY)
        {
          status = database_helper->mv_dir(app_id, uid, s_ppid, s_pid, s_pname, s_pname_len,
              d_ppid, d_pid, d_pname, d_pname_len, s_name, s_name_len, d_name, d_name_len, proc_ret);
          if (TFS_SUCCESS != status)
          {
            TBSYS_LOG(DEBUG, "database helper mv dir, status: %d", status);
          }
        }

        ret = get_return_status(status, proc_ret);
        database_pool_->release(database_helper);
      }

      return ret;
    }

    int MetaStoreManager::remove(const int64_t app_id, const int64_t uid, const int64_t ppid,
        const char* pname, const int64_t pname_len, const int64_t pid, const int64_t id,
        const char* name, const int64_t name_len,
        const FileType type)
    {
      int ret = TFS_ERROR;
      int status = TFS_ERROR;
      int64_t proc_ret = EXIT_UNKNOWN_SQL_ERROR;

      DatabaseHelper* database_helper = NULL;
      database_helper = database_pool_->get(database_pool_->get_hash_flag(app_id, uid));
      if (NULL != database_helper)
      {
        if (type & NORMAL_FILE)
        {
          status = database_helper->rm_file(app_id, uid, ppid, pid, pname, pname_len, name, name_len, proc_ret);
          if (TFS_SUCCESS != status)
          {
            TBSYS_LOG(DEBUG, "database helper rm file, status: %d", status);
          }
        }
        else if (type & DIRECTORY)
        {
          if (id == 0)
          {
            TBSYS_LOG(DEBUG, "wrong type, target is file.");
          }
          else
          {
            PROFILER_BEGIN("rm_dir");
            status = database_helper->rm_dir(app_id, uid, ppid, pname, pname_len, pid, id, name, name_len, proc_ret);
            PROFILER_END();
            if (TFS_SUCCESS != status)
            {
              TBSYS_LOG(DEBUG, "database helper rm dir, status: %d", status);
            }
          }
        }

        ret = get_return_status(status, proc_ret);
        database_pool_->release(database_helper);
      }

      return ret;
    }

    // proc_ret depend database's logic
    // database level and manager level should be be consensus about error code definition,
    // otherwise, manager level SHOULD merge difference
    int MetaStoreManager::get_return_status(const int status, const int proc_ret)
    {
      int ret = TFS_SUCCESS;
      // maybe network fail
      if (status != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "ret is %d when we ask data from database");
        // // TODO. maybe do sth.
        // ret = status;
      }
      if (proc_ret < 0)
      {
        ret = proc_ret;
      }

      return ret;
    }

    int MetaStoreManager::get_all_children_from_db(const int64_t app_id, const int64_t uid,
        CacheDirMetaNode* p_dir_node)
    {
      int ret = TFS_ERROR;
      assert (NULL != p_dir_node);
      std::vector<MetaInfo> tmp_v_meta_info;
      vector<MetaInfo>::iterator tmp_v_meta_info_it;
      char name[MAX_META_FILE_NAME_LEN + 16];
      int32_t name_len = 1;
      name[0] = '\0';
      bool still_have = false;
      int my_file_type = DIRECTORY;
      do
      {
        tmp_v_meta_info.clear();
        still_have = false;
        ret = ls(app_id, uid, p_dir_node->id_, 
            name, name_len, NULL, 0,
            my_file_type != DIRECTORY, tmp_v_meta_info, still_have);

        if (!tmp_v_meta_info.empty())
        {
          tmp_v_meta_info_it = tmp_v_meta_info.begin();

          if (my_file_type != DIRECTORY)
          {
            // fill file meta info
            ret = fill_file_meta_info(tmp_v_meta_info_it, tmp_v_meta_info.end(), p_dir_node);
            if (TFS_SUCCESS != ret)
            {
              break;
            }
          }
          else
          {
            ret = fill_dir_info(tmp_v_meta_info_it, tmp_v_meta_info.end(), p_dir_node);
          }

          // still have and need continue
          if (still_have)
          {
            tmp_v_meta_info_it--;
            MetaServerService::next_file_name_base_on(name, name_len,
                tmp_v_meta_info_it->get_name(), tmp_v_meta_info_it->get_name_len());
          }
        }

        // directory over, continue list file
        if (my_file_type == DIRECTORY && !still_have)
        {
          my_file_type = NORMAL_FILE;
          still_have = true;
          name[0] = '\0';
          name_len = 1;
        }
      } while (TFS_SUCCESS == ret && still_have);
      if (TFS_SUCCESS == ret)
      {
        p_dir_node->set_got_all_children();
      }

      return ret;
    }
    int MetaStoreManager::fill_file_meta_info(std::vector<common::MetaInfo>::iterator& meta_info_begin,
        const std::vector<common::MetaInfo>::iterator meta_info_end,
        CacheDirMetaNode* p_dir_node)
    {
      int ret = TFS_SUCCESS;
      assert(NULL != p_dir_node);
      for (; meta_info_begin != meta_info_end; meta_info_begin++)
      {
        CacheFileMetaNode* file_meta;
        ret = MetaCacheHelper::find_file(p_dir_node, 
            meta_info_begin->file_info_.name_.c_str(), file_meta);
        if (TFS_SUCCESS != ret)
        {
          break;
        }
        // is the first line of file?
        if (meta_info_begin->file_info_.name_.length() > 0
            && *((unsigned char*)(meta_info_begin->file_info_.name_.c_str())) == meta_info_begin->file_info_.name_.length() -1)
        {
          //first line of file, insert it;
          if (NULL == file_meta)
          {
            file_meta = (CacheFileMetaNode*)malloc(sizeof(CacheFileMetaNode));
            assert(NULL != file_meta);
            file_meta->name_ = (char*)malloc(meta_info_begin->file_info_.name_.length());
            memcpy(file_meta->name_, meta_info_begin->file_info_.name_.c_str(), 
                meta_info_begin->file_info_.name_.length());
            file_meta->version_ = meta_info_begin->file_info_.ver_no_;

            int64_t buff_len = 0;
            int64_t pos = 0;
            file_meta->meta_info_ = NULL;
            buff_len = meta_info_begin->frag_info_.get_length();
            file_meta->meta_info_ = (char*)malloc(buff_len);
            assert(NULL != file_meta->meta_info_);
            assert(TFS_SUCCESS == 
                meta_info_begin->frag_info_.serialize(file_meta->meta_info_, buff_len, pos));

            ret = MetaCacheHelper::insert_file(p_dir_node, file_meta);
            if (TFS_SUCCESS != ret)
            {
              free(file_meta->meta_info_);
              free(file_meta->name_);
              free(file_meta);
              break;
            }
          }
        }
        assert (NULL != file_meta);
        file_meta->size_ = meta_info_begin->frag_info_.get_last_offset();
        file_meta->create_time_ = meta_info_begin->file_info_.create_time_;
        file_meta->modify_time_ = meta_info_begin->file_info_.modify_time_;
      }
      return ret;
    }
    int MetaStoreManager::fill_dir_info(std::vector<common::MetaInfo>::iterator& meta_info_begin,
        const std::vector<common::MetaInfo>::iterator meta_info_end,
        CacheDirMetaNode* p_dir_node)
    {
      int ret = TFS_SUCCESS;
      assert(NULL != p_dir_node);
      for (; meta_info_begin != meta_info_end; meta_info_begin++)
      {
        CacheDirMetaNode* dir_meta;
        ret = MetaCacheHelper::find_dir(p_dir_node, 
            meta_info_begin->file_info_.name_.c_str(), dir_meta);
        if (TFS_SUCCESS != ret)
        {
          break;
        }
        if (NULL == dir_meta)
        {
          //insert a dir meta
          dir_meta = (CacheDirMetaNode*)malloc(sizeof(CacheDirMetaNode));
          assert(NULL != dir_meta);
          dir_meta->id_= meta_info_begin->file_info_.id_;
          dir_meta->name_ = (char*)malloc(meta_info_begin->file_info_.name_.length());
          memcpy(dir_meta->name_, meta_info_begin->file_info_.name_.c_str(), 
              meta_info_begin->file_info_.name_.length());

          dir_meta->flag_ = 0;
          dir_meta->child_dir_infos_ = dir_meta->child_file_infos_ = NULL;
          ret = MetaCacheHelper::insert_dir(p_dir_node, dir_meta);
          if (TFS_SUCCESS != ret)
          {
            free(dir_meta->name_);
            free(dir_meta);
            break;
          }
        }
        assert (NULL != dir_meta);
        dir_meta->create_time_ = meta_info_begin->file_info_.create_time_;
        dir_meta->modify_time_ = meta_info_begin->file_info_.modify_time_;
        dir_meta->version_ = meta_info_begin->file_info_.ver_no_;
      }
      return ret;
    }

  }
}
