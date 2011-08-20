/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */

#include "common/base_packet.h"
#include "meta_server_service.h"

using namespace tfs::common;
using namespace std;

namespace tfs
{
  namespace namemetaserver
  {
    MetaServerService::MetaServerService() : store_manager_(NULL)
    {
      store_manager_ =  new MetaStoreManager();
    }

    MetaServerService::~MetaServerService()
    {
      tbsys::gDelete(store_manager_);
    }

    tbnet::IPacketStreamer* MetaServerService::create_packet_streamer()
    {
      return new common::BasePacketStreamer();
    }

    void MetaServerService::destroy_packet_streamer(tbnet::IPacketStreamer* streamer)
    {
      tbsys::gDelete(streamer);
    }

    common::BasePacketFactory* MetaServerService::create_packet_factory()
    {
      return new message::MessageFactory();
    }

    void MetaServerService::destroy_packet_factory(common::BasePacketFactory* factory)
    {
      tbsys::gDelete(factory);
    }

    const char* MetaServerService::get_log_file_path()
    {
      return NULL;
    }

    const char* MetaServerService::get_pid_file_path()
    {
      return NULL;
    }

    int MetaServerService::initialize(int argc, char* argv[])
    {
      UNUSED(argc);
      UNUSED(argv);
      return TFS_SUCCESS;
    }

    int MetaServerService::destroy_service()
    {
      return TFS_SUCCESS;
    }

    bool MetaServerService::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      int ret = true;
      BasePacket* base_packet = NULL;
      if (!(ret = BaseService::handlePacketQueue(packet, args)))
      {
        TBSYS_LOG(ERROR, "call BaseService::handlePacketQueue fail. ret: %d", ret);
      }
      else
      {
        base_packet = dynamic_cast<BasePacket*>(packet);
        switch (base_packet->getPCode())
        {
        default:
          ret = EXIT_UNKNOWN_MSGTYPE;
          TBSYS_LOG(ERROR, "unknown msg type: %d", base_packet->getPCode());
          break;
        }
      }

      if (ret != TFS_SUCCESS && NULL != base_packet)
      {
        base_packet->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "execute message failed");
      }

      // always return true. packet will be freed by caller
      return true;
    }

    int MetaServerService::create(const int64_t app_id, const int64_t uid,
                                  const char* file_path, const FileType type)
    {
      char name[MAX_FILE_PATH_LEN], pname[MAX_FILE_PATH_LEN];
      int32_t name_len = 0, pname_len = 0;
      int ret = TFS_SUCCESS;

      MetaInfo p_meta_info;
      std::vector<std::string> v_name;
      if ((ret = parse_name(file_path, v_name)) != TFS_SUCCESS)
      {
        TBSYS_LOG(WARN, "file_path(%s) is invalid", file_path);
      }
      else
      {
        int32_t depth = get_depth(v_name);

        if ((ret = get_p_meta_info(app_id, uid, v_name, p_meta_info, pname_len)) != TFS_SUCCESS)
        {
          get_name(v_name, depth - 1, pname, MAX_FILE_PATH_LEN, pname_len);
          if (depth == 1)
          {
            ret = create_top_dir(app_id, uid);
            if (TFS_SUCCESS == ret)
            {
              ret = get_p_meta_info(app_id, uid, v_name, p_meta_info, pname_len);
            }
          }
        }

        if (TFS_SUCCESS == ret) 
        {
          get_name(v_name, depth, name, MAX_FILE_PATH_LEN, name_len);
          TBSYS_LOG(DEBUG, "name: %s, ppid: %lu, pid: %lu", name, p_meta_info.pid_, p_meta_info.id_);
          if ((ret = store_manager_->insert(app_id, uid, p_meta_info.pid_, p_meta_info.name_.c_str(), pname_len,
                                            p_meta_info.id_, name, name_len, type)) != TFS_SUCCESS)
            TBSYS_LOG(ERROR, "create fail: %s, type: %d, ret: %d", file_path, type, ret);
        }
      }

      TBSYS_LOG(DEBUG, "create %s, type: %d, appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, filepath: %s",
                TFS_SUCCESS == ret ? "success" : "fail", type, app_id, uid, file_path);

      return ret;
    }

    int MetaServerService::rm(const int64_t app_id, const int64_t uid,
                              const char* file_path, const FileType type)
    {
      char name[MAX_FILE_PATH_LEN], pname[MAX_FILE_PATH_LEN];
      int32_t name_len = 0, pname_len = 0;
      int ret = TFS_SUCCESS;

      MetaInfo p_meta_info;
      std::vector<std::string> v_name;
      if ((ret = parse_name(file_path, v_name)) != TFS_SUCCESS)
      {
        TBSYS_LOG(WARN, "file_path(%s) is invalid", file_path);
      }
      else
      {
        int32_t depth = get_depth(v_name);

        if ((ret = get_p_meta_info(app_id, uid, v_name, p_meta_info, pname_len)) != TFS_SUCCESS)
        {
          get_name(v_name, depth - 1, pname, MAX_FILE_PATH_LEN, pname_len);
        }

        if (TFS_SUCCESS == ret) 
        {
          get_name(v_name, depth, name, MAX_FILE_PATH_LEN, name_len);

          std::vector<MetaInfo> v_meta_info;
          std::vector<MetaInfo>::iterator iter;

          ret = store_manager_->select(app_id, uid, p_meta_info.id_, name, name_len, v_meta_info);
          if ((TFS_SUCCESS == ret) && (static_cast<int32_t>(v_meta_info.size()) > 0))
          {
            iter = v_meta_info.begin();
            if ((ret = store_manager_->remove(app_id, uid, 
                                              p_meta_info.pid_, p_meta_info.name_.c_str(), pname_len,
                                              p_meta_info.id_, (*iter).id_, name, name_len, type)) != TFS_SUCCESS)
            {
              TBSYS_LOG(ERROR, "rm fail: %s, type: %d, ret: %d", file_path, type, ret);
            }
          }
        }
      }

      TBSYS_LOG(DEBUG, "rm %s, type: %d, appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, filepath: %s",
                TFS_SUCCESS == ret ? "success" : "fail", type, app_id, uid, file_path);

      return ret;
    }

    int MetaServerService::mv(const int64_t app_id, const int64_t uid,
                              const char* file_path, const char* dest_file_path,
                              const FileType type)
    {
      char name[MAX_FILE_PATH_LEN], pname[MAX_FILE_PATH_LEN],
        dest_name[MAX_FILE_PATH_LEN], dest_pname[MAX_FILE_PATH_LEN];
      std::vector<std::string> v_name, dest_v_name;
      int32_t name_len = 0, pname_len = 0, dest_name_len = 0, dest_pname_len = 0;
      int ret = TFS_SUCCESS;
      MetaInfo p_meta_info, dest_p_meta_info;

      if ((ret = parse_name(file_path, v_name)) != TFS_SUCCESS)
      {
        TBSYS_LOG(WARN, "file_path(%s) is invalid", file_path);
      }
      else if ((ret = parse_name(dest_file_path, dest_v_name)) != TFS_SUCCESS)
      {
        TBSYS_LOG(WARN, "dest_file_path(%s) is invalid", dest_file_path);
      }
      else
      {
        int32_t depth = get_depth(v_name);
        int32_t dest_depth = get_depth(dest_v_name);

        if ((ret = get_p_meta_info(app_id, uid, v_name, p_meta_info, pname_len)) != TFS_SUCCESS)
        {
          get_name(v_name, depth - 1, pname, MAX_FILE_PATH_LEN, pname_len);
        }
        else if ((ret = get_p_meta_info(app_id, uid, dest_v_name, dest_p_meta_info, dest_pname_len)) != TFS_SUCCESS)
        {
          get_name(dest_v_name, depth - 1, dest_pname, MAX_FILE_PATH_LEN, dest_pname_len);
        }

        if (TFS_SUCCESS == ret) 
        {
          get_name(v_name, depth, name, MAX_FILE_PATH_LEN, name_len);
          get_name(dest_v_name, dest_depth, dest_name, MAX_FILE_PATH_LEN, dest_name_len);

          if ((ret = store_manager_->update(app_id, uid, 
                                            p_meta_info.pid_, p_meta_info.id_, p_meta_info.name_.c_str(), pname_len,
                                            dest_p_meta_info.pid_, dest_p_meta_info.id_, dest_p_meta_info.name_.c_str(), dest_pname_len,
                                            name, name_len, dest_name, dest_name_len, type)) != TFS_SUCCESS)
            TBSYS_LOG(ERROR, "mv fail: %s, type: %d, ret: %d", file_path, type, ret);
        }
      }

      TBSYS_LOG(DEBUG, "mv %s, type: %d, appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, filepath: %s",
                TFS_SUCCESS == ret ? "success" : "fail", type, app_id, uid, file_path);

      return ret;
    }

    int MetaServerService::parse_name(const char* file_path, std::vector<std::string>& v_name)
    {
      int ret = TFS_ERROR;
      if ((file_path[0] != '/') || (strlen(file_path) > 256))
      {
        TBSYS_LOG(WARN, "file_path(%s) is invalid, length(%d)", file_path, strlen(file_path));
      }
      else
      {
        v_name.clear();
        v_name.push_back("/");
        Func::split_string(file_path, '/', v_name);
        ret = TFS_SUCCESS;
      }
      return ret;
    }

    int32_t MetaServerService::get_depth(const std::vector<std::string>& v_name)
    {
      return static_cast<int32_t>(v_name.size() - 1);
    }

    // add length to file name, fit to data struct
    int MetaServerService::get_name(const std::vector<std::string>& v_name, const int32_t depth, char* buffer, const int32_t buffer_len, int32_t& name_len)
    {
      name_len = 0;

      int ret = TFS_SUCCESS;
      if (depth < 0 || depth >= static_cast<int32_t>(v_name.size()))
      {
        TBSYS_LOG(ERROR, "depth is less than 1 or more than depth, %d", depth);
        ret = TFS_ERROR;
      }
      else
      {
        name_len = strlen(v_name[depth].c_str()) + 1;
        if (name_len >= buffer_len || buffer == NULL)
        {
          TBSYS_LOG(ERROR, "buffer is not enough or buffer is null, %d < %d", buffer_len, name_len);
          ret = TFS_ERROR;
        }
        else
        {
          snprintf(buffer, name_len + 1, "%c%s", char(name_len - 1), v_name[depth].c_str());
          TBSYS_LOG(DEBUG, "old_name: %s, new_name: %s, name_len: %d", v_name[depth].c_str(), buffer, name_len);
        }
      }

      return ret;
    }

    int MetaServerService::get_p_meta_info(const int64_t app_id, const int64_t uid, const std::vector<std::string> & v_name, MetaInfo& out_meta_info, int32_t& name_len)
    {
      int ret = TFS_ERROR;
      int32_t depth = get_depth(v_name);

      char name[MAX_FILE_PATH_LEN];
      std::vector<MetaInfo> tmp_v_meta_info;
      int64_t pid = 0;
      for (int32_t i = 0; i < depth; i++)
      {
        if ((ret = get_name(v_name, i, name, MAX_FILE_PATH_LEN, name_len)) != TFS_SUCCESS)
        {
          break;
        }
        ret = store_manager_->select(app_id, uid, pid, name, name_len, tmp_v_meta_info);

        if (ret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "select name(%s) failed, ret: %d", name, ret);
          break;
        }
        std::vector<MetaInfo>::const_iterator iner_iter;
        for (iner_iter = tmp_v_meta_info.begin(); iner_iter != tmp_v_meta_info.end(); iner_iter++)
        {
          TBSYS_LOG(DEBUG, "***%s*** -> ***%s***, ret: %d", iner_iter->name_.c_str(), name, memcmp(iner_iter->name_.c_str(), name, name_len));
          if (memcmp(iner_iter->name_.c_str(), name, name_len) == 0)
          {
            pid = iner_iter->id_;
            // get parent info
            if (i == depth - 1)
            {
              out_meta_info = (*iner_iter);
              ret = TFS_SUCCESS;
            }
            break;
          }
        }
        if (iner_iter == tmp_v_meta_info.end())
        {
          ret = TFS_ERROR;
          TBSYS_LOG(DEBUG, "file(%s) not found, ret: %d", name, ret);
          break;
        }
      }
      return ret;
    }

    int MetaServerService::create_top_dir(const int64_t app_id, const int64_t uid)
    {
      char name[3];
      name[0] = 1;
      name[1] = '/';
      name[2] = '\0';
      
      return store_manager_->insert(app_id, uid, 0, "", 0, 0, name, 2, DIRECTORY);
    }

    int MetaServerService::read(const int64_t app_id, const int64_t uid, const char* file_path,
                                const int64_t offset, const int64_t size,
                                int32_t& cluster_id, vector<FragMeta>& v_frag_info)
    {
      char name[MAX_FILE_PATH_LEN];
      int32_t name_len = 0, pname_len = 0;
      int ret = TFS_SUCCESS;

      MetaInfo p_meta_info;
      std::vector<MetaInfo> tmp_v_meta_info;
      std::vector<std::string> v_name;

      if ((ret = parse_name(file_path, v_name)) != TFS_SUCCESS)
      {
        TBSYS_LOG(WARN, "file_path(%s) is invalid", file_path);
      }
      else
      {
        int32_t depth = get_depth(v_name);

        if ((ret = get_p_meta_info(app_id, uid, v_name, p_meta_info, pname_len)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "get parent meta info fail. ret: %d", ret);
        }
        else
        {
          get_name(v_name, depth, name, MAX_FILE_PATH_LEN, name_len);

          if ((ret = store_manager_->select(app_id, uid, p_meta_info.id_, name, name_len, tmp_v_meta_info)) != TFS_SUCCESS)
          {
            TBSYS_LOG(WARN, "get read meta info fail, ret: %d", ret);
          }
          else if ((ret = read_frag_info(tmp_v_meta_info, offset, size, cluster_id, v_frag_info)) != TFS_SUCCESS)
          {
            TBSYS_LOG(WARN, "parse read frag info fail. ret: %d", ret);
          }
        }
      }
      return ret;
    }

    int MetaServerService::write(const int64_t app_id, const int64_t uid,
                                 const char* file_path, const int32_t cluster_id, vector<FragMeta>& v_frag_info)
    {
      char name[MAX_FILE_PATH_LEN], pname[MAX_FILE_PATH_LEN];
      int32_t name_len = 0, pname_len = 0;
      int ret = TFS_SUCCESS;

      MetaInfo p_meta_info;
      std::vector<MetaInfo> tmp_v_meta_info;
      std::vector<std::string> v_name;

      if ((ret = parse_name(file_path, v_name)) != TFS_SUCCESS)
      {
        TBSYS_LOG(WARN, "file_path(%s) is invalid", file_path);
      }
      else
      {
        int32_t depth = get_depth(v_name);

        if ((ret = get_p_meta_info(app_id, uid, v_name, p_meta_info, pname_len)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "get parent meta info fail. ret: %d", ret);
        }
        else
        {
          get_name(v_name, depth, name, MAX_FILE_PATH_LEN, name_len);
          int32_t retry = 3;
          do
          {
            tmp_v_meta_info.clear();
            if ((ret = store_manager_->select(app_id, uid, p_meta_info.id_, name, name_len, tmp_v_meta_info))
                != TFS_SUCCESS)
            {
              TBSYS_LOG(WARN, "get meta info fail, ret: %d", ret);
            }
            else if ((ret = write_frag_info(cluster_id, tmp_v_meta_info, v_frag_info)) != TFS_SUCCESS)
            {
              TBSYS_LOG(WARN, "merge write frag info fail.");
            }
            else if ((ret = update_write_frag_info(tmp_v_meta_info, app_id, uid, p_meta_info, pname, pname_len,
                                                   name, name_len)) != TFS_SUCCESS)
            {
              TBSYS_LOG(WARN, "insert new write meta info fail. ret: %d", ret);
            }
          } while (INSERT_VERSION_ERROR == ret && retry-- > 0);
        }
      }

      return ret;
    }

    int MetaServerService::read_frag_info(const vector<MetaInfo>& v_meta_info, const int64_t offset,
                                          const int64_t& size, int32_t& cluster_id,
                                          vector<FragMeta>& v_out_frag_info)
    {
      int64_t end_offset = offset + size;
      vector<MetaInfo>::const_iterator meta_info_it = v_meta_info.begin();;
      vector<FragMeta>::const_iterator start_it, end_it;
      bool get_all = false;

      if (meta_info_it != v_meta_info.end())
      {
        cluster_id = meta_info_it->frag_info_.cluster_id_;
      }

      while (meta_info_it != v_meta_info.end())
      {
        const vector<FragMeta>& v_in_frag_info = meta_info_it->frag_info_.v_frag_meta_;

        meta_info_it++;

        start_it = lower_find(v_in_frag_info, offset);

        if (start_it != v_in_frag_info.end())
        {
          end_it = upper_find(v_in_frag_info, end_offset);
          for (vector<FragMeta>::const_iterator tmp_it = start_it;
               start_it != end_it; ++start_it)
          {
            v_out_frag_info.push_back(*tmp_it);
          }

          if (end_it != v_in_frag_info.end())
          {
            get_all = true;
          }

          break;
        }
      }

      while (!get_all && meta_info_it != v_meta_info.end())
      {
        const vector<FragMeta>& v_in_frag_info = meta_info_it->frag_info_.v_frag_meta_;
        end_it = upper_find(v_in_frag_info, end_offset);

        for (vector<FragMeta>::const_iterator tmp_it = start_it;
             start_it != end_it; ++start_it)
        {
          v_out_frag_info.push_back(*tmp_it);
        }

        if (end_it != v_in_frag_info.end()) // got all
        {
          break;
        }

        meta_info_it++;
      }

      return TFS_SUCCESS;
    }

    int MetaServerService::write_frag_info(int32_t cluster_id, vector<MetaInfo>& v_meta_info,
                                           const vector<FragMeta>& v_frag_info)
    {
      int32_t frag_info_count = v_frag_info.size(), end_index = 0;
      int ret = TFS_SUCCESS;

      for (vector<MetaInfo>::iterator it = v_meta_info.begin();
           it != v_meta_info.end() && end_index < frag_info_count; ++it)
      {
        if (cluster_id != it->frag_info_.cluster_id_)
        {
          TBSYS_LOG(ERROR, "cluster id conflict %d <> %d", cluster_id, it->frag_info_.cluster_id_);
          ret = TFS_ERROR;
          break;
        }

        if ((ret = merge_frag_info(*it, v_frag_info, end_index))
            != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "merge frag info fail. ret: %d", ret); // too many fraginfos in one record
          break;
        }
      }

      if (TFS_SUCCESS == ret && end_index != frag_info_count) // remain fraginfo to insert
      {
        if ((ret = insert_frag_info(v_meta_info, v_frag_info, end_index)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "insert frag info fail. ret: %d", ret); // too many metainfos record in one file
        }
      }

      return ret;
    }

    int MetaServerService::update_write_frag_info(vector<MetaInfo>& v_meta_info, const int64_t app_id,
                                                  const int64_t uid, MetaInfo& p_meta_info,
                                                  const char* pname, const int32_t pname_len,
                                                  char* name, const int32_t name_len)
    {
      int ret = TFS_SUCCESS;
      int64_t pos = 0;
      for (vector<MetaInfo>::iterator it = v_meta_info.begin();
           it != v_meta_info.end();
           ++it)
      {
        pos = name_len;
        common::Serialization::set_int64(name, MAX_FILE_PATH_LEN, pos, it->frag_info_.v_frag_meta_[0].offset_);
        if ((ret = store_manager_->insert(app_id, uid, p_meta_info.pid_, pname, pname_len,
                                          p_meta_info.id_, name, name_len, PWRITE_FILE))
            != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "insert new meta info fail. ret: %d", ret);
          break;
        }
      }
      return ret;
    }
    
    int MetaServerService::insert_frag_info(vector<MetaInfo>& v_meta_info, const vector<FragMeta>& src_v_frag_info,
                                            int32_t& end_index)
    {
      int ret = TFS_SUCCESS;
      if (end_index >= static_cast<int32_t>(src_v_frag_info.size()))
      {
        ret = TFS_ERROR;
        TBSYS_LOG(ERROR, "overflow index. %d >= %d", end_index, src_v_frag_info.size());
      }
      else
      {
        int32_t left_frag_info_count = src_v_frag_info.size() - end_index;
        vector<FragMeta>::const_iterator start_it = src_v_frag_info.begin();
        advance(start_it, end_index);
        vector<FragMeta>::const_iterator end_it = start_it;

        while (left_frag_info_count > 0)
        {
          if (left_frag_info_count <= SOFT_MAX_FRAG_INFO_COUNT)
          {
            advance(end_it, left_frag_info_count);
            left_frag_info_count = 0;
          }
          else
          {
            advance(end_it, SOFT_MAX_FRAG_INFO_COUNT);
            left_frag_info_count -= SOFT_MAX_FRAG_INFO_COUNT;
          }

          v_meta_info.push_back(MetaInfo(FragInfo(vector<FragMeta>(start_it, end_it))));
          start_it = end_it;
        }
      }
      return ret;
    }

    int MetaServerService::merge_frag_info(const MetaInfo& meta_info, const vector<FragMeta>& src_v_frag_info,
                                           int32_t& end_index)
    {
      int ret = TFS_SUCCESS;
      if (end_index >= static_cast<int32_t>(src_v_frag_info.size()))
      {
        ret = TFS_ERROR;
        TBSYS_LOG(WARN, "overflow index. %d >= %d", end_index, src_v_frag_info.size());
      }
      else
      {
        vector<FragMeta>::const_iterator src_it = src_v_frag_info.begin();
        vector<FragMeta>& dest_v_frag_info = const_cast<vector<FragMeta>& >(meta_info.frag_info_.v_frag_meta_);
        vector<FragMeta>::iterator dest_it = dest_v_frag_info.begin();
        int32_t frag_info_count = dest_v_frag_info.size();
        advance(src_it, end_index);

        if (src_it != src_v_frag_info.end() && meta_info.size_ <= src_it->offset_) // append
        {
          ret = fast_merge_frag_info(dest_v_frag_info, src_v_frag_info, src_it, end_index);
        }
        else
        {
          // just merge in orignal container
          for (; src_it != src_v_frag_info.end() && dest_it != dest_v_frag_info.end() &&
                 frag_info_count <= HARD_MAX_FRAG_INFO_COUNT;
               ++frag_info_count)
          {
            if (src_it->offset_ < dest_it->offset_)
            {
              if (src_it->offset_ + src_it->size_ <= dest_it->offset_) // fill hole
              {
                // expensive op... 
                dest_it = dest_v_frag_info.insert(dest_it, *src_it);
                src_it++;
                end_index++;
              }
              else              // overlap
              {
                TBSYS_LOG(ERROR, "update overlap error. ");
                ret = UPDATE_FRAG_INFO_ERROR;
                break;
              }
            }
            else if (src_it->offset_ < dest_it->offset_ + dest_it->size_) // overlap
            {
              TBSYS_LOG(ERROR, "update overlap error. ");
              ret = UPDATE_FRAG_INFO_ERROR;
              break;
            }
            else
            {
              dest_it++;
            }
          }

          if (ret == TFS_SUCCESS && dest_it == dest_v_frag_info.end()) // fill hole over, still can append
          {
            ret = fast_merge_frag_info(dest_v_frag_info, src_v_frag_info, src_it, end_index);
          }
        }
      }
      return ret;
    }

    // just append to dest from src until match threshold
    int MetaServerService::fast_merge_frag_info(vector<FragMeta>& dest_v_frag_info,
                                                const vector<FragMeta>& src_v_frag_info,
                                                vector<FragMeta>::const_iterator& src_it,
                                                int32_t& end_index)
    {
      int32_t frag_info_count = dest_v_frag_info.size();
      for (; src_it != src_v_frag_info.end() && frag_info_count < SOFT_MAX_FRAG_INFO_COUNT;
           src_it++, end_index++, frag_info_count++)
      {
        dest_v_frag_info.push_back(*src_it);
      }
      return TFS_SUCCESS;
    }

    // find first one whose offset is not large than offset
    vector<FragMeta>::const_iterator
    MetaServerService::lower_find(const vector<FragMeta>& v_frag_info, const int64_t offset)
    {
      vector<FragMeta>::const_iterator it = 
        lower_bound(const_cast<vector<FragMeta>&>(v_frag_info).begin(),
                    const_cast<vector<FragMeta>&>(v_frag_info).end(), offset, FragMetaComp());

      if (it->offset_ != offset && it != v_frag_info.begin())
      {
        vector<FragMeta>::const_iterator pre_it = it;
        --pre_it;
        if (pre_it->offset_ + pre_it->size_ > offset)
        {
          --it;
        }
      }

      return it;
    }

    // find first one whose offset is not less than offset
    vector<FragMeta>::const_iterator
    MetaServerService::upper_find(const vector<FragMeta>& v_frag_info, const int64_t offset)
    {
      return lower_bound(const_cast<vector<FragMeta>&>(v_frag_info).begin(),
                         const_cast<vector<FragMeta>&>(v_frag_info).end(), offset, FragMetaComp());
    }

  }
}

