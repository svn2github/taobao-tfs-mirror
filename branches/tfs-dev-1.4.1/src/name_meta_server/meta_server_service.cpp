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
 *   nayan <nayan@taobao.com>
 *      - initial release
 *
 */

#include "common/base_packet.h"
#include "meta_server_service.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace std;

namespace tfs
{
  namespace namemetaserver
  {
    MetaServerService::MetaServerService() : store_manager_(NULL)
    {
      store_manager_ =  new MetaStoreManager();
      top_dir_name_[0] = 1;
      top_dir_name_[1] = '/';
      top_dir_size_ = 2;
      //TODO  use the number from conf
      store_manager_->init(5);
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
          case FILEPATH_ACTION_MESSAGE:
            ret = do_action(base_packet);
            break;
          case WRITE_FILEPATH_MESSAGE:
            ret = do_write(base_packet);
            break;
          case READ_FILEPATH_MESSAGE:
            ret = do_read(base_packet);
            break;
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
    int MetaServerService::do_action(common::BasePacket* packet)
    {
      int ret = TFS_SUCCESS;
      if (NULL == packet)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "MetaNameService::do action fail. input packet invaild. ret: %d", ret);
      }
      else
      {
        FilepathActionMessage* req_fa_msg = dynamic_cast<FilepathActionMessage*>(packet);
        int64_t app_id = req_fa_msg->get_app_id();
        int64_t uid = req_fa_msg->get_user_id();
        const char* file_path = req_fa_msg->get_file_path();
        const char* new_file_path = req_fa_msg->get_new_file_path();
        common::MetaActionOp action = req_fa_msg->get_action();
        TBSYS_LOG(DEBUG, "call FilepathActionMessage::do action start. app_id: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, file_path: %s, new_file_path: %s, action: %d ret: %d",
            app_id, uid, file_path, new_file_path, action, ret);
        switch (action)
        {
          case CREATE_DIR:
            ret = create(app_id, uid, file_path, DIRECTORY);
            break;
          case CREATE_FILE:
            ret = create(app_id, uid, file_path, NORMAL_FILE);
            break;
          case REMOVE_DIR:
            ret = rm(app_id, uid, file_path, DIRECTORY);
            break;
          case REMOVE_FILE:
            ret = rm(app_id, uid, file_path, NORMAL_FILE);
            break;
          case MOVE_DIR:
            ret = mv(app_id, uid, file_path, new_file_path, DIRECTORY);
            break;
          case MOVE_FILE:
            ret = mv(app_id, uid, file_path, new_file_path, NORMAL_FILE);
            break;
          default:
            break;
        }

        if (ret != TFS_SUCCESS)
        {
          ret = req_fa_msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "execute message failed");
        }
        else
        {
          ret = req_fa_msg->reply(new StatusMessage(STATUS_MESSAGE_OK));
        }
      }
      return ret;
    }

    int MetaServerService::do_write(common::BasePacket* packet)
    {
      int ret = TFS_SUCCESS;
      if (NULL == packet)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "MetaNameService::do write fail. input packet invaild. ret: %d", ret);
      }
      else
      {
        WriteFilepathMessage* req_wf_msg = dynamic_cast<WriteFilepathMessage*>(packet);
        TBSYS_LOG(DEBUG, "call FilepathActionMessage::do action start. app_id: %"PRI64_PREFIX"d, user_id: %"PRI64_PREFIX"d, file_path: %s, meta_size: %zd ret: %d",
            req_wf_msg->get_app_id(), req_wf_msg->get_user_id(), req_wf_msg->get_file_path(), req_wf_msg->get_frag_info().v_frag_meta_.size(), ret);
        req_wf_msg->get_frag_info().dump();

        ret = write(req_wf_msg->get_app_id(), req_wf_msg->get_user_id(), req_wf_msg->get_file_path(), req_wf_msg->get_frag_info());
        if (ret != TFS_SUCCESS)
        {
          ret = req_wf_msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "execute message failed");
        }
        else
        {
          ret = req_wf_msg->reply(new StatusMessage(STATUS_MESSAGE_OK));
        }
      }
      return ret;
    }

    int MetaServerService::do_read(common::BasePacket* packet)
    {
      int ret = TFS_SUCCESS;
      if (NULL == packet)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "MetaNameService::do read fail. input packet invaild. ret: %d", ret);
      }
      else
      {
        ReadFilepathMessage* req_rf_msg = dynamic_cast<ReadFilepathMessage*>(packet);
        TBSYS_LOG(DEBUG, "call FilepathActionMessage::do read start. app_id: %"PRI64_PREFIX"d, user_id: %"PRI64_PREFIX"d, "
            "file_path: %s, offset :%"PRI64_PREFIX"d, size: %"PRI64_PREFIX"d, ret: %d",
            req_rf_msg->get_app_id(), req_rf_msg->get_user_id(), req_rf_msg->get_file_path(), req_rf_msg->get_offset(), req_rf_msg->get_size(), ret);

        FragInfo frag_info;
        bool still_have;
        ret = read(req_rf_msg->get_app_id(), req_rf_msg->get_user_id(), req_rf_msg->get_file_path(), req_rf_msg->get_offset(), req_rf_msg->get_size(), frag_info, still_have);
        if (ret != TFS_SUCCESS)
        {
          ret = req_rf_msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "execute message failed");
        }
        else
        {
          RespReadFilepathMessage* resp_rf_msg = new RespReadFilepathMessage();
          resp_rf_msg->set_frag_info(frag_info);
          resp_rf_msg->set_still_have(still_have);
          ret = req_rf_msg->reply(resp_rf_msg);
        }
      }
      return ret;
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
          if (depth == 1)
          {
            ret = create_top_dir(app_id, uid);
            if (TFS_SUCCESS == ret)
            {
              get_name(v_name, depth - 1, pname, MAX_FILE_PATH_LEN, pname_len);
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
          {
            TBSYS_LOG(ERROR, "create fail: %s, type: %d, ret: %d", file_path, type, ret);
          }
        }
      }

      TBSYS_LOG(DEBUG, "create %s, type: %d, appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, filepath: %s",
                TFS_SUCCESS == ret ? "success" : "fail", type, app_id, uid, file_path);

      return ret;
    }

    int MetaServerService::rm(const int64_t app_id, const int64_t uid,
                              const char* file_path, const FileType type)
    {
      char name[MAX_FILE_PATH_LEN];
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

        if ((ret = get_p_meta_info(app_id, uid, v_name, p_meta_info, pname_len)) == TFS_SUCCESS)
        {
          get_name(v_name, depth, name, MAX_FILE_PATH_LEN, name_len);

          std::vector<MetaInfo> v_meta_info;
          std::vector<MetaInfo>::iterator iter;

          ret = store_manager_->select(app_id, uid, p_meta_info.id_, name, name_len, 
              type != DIRECTORY, v_meta_info);
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
      char name[MAX_FILE_PATH_LEN], dest_name[MAX_FILE_PATH_LEN];
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

        if ((ret = get_p_meta_info(app_id, uid, v_name, p_meta_info, pname_len)) == TFS_SUCCESS)
        {
          if ((ret = get_p_meta_info(app_id, uid, dest_v_name, dest_p_meta_info, dest_pname_len)) == TFS_SUCCESS)
          {
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
        }
      }

      TBSYS_LOG(DEBUG, "mv %s, type: %d, appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, filepath: %s",
                TFS_SUCCESS == ret ? "success" : "fail", type, app_id, uid, file_path);

      return ret;
    }

    int MetaServerService::read(const int64_t app_id, const int64_t uid, const char* file_path,
                                const int64_t offset, const int64_t size,
                                FragInfo& frag_info, bool& still_have)
    {
      char name[MAX_FILE_PATH_LEN];
      int32_t name_len = 0, pname_len = 0;
      int ret = TFS_SUCCESS;
      still_have = false;

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
          int64_t last_offset = 0;
          ret = get_meta_info(app_id, uid, p_meta_info.id_, name, name_len, offset,
              tmp_v_meta_info, frag_info.cluster_id_, last_offset);
          if (ret == TFS_SUCCESS)
          {
            if ((ret = read_frag_info(tmp_v_meta_info, offset, size,
                    frag_info.cluster_id_, frag_info.v_frag_meta_, still_have)) != TFS_SUCCESS)
            {
              TBSYS_LOG(WARN, "parse read frag info fail. ret: %d", ret);
            }
          }
        }
      }
      return ret;
    }
    int MetaServerService::get_meta_info(const int64_t app_id, const int64_t uid, const int64_t pid,
        const char* name, const int32_t name_len, const int64_t offset,
        std::vector<MetaInfo>& tmp_v_meta_info, int32_t& cluster_id, int64_t& last_offset)
    {
      int ret = TFS_ERROR;
      char search_name[MAX_FILE_PATH_LEN + 8];
      int32_t search_name_len = name_len;
      assert(name_len <= MAX_FILE_PATH_LEN);
      memcpy(search_name, name, search_name_len);
      bool still_have = false;
      last_offset = 0;
      cluster_id = -1;
      do {
        tmp_v_meta_info.clear();
        still_have = false;
        ret = store_manager_->select(app_id, uid, pid,
            search_name, search_name_len, true, tmp_v_meta_info);
        TBSYS_LOG(DEBUG, "select size: %zd", tmp_v_meta_info.size());
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get read meta info fail, ret: %d", ret);
          break;
        }
        if (!tmp_v_meta_info.empty())
        {
          const MetaInfo& last_metaInfo = tmp_v_meta_info[tmp_v_meta_info.size() - 1];
          cluster_id = last_metaInfo.frag_info_.cluster_id_;
          if (((-1 == offset || last_metaInfo.frag_info_.get_last_offset() <= offset) &&
              last_metaInfo.frag_info_.had_been_split_)) 

          {
            still_have = true;
            memcpy(search_name, last_metaInfo.name_.data(), last_metaInfo.name_.length());
            search_name_len = last_metaInfo.name_.length();
            if (search_name_len == (unsigned char)search_name[0] + 1)
            {
              last_offset = last_metaInfo.frag_info_.get_last_offset();
              int64_to_char(search_name + search_name_len, 8, last_offset);
              search_name_len += 8;
            }
          }
        }
      } while(TFS_SUCCESS == ret && still_have);
      return ret;
    }

    int MetaServerService::write(const int64_t app_id, const int64_t uid,
        const char* file_path,
        const FragInfo& frag_info)
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
          vector<FragMeta> v_frag_meta(frag_info.v_frag_meta_);
          sort(v_frag_meta.begin(), v_frag_meta.end());
          vector<FragMeta>::iterator write_frag_info_it = v_frag_meta.begin();
          //we use while, but no
          while (write_frag_info_it != v_frag_meta.end() && TFS_SUCCESS == ret)
          {
            tmp_v_meta_info.clear();
            int32_t in_cluster_id = -1;
            int64_t last_offset = 0;
            ret = get_meta_info(app_id, uid, p_meta_info.id_, name, name_len,
                write_frag_info_it->offset_, tmp_v_meta_info, in_cluster_id, last_offset);
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(DEBUG, "record not exist, name(%s)", name);
              break;
            }
            if (in_cluster_id == -1)
            {
              //TODO give a error_no. this means no create file found
              ret = TFS_ERROR;
              break;
            }
            if (in_cluster_id != 0 && frag_info.cluster_id_ != in_cluster_id)
            {
              //TODO give a error_no. this means cluster_id error
              ret = TFS_ERROR;
              break;
            }
            if (tmp_v_meta_info.empty())
            {
              //this is for split, we make a new metainfo
              MetaInfo tmp;
              tmp.pid_ = p_meta_info.id_;
              tmp.frag_info_.cluster_id_ = frag_info.cluster_id_;
              tmp.frag_info_.had_been_split_ = false;
              char tmp_name[MAX_FILE_PATH_LEN + 8];
              memcpy(tmp_name, name, name_len);
              int64_to_char(tmp_name+name_len, MAX_FILE_PATH_LEN + 8 - name_len,
                  last_offset);

              tmp.name_.assign(tmp_name, name_len + 8);
              tmp_v_meta_info.push_back(tmp);
            }
            bool found_meta_info_should_be_updated = false;
            std::vector<MetaInfo>::iterator v_meta_info_it = tmp_v_meta_info.begin();
            for (; v_meta_info_it != tmp_v_meta_info.end(); v_meta_info_it++)
            {
              if (!v_meta_info_it->frag_info_.had_been_split_ ||
                  (write_frag_info_it->offset_ != -1 && 
                   v_meta_info_it->frag_info_.get_last_offset() > write_frag_info_it->offset_))
              {
                found_meta_info_should_be_updated = true;
                break;
              }
            }

            if (!found_meta_info_should_be_updated)
            {
              ret = EXIT_UPDATE_FRAG_INFO_ERROR;
              break;
            }
            //now  write_frag_info_it  should be write to v_meta_info_it
            if (!v_meta_info_it->frag_info_.had_been_split_)
            {
              while(write_frag_info_it != v_frag_meta.end())
                 // && static_cast<int32_t>(v_meta_info_it->frag_info_.v_frag_meta_.size()) <= SOFT_MAX_FRAG_INFO_COUNT)
              {
                if (-1 == write_frag_info_it->offset_)
                {
                  write_frag_info_it->offset_ = last_offset;
                  last_offset += write_frag_info_it->size_;
                }
                v_meta_info_it->frag_info_.v_frag_meta_.push_back(*write_frag_info_it);
                write_frag_info_it++;
              }
              if (static_cast<int32_t>(v_meta_info_it->frag_info_.v_frag_meta_.size()) 
                  > SOFT_MAX_FRAG_INFO_COUNT)
              {
                TBSYS_LOG(DEBUG, "split meta_info");
                v_meta_info_it->frag_info_.had_been_split_ = true;
              }
            }
            else
            {
              int64_t last_offset = v_meta_info_it->frag_info_.get_last_offset();
              while(write_frag_info_it != frag_info.v_frag_meta_.end())
              {
                if (write_frag_info_it->offset_ >= last_offset)
                {
                  ret = TFS_ERROR;
                  break;
                }
                v_meta_info_it->frag_info_.v_frag_meta_.push_back(*write_frag_info_it);
                write_frag_info_it ++;
              }
              if (TFS_SUCCESS != ret)
              {
                break;
              }
              if (static_cast<int32_t>(v_meta_info_it->frag_info_.v_frag_meta_.size()) >= MAX_FRAG_INFO_COUNT)
              {
                //TODO
                ret = TFS_ERROR;
                break;
              }

            }
            if (TFS_SUCCESS == ret)
            {
              //resort it
              sort(v_meta_info_it->frag_info_.v_frag_meta_.begin(),
                  v_meta_info_it->frag_info_.v_frag_meta_.end());
              if (v_meta_info_it->frag_info_.cluster_id_ == 0)
              {
                v_meta_info_it->frag_info_.cluster_id_ = frag_info.cluster_id_;
              }
              ret = check_frag_info(v_meta_info_it->frag_info_);
              if (TFS_SUCCESS == ret)
              {
                //update this info;
                v_meta_info_it->size_ = v_meta_info_it->frag_info_.get_last_offset();
                ret = store_manager_->insert(app_id, uid, p_meta_info.pid_,
                    p_meta_info.name_.data(), p_meta_info.name_.length(), p_meta_info.id_,
                    v_meta_info_it->name_.data(), v_meta_info_it->name_.length(), PWRITE_FILE, 
                    &(*v_meta_info_it));
              }
            }
            assert(write_frag_info_it == v_frag_meta.end());
          }
        }
      }

      return ret;
    }

    int MetaServerService::read_frag_info(const vector<MetaInfo>& v_meta_info, const int64_t offset,
        const int32_t size, int32_t& cluster_id,
        vector<FragMeta>& v_out_frag_info, bool& still_have)
    {
      int32_t end_offset = offset + size;
      vector<MetaInfo>::const_iterator meta_info_it = v_meta_info.begin();;
      cluster_id = 0;
      v_out_frag_info.clear();
      still_have = false;

      while (meta_info_it != v_meta_info.end())
      {
        if (meta_info_it->frag_info_.get_last_offset() <= offset)
        {
          meta_info_it++;
          continue;
        }
        const vector<FragMeta>& v_in_frag_info = meta_info_it->frag_info_.v_frag_meta_;
        if (v_in_frag_info.empty())
        {
          //no frag to be read
          break;
        }
        FragMeta fragmeta_for_search;
        fragmeta_for_search.offset_ = offset;
        vector<FragMeta>::const_iterator it =
          lower_bound(v_in_frag_info.begin(), v_in_frag_info.end(), fragmeta_for_search);
        if (it != v_in_frag_info.begin())
        {
          vector<FragMeta>::const_iterator tmp_it = it - 1;
          if (offset < tmp_it->offset_ + tmp_it->size_)
          {
            it--;
          }
        }
        cluster_id = meta_info_it->frag_info_.cluster_id_;
        still_have = true;
        for(int s = 0; it != v_in_frag_info.end() && s < MAX_OUT_FRAG_INFO; it++, s++)
        {
          v_out_frag_info.push_back(*it);
          if (it->offset_ + it->size_ > end_offset)
          {
            still_have = false;
            break;
          }
        }
        if (!meta_info_it->frag_info_.had_been_split_ && it == v_in_frag_info.end())
        {
          still_have = false;
        }
        break;
      }
      return TFS_SUCCESS;
    }

    int MetaServerService::parse_name(const char* file_path, std::vector<std::string>& v_name)
    {
      int ret = TFS_ERROR;
      if ((file_path[0] != '/') || (strlen(file_path) > (MAX_FILE_PATH_LEN -1)))
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
        ret = store_manager_->select(app_id, uid, pid, name, name_len, false, tmp_v_meta_info);

        if (ret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "select name(%s) failed, ret: %d", name, ret);
          break;
        }
        std::vector<MetaInfo>::const_iterator iner_iter;
        for (iner_iter = tmp_v_meta_info.begin(); iner_iter != tmp_v_meta_info.end(); iner_iter++)
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

      return store_manager_->insert(app_id, uid, 0, "", 0, 0, 
          top_dir_name_, top_dir_size_, DIRECTORY);
    }

    int MetaServerService::check_frag_info(const FragInfo& frag_info)
    {
      int ret = TFS_SUCCESS;
      if (frag_info.v_frag_meta_.size() > 0 && frag_info.cluster_id_ <= 0)
      {
        TBSYS_LOG(ERROR, "cluster id error %d", frag_info.cluster_id_);
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        if (static_cast<int32_t>(frag_info.v_frag_meta_.size())
            > SOFT_MAX_FRAG_INFO_COUNT &&
            !frag_info.had_been_split_)
        {
          TBSYS_LOG(ERROR, "frag_meta count %d, should be split", 
              frag_info.v_frag_meta_.size());
          ret = TFS_ERROR;
        }
      }
      if (TFS_SUCCESS == ret)
      {
        int64_t last_offset = -1;
        for (size_t i = 0; i < frag_info.v_frag_meta_.size(); i++)
        {
          if (frag_info.v_frag_meta_[i].offset_ < last_offset)
          {
            TBSYS_LOG(ERROR, "frag info have some error, %ld < %ld", frag_info.v_frag_meta_[i].offset_, last_offset);
            ret = TFS_ERROR;
            break;
          }
          last_offset = frag_info.v_frag_meta_[i].offset_ + 
            frag_info.v_frag_meta_[i].size_;
        }

      }
      return ret;
    }
    int MetaServerService::int64_to_char(char* buff, const int32_t buff_size, const int64_t v)
    {
      int ret = TFS_ERROR;
      if (NULL != buff && buff_size >= 8)
      {
        buff[7] = v & 0xFF;
        buff[6] = (v>>8) & 0xFF;
        buff[5] = (v>>16) & 0xFF;
        buff[4] = (v>>24) & 0xFF;
        buff[3] = (v>>32) & 0xFF;
        buff[2] = (v>>40) & 0xFF;
        buff[1] = (v>>48) & 0xFF;
        buff[0] = (v>>56) & 0xFF;
        ret = TFS_SUCCESS;
      }
      return ret;
    }
    int MetaServerService::char_to_int64(char* data, const int32_t data_size, int64_t& v)
    {
      int ret = TFS_ERROR;
      if (data_size >= 8)
      {
        v = static_cast<unsigned char>(data[0]);
        v = v << 8;
        v |= static_cast<unsigned char>(data[1]);
        v = v << 8;
        v |= static_cast<unsigned char>(data[2]);
        v = v << 8;
        v |= static_cast<unsigned char>(data[3]);
        v = v << 8;
        v |= static_cast<unsigned char>(data[4]);
        v = v << 8;
        v |= static_cast<unsigned char>(data[5]);
        v = v << 8;
        v |= static_cast<unsigned char>(data[6]);
        v = v << 8;
        v |= static_cast<unsigned char>(data[7]);
        ret = TFS_SUCCESS;
      }
      return ret;
    }
  }
}
