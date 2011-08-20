/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: meta_server_service.cpp 49 2011-08-08 09:58:57Z nayan@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */

#include "meta_server_service.h"

#include "common/base_packet.h"
#include "common/parameter.h"
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
      int ret = TFS_SUCCESS;
      if ((ret = SYSPARAM_NAMEMETASERVER.initialize()) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "call SYSPARAM_NAMEMETASERVER::initialize fail. ret: %d", ret);
      }
      else
      {
        ret = store_manager_->init(SYSPARAM_NAMEMETASERVER.max_pool_size_);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "init store_manager error");
        }
      }
      return ret;
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
        case LS_FILEPATH_MESSAGE:
          ret = do_ls(base_packet);
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

    // action over file. create, mv, rm
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
        TBSYS_LOG(DEBUG, "call FilepathActionMessage::do action start. app_id: %"PRI64_PREFIX"d, uid: %"
                  PRI64_PREFIX"d, file_path: %s, new_file_path: %s, action: %d ret: %d",
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
          TBSYS_LOG(INFO, "unknown action type: %d", action);
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

    // get file frag info to read
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
        TBSYS_LOG(DEBUG, "call FilepathActionMessage::do read start."
                  "app_id: %"PRI64_PREFIX"d, user_id: %"PRI64_PREFIX"d, "
                  "file_path: %s, offset :%"PRI64_PREFIX"d, size: %"PRI64_PREFIX"d, ret: %d",
                  req_rf_msg->get_app_id(), req_rf_msg->get_user_id(),
                  req_rf_msg->get_file_path(), req_rf_msg->get_offset(), req_rf_msg->get_size(), ret);

        RespReadFilepathMessage* resp_rf_msg = new RespReadFilepathMessage();
        bool still_have;

        ret = read(req_rf_msg->get_app_id(), req_rf_msg->get_user_id(), req_rf_msg->get_file_path(),
                   req_rf_msg->get_offset(), req_rf_msg->get_size(),
                   resp_rf_msg->get_frag_info(), still_have);

        if (ret != TFS_SUCCESS)
        {
          ret = req_rf_msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "execute message failed");
        }
        else
        {
          resp_rf_msg->set_still_have(still_have);
          ret = req_rf_msg->reply(resp_rf_msg);
        }
      }
      return ret;
    }

    // write frag info
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

        TBSYS_LOG(DEBUG, "call FilepathActionMessage::do action start. "
                  "app_id: %"PRI64_PREFIX"d, user_id: %"PRI64_PREFIX"d, file_path: %s, meta_size: %zd ret: %d",
                  req_wf_msg->get_app_id(), req_wf_msg->get_user_id(), req_wf_msg->get_file_path(),
                  req_wf_msg->get_frag_info().v_frag_meta_.size(), ret);

        ret = write(req_wf_msg->get_app_id(), req_wf_msg->get_user_id(),
                    req_wf_msg->get_file_path(), req_wf_msg->get_frag_info());

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

    // list file or directory meta info
    int MetaServerService::do_ls(common::BasePacket* packet)
    {
      int ret = TFS_SUCCESS;
      if (NULL == packet)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "MetaNameService::do ls fail. input packet invaild. ret: %d", ret);
      }
      else
      {
        LsFilepathMessage* req_lf_msg = dynamic_cast<LsFilepathMessage*>(packet);
        TBSYS_LOG(DEBUG, "call FilepathActionMessage::do ls start."
                  " app_id: %"PRI64_PREFIX"d, user_id: %"PRI64_PREFIX"d,"
                  " pid: %"PRI64_PREFIX"d, file_path: %s, file_type: %d",
                  req_lf_msg->get_app_id(), req_lf_msg->get_user_id(), req_lf_msg->get_pid(),
                  req_lf_msg->get_file_path(), req_lf_msg->get_file_type());

        bool still_have;
        RespLsFilepathMessage* resp_lf_msg = new RespLsFilepathMessage();

        ret = ls(req_lf_msg->get_app_id(), req_lf_msg->get_user_id(), req_lf_msg->get_pid(),
                 req_lf_msg->get_file_path(), static_cast<FileType>(req_lf_msg->get_file_type()),
                 resp_lf_msg->get_meta_infos(), still_have);

        if (ret != TFS_SUCCESS)
        {
          ret = req_lf_msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "execute message failed");
        }
        else
        {
          resp_lf_msg->set_still_have(still_have);
          ret = req_lf_msg->reply(resp_lf_msg);
        }
      }
      return ret;
    }

    int MetaServerService::create(const int64_t app_id, const int64_t uid,
                                  const char* file_path, const FileType type)
    {
      int ret = TFS_SUCCESS;
      char name[MAX_META_FILE_NAME_LEN];
      int32_t name_len = 0;
      MetaInfo p_meta_info;

      // create need check top directory's existence
      // not use parse_file_path to avoid dummy reparse.
      std::vector<std::string> v_name;

      if ((ret = parse_name(file_path, v_name)) != TFS_SUCCESS)
      {
        TBSYS_LOG(INFO, "file_path(%s) is invalid", file_path);
      }
      else if ((ret = get_p_meta_info(app_id, uid, v_name, p_meta_info)) != TFS_SUCCESS)
      {
        if (1 == get_depth(v_name))
        {
          TBSYS_LOG(DEBUG, "create top directory. appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, filepath: %s",
                    app_id, uid, file_path);
          // first create, "/foo", maybe no top directory
          if ((ret = create_top_dir(app_id, uid)) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "create top dir fail. appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, filepath: %s",
                      app_id, uid, file_path);
          }
          // re-get info.
          else if ((ret = get_p_meta_info(app_id, uid, v_name, p_meta_info)) != TFS_SUCCESS)
          {
            TBSYS_LOG(INFO, "get info fail. appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, %s",
                      app_id, uid, file_path);
          }
        }
        else
        {
          TBSYS_LOG(INFO, "get info fail. appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, %s",
                    app_id, uid, file_path);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        if ((ret = get_name(v_name[get_depth(v_name)].c_str(), name, MAX_META_FILE_NAME_LEN, name_len)) != TFS_SUCCESS)
        {
          TBSYS_LOG(INFO, "get name fail. ret: %d", ret);
        }
        else if ((ret = store_manager_->insert(app_id, uid, p_meta_info.get_pid(),
                                          p_meta_info.get_name(), p_meta_info.get_name_len(),
                                          p_meta_info.get_id(), name, name_len, type)) != TFS_SUCCESS)
        {
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
      char name[MAX_META_FILE_NAME_LEN];
      int32_t name_len = 0;
      int ret = TFS_SUCCESS;

      MetaInfo p_meta_info;

      if ((ret = parse_file_path(app_id, uid, file_path, p_meta_info, name, name_len)) != TFS_SUCCESS)
      {
        TBSYS_LOG(INFO, "get info fail. appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, %s",
                  app_id, uid, file_path);
      }
      else
      {
        std::vector<MetaInfo> v_meta_info;

        ret = store_manager_->select(app_id, uid, p_meta_info.get_id(), name, name_len,
                                     type != DIRECTORY, v_meta_info);

        // file not exist
        if (TFS_SUCCESS != ret || v_meta_info.empty())
        {
          ret = TFS_ERROR;
        }
        else
        {
          std::vector<MetaInfo>::iterator iter = v_meta_info.begin();
          if ((ret = store_manager_->remove(app_id, uid, p_meta_info.get_pid(),
                                            p_meta_info.get_name(), p_meta_info.get_name_len(),
                                            p_meta_info.get_id(), iter->get_id(),
                                            name, name_len, type)) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "rm fail: %s, type: %d, ret: %d", file_path, type, ret);
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
      char name[MAX_META_FILE_NAME_LEN], dest_name[MAX_META_FILE_NAME_LEN];
      int32_t name_len = 0, dest_name_len = 0;
      int ret = TFS_SUCCESS;
      MetaInfo p_meta_info, dest_p_meta_info;

      if ((ret = parse_file_path(app_id, uid, file_path, p_meta_info, name, name_len)) != TFS_SUCCESS)
      {
        TBSYS_LOG(INFO, "parse file path fail. appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, %s",
                  app_id, uid, file_path);
      }
      else if ((ret = parse_file_path(app_id, uid, dest_file_path, dest_p_meta_info, dest_name, dest_name_len))
               != TFS_SUCCESS)
      {
        TBSYS_LOG(INFO, "parse dest file fail. appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, %s",
                  app_id, uid, dest_file_path);
      }
      else
      {
        if ((ret = store_manager_->update(app_id, uid,
                                          p_meta_info.get_pid(), p_meta_info.get_id(),
                                          p_meta_info.get_name(), p_meta_info.get_name_len(),
                                          dest_p_meta_info.get_pid(), dest_p_meta_info.get_id(),
                                          dest_p_meta_info.get_name(), dest_p_meta_info.get_name_len(),
                                          name, name_len, dest_name, dest_name_len, type)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "mv fail: %s, type: %d, ret: %d", file_path, type, ret);
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
      char name[MAX_META_FILE_NAME_LEN];
      int32_t name_len = 0;
      int ret = TFS_SUCCESS;
      still_have = false;

      MetaInfo p_meta_info;
      std::vector<MetaInfo> tmp_v_meta_info;
      std::vector<std::string> v_name;

      if ((ret = parse_file_path(app_id, uid, file_path, p_meta_info, name, name_len)) != TFS_SUCCESS)
      {
        TBSYS_LOG(INFO, "parse file path fail. appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, %s",
                  app_id, uid, file_path);
      }
      else
      {
        int64_t last_offset = 0;
        ret = get_meta_info(app_id, uid, p_meta_info.get_id(), name, name_len, offset, true,
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

      return ret;
    }

    int MetaServerService::write(const int64_t app_id, const int64_t uid,
                                 const char* file_path, const FragInfo& frag_info)
    {
      char name[MAX_META_FILE_NAME_LEN];
      int32_t name_len = 0;
      int ret = TFS_SUCCESS;

      MetaInfo p_meta_info;
      std::vector<MetaInfo> tmp_v_meta_info;

      if ((ret = parse_file_path(app_id, uid, file_path, p_meta_info, name, name_len)) != TFS_SUCCESS)
      {
        TBSYS_LOG(INFO, "parse file path fail. appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, %s",
                  app_id, uid, file_path);
      }
      else
      {
        vector<FragMeta> v_frag_meta(frag_info.v_frag_meta_);
        sort(v_frag_meta.begin(), v_frag_meta.end());
        vector<FragMeta>::iterator write_frag_meta_it = v_frag_meta.begin();

        // we use while, so we can use break instead of goto, no loop here actually
        do
        {
          tmp_v_meta_info.clear();
          int32_t in_cluster_id = -1;
          int64_t last_offset = 0;
          ret = get_meta_info(app_id, uid, p_meta_info.get_id(), name, name_len,
                              write_frag_meta_it->offset_, true, tmp_v_meta_info, in_cluster_id, last_offset);

          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(DEBUG, "record not exist, name(%s)", name);
            break;
          }
          if (in_cluster_id == -1)
          {
            ret = EXIT_NOT_CREATE_ERROR;
            break;
          }
          if (in_cluster_id != 0 && frag_info.cluster_id_ != in_cluster_id)
          {
            ret = EXIT_CLUSTER_ID_ERROR;
            break;
          }
          if (tmp_v_meta_info.empty())
          {
            //this is for split, we make a new metainfo
            add_new_meta_info(p_meta_info.get_id(), frag_info.cluster_id_,
                              name, name_len, last_offset, tmp_v_meta_info);
          }

          bool found_meta_info_should_be_updated = false;
          std::vector<MetaInfo>::iterator v_meta_info_it = tmp_v_meta_info.begin();
          for (; v_meta_info_it != tmp_v_meta_info.end(); v_meta_info_it++)
          {
            if (!v_meta_info_it->frag_info_.had_been_split_ ||
                (write_frag_meta_it->offset_ != -1 &&
                 v_meta_info_it->get_last_offset() > write_frag_meta_it->offset_))
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

          //now write_frag_meta_it  should be write to v_meta_info_it
          if (!v_meta_info_it->frag_info_.had_been_split_)
          {
            add_frag_to_meta(write_frag_meta_it, v_frag_meta.end(), *v_meta_info_it, last_offset);
          }
          else
          {
            int64_t orig_last_offset = v_meta_info_it->get_last_offset();
            while(write_frag_meta_it != frag_info.v_frag_meta_.end())
            {
              if (write_frag_meta_it->offset_ >= orig_last_offset)
              {
                ret = TFS_ERROR;
                break;
              }
              v_meta_info_it->frag_info_.v_frag_meta_.push_back(*write_frag_meta_it);
              write_frag_meta_it ++;
            }

            if (TFS_SUCCESS != ret)
            {
              break;
            }

            // too many frag info
            if (static_cast<int32_t>(v_meta_info_it->frag_info_.v_frag_meta_.size()) >= MAX_FRAG_META_COUNT)
            {
              ret = EXIT_FRAG_META_OVERFLOW_ERROR;
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
              v_meta_info_it->file_info_.size_ = v_meta_info_it->get_last_offset();
              ret = store_manager_->insert(app_id, uid, p_meta_info.get_pid(),
                                           p_meta_info.get_name(), p_meta_info.get_name_len(),
                                           p_meta_info.get_id(), v_meta_info_it->get_name(),
                                           v_meta_info_it->get_name_len(),PWRITE_FILE,
                                           &(*v_meta_info_it));
            }
          }
          assert(write_frag_meta_it == v_frag_meta.end());
        } while (write_frag_meta_it != v_frag_meta.end() && TFS_SUCCESS == ret);

      }

      return ret;
    }

    int MetaServerService::ls(const int64_t app_id, const int64_t uid, const int64_t pid,
                              const char* file_path, const FileType file_type,
                              std::vector<MetaInfo>& v_meta_info, bool& still_have)
    {
      // for inner name data struct
      char name[MAX_META_FILE_NAME_LEN+16] = {'\0'};
      int32_t name_len = 1;
      int ret = TFS_SUCCESS;
      FileType my_file_type = file_type;
      MetaInfo p_meta_info;
      bool ls_file = false;
      still_have = true;

      // first ls. parse file info
      if (-1 == pid)
      {
        // just list single file
        ls_file = (file_type != DIRECTORY);

        if ((ret = parse_file_path(app_id, uid, file_path, p_meta_info, name, name_len, true)) != TFS_SUCCESS)
        {
          TBSYS_LOG(INFO, "parse file path fail. appid: %"PRI64_PREFIX"d, uid: %"PRI64_PREFIX"d, %s",
                    app_id, uid, file_path);
        }
        else if (DIRECTORY == file_type) // ls directory, should get current directory info
        {
          if ((ret = get_dir_meta_info(app_id, uid, p_meta_info.get_id(), name, name_len, p_meta_info))
              == TFS_SUCCESS)
          {
            // start over
            name[0] = '\0';
            name_len = 1;
          }
        }
      }
      else                      // pid is not -1 means continue last ls
      {
        int32_t file_len = (NULL == file_path ? 0 : strlen(file_path));
        if (file_len >= MAX_META_FILE_NAME_LEN)
        {
          TBSYS_LOG(WARN, "file_path(%s) is invalid", file_path);
          ret = TFS_ERROR;
        }
        else if (file_len > 0)  // continue from file_path
        {
          get_name(file_path, name, MAX_META_FILE_NAME_LEN, name_len);
          next_file_name(name, name_len);
        }

        p_meta_info.file_info_.id_ = pid;
      }

      if (TFS_SUCCESS == ret)
      {
        MetaInfo last_meta_info;
        std::vector<MetaInfo> tmp_v_meta_info;
        vector<MetaInfo>::iterator tmp_v_meta_info_it;
        do
        {
          tmp_v_meta_info.clear();
          still_have = false;

          // return directory and file separately
          // first directory type, then file type
          ret = store_manager_->ls(app_id, uid, p_meta_info.get_id(), name, name_len,
                                   my_file_type != DIRECTORY, tmp_v_meta_info, still_have);

          tmp_v_meta_info_it = tmp_v_meta_info.begin();

          if (my_file_type != DIRECTORY)
          {
            // caclulate file meta info
            calculate_file_meta_info(tmp_v_meta_info_it, tmp_v_meta_info.end(),
                                     ls_file, v_meta_info, last_meta_info);
            // ls file only need one meta info
            if (ls_file && !v_meta_info.empty())
            {
              still_have = false;
              break;
            }
          }
          else
          {
            // just push directory's metainfo
            for (; tmp_v_meta_info_it != tmp_v_meta_info.end() && check_not_out_over(v_meta_info);
                 tmp_v_meta_info_it++)
            {
              v_meta_info.push_back(*tmp_v_meta_info_it);
            }
          }

          if (!check_not_out_over(v_meta_info))
          {
            // not list all
            if (tmp_v_meta_info_it != tmp_v_meta_info.end())
            {
              still_have = true;
            }
            break;
          }

          // directory over, continue list file
          if (my_file_type == DIRECTORY && !still_have)
          {
            my_file_type = NORMAL_FILE;
            still_have = true;
          }

          // still have and need continue
          if (still_have)
          {
            next_file_name(name, name_len);
          }
        } while (TFS_SUCCESS == ret && check_not_out_over(v_meta_info) && still_have);


        if (TFS_SUCCESS == ret && !ls_file && check_not_out_over(v_meta_info)
            && !last_meta_info.empty())
        {
          v_meta_info.push_back(last_meta_info);
        }
      }

      return ret;
    }

    // parse file path.
    // return parent directory's metainfo of current file(directory),
    // and file name(store format) without any prefix directory
    int MetaServerService::parse_file_path(const int64_t app_id, const int64_t uid, const char* file_path,
                                           MetaInfo& p_meta_info, char* name, int32_t& name_len, const bool root_ok)
    {
      int ret = TFS_SUCCESS;
      std::vector<std::string> v_name;

      if ((ret = parse_name(file_path, v_name)) != TFS_SUCCESS)
      {
        TBSYS_LOG(INFO, "file_path(%s) is invalid", file_path);
      }
      else if (get_depth(v_name) == 0)
      {
        if (root_ok) // file_path only has "/"
        {
          p_meta_info.file_info_.id_ = 0;
          ret = get_name(v_name[0].c_str(), name, MAX_META_FILE_NAME_LEN, name_len);
        }
        else
        {
          ret = TFS_ERROR;
        }
      }
      else if ((ret = get_p_meta_info(app_id, uid, v_name, p_meta_info)) == TFS_SUCCESS)
      {
        ret = get_name(v_name[get_depth(v_name)].c_str(), name, MAX_META_FILE_NAME_LEN, name_len);
      }

      return ret;
    }


    // get direct parent directory's meta info
    int MetaServerService::get_p_meta_info(const int64_t app_id, const int64_t uid,
                                           const std::vector<std::string>& v_name,
                                           MetaInfo& out_meta_info)
    {
      int ret = TFS_ERROR;
      int32_t depth = get_depth(v_name);

      char name[MAX_META_FILE_NAME_LEN];
      int32_t name_len = 0;
      std::vector<MetaInfo> tmp_v_meta_info;
      int64_t pid = 0;

      for (int32_t i = 0; i < depth; i++)
      {
        if ((ret = get_name(v_name[i].c_str(), name, MAX_META_FILE_NAME_LEN, name_len)) != TFS_SUCCESS)
        {
          break;
        }

        ret = store_manager_->select(app_id, uid, pid, name, name_len, false, tmp_v_meta_info);

        if (ret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "select name(%s) failed, ret: %d", name, ret);
          break;
        }

        if (tmp_v_meta_info.empty())
        {
          ret = TFS_ERROR;
          TBSYS_LOG(DEBUG, "file(%s) not found, ret: %d", name, ret);
          break;
        }

        // parent's id is its child's pid
        pid = tmp_v_meta_info.begin()->get_id();

        if (i == depth - 1)
        {
          // just get metainfo, no matter which record
          out_meta_info = *(tmp_v_meta_info.begin());
          ret = TFS_SUCCESS;
          break;
        }
      }

      return ret;
    }

    int MetaServerService::get_dir_meta_info(const int64_t app_id, const int64_t uid, const int64_t pid,
                                             const char* name, const int32_t name_len,
                                             MetaInfo& out_meta_info)
    {
      std::vector<MetaInfo> tmp_v_meta_info;
      int ret = store_manager_->select(app_id, uid, pid,
                                       name, name_len, false, tmp_v_meta_info);
      if (ret != TFS_SUCCESS || tmp_v_meta_info.empty())
      {
        TBSYS_LOG(INFO, "get directory meta info fail. %s, ret: %d", name, ret);
        ret = TFS_ERROR;
      }
      else
      {
        // copy
        out_meta_info.copy_no_frag(*tmp_v_meta_info.begin());
      }

      return ret;
    }

    int MetaServerService::get_meta_info(const int64_t app_id, const int64_t uid, const int64_t pid,
                                         const char* name, const int32_t name_len,
                                         const int64_t offset, const bool is_file,
                                         std::vector<MetaInfo>& tmp_v_meta_info,
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

        ret = store_manager_->select(app_id, uid, pid,
                                     search_name, search_name_len, is_file, tmp_v_meta_info);

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
            memcpy(search_name, last_metaInfo.get_name(), last_metaInfo.get_name_len());
            search_name_len = last_metaInfo.get_name_len();
            next_file_name(search_name, search_name_len);
          }
        }
      } while(TFS_SUCCESS == ret && still_have);

      return ret;
    }

    void MetaServerService::calculate_file_meta_info(std::vector<common::MetaInfo>::iterator& meta_info_begin,
                                                     const std::vector<common::MetaInfo>::iterator meta_info_end,
                                                     const bool ls_file,
                                                     std::vector<common::MetaInfo>& v_meta_info,
                                                     common::MetaInfo& last_meta_info)
    {
      for (; meta_info_begin != meta_info_end && check_not_out_over(v_meta_info); meta_info_begin++)
      {
        if (last_meta_info.empty()) // no last file
        {
          last_meta_info.copy_no_frag(*meta_info_begin);
          if (!meta_info_begin->frag_info_.had_been_split_) // had NOT split, this is a completed file recored
          {
            v_meta_info.push_back(last_meta_info);
            last_meta_info.file_info_.name_.clear(); // empty last file
          }
        }
        else                    // have last file, need check whether this metainfo is of last file or not.
        {
          // this metaInfo is also of last file.
          if (0 == memcmp(last_meta_info.get_name(), meta_info_begin->get_name(),
                          last_meta_info.get_name_len()))
          {
            // get_size() is the max file size that current recored hold
            last_meta_info.file_info_.size_ = meta_info_begin->get_size();
            if (!meta_info_begin->frag_info_.had_been_split_) // had NOT split, last file is completed
            {
              v_meta_info.push_back(last_meta_info);
              last_meta_info.file_info_.name_.clear();
            }
          }
          else                  // this metainfo is not of last file,
          {
            v_meta_info.push_back(last_meta_info); // then last file is completed
            last_meta_info.copy_no_frag(*meta_info_begin);
            if (!meta_info_begin->frag_info_.had_been_split_) // had NOT split, thie metainfo is completed
            {
              v_meta_info.push_back(last_meta_info);
              last_meta_info.file_info_.name_.clear();
            }
          }
        }

        if (ls_file && !v_meta_info.empty()) // if list file, only need one metainfo.
        {
          break;
        }
      }
      return;
    }

    void MetaServerService::add_frag_to_meta(vector<FragMeta>::iterator& frag_meta_begin,
                                             vector<FragMeta>::iterator frag_meta_end,
                                             MetaInfo& meta_info, int64_t& last_offset)
    {
      while(frag_meta_begin != frag_meta_end)
      {
        if (-1 == frag_meta_begin->offset_) // new metainfo
        {
          frag_meta_begin->offset_ = last_offset;
          last_offset += frag_meta_begin->size_;
        }
        meta_info.frag_info_.v_frag_meta_.push_back(*frag_meta_begin);
        frag_meta_begin++;
      }

      if (static_cast<int32_t>(meta_info.frag_info_.v_frag_meta_.size())
          > SOFT_MAX_FRAG_META_COUNT)
      {
        TBSYS_LOG(DEBUG, "split meta_info");
        meta_info.frag_info_.had_been_split_ = true;
      }
    }

    void MetaServerService::add_new_meta_info(const int64_t pid, const int32_t cluster_id,
                                               const char* name, const int32_t name_len, const int64_t last_offset,
                                               std::vector<MetaInfo>& tmp_v_meta_info)
    {
      MetaInfo tmp;
      tmp.file_info_.pid_ = pid;
      tmp.frag_info_.cluster_id_ = cluster_id;
      tmp.frag_info_.had_been_split_ = false;
      char tmp_name[MAX_META_FILE_NAME_LEN + 8];
      memcpy(tmp_name, name, name_len);
      int64_to_char(tmp_name+name_len, MAX_META_FILE_NAME_LEN + 8 - name_len,
                    last_offset);
      tmp.file_info_.name_.assign(tmp_name, name_len + 8);
      tmp_v_meta_info.push_back(tmp);
    }

    int MetaServerService::read_frag_info(const vector<MetaInfo>& v_meta_info, const int64_t offset,
                                          const int64_t size, int32_t& cluster_id,
                                          vector<FragMeta>& v_out_frag_meta, bool& still_have)
    {
      int64_t end_offset = offset + size;
      vector<MetaInfo>::const_iterator meta_info_it = v_meta_info.begin();;
      cluster_id = 0;
      v_out_frag_meta.clear();
      still_have = false;

      while (meta_info_it != v_meta_info.end())
      {
        if (meta_info_it->get_last_offset() <= offset)
        {
          meta_info_it++;
          continue;
        }

        const vector<FragMeta>& v_in_frag_meta = meta_info_it->frag_info_.v_frag_meta_;
        if (v_in_frag_meta.empty())
        {
          //no frag to be read
          break;
        }

        FragMeta fragmeta_for_search;
        fragmeta_for_search.offset_ = offset;
        vector<FragMeta>::const_iterator it =
          lower_bound(v_in_frag_meta.begin(), v_in_frag_meta.end(), fragmeta_for_search);

        // check whether previous metainfo is needed
        if (it != v_in_frag_meta.begin())
        {
          vector<FragMeta>::const_iterator tmp_it = it - 1;
          if (offset < tmp_it->offset_ + tmp_it->size_)
          {
            it--;
          }
        }

        cluster_id = meta_info_it->frag_info_.cluster_id_;
        still_have = true;

        for(; it != v_in_frag_meta.end() && check_not_out_over(v_out_frag_meta); it++)
        {
          v_out_frag_meta.push_back(*it);
          if (it->offset_ + it->size_ > end_offset)
          {
            still_have = false;
            break;
          }
        }

        if (!meta_info_it->frag_info_.had_been_split_ && it == v_in_frag_meta.end())
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
    int MetaServerService::get_name(const char* name, char* buffer, const int32_t buffer_len, int32_t& name_len)
    {
      int ret = TFS_SUCCESS;
      name_len = strlen(name) + 1;

      if (name_len > MAX_META_FILE_NAME_LEN || name_len >= buffer_len || buffer == NULL)
      {
        TBSYS_LOG(ERROR, "buffer is not enough or buffer is null, %d < %d", buffer_len, name_len);
        ret = TFS_ERROR;
      }
      else
      {
        snprintf(buffer, name_len + 1, "%c%s", char(name_len - 1), name);
        TBSYS_LOG(DEBUG, "old_name: %s, new_name: %s, name_len: %d", name, buffer, name_len);
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
      // TODO. no need check all over..
      int ret = TFS_SUCCESS;

      if (frag_info.v_frag_meta_.size() > 0 && frag_info.cluster_id_ <= 0)
      {
        TBSYS_LOG(ERROR, "cluster id error %d", frag_info.cluster_id_);
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        if (static_cast<int32_t>(frag_info.v_frag_meta_.size())
            > SOFT_MAX_FRAG_META_COUNT &&
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
            TBSYS_LOG(ERROR, "frag info have some error, %"PRI64_PREFIX"d < %"PRI64_PREFIX"d",
                      frag_info.v_frag_meta_[i].offset_, last_offset);
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

    void MetaServerService::next_file_name(char* name, int32_t& name_len)
    {
      int64_t skip = 1;
      if (name_len == (unsigned char)name[0] + 1)
      {
        int64_to_char(name + name_len, 8, skip);
        name_len += 8;
      }
      else
      {
        char_to_int64(name + name_len - 8, 8, skip);
        skip += 1;
        int64_to_char(name + name_len - 8, 8, skip);
      }

    }
  }
}
