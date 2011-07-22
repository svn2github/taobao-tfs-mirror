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

namespace tfs
{
  namespace namemetaserver
  {
    MetaServerService::MetaServerService() : store_manager_(NULL)
    {
      
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

      if ((ret = parse_file_path(p_meta_info, app_id, uid, file_path)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "parse file path fail: %s, ret: %d", file_path, ret);
      }
      else if ((ret = store_manager_->insert(app_id, uid, p_meta_info.pid_, p_meta_info.id_,,
                                             pname, pname_len, name, name_len, type)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "create fail: %s, type: %d, ret: %d", file_path, type, ret);
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

      else if ((ret = parse_file_path(p_meta_info, app_id, uid, file_path)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "parse file path fail: %s, ret: %d", file_path, ret);
      }
      else if ((ret = store_manager_->delete(app_id, uid, p_meta_info.pid_, p_meta_info.id_,
                                             pname, pname_len, name, name_len, store_status)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rm fail: %s, type: %d, ret: %d", file_path, type, ret);
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
      int32_t name_len = 0, pname_len = 0, dest_name_len = 0, dest_pname_len = 0;
      int ret = TFS_SUCCESS;
      int sotre_status;
      MetaInfo p_meta_info, dest_p_meta_info;

      if ((ret = parse_file_path(p_meta_info, app_id, uid, file_path)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "parse file path fail: %s, ret: %d", file_path, ret);
      }
      else if ((ret = parse_file_path(dest_p_meta_info, app_id, uid, dest_file_path)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "parse dest file path fail: %s, ret: %d", file_path, ret);
      }
      else if ((ret = store_manager_->
                update(app_id, uid, p_meta_info.pid_, p_meta_info.id_, pname, pname_len, name, name_len
                       dest_p_meta_info.pid_, dest_p_meta_info.id_,
                       dest_pname, dest_pname_len, dest_name, dest_name_len, type, store_stauts)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "mv fail: %s, ");
      }
      return ret;
    }
 
    int MetaServerService::write(const int32_t app_id, const int64_t uid,
                                 const char* file_path, const char* buf,
                                 const int64_t offset, const int64_t size)
    {
      
    }

    int MetaServerService::read(const int64_t app_id, const int64_t uid,
                                const char* file_path, char* buf,
                                const int64_t offset, const int64_t size)
    {
      
    }
  }
}
