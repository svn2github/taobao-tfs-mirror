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
 *   daoan <daoan@taobao.com>
 *      - initial release
 *
 */

#include "meta_kv_service.h"

#include <zlib.h>
#include "common/config_item.h"
#include "common/parameter.h"
#include "common/base_packet.h"
using namespace tfs::common;
using namespace tfs::message;
using namespace std;
namespace tfs
{
  namespace metawithkv
  {
    MetaKvService::MetaKvService()
    {
    }

    MetaKvService::~MetaKvService()
    {
    }

    tbnet::IPacketStreamer* MetaKvService::create_packet_streamer()
    {
      return new common::BasePacketStreamer();
    }

    void MetaKvService::destroy_packet_streamer(tbnet::IPacketStreamer* streamer)
    {
      tbsys::gDelete(streamer);
    }

    common::BasePacketFactory* MetaKvService::create_packet_factory()
    {
      return new message::MessageFactory();
    }

    void MetaKvService::destroy_packet_factory(common::BasePacketFactory* factory)
    {
      tbsys::gDelete(factory);
    }

    const char* MetaKvService::get_log_file_path()
    {
      return NULL;
    }

    const char* MetaKvService::get_pid_file_path()
    {
      return NULL;
    }

    int MetaKvService::initialize(int argc, char* argv[])
    {
      int ret = TFS_SUCCESS;
      UNUSED(argc);
      UNUSED(argv);

      if ((ret = SYSPARAM_KVMETA.initialize(config_file_)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "call SYSPARAM_METAKVSERVER::initialize fail. ret: %d", ret);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = meta_info_helper_.init();
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "MetaKv server initial fail: %d", ret);
        }
      }

      return ret;
    }

    int MetaKvService::destroy_service()
    {
      return TFS_SUCCESS;
    }

    bool MetaKvService::handlePacketQueue(tbnet::Packet *packet, void *args)
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
        KvReqPutMetaMessage* kv_req_put_meta_msg = NULL;
        KvReqGetMetaMessage* kv_req_get_meta_msg = NULL;
        switch (base_packet->getPCode())
        {
          case KV_REQ_PUT_META_MESSAGE:
            kv_req_put_meta_msg = dynamic_cast<KvReqPutMetaMessage*>(base_packet);
            ret = put_meta(kv_req_put_meta_msg);
            break;
          case KV_REQ_GET_META_MESSAGE:
            kv_req_get_meta_msg = dynamic_cast<KvReqGetMetaMessage*>(base_packet);
            ret = get_meta(kv_req_get_meta_msg);
            break;
          default:
            ret = EXIT_UNKNOWN_MSGTYPE;
            TBSYS_LOG(ERROR, "unknown msg type: %d", base_packet->getPCode());
            break;
        }
      }

      if (ret != TFS_SUCCESS && NULL != base_packet)
      {
        base_packet->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "execute message failed");
      }

      // always return true. packet will be freed by caller
      return true;
    }

    int MetaKvService::put_meta(KvReqPutMetaMessage* kv_req_put_meta_msg)
    {
      int ret = TFS_SUCCESS;

      if (NULL == kv_req_put_meta_msg)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "MetaKvService::put_meta fail, ret: %d", ret);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = meta_info_helper_.put_meta(kv_req_put_meta_msg->get_bucket_name(), kv_req_put_meta_msg->get_file_name(), kv_req_put_meta_msg->get_file_info());
      }

      if (TFS_SUCCESS != ret)
      {
        ret = kv_req_put_meta_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "execute message fail");
      }
      else
      {
        ret = kv_req_put_meta_msg->reply(new StatusMessage(STATUS_MESSAGE_OK));
      }
      //stat_info_helper_.put_meta()
      return ret;
    }

    int MetaKvService::get_meta(KvReqGetMetaMessage* kv_req_get_meta_msg)
    {
      int ret = TFS_SUCCESS;

      if (NULL == kv_req_get_meta_msg)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "MetaKvService::get_meta fail, ret: %d", ret);
      }

      KvRspGetMetaMessage* kv_rsp_get_meta_msg = new KvRspGetMetaMessage();
      if (TFS_SUCCESS == ret)
      {
        ret = meta_info_helper_.get_meta(kv_req_get_meta_msg->get_bucket_name(), kv_req_get_meta_msg->get_file_name(), kv_rsp_get_meta_msg->get_file_info());
      }

      if (TFS_SUCCESS != ret)
      {
        ret = kv_req_get_meta_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "execute message fail");
      }
      else
      {
        ret = kv_req_get_meta_msg->reply(kv_rsp_get_meta_msg);
      }
      //stat_info_helper_.put_meta()
      return ret;
    }

  }/** metawithkv **/
}/** tfs **/
