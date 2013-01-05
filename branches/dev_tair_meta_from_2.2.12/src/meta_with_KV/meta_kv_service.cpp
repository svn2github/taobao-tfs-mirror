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

#include <time.h>
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
        switch (base_packet->getPCode())
        {
          case REQ_KVMETA_PUT_OBJECT_MESSAGE:
            ret = put_object(dynamic_cast<ReqKvMetaPutObjectMessage*>(base_packet));
            break;
          case REQ_KVMETA_GET_OBJECT_MESSAGE:
            ret = get_object(dynamic_cast<ReqKvMetaGetObjectMessage*>(base_packet));
            break;
          case REQ_KVMETA_PUT_BUCKET_MESSAGE:
            ret = put_bucket(dynamic_cast<ReqKvMetaPutBucketMessage*>(base_packet));
            break;
          case REQ_KVMETA_GET_BUCKET_MESSAGE:
            ret = get_bucket(dynamic_cast<ReqKvMetaGetBucketMessage*>(base_packet));
            break;
          case REQ_KVMETA_DEL_BUCKET_MESSAGE:
            ret = del_bucket(dynamic_cast<ReqKvMetaDelBucketMessage*>(base_packet));
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

    int MetaKvService::put_object(ReqKvMetaPutObjectMessage* put_object_msg)
    {
      int ret = TFS_SUCCESS;

      if (NULL == put_object_msg)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "MetaKvService::put_object fail, ret: %d", ret);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = meta_info_helper_.put_meta(put_object_msg->get_bucket_name(), put_object_msg->get_file_name(), put_object_msg->get_file_info());
      }

      if (TFS_SUCCESS != ret)
      {
        ret = put_object_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "execute message fail");
      }
      else
      {
        ret = put_object_msg->reply(new StatusMessage(STATUS_MESSAGE_OK));
      }
      //stat_info_helper_.put_meta()
      return ret;
    }

    int MetaKvService::get_object(ReqKvMetaGetObjectMessage* get_meta_msg)
    {
      int ret = TFS_SUCCESS;

      if (NULL == get_meta_msg)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "MetaKvService::get_object fail, ret: %d", ret);
      }
      if (TFS_SUCCESS == ret)
      {
        common::TfsFileInfo tfs_file_info;
        ret = meta_info_helper_.get_meta(get_meta_msg->get_bucket_name(), get_meta_msg->get_file_name(), &tfs_file_info);
        if (TFS_SUCCESS == ret)
        {
          RspKvMetaGetObjectMessage* rsp_get_object_msg =
            new(std::nothrow) RspKvMetaGetObjectMessage();
          assert(NULL != rsp_get_object_msg);
          rsp_get_object_msg->set_tfs_file_info(tfs_file_info);
          get_meta_msg->reply(rsp_get_object_msg);
        }
        else
        {
          get_meta_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO),
               ret,  "get object error ret %d", ret);
        }
      }

      //stat_info_helper_.put_meta()
      return ret;
    }

    int MetaKvService::put_bucket(ReqKvMetaPutBucketMessage* put_bucket_msg)
    {
      int ret = TFS_SUCCESS;

      if (NULL == put_bucket_msg)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "MetaKvService::put_bucket fail, ret: %d", ret);
      }

      if (TFS_SUCCESS == ret)
      {
        int64_t now_time = static_cast<int64_t>(time(NULL));
        ret = meta_info_helper_.put_bucket(put_bucket_msg->get_bucket_name(), now_time);
      }

      if (TFS_SUCCESS != ret)
      {
        ret = put_bucket_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "execute message fail");
      }
      else
      {
        ret = put_bucket_msg->reply(new StatusMessage(STATUS_MESSAGE_OK));
      }
      //stat_info_helper_.put_bucket()
      return ret;
    }

    int MetaKvService::get_bucket(ReqKvMetaGetBucketMessage* get_bucket_msg)
    {
      int ret = TFS_SUCCESS;

      if (NULL == get_bucket_msg)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "MetaKvService::get_bucket fail, ret: %d", ret);
      }

      RspKvMetaGetBucketMessage* rsp = new RspKvMetaGetBucketMessage();

      if (TFS_SUCCESS == ret)
      {
        ret = meta_info_helper_.get_bucket(get_bucket_msg->get_bucket_name(), get_bucket_msg->get_prefix(),
            get_bucket_msg->get_start_key(), get_bucket_msg->get_limit(), rsp->get_v_object_name());
      }

      if (TFS_SUCCESS != ret)
      {
        ret = get_bucket_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "execute message fail");
        tbsys::gDelete(rsp);
      }
      else
      {
        ret = get_bucket_msg->reply(rsp);
      }
      //stat_info_helper_.get_bucket()
      return ret;
    }

    int MetaKvService::del_bucket(ReqKvMetaDelBucketMessage* del_bucket_msg)
    {
      int ret = TFS_SUCCESS;

      if (NULL == del_bucket_msg)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "MetaKvService::del_bucket fail, ret: %d", ret);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = meta_info_helper_.del_bucket(del_bucket_msg->get_bucket_name());
      }

      if (TFS_SUCCESS != ret)
      {
        ret = del_bucket_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "execute message fail");
      }
      else
      {
        ret = del_bucket_msg->reply(new StatusMessage(STATUS_MESSAGE_OK));
      }
      //stat_info_helper_.put_bucket()
      return ret;
    }




  }/** metawithkv **/
}/** tfs **/
