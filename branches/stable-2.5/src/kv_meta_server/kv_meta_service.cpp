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

#include "kv_meta_service.h"

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
  namespace kvmetaserver
  {
    KvMetaService::KvMetaService()
    :tfs_kv_meta_stat_ ("tfs_kv_meta_stat_")
    {
    }

    KvMetaService::~KvMetaService()
    {
    }

    tbnet::IPacketStreamer* KvMetaService::create_packet_streamer()
    {
      return new BasePacketStreamer();
    }

    void KvMetaService::destroy_packet_streamer(tbnet::IPacketStreamer* streamer)
    {
      tbsys::gDelete(streamer);
    }

    BasePacketFactory* KvMetaService::create_packet_factory()
    {
      return new message::MessageFactory();
    }

    void KvMetaService::destroy_packet_factory(BasePacketFactory* factory)
    {
      tbsys::gDelete(factory);
    }

    const char* KvMetaService::get_log_file_path()
    {
      const char* log_file_path = NULL;
      const char* work_dir = get_work_dir();
      if (work_dir != NULL)
      {
        log_file_path_ = work_dir;
        log_file_path_ += "/logs/kvmetaserver";
        log_file_path_ +=  ".log";
        log_file_path = log_file_path_.c_str();
      }
      return log_file_path;
    }

    const char* KvMetaService::get_pid_file_path()
    {
      const char* pid_file_path = NULL;
      const char* work_dir = get_work_dir();
      if (work_dir != NULL)
      {
        pid_file_path_ = work_dir;
        pid_file_path_ += "/logs/kvmetaserver";
        pid_file_path_ += ".pid";
        pid_file_path = pid_file_path_.c_str();
      }
      return pid_file_path;
    }

    int KvMetaService::initialize(int argc, char* argv[])
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

      //init global stat
      ret = stat_mgr_.initialize(get_timer());
      if (ret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "%s", "initialize stat manager fail");
      }
      else
      {
        int64_t current = tbsys::CTimeUtil::getTime();
        StatEntry<string, string>::StatEntryPtr stat_ptr = new StatEntry<string, string>(tfs_kv_meta_stat_, current, false);
        stat_ptr->add_sub_key("put_bucket");
        stat_ptr->add_sub_key("get_bucket");
        stat_ptr->add_sub_key("del_bucket");
        stat_ptr->add_sub_key("head_bucket");
        stat_ptr->add_sub_key("put_object");
        stat_ptr->add_sub_key("get_object");
        stat_ptr->add_sub_key("del_object");
        stat_ptr->add_sub_key("head_object");
        stat_mgr_.add_entry(stat_ptr, SYSPARAM_KVMETA.dump_stat_info_interval_);
      }

      //init heart
      if (TFS_SUCCESS == ret)
      {

        bool ms_ip_same_flag = false;
        ms_ip_same_flag = tbsys::CNetUtil::isLocalAddr(SYSPARAM_KVMETA.ms_ip_port_);

        if (true == ms_ip_same_flag)
        {
          local_ipport_id_ = SYSPARAM_KVMETA.ms_ip_port_;
          kvroot_ipport_id_ = SYSPARAM_KVMETA.rs_ip_port_;
          server_start_time_ = time(NULL);
          ret = heart_manager_.initialize(kvroot_ipport_id_, local_ipport_id_, server_start_time_);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "init heart_manager error");
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "is not local ip ret: %d", ret);
          ret = TFS_ERROR;
        }
      }
      return ret;
    }

    int KvMetaService::destroy_service()
    {
      //global stat destroy
      stat_mgr_.destroy();
      heart_manager_.destroy();
      return TFS_SUCCESS;
    }

    bool KvMetaService::handlePacketQueue(tbnet::Packet *packet, void *args)
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
          case REQ_KVMETA_DEL_OBJECT_MESSAGE:
            ret = del_object(dynamic_cast<ReqKvMetaDelObjectMessage*>(base_packet));
            break;
          case REQ_KVMETA_HEAD_OBJECT_MESSAGE:
            ret = head_object(dynamic_cast<ReqKvMetaHeadObjectMessage*>(base_packet));
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
          case REQ_KVMETA_HEAD_BUCKET_MESSAGE:
            ret = head_bucket(dynamic_cast<ReqKvMetaHeadBucketMessage*>(base_packet));
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

    int KvMetaService::put_object(ReqKvMetaPutObjectMessage* req_put_object_msg)
    {
      int ret = TFS_SUCCESS;
      if (NULL == req_put_object_msg)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "KvMetaService::put_object fail, ret: %d", ret);
      }

      if (TFS_SUCCESS == ret)
      {
        //tbutil::Time start = tbutil::Time::now();
        ObjectInfo object_info = req_put_object_msg->get_object_info();
        const UserInfo &user_info = req_put_object_msg->get_user_info();
        ret = meta_info_helper_.put_object(req_put_object_msg->get_bucket_name(),
            req_put_object_msg->get_file_name(), object_info, user_info);
        //tbutil::Time end = tbutil::Time::now();
        //TBSYS_LOG(INFO, "put_object cost: %"PRI64_PREFIX"d", (int64_t)(end - start).toMilliSeconds());
      }

      if (TFS_SUCCESS != ret)
      {
        ret = req_put_object_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "put object fail");
      }
      else
      {
        ret = req_put_object_msg->reply(new StatusMessage(STATUS_MESSAGE_OK));
        stat_mgr_.update_entry(tfs_kv_meta_stat_, "put_object", 1);
      }
      return ret;
    }

    int KvMetaService::get_object(ReqKvMetaGetObjectMessage* req_get_object_msg)
    {
      int ret = TFS_SUCCESS;
      if (NULL == req_get_object_msg)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "KvMetaService::get_object fail, ret: %d", ret);
      }
      if (TFS_SUCCESS == ret)
      {
        ObjectInfo object_info;
        bool still_have = false;
        ret = meta_info_helper_.get_object(req_get_object_msg->get_bucket_name(),
                                           req_get_object_msg->get_file_name(),
                                           req_get_object_msg->get_offset(),
                                           req_get_object_msg->get_length(),
                                           &object_info, &still_have);
        TBSYS_LOG(DEBUG, "get object, bucket_name: %s , object_name: %s, still_have: %d owner_id: %"PRI64_PREFIX"d ret: %d",
                  req_get_object_msg->get_bucket_name().c_str(),
                  req_get_object_msg->get_file_name().c_str(),
                  still_have,
                  object_info.meta_info_.owner_id_,
                  ret);

        if (TFS_SUCCESS == ret)
        {
          RspKvMetaGetObjectMessage* rsp_get_object_msg = new(std::nothrow) RspKvMetaGetObjectMessage();
          assert(NULL != rsp_get_object_msg);
          rsp_get_object_msg->set_object_info(object_info);
          rsp_get_object_msg->set_still_have(still_have);
          object_info.dump();

          ret = req_get_object_msg->reply(rsp_get_object_msg);
          stat_mgr_.update_entry(tfs_kv_meta_stat_, "get_object", 1);
        }
        else
        {
          ret = req_get_object_msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR),
              ret, "get object fail");
        }
      }
      return ret;
    }

    int KvMetaService::del_object(ReqKvMetaDelObjectMessage* req_del_object_msg)
    {
      int ret = TFS_SUCCESS;

      if (NULL == req_del_object_msg)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "KvMetaService::del_object fail, ret: %d", ret);
      }

      ObjectInfo object_info;
      bool still_have = false;
      if (TFS_SUCCESS == ret)
      {
        ret = meta_info_helper_.del_object(req_del_object_msg->get_bucket_name(),
                                           req_del_object_msg->get_file_name(),
                                           &object_info, &still_have);
      }

      if (TFS_SUCCESS == ret)
      {
        RspKvMetaDelObjectMessage* rsp_del_object_msg = new(std::nothrow) RspKvMetaDelObjectMessage();
          assert(NULL != rsp_del_object_msg);
          rsp_del_object_msg->set_object_info(object_info);
          rsp_del_object_msg->set_still_have(still_have);
          req_del_object_msg->reply(rsp_del_object_msg);
        stat_mgr_.update_entry(tfs_kv_meta_stat_, "del_object", 1);
      }
      else
      {
        ret = req_del_object_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "del object fail");
      }
      return ret;
    }

    int KvMetaService::head_object(ReqKvMetaHeadObjectMessage* req_head_object_msg)
    {
      int ret = TFS_SUCCESS;

      if (NULL == req_head_object_msg)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "KvMetaService::head_object fail, ret: %d", ret);
      }

      RspKvMetaHeadObjectMessage *rsp = new RspKvMetaHeadObjectMessage();
      if (TFS_SUCCESS == ret)
      {
        ret = meta_info_helper_.head_object(req_head_object_msg->get_bucket_name(),
            req_head_object_msg->get_file_name(), rsp->get_mutable_object_info());
      }

      if (TFS_SUCCESS != ret)
      {
        ret = req_head_object_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "head object fail");
        tbsys::gDelete(rsp);
      }
      else
      {
        ret = req_head_object_msg->reply(rsp);
        stat_mgr_.update_entry(tfs_kv_meta_stat_, "head_object", 1);
      }
      return ret;
    }

    int KvMetaService::put_bucket(ReqKvMetaPutBucketMessage* put_bucket_msg)
    {
      int ret = TFS_SUCCESS;

      if (NULL == put_bucket_msg)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "KvMetaService::put_bucket fail, ret: %d", ret);
      }

      if (TFS_SUCCESS == ret)
      {
        BucketMetaInfo *bucket_meta_info = put_bucket_msg->get_mutable_bucket_meta_info();
        const UserInfo &user_info = put_bucket_msg->get_user_info();

        ret = meta_info_helper_.put_bucket(put_bucket_msg->get_bucket_name(), *bucket_meta_info, user_info);
      }

      if (TFS_SUCCESS != ret)
      {
        ret = put_bucket_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "put bucket fail");
      }
      else
      {
        ret = put_bucket_msg->reply(new StatusMessage(STATUS_MESSAGE_OK));
        stat_mgr_.update_entry(tfs_kv_meta_stat_, "put_bucket", 1);
      }
      //stat_info_helper_.put_bucket()
      return ret;
    }

    int KvMetaService::get_bucket(ReqKvMetaGetBucketMessage* get_bucket_msg)
    {
      int ret = TFS_SUCCESS;

      if (NULL == get_bucket_msg)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "KvMetaService::get_bucket fail, ret: %d", ret);
      }

      RspKvMetaGetBucketMessage* rsp = new RspKvMetaGetBucketMessage();

      if (TFS_SUCCESS == ret)
      {
        const string& bucket_name = get_bucket_msg->get_bucket_name();
        const string& prefix = get_bucket_msg->get_prefix();
        const string& start_key = get_bucket_msg->get_start_key();
        char delimiter = get_bucket_msg->get_delimiter();
        int32_t limit = get_bucket_msg->get_mutable_limit();

        ret = meta_info_helper_.get_bucket(bucket_name, prefix, start_key, delimiter, &limit,
            rsp->get_mutable_v_object_meta_info(), rsp->get_mutable_v_object_name(), rsp->get_mutable_s_common_prefix(),
            rsp->get_mutable_truncated());

        if (TFS_SUCCESS == ret)
        {
          rsp->set_bucket_name(bucket_name);
          rsp->set_prefix(prefix);
          rsp->set_start_key(start_key);
          rsp->set_delimiter(delimiter);
          rsp->set_limit(limit);
        }
      }

      if (TFS_SUCCESS != ret)
      {
        ret = get_bucket_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "get bucket fail");
        tbsys::gDelete(rsp);
      }
      else
      {
        ret = get_bucket_msg->reply(rsp);
        stat_mgr_.update_entry(tfs_kv_meta_stat_, "get_bucket", 1);
      }
      //stat_info_helper_.get_bucket()
      return ret;
    }

    int KvMetaService::del_bucket(ReqKvMetaDelBucketMessage* del_bucket_msg)
    {
      int ret = TFS_SUCCESS;

      if (NULL == del_bucket_msg)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "KvMetaService::del_bucket fail, ret: %d", ret);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = meta_info_helper_.del_bucket(del_bucket_msg->get_bucket_name());
      }

      if (TFS_SUCCESS != ret)
      {
        ret = del_bucket_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "del bucket fail");
      }
      else
      {
        ret = del_bucket_msg->reply(new StatusMessage(STATUS_MESSAGE_OK));
        stat_mgr_.update_entry(tfs_kv_meta_stat_, "del_bucket", 1);
      }
      //stat_info_helper_.put_bucket()
      return ret;
    }

    int KvMetaService::head_bucket(ReqKvMetaHeadBucketMessage* head_bucket_msg)
    {
      int ret = TFS_SUCCESS;

      if (NULL == head_bucket_msg)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "KvMetaService::head_bucket fail, ret: %d", ret);
      }

      RspKvMetaHeadBucketMessage *rsp = new RspKvMetaHeadBucketMessage();
      if (TFS_SUCCESS == ret)
      {
        ret = meta_info_helper_.head_bucket(head_bucket_msg->get_bucket_name(), rsp->get_mutable_bucket_meta_info());
      }

      if (TFS_SUCCESS != ret)
      {
        ret = head_bucket_msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret, "head bucket fail");
        tbsys::gDelete(rsp);
      }
      else
      {
        ret = head_bucket_msg->reply(rsp);
        stat_mgr_.update_entry(tfs_kv_meta_stat_, "head_bucket", 1);
      }
      //stat_info_helper_.put_bucket()
      return ret;
    }

  }/** kvmetaserver **/
}/** tfs **/
