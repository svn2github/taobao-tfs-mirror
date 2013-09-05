/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: meta_server_service.h 49 2011-08-08 09:58:57Z nayan@taobao.com $
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_KVMETASERVER_KV_META_SERVICE_H_
#define TFS_KVMETASERVER_KV_META_SERVICE_H_

#include "common/parameter.h"
#include "common/base_service.h"
#include "common/status_message.h"
#include "common/statistics.h"
#include "message/message_factory.h"

#include "meta_info_helper.h"
#include "life_cycle_helper.h"
#include "kv_meta_heart_manager.h"
namespace tfs
{
  namespace kvmetaserver
  {
    class KvMetaService : public common::BaseService
    {
    public:
      KvMetaService();
      virtual ~KvMetaService();

    public:
      // override
      virtual tbnet::IPacketStreamer* create_packet_streamer();
      virtual void destroy_packet_streamer(tbnet::IPacketStreamer* streamer);
      virtual common::BasePacketFactory* create_packet_factory();
      virtual void destroy_packet_factory(common::BasePacketFactory* factory);
      virtual const char* get_log_file_path();
      virtual const char* get_pid_file_path();
      virtual bool handlePacketQueue(tbnet::Packet* packet, void* args);
      virtual int initialize(int argc, char* argv[]);
      virtual int destroy_service();

      int put_object(message::ReqKvMetaPutObjectMessage* put_object_msg);
      int get_object(message::ReqKvMetaGetObjectMessage* get_object_msg);
      int put_bucket(message::ReqKvMetaPutBucketMessage* put_bucket_msg);
      int get_bucket(message::ReqKvMetaGetBucketMessage* get_bucket_msg);
      int del_bucket(message::ReqKvMetaDelBucketMessage* del_bucket_msg);
      int del_object(message::ReqKvMetaDelObjectMessage* del_object_msg);
      int head_object(message::ReqKvMetaHeadObjectMessage *head_object_msg);
      int head_bucket(message::ReqKvMetaHeadBucketMessage *head_bucket_msg);
      int set_file_lifecycle(message::ReqKvMetaSetLifeCycleMessage *set_lifecycle_msg);
      int get_file_lifecycle(message::ReqKvMetaGetLifeCycleMessage *get_lifecycle_msg);
      int rm_file_lifecycle(message::ReqKvMetaRmLifeCycleMessage *rm_lifecycle_msg);

    private:
      DISALLOW_COPY_AND_ASSIGN(KvMetaService);

      uint64_t kvroot_ipport_id_;
      uint64_t local_ipport_id_;
      int64_t server_start_time_;
      MetaInfoHelper meta_info_helper_;
      LifeCycleHelper life_cycle_helper_;
      common::KvEngineHelper* kv_engine_helper_;

      //global stat
      tbutil::TimerPtr timer_;
      common::StatManager<std::string, std::string, common::StatEntry > stat_mgr_;
      std::string tfs_kv_meta_stat_;
      //StatInfoHelper stat_info_helper_;

      HeartManager heart_manager_;
    };
  }
}

#endif
