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

#ifndef TFS_NAMEMETASERVER_NAMEMETASERVERSERVICE_H_
#define TFS_NAMEMETASERVER_NAMEMETASERVERSERVICE_H_

#include "common/base_service.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "message/meta_nameserver_client_message.h"
#include "meta_store_manager.h"

namespace tfs
{
  namespace namemetaserver
  {
    const int32_t MAX_FRAG_INFO_COUNT = MAX_FRAG_INFO_SIZE/sizeof(FragMeta) - 1;
    class MetaServerService : public common::BaseService
    {
    public:
      MetaServerService();
      ~MetaServerService();

    public:
      // override
      virtual tbnet::IPacketStreamer* create_packet_streamer();
      virtual void destroy_packet_streamer(tbnet::IPacketStreamer* streamer);
      virtual common::BasePacketFactory* create_packet_factory();
      virtual void destroy_packet_factory(common::BasePacketFactory* factory);
      virtual const char* get_log_file_path();
      virtual const char* get_pid_file_path();
      virtual bool handlePacketQueue(tbnet::Packet* packet, void* args);

      int do_action(common::BasePacket* packet);
      int do_write(common::BasePacket* packet);
      int do_read(common::BasePacket* packet);

      int create(const int64_t app_id, const int64_t uid,
          const char* file_path, const FileType type);
      int rm(const int64_t app_id, const int64_t uid,
          const char* file_path, const FileType type);
      int mv(const int64_t app_id, const int64_t uid,
          const char* file_path, const char* dest_file_path, const FileType type);
      int write(const int64_t app_id, const int64_t uid,
          const char* file_path,
          const FragInfo& in_v_frag_info);
      int read(const int64_t app_id, const int64_t uid, const char* file_path,
          const int64_t offset, const int64_t size,
          FragInfo& frag_info, bool& still_have);
      //TODO ls

      static int check_frag_info(const FragInfo& frag_info);
      static int int64_to_char(char* buff, const int32_t buff_size, const int64_t v);
      static int char_to_int64(char* data, const int32_t data_size, int64_t& v);

    private:
      // override
      virtual int initialize(int argc, char* argv[]);
      virtual int destroy_service();

      int get_p_meta_info(const int64_t app_id, const int64_t uid,
          const std::vector<std::string>& v_name,
          MetaInfo& out_meta_info, int32_t& name_len);
      int create_top_dir(const int64_t app_id, const int64_t uid);

      static int parse_name(const char* file_path, std::vector<std::string>& v_name);
      static int32_t get_depth(const std::vector<std::string>& v_name);
      static int get_name(const std::vector<std::string>& v_name, const int32_t depth, char* buffer, const int32_t buffer_len, int32_t& name_len);


      int get_meta_info(const int64_t app_id, const int64_t uid, const int64_t pid,
          const char* name, const int32_t name_len, const int64_t offset,
          std::vector<MetaInfo>& v_meta_info, int32_t& cluster_id, int64_t& last_offset);

      int read_frag_info(const std::vector<MetaInfo>& v_meta_info,
          const int64_t offset, const int32_t size,
          int32_t& cluster_id, std::vector<FragMeta>& v_out_frag_info, bool& still_have);
      

    private:
      char top_dir_name_[10];
      int32_t top_dir_size_;
      DISALLOW_COPY_AND_ASSIGN(MetaServerService);

      MetaStoreManager* store_manager_;
      tbutil::Mutex mutex_;
    };
  }
}

#endif
