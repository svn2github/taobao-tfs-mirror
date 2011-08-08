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
 *   chuyu <chuyu@taobao.com>
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
      int do_ls(common::BasePacket* packet);

      int create(const int64_t app_id, const int64_t uid,
                 const char* file_path, const common::FileType type);
      int rm(const int64_t app_id, const int64_t uid,
             const char* file_path, const common::FileType type);
      int mv(const int64_t app_id, const int64_t uid,
             const char* file_path, const char* dest_file_path, const common::FileType type);
      int write(const int64_t app_id, const int64_t uid,
                const char* file_path,
                const common::FragInfo& in_v_frag_info);
      int read(const int64_t app_id, const int64_t uid, const char* file_path,
               const int64_t offset, const int64_t size,
               common::FragInfo& frag_info, bool& still_have);

      int ls(const int64_t app_id, const int64_t uid, const int64_t pid,
             const char* file_path, const common::FileType,
             std::vector<common::MetaInfo>& meta_info, bool& still_have);

      static int check_frag_info(const common::FragInfo& frag_info);
      static int int64_to_char(char* buff, const int32_t buff_size, const int64_t v);
      static int char_to_int64(char* data, const int32_t data_size, int64_t& v);
      static void next_file_name(char* name, int32_t& name_len);

    private:
      // override
      virtual int initialize(int argc, char* argv[]);
      virtual int destroy_service();

      int create_top_dir(const int64_t app_id, const int64_t uid);

      static int parse_name(const char* file_path, std::vector<std::string>& v_name);
      static int32_t get_depth(const std::vector<std::string>& v_name);
      static int get_name(const char* name, char* buffer, const int32_t buffer_len, int32_t& name_len);

      int parse_file_path(const int64_t app_id, const int64_t uid, const char* file_path,
                          common::MetaInfo& p_meta_info, char* name, int32_t& name_len);
      int get_p_meta_info(const int64_t app_id, const int64_t uid,
                          const std::vector<std::string>& v_name, common::MetaInfo& out_meta_info);
      int get_dir_meta_info(const int64_t app_id, const int64_t uid, const int64_t pid,
                            const char* name, const int32_t name_len,
                            MetaInfo& out_meta_info);
      int get_meta_info(const int64_t app_id, const int64_t uid, const int64_t pid,
                        const char* name, const int32_t name_len, const int64_t offset, const bool is_file,
                        std::vector<common::MetaInfo>& v_meta_info, int32_t& cluster_id, int64_t& last_offset);

      int read_frag_info(const std::vector<common::MetaInfo>& v_meta_info,
                         const int64_t offset, const int64_t size,
                         int32_t& cluster_id, std::vector<common::FragMeta>& v_out_frag_info, bool& still_have);

      void calculate_file_meta_info(std::vector<common::MetaInfo>::iterator& meta_info_begin,
                                    const std::vector<common::MetaInfo>::iterator meta_info_end, const bool ls_file,
                                    std::vector<common::MetaInfo>& v_meta_info, common::MetaInfo& last_meta_info);

      void add_new_meta_info(const int64_t pid, const int32_t cluster_id,
                              const char* name, const int32_t name_len, const int64_t last_offset,
                              std::vector<common::MetaInfo>& tmp_v_meta_info);

      void add_frag_to_meta(std::vector<common::FragMeta>::iterator& frag_meta_begin,
                            std::vector<common::FragMeta>::iterator frag_meta_end,
                            common::MetaInfo& meta_info, int64_t& last_offset);

      template<class T>
      bool check_not_out_over(const T& v)
      {
        return static_cast<int32_t>(v.size()) < common::MAX_OUT_INFO_COUNT;
      }

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
