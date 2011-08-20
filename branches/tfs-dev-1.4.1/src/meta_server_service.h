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

#ifndef TFS_NAMEMETASERVER_NAMEMETASERVERSERVICE_H_
#define TFS_NAMEMETASERVER_NAMEMETASERVERSERVICE_H_

#include "common/base_service.h"
#include "message/message_factory.h"
#include "meta_store_manager.h"

#define MAX_FILE_PATH_LEN 512
namespace tfs
{
  namespace namemetaserver
  {
    const int32_t INSERT_VERSION_ERROR = -1045;
    const int32_t UPDATE_FRAG_INFO_ERROR = -1046;
    const int32_t SOFT_MAX_FRAG_INFO_COUNT = 1024;
    const int32_t HARD_MAX_FRAG_INFO_COUNT = 65535;

    struct FragMetaComp
    {
      bool operator() (FragMeta& meta, int64_t offset)
        {
          return meta.offset_ < offset;
        }
  
      bool operator() (int64_t offset, FragMeta& meta)
        {
          return offset > meta.offset_;
        }
    };

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
      const char* get_log_file_path();
      const char* get_pid_file_path();
      virtual bool handlePacketQueue(tbnet::Packet* packet, void* args);

      int create(const int64_t app_id, const int64_t uid,
                 const char* file_path, const FileType type);
      int rm(const int64_t app_id, const int64_t uid,
             const char* file_path, const FileType type);
      int mv(const int64_t app_id, const int64_t uid,
             const char* file_path, const char* dest_file_path, const FileType type);
      int write(const int64_t app_id, const int64_t uid,
                const char* file_path, const int32_t cluster_id,
                std::vector<FragMeta>& v_frag_info);
      int read(const int64_t app_id, const int64_t uid, const char* file_path,
               const int64_t offset, const int64_t size,
               int32_t& cluster_id, std::vector<FragMeta>& v_frag_info);

    private:
      // override
      virtual int initialize(int argc, char* argv[]);
      virtual int destroy_service();

      int get_p_meta_info(const int64_t app_id, const int64_t uid, const std::vector<std::string>& v_name,
                          MetaInfo& out_meta_info, int32_t& name_len);
      int create_top_dir(const int64_t app_id, const int64_t uid);

      static int parse_name(const char* file_path, std::vector<std::string>& v_name);
      static int32_t get_depth(const std::vector<std::string>& v_name);
      static int get_name(const std::vector<std::string>& v_name, const int32_t depth, char* buffer, const int32_t buffer_len, int32_t& name_len);

      int read_frag_info(const std::vector<MetaInfo>& v_meta_info, const int64_t offset,
                         const int64_t& size, int32_t& cluster_id,
                         std::vector<FragMeta>& v_out_fraginfo);
      int write_frag_info(int32_t cluster_id, std::vector<MetaInfo>& v_meta_info,
                          const std::vector<FragMeta>& v_frag_info);
      int insert_frag_info(std::vector<MetaInfo>& v_meta_info, const std::vector<FragMeta>& src_v_frag_info,
                           int32_t& end_index);
      int merge_frag_info(const MetaInfo& meta_info, const std::vector<FragMeta>& src_v_frag_info,
                          int32_t& end_index);
      int fast_merge_frag_info(std::vector<FragMeta>& dest_v_frag_info,
                               const std::vector<FragMeta>& dest_v_frag_info,
                               std::vector<FragMeta>::const_iterator& src_it,
                               int32_t& end_index);
      int update_write_frag_info(std::vector<MetaInfo>& v_meta_info, const int64_t app_id, const int64_t uid,
                                 MetaInfo& p_meta_info, const char* panme, const int32_t pname_len,
                                 char* name, const int32_t name_len);
      std::vector<FragMeta>::const_iterator
      lower_find(const std::vector<FragMeta>& v_frag_info, const int64_t offset);
      std::vector<FragMeta>::const_iterator
      upper_find(const std::vector<FragMeta>& v_frag_info, const int64_t offset);

    private:
      DISALLOW_COPY_AND_ASSIGN(MetaServerService);

      MetaStoreManager* store_manager_;
      tbutil::Mutex mutex_;
    };
  }
}

#endif
