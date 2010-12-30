/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com> 
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_NAMESERVER_COMPACT_H_
#define TFS_NAMESERVER_COMPACT_H_

#include "meta_manager.h"
#include "proactor_data_pipe.h"
#include "data_container.h"
#include "ns_define.h"
#include "scanner_manager.h"

namespace tfs
{
  namespace nameserver
  {

    struct DsCompacting
    {
      time_t start_time_;
      int64_t old_size_;
      uint32_t block_id_;
      DsCompacting(uint32_t block_id, time_t start_time) :
        start_time_(start_time), old_size_(0), block_id_(block_id)
      {

      }
    };

    struct CheckBlockCompactParam
    {
      uint64_t id_;
      uint32_t block_id_;
      int32_t status_;
      bool all_success_;
      bool has_success_;
      bool is_complete_;
      bool is_first_success_;
      CheckBlockCompactParam(uint64_t dsid, uint32_t block_id, uint32_t st) :
        id_(dsid), block_id_(block_id), status_(st), all_success_(true), has_success_(false), is_complete_(true),
            is_first_success_(true)
      {

      }
    };

    class CompactLauncher: public ProactorDataPipe<PipeDataAdaptor<uint32_t> , CompactLauncher> , public Launcher
    {
    public:
      typedef ProactorDataPipe<PipeDataAdaptor<uint32_t> , CompactLauncher> base_type;
      typedef __gnu_cxx::hash_map<uint64_t, DsCompacting*, __gnu_cxx::hash<uint64_t> > COMPACTING_SERVER_MAP;
      typedef __gnu_cxx::hash_map<uint32_t, std::vector<std::pair<uint64_t, int32_t> > > COMPACTING_BLOCK_MAP;

      enum CompactStatus
      {
        COMPACT_STATUS_NOT_EXIST = -1,
        COMPACT_STATUS_OK = 0x00,
        COMPACT_STATUS_EXPIRED = 0x01
      };

      CompactLauncher(MetaManager& m);
      virtual ~CompactLauncher();

    public:
      bool check(const BlockCollect* block_collect);
      int check_time_out();
      int build_plan(const common::VUINT32& compactBlocks);
      int execute(std::vector<uint32_t>& compactBlocks, void* args);
      int handle_complete_msg(message::CompactBlockCompleteMessage* message);
      int send_compact_cmd(const std::vector<uint64_t>& servers, uint32_t block_id);
      bool is_compacting_time();
      bool is_compacting_block(const uint32_t block_id);
      void clean();

    private:
      DISALLOW_COPY_AND_ASSIGN( CompactLauncher);
      int check_compact_ds(const std::vector<uint64_t>& servers);
      int send_compact_cmd(const uint64_t server_id, const uint32_t block_id, int owner);
      int register_compact_block(const uint64_t server_id, const uint32_t block_id);
      int check_compact_status(const uint64_t server_id, const uint32_t block_id, const bool complete,
          const bool timeout);
      int check_compact_complete(CheckBlockCompactParam& cbcp, common::VUINT64& dsList,
          const common::BlockInfo* block_info = NULL);
      int do_compact_complete(CheckBlockCompactParam& cbcp, common::VUINT64& dsList,
          const common::BlockInfo* block_info = NULL);

    private:
      MetaManager& meta_mgr_;
      COMPACTING_SERVER_MAP compacting_ds_map_;
      COMPACTING_BLOCK_MAP compacting_block_map_;
      common::RWLock mutex_;
    };
  }
}

#endif //__COMPACT_H__
