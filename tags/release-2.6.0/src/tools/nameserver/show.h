/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: show.h 432 2011-06-08 07:06:11Z nayan@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_TOOLS_SHOW_H_
#define TFS_TOOLS_SHOW_H_

#include <stdio.h>
#include <vector>
#include "show_factory.h"
#include "common/directory_op.h"
#include "common/internal.h"
#include "requester/ns_requester.h"

namespace tfs
{
  namespace tools
  {
    class BaseBlockWorker
    {
      public:
        BaseBlockWorker(int8_t block_type, int8_t sub_type) : block_type_(block_type), sub_type_(sub_type)
        {}
        virtual ~BaseBlockWorker()
        {}

        virtual int begin() { return common::TFS_SUCCESS; }
        virtual int end(FILE* fp)
        {
          UNUSED(fp);
          return common::TFS_SUCCESS;
        }

        virtual BlockBase* create_block() = 0;
        virtual int process(BlockBase* block, FILE* fp) = 0;
      public:
        int8_t block_type_;
        int8_t sub_type_;
    };

    class BlockInfoWorker : public BaseBlockWorker
    {
      public:
        BlockInfoWorker(int8_t sub_type) : BaseBlockWorker(BLOCK_TYPE, sub_type) {}
        ~BlockInfoWorker() {}
        int begin()
        {
          memset(&stat_, 0, sizeof(stat_));
          return common::TFS_SUCCESS;
        }
        BlockBase* create_block()
        {
          return new BlockInfoShow();
        }
        int process(BlockBase* b, FILE* fp)
        {
          BlockInfoShow* block = dynamic_cast<BlockInfoShow*>(b);
          stat_.add(*block);
          block->dump(sub_type_, fp);
          return common::TFS_SUCCESS;
        }
        int end(FILE* fp)
        {
          stat_.dump(BLOCK_TYPE, sub_type_, fp);
          return common::TFS_SUCCESS;
        }
      private:
        StatStruct stat_;
    };

    class BlockDistWorker : public BaseBlockWorker
    {
      public:
        BlockDistWorker(uint32_t ip_mask, int8_t sub_type) : BaseBlockWorker(BLOCK_DISTRIBUTION_TYPE, sub_type),
            ip_mask_(ip_mask) {}
        ~BlockDistWorker() {}
        int begin()
        {
          memset(&stat_, 0, sizeof(stat_));
          return common::TFS_SUCCESS;
        }
        BlockBase* create_block()
        {
          return new BlockDistributionShow();
        }
        int process(BlockBase* b, FILE* fp)
        {
          BlockDistributionShow* block = dynamic_cast<BlockDistributionShow*>(b);
          if(block->check_block_rack_distribution(ip_mask_))
          {
            stat_.add(*block);
            block->dump_rack(fp);
          }
          return common::TFS_SUCCESS;
        }
        int end(FILE* fp)
        {
          stat_.dump(sub_type_, fp);
          return common::TFS_SUCCESS;
        }
      private:
        BlockDistributionStruct stat_;
        uint32_t ip_mask_;
    };

    class BlockRackWorker : public BaseBlockWorker
    {
      public:
        BlockRackWorker(uint32_t ip_mask, uint32_t ip_group, int8_t sub_type)
            : BaseBlockWorker(RACK_BLOCK_TYPE, sub_type),
            ip_mask_(ip_mask), ip_group_(ip_group)
        {}
        ~BlockRackWorker() {}
        int begin()
        {
          return common::TFS_SUCCESS;
        }
        BlockBase* create_block()
        {
          return new BlockInfoShow();
        }
        int process(BlockBase* b, FILE* fp)
        {
          UNUSED(fp);
          BlockInfoShow* block = dynamic_cast<BlockInfoShow*>(b);
          rack_block_.add(*block, ip_mask_);
          return common::TFS_SUCCESS;
        }
        int end(FILE* fp)
        {
          //把所有的block都拉取下来才输出
          rack_block_.dump(sub_type_, ip_group_, fp);
          return common::TFS_SUCCESS;
        }
      private:
        RackBlockShow rack_block_;
        uint32_t ip_mask_;
        uint32_t ip_group_;
    };

    class BlockCheckWorker : public BaseBlockWorker
    {
      public:
        BlockCheckWorker(uint64_t ns_ip, int8_t sub_type)
            : BaseBlockWorker(CHECK_BLOCK_TYPE, sub_type),
            total_count_(0), inconsistent_count_(0), ns_ip_(ns_ip)
        {}
        ~BlockCheckWorker() {}
        int begin()
        {
          common::VUINT64 ds_list;
          int ret = requester::NsRequester::get_ds_list(ns_ip_, ds_list);
          if (common::TFS_SUCCESS == ret)
          {
            ret = get_all_blocks_from_ds(ds_list, all_ds_blocks_map_);
          }
          if (common::TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "get all ds blocks list error, ret: %d", ret);
          }
          return ret;
        }
        BlockBase* create_block()
        {
          return new BlockCheckShow();
        }
        int process(BlockBase* b, FILE* fp)
        {
          int ret = common::TFS_SUCCESS;
          ++total_count_;
          BlockCheckShow* block = dynamic_cast<BlockCheckShow*>(b);
          // get suspicious block firstly by batch message
          if (!block->check_consistency(all_ds_blocks_map_))
          {
            bool result = true;
            // check real result between ns and ds
            ret = block->check_block_finally(ns_ip_, result);
            if (common::TFS_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "check block: %"PRI64_PREFIX"u finally error, ret: %d", block->info_.block_id_, ret);
            }
            else if (!result)
            {
              ++inconsistent_count_;
              block->dump(fp);
            }
          }
          return ret;
        }

        int end(FILE* fp)
        {
          fprintf(fp, "\nInconsistent Block Count %"PRI64_PREFIX"d, Total Block Count %"PRI64_PREFIX"d\n",
              inconsistent_count_, total_count_);
          return common::TFS_SUCCESS;
        }
      private:
        DS_BLOCKS_MAP all_ds_blocks_map_;
        uint64_t total_count_;
        uint64_t inconsistent_count_;
        uint64_t ns_ip_;
    };

    class ShowInfo
    {
      public:
        ShowInfo(){}
        ~ShowInfo(){}

        int set_ns_ip(const std::string& ns_ip_port);
        void clean_last_file();
        int show_block_common(const int32_t num, const uint64_t block_id, int32_t count, const int32_t interval,
            const std::string& filename, BaseBlockWorker* worker);
        int show_server(const int8_t type, const int32_t num, const std::string& server_ip_port,
            int32_t count, const int32_t interval, const std::string& filename);
        int show_machine(const int8_t type, const int32_t num,
            int32_t count, const int32_t interval, const std::string& filename);
        int show_block(const int8_t type, const int32_t num, const uint64_t block_id, int32_t count, const int32_t interval, const std::string& filename);
        int check_block(const int8_t type, const int32_t num, const std::string& filename);
        int show_block_distribution(const int8_t type, std::string& rack_ip_mask, const int32_t num, const uint64_t block_id, int32_t count, const int32_t interval, const std::string& filename);
        int show_rack_block(const int8_t type, std::string& rack_ip_mask, std::string& rack_ip_group, const int32_t num, int32_t count, const int32_t interval,
          const std::string& filename);
        int show_family(const int8_t type, const int32_t num, const int64_t family_id, int32_t count, const int32_t interval, const std::string& filename);
        bool is_loop_;
        bool interrupt_;

      private:
        void load_last_ds();
        void save_last_ds();
        int get_file_handle(const std::string& filename, FILE** fp);
        void put_file_handle(FILE* fp);
        uint64_t get_machine_id(const uint64_t server_id);
        std::map<uint64_t, ServerShow> last_server_map_;
        std::map<uint64_t, ServerShow> server_map_;
        std::map<uint64_t, MachineShow> machine_map_;
        uint64_t ns_ip_;
    };
  }
}

#endif

