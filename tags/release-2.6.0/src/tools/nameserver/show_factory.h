/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: func.cpp 400 2011-06-02 07:26:40Z duanfei@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_TOOLS_SHOWFACTORY_H_
#define TFS_TOOLS_SHOWFACTORY_H_

#include <stdio.h>
#include <vector>
#include <map>
#include <set>
#include "common.h"

namespace tfs
{
  namespace tools
  {
    typedef std::map<uint64_t, std::set<uint64_t> > DS_BLOCKS_MAP;
    typedef std::map<uint64_t, std::set<uint64_t> >::iterator DS_BLOCKS_MAP_ITER;

    int get_all_blocks_from_ds(const common::VUINT64& ds_list, DS_BLOCKS_MAP& all_ds_blocks_map);
    void compute_tp(common::Throughput* tp, const int32_t time);
    void add_tp(const common::Throughput* atp, const common::Throughput* btp, common::Throughput*, const int32_t sign);
    void print_header(const int8_t print_type, const int8_t type, FILE* fp);

    class ServerShow : public ServerBase
    {
      public:
        ServerShow(){}
        virtual ~ServerShow(){}
        int serialize(tbnet::DataBuffer& output, int32_t& length);
        int deserialize(tbnet::DataBuffer& input, int32_t& length);
        int calculate(ServerShow& old_server);
        void dump(const int8_t flag, FILE* fp) const;
      private:
        void dump(const uint64_t server_id, const std::set<uint64_t>& blocks, FILE* fp) const;
    };

    class BlockInfoShow : public BlockBase
    {
      public:
        BlockInfoShow(){}
        virtual ~BlockInfoShow(){}
        void dump(const int8_t flag, FILE* fp) const;
    };

    class BlockCheckShow : public BlockBase
    {
      public:
        BlockCheckShow(){}
        virtual ~BlockCheckShow(){}
        bool check_consistency(DS_BLOCKS_MAP& all_ds_map);
        int check_block_finally(const uint64_t ns_ip,  bool& is_inconsistent);
        void dump(FILE* fp) const;
        std::vector<uint64_t> ns_more_;
        //std::vector<uint64_t> ds_more_;
    };

    class BlockDistributionShow : public BlockBase
    {
      public:
        BlockDistributionShow() : has_same_ip_(false), has_same_ip_rack_(false){}
        virtual ~BlockDistributionShow();
        bool check_block_ip_distribution();
        bool check_block_rack_distribution(const uint32_t ip_mask);
        void dump_rack(FILE* fp) const;

        bool has_same_ip_;
        bool has_same_ip_rack_;
        std::map<uint32_t, std::vector<uint64_t>* > ip_servers_;
        std::map<uint32_t, std::vector<uint64_t>* > ip_rack_servers_;
    };

    class RackBlockShow
    {
      public:
        RackBlockShow() : total_block_replicate_count_(0){}
        virtual ~RackBlockShow();
        void add(BlockInfoShow& block, const uint32_t ip_mask);
        void dump(const int8_t flag, const uint32_t ip_group,  FILE* fp) const;
      private:
        std::map<uint32_t, std::vector<uint64_t> *> rack_blocks_;
        int64_t total_block_replicate_count_;
    };

    class MachineShow
    {
      public:
        MachineShow();
        ~MachineShow(){}
        void dump(const int8_t flag, FILE* fp) const;
        int init(ServerShow& server, ServerShow& old_server);
        int add(ServerShow& server, ServerShow& old_server);
        int calculate();

      public:
        uint64_t machine_id_;
        int64_t use_capacity_;
        int64_t total_capacity_;
        common::Throughput total_tp_;
        common::Throughput last_tp_;
        common::Throughput max_tp_;
        int32_t current_load_;
        int32_t block_count_;
        time_t last_startup_time_;
        time_t consume_time_;
        int32_t index_;

    };

    class FamilyShow
    {
      public:
        FamilyShow();
        virtual ~FamilyShow();
        bool operator < (const FamilyShow& b) const
        {
          return family_id_ < b.family_id_;
        }

        int32_t deserialize(tbnet::DataBuffer& input, const int32_t length, int32_t& offset);
        int get_members_ds_list(const uint64_t ns_ip);
        void dump(const int8_t type, FILE* fp) const;
        int64_t family_id_;
        int32_t family_aid_info_;
        std::pair<uint64_t, uint64_t> members_[common::MAX_MARSHALLING_NUM];//blockid, ds_id
    };

    struct StatStruct
    {
      public:
        int32_t server_count_;
        int32_t machine_count_;
        int64_t use_capacity_;
        int64_t total_capacity_;
        common::Throughput total_tp_;
        common::Throughput last_tp_;
        int32_t current_load_;
        int32_t block_count_;
        time_t last_update_time_;
        int64_t file_count_;
        int64_t block_size_;
        int64_t delfile_count_;
        int64_t block_del_size_;
        StatStruct();
        ~StatStruct();

        int add(ServerShow& server);
        int add(MachineShow& machine);
        int add(BlockInfoShow& block);
        void dump(const int8_t flag, const int8_t sub_flag, FILE* fp) const;
    };

    struct BlockDistributionStruct
    {
      public:
        BlockDistributionStruct();
        ~BlockDistributionStruct();
        int32_t total_block_count_;
        int32_t ip_same_block_count_;
        int32_t ip_rack_same_block_count_;
        void add(BlockDistributionShow& block);
        void dump(const int8_t flag,  FILE* fp) const;
    };
  }
}
#endif
