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
 *   daoan <daoan@taobao.com>
 *      - initial release
 *   duanfei <duanfei@taobao.com>
 *      -modify interface-2012.03.15
 *
 */
#ifndef TFS_NAMESERVER_CLIENT_REQUEST_SERVER_H_
#define TFS_NAMESERVER_CLIENT_REQUEST_SERVER_H_
#include <tbnet.h>
#include "common/lock.h"
#include "block_collect.h"
#include "server_collect.h"
#include "common/base_packet.h"
#include "oplog_sync_manager.h"
namespace tfs
{
  namespace nameserver
  {
    struct CloseParameter
    {
      common::BlockInfoV2 block_info_;
      uint64_t id_;
      uint64_t lease_id_;
      int32_t  type_;
      common::WriteCompleteStatus status_;
      bool need_new_;
      char error_msg_[256];
    };
    class LayoutManager;
    class NameServer;
    class ClientRequestServer
    {
      public:
        explicit ClientRequestServer(LayoutManager& manager);
        virtual ~ClientRequestServer(){}
        int apply(common::DataServerStatInfo& info, int32_t& expire_time, int32_t& next_renew_time, int32_t& renew_retry_times, int32_t& renew_retry_timeout);
        int renew(const common::ArrayHelper<common::BlockInfoV2>& input,
              common::DataServerStatInfo& info, common::ArrayHelper<common::BlockLease>& output,
              int32_t& expire_time, int32_t& next_renew_time, int32_t& renew_retry_times, int32_t& renew_rety_timeout);
        int giveup(const common::ArrayHelper<common::BlockInfoV2>& input,common::DataServerStatInfo& info);
        int apply_block(const uint64_t server, common::ArrayHelper<common::BlockLease>& output);
        int apply_block_for_update(const uint64_t server, common::ArrayHelper<common::BlockLease>& output);
        int giveup_block(const uint64_t server, const common::ArrayHelper<common::BlockInfoV2>& input,common::ArrayHelper<common::BlockLease>& output);
        int report_block(std::vector<uint64_t>& expires, const uint64_t server, const time_t now,
            const common::ArrayHelper<common::BlockInfoV2>& blocks);

        int open(const uint64_t block_id, const int32_t mode, const int32_t flag, const time_t now,uint64_t& lease_id,
            common::ArrayHelper<uint64_t>& servers, common::FamilyInfoExt& family_info);
        int open(const int64_t family_id, const int32_t mode, int32_t& family_aid_info, common::ArrayHelper<std::pair<uint64_t, uint64_t> >& members) const;
        int batch_open(const int32_t mode, const int32_t flag, common::ArrayHelper<common::BlockMeta>& out);

        void dump_plan(tbnet::DataBuffer& output);
        void client_keepalive(const int32_t flag, tbnet::DataBuffer& output, common::ClusterConfig& replica_num, int32_t& interval);

        int handle_control_cmd(const common::ClientCmdInformation& info, common::BasePacket* msg, const int64_t buf_length, char* buf);

        int handle(common::BasePacket* msg);

        int resolve_block_version_conflict(const uint64_t block, const common::ArrayHelper<std::pair<uint64_t, common::BlockInfoV2> >& info);

      private:
        int handle_control_load_block(const time_t now, const common::ClientCmdInformation& info, common::BasePacket* message, const int64_t buf_length, char* error_buf);
        int handle_control_delete_block(const time_t now, const common::ClientCmdInformation& info,const int64_t buf_length, char* error_buf);
        int handle_control_compact_block(const time_t now, const common::ClientCmdInformation& info, const int64_t buf_length, char* error_buf);
        int handle_control_immediately_replicate_block(const time_t now, const common::ClientCmdInformation& info, const int64_t buf_length, char* error_buf);
        int handle_control_rotate_log(void);
        int handle_control_set_runtime_param(const common::ClientCmdInformation& info, const int64_t buf_length, char* error_buf);
        int handle_control_get_balance_percent(const int64_t buf_length, char* error_buf);
        int handle_control_set_balance_percent(const common::ClientCmdInformation& info, const int64_t buf_length, char* error_buf);
        int handle_control_clear_system_table(const common::ClientCmdInformation& info, const int64_t buf_length, char* error_buf);
        int handle_control_delete_family(const common::ClientCmdInformation& info, const int64_t buf_length, char* buf);
        int handle_control_set_all_server_report_block(const common::ClientCmdInformation& info, const int64_t buf_length, char* buf);

        bool is_discard(void);

        void calc_lease_expire_time_(int32_t& expire_time, int32_t& next_renew_time, int32_t& renew_retry_times, int32_t& renew_retry_timeout) const;
      private:
        volatile uint32_t ref_count_;
        LayoutManager& manager_;
    };
  }/** end namespace nameserver **/
}/** end namespace tfs **/
#endif

