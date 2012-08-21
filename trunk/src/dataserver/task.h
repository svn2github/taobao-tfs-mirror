/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: task.h 390 2012-08-06 10:11:49Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing.zyd@taobao.com
 *      - initial release
 *
 */

#ifndef TFS_DATASERVER_TASK_H_
#define TFS_DATASERVER_TASK_H_

#include <Mutex.h>
#include <Monitor.h>
#include "common/internal.h"
#include "common/error_msg.h"
#include "logic_block.h"
#include "blockfile_manager.h"
#include "dataserver_define.h"
#include "common/base_packet.h"

namespace tfs
{
  namespace dataserver
  {
    class TaskManager;

    class Task
    {
      public:
        Task(TaskManager& manager, const int64_t seqno,
          const uint64_t source_id, const int32_t expire_time):
          manager_(manager), seqno_(seqno), source_id_(source_id), expire_time_(expire_time)
        {
          start_time_ = common::Func::get_monotonic_time();
        }
        virtual ~Task() {}

        common::PlanType get_type() const { return type_; }
        void set_type(const common::PlanType type) { type_ = type; }

        int64_t get_seqno() const { return seqno_; }
        void set_seqno(const int64_t seqno) { seqno_ = seqno; }

        uint64_t get_source_id() const { return source_id_; }
        void set_source_id(const uint64_t source_id) { source_id_ = source_id; }

        /**
         * @brief send simple request, reply shoule be StatusMessage
         *
         * @param server_id: target server
         * @param message: message content
         *
         * @return 0 on sucess
         */
        static int send_simple_request(uint64_t server_id, common::BasePacket* message);

        /**
        * @brief write raw data to block
        *
        * @param server_id: ds ip:port
        * @param block_id: target block id
        * @param data: data to write
        * @param length: data length to write
        * @param offset: data offset in block
        *
        * @return 0 on success
        */
        static int write_raw_data(const uint64_t server_id, const uint32_t block_id,
          const char* data, const int32_t length, const int32_t offset);

        /**
        * @brief read raw data from block
        *
        * @param server_id: ds ip:port
        * @param block_id: target block id
        * @param data: data to store read data
        * @param length: data length to read
        * @param offset: data offset in block
        *
        * @return 0 on success
        */
        static int read_raw_data(const uint64_t server_id, const uint32_t block_id,
          char* data, const int32_t length, const int32_t offset, int32_t& data_file_size);

        /**
        * @brief batch write index to server
        *
        * @param server_id: target ds id
        * @param block_id: block id
        *
        * @return 0 on success
        */
        static int batch_write_index(const uint64_t server_id, const uint32_t block_id);

        /**
        * @brief write raw index to ds
        *
        * @param server_id: target ds id
        * @param block_id: target block id
        * @param family_id: family id
        * @param data: raw index data
        * @param length: raw index length
        *
        * @return 0 on success
        */
        static int write_raw_index(const uint64_t server_id, const uint32_t block_id,
            const int64_t family_id, const common::RawIndexOp index_op, const common::RawIndexVec& index_vec);

       /**
        * @brief read raw index from ds
        *
        * @param server_id: target ds id
        * @param block_id: target block id
        * @param data: raw index data
        * @param length: raw index length
        *
        * @return 0 on success
        */
        static int read_raw_index(const uint64_t server_id, const uint32_t block_id,
          const common::RawIndexOp index_op, const uint32_t index_id, char* & data, int32_t& length);

        /**
        * @brief DS post response callback function
        *
        * @param new_client
        *
        * @return 0 on success
        */
        static int ds_task_callback(common::NewClient* new_client)
        {
          UNUSED(new_client);
          return common::TFS_SUCCESS;
        }

        /**
        * @brief is task from ds??
        *
        * @return true if task from ds
        */
        bool task_from_ds() const;

        /**
        * @brief is a task expired?
        *
        * @return true if expired
        */
        bool is_expired()
        {
          bool expire = false;
          int32_t now = common::Func::get_monotonic_time();
          if (now > start_time_ + expire_time_)
          {
            expire = true;
          }
          return expire;
        }

        /**
        * @brief translate error code to PlanStus
        *
        * @param err_code
        *
        * @return plan status
        */
        static common::PlanStatus translate_status(const int err_code)
        {
          common::PlanStatus ret = common::PLAN_STATUS_NONE;
          if (common::TFS_SUCCESS == err_code)
          {
            ret = common::PLAN_STATUS_END;
          }
          else
          {
            ret = common::PLAN_STATUS_FAILURE;
          }
          return ret;
        }

        /**
        * @brief report task status to ds
        *
        * @param status
        *
        * @return 0 on success
        */
        virtual int report_to_ds(int status) const
        {
          UNUSED(status);
          return common::TFS_SUCCESS;
        }

        /**
        * @brief report task status to ns
        *
        * @param status
        *
        * @return 0 on success
        */
        virtual int report_to_ns(const int status) = 0;

        /**
        * @brief check if task completed
        *
        * @return true if completed
        */
        virtual bool is_completed() const { return true; }

        /**
         * @brief handle local task
         *
         * @return TFS_SUCCESS on success
         */
        virtual int handle() = 0;

        /**
         * @brief receive response message
         *
         * @param packet
         *
         * @return
         */
        virtual int handle_complete(common::BasePacket* packet)
        {
          UNUSED(packet);
          return common::TFS_SUCCESS;
        }

        /**
         * @brief dump task information
         */
        virtual std::string dump() const ;

      private:
        DISALLOW_COPY_AND_ASSIGN(Task);

      protected:
        TaskManager& manager_;
        common::PlanType type_;
        int64_t seqno_;
        uint64_t source_id_;
        int32_t start_time_;
        int32_t expire_time_;
    };

    class CompactTask: public Task
    {
      public:
        CompactTask(TaskManager& manager, const int64_t seqno,
          const uint64_t source_id, const int32_t expire_time,
          const uint32_t block_id);
        virtual ~CompactTask();

        void set_servers(const std::vector<uint64_t>& servers)
        {
          servers_ = servers;
          for (uint32_t i = 0; i < servers_.size(); i++)
          {
            result_.push_back(std::make_pair(servers_[i], common::PLAN_STATUS_TIMEOUT));
          }
        }

        virtual bool is_completed() const ;
        virtual int handle();
        virtual int handle_complete(common::BasePacket* packet);
        virtual std::string dump() const ;
        virtual int report_to_ns(const int status);
        virtual int report_to_ds(const int status);

      private:
        DISALLOW_COPY_AND_ASSIGN(CompactTask);

        int do_compact(const uint32_t block_id);
        int real_compact(LogicBlock* src, LogicBlock* dest);
        int write_big_file(LogicBlock* src, LogicBlock* dest,
            const common::FileInfo& src_info,
            const common::FileInfo& dest_info, int32_t woffset);
        int request_ds_to_compact();
        void add_response(const uint64_t server, const int status, const common::BlockInfo& info);

      protected:
        uint32_t block_id_;
        common::BlockInfo info_;
        std::vector<uint64_t> servers_;
        std::vector<std::pair<uint64_t, int8_t> > result_;
    };

    class ReplicateTask: public Task
    {
      public:
        ReplicateTask(TaskManager& manager, const int64_t seqno,
          const uint64_t source_id, const int32_t expire_time,
          const common::ReplBlock& repl_info);
        virtual ~ReplicateTask();

        virtual int handle();
        virtual std::string dump() const;
        virtual int report_to_ns(const int status);
        virtual int report_to_ds(const int status);

      private:
        DISALLOW_COPY_AND_ASSIGN(ReplicateTask);

        int do_replicate(const common::ReplBlock& repl_block);

      protected:
        common::ReplBlock repl_info_;
    };

    class MarshallingTask: public Task
    {
      public:
        MarshallingTask(TaskManager& manager, const int64_t seqno,
          const uint64_t source_id, const int32_t expire_time,
          const int64_t family_id);
        virtual ~MarshallingTask();

        int set_family_member_info(const common::FamilyMemberInfo* members, const int32_t family_aid_info);

        virtual int handle();
        virtual std::string dump() const;
        virtual int report_to_ns(const int status);

      private:
        DISALLOW_COPY_AND_ASSIGN(MarshallingTask);

        int do_marshalling();

      protected:
        common::FamilyMemberInfo* family_members_;
        int64_t family_id_;
        int32_t family_aid_info_;
    };

    class ReinstateTask: public MarshallingTask
    {
      public:
        ReinstateTask(TaskManager& manager, const int64_t seqno,
          const uint64_t source_id, const int32_t expire_time,
          const int64_t family_id);
        virtual ~ReinstateTask();

        virtual int handle();

      private:
        DISALLOW_COPY_AND_ASSIGN(ReinstateTask);
    };

    class DissolveTask: public MarshallingTask
    {
      public:
        DissolveTask(TaskManager& manager, const int64_t seqno,
          const uint64_t source_id, const int32_t expire_time,
          const int64_t family_id);
        virtual ~DissolveTask();

        virtual bool is_completed() const;
        virtual int handle();
        virtual int handle_complete(common::BasePacket* packet);

      private:
        DISALLOW_COPY_AND_ASSIGN(DissolveTask);
    };

  }
}
#endif //TFS_DATASERVER_TASK_H_
