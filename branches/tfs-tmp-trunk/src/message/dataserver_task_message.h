/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_MESSAGE_DATASERVERTASK_H_
#define TFS_MESSAGE_DATASERVERTASK_H_
#include "base_task_message.h"
#include "common/internal.h"
namespace tfs
{
  namespace message
  {
    class DsCompactBlockMessage: public BaseTaskMessage
    {
      public:
        DsCompactBlockMessage();
        virtual ~DsCompactBlockMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_block_id(const uint32_t block_id) { block_id_ = block_id;}
        inline uint32_t get_block_id() const { return block_id_;}
        inline void set_source_id(const uint64_t source_id) {source_id_ = source_id;}
        inline uint64_t get_source_id() const {return source_id_;}
      private:
        uint32_t block_id_;
        uint64_t source_id_;
    };

    class DsReplicateBlockMessage: public BaseTaskMessage
    {
      public:
        DsReplicateBlockMessage();
        virtual ~DsReplicateBlockMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline void set_source_id(const uint64_t source_id) { source_id_ = source_id; }
        inline uint64_t get_source_id() { return source_id_; }
        inline void set_repl_info(const common::ReplBlock& repl_info) {repl_info_ = repl_info; }
        inline const common::ReplBlock* get_repl_block() {return &repl_info_;}
      private:
        uint64_t source_id_;
        common::ReplBlock repl_info_;
    };

    class RespDsReplicateBlockMessage: public BaseTaskMessage
    {
      public:
        RespDsReplicateBlockMessage();
        virtual ~RespDsReplicateBlockMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        int deserialize(const char* data, const int64_t data_len, int64_t& pos);
        virtual int64_t length() const;
        inline int32_t get_status() const { return status_;}
        inline void set_status(const int32_t status) { status_ = status;}
        inline uint64_t get_ds_id() const { return ds_id_; }
        inline void set_ds_id(const uint64_t ds_id) { ds_id_ = ds_id; }
      private:
        int32_t status_;
        uint64_t ds_id_;
    };

    class RespDsCompactBlockMessage: public BaseTaskMessage
    {
      public:
        RespDsCompactBlockMessage();
        virtual ~RespDsCompactBlockMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        int deserialize(const char* data, const int64_t data_len, int64_t& pos);
        virtual int64_t length() const;
        inline int32_t get_status() const { return status_;}
        inline void set_status(const int32_t status) { status_ = status;}
        inline uint64_t get_ds_id() const { return ds_id_; }
        inline void set_ds_id(const uint64_t ds_id) { ds_id_ = ds_id; }
        inline void set_block_info(const common::BlockInfo& info) { info_ = info; }
        inline common::BlockInfo* get_block_info() { return &info_; }
      private:
        int32_t status_;
        uint64_t ds_id_;
        common::BlockInfo info_;
    };

  }
}
#endif
