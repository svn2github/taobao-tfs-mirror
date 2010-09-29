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
 *
 */
#ifndef TFS_MESSAGE_WRITEDATAMESSAGE_H_
#define TFS_MESSAGE_WRITEDATAMESSAGE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <errno.h>

#include "common/interval.h"
#include "message.h"

namespace tfs
{
  namespace message
  {
    class WriteDataMessage: public Message
    {
      public:
        WriteDataMessage();
        virtual ~WriteDataMessage();

        inline void set_block_id(const uint32_t block_id)
        {
          write_data_info_.block_id_ = block_id;
        }

        inline uint32_t get_block_id() const
        {
          return write_data_info_.block_id_;
        }

        inline void set_file_id(const uint64_t file_id)
        {
          write_data_info_.file_id_ = file_id;
        }

        inline uint64_t get_file_id() const
        {
          return write_data_info_.file_id_;
        }

        inline void set_offset(const int32_t offset)
        {
          write_data_info_.offset_ = offset;
        }

        inline int32_t get_offset() const
        {
          return write_data_info_.offset_;
        }

        inline void set_length(const int32_t length)
        {
          write_data_info_.length_ = length;
        }

        inline int32_t get_length() const
        {
          return write_data_info_.length_;
        }

        inline void set_data(char* const data)
        {
          data_ = data;
        }

        inline char* get_data() const
        {
          return data_;
        }

        inline void set_ds_list(const common::VUINT64 &ds)
        {
          ds_ = ds;
        }

        inline common::VUINT64 &get_ds_list()
        {
          return ds_;
        }

        inline void set_server(const common::ServerRole is_server)
        {
          write_data_info_.is_server_ = is_server;
        }

        inline common::ServerRole get_server() const
        {
          return write_data_info_.is_server_;
        }

        inline void set_file_number(const uint64_t file_num)
        {
          write_data_info_.file_number_ = file_num;
        }

        inline uint64_t get_file_number() const
        {
          return write_data_info_.file_number_;
        }

        inline void set_block_version(const int32_t version)
        {
          version_ = version;
          has_lease_ = true;
        }

        inline int32_t get_block_version() const
        {
          return version_;
        }

        inline void set_lease_id(const int32_t lease)
        {
          lease_ = lease;
          has_lease_ = true;
        }

        inline int32_t get_lease_id() const
        {
          return lease_;
        }
        inline common::WriteDataInfo get_write_info() const
        {
          return write_data_info_;
        }

        char* alloc_data(int32_t len);

        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();

        static Message* create(const int32_t type);
      protected:
        common::WriteDataInfo write_data_info_;
        char* data_;
        common::VUINT64 ds_;
        int32_t version_;
        int32_t lease_;
        bool has_lease_;
    };

#ifdef _DEL_001_
    class RespWriteDataMessage : public Message
    {
      public:
      RespWriteDataMessage();
      virtual ~RespWriteDataMessage();

      void set_length(const int32_t length)
      {
        length_ = length;
      }

      int32_t get_length() const
      {
        return length_;
      }

      virtual int parse(char* data, int32_t len);
      virtual int build(char* data, int32_t len);
      virtual int32_t message_length();
      virtual char* get_name();

      static Message* create(const int32_t type);
      protected:
      int32_t length_;
    };
#endif

    class WriteRawDataMessage: public Message
    {
      public:
        WriteRawDataMessage();
        virtual ~WriteRawDataMessage();

        inline void set_block_id(const uint32_t block_id)
        {
          write_data_info_.block_id_ = block_id;
        }

        inline uint32_t get_block_id() const
        {
          return write_data_info_.block_id_;
        }

        inline void set_offset(const int32_t offset)
        {
          write_data_info_.offset_ = offset;
        }

        inline int32_t get_offset() const
        {
          return write_data_info_.offset_;
        }

        inline void set_length(const int32_t length)
        {
          write_data_info_.length_ = length;
        }

        inline int32_t get_length() const
        {
          return write_data_info_.length_;
        }

        inline void set_data(const char* data)
        {
          data_ = const_cast<char*> (data);
        }

        inline char* get_data() const
        {
          return data_;
        }

        inline void set_new_block(const int32_t flag)
        {
          flag_ = flag;
        }

        inline int32_t get_new_block() const
        {
          return flag_;
        }

        char* alloc_data(int32_t len);
        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();

        static Message* create(const int32_t type);
      protected:
        common::WriteDataInfo write_data_info_;
        char* data_;
        int32_t flag_;
    };

    class WriteInfoBatchMessage: public Message
    {
      public:
        WriteInfoBatchMessage();
        virtual ~WriteInfoBatchMessage();

        inline void set_block_id(const uint32_t block_id)
        {
          write_data_info_.block_id_ = block_id;
        }

        inline uint32_t get_block_id() const
        {
          return write_data_info_.block_id_;
        }

        inline void set_offset(const int32_t offset)
        {
          write_data_info_.offset_ = offset;
        }

        inline int32_t get_offset() const
        {
          return write_data_info_.offset_;
        }

        inline void set_length(const int32_t length)
        {
          write_data_info_.length_ = length;
        }

        inline int32_t get_length() const
        {
          return write_data_info_.length_;
        }

        inline void set_cluster(const int32_t cluster)
        {
          cluster_ = cluster;
        }

        inline int32_t get_cluster() const
        {
          return cluster_;
        }
        inline void set_raw_meta_list(const common::RawMetaVec* list)
        {
          meta_list_ = (*list);
        }

        inline const common::RawMetaVec* get_raw_meta_list() const
        {
          return &meta_list_;
        }

        inline void set_block_info(common::BlockInfo* const block_info)
        {
          block_info_ = block_info;
        }

        inline common::BlockInfo* get_block_info() const
        {
          return block_info_;
        }

        char* alloc_data(int32_t len);
        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();

        static Message* create(const int32_t type);

      protected:
        common::WriteDataInfo write_data_info_;
        common::BlockInfo* block_info_;
        common::RawMetaVec meta_list_;
        int32_t cluster_;
    };
  }
}
#endif
