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
#ifndef TFS_MESSAGE_READDATAMESSAGE_H_
#define TFS_MESSAGE_READDATAMESSAGE_H_

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
#pragma pack(4)
    struct ReadDataInfo
    {
        uint32_t block_id_;
        uint64_t file_id_;
        int32_t offset_;
        int32_t length_;
    };
#pragma pack()

    class ReadDataMessage: public Message
    {
      public:
        ReadDataMessage();
        virtual ~ReadDataMessage();

        void set_block_id(const uint32_t block_id)
        {
          read_data_info_.block_id_ = block_id;
        }

        uint32_t get_block_id() const
        {
          return read_data_info_.block_id_;
        }

        void set_file_id(const uint64_t file_id)
        {
          read_data_info_.file_id_ = file_id;
        }

        uint64_t get_file_id() const
        {
          return read_data_info_.file_id_;
        }

        void set_offset(const int32_t offset)
        {
          read_data_info_.offset_ = offset;
        }

        int32_t get_offset() const
        {
          return read_data_info_.offset_;
        }

        void set_length(const int32_t length)
        {
          read_data_info_.length_ = length;
        }

        int32_t get_length() const
        {
          return read_data_info_.length_;
        }

        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();

        static Message* create(const int32_t type);
      protected:
        ReadDataInfo read_data_info_;
    };

    class RespReadDataMessage: public Message
    {
      public:
        RespReadDataMessage();
        virtual ~RespReadDataMessage();

        char* alloc_data(const int32_t len);

        void set_length(const int32_t len)
        {
          length_ = len;
        }

        char* get_data() const
        {
          return data_;
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
        char* data_;
        int32_t length_;
        bool alloc_;
    };

    class ReadDataMessageV2: public Message
    {
      public:
        ReadDataMessageV2();
        virtual ~ReadDataMessageV2();

        void set_block_id(const uint32_t block_id)
        {
          read_data_info_.block_id_ = block_id;
        }

        uint32_t get_block_id() const
        {
          return read_data_info_.block_id_;
        }

        void set_file_id(const uint64_t file_id)
        {
          read_data_info_.file_id_ = file_id;
        }

        uint64_t get_file_id() const
        {
          return read_data_info_.file_id_;
        }

        void set_offset(const int32_t offset)
        {
          read_data_info_.offset_ = offset;
        }

        int32_t get_offset() const
        {
          return read_data_info_.offset_;
        }

        void set_length(const int32_t length)
        {
          read_data_info_.length_ = length;
        }

        int32_t get_length() const
        {
          return read_data_info_.length_;
        }

        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();

        static Message* create(const int32_t type);
      protected:
        ReadDataInfo read_data_info_;
    };

    class RespReadDataMessageV2: public Message
    {
      public:
        RespReadDataMessageV2();
        virtual ~RespReadDataMessageV2();

        char* alloc_data(int32_t len);

        char* get_data() const
        {
          return data_;
        }

        void set_length(const int32_t len)
        {
          length_ = len;
        }

        int32_t get_length() const
        {
          return length_;
        }

        void set_file_info(common::FileInfo* const file_info)
        {
          file_info_ = file_info;
        }

        common::FileInfo* get_file_info() const
        {
          return file_info_;
        }

        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();

        static Message* create(const int32_t type);
      protected:
        char* data_;
        int32_t length_;
        bool alloc_;
        common::FileInfo* file_info_;
    };

    class ReadRawDataMessage: public Message
    {
      public:
        ReadRawDataMessage();
        virtual ~ReadRawDataMessage();

        void set_block_id(const uint32_t block_id)
        {
          read_data_info_.block_id_ = block_id;
        }

        uint32_t get_block_id() const
        {
          return read_data_info_.block_id_;
        }

        void set_offset(const int32_t offset)
        {
          read_data_info_.offset_ = offset;
        }

        int32_t get_offset() const
        {
          return read_data_info_.offset_;
        }

        void set_length(const int32_t length)
        {
          read_data_info_.length_ = length;
        }

        int32_t get_length() const
        {
          return read_data_info_.length_;
        }

        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();

        static Message* create(const int32_t type);
      protected:
        ReadDataInfo read_data_info_;
    };

    class RespReadRawDataMessage: public Message
    {
      public:
        RespReadRawDataMessage();
        virtual ~RespReadRawDataMessage();

        void set_length(const int32_t length)
        {
          length_ = length;
        }

        int32_t get_length() const
        {
          return length_;
        }

        void set_data(char* data)
        {
          data_ = data;
        }

        char* get_data() const
        {
          return data_;
        }

        char* alloc_data(int32_t len);

        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();

        static Message* create(const int32_t type);
      protected:
        char* data_;
        int32_t length_;
        bool alloc_;
    };

    class ReadScaleImageMessage: public ReadDataMessageV2
    {
      public:
        enum ZoomStatus
        {
          ZOOM_NONE = 0,
          ZOOM_SPEC = 1,
          ZOOM_PROP = 2
        };
        struct ZoomData
        {
            int32_t zoom_width_;
            int32_t zoom_height_;
            int32_t zoom_type_;
        };

        ReadScaleImageMessage();
        virtual ~ReadScaleImageMessage();
      public:
        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();

        static Message* create(const int32_t type);

        void set_zoom_data(const ZoomData & zoom)
        {
          zoom_ = zoom;
        }

        const ZoomData & get_zoom_data() const
        {
          return zoom_;
        }

      protected:
        ZoomData zoom_;
    };
  }
}
#endif
