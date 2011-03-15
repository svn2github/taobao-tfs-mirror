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
#ifndef TFS_MESSAGE_UNLINKFILEMESSAGE_H_
#define TFS_MESSAGE_UNLINKFILEMESSAGE_H_

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
    struct UnlinkFileInfo
    {
      uint32_t block_id_;
      uint64_t file_id_;
      int32_t is_server_;
    };
#pragma pack()

    class UnlinkFileMessage: public Message
    {
      public:
        UnlinkFileMessage();
        virtual ~UnlinkFileMessage();

        inline void set_block_id(const uint32_t block_id)
        {
          unlink_file_info_.block_id_ = block_id;
        }

        inline uint32_t get_block_id() const
        {
          return unlink_file_info_.block_id_;
        }

        inline void set_file_id(const uint64_t file_id)
        {
          unlink_file_info_.file_id_ = file_id;
        }

        inline uint64_t get_file_id() const
        {
          return unlink_file_info_.file_id_;
        }

        inline void set_ds_list(const common::VUINT64 &ds)
        {
          dataservers_ = ds;
        }

        inline const common::VUINT64 &get_ds_list() const
        {
          return dataservers_;
        }

        inline void set_server()
        {
          unlink_file_info_.is_server_ |= 0x1;
        }

        inline int32_t get_server() const
        {
          return unlink_file_info_.is_server_;
        }

        inline void set_unlink_type(const int action)
        {
          unlink_file_info_.is_server_ |= action;
        }

        inline int get_unlink_type() const
        {
          return unlink_file_info_.is_server_ & REVEAL;
        }

        inline void set_option_flag(const int32_t flag)
        {
          option_flag_ = flag;
        }

        inline int32_t get_option_flag() const
        {
          return option_flag_;
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

        inline bool is_server() const
        {
          return (unlink_file_info_.is_server_ & 0x1) != 0;
        }

        inline bool get_has_lease() const
        {
          return has_lease_;
        }

        inline void set_del()
        {
          unlink_file_info_.is_server_ |= DELETE;
        }

        inline void set_undel()
        {
          unlink_file_info_.is_server_ |= UNDELETE;
        }

        inline void set_conceal()
        {
          unlink_file_info_.is_server_ |= CONCEAL;
        }

        inline void set_reveal()
        {
          unlink_file_info_.is_server_ |= REVEAL;
        }

        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();

        static Message* create(const int32_t type);
      protected:
        UnlinkFileInfo unlink_file_info_;
        int32_t option_flag_;
        common::VUINT64 dataservers_;
        int32_t version_;
        int32_t lease_;
        bool has_lease_;
    };

  }

}
#endif
