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
#ifndef TFS_MESSAGE_SERVERSTATUSMESSAGE_H_
#define TFS_MESSAGE_SERVERSTATUSMESSAGE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <errno.h>

#include "common/func.h"
#include "message.h"



namespace tfs
{
  namespace message
  {
    class GetServerStatusMessage: public Message
    {
      public:
        GetServerStatusMessage();
        virtual ~GetServerStatusMessage();
        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();
        char* get_error();
        int32_t get_status() const;

        static Message* create(const int32_t type);

        inline void set_status_type(const int32_t type)
        {
          status_type_ = type;
        }

        inline int32_t get_status_type() const
        {
          return status_type_;
        }

        inline void set_from_row(const int32_t row)
        {
          from_row_ = row;
        }

        inline int32_t get_from_row() const
        {
          return from_row_;
        }

        inline void set_return_row(const int32_t row)
        {
          return_row_ = row;
        }

        inline int32_t get_return_row() const
        {
          return return_row_;
        }
      protected:
        int32_t status_type_;
        int32_t from_row_;
        int32_t return_row_;
    };

    class ReplicateInfoMessage: public Message
    {
      public:
        typedef __gnu_cxx::hash_map<uint32_t, common::ReplBlock> REPL_BLOCK_MAP;
        typedef std::map<uint64_t, uint32_t> COUNTER_TYPE;

        ReplicateInfoMessage();
        virtual ~ReplicateInfoMessage();
        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();

        static Message* create(const int32_t type);

        inline const REPL_BLOCK_MAP & get_replicating_map() const
        {
          return replicating_map_;
        }
        inline const COUNTER_TYPE & get_source_ds_counter() const
        {
          return source_ds_counter_;
        }
        inline const COUNTER_TYPE & get_dest_ds_counter() const
        {
          return dest_ds_counter_;
        }

        void set_replicating_map(const __gnu_cxx::hash_map<uint32_t, common::ReplBlock*>& src);
        inline void set_source_ds_counter(const COUNTER_TYPE & src)
        {
          source_ds_counter_ = src;
        }
        inline void set_dest_ds_counter(const COUNTER_TYPE & dst)
        {
          dest_ds_counter_ = dst;
        }

      private:
        int set_counter_map(char** data, int32_t* len, const COUNTER_TYPE& map, int32_t size);
        int get_counter_map(char** data, int32_t* len, COUNTER_TYPE& map);

      protected:
        REPL_BLOCK_MAP replicating_map_;
        COUNTER_TYPE source_ds_counter_;
        COUNTER_TYPE dest_ds_counter_;
    };

    class AccessStatInfoMessage: public Message
    {
      public:
        typedef __gnu_cxx ::hash_map<uint32_t, common::Throughput> COUNTER_TYPE;

        AccessStatInfoMessage();
        virtual ~AccessStatInfoMessage();
        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();

        static Message* create(const int32_t type);

        inline void set_from_row(int32_t start)
        {
          if (start < 0)
          {
            start = 0;
          }
          from_row_ = start;
        }
        inline int32_t get_from_row() const
        {
          return from_row_;
        }
        inline void set_return_row(int32_t row)
        {
          if (row < 0)
          {
            row = 0;
          }
          return_row_ = row;
        }
        inline int32_t get_return_row() const
        {
          return return_row_;
        }
        inline int32_t has_next() const
        {
          return has_next_;
        }

        inline const COUNTER_TYPE & get() const
        {
          return stats_;
        }

        inline void set(const COUNTER_TYPE & type)
        {
          stats_ = type;
        }

        inline void set(const uint32_t ip, const common::Throughput& through_put)
        {
          stats_.insert(COUNTER_TYPE::value_type(ip, through_put));
        }

      private:
        int set_counter_map(char** data, int32_t* len, const COUNTER_TYPE & map, int32_t from_row, int32_t return_row,
            int32_t size);
        int get_counter_map(char** data, int32_t* len, COUNTER_TYPE & map);

      protected:
        COUNTER_TYPE stats_;
        int32_t from_row_;
        int32_t return_row_;
        int32_t has_next_;
    };

    class ShowServerInformationMessage : public Message
    {
    public:
      ShowServerInformationMessage();
      virtual ~ShowServerInformationMessage();
      virtual int parse(char* data, int32_t len);
      virtual int build(char* data, int32_t len);
      virtual int32_t message_length();
      virtual char* get_name();

      common::SSMScanParameter& get_param()
      {
        return param;
      }
      static Message* create(const int32_t type);
    private:
      common::SSMScanParameter param;
    };
  }
}
#endif
