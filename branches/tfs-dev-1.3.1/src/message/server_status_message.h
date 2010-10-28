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

#include "common/interval.h"
#include "common/func.h"
#include "nameserver/layout_manager.h"
#include "message.h"



namespace tfs
{
  namespace message
  {
    static const int32_t MAX_WBLOCK = 100;         

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

    class SetServerStatusMessage: public Message
    {
      public:
        SetServerStatusMessage();
        virtual ~SetServerStatusMessage();
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
        inline int32_t has_next() const
        {
          return has_next_;
        }
        inline void set_need_free(const int32_t need_free)
        {
          need_free_ = need_free;
        }

        inline const common::BLOCK_MAP* get_block_map() const
        {
          return &block_map_;
        }
        inline const common::SERVER_MAP* get_server_map() const
        {
          return &server_map_;
        }
        inline const common::VUINT32* get_wblock_list() const
        {
          return &wblock_list_;
        }

        inline void set_block_server_map(const int32_t type, const tfs::nameserver::LayoutManager* ptr)
        {
          if (type & common::SSM_BLOCK)
          {
            status_type_ |= common::SSM_BLOCK;
            block_server_map_ptr_ = ptr;
          }
        }

        inline void set_server_map(const int32_t type, const common::SERVER_MAP* map)
        {
          if (type & common::SSM_SERVER)
          {
            status_type_ = status_type_ | common::SSM_SERVER;
            server_map_ = (*map);
          }
        }

        inline void set_wblock_list(const int32_t type, const common::VUINT32* list)
        {
          if (type & common::SSM_WBLIST)
          {
            status_type_ = status_type_ | common::SSM_WBLIST;
            wblock_list_ = (*list);
          }
        }

      private:
        int set_int_map(char** data, int32_t* len, std::set<uint32_t>* map, int32_t size);
        int get_int_map(char** data, int32_t* len, std::set<uint32_t>* map);

      protected:
        int32_t status_type_;
        int32_t from_row_;
        int32_t return_row_;
        int32_t has_next_;
        int32_t need_free_;
        const tfs::nameserver::LayoutManager* block_server_map_ptr_;
        common::BLOCK_MAP block_map_;
        common::SERVER_MAP server_map_;
        common::VUINT32 wblock_list_;
    };

    class ReplicateInfoMessage: public Message
    {
      public:
        typedef hash_map<uint32_t, common::ReplBlock> REPL_BLOCK_MAP;
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

        void set_replicating_map(const hash_map<uint32_t, common::ReplBlock*>& src);
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

    class GetBlockListMessage: public Message
    {
      public:
        GetBlockListMessage();
        virtual ~GetBlockListMessage();
        virtual int parse(char* data, int32_t len);
        virtual int build(char* data, int32_t len);
        virtual int32_t message_length();
        virtual char* get_name();
        char* get_error();
        int32_t get_status();

        static Message* create(const int32_t type);

        inline common::BLOCK_MAP* get_block_map()
        {
          return &block_map_;
        }

        inline void set_block_server_map(const tfs::nameserver::LayoutManager* ptr)
        {
          block_server_map_ptr_ = ptr;
        }

        inline void set_request_flag(const int32_t flag)
        {
          request_flag_ = flag;
        }
				inline int32_t get_request_flag() const
				{
					return request_flag_;
				}
        inline void set_write_flag(const int32_t write_flag)
        {
          write_flag_ = write_flag;
        }
				inline int32_t get_write_flag() const
				{
					return write_flag_;
				}
        inline void set_start_block_id(const int32_t start_block_id)
        {
          start_block_id_ = start_block_id;
        }
				inline uint32_t get_start_block_id() const
				{
					return start_block_id_;
				}
        inline void set_start_inclusive(const int32_t start_inclusive)
        {
          start_inclusive_ = start_inclusive;
        }
				inline int32_t get_start_inclusive() const
				{
					return start_inclusive_;
				}
        inline void set_read_count(const int32_t read_count)
        {
          read_count_ = read_count;
        }
				inline int32_t get_read_count() const
				{
					return read_count_;
				}
        inline void set_start_block_chunk(const int32_t start_block_chunk)
        {
          start_block_chunk_ = start_block_chunk;
        }
				inline int32_t get_start_block_chunk() const
				{
					return start_block_chunk_;
				}
        inline int32_t get_next_block_chunk() const
        {
          return next_block_chunk_;
        }
				inline void set_next_block_chunk(const int32_t next_block_chunk)
				{
					next_block_chunk_ = next_block_chunk;
				}
        inline int32_t get_return_count() const
        {
          return return_count_;
        }
				inline void set_end_block_id(const uint32_t end_block_id)
				{
					end_block_id_ = end_block_id;
				}
				inline void set_return_count( const int32_t return_count)
				{
					return_count_ = return_count;
				}
				inline uint32_t get_end_block_id() const
				{
					return end_block_id_;
				}

      public:
        int32_t request_flag_;
        int32_t write_flag_;
        uint32_t start_block_id_;
        int32_t start_inclusive_;
        uint32_t end_block_id_;
        int32_t read_count_;
        int32_t return_count_;
        int32_t start_block_chunk_;
        int32_t next_block_chunk_;

        common::BLOCK_MAP block_map_;
        const tfs::nameserver::LayoutManager* block_server_map_ptr_;
    };
  }
}
#endif
