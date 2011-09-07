/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: rootserver.h 590 2011-08-18 13:36:13Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#include <ext/hash_map>
#include <gtest/gtest.h>

#include "common/buffer.h"
#include "common/mmap_file.h"
#include "common/new_client.h"
#include "common/rts_define.h"

namespace tfs
{
  namespace rootserver
  {
    extern int rs_async_callback(common::NewClient* client);
    class BuildTable
    {
      friend class BuildTableTest;
      FRIEND_TEST(BuildTableTest, initialize);
      FRIEND_TEST(BuildTableTest, build_table);
      FRIEND_TEST(BuildTableTest, update_table);
      FRIEND_TEST(BuildTableTest, check_update_table_complete);
      FRIEND_TEST(BuildTableTest, switch_table);
      FRIEND_TEST(BuildTableTest, load_tables);
      FRIEND_TEST(BuildTableTest, get_old_servers);
      FRIEND_TEST(BuildTableTest, get_difference);
      FRIEND_TEST(BuildTableTest, fill_old_tables);
      FRIEND_TEST(BuildTableTest, fill_new_tables);
      struct TablesHeader
      {
        int64_t magic_number_;
        int64_t active_table_version_;
        int64_t server_item_;
        int64_t build_table_version_;
        int64_t bucket_item_;
        int64_t reserve_;
        int serialize(char* data, const int64_t data_len, int64_t& pos) const;
        int deserialize(const char* data, const int64_t data_len, int64_t& pos);
        int64_t length() const;
      };
    public:
      BuildTable();
      virtual ~BuildTable();
      int intialize(const std::string& file_path, const int64_t max_bucket_item = common::MAX_BUCKET_ITEM_DEFAULT,
                    const int64_t max_server_item = common::MAX_SERVER_ITEM_DEFAULT);
      int destroy();
      int build_table(const int8_t interrupt, bool& change, common::NEW_TABLE& new_tables,
                      const std::set<uint64_t>& servers);
      int update_tables_item_status(const uint64_t server, const int64_t version,
                                    const int8_t status, const int8_t phase, common::NEW_TABLE& new_tables);
      int update_table(const int8_t interrupt, const int8_t phase, common::NEW_TABLE& new_tables, bool& update_complete);
      int check_update_table_complete(const int8_t interrupt, const int8_t phase,
                       common::NEW_TABLE& new_tables, bool& update_complete);
      int switch_table(void);
      int dump_tables(const int8_t type = common::DUMP_TABLE_TYPE_ACTIVE_TABLE);
      int dump_tables(common::Buffer& buf, const int8_t type = common::DUMP_TABLE_TYPE_ACTIVE_TABLE);

      int64_t get_build_table_version() const;
      int64_t get_active_table_version() const;
      const char* get_active_table() const;
      const char* get_build_table() const;
      int64_t get_build_table_length() const { return common::MAX_BUCKET_ITEM_DEFAULT * common::INT64_SIZE;}
      int64_t get_active_table_length() const { return common::MAX_BUCKET_ITEM_DEFAULT * common::INT64_SIZE;}
    private:
      int load_tables(const int64_t file_size, const int64_t size, const int64_t max_bucket_item = common::MAX_BUCKET_ITEM_DEFAULT,
                    const int64_t max_server_item = common::MAX_SERVER_ITEM_DEFAULT);
      int set_tables_pointer(const int64_t max_bucket_item, const int64_t max_server_item);
      void inc_build_version();
      void inc_active_version();
      void get_old_servers(const int8_t interrupt, std::set<uint64_t>& servers);
      int64_t get_difference(const int8_t interrupt, std::set<uint64_t>& old_servers,
                            const std::set<uint64_t>& new_servers,std::vector<uint64_t>& news,
                            std::vector<uint64_t>& deads);
      int64_t get_difference(const int8_t interrupt, int64_t& new_count, int64_t& old_count);
      int fill_old_tables(const int8_t interrupt, std::vector<uint64_t>& news,
                          std::vector<uint64_t>& deads);
      int fill_new_tables(const int8_t interrupt, const std::set<uint64_t>& servers,
                          std::vector<uint64_t>& news, std::vector<uint64_t>& deads);
      int send_msg_to_server(const uint64_t server, const int8_t phase);
    public:
      static const int64_t MIN_BUCKET_ITEM;
      static const int64_t MAX_BUCKET_ITEM;
      static const int64_t MAX_SERVER_COUNT;
      static const int64_t MAGIC_NUMBER;
      static const int8_t BUCKET_TABLES_COUNT;
    private:
      uint64_t* tables_;
      uint64_t* tables_end_;
      uint64_t* active_tables_;
      uint64_t* active_tables_end_;
      uint64_t* server_tables_;
      uint64_t* server_tables_end_;
      TablesHeader* header_;
      int64_t fd_; 
      common::MMapFile* file_;
      volatile uint8_t interrupt_;
    private:
      DISALLOW_COPY_AND_ASSIGN(BuildTable);
   };
  } /** rootserver **/
}/** tfs **/
