/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: rootserver.h 590 2011-08-17 16:36:13Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#include <Time.h>
#include <Mutex.h>
#include <Monitor.h>
#include <TbThread.h>
#include <gtest/gtest.h>

#include "common/lock.h"
#include "common/buffer.h"
#include "common/rts_define.h"

#include "build_table.h"
#include "lru.h"

namespace tfs
{
  namespace rootserver
  {
    class MetaServerManager
    {
      friend class MetaServerManagerTest;
      FRIEND_TEST(MetaServerManagerTest, exist);
      FRIEND_TEST(MetaServerManagerTest, lease_exist);
      FRIEND_TEST(MetaServerManagerTest, register_);
      FRIEND_TEST(MetaServerManagerTest, unregister);
      FRIEND_TEST(MetaServerManagerTest, renew);
      FRIEND_TEST(MetaServerManagerTest, check_ms_lease_expired);
    public:
      MetaServerManager();
      virtual ~MetaServerManager();
      int initialize(const std::string& table_file_path);
      int destroy();
      bool exist(const uint64_t id);
      bool lease_exist(const uint64_t id);
      int keepalive(const int8_t type, common::MetaServer& server);
      int dump_meta_server(void);
      int dump_meta_server(common::Buffer& buffer);
      int check_ms_lease_expired(void);
      int switch_table(common::NEW_TABLE& tables);
      int update_tables_item_status(const uint64_t server, const int64_t version,
                                    const int8_t status, const int8_t phase);
      int get_tables(char* table, int64_t& length);
    private:
      int register_(common::MetaServer& server);
      int unregister(const uint64_t id);
      int renew(common::MetaServer& server);
      void build_table(void);
      uint64_t new_lease_id(void);
      void interrupt(void) ;
      void check_ms_lease_expired_helper(const tbutil::Time& now, std::vector<uint64_t>& servers);
      int build_table_helper(int8_t& phase, common::NEW_TABLE& tables, bool& update_complete);
      int update_table_helper(int8_t& phase, common::NEW_TABLE& tables, bool& update_complete);

      //debug helper
      common::MetaServer* get(const uint64_t id);
    private:
      class BuildTableThreadHelper: public tbutil::Thread
      {
      public:
        explicit BuildTableThreadHelper(MetaServerManager& manager): manager_(manager){start();};
        virtual ~BuildTableThreadHelper(){};
        void run();
      private:
        MetaServerManager& manager_;
        DISALLOW_COPY_AND_ASSIGN(BuildTableThreadHelper);
      };
      typedef tbutil::Handle<BuildTableThreadHelper> BuildTableThreadHelperPtr;

      class CheckMetaServerLeaseThreadHelper: public tbutil::Thread
      {
      public:
        explicit CheckMetaServerLeaseThreadHelper(MetaServerManager& manager) : manager_(manager){start();}
        virtual ~CheckMetaServerLeaseThreadHelper(){}
        void run();
      private:
        MetaServerManager& manager_;
        DISALLOW_COPY_AND_ASSIGN(CheckMetaServerLeaseThreadHelper);
      };
      typedef tbutil::Handle<CheckMetaServerLeaseThreadHelper> CheckMetaServerLeaseThreadHelperPtr;
    private:
      common::RWLock mutex_;
      common::META_SERVER_MAPS servers_;
      tbutil::Monitor<tbutil::Mutex> build_table_monitor_;
      tbutil::Monitor<tbutil::Mutex> check_ms_lease_monitor_;
      BuildTable build_tables_;
      volatile uint64_t lease_id_factory_; 
      BuildTableThreadHelperPtr build_table_thread_;
      CheckMetaServerLeaseThreadHelperPtr check_ms_lease_thread_;
      common::NEW_TABLE new_tables_;
      bool initialize_;
      bool destroy_;
      int8_t interrupt_;
    private:
      DISALLOW_COPY_AND_ASSIGN(MetaServerManager);
   };
  } /** rootserver **/
}/** tfs **/
