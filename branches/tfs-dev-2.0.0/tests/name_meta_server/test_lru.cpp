/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_block_chunk.cpp 5 2010-10-21 07:44:56Z
 *
 * Authors:
 *   duanfei 
 *      - initial release
 *
 */
#include <set>
#include <gtest/gtest.h>
#include <tbsys.h>
#include <Memory.hpp>
#include "lru.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::namemetaserver;
using namespace tbsys;

namespace tfs
{
  namespace namemetaserver
  {
    struct Key
    {
      int64_t app_id_;
      int64_t user_id_;
      bool operator < (const Key& key) const;
    };

    bool Key::operator < (const Key& key) const
    {
      if (app_id_ < key.app_id_)
        return true;
      if (app_id_ > key.app_id_)
        return false;
      return user_id_ < key.user_id_;
    }

    struct RootNode
    {
      int32_t key_;
    };

    class LruTest: public ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("error");
        }
        static void TearDownTestCase(){}
        LruTest(){}
        virtual ~LruTest(){}
        virtual void SetUp(){}
        virtual void TearDown(){}
        static const int32_t SERVER_COUNT;
    };

    TEST_F(LruTest, insert)
    {
      std::vector<RootNode*> rs;
      Lru<Key,RootNode> lru;
      Key key;
      key.app_id_ = 1;
      key.user_id_ = 2;
      RootNode* node = new RootNode();
      node->key_ = 2;
      int32_t iret = lru.insert(key, node); 
      node = NULL;
      EXPECT_EQ(TFS_SUCCESS , iret);
      node = lru.get(key);
      EXPECT_TRUE(NULL != node);
      EXPECT_EQ(2, node->key_);
      iret = lru.insert(key, new RootNode());
      EXPECT_EQ(EXIT_LRU_VALUE_EXIST, iret);
      lru.gc(rs);
      std::vector<RootNode*>::iterator iter = rs.begin();
      for(; iter != rs.end();++iter)
      {
        tbsys::gDelete((*iter));
      }
    }

    TEST_F(LruTest, get)
    {
      std::vector<RootNode*> rs;
      Lru<Key,RootNode> lru;
      Key key;
      key.app_id_ = 1;
      key.user_id_ = 2;
      RootNode* node = new RootNode();
      node->key_ = 2;
      int32_t iret = lru.insert(key, node); 
      node = NULL;
      EXPECT_EQ(TFS_SUCCESS , iret);
      node = lru.get(key);
      EXPECT_TRUE(NULL != node);
      EXPECT_EQ(2, node->key_);
      Lru<Key, RootNode>::Node<RootNode>* inode = lru.get_node(key);
      EXPECT_TRUE(NULL != inode);
      EXPECT_EQ(1, inode->ref_count_);
      lru.gc(rs);
      std::vector<RootNode*>::iterator iter = rs.begin();
      for(; iter != rs.end();++iter)
      {
        tbsys::gDelete((*iter));
      }
    }

    TEST_F(LruTest, put)
    {
      std::vector<RootNode*> rs;
      Lru<Key,RootNode> lru;
      Key key;
      key.app_id_ = 1;
      key.user_id_ = 2;
      RootNode* node = new RootNode();
      node->key_ = 2;
      int32_t iret = lru.insert(key, node); 
      node = NULL;
      EXPECT_EQ(TFS_SUCCESS , iret);
      node = lru.get(key);
      EXPECT_TRUE(NULL != node);
      EXPECT_EQ(2, node->key_);
      Lru<Key, RootNode>::Node<RootNode>* inode = lru.get_node(key);
      EXPECT_TRUE(NULL != inode);
      EXPECT_EQ(1, inode->ref_count_);
      lru.put(key);
      inode = lru.get_node(key);
      EXPECT_TRUE(NULL != inode);
      EXPECT_EQ(0, inode->ref_count_);
      lru.gc(rs);
      std::vector<RootNode*>::iterator iter = rs.begin();
      for(; iter != rs.end();++iter)
      {
        tbsys::gDelete((*iter));
      }
    }

    TEST_F(LruTest, gc)
    {
      Key key;
      int32_t iret = TFS_SUCCESS;
      std::vector<RootNode*> rs;
      Lru<Key,RootNode> lru;
      int32_t MAX_COUNT = 1000000;
      for (int32_t i = 0; i < MAX_COUNT; ++i)
      {
        key.app_id_ = i;
        key.user_id_ = i + MAX_COUNT;
        iret = lru.insert(key, new RootNode());
        EXPECT_EQ(TFS_SUCCESS, iret);
        if (i > MAX_COUNT / 2)
          lru.get(key);
      }
      double ratio = 0.2;
      uint64_t gc_count = static_cast<uint64_t>(MAX_COUNT * ratio) + 1;
      TBSYS_LOG(INFO, "gc_count %ld", gc_count);
      BaseStrategy<Key, RootNode> bs(lru);
      iret = lru.gc(ratio, &bs, rs);
      EXPECT_EQ(TFS_SUCCESS, iret);
      EXPECT_EQ(gc_count, rs.size());
      EXPECT_EQ(MAX_COUNT - gc_count, lru.list_.size());
      std::vector<RootNode*>::iterator iter = rs.begin();
      for(; iter != rs.end();++iter)
      {
        tbsys::gDelete((*iter));
      }
    }
  }/** rootserver **/
}/** tfs **/

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
