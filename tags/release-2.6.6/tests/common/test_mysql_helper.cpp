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
 *   daoan
 *      - initial release
 *
 */
#include <set>
#include <Time.h>
#include <gtest/gtest.h>
#include "common/mysql_cluster/mysql_database_helper.h"
#include "common/mysql_cluster/database_pool.h"
#include "common/error_msg.h"

using namespace std;
using namespace tbsys;

namespace tfs
{
  namespace common
  {
    class MysqlClusterTest: public ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("debug");
        }
        static void TearDownTestCase(){}
        MysqlClusterTest(){}
        virtual ~MysqlClusterTest(){}
        virtual void SetUp(){
          assert(database_pool_.init_pool(5, "10.232.137.34:8808:tfsmeta", "tfsmeta", "tfsmeta"));
        }
        virtual void TearDown(){}
        DataBasePool database_pool_;
    };

    TEST_F(MysqlClusterTest, put_without_version)
    {
      MysqlDatabaseHelper* database_ = database_pool_.get();
      ASSERT_TRUE(NULL != database_);

      int area = 1;
      char* key_str ="key1";
      char* value_str ="value1";

      KvKey key;
      key.key_ = key_str;
      key.key_size_ = 4;

      KvMemValue value;
      value.set_data(value_str, 6);
      int ret = 0;
      ret = database_->insert_kv(area, key, value);
      ASSERT_EQ(TFS_SUCCESS, ret);
      ret = database_->insert_kv(area, key, value);
      ASSERT_EQ(EXIT_KV_RETURN_VERSION_ERROR, ret);
      database_->rm_kv(area, key);

      database_pool_.release(database_);
    }
    TEST_F(MysqlClusterTest, replace)
    {
      MysqlDatabaseHelper* database_ = database_pool_.get();
      ASSERT_TRUE(NULL != database_);

      int area = 1;
      char* key_str ="key1";
      char value_str[50];
      sprintf(value_str,"%s","value1");;

      KvKey key;
      key.key_ = key_str;
      key.key_size_ = 4;

      KvMemValue value;
      KvValue* p_value = NULL;
      value.set_data(value_str, 6);
      int ret = 0;
      int64_t version = 0;
      ret = database_->replace_kv(area, key, value);
      ASSERT_EQ(TFS_SUCCESS, ret);
      ret = database_->get_v(area, key, &p_value, &version);
      ASSERT_EQ(TFS_SUCCESS, ret);
      ASSERT_TRUE(0 == memcmp(value_str, p_value->get_data(), p_value->get_size()));
      p_value->free();

      value_str[1]=3;

      ret = database_->replace_kv(area, key, value);
      ASSERT_EQ(TFS_SUCCESS, ret);
      ret = database_->get_v(area, key, &p_value, &version);
      ASSERT_EQ(TFS_SUCCESS, ret);
      ASSERT_TRUE(0 == memcmp(value_str, p_value->get_data(), p_value->get_size()));
      p_value->free();

      database_->rm_kv(area, key);

      database_pool_.release(database_);
    }
    TEST_F(MysqlClusterTest, update)
    {
      MysqlDatabaseHelper* database_ = database_pool_.get();
      ASSERT_TRUE(NULL != database_);

      int area = 1;
      char key_str[50] ;
      sprintf(key_str, "%s","key1");
      char value_str[50];
      sprintf(value_str, "%s", "value2");

      KvKey key;
      key.key_ = key_str;
      key.key_size_ = 4;

      KvMemValue value;
      value.set_data(value_str, 6);
      int ret = 0;
      KvValue* p_value = NULL;
      int64_t version = 0;

      //no data in mysql, update return err
      ret = database_->update_kv(area, key, value, version);
      ASSERT_EQ(EXIT_KV_RETURN_VERSION_ERROR, ret);

      ret = database_->insert_kv(area, key, value);
      ASSERT_EQ(TFS_SUCCESS, ret);

      *value_str = 'p';

      // update with wrong version
      ret = database_->update_kv(area, key, value, version);
      ASSERT_EQ(EXIT_KV_RETURN_VERSION_ERROR, ret);


      //get vaule, still the old one
      *value_str = 'v';
      ret = database_->get_v(area, key, &p_value, &version);
      ASSERT_EQ(TFS_SUCCESS, ret);
      ASSERT_TRUE(0 == memcmp(value_str, p_value->get_data(), p_value->get_size()));
      p_value->free();

      //update with the right version
      *value_str = 'p';
      ret = database_->update_kv(area, key, value, version);
      ASSERT_EQ(TFS_SUCCESS, ret);

      ret = database_->get_v(area, key, &p_value, &version);
      ASSERT_EQ(TFS_SUCCESS, ret);
      ASSERT_TRUE(0 == memcmp(value_str, p_value->get_data(), p_value->get_size()));
      p_value->free();

      database_->rm_kv(area, key);

      database_pool_.release(database_);
    }
    TEST_F(MysqlClusterTest, get_v)
    {
      MysqlDatabaseHelper* database_ = database_pool_.get();
      ASSERT_TRUE(NULL != database_);

      int area = 1;
      char key_str[50] ;
      sprintf(key_str, "%s","key1");
      char value_str[50];
      sprintf(value_str, "%s", "value2");

      KvKey key;
      key.key_ = key_str;
      key.key_size_ = 4;

      KvMemValue value;
      value.set_data(value_str, 6);
      int ret = 0;
      KvValue* p_value = NULL;
      int64_t version = 0;


      ret = database_->insert_kv(area, key, value);
      ASSERT_EQ(TFS_SUCCESS, ret);

      *key_str = 'p';

      ret = database_->get_v(area, key, &p_value, &version);

      ASSERT_EQ(EXIT_KV_RETURN_DATA_NOT_EXIST, ret);

      *key_str = 'k';
      database_->rm_kv(area, key);

      database_pool_.release(database_);
    }
    TEST_F(MysqlClusterTest, scan_v)
    {
      MysqlDatabaseHelper* database_ = database_pool_.get();
      ASSERT_TRUE(NULL != database_);

      int area = 1;
      char key_str[10][50] ;
      char value_str[10][50];
      for (int i = 0; i < 10; i++)
      {
        sprintf(key_str[i], "key%d", i);
        sprintf(value_str[i], "value%d", i);
      }

      KvKey key;
      KvMemValue value;
      int ret = 0;
      for (int i = 0; i < 10; i++)
      {
        key.key_ = key_str[i];
        key.key_size_ = 4;

        value.set_data(value_str[i], 6);
        ret = database_->insert_kv(area, key, value);
        ASSERT_EQ(TFS_SUCCESS, ret);
      }
      std::vector<KvValue*> keys;
      std::vector<KvValue*> values;
      char start_key_str[50];
      char end_key_str[50];
      int32_t count=0;
      sprintf(start_key_str, "k");
      sprintf(end_key_str,"l");
      KvKey s_key;
      KvKey e_key;
      s_key.key_ = start_key_str;
      s_key.key_size_=1;
      e_key.key_ = end_key_str;
      e_key.key_size_=1;

      ret = database_->scan_v(area, s_key, e_key, 5, false, &keys, &values, &count);
      ASSERT_EQ(TFS_SUCCESS, ret);
      ASSERT_EQ(5, count);
      for (int i = 0; i < 5; i++)
      {
        ASSERT_TRUE(0 == memcmp(key_str[i], keys[i]->get_data(), keys[i]->get_size()));
        keys[i]->free();
        ASSERT_TRUE(0 == memcmp(value_str[i], values[i]->get_data(), values[i]->get_size()));
        values[i]->free();
      }
      keys.clear();
      values.clear();

      sprintf(start_key_str, "key3");
      s_key.key_size_=4;
      ret = database_->scan_v(area, s_key, e_key, 5, false, &keys, &values, &count);
      ASSERT_EQ(TFS_SUCCESS, ret);
      ASSERT_EQ(5, count);
      for (int i = 0; i < 5; i++)
      {
        printf("%s %d %s %d\n", keys[i]->get_data(), keys[i]->get_size(),
            values[i]->get_data(), values[i]->get_size());
        ASSERT_TRUE(0 == memcmp(key_str[i+3], keys[i]->get_data(), keys[i]->get_size()));
        keys[i]->free();
        ASSERT_TRUE(0 == memcmp(value_str[i+3], values[i]->get_data(), values[i]->get_size()));
        values[i]->free();
      }
      keys.clear();
      values.clear();
      *end_key_str = 0;

      ret = database_->scan_v(area, s_key, e_key, -2, true, &keys, &values, &count);
      ASSERT_EQ(TFS_SUCCESS, ret);
      ASSERT_EQ(2, count);
      for (int i = 0; i < 2; i++)
      {
        printf("%s %d %s %d\n", keys[i]->get_data(), keys[i]->get_size(),
            values[i]->get_data(), values[i]->get_size());
        ASSERT_TRUE(0 == memcmp(key_str[2-i], keys[i]->get_data(), keys[i]->get_size()));
        keys[i]->free();
        ASSERT_TRUE(0 == memcmp(value_str[2-i], values[i]->get_data(), values[i]->get_size()));
        values[i]->free();
      }
      keys.clear();
      values.clear();

      for (int i = 0; i < 10; i++)
      {
        key.key_ = key_str[i];
        key.key_size_ = 4;

        ret = database_->rm_kv(area, key);
      }

      database_pool_.release(database_);
    }

  }
}
int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

