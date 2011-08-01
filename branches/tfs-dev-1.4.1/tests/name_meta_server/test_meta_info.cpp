/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_bit_map.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *    daoan(daoan@taobao.com)
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include "meta_server_service.h"

using namespace tfs::namemetaserver;
using namespace tfs::common;

class MetaInfoTest: public ::testing::Test
{
  public:
    MetaInfoTest()
    {
    }
    ~MetaInfoTest()
    {
    }
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
};
TEST_F(MetaInfoTest, check_frag_info_ok)
{
  FragInfo fi;
  fi.cluster_id_ = 1;
  fi.had_been_split_ = false;
  
  FragMeta fm;
  fm.block_id_ = 1;
  fm.file_id_ = 1;
  fm.offset_ = 10;
  fm.size_ = 10;
  fi.v_frag_meta_.push_back(fm);  
  
  fm.offset_ = 20;
  fm.size_ = 5;
  fi.v_frag_meta_.push_back(fm);  
  fm.offset_ = 30;
  fm.size_ = 5;
  fi.v_frag_meta_.push_back(fm);  

  EXPECT_EQ(TFS_SUCCESS, MetaServerService::check_frag_info(fi));
}

TEST_F(MetaInfoTest, check_frag_info_overerite)
{
  FragInfo fi;
  fi.cluster_id_ = 1;
  fi.had_been_split_ = true;
  
  FragMeta fm;
  fm.block_id_ = 1;
  fm.file_id_ = 1;
  fm.offset_ = 0;
  fm.size_ = 10;
  fi.v_frag_meta_.push_back(fm);  //0 - 10
  
  fm.offset_ = 8;
  fm.size_ = 2;
  fi.v_frag_meta_.push_back(fm);  //8 - 10

  EXPECT_EQ(TFS_ERROR, MetaServerService::check_frag_info(fi));
}
TEST_F(MetaInfoTest, check_frag_info_no_sort)
{
  FragInfo fi;
  fi.cluster_id_ = 1;
  fi.had_been_split_ = true;
  
  FragMeta fm;
  fm.block_id_ = 1;
  fm.file_id_ = 1;
  fm.offset_ = 10;
  fm.size_ = 10;
  fi.v_frag_meta_.push_back(fm);  
  
  fm.offset_ = 8;
  fm.size_ = 2;
  fi.v_frag_meta_.push_back(fm);  

  EXPECT_EQ(TFS_ERROR, MetaServerService::check_frag_info(fi));
}

TEST_F(MetaInfoTest, check_frag_info_error_cross)
{
  FragInfo fi;
  fi.cluster_id_ = 1;
  fi.had_been_split_ = true;
  
  FragMeta fm;
  fm.block_id_ = 1;
  fm.file_id_ = 1;
  fm.offset_ = 10;
  fm.size_ = 10;
  fi.v_frag_meta_.push_back(fm);  
  
  fm.offset_ = 19;
  fm.size_ = 5;
  fi.v_frag_meta_.push_back(fm);  

  EXPECT_EQ(TFS_ERROR, MetaServerService::check_frag_info(fi));
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
