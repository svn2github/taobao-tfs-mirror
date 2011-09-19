/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_case_factory.cpp 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */
#include "test_tfs_case.h"
#include "test_case_factory.h"
#include "test_tfs_seed.h"
#include "test_tfs_read.h"
#include "test_tfs_unlink.h"
#include "test_tfs_update.h"

TestTfsCase * TestCaseFactory::getTestCase(string test_index)
{
  if (test_index == "tfsSeed") {
    return new TestTfsSeed();
  } else if (test_index == "tfsUpdate") {
    return new TestTfsUpdate();
  } else if (test_index == "tfsRead") {
    return new TestTfsRead();
  } else if (test_index == "tfsUnlink") {
    return new TestTfsUnlink();
  } else {
    TBSYS_LOG(ERROR, "Test index error : %s", test_index.c_str());    
  }

  return NULL;
}
