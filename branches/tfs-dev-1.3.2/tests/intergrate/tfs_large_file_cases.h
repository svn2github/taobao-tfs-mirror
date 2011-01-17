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
 *   qushan <qushan@taobao.com>
 *      - initial release
 *
 */
#include <tbsys.h>
#include <gtest/gtest.h>
#include "message/message_factory.h"
#include "common/define.h"
#include "new_client/tfs_client_api.h"

#define EXIT_FILENAME_MATCH_ERROR1 -100
#define EXIT_FILENAME_MATCH_ERROR2 -101
#define EXIT_FILECONTENT_MATCHERROR -102

int generate_length(int64_t& length, int64_t base);

class TfsLargeFileTest : public testing::Test
{
  public:
    static void SetUpTestCase();
    static void TearDownTestCase();
  public:
    static int64_t write_new_file(const bool large_flag, const int64_t length, uint32_t& crc,
                   char* tfsname = NULL, const char* suffix = NULL, const int32_t name_len = 0);
    static int read_exist_file(const bool large_flag, const char* tfs_name, const char* suffix, uint32_t& read_crc);
    static int unlink_file(const char* tfsname, const char* suffix = NULL, const int action = 0);
    static int stat_exist_file(const bool large_flag, char* tfsname, tfs::common::FileStat& file_info);

    static void test_read(const bool large_flag, int64_t base, const char* suffix = NULL);
    static void unlink_process(const bool large_flag);
    static void test_update(const bool large_flag, int64_t base, const char* suffix = NULL);
  private:
    static int generate_data(char* buffer, const int32_t length);


  private:
    //static const int64_t PER_SIZE = 8 * 1024 * 1024;
    static const int64_t PER_SIZE = 19 * 512 * 1024;
    //static const int64_t SEG_SIZE = 1024 * 1024;
    static const int64_t SEG_SIZE = PER_SIZE;

  public:
    static tfs::client::TfsClient* tfs_client_;
};
