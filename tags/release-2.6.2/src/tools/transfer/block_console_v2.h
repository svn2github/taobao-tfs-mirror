/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: block_console.h 371 2011-05-27 07:24:52Z nayan@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */
#ifndef TFS_TOOL_BLOCKCONSOLE_H_
#define TFS_TOOL_BLOCKCONSOLE_H_
#include <tbnet.h>
#include <Mutex.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <string>
#include <set>

#include "common/internal.h"
#include "common/error_msg.h"

const int32_t TRANSFER_FINISH = 10000;

struct StatParam
{
  int64_t  total_;
  atomic_t copy_success_;
  atomic_t copy_failure_;

  StatParam() : total_(0)
  {
    atomic_set(&copy_success_, 0);
    atomic_set(&copy_failure_, 0);
  }

  void dump(void)
  {
    TBSYS_LOG(ERROR, "[copy] total: %"PRI64_PREFIX"d, success: %d, fail: %d\n",
        total_, atomic_read(&copy_success_), atomic_read(&copy_failure_));
  }
};

class BlockConsole
{
  public:
    BlockConsole();
    ~BlockConsole();

    int initialize(const std::string& ts_input_blocks_file, const std::string& dest_ds_ip_file);
    int get_transfer_param(uint64_t& blockid, uint64_t& ds_id);
    int finish_transfer_block(const uint64_t blockid, const int32_t transfer_ret);

  private:
    int locate_cur_pos();

  private:
    std::set<uint64_t> input_blockids_;
    std::set<uint64_t> succ_blockids_;
    std::set<uint64_t> fail_blockids_;
    std::set<uint64_t> dest_ds_ids_;
    std::set<uint64_t>::iterator cur_ds_sit_;
    std::set<uint64_t>::iterator cur_sit_;
    FILE* input_file_ptr_;
    FILE* succ_file_ptr_;
    FILE* fail_file_ptr_;
    StatParam stat_param_;
    tbutil::Mutex mutex_;
};

struct FileInfoV2Cmp
{
  bool operator()(const tfs::common::FileInfoV2& left, const tfs::common::FileInfoV2& right) const
  {
    return (left.id_ < right.id_);
  }
};

typedef std::set<tfs::common::FileInfoV2, FileInfoV2Cmp> FileInfoV2Set;
typedef FileInfoV2Set::iterator FileInfoV2SetIter;

class TranBlock
{
  public:
    TranBlock(const uint64_t blockid, const std::string& src_ns_addr, const std::string& dest_ns_addr, const uint64_t dest_ds_id, const int64_t traffic);
    ~TranBlock();

    int run();
    int get_tran_size() const
    {
      return total_tran_size_;
    }

  private:
    int get_src_ds();
    int read_index();
    int read_index(uint64_t ds_ip_addr, FileInfoV2Set& file_set);
    int read_data();
    int recombine_data();
    int check_dest_blk();
    int write_data();
    int write_index();
    int check_integrity();
    int rm_block_from_ns();
    int rm_block_from_ds(uint64_t ds_id);

  private:
    static const int64_t TRAN_BUFFER_SIZE;
    static const int64_t RETRY_TIMES;

  private:
    std::string src_ns_addr_;
    std::string dest_ns_addr_;
    uint64_t dest_ds_id_;
    int32_t cur_offset_;
    int64_t total_tran_size_;
    int64_t traffic_;
    uint64_t src_block_id_;
    uint64_t src_ds_addr_random_;
    tfs::common::IndexHeaderV2 src_header_;
    FileInfoV2Set src_file_set_;
    tbnet::DataBuffer src_content_buf_;
    tbnet::DataBuffer dest_content_buf_;
    tfs::common::IndexDataV2 dest_index_data_;
    StatParam stat_param_;
};

#endif
