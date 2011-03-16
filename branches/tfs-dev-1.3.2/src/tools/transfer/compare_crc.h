#ifndef TFS_TOOLS_COMPARE_CRC_H_
#define TFS_TOOLS_COMPARE_CRC_H_

#include <string>
#include <vector>
#include "common/define.h"
#include "common/statistics.h"
#include "common/directory_op.h"
#include "client/tfs_session.h"
#include "client/tfs_file.h"
#include "client/fsname.h"
#include "message/client.h"
#include "message/client_pool.h"

struct StatStruct
{
  int64_t total_count_;
  int64_t succ_count_;
  int64_t fail_count_;
  int64_t error_count_;
  int64_t unsync_count_;
};

struct log_file
{
  FILE** fp_;
  const char* file_;
};

int get_crc_from_tfsname_list(tfs::client::TfsClient& old_tfs_client, tfs::client::TfsClient& new_tfs_client, const char* filename_list, string& modify_time);
int get_crc_from_blockid(tfs::client::TfsClient& old_tfs_client, tfs::client::TfsClient& new_tfs_client, const char* block_list, string& modify_time);

static StatStruct cmp_stat_;

#endif
