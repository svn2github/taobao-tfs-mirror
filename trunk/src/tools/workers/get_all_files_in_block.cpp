/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#include "common/internal.h"
#include "common/func.h"
#include "clientv2/fsname.h"
#include "dataserver/ds_define.h"
#include "tools/util/tool_util.h"
#include "tools/util/base_worker.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::clientv2;
using namespace tfs::dataserver;
using namespace tfs::tools;

FILE* result_fp = NULL;

class ListFileWorker : public BaseWorker
{
  public:
    int process(string& line)
    {
      uint64_t ns_ip = Func::get_host_ip(src_addr_.c_str());
      uint64_t block_id = strtoull(line.c_str(), NULL, 10);
      vector<FileInfo> finfos;
      vector<uint64_t> replicas;
      int ret = ToolUtil::get_block_ds_list(ns_ip, block_id, replicas);
      if (TFS_SUCCESS == ret)
      {
        vector<uint64_t>::iterator iter = replicas.begin();
        for ( ; iter != replicas.end(); iter++)
        {
          ret = ToolUtil::list_file(*iter, block_id, finfos);
          if (TFS_SUCCESS == ret)
          {
            break;
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        vector<FileInfo>::iterator iter = finfos.begin();
        for ( ; iter != finfos.end(); iter++)
        {
          if (!(iter->flag_ & FI_DELETED))
          {
            FSName fsname(block_id, iter->id_);
            fprintf(result_fp, "%s\n", fsname.get_name());
          }
        }
      }

      return ret;
    }
};

class ListFileWorkerManager : public BaseWorkerManager
{
  public:
    BaseWorker* create_worker()
    {
      return new ListFileWorker();
    }

    int begin()
    {
      string result_path = output_dir_ + "/file_list";
      result_fp = fopen(result_path.c_str(), "a");
      assert(NULL != result_fp);
      return TFS_SUCCESS;
    }

    void end()
    {
      if (NULL != result_fp)
      {
        fclose(result_fp);
      }
    }
};


int main(int argc, char** argv)
{
  ListFileWorkerManager manager;
  return manager.main(argc, argv);
}
