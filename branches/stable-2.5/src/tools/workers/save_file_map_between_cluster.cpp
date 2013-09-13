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
#include "new_client/fsname.h"
#include "new_client/tfs_client_impl.h"
#include "tools/util/tool_util.h"
#include "tools/util/base_worker.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::client;
using namespace tfs::tools;

#define tfs_client TfsClientImpl::Instance()

FILE* result_fp = NULL;

class SaveFileWorker : public BaseWorker
{
  public:
    int process(string& line)
    {
      return save_file_between_cluster(src_addr_.c_str(),
          dest_addr_.c_str(), line.c_str());
    }

    int save_file_between_cluster(const char* src_ns, const char* dest_ns,
        const char* tfs_file)
    {
      int ret = TFS_SUCCESS;
      char buf[MAX_READ_SIZE];
      char tfs_name[TFS_FILE_LEN];
      int src_fd = -1;
      int dest_fd = -1;

      src_fd = tfs_client->open(tfs_file, NULL, src_ns, T_READ | T_FORCE);
      ret = (src_fd >= 0) ? TFS_SUCCESS : src_fd;
      if (TFS_SUCCESS == ret)
      {
        tfs_client->set_option_flag(src_fd,
            static_cast<OptionFlag>(READ_DATA_OPTION_FLAG_FORCE));
      }

      if (TFS_SUCCESS == ret)
      {
        dest_fd = tfs_client->open(NULL, NULL, dest_ns, T_CREATE | T_WRITE);
        ret = (dest_fd >= 0) ? TFS_SUCCESS : dest_fd;
      }

      if (TFS_SUCCESS == ret)
      {
        while (true)
        {
          int read_len = tfs_client->read(src_fd, buf, MAX_READ_SIZE);
          ret = (read_len >= 0) ? TFS_SUCCESS : EXIT_READ_FILE_ERROR;
          if (TFS_SUCCESS == ret)
          {
            if (read_len > 0)
            {
              int write_len = tfs_client->write(dest_fd, buf, read_len);
              ret = (read_len == write_len) ? TFS_SUCCESS : EXIT_WRITE_FILE_ERROR;
            }

            if (read_len < MAX_READ_SIZE)  // break when EOF
            {
              break;
            }
          }

          // error or EOF, just break
          if ((TFS_SUCCESS != ret) || (read_len < MAX_READ_SIZE))
          {
            break;
          }
        }
      }

      if (src_fd >= 0)
      {
        int tmp_ret = tfs_client->close(src_fd);
        ret = (tmp_ret == TFS_SUCCESS) ? ret : tmp_ret;
      }

      if (dest_fd >= 0)
      {
        int tmp_ret = tfs_client->close(dest_fd, tfs_name, TFS_FILE_LEN);
        ret = (tmp_ret == TFS_SUCCESS) ? ret : tmp_ret;
      }

      if (TFS_SUCCESS == ret)
      {
        fprintf(result_fp, "%s %s\n", tfs_file, tfs_name);
      }

      return ret;
    }


};

class SaveFileWorkerManager : public BaseWorkerManager
{
  public:
    BaseWorker* create_worker()
    {
      return new SaveFileWorker();
    }

    int begin()
    {
      string result_path = output_dir_ + "/file_map";
      result_fp = fopen(result_path.c_str(), "a");
      assert(NULL != result_fp);

      int ret = tfs_client->initialize(NULL, 1800, 0, true);
      assert(TFS_SUCCESS == ret);
      return ret;
    }

    void end()
    {
      if (NULL != result_fp)
      {
        fclose(result_fp);
      }

      tfs_client->destroy();
    }
};

int main(int argc, char** argv)
{
  SaveFileWorkerManager manager;
  return manager.main(argc, argv);
}
