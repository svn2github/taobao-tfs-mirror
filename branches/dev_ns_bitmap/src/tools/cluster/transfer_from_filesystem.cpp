#include <tbsys.h>
#include <list>
#include <Memory.hpp>
#include "common/internal.h"
#include "common/error_msg.h"
#include "common/func.h"
#include "common/meta_server_define.h"
#include "tools/util/base_worker.h"
#include "tfs_client_api.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::tools;
using namespace tfs::client;

#define MAX_FILE_LEN 1000
#define BUFFER_SIZE 4*1024*1024

RestClient* g_tfs_client = NULL;
string g_app_key;
string g_local_dir;
bool g_need_private = false;
int64_t g_app_id = -1;
int64_t g_hash_count = 10000;

class TransferFileManger;
class TransferFileWorker: public BaseWorker
{
  public:
    TransferFileWorker(TransferFileManger* manager) : manager_(manager)
    {
      buf_ = new (std::nothrow) char[BUFFER_SIZE];
      assert(NULL != buf_);
    }
    virtual ~TransferFileWorker()
    {
      if (NULL != buf_)
      {
        tbsys::gDeleteA(buf_);
      }
    }
    virtual int process(string& line);
    TransferFileManger* manager_;
  private:
    int transfer_file(const char* localfile, const char* filename, const int64_t uid);
    void remove_fail_file(const char* filename, const int64_t uid);
    char* buf_;
};


class TransferFileManger: public BaseWorkerManager
{
  public:
    TransferFileManger() {}
    ~TransferFileManger(){}

    virtual int begin();
    virtual void end();
    virtual BaseWorker* create_worker();

  private:
    virtual void usage(const char* app_name);
};


void TransferFileManger::usage(const char* app_name)
{
  char *options =
    "-s           rs server ip:port\n"
    "-x           app_key\n"
    "-f           input files name, each line is a self-defined filename start-with '/'\n"
    "-d           local files dir, default is root mount point.\n"
    "-e           private flag, use /tfsprivate/xxx, optional, default false\n"
    "-t           thread count, optional, defaul 1\n"
    "-i           sleep interval (ms), optional, default 0\n"
    "-l           log level, optional, default info\n"
    "-p           output directory, optional, default ./\n"
    "-n           deamon, default false\n"
    "-h           print help information\n"
    "signal       SIGTERM/SIGINT stop transfer\n"
    "             SIGUSR1 inc sleep interval 1000ms\n"
    "             SIGUSR2 dec sleep interval 1000ms\n";
  fprintf(stderr, "tool will read local file by self-defined filename in local file dir\n");
  fprintf(stderr, "%s usage:\n%s", app_name, options);
  fprintf(stderr, "eg: %s -s 192.168.0.1:9999 -x app_key [-d /tfs/data] [-e] [-n] -f input_file [-p transfer_dir] [-t thread_cnt]\n", app_name);
  fprintf(stderr, "\t# NAS file path: /tfs/data/dir1/abc/f1, if you want to save self-defined into tfs as '/dir1/abc/f1', please use prefix dir '-d /tfs/data' and put '/tfs/data/dir1/abc/f1' in input_file; or if you want to save self-defined as the whole path '/tfs/data/dir1/abc/f1', no need specify -d param.\n");
  fprintf(stderr, "\t# you can run many times with the whole same command, when lines number of ./data/success equal with input_file, indicate all files transfer to tfs successfully \n");
  exit(-1);
}


void TransferFileWorker::remove_fail_file(const char* filename, const int64_t uid)
{
  int ret = g_tfs_client->rm_file(uid, filename, g_app_key.c_str(), g_app_id);
  TBSYS_LOG_DW(ret, "remove fail file: %s %s", filename,  TFS_SUCCESS == ret ? "success" : "fail");
}

int TransferFileWorker::transfer_file(const char* localfile, const char* filename, const int64_t uid)
{
  int ret = TFS_SUCCESS;
  if (NULL == filename || NULL == buf_ || strlen(filename) <= 0)
  {
    ret = EXIT_PARAMETER_ERROR;
  }
  else if (filename[0] != '/')
  {
     TBSYS_LOG(ERROR, "filename: %s format error, must start with '/'\n", filename);
    ret = EXIT_PARAMETER_ERROR;
  }

  // 1. make sure file not exist before
  if (TFS_SUCCESS == ret && g_tfs_client->is_file_exist(g_app_id, uid, filename))//TODO: must to modify c++ restful api
  {
    TBSYS_LOG(ERROR, "file: %s already exist in tfs", filename);
    ret = EXIT_TARGET_EXIST_ERROR;
  }

  int local_fd = -1;
  if (TFS_SUCCESS == ret)
  {
    local_fd = ::open(localfile, O_RDONLY);
    if (-1 == local_fd)
    {
      TBSYS_LOG(ERROR, "open local file: %s failed, error: %s", localfile, strerror(errno));
      ret = EXIT_OPEN_FILE_ERROR;
    }
  }

  int tfs_fd = -1;
  if (TFS_SUCCESS == ret)
  {
    ret = g_tfs_client->create_file(uid, filename);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "create tfs file: %s failed, ret: %d", filename, ret);
    }
    else
    {
      tfs_fd = g_tfs_client->open(g_app_id, uid, filename, RestClient::WRITE);
      if (tfs_fd <= 0)
      {
        TBSYS_LOG(ERROR, "open tfs file: %s failed, ret: %d", filename, tfs_fd);
        ret = EXIT_OPEN_FILE_ERROR;
      }
    }
  }

  // 2. write data per BUFFER_SIZE
  int64_t total_size = 0;
  int64_t offset = 0;
  uint32_t crc = 0;
  if (TFS_SUCCESS == ret)
  {
    while (1)
    {
      int64_t rsize = ::pread(local_fd, buf_, BUFFER_SIZE, offset);
      if (-1 == rsize)
      {
        TBSYS_LOG(ERROR, "read local file: %s failed, offset: %"PRI64_PREFIX"d, wsize: %d, error: %s", localfile, offset, BUFFER_SIZE, strerror(errno));
        ret = EXIT_READ_FILE_ERROR;
        break;
      }
      else if(rsize == 0)
      {
        break;// read finish
      }

      int64_t wsize = g_tfs_client->pwrite(tfs_fd, buf_, rsize, offset);
      if (wsize != rsize)
      {
        TBSYS_LOG(ERROR, "write tfs file: %s failed, offset: %"PRI64_PREFIX"d, wsize: %"PRI64_PREFIX"d, ret: %d", filename, offset, rsize, (int)wsize);
        ret = EXIT_WRITE_FILE_ERROR;
        break;
      }

      crc = Func::crc(crc, buf_, rsize);
      offset += rsize;

      if (rsize < BUFFER_SIZE)
      {
        break;// read finish
      }
    }
    total_size = offset;
  }

  if (local_fd >= 0)
  {
    ::close(local_fd);
  }

  if (tfs_fd > 0)
  {
    int close_ret = g_tfs_client->close(tfs_fd);
    if (TFS_SUCCESS != close_ret)
    {
      TBSYS_LOG(ERROR, "close tfs file: %s failed, ret: %d", filename, close_ret);
      ret = (TFS_SUCCESS == ret) ? close_ret : ret; // keep first error return code
    }
  }


  // 3. check crc
  tfs::restful::FileMetaInfo file_meta_info;
  if (TFS_SUCCESS == ret)
  {
    // get file size
    ret = g_tfs_client->ls_file(g_app_id, uid, filename, file_meta_info);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "ls_file tfs file: %s failed, ret: %d", filename, ret);
    }
  }

  // calc crc
  tfs_fd = -1;
  if (TFS_SUCCESS == ret)
  {
    tfs_fd = g_tfs_client->open(g_app_id, uid, filename, RestClient::READ);
    if (tfs_fd <= 0)
    {
      TBSYS_LOG(ERROR, "open tfs file: %s failed, ret: %d", filename, tfs_fd);
      ret = EXIT_OPEN_FILE_ERROR;
    }
  }

  int64_t ret_len = 0;
  uint32_t ret_crc = 0;
  if (TFS_SUCCESS == ret)
  {
    offset = 0;
    while (offset < file_meta_info.size_)
    {
      int64_t read_size = min((int64_t)(BUFFER_SIZE), file_meta_info.size_ - offset);
      ret_len = g_tfs_client->pread(tfs_fd, buf_, read_size, offset);
      if (ret_len < 0)
      {
        TBSYS_LOG(ERROR, "pread tfs file: %s failed, , ret: %d", filename, (int)ret_len);
        ret = ret_len;
        break;
      }
      else if (0 == ret_len)
      {
        break;
      }
      ret_crc = Func::crc(ret_crc, buf_, ret_len);
      offset += ret_len;
    }
  }

  if (TFS_SUCCESS == ret)
  {
    if (offset != file_meta_info.size_ || offset != total_size || ret_crc != crc)
    {
      TBSYS_LOG(ERROR, "tfs file: %s crc diff, %"PRI64_PREFIX"d, %"PRI64_PREFIX"d, %"PRI64_PREFIX"d, crc %u:%u", filename,
          offset, file_meta_info.size_, total_size, ret_crc, crc);
      ret = EXIT_CHECK_CRC_ERROR;
    }
  }

  if (tfs_fd > 0)
  {
    int close_ret = g_tfs_client->close(tfs_fd);
    if (TFS_SUCCESS != close_ret)
    {
      TBSYS_LOG(ERROR, "close tfs file: %s failed, ret: %d", filename, close_ret);
      ret = (TFS_SUCCESS == ret) ? close_ret : ret; // keep first error return code
    }
  }
  return ret;
}

int TransferFileWorker::process(string& line)
{
  int ret = TFS_SUCCESS;
  vector<string> fields;
  int len = Func::split_string(line.c_str(), '/', fields);
  if (len < 1 || line.length() >= MAX_LINE_LENGTH-1) // one line length more than 512byte(include '\0')
  {
    ret = EXIT_PARAMETER_ERROR;
    TBSYS_LOG(ERROR, "this line(%s) is a null or invalid input file", line.c_str());
  }
  else if(NULL == buf_) // copy file data buffer
  {
    TBSYS_LOG(ERROR, "buf is NULL error");
    ret = EXIT_PARAMETER_ERROR;
  }
  else
  {
    if (line.length() > g_local_dir.length() + 1 &&
        0 == memcmp(line.c_str(), g_local_dir.c_str(), g_local_dir.length()) &&
        '/' == line.at(g_local_dir.length()))// prefix must be g_local_dir
    {
      char filename[1024] = {'\0'};
      char prefix[32]={'\0'};
      if (g_need_private)
      {
        strcpy(prefix, "/tfsprivate");
      }
      snprintf(filename, sizeof(filename), "%s%s", prefix, line.c_str() + g_local_dir.length());
      TBSYS_LOG(INFO, "self-defined filename: %s, local_file: %s\n", filename, line.c_str());
      // calcalatue uid by hashing whole filename
      int64_t hash_value = tbsys::CStringUtil::murMurHash((const void*)(filename), strlen(filename));
      int64_t uid = (hash_value % g_hash_count + 1);
      ret = transfer_file(line.c_str(), filename, uid);
      if (TFS_SUCCESS != ret && EXIT_TARGET_EXIST_ERROR != ret) // delete transfer fail file
      {
        remove_fail_file(filename, uid);
      }
    }
    else
    {
      TBSYS_LOG(WARN, "local dir(%s) or local_file(%s) invalid\n", g_local_dir.c_str(), line.c_str());
      ret = EXIT_PARAMETER_ERROR;
    }
  }
  return ret;
}

int TransferFileManger::begin()
{
  int ret = TFS_SUCCESS;
  g_tfs_client = new RestClient();
  assert(NULL != g_tfs_client);
  string rs_addr = get_src_addr();
  g_app_key = get_extra_arg();
  g_need_private = get_force();
  g_local_dir = get_dest_addr();
  if (g_local_dir.length() > 0 && '/' == g_local_dir.at(g_local_dir.length()-1))
  {
    ret = EXIT_PARAMETER_ERROR;
    fprintf(stderr, "local files dir(%s) is invalid. if not null, it must like '/Dir1/..../Subdir', don't trailing with '/'\n", g_local_dir.c_str());
  }
  else if (NULL == rs_addr.c_str() || NULL == g_app_key.c_str())
  {
    tbsys::gDelete(g_tfs_client);
    ret = EXIT_PARAMETER_ERROR;
    fprintf(stderr, "rs_addr and g_app_key can't be NULL\n");
  }
  else
  {
    ret = g_tfs_client->initialize(rs_addr.c_str(), g_app_key.c_str(), "INFO");
    if (TFS_SUCCESS != ret)
    {
      tbsys::gDelete(g_tfs_client);
      fprintf(stderr, "tfs_client initialize ret: %d\n", ret);
    }
    else
    {
      // 获取g_app_id
      g_app_id = g_tfs_client->get_app_id();
    }
  }
  return ret;
}


void TransferFileManger::end()
{
  if (NULL != g_tfs_client)
  {
    tbsys::gDelete(g_tfs_client);
  }

  printf("succ and fail fail output path: %s/data, all success only if succ file equal withc input file\n", get_output_dir().c_str());
  printf("log output path: %s/logs\n", get_output_dir().c_str());
}

BaseWorker* TransferFileManger::create_worker()
{
  BaseWorker* work = new TransferFileWorker(this);
  assert(NULL != work);
  return work;
}

int main(int argc, char* argv[])
{
  TransferFileManger work_manager;
  return work_manager.main(argc, argv);
}


