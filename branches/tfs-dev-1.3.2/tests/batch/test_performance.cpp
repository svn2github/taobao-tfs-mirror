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
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#include <vector>
#include <algorithm>
#include <functional>
#include <Mutex.h>
#include <Timer.h>
#include <Handle.h>
#include <sys/types.h>
#include <pthread.h>
#include <Memory.hpp>
#include <curses.h>
#include "common/func.h"
#include "common/error_msg.h"
#include "client/tfs_client_api.h"
#include "util.h"
#include "thread.h"

using namespace KFS;
using namespace tfs::common;
using namespace tfs::client;

#define FILE_NAME_LEN 18
struct StatParam
{
  int64_t write_success_count_;
  int64_t write_fail_count_;
  int64_t write_response_;
  int64_t read_success_count_;
  int64_t read_fail_count_;
  int64_t read_response_;
  StatParam()
  {
    memset(this, 0, sizeof(StatParam));
  }
};

tbutil::Mutex mutex_;
StatParam gstat_;
StatParam gtotal_stat_;
static FILE* gdump_file = NULL;
static const int32_t READ_MAX_SIZE = 32;

enum ModeType
{
  READ_TYPE = 0x01,
  WRITE_TYPE = 0x02,
  MIX_TYPE = 0x04
};

class StatTimerTask : public tbutil::TimerTask
{
  public:
    StatTimerTask(int32_t interval):
      interval_(interval)
  {

  }
    virtual ~StatTimerTask(){}
    virtual void runTimerTask()
    {
      tbutil::Mutex::Lock lock(mutex_);
      TBSYS_LOG(INFO, "[read] success: %"PRI64_PREFIX"d fail: %"PRI64_PREFIX"d, [write] success: %"PRI64_PREFIX"d fail : %"PRI64_PREFIX"d",
          (gstat_.read_success_count_/ interval_) * 1000,
          (gstat_.read_fail_count_/ interval_) *1000, 
          (gstat_.write_success_count_/ interval_) * 1000,
          (gstat_.write_fail_count_/ interval_) * 1000); 
      TBSYS_LOG(DEBUG, "gstat_.read_success_count_(%ld), gstat_.read_success_count_(%ld)", gstat_.read_success_count_, gstat_.write_success_count_);
      gtotal_stat_.read_success_count_ += gstat_.read_success_count_;
      gtotal_stat_.write_success_count_ += gstat_.write_success_count_;
      gtotal_stat_.read_fail_count_ += gstat_.read_fail_count_;
      gtotal_stat_.write_fail_count_ += gstat_.write_fail_count_;
      memset(&gstat_, 0, sizeof(gstat_));
    }
  private:
    DISALLOW_COPY_AND_ASSIGN( StatTimerTask);
    int32_t interval_;
};
typedef tbutil::Handle<StatTimerTask> StatTimerTaskPtr;

/*int get_read_list(std::string& file_name, int32_t index, vector<std::string>& list)
{
  FILE* input_fd = fopen(file_name.c_str(), "rb");
  if (NULL == input_fd)
  {
    printf("open read file(%s) failed, exit\n", file_name.c_str());
    return TFS_ERROR;
  }
  const int32_t BUFLEN = 32;
  char name_buf[BUFLEN];
  while (fgets(name_buf, BUFLEN, input_fd))
  {
    if(count % thread == index)
    {
      name_buf[FILE_NAME_LEN] = '\0';
      list.push_back(static_cast<std::string> (name_buf));
    }
  }
  fclose(input_fd);
  return TFS_SUCCESS;
}*/

int read_file(TfsClient& tfs_file, char* tfs_name, StatParam& stat_param)
{
  char *prefix = NULL;
  if (static_cast<int32_t> (strlen(tfs_name)) < FILE_NAME_LEN || tfs_name[0] != 'T')
  {
    fprintf(stderr, "tfs file name is not valid.%s\n", tfs_name);
    return EXIT_FAILURE;
  }
  prefix = tfs_name + FILE_NAME_LEN;

  Timer timer;
  timer.start();

  if (tfs_file.tfs_open(tfs_name, prefix, READ_MODE) != EXIT_SUCCESS)
  {
    fprintf(stderr, "open tfsfile fail: %s\n", tfs_file.get_error_message());
    stat_param.read_fail_count_++;
    return EXIT_FAILURE;
  }

  FileInfo finfo;
  if (tfs_file.tfs_stat(&finfo) == EXIT_FAILURE)
  {
    tfs_file.tfs_close();
    fprintf(stderr, "fstat tfsfile fail: %s\n", tfs_file.get_error_message());
    stat_param.read_fail_count_++;
    return EXIT_FAILURE;
  }

  char data[READ_MAX_SIZE];
  int total_size = 0;
  int rlen = 0;
  for (;;)
  {
    rlen = tfs_file.tfs_read(data, READ_MAX_SIZE);
    if (rlen < 0)
    {
      fprintf(stderr, "read tfsfile fail: %s\n", tfs_file.get_error_message());
      tfs_file.tfs_close();
      stat_param.read_fail_count_++;
      return EXIT_FAILURE;
    }
    if (rlen == 0)
    {
      break;
    }
    total_size += rlen;
    //if (rlen != READ_MAX_SIZE)
      break;
  }
  if (total_size == 0)
  {
    tfs_file.tfs_close();
    stat_param.read_fail_count_++;
    fprintf(stderr, "read tfsfile fail(%s), total len(%d)\n", tfs_file.get_error_message(), total_size);
    return EXIT_FAILURE;
  }

  tfs_file.tfs_close();
  stat_param.read_success_count_++;
  stat_param.read_response_ = timer.consume();
  return EXIT_SUCCESS;
}
int write_file(TfsClient& tfs_file, const char* data, int32_t length ,StatParam& stat_param)
{
  uint32_t block_id = 0;
  uint64_t file_id = 0;

  Timer timer;
  timer.start();

  int ret = retry_open_file(&tfs_file, NULL, NULL,WRITE_MODE);
  convname(tfs_file.get_file_name(), NULL, block_id, file_id);
  if (ret == TFS_ERROR)
  {
    fprintf(stderr, "tfsopen failed(%u), (%" PRI64_PREFIX "u), (%s), err(%s)\n", block_id, file_id,
        tfs_file.get_file_name(), tfs_file.get_error_message());
    stat_param.write_fail_count_++;
    return ret;
  }

  ret = write_data(&tfs_file, const_cast<char*>(data), length);
  if (ret < 0)
  {
    fprintf(stderr, "tfswrite failed(%u), (%" PRI64_PREFIX "u), (%s), err(%s)\n", block_id, file_id, tfs_file.get_file_name(), tfs_file.get_error_message());
    tfs_file.tfs_close();
    stat_param.write_fail_count_++;
    return ret;
  }
  else
  {
    ret = tfs_file.tfs_close();
    if (ret == TFS_SUCCESS)
    {
      stat_param.write_success_count_++;
      stat_param.write_response_ = timer.consume();
    }
    else
    {
      fprintf(stderr, "tfs_close(%s) failed: err(%s)\n", tfs_file.get_file_name(), tfs_file.get_error_message());
      stat_param.write_fail_count_++;
      return ret;
    }
  }
  return TFS_SUCCESS;
}

class WorkThread : public tbutil::Thread
{
  public:
    WorkThread();
    WorkThread(std::string& ip, int8_t mode, int32_t loop_max, int32_t read_ratio, int32_t write_ratio, int32_t base_max_count, std::string& file_path):
      ns_ip_port_(ip),
      mode_(mode),
      file_path_(file_path),
      read_ratio_(read_ratio),
      write_ratio_(write_ratio),
      LOOP_MAX(loop_max),
      BASE_MAX_COUNT(base_max_count),
      destroy_(false)
  {
    memset(&stat_, 0, sizeof(stat_));
  }
    virtual ~WorkThread()
    {

    }

    void wait_for_shut_down()
    {
      join();
    }

    void destroy()
    {
      destroy_ = true;
    }

    void dump(FILE* file)
    {
      bool bret = file == NULL ? false : true;
      if (bret)
      {
        vector<std::string>::iterator iter = write_request_queue_.begin();
        const int32_t BUFLEN = 32;
        char name_buf[BUFLEN];
        for(; iter != write_request_queue_.end(); iter++)
        {
          strncpy(name_buf, (*iter).c_str(), FILE_NAME_LEN);
          name_buf[FILE_NAME_LEN] = '\n';
          name_buf[FILE_NAME_LEN + 1] = '\0';
          fwrite(name_buf, strlen(name_buf), 1, file);
        }
      }
    }

    virtual void run()
    {
      TfsClient tfsclient;
      int iret = tfsclient.initialize(ns_ip_port_, 0, 0);
      if (iret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "tfsclient initialize failed: %s", tfsclient.get_error_message());
        return ;
      }
      static const int32_t DATA_MAX_SIZE = 128;
      char data[ DATA_MAX_SIZE];
      generate_data(data, DATA_MAX_SIZE);
      int32_t loop = 0;
      const int32_t MAX_READ_COUNT = read_ratio_ *  BASE_MAX_COUNT;
      const int32_t MAX_WRITE_COUNT = write_ratio_ * BASE_MAX_COUNT;
      while(!destroy_)
      {
        if (mode_ & WRITE_TYPE)
        {
          if (write_file(tfsclient, data, DATA_MAX_SIZE, stat_) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "write file(%s) fail", file_path_.c_str());
          }
          else
          {
            write_request_queue_.push_back(tfsclient.get_file_name());
          }
          ++loop;
          if (loop >= LOOP_MAX)
          {
            loop = 0;
            {
              tbutil::Mutex::Lock lock(mutex_);
              gstat_.write_success_count_ += stat_.write_success_count_;
              gstat_.write_fail_count_ += stat_.write_fail_count_;
              gstat_.write_response_ += stat_.write_response_;
              memset(&stat_, 0, sizeof(stat_));
            }
            dump(gdump_file);
            write_request_queue_.clear();
          }
        }
        else if (mode_ & READ_TYPE)
        {
          int64_t queue_size = read_request_queue_.size();
          if(queue_size <= 0)
          {
            TBSYS_LOG(ERROR, "%s", "get file list fail");
            return;
          }
          srand(time(NULL));
          int32_t index = rand() % queue_size;
          std::string file_name = read_request_queue_[index];
          if (read_file(tfsclient, const_cast<char*> (file_name.c_str()), stat_) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "read file(%s) fail", file_name.c_str());
          }
          ++loop;
          if (loop >= LOOP_MAX)
          {
            loop = 0;
            {
              tbutil::Mutex::Lock lock(mutex_);
              gstat_.read_success_count_ += stat_.read_success_count_;
              gstat_.read_fail_count_ += stat_.read_fail_count_;
              gstat_.read_response_ += stat_.read_response_;
              memset(&stat_, 0, sizeof(stat_));
            }
          }
        }
        else if (mode_ & MIX_TYPE)
        {
          int32_t i = 0;
          while ( i < MAX_WRITE_COUNT )
          {
            if (write_file(tfsclient, data, DATA_MAX_SIZE, stat_) != TFS_SUCCESS)
            {
              TBSYS_LOG(ERROR, "write file(%s) fail", file_path_.c_str());
            }
            else
            {
              write_request_queue_.push_back(tfsclient.get_file_name());
            }
            ++loop;
            if (loop >= LOOP_MAX)
            {
              loop = 0;
              {
                tbutil::Mutex::Lock lock(mutex_);
                gstat_.write_success_count_ += stat_.write_success_count_;
                gstat_.write_fail_count_ += stat_.write_fail_count_;
                gstat_.write_response_ += stat_.write_response_;
                stat_.write_success_count_ = 0;
                stat_.write_fail_count_ = 0;
                stat_.write_response_ = 0;
              }
            }
            ++i;
          }

          {
             tbutil::Mutex::Lock lock(mutex_);
             gstat_.write_success_count_ += stat_.write_success_count_;
             gstat_.write_fail_count_ += stat_.write_fail_count_;
             gstat_.write_response_ += stat_.write_response_;
             stat_.write_success_count_ = 0;
             stat_.write_fail_count_ = 0;
             stat_.write_response_ = 0;
          }
          loop = 0;
          i = 0;

          while (i < MAX_READ_COUNT)
          {
            int64_t queue_size = write_request_queue_.size();
            srand(time(NULL));
            if(queue_size <= 0)
            {
              break;
            }
            int32_t index = rand() % queue_size;
            std::string file_name = write_request_queue_[index];
            if (read_file(tfsclient, const_cast<char*>(file_name.c_str()), stat_) != TFS_SUCCESS)
            {
              TBSYS_LOG(ERROR, "read file(%s) fail", file_name.c_str());
            }
            ++loop;
            if (loop >= LOOP_MAX)
            {
              loop = 0;
              {
                tbutil::Mutex::Lock lock(mutex_);
                gstat_.read_success_count_ += stat_.read_success_count_;
                gstat_.read_fail_count_ += stat_.read_fail_count_;
                gstat_.read_response_ += stat_.read_response_;
                stat_.read_success_count_ = 0;
                stat_.read_fail_count_ = 0;
                stat_.read_response_ = 0;
              }
            }
            ++i;
          }
          write_request_queue_.clear();
        }
      }
    }
    void push_back(const char* name)
    {
      read_request_queue_.push_back(name);
    }
  private:
    WorkThread(const WorkThread&);
    WorkThread& operator=(const WorkThread&);
    std::vector<std::string> read_request_queue_;
    std::vector<std::string> write_request_queue_;
    StatParam stat_;
    std::string ns_ip_port_;
    int8_t mode_;
    std::string file_path_;
    int32_t read_ratio_;
    int32_t write_ratio_;
    const int32_t LOOP_MAX;
    const int32_t BASE_MAX_COUNT;
    bool destroy_;
};
typedef tbutil::Handle<WorkThread> WorkThreadPtr;

void helper()
{
  std::string options=
    "Options:\n"
    "-s                 ip:port\n"
    "-t                 thread count\n"
    "-m                 mode. read:1, write:2, mix:4\n"
    "-o                 the ratio of write and read(read:write), default is 5:1\n"
    "-l                 loop times\n"
    "-b                 base file count\n"
    "-i                 the interval of print\n"
    "-f                 the filename of tfsname list\n"
    "-p                 pid file name\n"
    "-g                 log file name\n"
    "-d                 Run as a daemon\n"
    "-h                 Show this message\n"
    "-v                 Show porgram version\n";
  fprintf(stderr,"Usage:\n%s" ,options.c_str());
}

static int32_t thread_count = 1;
static WorkThreadPtr* gworks = NULL;

static void interruptcallback(int signal)
{
  TBSYS_LOG(INFO, "application signal[%d]", signal );
  switch( signal )
  {
    case SIGTERM:
    case SIGINT:
    default:
      if (gworks != NULL)
      {
        for (int32_t i = 0; i < thread_count; ++i)
        {
          if (gworks != 0)
          {
            gworks[i]->destroy();
          }
        }
      }
      break;
  }
}

int main(int argc, char* argv[])
{
  std::string ns_ip_port;
  std::string file_path;
  std::string ratio("5:1");
  std::string pid_file;
  std::string log_file;
  int32_t loop_max = 100;
  int32_t base_max_count = 100;
  int32_t interval = 1000;
  int32_t index = 0;
  int8_t mode  = 1;
  bool help = false;
  bool daemon = false;
  while ((index = getopt(argc, argv, "s:t:m:o:l:b:i:f:p:g:dvh")) != EOF)
  {
    switch (index)
    {   
      case 's':
        {   
          ns_ip_port = optarg;
          string::size_type tmppos = ns_ip_port.find_first_of(":");
          if (string::npos == tmppos)
          {   
            return EXIT_FAILURE;
          }   
        }   
        break;
      case 't':
        thread_count = atoi(optarg);
        break;
      case 'm':
        mode = atoi(optarg);
        break;
      case 'o':
        ratio = optarg;
        break;
      case 'b':
        base_max_count = atoi(optarg);
        break;
      case 'l':
        loop_max = atoi(optarg);
        break;
      case 'i':
        interval = atoi(optarg);
        break;
      case 'f':
        file_path = optarg;
        break;
      case 'd':
        daemon = true;
        break;
      case 'p':
        pid_file = optarg;
        break;
      case 'g':
        log_file = optarg;
        break;
      case 'v':
        break;
      case 'h':
      default:
        help = true;
        break;
    }
  }
  help = ns_ip_port.empty() || ratio.empty() || pid_file.empty()  || log_file.empty() || help || ((mode & WRITE_TYPE) && file_path.empty())
    || ((mode & READ_TYPE) && file_path.empty());
  if (help)
  {
    helper();
    return TFS_ERROR;
  }

  int32_t pid = 0;
  if ((pid = tbsys::CProcess::existPid(pid_file.c_str())))
  {
    fprintf(stderr, "%s", "test_performance is running\n");
    return EXIT_SYSTEM_ERROR;
  }

  pid = 0;
  if (daemon)
  {
    pid = tbsys::CProcess::startDaemon(pid_file.c_str(), log_file.c_str());
  }

  if (pid == 0)
  {
    signal(SIGPIPE, SIG_IGN);
    signal(SIGHUP, SIG_IGN);
    signal(SIGINT, SIG_IGN);                                                                   
    signal(SIGTERM, SIG_IGN);
    signal(SIGUSR1, SIG_IGN);
    int32_t read_ratio = 5;
    int32_t write_ratio = 1;
    std::string::size_type pos = ratio.find_first_of(":");
    if (std::string::npos != pos)
    {
      read_ratio = atoi(ratio.substr(0, pos).c_str());
      if (0 == read_ratio)
      {
        TBSYS_LOG(ERROR, " parse ratio(%s) fail..", ratio.c_str());
        return TFS_ERROR;
      }
      write_ratio = atoi(ratio.substr(pos + 1, ratio.length()).c_str());
    }

    tbutil::TimerPtr timer = new tbutil::Timer();
    StatTimerTaskPtr stat_timer_task_ = new StatTimerTask(interval);
    int ret = timer->scheduleRepeated(stat_timer_task_, tbutil::Time::milliSeconds(interval));
    if (ret < 0)
    {
      TBSYS_LOG(ERROR, "%s", "timer scheduleRepeated fail..");
      return TFS_ERROR;
    }

    gworks = new WorkThreadPtr[thread_count];
    int32_t i = 0;
    for (; i < thread_count; ++i)
    {
      gworks[i] = new WorkThread(ns_ip_port, mode, loop_max, read_ratio, write_ratio, base_max_count, file_path);
    }

    FILE* file = fopen(file_path.c_str(), "rb");
    if (file == NULL)
    {
      fprintf(stderr, "open read file(%s) failed, exit\n", file_path.c_str());
      timer->cancel(stat_timer_task_);
      timer->destroy();
      tbsys::gDeleteA(gworks);
      return TFS_ERROR;
    }
    else
    {
      int32_t index = 0;
      int64_t count = 0;
      const int32_t BUF_LEN = 32;
      char name[BUF_LEN] = {'\0'};
      while (fgets(name, BUF_LEN, file))
      {
        index = count % thread_count;
        gworks[index]->push_back(name);
        ++count;
      }

      fclose(file);
      for (i = 0; i < thread_count; ++i)
      {
        gworks[i]->start();
      }
    }

    signal(SIGHUP, interruptcallback);
    signal(SIGINT, interruptcallback);                                                           
    signal(SIGTERM, interruptcallback);
    signal(SIGUSR1, interruptcallback);

    gdump_file = fopen(file_path.c_str(), "w+");
    if (gdump_file == NULL)
    {
      fprintf(stderr, "open read file(%s) failed, exit\n", file_path.c_str());
      for (i = 0; i < thread_count; ++i)
      {
        gworks[i]->destroy();
      }
    }

    for (i = 0; i < thread_count; ++i)
    {
      gworks[i]->wait_for_shut_down();
      gworks[i]->dump(gdump_file);
    }

    timer->cancel(stat_timer_task_);
    timer->destroy();

    tbsys::gDeleteA(gworks);

    TBSYS_LOG(INFO, "[read] success: %"PRI64_PREFIX"d fail: %"PRI64_PREFIX"d, [write] success: %"PRI64_PREFIX"d fail : %"PRI64_PREFIX"d",
          gtotal_stat_.read_success_count_,
          gtotal_stat_.read_fail_count_, 
          gtotal_stat_.write_success_count_,
          gtotal_stat_.write_fail_count_); 
    if (gdump_file != NULL)
    {
      fclose(gdump_file);
    }
    unlink(pid_file.c_str());
  }
  return TFS_SUCCESS;
}
