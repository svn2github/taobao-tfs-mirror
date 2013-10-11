/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_batch_mix.cpp 575 2011-07-18 06:51:44Z daoan@taobao.com $
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *      - initial release
 *
 */
#include <vector>
#include <algorithm>
#include <functional>
#include <Memory.hpp>
#include "common/internal.h"
#include "common/func.h"
#include "clientv2/tfs_client_impl_v2.h"
#include "util.h"
#include "thread.h"

using namespace KFS;
using namespace tfs::common;
using namespace tfs::clientv2;
using namespace std;

int write_file(ThreadParam& param, TfsClientImplV2* tfsclient, const char* tfs_name, vector<std::string>& file_list,
    TimeConsumed& write_time_consumed, Stater& write_stater, const char* data, vector<std::string>& read_file_list, const bool update)
{
  ++write_time_consumed.total_count_;
  bool random = param.max_size_ > param.min_size_;
  uint32_t write_size = param.max_size_;
  Timer timer;
  timer.start();
  int32_t fd = tfsclient->open(tfs_name, NULL, NULL, T_WRITE);
  int32_t ret = fd > 0 ? TFS_SUCCESS : fd;
  if (TFS_SUCCESS == ret)
  {
    if (random)
    {
      struct timeval val;
      gettimeofday(&val, NULL);
      srand(val.tv_usec);
      write_size = rand() % (param.max_size_ - param.min_size_) + param.min_size_;
    }
    param.file_size_ += write_size;
    ret = write_data(tfsclient, fd, const_cast<char*>(data), write_size);
    ret = ret > 0 ? TFS_SUCCESS : ret;
  }

  char ret_name[FILE_NAME_LEN_V2+1];
  if (fd > 0)
  {
    int32_t result = tfsclient->close(fd, ret_name, FILE_NAME_LEN_V2+1);
    ret = TFS_SUCCESS == ret ? result : ret;
    if (TFS_SUCCESS == ret)
    {
      file_list.push_back(ret_name);
      if (read_file_list.size() < 0xffff && !update)
        read_file_list.push_back(ret_name);
    }
  }

  if (TFS_SUCCESS != ret)
  {
    ++write_time_consumed.fail_count_;
  }

  write_time_consumed.time_consumed_ = timer.consume();
  write_stater.stat_time_count(write_time_consumed.time_consumed_);
  write_time_consumed.process();
  return ret;
}

int read_file(TfsClientImplV2* tfsclient, const char* tfsname)
{
  int32_t ret = (static_cast<int32_t> (strlen(tfsname)) < FILE_NAME_LEN || (tfsname[0] != 'T' && tfsname[0] != 'L')) ? TFS_ERROR : TFS_SUCCESS;
  if (TFS_SUCCESS == ret)
  {
    int32_t fd = tfsclient->open(tfsname, NULL, NULL, T_READ);
    ret = (fd > 0) ? TFS_SUCCESS : fd;
    if (TFS_SUCCESS == ret)
    {
      uint32_t crc = 0;
      TfsFileStat stat;
      memset(&stat, 0, sizeof(stat));
      int32_t length = 0, total = 0, read_length = 0;
      char data[MAX_READ_SIZE];
      do
      {
        read_length = stat.size_ <= 0 ? MAX_READ_SIZE : stat.size_ - total;
        read_length = std::min(MAX_READ_SIZE, read_length);
        length = tfsclient->readv2(fd, data, read_length, &stat);
        ret = length >= 0 ? TFS_SUCCESS : length;
        if (TFS_SUCCESS == ret && length > 0)
        {
          total += length;
          crc = Func::crc(crc, (data), (length));
        }
      }
      while (total < stat.size_  && TFS_SUCCESS == ret);
      tfsclient->close(fd);
      if (crc != stat.crc_)
        TBSYS_LOG(INFO, " crc error. filename : %s, crc: %u<> %u", tfsname, crc, stat.crc_);
      ret = (total == stat.size_ && crc == stat.crc_ )? TFS_SUCCESS : TFS_ERROR;
    }
  }
  return ret;
}

void* mix_worker(void* arg)
{
  ThreadParam param = *(ThreadParam*) (arg);
  int32_t read_ratio = 200, write_ratio = 8, delete_ratio = 2, update_ratio = 1;
  std::vector<string> vec;
  Func::split_string(param.oper_ratio_.c_str(), ':', vec);
  assert(vec.size() == 4U);
  read_ratio = atoi((vec[0]).c_str());
  write_ratio = atoi((vec[1]).c_str());
  delete_ratio = atoi((vec[2]).c_str());
  update_ratio = atoi((vec[3]).c_str());
  int32_t PER_READ = 100;
  int32_t PER_WRITE = PER_READ * write_ratio / read_ratio;
  int32_t PER_DELETE = PER_READ * delete_ratio / read_ratio;
  int32_t PER_UPDATE = PER_READ * update_ratio / read_ratio;

  printf(
      "read_ratio: %d, write_ratio: %d,deleteratio: %d,updateratio: %d, perread: %d, perwrite: %d, perdelete: %d, per_update: %d\n",
      read_ratio, write_ratio, delete_ratio, update_ratio, PER_READ, PER_WRITE, PER_DELETE, PER_UPDATE);
  printf("max_size = %d, min_size = %d, random: %d, count = %d\n", param.max_size_, param.min_size_, param.random_,
      param.file_count_);
  vector<std::string> update_list, delete_list;
  vector<std::string> read_file_list, write_file_list;
  printf("init connection to nameserver:%s\n", param.ns_ip_port_.c_str());
  TfsClientImplV2 *tfsclient = TfsClientImplV2::Instance();
  int64_t time_start = Func::curr_time();
  Timer timer;
  Stater read_stater("read"), write_stater("write"), delete_stater("delete"), update_stater("update");
  TimeConsumed read_time_consumed("read"), write_time_consumed("write"), delete_time_consumed("delete"),
      update_time_consumed("update");
  int32_t loop_times = param.file_count_ / (PER_READ + PER_DELETE + PER_UPDATE);
  int32_t loop_times_bak = loop_times;
	int ret = tfsclient->initialize(param.ns_ip_port_.c_str());
  if (TFS_SUCCESS == ret)
  {
    tfsclient->set_log_level(param.log_level_.c_str());
    //for write
    char* data = new char[param.max_size_];
    memset(data, 0, param.max_size_);
    generate_data(data, param.max_size_);
    TBSYS_LOG(DEBUG, "GEN data %s", data);

    vector<std::string>::iterator iter;
    do
    {
      int32_t index = 0, random_index;
      for (index = 0; index < PER_WRITE; ++index)
      {
        write_file(param, tfsclient, NULL, write_file_list, write_time_consumed, write_stater, data, read_file_list, false);
      }

      for (index = 0; index < PER_READ && !read_file_list.empty(); ++index)
      {
        timer.start();
        ++read_time_consumed.total_count_;
        random_index = random() % read_file_list.size();
        std::string file_name = read_file_list[random_index];
        ret = read_file(tfsclient, file_name.c_str());
        if (TFS_SUCCESS != ret)
        {
          ++read_time_consumed.fail_count_;
        }
        else
        {
          read_time_consumed.time_consumed_ = timer.consume();
          read_stater.stat_time_count(read_time_consumed.time_consumed_);
          read_time_consumed.process();
        }
      }

      for (index = 0; index < PER_UPDATE && !read_file_list.empty(); ++index)
      {
        random_index = random() % read_file_list.size();
        iter = read_file_list.begin() + random_index;
        std::string file_name = (*iter);
        read_file_list.erase(iter);
        ret = write_file(param, tfsclient, file_name.c_str(), write_file_list, update_time_consumed,
          update_stater, data, read_file_list, true);
      }

      for (index = 0; index < PER_DELETE && !read_file_list.empty(); ++index)
      {
        ++delete_time_consumed.total_count_;
        random_index = random() % read_file_list.size();
        iter = read_file_list.begin() + random_index;
        std::string file_name = (*iter);
        read_file_list.erase(iter);
        timer.start();
        int64_t file_size;
        ret = tfsclient->unlink(file_size, file_name.c_str(), NULL);
        if (TFS_SUCCESS == ret)
        {
          delete_time_consumed.time_consumed_ = timer.consume();
          delete_stater.stat_time_count(delete_time_consumed.time_consumed_);
          delete_time_consumed.process();
        }
        else
        {
          ++delete_time_consumed.fail_count_;
        }
      }
    }
    while (loop_times-- > 0);
    tbsys::gDeleteA(data);
  }

  int64_t time_stop = Func::curr_time();
  uint64_t time_consumed = time_stop - time_start;

  uint32_t total_succ_count = read_time_consumed.succ_count() + write_time_consumed.succ_count()
      + delete_time_consumed.succ_count() + update_time_consumed.succ_count();
  uint32_t total_fail_count = read_time_consumed.fail_count_ + write_time_consumed.fail_count_
      + delete_time_consumed.fail_count_ + update_time_consumed.fail_count_;

  ((ThreadParam*) arg)->file_count_ = total_succ_count + total_fail_count;
  ((ThreadParam*) arg)->succ_count_ = total_succ_count;
  ((ThreadParam*) arg)->succ_time_consumed_ = read_time_consumed.accumlate_time_consumed_
      + write_time_consumed.accumlate_time_consumed_ + delete_time_consumed.accumlate_time_consumed_
      + update_time_consumed.accumlate_time_consumed_;
  ((ThreadParam*) arg)->fail_time_consumed_ = time_consumed - ((ThreadParam*) arg)->succ_time_consumed_;
  ((ThreadParam*) arg)->file_size_ = param.file_size_;

  double iops = static_cast<double> (total_succ_count) / (static_cast<double> (time_consumed) / 1000000);
  double rate = static_cast<double> (time_consumed) / total_succ_count;
  double aiops = static_cast<double> (total_succ_count)
      / (static_cast<double> (((ThreadParam*) arg)->succ_time_consumed_) / 1000000);
  //double arate = static_cast<double> (((ThreadParam*) arg)->succ_time_consumed_) / total_succ_count;

  uint32_t oper_total_count = loop_times_bak * (PER_READ + PER_WRITE + PER_UPDATE + PER_DELETE);
  printf("\nINDEX | COUNT  | SUCC   | FAIL   | IOPS   | RATE   | AIOPS  | ARATE\n");
  printf("%-5d | %-6d | %-6d | %-6d | %-6f | %-6f | %-6f | %-6f\n\n", param.index_, oper_total_count, total_succ_count,
      total_fail_count, iops, rate, aiops, aiops);
  printf("------ | --------- | --------- | --------- | --------- | ---------\n");
  printf("TYPE   | SUCCCOUNT | FAILCOUNT | AVG       | MIN       | MAX      \n");
  printf("------ | --------- | --------- | --------- | --------- | ---------\n");
  read_time_consumed.display();
  write_time_consumed.display();
  update_time_consumed.display();
  delete_time_consumed.display();
  printf("------ | --------- | --------- | --------- | --------- | ---------\n\n\n");
  read_stater.dump_time_stat();
  write_stater.dump_time_stat();
  update_stater.dump_time_stat();
  delete_stater.dump_time_stat();
  return NULL;
}

int main(int argc, char* argv[])
{
  int32_t thread_count = 1;
  ThreadParam input_param;
  int ret = fetch_input_opt(argc, argv, input_param, thread_count);
  if (ret != TFS_SUCCESS || input_param.ns_ip_port_.empty() || thread_count > THREAD_SIZE)
  {
    printf("usage: -d ip:port -c count -t thread_count -o ratio(read:write:delete:update) -s random(0/1) -r range(low:high Bytes)\n");
    exit(-1);
  }

  MetaThread threads[THREAD_SIZE];
  ThreadParam param[THREAD_SIZE];
  tbsys::CThreadMutex glocker;

  int64_t time_start = Func::curr_time();
  for (int32_t i = 0; i < thread_count; ++i)
  {
    param[i].index_ = i;
    param[i].conf_ = input_param.conf_;
    param[i].file_count_ = input_param.file_count_;
    param[i].file_size_ = input_param.file_size_;
    param[i].profile_ = input_param.profile_;
    param[i].random_ = input_param.random_;
    param[i].oper_ratio_ = input_param.oper_ratio_;
    param[i].min_size_ = input_param.min_size_;
    param[i].max_size_ = input_param.max_size_;
    param[i].locker_ = &glocker;
    param[i].ns_ip_port_ = input_param.ns_ip_port_;
    param[i].log_level_ = input_param.log_level_;
    threads[i].start(mix_worker, (void*) &param[i]);
  }

  for (int32_t i = 0; i < thread_count; ++i)
  {
    threads[i].join();
  }

  int32_t total_count = 0;
  int32_t total_succ_count = 0;
  int32_t total_fail_count = 0;
  int64_t total_succ_consumed = 0;
  int64_t total_fail_consumed = 0;
  int64_t total_file_size = 0;
  for (int32_t i = 0; i < thread_count; ++i)
  {
    total_count += param[i].file_count_;
    total_file_size += param[i].file_size_;
    total_succ_count += param[i].succ_count_;
    total_succ_consumed += param[i].succ_time_consumed_;
    total_fail_consumed += param[i].fail_time_consumed_;
  }

  total_fail_count = total_count - total_succ_count;

  int64_t time_stop = Func::curr_time();
  int64_t time_consumed = time_stop - time_start;

  double iops = calc_iops(total_count, time_consumed);
  double siops = calc_iops(total_succ_count, time_consumed);
  double fiops = calc_iops(total_fail_count, time_consumed);
  double srate = calc_rate(total_succ_count, total_succ_consumed);
  double frate = calc_rate(total_fail_count, total_fail_consumed);
  double asize = total_count > 0 ? total_file_size / total_count : 0;
  printf("thread_num count filesize     iops    siops     fiops     srate       frate\n");
  printf("%10d  %4d %5.2f  %8.3f %8.3f %8.3f %10.3f  %10.3f\n", thread_count, total_count, asize, iops, siops, fiops,
      srate, frate);
  return 0;
}
