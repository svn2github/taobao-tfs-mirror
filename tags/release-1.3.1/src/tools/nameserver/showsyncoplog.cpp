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
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include <pthread.h>
#include <vector>
#include <string>
#include <queue>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "tbsys.h"
#include "common/func.h"
#include "common/file_queue.h"
#include "nameserver/oplog.h"

using namespace tfs::common;
using namespace tfs::nameserver;

int32_t global_stop = 0x00;

void signal_handler(int32_t signal)
{
  switch (signal)
  {
  case SIGINT:
    global_stop = 0x01;
    break;
  default:
    fprintf(stderr, "[INFO]: occur signal(%d)", signal);
    break;
  }
}

int print_information(std::string& dir_name, int32_t type = 0x00)
{
  OpLogRotateHeader header;
  ::memset(&header, 0, sizeof(header));
  std::string head_path = dir_name + "/rotateheader.dat";
  int fd = open(head_path.c_str(), O_RDWR, 0600);
  if (fd < 0)
  {
    fprintf(stderr, "[ERROR]: open file(%s) failed(%s)\n", head_path.c_str(), strerror(errno));
    return TFS_ERROR;
  }
  int ret = read(fd, &header, sizeof(header));
  if (ret != sizeof(header))
  {
    fprintf(stderr, "[ERROR]: read file(%s) failed\n", head_path.c_str());
    ::close(fd);
    return TFS_ERROR;
  }
  fprintf(stderr, "--------------------------------------------------------------\n");
  fprintf(stderr, "        current rotate information           \n\n");
  fprintf(stderr, "RotateSeqno:%d  RotateOffset: %d \n\n", header.rotate_seqno_, header.rotate_offset_);
  ::close(fd);

  std::string file_queue_path= dir_name + "/header.dat";
  int q_fd = open(file_queue_path.c_str(), O_RDWR, 0600);
  if (q_fd < 0)
  {
    fprintf(stderr, "%s:%d [ERROR]: open file(%s) failed(%s)\n", __FILE__, __LINE__, file_queue_path.c_str(), strerror(
        errno));
    return TFS_ERROR;
  }
  QueueInformationHeader q_head;
  ::memset(&q_head, 0, sizeof(q_head));
  if (read(q_fd, &q_head, sizeof(q_head)) != sizeof(q_head))
  {
    fprintf(stderr, "%s:%d [ERROR]: read file(%s) failed\n", __FILE__, __LINE__, file_queue_path.c_str());
    ::close(q_fd);
    return TFS_ERROR;
  }
  fprintf(stderr, "        current synchronization oplog information          \n\n");
  fprintf(stderr, "ReadSeqno:  %d     ReadOffset: %d  \n", q_head.read_seqno_, q_head.read_offset_);
  fprintf(stderr, "WriteSeqno: %d     WriteFileSize: %d  \n", q_head.write_seqno_, q_head.write_filesize_);
  fprintf(stderr, "QueueSize: %d\n", q_head.queue_size_);
  if (type == 0x00)
  {
    ostringstream current;
    int i = 0;
    for (i = 0; i < FILE_QUEUE_MAX_THREAD_SIZE; i++)
    {
      current << "     " << i << "              " << q_head.pos_[i].seqno_ << "         " << q_head.pos_[i].offset_ << "\n";
    }
    fprintf(stderr, "Current Process: \n");
    fprintf(stderr, "Thread Number     SeqNo     Offset \n");
    fprintf(stderr, "%s\n", current.str().c_str());
  }
  ::close(q_fd);
  return TFS_SUCCESS;
}

int main(int argc, char *argv[])
{
  signal(SIGINT, signal_handler);
  std::string dir_name;
  int i = 0;
  int32_t count = 0;
  int32_t type = 0x00;
  int32_t interval = 0x01;
  while ((i = getopt(argc, argv, "f:i:c:t:h")) != EOF)
  {
    switch (i)
    {
    case 'f':
      dir_name = optarg;
      break;
    case 'c':
      count = atoi(optarg);
      break;
    case 'i':
      interval = atoi(optarg);
      break;
    case 't':
      type = atoi(optarg);
      break;
    case 'h':
    default:
      fprintf(stderr, "Usage: %s -f fileQueueDirPath -t type -c count -i interval\n", argv[0]);
      fprintf(stderr, "       -f fileQueueDirPath: File Queue directory\n");
      fprintf(stderr, "       -t type : 0: all 1: not print currnet process information\n");
      fprintf(stderr, "       -c count: count\n");
      fprintf(stderr, "       -i interval: interval time\n");
      fprintf(stderr, "       -h help\n");
      return TFS_SUCCESS;
    }
  }
  if (dir_name.empty())
  {
    fprintf(stderr, "Usage: %s -f fileQueueDirPath -t type -c count -i interval\n", argv[0]);
    fprintf(stderr, "       -f fileQueueDirPath: File Queue directory\n");
    fprintf(stderr, "       -t type : 0: all 1: not print currnt process information\n");
    fprintf(stderr, "       -c count: count\n");
    fprintf(stderr, "       -i interval: interval time\n");
    return TFS_SUCCESS;
  }
  struct stat stat_buf;
  if (stat(dir_name.c_str(), &stat_buf) != 0)
  {
    fprintf(stderr, "%s:%d [ERROR]: (%s%s)\n", __FILE__, __LINE__, dir_name.c_str(), strerror(errno));
    return TFS_SUCCESS;
  }
  if (!S_ISDIR(stat_buf.st_mode))
  {
    fprintf(stderr, "%s:%d [ERROR]: (%s) not directory\n", __FILE__, __LINE__, dir_name.c_str());
    return TFS_SUCCESS;
  }

  int ret = 0;
  for (i = 0; (i < count || count == 0); i++)
  {
    ret = print_information(dir_name, type);
    if ((ret == TFS_ERROR) || ((count == i + 1) && (count != 0)) || (global_stop == 0x01))
      break;
    Func::sleep(interval, &global_stop);
  }
  return TFS_SUCCESS;
}

