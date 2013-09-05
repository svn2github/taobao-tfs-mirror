/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_by_blk.cpp 2312 2013-06-13 08:46:08Z duanfei $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */

#include "tools/util/util.h"
#include "tools/util/tool_util.h"
#include "tools/util/base_worker.h"
#include "sync_file_base_v2.h"
#include "clientv2/tfs_client_impl_v2.h"
#include "clientv2/fsname.h"
#include "common/func.h"
#include "message/read_index_message_v2.h"
#include "message/server_status_message.h"
#include "message/block_info_message_v2.h"
#include "message/read_data_message_v2.h"
#include "common/status_message.h"
#include "common/error_msg.h"
#include "common/client_manager.h"
#include "common/new_client.h"

#include <sys/types.h>
#include <sys/syscall.h>

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::clientv2;
using namespace tfs::tools;

const int64_t TRAN_BUFFER_SIZE = 8 * 1024 * 1024;
const int64_t RETRY_TIMES = 2;

class SyncByBlockManger;
class SyncByBlockWorker: public BaseWorker
{
  public:
    SyncByBlockWorker(SyncByBlockManger* manager) : manager_(manager), block_id_(INVALID_BLOCK_ID) {}
    ~SyncByBlockWorker(){}

    virtual int process(string& line);
    int sync_block_by_block();
    int sync_block_by_file(vector<FileInfoV2>& src_finfos, vector<FileInfoV2>& dest_finfos);
    int get_file_infos(const string& ns_addr, const uint64_t block_id_, vector<FileInfoV2> &finfos);
    SyncByBlockManger* get_manager() { return manager_; }

  private:
    int get_src_one_ds(uint64_t& src_ds_id);
    int read_index(const uint64_t src_ds_id);
    int read_data(const uint64_t src_ds_id);
    void write_block();
    int write_file(const string& file_name, char* data, int32_t size, int32_t status);
    SyncByBlockManger* manager_;
    uint64_t block_id_;
    FILE_INFO_LIST_V2 src_file_list_;
    tbnet::DataBuffer src_data_buf_;
};


class SyncByBlockManger: public BaseWorkerManager
{
  public:
    SyncByBlockManger(bool force = false, bool unlink = false, bool need_remove_file = false, int64_t traffic_threshold = 1024)
      : force_(force), unlink_(unlink), need_remove_file_(need_remove_file), traffic_threshold_(traffic_threshold * 1024),
        log_file_succ_("sync_succ_file"), log_file_fail_("sync_fail_file"), log_file_unsync_("sync_unsync_file")
    {
    }
    ~SyncByBlockManger(){}

    virtual int begin();
    virtual void end();
    void write_log_file(const string& tfs_file_name, const SyncResult& result);
    virtual BaseWorker* create_worker();

    bool need_force_sync() { return force_; }
    bool need_unlink_sync() { return unlink_; }
    bool need_remove_redundant_file() { return need_remove_file_; }
    int64_t get_traffic_threshold() { return traffic_threshold_; }

  private:
    void destroy_log_file(LogFile& log_file);
    virtual void usage(const char* app_name);

    bool force_;
    bool unlink_;
    bool need_remove_file_;
    int64_t traffic_threshold_;
    SyncStat sync_stat_;//记录各种操作结果的数目信息
    LogFile log_file_succ_;//记录同步成功的文件名
    LogFile log_file_fail_;
    LogFile log_file_unsync_;
    tbutil::Mutex mutex_;
};


void SyncByBlockManger::usage(const char* app_name)
{
  char *options =
    "-s           source server ip:port\n"
    "-d           dest server ip:port, optional\n"
    "-f           input block id\n"
    "-m           timestamp eg: 20130610, optional, default next day 0'clock\n"
    "-i           sleep interval (ms), optional, default 0\n"
    "-e           force flag, need strong consistency(crc), optional\n"
    "-r           unlink flag, need sync unlink(DELETE) file to dest cluster, optional\n"
    "-u           flag, need delete redundent file in dest cluster, optional\n"
    "-w           traffic threshold per thread, default 1024(kB/s)\n"
    "-t           thread count, optional, defaul 1\n"
    "-l           log level, optional, default info\n"
    "-o           output directory, optional, default ./output\n"
    "-v           print version information\n"
    "-h           print help information\n"
    "signal       SIGUSR1 inc sleep interval 1000ms\n"
    "             SIGUSR2 dec sleep interval 1000ms\n";
  fprintf(stderr, "%s usage:\n%s", app_name, options);
  exit(-1);
}


void SyncByBlockManger::destroy_log_file(LogFile& log_file)
{
  if (NULL != log_file.fp_)
  {
    fclose(log_file.fp_);
    log_file.fp_ = NULL;
  }
}

void SyncByBlockManger::write_log_file(const string& tfs_file_name, const SyncResult& result)
{
  if ( !tfs_file_name.empty() )
  {
    tbutil::Mutex::Lock lock(mutex_);
    if (SYNC_SUCCESS == result)
    {
      sync_stat_.success_count_++;
      fprintf(log_file_succ_.fp_, "%s\n", tfs_file_name.c_str());
    }
    else if (SYNC_FAILED == result)
    {
      TBSYS_LOG(WARN, "sync file(%s) failed.", tfs_file_name.c_str());
      sync_stat_.fail_count_++;
      fprintf(log_file_fail_.fp_, "%s\n", tfs_file_name.c_str());
    }
    else
    {
      sync_stat_.unsync_count_++;
      fprintf(log_file_unsync_.fp_, "%s\n", tfs_file_name.c_str());
    }
    sync_stat_.total_count_++;
  }
  else
  {
    TBSYS_LOG(WARN, "sync tfs_file_name is empty");
  }
}


int SyncByBlockWorker::get_src_one_ds(uint64_t& src_ds_id)
{
  VUINT64 servers;
  int ret = ToolUtil::get_block_ds_list_v2(Func::get_host_ip(src_addr_.c_str()), block_id_, servers);
  if (TFS_SUCCESS == ret)
  {
    ret = (servers.size() == 0) ? EXIT_GENERAL_ERROR : TFS_SUCCESS;
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "get src ds fail. dataserver servers list is empty , blockid: %"PRI64_PREFIX"u", block_id_);
    }
    else
    {
      int src_ds_addr_index = random() % servers.size();
      src_ds_id = servers.at(src_ds_addr_index);
      TBSYS_LOG(DEBUG, "src ds list size:%zd, random index:%d, src_ds_ip_random:%s", servers.size(), src_ds_addr_index, tbsys::CNetUtil::addrToString(src_ds_id).c_str());
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "get src ds fail. blockid: %"PRI64_PREFIX"u, ret:%d", block_id_, ret);
  }
  return ret;
}


int SyncByBlockWorker::read_index(const uint64_t src_ds_id)
{
  IndexDataV2 index_data;
  ReadIndexMessageV2 rdindex_msg;
  rdindex_msg.set_attach_block_id(block_id_);
  rdindex_msg.set_block_id(block_id_);

  int ret = TFS_SUCCESS;
  NewClient* client = NewClientManager::get_instance().create_client();
  if (NULL != client)
  {
    tbnet::Packet* rsp = NULL;
    if ((ret = send_msg_to_server(src_ds_id, client, &rdindex_msg, rsp)) == TFS_SUCCESS)
    {
      if (READ_INDEX_RESP_MESSAGE_V2 == rsp->getPCode())
      {
        ReadIndexRespMessageV2* resp_msg = dynamic_cast<ReadIndexRespMessageV2* >(rsp);
        src_file_list_ = resp_msg->get_index_data().finfos_;
      }
      else if (STATUS_MESSAGE == rsp->getPCode())
      {
        StatusMessage* msg = dynamic_cast<StatusMessage*>(rsp);
        TBSYS_LOG(ERROR, "read index from src ds: %s failed, blockid: %"PRI64_PREFIX"u, error msg: %s, ret status: %d",
            tbsys::CNetUtil::addrToString(src_ds_id).c_str(), block_id_, msg->get_error(), msg->get_status());
        ret = msg->get_status();
      }
      else
      {
        TBSYS_LOG(ERROR, "read index from src ds: %s failed, blockid: %"PRI64_PREFIX"u, unknow msg type: %d",
            tbsys::CNetUtil::addrToString(src_ds_id).c_str(), block_id_, rsp->getPCode());
        ret = EXIT_UNKNOWN_MSGTYPE;
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "read index from src ds: %s failed, blockid: %"PRI64_PREFIX"u, ret: %d.",
            tbsys::CNetUtil::addrToString(src_ds_id).c_str(), block_id_, ret);
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  else
  {
    ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
    TBSYS_LOG(ERROR, "read index from src ds: %s failed, blockid: %"PRI64_PREFIX"u, create client failed.",
        tbsys::CNetUtil::addrToString(src_ds_id).c_str(), block_id_);
  }

  TBSYS_LOG(DEBUG, "read index from src ds: %s %s, blockid: %"PRI64_PREFIX"u",
        tbsys::CNetUtil::addrToString(src_ds_id).c_str(), ret == TFS_SUCCESS  ? "succ" : "fail", block_id_);
  return ret;
}

int SyncByBlockWorker::read_data(const uint64_t src_ds_id)
{
  bool eof_flag = false;
  int ret = TFS_SUCCESS;
  int32_t cur_offset = 0;
  int64_t traffic = get_manager()->get_traffic_threshold();
  int64_t read_size = std::min(traffic, TRAN_BUFFER_SIZE);
  int64_t micro_sec = 1000 * 1000;

  src_data_buf_.clear();
  while (!eof_flag)
  {
    int64_t remainder_retrys = RETRY_TIMES;
    TIMER_START();
    while (remainder_retrys > 0)
    {
      ReadRawdataMessageV2 rrd_msg;
      rrd_msg.set_block_id(block_id_);
      rrd_msg.set_offset(cur_offset);
      rrd_msg.set_length(read_size);
      //rrd_msg.set_degrade_flag(false); //不考虑degrade read

      // fetch data from random ds
      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet* rsp = NULL;
      ret = send_msg_to_server(src_ds_id, client, &rrd_msg, rsp);
      if (TFS_SUCCESS != ret)//只有发送的读消息没有收到回应才需要重试
      {
        --remainder_retrys;//need to retry
        TBSYS_LOG(WARN, "read raw data from ds: %s failed, blockid: %"PRI64_PREFIX"u, offset: %d, remainder_retrys: %"PRI64_PREFIX"d, ret: %d",
                    tbsys::CNetUtil::addrToString(src_ds_id).c_str(), block_id_, cur_offset, remainder_retrys, ret);
      }
      else
      {
        remainder_retrys = 0;//retry end
        assert(NULL != rsp);
        if (READ_RAWDATA_RESP_MESSAGE_V2 == rsp->getPCode())
        {
          ReadRawdataRespMessageV2* rsp_rrd_msg = dynamic_cast<ReadRawdataRespMessageV2*>(rsp);
          int len = rsp_rrd_msg->get_length();
          assert(len >= 0);
          if (len < read_size || len == 0)
          {
            eof_flag = true;
            TBSYS_LOG(INFO, "read raw data from ds: %s finish, blockid: %"PRI64_PREFIX"u, offset: %d, remainder_retrys: %"PRI64_PREFIX"d, ret: %d, read_size:%ld, len: %d",
              tbsys::CNetUtil::addrToString(src_ds_id).c_str(), block_id_, cur_offset, remainder_retrys, ret, read_size, len);
          }
          if (len > 0)
          {
            TBSYS_LOG(DEBUG, "read raw data from ds: %s succ, blockid: %"PRI64_PREFIX"u, offset: %d, len: %d, data: %p",
                tbsys::CNetUtil::addrToString(src_ds_id).c_str(), block_id_, cur_offset, len, rsp_rrd_msg->get_data());
            src_data_buf_.writeBytes(rsp_rrd_msg->get_data(), len);
            cur_offset += len;
          }
        }
        else if (STATUS_MESSAGE == rsp->getPCode())
        {
          StatusMessage* sm = dynamic_cast<StatusMessage*>(rsp);
          TBSYS_LOG(ERROR, "read raw data from ds: %s failed, blockid: %"PRI64_PREFIX"u, offset: %d, remainder_retrys: %"PRI64_PREFIX"d, error msg:%s, ret: %d",
          tbsys::CNetUtil::addrToString(src_ds_id).c_str(), block_id_, cur_offset, remainder_retrys, sm->get_error(), sm->get_status());
          ret = sm->get_status();
        }
        else //unknow type
        {
          TBSYS_LOG(ERROR, "read raw data from ds: %s failed, blockid: %"PRI64_PREFIX"u, offset: %d, remainder_retrys: %"PRI64_PREFIX"d, unknow msg type: %d",
              tbsys::CNetUtil::addrToString(src_ds_id).c_str(), block_id_, cur_offset, remainder_retrys, rsp->getPCode());
          ret = EXIT_READ_FILE_ERROR;
        }
      }
      NewClientManager::get_instance().destroy_client(client);
    }
    TIMER_END();

    if (TFS_SUCCESS != ret)
    {
      break;
    }

    // restrict speed
    int64_t speed = 0;
    int64_t d_value = micro_sec - TIMER_DURATION();
    if (d_value > 0)
    {
      usleep(d_value);
      speed = read_size * 1000 * 1000 / TIMER_DURATION();
    }

    TBSYS_LOG(DEBUG, "read data, cost time: %"PRI64_PREFIX"d, read size: %"PRI64_PREFIX"d, speed: %"PRI64_PREFIX"d byte/s, need sleep time: %"PRI64_PREFIX"d",
        TIMER_DURATION(), read_size, speed, d_value);
  }

  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "read raw data from ds: %s failed, blockid: %"PRI64_PREFIX"u, ret:%d", tbsys::CNetUtil::addrToString(src_ds_id).c_str(), block_id_, ret);
  }
  return ret;
}


int SyncByBlockWorker::write_file(const string& file_name, char* data, int32_t size, int32_t status)
{
  int ret = TFS_SUCCESS;
  if (NULL == data)
  {
    TBSYS_LOG(ERROR, "input source data is null, filename: %s", file_name.c_str());
    ret = EXIT_PARAMETER_ERROR;
  }
  else
  {
    int dest_fd = TfsClientImplV2::Instance()->open(file_name.c_str(), NULL, dest_addr_.c_str(), T_WRITE | T_NEWBLK);
    if (dest_fd < 0)
    {
      ret = dest_fd;
      TBSYS_LOG(ERROR, "open dest tfsfile fail when copy file, filename: %s, ret:%d", file_name.c_str(), ret);
    }
    else
    {
      if ((ret = TfsClientImplV2::Instance()->set_option_flag(dest_fd, TFS_FILE_NO_SYNC_LOG)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "set option flag failed. ret: %d", ret);
      }
      else
      {
        int32_t wlen = TfsClientImplV2::Instance()->write(dest_fd, data, size);
        if (wlen != size)
        {
          ret = wlen;
          TBSYS_LOG(ERROR, "write tfsfile fail, filename: %s, datalen: %d, ret:%d", file_name.c_str(), size, ret);
        }
      }
      if (TFS_SUCCESS != (ret = TfsClientImplV2::Instance()->close(dest_fd, NULL, 0, status)) )
      {
        TBSYS_LOG(ERROR, "close dest tfsfile fail, filename: %s, ret:%d", file_name.c_str(), ret);
      }
    }
  }
  return ret;
}

void SyncByBlockWorker::write_block()
{
  int ret = TFS_SUCCESS;
  char* pstart = NULL;
  vector<FileInfoV2>::iterator it = src_file_list_.begin();
  for ( ; it != src_file_list_.end(); ++it)
  {
    SyncResult result = SYNC_NOTHING;
    FSName fsname(block_id_, it->id_);
    string file_name = string(fsname.get_name());
    if (0 == (it->status_ & FILE_STATUS_DELETE) || get_manager()->need_unlink_sync())
    {
      assert(it->offset_ + it->size_ <= src_data_buf_.getDataLen());
      pstart = src_data_buf_.getData() + it->offset_ + FILEINFO_EXT_SIZE;
      ret = write_file(file_name, pstart, it->size_ - FILEINFO_EXT_SIZE, it->status_);
      if (TFS_SUCCESS != ret)
      {
        result = SYNC_FAILED;
        TBSYS_LOG(WARN, "write file fail, filename: %s, blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"d, ret:%d", file_name.c_str(), block_id_, it->id_, ret);
      }
      else
      {
        result = SYNC_SUCCESS;
      }
    }
    else
    {
      TBSYS_LOG(DEBUG, "skip 'DELETE' file, blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"d", block_id_, it->id_);
    }
    get_manager()->write_log_file(file_name, result);
  }
  TBSYS_LOG(INFO, "write block %s, blockid: %"PRI64_PREFIX"u", TFS_SUCCESS == ret ? "success" : "fail", block_id_);
}

int SyncByBlockWorker::sync_block_by_block()
{
  int ret = TFS_SUCCESS;
  while (true)
  {
    uint64_t src_ds_id = INVALID_SERVER_ID;
    if ((ret = get_src_one_ds(src_ds_id)) != TFS_SUCCESS)
    {
      break;
    }
    if ((ret = read_index(src_ds_id)) != TFS_SUCCESS)
    {
      break;
    }
    if (0 == src_file_list_.size())//block is empty
    {
      break;
    }
    if ((ret = read_data(src_ds_id)) != TFS_SUCCESS)
    {
      break;
    }
    write_block();
    break;
  }
  return ret;
}

int SyncByBlockWorker::sync_block_by_file(vector<FileInfoV2>& src_finfos, vector<FileInfoV2>& dest_finfos)
{
  int ret = TFS_SUCCESS;
  set<FileInfoV2, CompareFileInfoV2ByFileId> dest_finfos_set;
  dest_finfos_set.insert(dest_finfos.begin(), dest_finfos.end());

  SyncFileBase sync_file_op(src_addr_, dest_addr_);
  FileInfoV2 dest_file_info;
  bool sync_block_all_success = true;
  bool no_need_sync = true;
  vector<FileInfoV2>::const_iterator src_file_iter = src_finfos.begin();
  for (; src_file_iter != src_finfos.end() && !stop_; ++src_file_iter)
  {
    set<FileInfoV2, CompareFileInfoV2ByFileId>::const_iterator dest_file_iter = dest_finfos_set.find( (*src_file_iter) );
    if (dest_file_iter != dest_finfos_set.end())
    {
      dest_file_info = (*dest_file_iter);
      dest_finfos_set.erase(*dest_file_iter);
    }
    else
    {
      dest_file_info.id_ = 0;//目标集群中该文件不存在
    }

    string tfs_file_name = "";
    SyncResult result = SYNC_NOTHING;
    ret = sync_file_op.cmp_and_sync_file(block_id_, (*src_file_iter), dest_file_info, timestamp_,
      result, tfs_file_name, get_manager()->need_force_sync(), get_manager()->need_unlink_sync());
    TBSYS_LOG(DEBUG, "sync file: %s finished, %s", tfs_file_name.c_str(), (TFS_SUCCESS == ret) ? "success" : "failed");

    get_manager()->write_log_file(tfs_file_name, result);
    if (sync_block_all_success && TFS_SUCCESS != ret)
    {
      sync_block_all_success = false;
    }
    if (no_need_sync && SYNC_NOTHING != result)
    {
      no_need_sync = false;
    }
  }

  if (src_file_iter == src_finfos.end())
  {
    TBSYS_LOG(DEBUG, "sync block finished. blockid: %"PRI64_PREFIX"u, %s", block_id_, no_need_sync ? "no need sync" : "sync have done");

    //dest_finfos_set最后剩下的即为多余文件
    int32_t succ_del_count = 0;
    int32_t unsync_del_count = 0;
    if (get_manager()->need_remove_redundant_file())
    {
      set<FileInfoV2, CompareFileInfoV2ByFileId>::const_iterator it = dest_finfos_set.begin();
      for (; it != dest_finfos_set.end(); ++it)
      {
        FSName fsname(block_id_, it->id_);
        string file_name = string(fsname.get_name());
        if (it->status_ & FILE_STATUS_DELETE)
        {
          ++unsync_del_count;
          continue;
        }

        ret = sync_file_op.unlink_file(file_name, FILE_STATUS_DELETE);
        if (TFS_SUCCESS != ret)
        {
          if (sync_block_all_success)
          {
            sync_block_all_success = false;
          }
          TBSYS_LOG(WARN, "unlink redundant dest file fail, blockid: %"PRI64_PREFIX"u, file_id:%"PRI64_PREFIX"u, filename: %s, ret:%d", block_id_, it->id_, file_name.c_str(), ret);
        }
        else
        {
          ++succ_del_count;
        }
      }
      TBSYS_LOG(INFO, "redundant file count:%zd in dest cluster, delete succ count:%d, no need delete count:%d, blockid: %"PRI64_PREFIX"u", dest_finfos_set.size(), succ_del_count, unsync_del_count, block_id_);
    }

    if (!sync_block_all_success)
    {
      ret = EXIT_GENERAL_ERROR;//block中部分文件同步失败
    }
  }
  else
  {
    TBSYS_LOG(INFO, "sync stopped when doing blockid: %"PRI64_PREFIX"u", block_id_);
    ret = EXIT_GENERAL_ERROR;//block同步中途终止，没有全部同步完成
  }

  return ret;
}


int SyncByBlockWorker::process(string& line)
{
  int ret = TFS_SUCCESS;
  if ( !line.empty() )
  {
    block_id_ = strtoull(line.c_str(), NULL, 10);
    if ( !IS_VERFIFY_BLOCK(block_id_) )
    {
      vector<FileInfoV2> src_finfos, dest_finfos;
      ret = ToolUtil::read_file_infos_v2(Func::get_host_ip(src_addr_.c_str()), block_id_, src_finfos);
      if (TFS_SUCCESS == ret)
      {
        ret = ToolUtil::read_file_infos_v2(Func::get_host_ip(dest_addr_.c_str()), block_id_, dest_finfos);
        if (TFS_SUCCESS == ret)
        {
          ret = sync_block_by_file(src_finfos, dest_finfos);
          TBSYS_LOG(INFO, "sync blockid: %"PRI64_PREFIX"u' %s, src files list size: %zd", block_id_, TFS_SUCCESS == ret ? "success" : "fail", src_finfos.size());
        }
        else
        {
          if (EXIT_BLOCK_NOT_FOUND == ret || EXIT_NO_DATASERVER == ret)
          {
            ret = TFS_SUCCESS;
          }
          else if (EXIT_BLOCK_CANNOT_REINSTATE == ret)
          {
            //必须先确保ns解组恢复正常的block成功，然后强制删除持久化的编组信息；但是这个过程不可能一下子就完成所以这里其实什么都不比做
            //delete_family_info_from_ns(block_id_);顺便把block对象删掉,否则NEW_BLK open可能个失败
            TBSYS_LOG(WARN, "dest cluster's block is lost, family can not reinstate, try to let nameserver dissolve family right now, blockid: %"PRI64_PREFIX"u'", block_id_);
          }

          if (TFS_SUCCESS == ret)
          {
            ret = sync_block_by_block();
            TBSYS_LOG(INFO, "blockid: %"PRI64_PREFIX"u is not exist in dest cluster, sync block %s by read raw data finally", block_id_, TFS_SUCCESS == ret ? "success":"fail");
          }
          else
          {
            TBSYS_LOG(WARN, "get dest block fileinfos fail, blockid: %"PRI64_PREFIX"u sync fail, ret:%d", block_id_, ret);
          }
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "get src block fileinfo fail, blockid: %"PRI64_PREFIX"u sync fail", block_id_);
      }
    }
    else
    {
      ret = EXIT_PARAMETER_ERROR;
      TBSYS_LOG(ERROR, "block_id: %"PRI64_PREFIX"u is not a data block", block_id_);
    }
  }
  else
  {
    ret = EXIT_PARAMETER_ERROR;
    TBSYS_LOG(ERROR, "this is a null string in putfile");
  }
  return ret;
}

int SyncByBlockManger::begin()
{
  srandom((unsigned)time(NULL));
  int ret = TfsClientImplV2::Instance()->initialize();
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "TfsClientImplV2 initialize failed, ret:%d", ret);
  }
  else
  {
    if ( !log_file_succ_.init_log_file(get_output_dir().c_str())
       || !log_file_fail_.init_log_file(get_output_dir().c_str())
       || !log_file_unsync_.init_log_file(get_output_dir().c_str()))
    {
      TBSYS_LOG(ERROR, "init sync log file failed.");
      ret = EXIT_OPEN_FILE_ERROR;
    }
  }
  return ret;
}


void SyncByBlockManger::end()
{
  TfsClientImplV2::Instance()->destroy();
  destroy_log_file(log_file_succ_);
  destroy_log_file(log_file_fail_);
  destroy_log_file(log_file_unsync_);

  //rotato log默认标准输出全部到log
  char file_path[256];
  snprintf(file_path, 256, "%s/%s", get_output_dir().c_str(), "result");
  FILE* result_fp = fopen(file_path, "wb");
  if (NULL != result_fp)
  {
    sync_stat_.dump(result_fp);
    fclose(result_fp);
  }
  else
  {
    TBSYS_LOG(ERROR, "open result file:%s to write fail", file_path);
  }
  TBSYS_LOG(INFO, "log and result output path: %s\n", get_output_dir().c_str());
}

BaseWorker* SyncByBlockManger::create_worker()
{
  return new SyncByBlockWorker(this);
}


int main(int argc, char* argv[])
{
  bool force = false;
  bool unlink = false;
  bool need_remove_file = false;
  int64_t traffic_threshold = 1024;
  int idx = 0;
  while (idx < argc)
  {
    if (strcmp(argv[idx], "-e")== 0)
    {
      force = true;
      for (int i = idx; i + 1 < argc; ++i)
      {
        argv[i] = argv[i + 1];
      }
      argc -= 1;
    }
    else if (strcmp(argv[idx], "-r")== 0)
    {
      unlink = true;
      for (int i = idx; i + 1 < argc; ++i)
      {
        argv[i] = argv[i + 1];
      }
      argc -= 1;
    }
    else if (strcmp(argv[idx], "-u")== 0)
    {
      need_remove_file = true;
      for (int i = idx; i + 1 < argc; ++i)
      {
        argv[i] = argv[i + 1];
      }
      argc -= 1;
    }
    else if (strcmp(argv[idx], "-w")== 0)
    {
      traffic_threshold = strtoull(argv[idx + 1], NULL, 10);
      if (traffic_threshold <= 0)
      {
        TBSYS_LOG(ERROR, "traffic(%"PRI64_PREFIX"d <= 0) set error", traffic_threshold);
        exit(-1);
      }
      for (int i = idx; i + 2 < argc; ++i)
      {
        argv[i] = argv[i + 2];
      }
      argc -= 2;
    }
    else
    {
      ++idx;
    }
  }

  SyncByBlockManger work_manager(force, unlink, need_remove_file, traffic_threshold);
  return work_manager.main(argc, argv);
}


