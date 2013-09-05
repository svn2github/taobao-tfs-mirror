 /*
 * * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: block_console.cpp 445 2011-06-08 09:27:48Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */

#include <algorithm>

#include "tbsys.h"
#include "Memory.hpp"

#include "common/func.h"
#include "common/internal.h"
#include "common/client_manager.h"
#include "common/new_client.h"
#include "common/status_message.h"
#include "nameserver/ns_define.h"
#include "message/block_info_message.h"
#include "message/block_info_message_v2.h"
#include "message/read_data_message.h"
#include "message/read_data_message_v2.h"
#include "message/write_data_message.h"
#include "message/client_cmd_message.h"
#include "message/write_index_message_v2.h"
#include "message/write_data_message_v2.h"
#include "clientv2/tfs_client_impl_v2.h"
#include "clientv2/fsname.h"
#include "message/read_index_message_v2.h"

#include "tools/util/tool_util.h"
#include "block_console_v2.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::clientv2;
using namespace tfs::tools;

const int64_t TranBlock::TRAN_BUFFER_SIZE = 8 * 1024 * 1024;
const int64_t TranBlock::RETRY_TIMES = 2;

BlockConsole::BlockConsole() :
  succ_file_ptr_(NULL), fail_file_ptr_(NULL)
{
  input_blockids_.clear();
  succ_blockids_.clear();
  dest_ds_ids_.clear();
  memset(&stat_param_, 0, sizeof(StatParam));
}

BlockConsole::~BlockConsole()
{
  if (NULL != input_file_ptr_)
  {
    fclose(input_file_ptr_);
  }
  if (NULL != succ_file_ptr_)
  {
    fclose(succ_file_ptr_);
  }
  if (NULL != fail_file_ptr_)
  {
    fclose(fail_file_ptr_);
  }
}

int BlockConsole::initialize(const string& ts_input_blocks_file, const std::string& dest_ds_ip_file)
{
  int ret = TFS_SUCCESS;
  srandom((unsigned)time(NULL));

  FILE* dest_ds_file_ptr = NULL;
  if (access(ts_input_blocks_file.c_str(), R_OK) < 0 || access(dest_ds_ip_file.c_str(), R_OK) < 0)
  {
    TBSYS_LOG(ERROR, "access input block file: %s, ds file: %s fail, error: %s\n",
        ts_input_blocks_file.c_str(), dest_ds_ip_file.c_str(), strerror(errno));
    ret = EXIT_ACCESS_PERMISSION_ERROR;
  }
  else if ((input_file_ptr_ = fopen(ts_input_blocks_file.c_str(), "r")) == NULL ||
      (dest_ds_file_ptr = fopen(dest_ds_ip_file.c_str(), "r")) == NULL)
  {
    TBSYS_LOG(ERROR, "open input block file: %s, ds file: %s fail, error: %s\n",
        ts_input_blocks_file.c_str(), dest_ds_ip_file.c_str(), strerror(errno));
    ret = EXIT_OPEN_FILE_ERROR;
  }
  else
  {
    // load dest ds
    uint64_t ds_id = 0;
    char ds_ip[SPEC_LEN] = {'\0'};
    while (fscanf(dest_ds_file_ptr, "%s", ds_ip) != EOF)
    {
      ds_id = Func::get_host_ip(ds_ip);
      if (0 == ds_id)
      {
        TBSYS_LOG(ERROR, "input ds ip: %s is illegal.\n", ds_ip);
      }
      else
      {
        dest_ds_ids_.insert(ds_id);
        TBSYS_LOG(DEBUG, "input ds ip: %s, ds id: %"PRI64_PREFIX"u succ.\n", ds_ip, ds_id);
      }
    }

    if (dest_ds_ids_.size() <= 0)
    {
      TBSYS_LOG(ERROR, "ds file: %s size: %d is illegal.\n",
          dest_ds_ip_file.c_str(), static_cast<int32_t>(dest_ds_ids_.size()));
      ret = TFS_ERROR;
    }
    else
    {
      cur_ds_sit_ = dest_ds_ids_.begin();
    }

    // load blocks
    if (TFS_SUCCESS == ret)
    {
      string ts_succ_blocks_file = ts_input_blocks_file + string(".succ");
      string ts_fail_blocks_file = ts_input_blocks_file + string(".fail");
      uint64_t blockid = 0;
      if ((succ_file_ptr_ = fopen(ts_succ_blocks_file.c_str(), "a+")) == NULL)
      {
        TBSYS_LOG(ERROR, "open succ output file: %s, error: %s\n", ts_succ_blocks_file.c_str(), strerror(errno));
        ret = EXIT_OPEN_FILE_ERROR;
      }
      else
      {
        // load succ blocks
        while (fscanf(succ_file_ptr_, "%lu", &blockid) != EOF)
        {
          succ_blockids_.insert(blockid);
          atomic_inc(&stat_param_.copy_success_);
        }
        // open failed file to rewrite transfer failed block
        if ((fail_file_ptr_ = fopen(ts_fail_blocks_file.c_str(), "w+")) == NULL)//truncate if exist
        {
          TBSYS_LOG(ERROR, "open fail output file: %s, error: %s\n", ts_fail_blocks_file.c_str(), strerror(errno));
          ret = EXIT_OPEN_FILE_ERROR;
        }
      }

      if(TFS_SUCCESS == ret)
      {
        // load input blocks
        while (fscanf(input_file_ptr_, "%lu", &blockid) != EOF)
        {
          if(succ_blockids_.find(blockid) == succ_blockids_.end())
          {
            input_blockids_.insert(blockid);
          }
        }
        stat_param_.total_ = input_blockids_.size();

        if(input_blockids_.size() <= 0)
        {
          TBSYS_LOG(ERROR, "can not find blockid which has not transfered success in input file\n");
          ret = TFS_ERROR;
        }
        else
        {
          cur_sit_ = input_blockids_.begin();
        }
      }
    }

    fclose(dest_ds_file_ptr);//关闭已经不需要的文件
    //succ_file_ptr_ 和 fail_file_ptr_ 最后程序执行完结果写入后，在BlockConsole析构函数关闭
  }
  TBSYS_LOG(INFO, "util now, there are still %"PRI64_PREFIX"d remainder blocks who need to transfer in input file\n", stat_param_.total_);

  return ret;
}

int BlockConsole::get_transfer_param(uint64_t& blockid, uint64_t& ds_id)
{
  int ret = TFS_SUCCESS;
  tbutil::Mutex::Lock lock(mutex_);
  // get block
  if (cur_sit_ == input_blockids_.end())
  {
    TBSYS_LOG(ERROR, "transfer block finish.\n");
    ret = TRANSFER_FINISH;
  }
  else
  {
    blockid = *cur_sit_;
    ++cur_sit_;
  }

  // get ds
  if (TFS_SUCCESS == ret)
  {
    if (dest_ds_ids_.size() <= 0)
    {
      TBSYS_LOG(ERROR, "ds size: %d is illegal.\n",
          static_cast<int32_t>(dest_ds_ids_.size()));
      ret = TFS_ERROR;
    }
    else
    {
      if (cur_ds_sit_ == dest_ds_ids_.end())
      {
        cur_ds_sit_ = dest_ds_ids_.begin();
      }

      ds_id = *cur_ds_sit_;
      ++cur_ds_sit_;
    }
  }

  return ret;
}

int BlockConsole::finish_transfer_block(const uint64_t blockid, const int32_t transfer_ret)
{
  FILE* file_ptr = NULL;
  if (TFS_SUCCESS == transfer_ret)
  {
    atomic_inc(&stat_param_.copy_success_);
    file_ptr = succ_file_ptr_;
  }
  else
  {
    atomic_inc(&stat_param_.copy_failure_);
    file_ptr = fail_file_ptr_;
  }

  TBSYS_LOG(INFO, "begin write blockid: %"PRI64_PREFIX"u to %s file", blockid, TFS_SUCCESS == transfer_ret ? "succ" : "fail");
  int ret = TFS_SUCCESS;
  tbutil::Mutex::Lock lock(mutex_);
  {
    if ((ret = fprintf(file_ptr, "%lu\n", blockid)) < 0)
    {
      TBSYS_LOG(ERROR, "failed to write blockid: %"PRI64_PREFIX"u to %s file", blockid, TFS_SUCCESS == transfer_ret ? "succ" : "fail");
      ret = TFS_ERROR;
    }
    fflush(file_ptr);
  }
  //stat_param_.dump();
  TBSYS_LOG(INFO, "transfer blockid: %"PRI64_PREFIX"u finished, until now transfer block succ count: %d already, failed count: %d, input file count:%"PRI64_PREFIX"d in total", blockid, atomic_read(&stat_param_.copy_success_), atomic_read(&stat_param_.copy_failure_), stat_param_.total_);

  return ret;
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////
TranBlock::TranBlock(const uint64_t blockid, const std::string& src_ns_addr,
    const std::string& dest_ns_addr, const uint64_t dest_ds_id, const int64_t traffic) :
    src_ns_addr_(src_ns_addr), dest_ns_addr_(dest_ns_addr), dest_ds_id_(dest_ds_id),
    cur_offset_(0), total_tran_size_(0), traffic_(traffic)
{
  src_block_id_ = blockid;
  src_file_set_.clear();
}

TranBlock::~TranBlock()
{
  src_file_set_.clear();
}

int TranBlock::run()
{
  int ret = TFS_SUCCESS;
  while (true)
  {
    if ((ret = get_src_ds()) != TFS_SUCCESS)
    {
      break;
    }
    if ((ret = read_index()) != TFS_SUCCESS)
    {
      break;
    }
    if (0 == src_file_set_.size())
    {
      break;// no need to transfer
    }

    if ((ret = read_data()) != TFS_SUCCESS)
    {
      break;
    }
    if ((ret = recombine_data()) != TFS_SUCCESS)
    {
      break;
    }
    if(0 == dest_index_data_.finfos_.size())
    {
      break;// all files is 'DELETE', so dest data is empty
    }
    if ((ret = check_dest_blk()) != TFS_SUCCESS)
    {
      break;
    }
    if ((ret = write_data()) != TFS_SUCCESS)
    {
      break;
    }
    if ((ret = write_index()) != TFS_SUCCESS)
    {
      break;
    }
    ret = check_integrity();
    break;
  }

  return ret;
}

int TranBlock::get_src_ds()
{
  int ret = TFS_SUCCESS;
  if (IS_VERFIFY_BLOCK(src_block_id_))
  {
    ret = EXIT_PARAMETER_ERROR;
    TBSYS_LOG(ERROR, "block_id: %"PRI64_PREFIX"u is a check block, but only data block need transfer", src_block_id_);
  }
  else
  {
    vector<uint64_t> servers;
    ret = ToolUtil::get_block_ds_list_v2(Func::get_host_ip(src_ns_addr_.c_str()), src_block_id_, servers);
    if (TFS_SUCCESS == ret)
    {
      ret = (servers.size() == 0) ? EXIT_GENERAL_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "get src ds fail. ds servers list size is %zd, blockid: %"PRI64_PREFIX"u", servers.size(), src_block_id_);
      }
      else
      {
        int src_ds_addr_index = random() % servers.size();
        src_ds_addr_random_ = servers[src_ds_addr_index];
        TBSYS_LOG(DEBUG, "src ds list size:%zd, random index:%d, src_ds_ip_random:%s", servers.size(), src_ds_addr_index, tbsys::CNetUtil::addrToString(src_ds_addr_random_).c_str());
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "get src ds fail. blockid: %"PRI64_PREFIX"u, ret:%d", src_block_id_, ret);
    }
  }
  return ret;
}


int TranBlock::read_index()
{
  int ret = TFS_SUCCESS;
  // fetch index from random ds
  ret = read_index(src_ds_addr_random_, src_file_set_);
  if (TFS_SUCCESS == ret)
  {
    if (0 == src_file_set_.size())
    {
      TBSYS_LOG(INFO, "file count is 0, need not to transfer, blockid: %"PRI64_PREFIX"u", src_block_id_);
    }
    else
    {
      total_tran_size_ += src_file_set_.size() * FILE_INFO_V2_LENGTH;
    }
  }

  return ret;
}

int TranBlock::read_index(uint64_t ds_ip_addr, FileInfoV2Set& file_set)
{
  IndexDataV2 index_data;
  ReadIndexMessageV2 rdindex_msg;
  rdindex_msg.set_attach_block_id(src_block_id_);
  rdindex_msg.set_block_id(src_block_id_);

  int ret = TFS_SUCCESS;
  NewClient* client = NewClientManager::get_instance().create_client();
  if (NULL != client)
  {
    tbnet::Packet* rsp = NULL;
    if ((ret = send_msg_to_server(ds_ip_addr, client, &rdindex_msg, rsp)) == TFS_SUCCESS)
    {
      if (READ_INDEX_RESP_MESSAGE_V2 == rsp->getPCode())
      {
        ReadIndexRespMessageV2* resp_msg = dynamic_cast<ReadIndexRespMessageV2* >(rsp);
        index_data = resp_msg->get_index_data();
        src_header_ = index_data.header_;
        file_set.insert(index_data.finfos_.begin(), index_data.finfos_.end());
      }
      else if (STATUS_MESSAGE == rsp->getPCode())
      {
        StatusMessage* msg = dynamic_cast<StatusMessage*>(rsp);
        TBSYS_LOG(ERROR, "read index from ds: %s failed, blockid: %"PRI64_PREFIX"u, error msg: %s, ret status:%d",
            tbsys::CNetUtil::addrToString(ds_ip_addr).c_str(), src_block_id_, msg->get_error(), msg->get_status());
        ret = msg->get_status();
      }
      else
      {
        TBSYS_LOG(ERROR, "read index from ds: %s failed, blockid: %"PRI64_PREFIX"u, unknow msg type: %d",
            tbsys::CNetUtil::addrToString(ds_ip_addr).c_str(), src_block_id_, rsp->getPCode());
        ret = EXIT_UNKNOWN_MSGTYPE;
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "read index from ds: %s failed, blockid: %"PRI64_PREFIX"u, ret: %d.",
            tbsys::CNetUtil::addrToString(ds_ip_addr).c_str(), src_block_id_, ret);
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  else
  {
    ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
    TBSYS_LOG(ERROR, "read index from ds: %s failed, blockid: %"PRI64_PREFIX"u, create client failed.",
        tbsys::CNetUtil::addrToString(ds_ip_addr).c_str(), src_block_id_);
  }

  TBSYS_LOG(DEBUG, "read index from ds: %s %s, blockid: %"PRI64_PREFIX"u",
        tbsys::CNetUtil::addrToString(ds_ip_addr).c_str(), ret == TFS_SUCCESS  ? "succ" : "fail", src_block_id_);
  return ret;
}

int TranBlock::read_data()
{
  bool eof_flag = false;
  int ret = TFS_SUCCESS;
  int64_t read_size = std::min(traffic_, TRAN_BUFFER_SIZE);
  int64_t micro_sec = 1000 * 1000;

  if (0 == src_file_set_.size())
  {
    TBSYS_LOG(WARN, "block's file count is 0, can't send ReadRawdataMessageV2, blockid: %"PRI64_PREFIX"u", src_block_id_);
    return ret;
  }
  cur_offset_ = 0;
  src_content_buf_.clear();
  while (!eof_flag)
  {
    int64_t remainder_retrys = RETRY_TIMES;
    TIMER_START();
    while (remainder_retrys > 0)
    {
      ReadRawdataMessageV2 rrd_msg;
      rrd_msg.set_block_id(src_block_id_);
      rrd_msg.set_offset(cur_offset_);
      rrd_msg.set_length(read_size);
      //rrd_msg.set_degrade_flag(true);
      //不考虑设置degrade read

      // fetch data from random ds
      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet* rsp = NULL;
      ret = send_msg_to_server(src_ds_addr_random_, client, &rrd_msg, rsp);
      if(TFS_SUCCESS != ret)//只有发送的读消息没有收到回应才需要重试
      {
        --remainder_retrys;//need to retry
        TBSYS_LOG(ERROR, "read raw data from ds: %s failed, blockid: %"PRI64_PREFIX"u, offset: %d, remainder_retrys: %"PRI64_PREFIX"d, ret: %d",
                    tbsys::CNetUtil::addrToString(src_ds_addr_random_).c_str(), src_block_id_, cur_offset_, remainder_retrys, ret);
      }
      else
      {
        remainder_retrys = 0;//need not to retry
        if (READ_RAWDATA_RESP_MESSAGE_V2 == rsp->getPCode())
        {
          ReadRawdataRespMessageV2* rsp_rrd_msg = dynamic_cast<ReadRawdataRespMessageV2*>(rsp);
          int len = rsp_rrd_msg->get_length();
          assert(len >= 0);
          if (len < read_size || len == 0)
          {
            eof_flag = true;
            TBSYS_LOG(INFO, "read raw data from ds: %s finish, blockid: %"PRI64_PREFIX"u, offset: %d, remainder_retrys: %"PRI64_PREFIX"d, ret: %d, read_size:%ld, len: %d",
              tbsys::CNetUtil::addrToString(src_ds_addr_random_).c_str(), src_block_id_, cur_offset_, remainder_retrys, ret, read_size, len);
          }
          if (len > 0)
          {
            TBSYS_LOG(DEBUG, "read raw data from ds: %s succ, blockid: %"PRI64_PREFIX"u, offset: %d, len: %d, data: %p",
                tbsys::CNetUtil::addrToString(src_ds_addr_random_).c_str(), src_block_id_, cur_offset_, len, rsp_rrd_msg->get_data());
            src_content_buf_.writeBytes(rsp_rrd_msg->get_data(), len);
            cur_offset_ += len;
          }
        }
        else if (STATUS_MESSAGE == rsp->getPCode())
        {
          StatusMessage* sm = dynamic_cast<StatusMessage*>(rsp);
          TBSYS_LOG(ERROR, "read raw data from ds: %s failed, blockid: %"PRI64_PREFIX"u, offset: %d, remainder_retrys: %"PRI64_PREFIX"d, error msg:%s, ret: %d",
              tbsys::CNetUtil::addrToString(src_ds_addr_random_).c_str(), src_block_id_, cur_offset_, remainder_retrys, sm->get_error(), sm->get_status());
          ret = sm->get_status();
        }
        else //unknow type
        {
          TBSYS_LOG(ERROR, "read raw data from ds: %s failed, blockid: %"PRI64_PREFIX"u, offset: %d, remainder_retrys: %"PRI64_PREFIX"d, unknow msg type: %d",
              tbsys::CNetUtil::addrToString(src_ds_addr_random_).c_str(), src_block_id_, cur_offset_, remainder_retrys, rsp->getPCode());
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

    TBSYS_LOG(DEBUG, "read data, cost time: %"PRI64_PREFIX"d, read size: %"PRI64_PREFIX"d, speed: %"PRI64_PREFIX"d, need sleep time: %"PRI64_PREFIX"d",
        TIMER_DURATION(), read_size, speed, d_value);
  }

  total_tran_size_ += src_content_buf_.getDataLen();
  return ret;
}

int TranBlock::recombine_data()
{
  int ret = TFS_SUCCESS;
  if (src_file_set_.size() <= 0)
  {
    TBSYS_LOG(ERROR, "reform data failed, blockid: %"PRI64_PREFIX"u, offset: %d, file list size: %d",
        src_block_id_, cur_offset_, static_cast<int32_t>(src_file_set_.size()));
    ret = TFS_ERROR;
  }
  else
  {
    dest_content_buf_.clear();
    FileInfoV2SetIter vit = src_file_set_.begin();
    for ( ; vit != src_file_set_.end(); ++vit)
    {
      const FileInfoV2& finfo = (*vit);
      // skip deleted file
      if (0 != (finfo.status_ & (FILE_STATUS_DELETE | FILE_STATUS_INVALID)))
      {
        TBSYS_LOG(INFO, "file deleted, skip it. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, flag: %d",
            src_block_id_, finfo.id_, finfo.status_);
        continue;
      }//concealed file need to transfer

      // skip illegal file
      if (0 == finfo.id_)
      {
        TBSYS_LOG(ERROR, "fileid illegal, skip it. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, flag: %d, size: %d, offset: %d",
            src_block_id_, finfo.id_, finfo.status_, finfo.size_, finfo.offset_);
        continue;
      }

      FileInfoV2 dest_finfo = finfo;
      dest_finfo.offset_ = dest_content_buf_.getDataLen();//new offset in compacted block data
      //size_含FileInfoInDiskExt, crc_这里不检查
      //next_ 在 ds write index时修改

      // write to dest buf
      char* data = (src_content_buf_.getData() + finfo.offset_);
      dest_content_buf_.writeBytes(data, finfo.size_);
      dest_index_data_.finfos_.push_back(dest_finfo);
    }

    if(0 == dest_index_data_.finfos_.size())
    {
      TBSYS_LOG(INFO, "all files are deleted, skip it. blockid: %"PRI64_PREFIX"u, file count: %Zd", src_block_id_, src_file_set_.size());
    }
    else
    {
      // set index header
      //BlockInfoV2 10个字段, 除family_id_外(原编组不管) last_update_time_  及header_中的 used_offset_ avail_offset_ max_index_num_ ds写时会修改
      memset(&dest_index_data_.header_, 0, sizeof(IndexHeaderV2));
      dest_index_data_.header_.info_.block_id_ = src_block_id_;
      dest_index_data_.header_.info_.version_ = dest_index_data_.finfos_.size();//version in block is independent between clusters
      dest_index_data_.header_.info_.file_count_ = dest_index_data_.finfos_.size();
      dest_index_data_.header_.info_.size_ = dest_content_buf_.getDataLen();
      dest_index_data_.header_.info_.del_file_count_ = 0;
      dest_index_data_.header_.info_.del_size_ = 0;
      dest_index_data_.header_.info_.update_file_count_ = 0;
      dest_index_data_.header_.info_.update_size_ = 0;
      dest_index_data_.header_.seq_no_ = src_header_.seq_no_;//last file id sequence
      dest_index_data_.header_.used_file_info_bucket_size_ = dest_index_data_.finfos_.size();
      dest_index_data_.header_.file_info_bucket_size_ = dest_index_data_.finfos_.size();
      TBSYS_LOG(INFO, "recombine_data success. blockid: %"PRI64_PREFIX"u, file count: %d, block size: %d",
                  src_block_id_, dest_index_data_.header_.info_.file_count_, dest_index_data_.header_.info_.size_);
    }
  }
  return ret;
}

int TranBlock::check_dest_blk()
{
  int ret = TFS_SUCCESS;
  vector<uint64_t> dest_ds;
  if(dest_ns_addr_ != src_ns_addr_)//必须是集群间迁移
  {
    ret = ToolUtil::get_block_ds_list_v2(Func::get_host_ip(dest_ns_addr_.c_str()), src_block_id_, dest_ds);
    if (TFS_SUCCESS == ret)
    {
      int32_t ds_size = static_cast<int32_t>(dest_ds.size());
      if (ds_size > 0)
      {
        TBSYS_LOG(WARN, "block exists in dest cluster, now we will remove it completely before transfer. block list size is %d, the first ds is:%s, blockid: %"PRI64_PREFIX"u", ds_size, tbsys::CNetUtil::addrToString(dest_ds[0]).c_str(), src_block_id_);
        ret = rm_block_from_ns();
        for (int i = 0; i < ds_size && TFS_SUCCESS == ret; i++)
        {
          ret = rm_block_from_ds(dest_ds[i]);
        }
        //必须保证目标集群中ns和所有ds上都没有该block的任何信息
      }
      else
      {
        ret = EXIT_FAMILY_EXISTED;
        TBSYS_LOG(ERROR, "block exists in dest cluster, but ds_list is empty, fatal error, exist family block");
      }
    }
    else
    {
      if(EXIT_NO_DATASERVER == ret)
      {
        rm_block_from_ns();//悬空的block也要删除,最后写索引成功会重新汇报给ns建立新关系
        ret = TFS_SUCCESS;
      }
      else if(EXIT_BLOCK_NOT_FOUND == ret)//blockid不在集群中,正是所期望的
      {
        ret = TFS_SUCCESS;
      }
      TBSYS_LOG(INFO, "Check OK, blockid: %"PRI64_PREFIX"u not exists in dest cluster, ret:%d", src_block_id_, ret);
    }
  }
  else//不支持集群内迁移
  {
    ret = EXIT_PARAMETER_ERROR;
    TBSYS_LOG(ERROR, "Can't transfer block in the same cluster, fatal error");
  }
  return ret;
}

int TranBlock::write_data()
{
  int block_len = dest_content_buf_.getDataLen();
  int cur_write_offset = 0;
  int cur_len = 0;
  int ret = TFS_SUCCESS;

  while (block_len > 0)
  {
    int64_t remainder_retrys = RETRY_TIMES;
    while (remainder_retrys > 0)//发送的消息没有收到回应则重试
    {
      cur_len = std::min(static_cast<int64_t>(block_len), TRAN_BUFFER_SIZE);

      WriteRawdataMessageV2 req;
      req.set_block_id(src_block_id_);
      req.set_offset(cur_write_offset);
      req.set_length(cur_len);
      req.set_data(dest_content_buf_.getData());

      //new block
      if (0 == cur_write_offset)
      {
        req.set_new_flag(true);
      }

      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet* rsp = NULL;
      ret = send_msg_to_server(dest_ds_id_, client, &req, rsp);
      if (TFS_SUCCESS != ret)
      {
        --remainder_retrys;//need to retry
        TBSYS_LOG(ERROR, "write data failed, blockid: %"PRI64_PREFIX"u, dest_ds: %s, cur_write_offset: %d, cur_len: %d, ret: %d",
            src_block_id_, tbsys::CNetUtil::addrToString(dest_ds_id_).c_str(), cur_write_offset, cur_len, ret);
      }
      else
      {
        remainder_retrys = 0;//retry end
        if (STATUS_MESSAGE == rsp->getPCode())
        {
          StatusMessage* sm = dynamic_cast<StatusMessage*>(rsp);
          if (STATUS_MESSAGE_OK != sm->get_status())
          {
            TBSYS_LOG(ERROR, "write raw data to ds: %s fail, blockid: %"PRI64_PREFIX"u, offset: %d, ret: %d",
                tbsys::CNetUtil::addrToString(dest_ds_id_).c_str(), src_block_id_, cur_write_offset, sm->get_status());
            ret = sm->get_status();
          }
          else
          {
            dest_content_buf_.drainData(cur_len);
            block_len = dest_content_buf_.getDataLen();
            cur_write_offset += cur_len;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "write raw data to ds: %s fail, blockid: %"PRI64_PREFIX"u, offset: %d, unknown msg type",
              tbsys::CNetUtil::addrToString(dest_ds_id_).c_str(), src_block_id_, cur_write_offset);
          ret = EXIT_UNKNOWN_MSGTYPE;
        }

        if(TFS_SUCCESS == ret)
        {
          if (cur_len != TRAN_BUFFER_SIZE)
          {
            //write end, quit
            block_len = 0;
          }
          TBSYS_LOG(INFO, "write raw data to ds: %s successful, blockid: %"PRI64_PREFIX"u",
              tbsys::CNetUtil::addrToString(dest_ds_id_).c_str(), src_block_id_);
        }
      }
      NewClientManager::get_instance().destroy_client(client);
    }

    if (TFS_SUCCESS != ret)
    {
      break;
    }
  }

  return ret;
}

int TranBlock::write_index()
{
  WriteIndexMessageV2 req_msg;
  req_msg.set_block_id(dest_index_data_.header_.info_.block_id_);
  req_msg.set_attach_block_id(dest_index_data_.header_.info_.block_id_);
  req_msg.set_index_data(dest_index_data_);
  req_msg.set_cluster_flag(true);

  tbnet::Packet* rsp = NULL;
  NewClient* client = NewClientManager::get_instance().create_client();
  int32_t ret = send_msg_to_server(dest_ds_id_, client, &req_msg, rsp);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "write index failed, blockid: %"PRI64_PREFIX"u, length: %zd, ret: %d",
        src_block_id_, dest_index_data_.finfos_.size(), ret);
  }
  else
  {
    if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* sm = dynamic_cast<StatusMessage*>(rsp);
      if (STATUS_MESSAGE_OK != sm->get_status())
      {
        TBSYS_LOG(ERROR, "write raw index to ds: %s fail, blockid: %"PRI64_PREFIX"u, file count: %zd, ret: %d",
            tbsys::CNetUtil::addrToString(dest_ds_id_).c_str(), src_block_id_, dest_index_data_.finfos_.size(), sm->get_status());
        ret = sm->get_status();
      }
      else
      {
        TBSYS_LOG(INFO, "write raw index to ds: %s succ, blockid: %"PRI64_PREFIX"u, file count: %zd", tbsys::CNetUtil::addrToString(dest_ds_id_).c_str(), src_block_id_, dest_index_data_.finfos_.size());
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "write raw index to ds: %s fail, blockid: %"PRI64_PREFIX"u, file count: %zd, unknown msg type",
            tbsys::CNetUtil::addrToString(dest_ds_id_).c_str(), src_block_id_, dest_index_data_.finfos_.size());
      ret = EXIT_UNKNOWN_MSGTYPE;
    }
  }
  NewClientManager::get_instance().destroy_client(client);

  return ret;
}

int TranBlock::check_integrity()
{
  int32_t ret = TFS_SUCCESS;
  char data[MAX_READ_SIZE];
  FileInfoV2Set dest_file_set;

  ret = read_index(dest_ds_id_, dest_file_set);//read all files for blockid in dest ds
  if(TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "read index from dest ds: %s failed when check_integrity, blockid: %"PRI64_PREFIX"u, ret: %d.",
            tbsys::CNetUtil::addrToString(dest_ds_id_).c_str(), src_block_id_, ret);
  }
  else
  {
    FileInfoV2SetIter vit = dest_file_set.begin();
    for ( ; vit != dest_file_set.end(); ++vit)
    {
      uint64_t file_id = (*vit).id_;
      FSName fname_helper(src_block_id_, file_id);
      int fd = TfsClientImplV2::Instance()->open(fname_helper.get_name(), NULL, dest_ns_addr_.c_str(), T_READ);
      ret = fd >= 0 ? TFS_SUCCESS : fd;
      if (TFS_SUCCESS !=  ret)
      {
        TBSYS_LOG(ERROR, "read data from dest fail, blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, ret: %d",
            src_block_id_, file_id, ret);
      }
      else if ((ret = TfsClientImplV2::Instance()->set_option_flag(fd, READ_DATA_OPTION_FLAG_FORCE)) != TFS_SUCCESS)
      {
         TBSYS_LOG(ERROR, "set option flag failed. ret: %d", ret);
      }
      else
      {
        TfsFileStat dest_info;
        uint32_t crc = 0;
        int32_t total_size = 0;
        data[0] = '\0';
        while (true)
        {
          int32_t read_len = TfsClientImplV2::Instance()->readv2(fd, data, MAX_READ_SIZE, &dest_info);
          if (read_len < 0)
          {
            TBSYS_LOG(ERROR, "read data from dest fail, blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, ret: %d",
                 src_block_id_, file_id, read_len);
            ret = EXIT_READ_FILE_ERROR;
            break;
          }
          if (read_len == 0)
          {
            break;
          }

          total_size += read_len;
          crc = Func::crc(crc, data, read_len);
          if (read_len < MAX_READ_SIZE)
          {
            break;
          }
        }

        if (TFS_SUCCESS == ret)
        {
          FileInfoV2 find_info;
          find_info.id_ = file_id;
          FileInfoV2SetIter src_fit = src_file_set_.find(find_info);
          if (src_fit == src_file_set_.end())
          {
            TBSYS_LOG(ERROR, "dest cluster has redundant file which not exist in src file list, fatal error. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u",
                src_block_id_, file_id);
            ret = EXIT_INVALID_FILE_ID_ERROR;
          }
          else
          {
            if (crc != (*src_fit).crc_ || total_size != ((*src_fit).size_ - FILEINFO_EXT_SIZE) || crc != dest_info.crc_ || total_size != dest_info.size_)
            {
              TBSYS_LOG(ERROR, "blockid: %"PRI64_PREFIX"u, src crc: %u, size: %d, data crc: %u, size: %d, dest crc: %u, size: %"PRI64_PREFIX"d,"
                "src status:%d, dest status:%d\n", src_block_id_, (*src_fit).crc_, (*src_fit).size_ - FILEINFO_EXT_SIZE, crc, total_size, dest_info.crc_,
                dest_info.size_, (*src_fit).status_, dest_info.flag_);
              ret = EXIT_CHECK_CRC_ERROR;
            }
          }
        }
        TfsClientImplV2::Instance()->close(fd);
      }

      if (TFS_SUCCESS != ret)
      {
        atomic_inc(&stat_param_.copy_failure_);
      }
      else
      {
        atomic_inc(&stat_param_.copy_success_);
      }
    }
    stat_param_.total_ = dest_file_set.size();

    TBSYS_LOG(INFO, "blockid: %"PRI64_PREFIX"u check write integrity, total: %"PRI64_PREFIX"d, success: %d, fail: %d",
        src_block_id_, stat_param_.total_, atomic_read(&stat_param_.copy_success_), atomic_read(&stat_param_.copy_failure_));
    if (atomic_read(&stat_param_.copy_failure_) != 0)
    {
      ret = EXIT_CHECK_CRC_ERROR;
    }
  }

  return ret;
}

int TranBlock::rm_block_from_ns()
{
  int ret = TFS_SUCCESS;
  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_cmd(CLIENT_CMD_EXPBLK);
  req_cc_msg.set_value3(src_block_id_);
  req_cc_msg.set_value4(tfs::nameserver::HANDLE_DELETE_BLOCK_FLAG_ONLY_RELATION);

  NewClient* client = NewClientManager::get_instance().create_client();
  tbnet::Packet* rsp = NULL;
  if((ret = send_msg_to_server(Func::get_host_ip(dest_ns_addr_.c_str()), client, &req_cc_msg, rsp)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "send remove block from ns command failed. block_id: %"PRI64_PREFIX"u, ret: %d", src_block_id_, ret);
  }
  else
  {
    assert(NULL != rsp);
    if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* sm = dynamic_cast<StatusMessage*>(rsp);
      if (STATUS_MESSAGE_OK == sm->get_status())
      {
        TBSYS_LOG(INFO, "remove block from ns success, ns_addr: %s, blockid: %"PRI64_PREFIX"u",
            dest_ns_addr_.c_str(), src_block_id_);
      }
      else
      {
        TBSYS_LOG(ERROR, "remove block from ns fail, ns_addr: %s, blockid: %"PRI64_PREFIX"u, ret: %d",
            dest_ns_addr_.c_str(), src_block_id_, sm->get_status());
        ret = sm->get_status();
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "remove block from ns: %s fail, blockid: %"PRI64_PREFIX"u, unkonw msg type",
         dest_ns_addr_.c_str(), src_block_id_);
      ret = EXIT_UNKNOWN_MSGTYPE;
    }
  }
  NewClientManager::get_instance().destroy_client(client);
  return ret;
}

int TranBlock::rm_block_from_ds(uint64_t ds_id)
{
  int ret = TFS_SUCCESS;
  RemoveBlockMessageV2 req_rb_msg;
  req_rb_msg.set_block_id(src_block_id_);
  req_rb_msg.set_tmp_flag(false);
  NewClient* client = NewClientManager::get_instance().create_client();
  tbnet::Packet* rsp = NULL;
  if((ret = send_msg_to_server(ds_id, client, &req_rb_msg, rsp)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "send remove block from ds command failed. block_id: %"PRI64_PREFIX"u, ret: %d", src_block_id_, ret);
    rsp = NULL;
  }
  else
  {
    if (rsp != NULL && STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* sm = dynamic_cast<StatusMessage*>(rsp);
      if (STATUS_MESSAGE_OK == sm->get_status())
      {
        TBSYS_LOG(INFO, "remove block from dest ds success, ds_addr: %s, blockid: %"PRI64_PREFIX"u",
            tbsys::CNetUtil::addrToString(ds_id).c_str(), src_block_id_);
      }
      else
      {
        TBSYS_LOG(ERROR, "remove block from dest ds fail, ds_addr: %s, blockid: %"PRI64_PREFIX"u, ret: %d",
            tbsys::CNetUtil::addrToString(ds_id).c_str(), src_block_id_, sm->get_status());
        ret = TFS_ERROR;
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "write raw data to ds: %s fail, blockid: %"PRI64_PREFIX"u, unkonw msg type",
          tbsys::CNetUtil::addrToString(ds_id).c_str(), src_block_id_);
      ret = TFS_ERROR;
    }
  }
  NewClientManager::get_instance().destroy_client(client);
  return ret;
}

