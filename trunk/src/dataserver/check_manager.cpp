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
 *
 */

#include "common/base_packet.h"
#include "message/message_factory.h"
#include "dataservice.h"
#include "check_manager.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace tbutil;
    using namespace common;
    using namespace message;
    using namespace std;

    CheckManager::CheckManager(DataService& service):
      service_(service)
    {
    }

    CheckManager::~CheckManager()
    {
    }

    inline BlockManager& CheckManager::get_block_manager()
    {
      return service_.get_block_manager();
    }

    inline DataHelper& CheckManager::get_data_helper()
    {
      return service_.get_data_helper();
    }

    void CheckManager::run_check()
    {
      const int32_t SLEEP_TIME_S = 1;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      while (!ds_info.is_destroyed())
      {
        mutex_.lock();
        if (!pending_.empty())
        {
          CheckParam* param = pending_.front();
          pending_.pop();
          mutex_.unlock();
          do_check(*param);
          tbsys::gDelete(param);
        }
        else
        {
          mutex_.unlock();
          sleep(SLEEP_TIME_S);
        }
      }
    }

    int CheckManager::handle(tbnet::Packet* packet)
    {
      int ret = TFS_SUCCESS;
      int pcode = packet->getPCode();
      switch(pcode)
      {
        case REQ_CHECK_BLOCK_MESSAGE:
          ret = get_check_blocks(dynamic_cast<CheckBlockRequestMessage*>(packet));
          break;
        case REPORT_CHECK_BLOCK_MESSAGE:
          ret = add_check_blocks(dynamic_cast<ReportCheckBlockMessage*>(packet));
          break;
        default:
          ret = TFS_ERROR;
          TBSYS_LOG(WARN, "unknown pcode : %d",  pcode);
          break;
      }
      return ret;
    }

    int CheckManager::get_check_blocks(CheckBlockRequestMessage* message)
    {
      const TimeRange& range = message->get_time_range();
      const int32_t group_count = message->get_group_count();
      const int32_t group_seq = message->get_group_seq();
      CheckBlockResponseMessage* reply = new (std::nothrow) CheckBlockResponseMessage();
      assert(NULL != reply);
      int ret = get_block_manager().get_blocks_in_time_range(range,
          reply->get_blocks(), group_count, group_seq);
      if (TFS_SUCCESS == ret)
      {
        message->reply(reply);
      }
      else
      {
        tbsys::gDelete(reply);
        TBSYS_LOG(WARN, "get check blocks fail. ret: %d", ret);
      }
      return ret;
    }

    int CheckManager::add_check_blocks(ReportCheckBlockMessage* message)
    {
      int ret = TFS_SUCCESS;
      Mutex::Lock lock(mutex_);
      if (pending_.size() >= static_cast<uint32_t>(MAX_CHECK_QUEUE_SIZE))
      {
        ret = EXIT_CHECK_QUEUE_FULL;
      }

      if (TFS_SUCCESS == ret)
      {
        CheckParam* param =  new (std::nothrow) CheckParam();
        assert(NULL != param);
        *param = message->get_param();
        pending_.push(param);
        ret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
      }

      return ret;
    }

    void CheckManager::do_check(const CheckParam& param)
    {
      DsRuntimeGlobalInformation& info = DsRuntimeGlobalInformation::instance();
      ReportCheckBlockResponseMessage rsp_msg;
      rsp_msg.set_seqno(param.seqno_);
      rsp_msg.set_server_id(info.information_.id_);
      check_block(param, rsp_msg.get_result());

      TBSYS_LOG(INFO, "report check status to %s, senqo: %"PRI64_PREFIX"d",
          tbsys::CNetUtil::addrToString(param.cs_id_).c_str(), param.seqno_);

      NewClient* client = NewClientManager::get_instance().create_client();
      int ret = post_msg_to_server(param.cs_id_, client, &rsp_msg, Task::ds_task_callback);
      if (TFS_SUCCESS != ret)
      {
        NewClientManager::get_instance().destroy_client(client);
      }
    }

    void CheckManager::check_block(const CheckParam& param, vector<CheckResult>& result)
    {
      vector<uint64_t>::const_iterator iter = param.blocks_.begin();
      for ( ; iter != param.blocks_.end(); iter++)
      {
        CheckResult current;
        check_single_block(*iter, param.peer_id_, param.flag_, current);
        result.push_back(current);
        if (param.interval_ > 0)
        {
          usleep(param.interval_ * 1000);
        }
      }
    }

    void CheckManager::check_single_block(const uint64_t block_id,
        const uint64_t peer_ip, const CheckFlag flag, CheckResult& result)
    {
      TIMER_START();
      IndexHeaderV2 main_header;
      vector<FileInfoV2> main_finfos;
      result.block_id_ = block_id;
      result.status_ = get_block_manager().traverse(main_header, main_finfos, block_id, block_id);
      if (TFS_SUCCESS == result.status_)
      {
        if (main_finfos.size() > 0)
        {
          result.status_ = check_single_block(block_id, main_finfos, peer_ip, flag, result);
        }
        else if (main_finfos.size() == 0)
        {
          TBSYS_LOG(DEBUG, "ignore empty block, won't check");
          result.more_ = 0;
          result.diff_ = 0;
          result.less_ = 0;
        }
      }
      TIMER_END();

      if (TFS_SUCCESS != result.status_)
      {
        TBSYS_LOG(WARN, "check block %"PRI64_PREFIX"u fail, ret: %d", block_id, result.status_);
      }
      else
      {
        TBSYS_LOG(DEBUG, "check block %"PRI64_PREFIX"u success. "
            "count: %zd, more: %d, diff: %d, less: %d, cost: %"PRI64_PREFIX"d",
            block_id, main_finfos.size(), result.more_, result.diff_, result.less_,
            TIMER_DURATION());
      }
    }

    int CheckManager::check_single_block(const uint64_t block_id,
        vector<FileInfoV2>& finfos, const uint64_t peer_ip, CheckFlag flag, CheckResult& result)
    {
      vector<uint64_t> replicas;
      IndexDataV2 peer_index;
      int ret = get_data_helper().get_block_replicas(peer_ip, block_id, replicas);
      if (TFS_SUCCESS == ret)
      {
        vector<uint64_t>::iterator iter = replicas.begin();
        for ( ; iter != replicas.end(); iter++)
        {
          ret = get_data_helper().read_index(*iter, block_id, block_id, peer_index);
          if (TFS_SUCCESS == ret)
          {
            break;
          }
        }
      }
      else if (EXIT_NO_BLOCK == ret || EXIT_BLOCK_NOT_FOUND == ret)
      {
        // block exist in peer cluster, we think the peer file list is empty
        ret = TFS_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "read block %"PRI64_PREFIX"u index fail, peer ns: %s, ret: %d",
            block_id, tbsys::CNetUtil::addrToString(peer_ip).c_str(), ret);
      }

      if (TFS_SUCCESS == ret)
      {
        vector<FileInfoV2> more;
        vector<FileInfoV2> diff;
        vector<FileInfoV2> less;
        compare_block_fileinfos(block_id, finfos, peer_index.finfos_, more, diff, less);

        // if CHECK_FLAG_SYNC not set, just compare, don't sync
        if (flag & CHECK_FLAG_SYNC)
        {
          ret = process_more_files(peer_ip, block_id, more);
          if (TFS_SUCCESS == ret)
          {
            ret = process_diff_files(peer_ip, block_id, diff);
          }
          if (TFS_SUCCESS == ret)
          {
            ret = process_less_files(peer_ip, block_id, less);
          }
        }

        if (TFS_SUCCESS == ret)
        {
          result.more_ = more.size();
          result.diff_ = diff.size();
          result.less_ = less.size();
        }
      }

      return ret;
    }

    int CheckManager::process_more_files(const uint64_t peer_ip,
        const uint64_t block_id, const vector<FileInfoV2>& more)
    {
      int ret = TFS_SUCCESS;
      vector<FileInfoV2>::const_iterator iter = more.begin();
      for ( ; (TFS_SUCCESS == ret) && (iter != more.end()); iter++)
      {
        SyncManager* manager = service_.get_sync_manager();
        if (NULL != manager)
        {
          ret =  manager->insert(peer_ip, 0, block_id, iter->id_, OPLOG_INSERT);
        }
        TBSYS_LOG(DEBUG, "MORE file compared with %s blockid %"PRI64_PREFIX"u fileid %"PRI64_PREFIX"u",
            tbsys::CNetUtil::addrToString(peer_ip).c_str(), block_id, iter->id_);
      }
      return ret;
    }

    int CheckManager::process_diff_files(const uint64_t peer_ip,
        const uint64_t block_id, const vector<FileInfoV2>& diff)
    {
      int ret = TFS_SUCCESS;
      vector<FileInfoV2>::const_iterator iter = diff.begin();
      for ( ; (TFS_SUCCESS == ret) && (iter != diff.end()); iter++)
      {
        SyncManager* manager = service_.get_sync_manager();
        if (NULL != manager)
        {
          ret =  manager->insert(peer_ip, 0, block_id, iter->id_, OPLOG_REMOVE);
        }
        TBSYS_LOG(DEBUG, "DIFF file compared with %s blockid %"PRI64_PREFIX"u fileid %"PRI64_PREFIX"u",
            tbsys::CNetUtil::addrToString(peer_ip).c_str(), block_id, iter->id_);
      }
      return ret;
    }

    int CheckManager::process_less_files(const uint64_t peer_ip,
        const uint64_t block_id, const vector<FileInfoV2>& less)
    {
      vector<FileInfoV2>::const_iterator iter = less.begin();
      for ( ; iter != less.end(); iter++)
      {
        // TODO: process less file in master cluster
        TBSYS_LOG(DEBUG, "LESS file compared with %s blockid %"PRI64_PREFIX"u fileid %"PRI64_PREFIX"u",
            tbsys::CNetUtil::addrToString(peer_ip).c_str(), block_id, iter->id_);
      }
      return TFS_SUCCESS;
    }

    void CheckManager::compare_block_fileinfos(const uint64_t block_id,
        const vector<FileInfoV2>& left,
        const vector<FileInfoV2>& right, vector<FileInfoV2>& more,
        vector<FileInfoV2>& diff, vector<FileInfoV2>& less)
    {
      // transform right vector to set for fast search
      set<FileInfoV2, FileInfoCompare> files;
      vector<FileInfoV2>::const_iterator iter = right.begin();
      for ( ; iter != right.end(); iter++)
      {
        files.insert(*iter);
      }

      iter = left.begin();
      for ( ; iter != left.end(); iter++)
      {
        // deleted or invalid, ignore
        if (iter->status_ & FI_DELETED)
        {
          TBSYS_LOG(DEBUG, "blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u has been deleted.",
              block_id, iter->id_);
          files.erase(*iter);
          continue;
        }

        set<FileInfoV2, FileInfoCompare>::iterator sit = files.find(*iter);
        if (sit == files.end()) // not found
        {
          more.push_back(*iter);
        }
        else
        {
          // size or crc diff, sync file data to peer
          if ((iter->size_ != sit->size_) || (iter->crc_ != sit->crc_))
          {
            more.push_back(*iter);
          }
          else if (iter->status_ != sit->status_) // file in diff status, do remove
          {
            diff.push_back(*iter);
          }
          files.erase(sit);
        }
      }

      set<FileInfoV2, FileInfoCompare>::iterator it = files.begin();
      for ( ; it != files.end(); it++)
      {
        if (!(it->status_ & FI_DELETED))
        {
          less.push_back(*it);
        }
      }
    }

  }
}
