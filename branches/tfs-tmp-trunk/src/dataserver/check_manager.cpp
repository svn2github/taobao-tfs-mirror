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
      service_(service), seqno_(0), check_server_id_(INVALID_SERVER_ID)
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

    std::vector<SyncBase*>& CheckManager::get_sync_mirror()
    {
      return service_.get_sync_mirror();
    }

    void CheckManager::run_check()
    {
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      while (!ds_info.is_destroyed())
      {
        bool need_report = false;
        int64_t seqno = seqno_;
        uint64_t block_id = INVALID_BLOCK_ID;
        mutex_.lock();
        if (!pending_blocks_.empty())
        {
          block_id = pending_blocks_.front();
          pending_blocks_.pop_front();

          // this is the last block that needs to be checked
          // after check finish, report to checkserver
          if (pending_blocks_.empty() && (seqno != 0))
          {
            need_report = true;
          }
        }
        mutex_.unlock();

        if (!ds_info.is_destroyed() && (INVALID_BLOCK_ID != block_id))
        {
          TBSYS_LOG(DEBUG, "check block %"PRI64_PREFIX"u", block_id);
          if (TFS_SUCCESS == check_block(block_id))
          {
            mutex_.lock();
            success_blocks_.push_back(block_id);
            mutex_.unlock();
          }
        }

        // during check the last block, a new task may already come
        // the previous task will just need to be ignored
        // so we check if seqno matches here to avoid lock
        if (!ds_info.is_destroyed() && need_report && (seqno_ == seqno))
        {
          report_check_blocks();
          seqno_ = 0;
          need_report = false;
        }

        if (!ds_info.is_destroyed() && pending_blocks_.empty())
        {
          sleep(2);
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
      CheckBlockResponseMessage* reply = new (std::nothrow) CheckBlockResponseMessage();
      assert(NULL != reply);
      int ret = get_block_manager().get_blocks_in_time_range(range, reply->get_blocks());
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
      int64_t seqno = message->get_seqno();
      uint64_t check_server_id = message->get_server_id();
      const VUINT64& blocks = message->get_blocks();
      int ret = ((seqno > 0) && (INVALID_SERVER_ID != check_server_id)) ?
        TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        add_check_blocks(seqno, check_server_id, blocks);
        ret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
      }
      return ret;
    }

    void CheckManager::add_check_blocks(const int64_t seqno, const uint64_t check_server_id, const common::VUINT64& blocks)
    {
      Mutex::Lock lock(mutex_);
      pending_blocks_.clear();
      success_blocks_.clear();
      seqno_ = seqno;
      check_server_id_ = check_server_id;
      VUINT64::const_iterator iter = blocks.begin();
      for ( ; iter != blocks.end(); iter++)
      {
        pending_blocks_.push_back(*iter);
      }
    }

    int CheckManager::report_check_blocks()
    {
      ReportCheckBlockMessage rep_msg;
      rep_msg.set_seqno(seqno_);
      VUINT64& blocks = rep_msg.get_blocks();
      mutex_.lock();
      VUINT64::iterator iter = success_blocks_.begin();
      for ( ; iter != success_blocks_.end(); iter++)
      {
        blocks.push_back(*iter);
      }
      mutex_.unlock();

      TBSYS_LOG(INFO, "report check status to %s", tbsys::CNetUtil::addrToString(check_server_id_).c_str());

      NewClient* client = NewClientManager::get_instance().create_client();
      return post_msg_to_server(check_server_id_, client, &rep_msg, Task::ds_task_callback);
    }

    int CheckManager::check_block(const uint64_t block_id)
    {
      TIMER_START();
      IndexHeaderV2 main_header;
      vector<FileInfoV2> main_finfos;
      int ret = get_block_manager().traverse(main_header, main_finfos, block_id, block_id);
      if (TFS_SUCCESS == ret && main_finfos.size() > 0) // ignore empty block
      {
        vector<SyncBase*>& sync_mirror = get_sync_mirror();
        vector<SyncBase*>::iterator iter = sync_mirror.begin();
        for ( ; (TFS_SUCCESS == ret) && (iter != sync_mirror.end()); iter++)
        {
          ret = check_single_block(block_id, main_finfos, **iter);
        }
      }
      TIMER_END();
      TBSYS_LOG(DEBUG, "check block %"PRI64_PREFIX"u %s, file count: %zd, cost: %"PRI64_PREFIX"d",
          block_id, (TFS_SUCCESS == ret) ? "success" : "fail", main_finfos.size(), TIMER_DURATION());

      return ret;
    }

    int CheckManager::check_single_block(const uint64_t block_id,
        vector<FileInfoV2>& finfos, SyncBase& peer)
    {
      uint64_t peer_ns = Func::get_host_ip(peer.get_dest_addr().c_str());
      vector<uint64_t> replicas;
      IndexDataV2 peer_index;
      int ret = get_data_helper().get_block_replicas(peer_ns, block_id, replicas);
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
            block_id, tbsys::CNetUtil::addrToString(peer_ns).c_str(), ret);
      }

      if (TFS_SUCCESS == ret)
      {
        vector<FileInfoV2> more;
        vector<FileInfoV2> diff;
        vector<FileInfoV2> less;
        compare_block_fileinfos(finfos, peer_index.finfos_, more, diff, less);
        ret = process_more_files(peer, block_id, more);
        if (TFS_SUCCESS == ret)
        {
          ret = process_diff_files(peer, block_id, diff);
        }
        if (TFS_SUCCESS == ret)
        {
          ret = process_less_files(peer, block_id, less);
        }
      }

      return ret;
    }

    int CheckManager::process_more_files(SyncBase& peer,
        const uint64_t block_id, const vector<FileInfoV2>& more)
    {
      int ret = TFS_SUCCESS;
      vector<FileInfoV2>::const_iterator iter = more.begin();
      for ( ; (TFS_SUCCESS == ret) && (iter != more.end()); iter++)
      {
        // ignore deleted and invalid files
        if (!(iter->status_ & FI_DELETED) && !(iter->status_ & FI_INVALID))
        {
          ret = peer.write_sync_log(OPLOG_INSERT, block_id, iter->id_);
        }
      }
      return ret;
    }

    int CheckManager::process_diff_files(SyncBase& peer,
        const uint64_t block_id, const vector<FileInfoV2>& diff)
    {
      int ret = TFS_SUCCESS;
      vector<FileInfoV2>::const_iterator iter = diff.begin();
      for ( ; (TFS_SUCCESS == ret) && (iter != diff.end()); iter++)
      {
        ret = peer.write_sync_log(OPLOG_REMOVE, block_id, iter->id_);
      }
      return ret;
    }

    // TODO: process less files
    int CheckManager::process_less_files(SyncBase& peer,
        const uint64_t block_id, const vector<FileInfoV2>& less)
    {
      UNUSED(peer);
      UNUSED(block_id);
      UNUSED(less);
      return TFS_SUCCESS;
    }

    struct FileInfoCompare
    {
      bool operator () (const FileInfoV2& left, const FileInfoV2& right)
      {
        return left.id_ < right.id_;
      }
    };

    void CheckManager::compare_block_fileinfos(const vector<FileInfoV2>& left,
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
        less.push_back(*it);
      }
    }

  }
}
