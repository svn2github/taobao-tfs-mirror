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

    void CheckManager::run_check()
    {
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      while (!ds_info.is_destroyed())
      {
        uint64_t block_id = INVALID_BLOCK_ID;
        mutex_.lock();
        if (!pending_blocks_.empty())
        {
          block_id = pending_blocks_.front();
          pending_blocks_.pop_front();
        }
        else if (seqno_ > 0)
        {
          report_check_blocks();
          // reset check manager status, start a new turn
          seqno_ = 0;
        }
        mutex_.unlock();

        if (INVALID_BLOCK_ID != block_id)
        {
          // TODO: do real check work
          TBSYS_LOG(DEBUG, "check block %"PRI64_PREFIX"u", block_id);
          // if check success && blockid valid
          success_blocks_.push_back(block_id);
        }

        if (pending_blocks_.empty() && (0 == seqno_))
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
      LockT<Mutex> lock(mutex_);
      pending_blocks_.clear();
      success_blocks_.clear();
      seqno_ = message->get_seqno();
      int ret = (seqno_ > 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        check_server_id_ = message->get_server_id();
        VUINT64& blocks = message->get_blocks();
        VUINT64::iterator iter = blocks.begin();
        for ( ; iter != blocks.end(); iter++)
        {
          pending_blocks_.push_back(*iter);
        }
        message->reply(new StatusMessage(STATUS_MESSAGE_OK));
      }
      return ret;
    }

    int CheckManager::report_check_blocks()
    {
      ReportCheckBlockMessage rep_msg;
      rep_msg.set_seqno(seqno_);
      VUINT64& blocks = rep_msg.get_blocks();
      VUINT64::iterator iter = success_blocks_.begin();
      for ( ; iter != success_blocks_.end(); iter++)
      {
        blocks.push_back(*iter);
      }

      TBSYS_LOG(DEBUG, "report check status to %s", tbsys::CNetUtil::addrToString(check_server_id_).c_str());

      NewClient* client = NewClientManager::get_instance().create_client();
      return post_msg_to_server(check_server_id_, client, &rep_msg, Task::ds_task_callback);
    }

    int CheckManager::check_block(const uint64_t block_id)
    {
      UNUSED(block_id);
      return 0;
    }

    bool file_info_compare(const FileInfoV2& left, const FileInfoV2& right)
    {
      return left.id_ < right.id_;
    }

    void CheckManager::compare_block_fileinfos(vector<FileInfoV2>& left,
        vector<FileInfoV2>& right, VUINT64& more, VUINT64& less)
    {
      sort(right.begin(), right.end(), file_info_compare);
      std::vector<FileInfoV2>::iterator iter = left.begin();
      for ( ; iter != left.end(); iter++)
      {
        std::vector<FileInfoV2>::iterator fit = lower_bound(right.begin(),
            right.end(), *iter, file_info_compare);
        if (fit != right.end())
        {
          fit->id_ = INVALID_FILE_ID;  // tag it's a checked fileinfo
        }
        else
        {
          if (fit->id_ != iter->id_)
          {
            more.push_back(iter->id_);
          }
        }
      }

      iter = right.begin();
      for ( ; iter != right.end(); iter++)
      {
        if (INVALID_FILE_ID != iter->id_)
        {
          less.push_back(iter->id_);
        }
      }
    }
  }
}
