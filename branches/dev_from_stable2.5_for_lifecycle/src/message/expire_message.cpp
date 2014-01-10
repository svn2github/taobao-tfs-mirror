/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: $
 *
 * Authors:
 *   qixiao.zs <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */
#include "common/stream.h"
#include "common/serialization.h"
#include "common/expire_define.h"

#include "expire_message.h"

using namespace tfs::common;


namespace tfs
{
  namespace message
  {
    //req_clean_task_msg
    ReqCleanTaskFromRtsMessage::ReqCleanTaskFromRtsMessage()
    {
      _packetHeader._pcode = REQ_EXPIRE_CLEAN_TASK_MESSAGE;
    }
    ReqCleanTaskFromRtsMessage::~ReqCleanTaskFromRtsMessage(){}

    int ReqCleanTaskFromRtsMessage::serialize(Stream& output) const
    {
      int64_t pos = 0;
      int32_t iret = task_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == iret)
      {
        iret = output.pour(task_.length());
      }

      return iret;
    }

    int ReqCleanTaskFromRtsMessage::deserialize(Stream& input)
    {
      int64_t pos = 0;
      int32_t iret = task_.deserialize(input.get_data(), input.get_data_length(), pos);

      if (TFS_SUCCESS == iret)
      {
        input.drain(task_.length());
      }

      return iret;
    }

    int64_t ReqCleanTaskFromRtsMessage::length() const
    {
      return task_.length();
    }

    /* finish task msg */

    ReqFinishTaskFromEsMessage::ReqFinishTaskFromEsMessage()
    {
      _packetHeader._pcode = REQ_RT_FINISH_TASK_MESSAGE;
    }
    ReqFinishTaskFromEsMessage::~ReqFinishTaskFromEsMessage(){}

    int ReqFinishTaskFromEsMessage::serialize(Stream& output) const
    {
      int iret = common::TFS_SUCCESS;
      int64_t pos = 0;

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(es_id_);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = task_.serialize(output.get_free(), output.get_free_length(), pos);
      }
      if (TFS_SUCCESS == iret)
      {
        iret = output.pour(task_.length());
      }
      return iret;
    }

    int ReqFinishTaskFromEsMessage::deserialize(Stream& input)
    {
      int32_t iret = TFS_SUCCESS;
      int64_t pos = 0;

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_int64(reinterpret_cast<int64_t*>(&es_id_));
      }
      if (TFS_SUCCESS == iret)
      {
        iret = task_.deserialize(input.get_data(), input.get_data_length(), pos);
      }

      if (TFS_SUCCESS == iret)
      {
        input.drain(task_.length());
      }
      return iret;
    }

    int64_t ReqFinishTaskFromEsMessage::length() const
    {
      return INT_SIZE + task_.length();
    }

    //heart msg
    ReqRtsEsHeartMessage::ReqRtsEsHeartMessage()
    {
      _packetHeader._pcode = common::REQ_RT_ES_KEEPALIVE_MESSAGE;
    }

    ReqRtsEsHeartMessage::~ReqRtsEsHeartMessage()
    {
    }

    int ReqRtsEsHeartMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t iret = base_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == iret)
      {
        input.drain(base_info_.length());
      }
      return iret;
    }

    int ReqRtsEsHeartMessage::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      int32_t iret = base_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == iret)
      {
        output.pour(base_info_.length());
      }
      return iret;
    }

    int64_t ReqRtsEsHeartMessage::length() const
    {
      return base_info_.length();
    }

    //rsp heart
    RspRtsEsHeartMessage::RspRtsEsHeartMessage()
    {
      _packetHeader._pcode = common::RSP_RT_ES_KEEPALIVE_MESSAGE;
    }

    RspRtsEsHeartMessage::~RspRtsEsHeartMessage()
    {

    }

    int RspRtsEsHeartMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(&heart_interval_);
      return iret;
    }

    int RspRtsEsHeartMessage::serialize(common::Stream& output) const
    {
      int32_t iret = output.set_int32(heart_interval_);
      return iret;
    }

    int64_t RspRtsEsHeartMessage::length() const
    {
      return INT_SIZE;
    }

    //req query task

    ReqQueryTaskMessage::ReqQueryTaskMessage()
    {
      _packetHeader._pcode = common::REQ_QUERY_TASK_MESSAGE;
    }

    ReqQueryTaskMessage::~ReqQueryTaskMessage(){}

    int ReqQueryTaskMessage::serialize(common::Stream &output) const
    {
      int32_t iret = output.set_int64(es_id_);

      return iret;
    }

    int ReqQueryTaskMessage::deserialize(common::Stream &input)
    {
      int32_t iret = input.get_int64((int64_t*)(&es_id_));

      return iret;
    }

    int64_t ReqQueryTaskMessage::length() const
    {
      return INT64_SIZE;
    }

    //rsp query task

    RspQueryTaskMessage::RspQueryTaskMessage()
    {
      _packetHeader._pcode = common::RSP_QUERY_TASK_MESSAGE;
    }

    RspQueryTaskMessage::~RspQueryTaskMessage(){}

    int RspQueryTaskMessage::serialize(common::Stream &output) const
    {
      int32_t res_running_tasks_count = static_cast<int32_t>(res_running_tasks_.size());
      int32_t iret = output.set_int32(res_running_tasks_count);
      if (common::TFS_SUCCESS == iret)
      {
        for (int32_t i = 0; common::TFS_SUCCESS == iret && i < res_running_tasks_count; i++)
        {
          int64_t pos = 0;
          iret = res_running_tasks_[i].serialize(output.get_free(), output.get_free_length(), pos);
          if (common::TFS_SUCCESS == iret)
          {
            output.pour(res_running_tasks_[i].length());
          }
        }
      }
      return iret;
    }

    int RspQueryTaskMessage::deserialize(common::Stream &input)
    {
      int32_t res_running_tasks_count = -1;
      int32_t iret = common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&res_running_tasks_count);
      }

      if (common::TFS_SUCCESS == iret)
      {
        ServerExpireTask one_tasks_;
        for (int i = 0; common::TFS_SUCCESS == iret && i < res_running_tasks_count; i++)
        {
          int64_t pos = 0;
          iret = one_tasks_.deserialize(input.get_data(), input.get_data_length(), pos);
          if (common::TFS_SUCCESS == iret)
          {
            res_running_tasks_.push_back(one_tasks_);
            input.drain(one_tasks_.length());
          }
        }
      }

      return iret;
    }

    int64_t RspQueryTaskMessage::length() const
    {
      int64_t len = 0;
      len = common::INT_SIZE;

      for (size_t i = 0; i < res_running_tasks_.size(); i++)
      {
        len +=  res_running_tasks_[i].length();
      }
      return len;
    }



  }/** message **/
}/** tfs **/
