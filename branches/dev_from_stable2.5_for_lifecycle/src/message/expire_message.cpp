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
      int32_t iret = output.set_int32(task_type_);

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_int32(total_es_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_int32(num_es_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_int32(note_interval_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = output.set_int64(task_time_);
      }
      return iret;
    }

    int ReqCleanTaskFromRtsMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_int32(&task_type_);

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&total_es_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&num_es_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&note_interval_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_int64(&task_time_);
      }

      return iret;
    }

    int64_t ReqCleanTaskFromRtsMessage::length() const
    {
      return INT_SIZE * 4 + INT64_SIZE;
    }

    /* finish task msg */

    ReqFinishTaskFromEsMessage::ReqFinishTaskFromEsMessage()
    {
      _packetHeader._pcode = REQ_EXPIRE_FINISH_TASK_MESSAGE;
    }
    ReqFinishTaskFromEsMessage::~ReqFinishTaskFromEsMessage(){}

    int ReqFinishTaskFromEsMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_int32(reserve_);
      return iret;
    }

    int ReqFinishTaskFromEsMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_int32(&reserve_);
      return iret;
    }

    int64_t ReqFinishTaskFromEsMessage::length() const
    {
      return INT_SIZE;
    }

    //heart msg
    RtsEsHeartMessage::RtsEsHeartMessage()
    {
      _packetHeader._pcode = common::REQ_RT_ES_KEEPALIVE_MESSAGE;
    }

    RtsEsHeartMessage::~RtsEsHeartMessage()
    {
    }

    int RtsEsHeartMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t iret = base_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == iret)
      {
        input.drain(base_info_.length());
      }
      return iret;
    }

    int RtsEsHeartMessage::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      int32_t iret = base_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == iret)
      {
        output.pour(base_info_.length());
      }
      return iret;
    }

    int64_t RtsEsHeartMessage::length() const
    {
      return base_info_.length();
    }

    //rsp heart
    RtsEsHeartResponseMessage::RtsEsHeartResponseMessage()
    {
      _packetHeader._pcode = common::RSP_RT_ES_KEEPALIVE_MESSAGE;
    }

    RtsEsHeartResponseMessage::~RtsEsHeartResponseMessage()
    {

    }

    int RtsEsHeartResponseMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(&heart_interval_);
      return iret;
    }

    int RtsEsHeartResponseMessage::serialize(common::Stream& output) const
    {
      int32_t iret = output.set_int32(heart_interval_);
      return iret;
    }

    int64_t RtsEsHeartResponseMessage::length() const
    {
      return INT_SIZE;
    }

  }/** message **/
}/** tfs **/
