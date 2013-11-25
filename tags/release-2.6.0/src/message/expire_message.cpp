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
        iret = output.set_int32(task_time_);
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
        iret = input.get_int32(&task_time_);
      }

      return iret;
    }

    int64_t ReqCleanTaskFromRtsMessage::length() const
    {
      return INT_SIZE * 5;
    }

    /* finish task msg */

    ReqFinishTaskFromEsMessage::ReqFinishTaskFromEsMessage()
    {
      _packetHeader._pcode = REQ_RT_FINISH_TASK_MESSAGE;
    }
    ReqFinishTaskFromEsMessage::~ReqFinishTaskFromEsMessage(){}

    int ReqFinishTaskFromEsMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_int32(reserve_);

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(es_id_);
      }
      return iret;
    }

    int ReqFinishTaskFromEsMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_int32(&reserve_);

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_int64(reinterpret_cast<int64_t*>(&es_id_));
      }
      return iret;
    }

    int64_t ReqFinishTaskFromEsMessage::length() const
    {
      return INT_SIZE;
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

    //req query progress

    ReqQueryProgressMessage::ReqQueryProgressMessage()
    {
      _packetHeader._pcode = common::REQ_QUERY_PROGRESS_MESSAGE;
    }

    ReqQueryProgressMessage::~ReqQueryProgressMessage(){}

    int ReqQueryProgressMessage::serialize(common::Stream &output) const
    {
      int32_t iret = output.set_int64(es_id_);

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(es_num_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(task_time_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(hash_bucket_id_);
      }

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(type_);
      }

      return iret;
    }

    int ReqQueryProgressMessage::deserialize(common::Stream &input)
    {
      int32_t iret = input.get_int64((int64_t*)(&es_id_));

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&es_num_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&task_time_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&hash_bucket_id_);
      }

      if (TFS_SUCCESS == iret)
      {
        iret = input.get_int32(reinterpret_cast<int32_t*>(&type_));
      }

      return iret;
    }

    int64_t ReqQueryProgressMessage::length() const
    {
      return 4 * INT_SIZE + INT64_SIZE;
    }

    //rsp query progress

    RspQueryProgressMessage::RspQueryProgressMessage()
    {
      _packetHeader._pcode = common::RSP_QUERY_PROGRESS_MESSAGE;
    }

    RspQueryProgressMessage::~RspQueryProgressMessage(){}

    int RspQueryProgressMessage::serialize(common::Stream &output) const
    {
      int32_t iret = output.set_int32(sum_file_num_);

      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(current_percent_);
      }

      return iret;
    }

    int RspQueryProgressMessage::deserialize(common::Stream &input)
    {
      int32_t iret = input.get_int32(&sum_file_num_);

      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&current_percent_);
      }

      return iret;
    }

    int64_t RspQueryProgressMessage::length() const
    {
      return 2 * INT_SIZE;
    }



  }/** message **/
}/** tfs **/
