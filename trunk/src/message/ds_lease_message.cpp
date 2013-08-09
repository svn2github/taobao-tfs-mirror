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
 */

#include "ds_lease_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    DsApplyLeaseMessage::DsApplyLeaseMessage()
    {
      _packetHeader._pcode = common::DS_APPLY_LEASE_MESSAGE;
    }

    DsApplyLeaseMessage::~DsApplyLeaseMessage()
    {
    }

    int DsApplyLeaseMessage::serialize(common::Stream& output)  const
    {
      int64_t pos = 0;
      int ret = ds_stat_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        output.pour(ds_stat_.length());
      }
      return ret;
    }

    int DsApplyLeaseMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int ret = ds_stat_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drain(ds_stat_.length());
      }
      return ret;
    }

    int64_t DsApplyLeaseMessage::length() const
    {
      return ds_stat_.length();
    }

    DsApplyLeaseResponseMessage::DsApplyLeaseResponseMessage()
    {
      _packetHeader._pcode = common::DS_APPLY_LEASE_RESPONSE_MESSAGE;
    }

    DsApplyLeaseResponseMessage::~DsApplyLeaseResponseMessage()
    {
    }

    int DsApplyLeaseResponseMessage::serialize(common::Stream& output)  const
    {
      int64_t pos = 0;
      int ret = lease_meta_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        output.pour(lease_meta_.length());
      }
      return ret;
    }

    int DsApplyLeaseResponseMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int ret = lease_meta_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drain(lease_meta_.length());
      }
      return ret;
    }

    int64_t DsApplyLeaseResponseMessage::length() const
    {
      return lease_meta_.length();
    }

    DsRenewLeaseMessage::DsRenewLeaseMessage(): size_(0)
    {
      _packetHeader._pcode = common::DS_RENEW_LEASE_MESSAGE;
    }

    DsRenewLeaseMessage::~DsRenewLeaseMessage()
    {
    }

    int DsRenewLeaseMessage::serialize(common::Stream& output)  const
    {
      int ret = DsApplyLeaseMessage::serialize(output);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(size_);
      }

      if (TFS_SUCCESS == ret)
      {
        for (int index = 0; index < size_ && TFS_SUCCESS == ret; index++)
        {
          int64_t pos = 0;
          ret = block_infos_[index].serialize(output.get_free(), output.get_free_length(), pos);
          if (TFS_SUCCESS == ret)
          {
            output.pour(block_infos_[index].length());
          }
        }
      }
      return ret;
    }

    int DsRenewLeaseMessage::deserialize(common::Stream& input)
    {
      int ret = DsApplyLeaseMessage::deserialize(input);
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&size_);
      }

      if (TFS_SUCCESS == ret)
      {
        for (int index = 0; index < size_ && TFS_SUCCESS == ret; index++)
        {
          int64_t pos = 0;
          ret = block_infos_[index].deserialize(input.get_data(), input.get_data_length(), pos);
          if (TFS_SUCCESS == ret)
          {
            input.drain(block_infos_[index].length());
          }
        }
      }
      return ret;
    }

    int64_t DsRenewLeaseMessage::length() const
    {
      BlockInfoV2 info;
      return DsApplyLeaseMessage::length() + INT_SIZE + size_ * info.length();
    }

    DsRenewLeaseResponseMessage::DsRenewLeaseResponseMessage(): size_(0)
    {
      _packetHeader._pcode = common::DS_RENEW_LEASE_RESPONSE_MESSAGE;
    }

    DsRenewLeaseResponseMessage::~DsRenewLeaseResponseMessage()
    {
    }

    int DsRenewLeaseResponseMessage::serialize(common::Stream& output)  const
    {
      int ret = DsApplyLeaseResponseMessage::serialize(output);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(size_);
      }

      if (TFS_SUCCESS == ret)
      {
        for (int index = 0; index < size_ && TFS_SUCCESS == ret; index++)
        {
          int64_t pos = 0;
          ret = block_lease_[index].serialize(output.get_free(), output.get_free_length(), pos);
          if (TFS_SUCCESS == ret)
          {
            output.pour(block_lease_[index].length());
          }
        }
      }
      return ret;
    }

    int DsRenewLeaseResponseMessage::deserialize(common::Stream& input)
    {
      int ret = DsApplyLeaseResponseMessage::deserialize(input);
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&size_);
      }

      if (TFS_SUCCESS == ret)
      {
        for (int index = 0; index < size_ && TFS_SUCCESS == ret; index++)
        {
          int64_t pos = 0;
          ret = block_lease_[index].deserialize(input.get_data(), input.get_data_length(), pos);
          if (TFS_SUCCESS == ret)
          {
            input.drain(block_lease_[index].length());
          }
        }
      }
      return ret;
    }

    int64_t DsRenewLeaseResponseMessage::length() const
    {
      int64_t len = DsApplyLeaseResponseMessage::length() + INT_SIZE;
      for (int index = 0; index < size_; index++)
      {
        len += block_lease_[index].length();
      }
      return len;
    }

    DsGiveupLeaseMessage::DsGiveupLeaseMessage()
    {
      _packetHeader._pcode = common::DS_GIVEUP_LEASE_MESSAGE;
    }

    DsGiveupLeaseMessage::~DsGiveupLeaseMessage()
    {
    }

    int DsGiveupLeaseMessage::serialize(common::Stream& output)  const
    {
      return DsRenewLeaseMessage::serialize(output);
    }

    int DsGiveupLeaseMessage::deserialize(common::Stream& input)
    {
      return DsRenewLeaseMessage::deserialize(input);
    }

    int64_t DsGiveupLeaseMessage::length() const
    {
      return DsRenewLeaseMessage::length();
    }

    DsApplyBlockMessage::DsApplyBlockMessage()
    {
      _packetHeader._pcode = common::DS_APPLY_BLOCK_MESSAGE;
    }

    DsApplyBlockMessage::~DsApplyBlockMessage()
    {
    }

    int DsApplyBlockMessage::serialize(common::Stream& output)  const
    {
      int ret = output.set_int64(server_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(count_);
      }
      return ret;
    }

    int DsApplyBlockMessage::deserialize(common::Stream& input)
    {
      int ret = input.get_int64(reinterpret_cast<int64_t*>(&server_id_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&count_);
      }
      return ret;
    }

    int64_t DsApplyBlockMessage::length() const
    {
      return INT_SIZE + INT64_SIZE;
    }

    DsApplyBlockResponseMessage::DsApplyBlockResponseMessage()
    {
      _packetHeader._pcode = common::DS_APPLY_BLOCK_RESPONSE_MESSAGE;
    }

    DsApplyBlockResponseMessage::~DsApplyBlockResponseMessage()
    {
    }

    int DsApplyBlockResponseMessage::serialize(common::Stream& output)  const
    {
      int ret = output.set_int32(size_);
      if (TFS_SUCCESS == ret)
      {
        for (int index = 0; index < size_ && TFS_SUCCESS == ret; index++)
        {
          int64_t pos = 0;
          ret = block_lease_[index].serialize(output.get_free(), output.get_free_length(), pos);
          if (TFS_SUCCESS == ret)
          {
            output.pour(block_lease_[index].length());
          }
        }
      }
      return ret;
    }

    int DsApplyBlockResponseMessage::deserialize(common::Stream& input)
    {
      int ret = input.get_int32(&size_);
      if (TFS_SUCCESS == ret)
      {
        for (int index = 0; index < size_ && TFS_SUCCESS == ret; index++)
        {
          int64_t pos = 0;
          ret = block_lease_[index].deserialize(input.get_data(), input.get_data_length(), pos);
          if (TFS_SUCCESS == ret)
          {
            input.drain(block_lease_[index].length());
          }
        }
      }
      return ret;
    }

    int64_t DsApplyBlockResponseMessage::length() const
    {
      int len = INT_SIZE;
      for (int index = 0; index < size_; index++)
      {
        len += block_lease_[index].length();
      }
      return len;

    }

    DsApplyBlockForUpdateMessage::DsApplyBlockForUpdateMessage()
    {
      _packetHeader._pcode = common::DS_APPLY_BLOCK_FOR_UPDATE_MESSAGE;
    }

    DsApplyBlockForUpdateMessage::~DsApplyBlockForUpdateMessage()
    {
    }

    int DsApplyBlockForUpdateMessage::serialize(common::Stream& output)  const
    {
      int ret = output.set_int64(block_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(server_id_);
      }
      return ret;
    }

    int DsApplyBlockForUpdateMessage::deserialize(common::Stream& input)
    {
      int ret = input.get_int64(reinterpret_cast<int64_t*>(&server_id_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t*>(&block_id_));
      }
      return ret;
    }

    int64_t DsApplyBlockForUpdateMessage::length() const
    {
      return INT64_SIZE * 2;
    }

    DsApplyBlockForUpdateResponseMessage::DsApplyBlockForUpdateResponseMessage()
    {
      _packetHeader._pcode = common::DS_APPLY_BLOCK_FOR_UPDATE_RESPONSE_MESSAGE;
    }

    DsApplyBlockForUpdateResponseMessage::~DsApplyBlockForUpdateResponseMessage()
    {
    }

    int DsApplyBlockForUpdateResponseMessage::serialize(common::Stream& output)  const
    {
      int64_t pos = 0;
      int ret = block_lease_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        output.pour(block_lease_.length());
      }
      return ret;
    }

    int DsApplyBlockForUpdateResponseMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int ret = block_lease_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drain(block_lease_.length());
      }
      return ret;
    }

    int64_t DsApplyBlockForUpdateResponseMessage::length() const
    {
      return block_lease_.length();
    }

    DsGiveupBlockMessage::DsGiveupBlockMessage()
    {
      _packetHeader._pcode = common::DS_GIVEUP_BLOCK_MESSAGE;
    }

    DsGiveupBlockMessage::~DsGiveupBlockMessage()
    {
    }

    int DsGiveupBlockMessage::serialize(common::Stream& output)  const
    {
      int ret = output.set_int64(server_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(size_);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int index = 0; index < size_ && TFS_SUCCESS == ret; index++)
        {
          int64_t pos = 0;
          ret = block_infos_[index].serialize(output.get_free(), output.get_free_length(), pos);
          if (TFS_SUCCESS == ret)
          {
            output.pour(block_infos_[index].length());
          }
        }
      }
      return ret;
    }

    int DsGiveupBlockMessage::deserialize(common::Stream& input)
    {
      int ret = input.get_int64(reinterpret_cast<int64_t*>(&server_id_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&size_);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int index = 0; index < size_ && TFS_SUCCESS == ret; index++)
        {
          int64_t pos = 0;
          ret = block_infos_[index].deserialize(input.get_data(), input.get_data_length(), pos);
          if (TFS_SUCCESS == ret)
          {
            input.drain(block_infos_[index].length());
          }
        }
      }
      return ret;
    }

    int64_t DsGiveupBlockMessage::length() const
    {
      BlockInfoV2 info;
      return INT64_SIZE + INT_SIZE + size_ * info.length();
    }

    DsGiveupBlockResponseMessage::DsGiveupBlockResponseMessage()
    {
      _packetHeader._pcode = common::DS_GIVEUP_BLOCK_RESPONSE_MESSAGE;
    }

    DsGiveupBlockResponseMessage::~DsGiveupBlockResponseMessage()
    {
    }

    int DsGiveupBlockResponseMessage::serialize(common::Stream& output)  const
    {
      return DsApplyBlockResponseMessage::serialize(output);
    }

    int DsGiveupBlockResponseMessage::deserialize(common::Stream& input)
    {
      return DsApplyBlockResponseMessage::deserialize(input);
    }

    int64_t DsGiveupBlockResponseMessage::length() const
    {
      return DsApplyBlockResponseMessage::length();
    }

  }/** end namespace message **/
}/** end namespace tfs **/

