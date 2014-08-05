/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: base_packet.cpp 213 2011-04-22 16:22:51Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#include "http_packet.h"
namespace tfs
{
  namespace common
  {
    HttpPacket::HttpPacket()
    {
    }

    HttpPacket::~HttpPacket()
    {
    }

    bool HttpPacket::encode(tbnet::DataBuffer *output)
    {
      bool bret = NULL != output;
      if (bret)
      {
        stream_.clear();
        stream_.expand(length());
        int32_t ret = serialize(stream_);
        bret = TFS_SUCCESS == ret;
        if (bret && stream_.get_data_length() > 0)
        {
          output->writeBytes(stream_.get_data(), stream_.get_data_length());
        }
      }
      return bret;
    }

    bool HttpPacket::decode(tbnet::DataBuffer* input, tbnet::PacketHeader* header )
    {
      bool bret = NULL != input && NULL != header;
      if (bret)
      {
        header->_pcode = (header->_pcode & 0xFFFF);
        int64_t length = header->_dataLen;
        bret = length > 0 && input->getDataLen() >= length;
        if (!bret)
        {
          TBSYS_LOG(ERROR, "invalid packet: %d, length: %"PRI64_PREFIX"d  input buffer length: %d",
            header->_pcode, length, input->getDataLen());
          input->clear();
        }

        if(bret)
        {
          stream_.clear();
          stream_.expand(length);
          stream_.set_bytes(input->getData(), length);
          input->drainData(length);
          int32_t iret = deserialize(stream_);
          bret = TFS_SUCCESS == iret;
          if (!bret)
          {
              TBSYS_LOG(ERROR, "packet: %d deserialize error, iret: %d", header->_pcode, iret);
          }
        }
      }
      return bret;
    }
  }/** end namespace common **/
}/** end namespace tfs **/
