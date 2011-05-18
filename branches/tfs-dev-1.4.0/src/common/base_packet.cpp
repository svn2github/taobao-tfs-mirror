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
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include <Service.h>
#include "base_packet.h"
#include "base_service.h"
#include "status_packet.h"

namespace tfs
{
  namespace common 
  {
    BasePacket::BasePacket():
      connection_(NULL),
      id_(0),
      crc_(0),
      direction_(DIRECTION_SEND),
      version_(TFS_PACKET_VERSION_V2),
      auto_free_(true),
      dump_flag_(false)
    {

    }

    BasePacket::~BasePacket()
    {

    }
    
    bool BasePacket::copy(BasePacket* src, const int32_t version, const bool deserialize)
    {
      bool bret = NULL != src;
      if (bret)
      {
        bret = getPCode() == src->getPCode() && src != this;
        if (bret)
        {
          //base class
          _next = NULL;
          _channel = NULL;
          _expireTime = 0;
          memcpy(&_packetHeader, &src->_packetHeader, sizeof(tbnet::PacketHeader));

          //self members 
          id_ = src->get_id();
          crc_ = src->get_crc();
          version_ = version >= 0 ? version : src->get_version();
          auto_free_ = src->get_auto_free();
          connection_ = src->get_connection();
          direction_  = src->get_direction();

          stream_.clear();
          stream_.expand(length());
          int32_t iret = src->serialize(stream_);
          bret = TFS_SUCCESS == iret;
          if (bret)
          {
            //recalculate crc
            if (version >= 1)
            {
              crc_ = common::Func::crc(TFS_PACKET_FLAG_V1, stream_.get_data(), stream_.get_data_length());
            }
            if (deserialize)
            {
              iret = this->deserialize(stream_);
              bret = TFS_SUCCESS == iret;
            }
          }
        }
      }
      return bret;
    }

    bool BasePacket::encode(tbnet::DataBuffer* output)
    {
      bool bret = NULL != output;
      if (bret)
      {
        if (stream_.get_data_length() > 0)
        {
          output->writeBytes(stream_.get_data(), stream_.get_data_length());
        }
      }
      return bret;
    }

    bool BasePacket::decode(tbnet::DataBuffer* input, tbnet::PacketHeader* header )
    {
      bool bret = NULL != input && NULL != header;
      if (bret)
      {
        version_ = ((header->_pcode >> 16) & 0xFFFF);
        header->_pcode = (header->_pcode & 0xFFFF);
        int64_t length = header->_dataLen;
        bret = length > 0 && input->getDataLen() >= length;
        if (bret)
        {
          if (version_ >= TFS_PACKET_VERSION_V1)
          {
            length -= TFS_PACKET_HEADER_DIFF_SIZE;
          }
          //both v1 and v2 have id and crc
          if (version_ >= TFS_PACKET_VERSION_V1)
          {
            int64_t pos = 0;
            int32_t iret = Serialization::get_int64(input->getData(), input->getDataLen(), pos, reinterpret_cast<int64_t*>(&id_));
            if (TFS_SUCCESS == iret)
              input->drainData(INT64_SIZE);
            else
              bret = false;
            if (bret)
            {
              pos = 0;
              iret = Serialization::get_int32(input->getData(), input->getDataLen(), pos, reinterpret_cast<int32_t*>(&crc_));
              if (TFS_SUCCESS == iret)
                input->drainData(INT_SIZE);
              else
                bret = false;
              if (bret)
              {
                uint32_t crc = Func::crc(TFS_PACKET_FLAG_V1, input->getData(), length); 
                bret = crc == crc_;
                if (!bret)
                {
                  TBSYS_LOG(ERROR, "decode packet crc check error, header crc(%u), calc crc(%u)",
                      crc_, crc);
                }
              }
              else
              {
                TBSYS_LOG(ERROR, "crc deserialize error, iret: %d", iret);
              }
            }
            else
            {
              TBSYS_LOG(ERROR, "channel id deserialize error, iret: %d", iret);
            }
          }
          if (bret)
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
      }
      return bret;
    }

    int BasePacket::reply(BasePacket* packet)
    {
      int32_t iret = NULL != packet ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        if (0 == getChannelId())
        {
          iret = TFS_ERROR;
          TBSYS_LOG(ERROR, "message (%d) channel is null, reply message (%d)", getPCode(), packet->getPCode());
        }
        if (TFS_SUCCESS == iret)
        {
          if (((direction_ & DIRECTION_RECEIVE) && _expireTime > 0) 
                && (tbsys::CTimeUtil::getTime() > _expireTime))
          {
            iret = TFS_ERROR;
            TBSYS_LOG(ERROR, "message (%d), timeout for response, reply message (%d)", getPCode(), packet->getPCode());
          }
        }

        if (TFS_SUCCESS == iret)
        {
          packet->setChannelId(getChannelId());
          packet->set_id(id_ + 1);
          packet->set_version(version_);

          BaseService* service = dynamic_cast<BaseService*>(tbutil::Service::instance());
          //clone & serialize message
          tbnet::Packet* reply_msg = service->get_packet_factory()->clone_packet(packet, TFS_PACKET_VERSION_V2, false);
          iret = NULL != reply_msg ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS == iret)
          {
            if (is_enable_dump())
            {
              dump();  
              dynamic_cast<BasePacket*>(reply_msg)->dump();
            }
            //post message
            bool bret= connection_->postPacket(reply_msg);
            iret = bret ? TFS_SUCCESS : TFS_ERROR;
            if (TFS_SUCCESS != iret)
            {
              TBSYS_LOG(ERROR, "post packet failure, server: %s, pcode:%d",
                tbsys::CNetUtil::addrToString(connection_->getServerId()).c_str(), packet->getPCode());
              reply_msg->free();
            }
          }
        }
      }
      return iret;
    }

    int BasePacket::reply_error_packet(const int32_t level, const char* file, const int32_t line,
               const char* function, const int32_t error_code, const char* fmt, ...) 
    {
      char msgstr[MAX_ERROR_MSG_LENGTH];
      va_list ap;
      va_start(ap, fmt);
      vsnprintf(msgstr, MAX_ERROR_MSG_LENGTH, fmt, ap);
      va_end(ap);
      TBSYS_LOGGER.logMessage(level, file, line, function, "%s", msgstr);

      BaseService* service = dynamic_cast<BaseService*>(tbutil::Service::instance());
      StatusPacket* packet = dynamic_cast<StatusPacket*>(service->get_packet_factory()->createPacket(STATUS_PACKET));
      this->reply(packet);
      return TFS_SUCCESS;
    }
 }
}
