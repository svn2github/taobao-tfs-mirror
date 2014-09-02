/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: base_packet_streamer.cpp 5 2011-04-22 16:22:56Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "base_packet.h"
#include "base_packet_streamer.h"
#include "easy_io.h"

namespace tfs
{
  namespace common
  {
    easy_atomic32_t BasePacketStreamer::global_chid = 100;

    BasePacketStreamer::BasePacketStreamer()
    {

    }

    BasePacketStreamer::BasePacketStreamer(tbnet::IPacketFactory* factory) :
      IPacketStreamer(factory)
    {
    }

    BasePacketStreamer::~BasePacketStreamer()
    {

    }

    void BasePacketStreamer::set_packet_factory(tbnet::IPacketFactory* factory)
    {
      _factory = factory;
    }

    bool BasePacketStreamer::getPacketInfo(tbnet::DataBuffer* input, tbnet::PacketHeader* header, bool* broken)
    {
      bool bret = NULL != input && NULL != header;
      if (bret)
      {
        bret = input->getDataLen() >= TFS_PACKET_HEADER_V0_SIZE;
        if (bret)
        {
          //Func::hex_dump(input->getData(), input->getDataLen());
          //first peek header size, check the flag , if flag == v1, need more data;
          TfsPacketNewHeaderV0 pheader;
          int64_t pos = 0;
          int32_t iret = pheader.deserialize(input->getData(), input->getDataLen(), pos);
          bret = TFS_SUCCESS == iret;
          if (!bret)
          {
            *broken = true;
            input->clear();
            TBSYS_LOG(ERROR, "header deserialize error, iret: %d", iret);
          }
          else
          {
            if ((TFS_PACKET_FLAG_V1 == pheader.flag_)
                && ((input->getDataLen() - pheader.length()) < TFS_PACKET_HEADER_DIFF_SIZE))
            {
              bret = false;
            }
            else
            {
              input->drainData(pheader.length());
              header->_dataLen = pheader.length_;
              header->_pcode   = pheader.type_;
              header->_chid = 1;
              uint64_t channel_id = 0;

              if (((TFS_PACKET_FLAG_V0 != pheader.flag_)
                    &&(TFS_PACKET_FLAG_V1 != pheader.flag_))
                  || ( 0 >= header->_dataLen)
                  || (0x4000000 < header->_dataLen))//max 64M
              {
                *broken = true;
                input->clear();
                TBSYS_LOG(ERROR, "stream error: %x<>%x,%x, dataLen: %d", pheader.flag_,
                    TFS_PACKET_FLAG_V0, TFS_PACKET_FLAG_V1, header->_dataLen);
              }
              else
              {
                if (TFS_PACKET_FLAG_V1 == pheader.flag_)
                {
                  header->_pcode |= (pheader.check_ << 16);
                  header->_dataLen += TFS_PACKET_HEADER_DIFF_SIZE;
                  pos = 0;
                  iret = Serialization::get_int64(input->getData(), input->getDataLen(), pos, reinterpret_cast<int64_t*>(&channel_id));
                  bret = TFS_SUCCESS == iret;
                  if (bret)
                  {
                    if (TFS_PACKET_VERSION_V2 == pheader.check_)
                    {
                      header->_chid = static_cast<uint32_t>((channel_id & 0xFFFFFFFF));
                    }
                    else if (TFS_PACKET_VERSION_V1 == pheader.check_)
                    {
                      //tbnet version client got origin version 1 packet
                      header->_chid = static_cast<uint32_t>((channel_id & 0xFFFFFFFF)) - 1;
                    }
                    if (header->_chid == 0)
                    {
                      TBSYS_LOG(ERROR, "get a packet chid==0, pcode: %d,channel_id:%"PRI64_PREFIX"d", header->_pcode, channel_id);
                    }
                  }
                  else
                  {
                    bret = false;
                    *broken = true;
                    input->clear();
                    TBSYS_LOG(ERROR, "channel id deserialize error, iret: %d", iret);
                  }
                }
              }
            }
          }
        }
      }
      return bret;
    }

    tbnet::Packet* BasePacketStreamer::decode(tbnet::DataBuffer* input, tbnet::PacketHeader* header)
    {
      assert(_factory != NULL);
      tbnet::Packet* packet = NULL;
      if (NULL != input && NULL != header)
      {
        packet = _factory->createPacket(header->_pcode);
        if (NULL != packet)
        {
          if (!packet->decode(input, header))
          {
            packet->free();
            packet = NULL;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "create packet: %d fail, drain data length: %d",
            header->_pcode, header->_dataLen);
          input->drainData(header->_dataLen);
        }
      }
      return packet;
    }

    bool BasePacketStreamer::encode(tbnet::Packet* packet, tbnet::DataBuffer* output)
    {
      bool bret = NULL != packet && NULL != output;
      if (bret)
      {
        tbnet::PacketHeader* header = packet->getPacketHeader();
        int32_t old_len = output->getDataLen();
        BasePacket* bpacket = dynamic_cast<BasePacket*>(packet);
        int32_t iret = TFS_SUCCESS;
        int32_t header_length = 0;
        int64_t pos  = 0;
        //v1
        if (TFS_PACKET_VERSION_V1 == bpacket->get_version())
        {
          TfsPacketNewHeaderV1 pheader;
          pheader.crc_ = bpacket->get_crc();
          pheader.flag_ = TFS_PACKET_FLAG_V1;
          pheader.id_  = bpacket->get_id();
          pheader.length_ = bpacket->get_data_length();
          pheader.type_ = header->_pcode;
          pheader.version_ = bpacket->get_version();
          header_length = pheader.length();
          output->ensureFree(header_length + pheader.length_);
          iret = pheader.serialize(output->getFree(), output->getFreeLen(), pos);
        }//v2 tbnet
        else if (TFS_PACKET_VERSION_V2 == bpacket->get_version())
        {
          TfsPacketNewHeaderV1 pheader;
          pheader.crc_ = bpacket->get_crc();
          pheader.flag_ = TFS_PACKET_FLAG_V1;
          pheader.id_  = bpacket->getChannelId();
          pheader.length_ = bpacket->get_data_length();
          pheader.type_ = header->_pcode;
          pheader.version_ = bpacket->get_version();
          header_length = pheader.length();
          output->ensureFree(header_length + pheader.length_);
          iret = pheader.serialize(output->getFree(), output->getFreeLen(), pos);
        }
        else//v0
        {
          TfsPacketNewHeaderV1 pheader;
          memset(&pheader, 0, sizeof(pheader));
          pheader.flag_ = TFS_PACKET_FLAG_V0;
          char* header_data = reinterpret_cast<char*>(&pheader);
          int32_t length = TFS_PACKET_HEADER_V0_SIZE - 2;
          for (int32_t i = 0; i < length; i++)
          {
            pheader.version_ = static_cast<uint16_t>(*(header_data + i));
          }
          header_length = pheader.length();
          output->ensureFree(header_length + pheader.length_);
          iret = pheader.serialize(output->getFree(), output->getFreeLen(), pos);
        }
        bret = TFS_SUCCESS == iret;
        if (bret)
        {
          //TBSYS_LOG(DEBUG, "pcode: %d, header length: %d, body length : %"PRI64_PREFIX"d", bpacket->getPCode(), header_length, bpacket->get_data_length());
          //Func::hex_dump(output->getData(), output->getDataLen());
          output->pourData(header_length);
          bret = bpacket->encode(output);
          //Func::hex_dump(output->getData(), output->getDataLen());
        }
        else
        {
          TBSYS_LOG(ERROR, "encode erorr, pcode: %d", header->_pcode);
          output->stripData(output->getDataLen() - old_len);
        }
      }
      return bret;
    }

    uint64_t BasePacketStreamer::get_packet_id_handler(easy_connection_t *c, void *packet) {
      UNUSED(c);
      int32_t packet_id = 0;
      if (packet != NULL) {
        BasePacket *bp = (BasePacket*)packet;
        packet_id = bp->getChannelId();
      }
      if (packet_id == 0) {
        while (packet_id == 0 || packet_id == -1) {
          packet_id = (int32_t)easy_atomic32_add_return(&global_chid, 1);
        }
      }
      return packet_id;
    }

    void* BasePacketStreamer::decode_handler(easy_message_t *m)
    {
      // check if header complete
      if (m->input->last - m->input->pos < TFS_PACKET_HEADER_V1_SIZE)
      {
        TBSYS_LOG(DEBUG, "packet header not complete");
        return NULL;
      }

      assert(m->pool->tlock <= 1 && m->pool->flags <= 1); // why?

      // decode packet header, get length and pcode
      Stream input(m->input);
      uint32_t flag = 0;
      int32_t len = 0;
      int16_t pcode = 0;
      int16_t version = 0;
      uint64_t id = 0;
      uint32_t crc = 0;
      input.get_int32(reinterpret_cast<int32_t*>(&flag));
      input.get_int32(&len);
      input.get_int16(&pcode);
      input.get_int16(&version);
      input.get_int64(reinterpret_cast<int64_t*>(&id));
      input.get_int32(reinterpret_cast<int32_t*>(&crc));

      if (flag != TFS_PACKET_FLAG_V1 || len < 0 || len > (1<<26) /* 64M */)
      {
        TBSYS_LOG(ERROR, "decoding failed: flag=%x, len=%d, pcode=%d",
            flag, len, pcode);
        m->status = EASY_ERROR;
        return NULL;
      }

      // only received part of packet data
      if (m->input->last - m->input->pos < len) {
        //TBSYS_LOG(DEBUG, "data in buffer not enough: data_len=%d, buf_len=%d, pcode=%d",
        //    len, static_cast<int32_t>(m->input->last - m->input->pos), pcode);
        m->next_read_len = len - (m->input->last - m->input->pos);
        m->input->pos -= TFS_PACKET_HEADER_V1_SIZE;
        return NULL;
      }

      TBSYS_LOG(DEBUG, "decode packet, pcode=%d, length=%d", pcode, len);

      BasePacket* bp = dynamic_cast<BasePacket*>(_factory->createPacket(pcode));
      assert(NULL != bp);

      tbnet::PacketHeader header;
      header._chid = id;
      header._pcode = pcode;
      header._dataLen = len;
      bp->setPacketHeader(&header);

      // copy raw data to BasePacket's stream
      // because some Message hold the pointer
      bp->stream_.reserve(len);
      bp->stream_.set_bytes(m->input->pos, len);
      m->input->pos += len;
      assert(m->input->pos <= m->input->last);

      if(TFS_SUCCESS != bp->deserialize(bp->stream_))
      {
        TBSYS_LOG(ERROR, "decoding packet failed, pcode=%d", pcode);
        tbsys::gDelete(bp);
        input.clear_last_read_mark();
        m->status = EASY_ERROR;
        return NULL;
      }

      // help to detect serialize/deserialize not match problem
      if (bp->stream_.get_data_length() != 0)
      {
        TBSYS_LOG(DEBUG, "some data are useless, pcode=%d, unused_length=%ld",
            pcode, bp->stream_.get_data_length());
      }

      assert(m->pool->tlock <= 1 && m->pool->flags <= 1);
      return bp;
    }

    int BasePacketStreamer::encode_handler(easy_request_t *r, void *packet)
    {
      BasePacket* bp = (BasePacket*)packet;
      int32_t pcode = bp->getPCode();
      int64_t length = bp->length();
      TBSYS_LOG(DEBUG, "encode packet, pcode=%d, length=%ld, chid=%d", pcode, length, bp->getChannelId());
      if (EASY_TYPE_CLIENT == r->ms->c->type)
      {
        uint32_t chid = ((easy_session_t*)r->ms)->packet_id;
        bp->setChannelId(chid);
      }
      else
      {
        if (((easy_message_t*)r->ms)->status == EASY_MESG_DESTROY)
        {
          return EASY_ERROR;
        }

        if (r->retcode == EASY_ABORT)
        {
          r->ms->c->pool->ref--;
          easy_atomic_dec(&r->ms->pool->ref);
          r->retcode = EASY_OK;
        }
      }

      easy_list_t list;
      easy_list_init(&list);
      Stream output(r->ms->pool, &list);

      output.reserve(TFS_PACKET_HEADER_V1_SIZE + bp->length());
      output.set_int32(TFS_PACKET_FLAG_V1);
      char* len_pos = output.get_free();  // length() may not equal real data length
      output.set_int32(bp->length());
      output.set_int16(bp->getPCode());
      output.set_int16(TFS_PACKET_VERSION_V2);
      output.set_int64(bp->getChannelId());
      output.set_int32(0); // crc, place holder

      if (TFS_SUCCESS != bp->serialize(output))
      {
        return EASY_ERROR;
      }

      // use real length to replace header length
      if (bp->length() != output.get_data_length() - TFS_PACKET_HEADER_V1_SIZE)
      {
        // help to detect serialize problem
        TBSYS_LOG(DEBUG, "length()=%ld not equals to serialize()=%ld",
            bp->length(), output.get_data_length() - TFS_PACKET_HEADER_V1_SIZE);
        int64_t pos = 0;
        Serialization::set_int32(len_pos, INT_SIZE, pos, output.get_data_length() - TFS_PACKET_HEADER_V1_SIZE);
      }

      easy_request_addbuf_list(r, &list);

      return EASY_OK;
    }

    // libeasy copy packet
    BasePacket* BasePacketStreamer::clone_packet(BasePacket* src)
    {
      int32_t pcode = src->getPCode();
      int64_t length = src->length();
      BasePacket* dest = dynamic_cast<BasePacket*>(_factory->createPacket(pcode));
      assert(NULL != dest);
      // Stream stream(length);
      dest->stream_.expand(length);
      int ret = src->serialize(dest->stream_);
      if (TFS_SUCCESS == ret)
      {
        ret = dest->deserialize(dest->stream_);
      }

      TBSYS_LOG_DW(ret, "clone packet, ret=%d, pcode=%d, length=%ld, src=%p, dest=%p",
          ret, pcode, length, src, dest);

      if (TFS_SUCCESS != ret)
      {
        tbsys::gDelete(dest);
        dest = NULL;
      }
      else
      {
        dest->setChannelId(src->getChannelId());
        dest->set_request(src->get_request());
        dest->set_direction(src->get_direction());
      }

      return dest;
    }

  }
}
