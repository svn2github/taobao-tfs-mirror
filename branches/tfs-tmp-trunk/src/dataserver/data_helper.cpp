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
#include "data_helper.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace message;
    using namespace std;

    DataHelper::DataHelper(DataService& service):
      service_(service)
    {
    }

    DataHelper::~DataHelper()
    {
    }

    inline BlockManager& DataHelper::block_manager()
    {
      return service_.block_manager();
    }

    int DataHelper::new_remote_block(const uint64_t server_id, const uint64_t block_id,
        const bool tmp, const uint64_t family_id, const int32_t index_num)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id) ||
          (index_num < 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        NewBlockMessageV2 req_msg;
        req_msg.set_block_id(block_id);
        req_msg.set_tmp_flag(tmp);
        req_msg.set_family_id(family_id);
        req_msg.set_index_num(index_num);

        int32_t status = TFS_ERROR;
        ret = send_msg_to_server(server_id, &req_msg, status);
        ret = (ret < 0) ? ret: status;
        if (TFS_SUCCESS != ret)
        {
          ret = EXIT_ADD_NEW_BLOCK_ERROR;
          TBSYS_LOG(WARN, "new remote block fail. "
              "blockid: "PRI64_PREFIX"u, familyid: %"PRI64_PREFIX"u, index_num: %d",
              block_id, family_id, index_num);
        }
      }

      return ret;
    }

    int DataHelper::read_raw_data_ex(const uint64_t server_id, const uint64_t block_id,
        char* data, int32_t& length, const int32_t offset)
    {
      int ret = ((INVALID_SERVER_ID == server_id) ||(INVALID_BLOCK_ID == block_id) ||
          (NULL == data) || (length <= 0) || (offset < 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      // if server_id is self, just read local
      if ((TFS_SUCCESS == ret) && (server_id == service_.get_ds_ipport()))
      {
        ret = block_manager().pread(data, length, offset, block_id);
        ret = (ret < 0) ? ret: TFS_SUCCESS;
      }
      else if (TFS_SUCCESS == ret)
      {
        NewClient* new_client = NewClientManager::get_instance().create_client();
        if (NULL == new_client)
        {
          ret = TFS_ERROR;
        }
        else
        {
          ReadDataMessageV2 req_msg;
          tbnet::Packet* ret_msg;

          req_msg.set_block_id(block_id);
          req_msg.set_length(length);
          req_msg.set_offset(offset);

          ret = send_msg_to_server(server_id, new_client, &req_msg, ret_msg);
          if (TFS_SUCCESS == ret)
          {
            if (READ_RAWDATA_RESP_MESSAGE_V2 == ret_msg->getPCode())
            {
              ReadRawdataRespMessageV2* resp_msg = dynamic_cast<ReadRawdataRespMessageV2* >(ret_msg);
              length = resp_msg->get_length();
              memcpy(data, resp_msg->get_data(), length);
            }
            else if (STATUS_MESSAGE == ret_msg->getPCode())
            {
              StatusMessage* resp_msg = dynamic_cast<StatusMessage* >(ret_msg);
              ret = resp_msg->get_status();
            }
            else
            {
              ret = TFS_ERROR;
            }
          }
          NewClientManager::get_instance().destroy_client(new_client);
        }
      }

      return ret;
    }

    int DataHelper::write_raw_data_ex(const uint64_t server_id, const uint64_t block_id,
        const char* data, const int32_t length, const int32_t offset)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id) ||
          (NULL == data) || (length <= 0) || (offset < 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if ((TFS_SUCCESS == ret) && (server_id == service_.get_ds_ipport()))
      {
        ret = block_manager().pwrite(data, length, offset, block_id);
        ret = (ret < 0) ? ret : TFS_SUCCESS;
      }
      else if (TFS_SUCCESS == ret)
      {
        WriteRawdataMessageV2 req_msg;
        req_msg.set_block_id(block_id);
        req_msg.set_length(length);
        req_msg.set_offset(offset);
        req_msg.set_data(data);

        int32_t status = TFS_ERROR;
        ret = send_msg_to_server(server_id, &req_msg, status);
        ret = (ret < 0) ? ret : status;
      }

      return ret;
    }

    int DataHelper::read_index_ex(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, common::IndexDataV2& index_data)
    {
       int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id) ||
          (INVALID_BLOCK_ID == attach_block_id)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

       if ((TFS_SUCCESS == ret) && (server_id == service_.get_ds_ipport()))
       {
         ret = block_manager().traverse(index_data.header_, index_data.finfos_,
             block_id, attach_block_id);
       }
       else if (TFS_SUCCESS == ret)
       {
         ReadIndexMessageV2 req_msg;
         tbnet::Packet* ret_msg;

         req_msg.set_block_id(block_id);
         req_msg.set_attach_block_id(attach_block_id);

         NewClient* new_client = NewClientManager::get_instance().create_client();
         if (NULL == new_client)
         {
           ret = TFS_ERROR;
         }
         else
         {
           ret = send_msg_to_server(server_id, new_client, &req_msg, ret_msg);
           if (TFS_SUCCESS == ret)
           {
             if (READ_INDEX_MESSAGE_V2 == ret_msg->getPCode())
             {
                ReadIndexRespMessageV2* resp_msg = dynamic_cast<ReadIndexRespMessageV2* >(ret_msg);
                index_data = resp_msg->get_index_data();
             }
             else if (STATUS_MESSAGE == ret_msg->getPCode())
             {
               StatusMessage* resp_msg = dynamic_cast<StatusMessage*>(ret_msg);
               ret = resp_msg->get_status();
             }
             else
             {
               ret = TFS_ERROR;
             }
           }
           NewClientManager::get_instance().destroy_client(new_client);
         }
       }

       return ret;
    }

    int DataHelper::write_index_ex(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, common::IndexDataV2& index_data, const int32_t switch_flag)
    {
       int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id) ||
          (INVALID_BLOCK_ID == attach_block_id)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

       if ((TFS_SUCCESS == ret) && (server_id == service_.get_ds_ipport()))
       {
         ret = block_manager().write_file_infos(index_data.header_, index_data.finfos_,
             block_id, attach_block_id);
       }
       else if (TFS_SUCCESS == ret)
       {
         WriteIndexMessageV2 req_msg;
         req_msg.set_block_id(block_id);
         req_msg.set_attach_block_id(attach_block_id);
         req_msg.set_index_data(index_data);
         req_msg.set_switch_flag(switch_flag);

         int32_t status = TFS_ERROR;
         ret = send_msg_to_server(server_id, &req_msg, status);
         ret = (ret < 0) ? ret : status;
       }

       return ret;
    }

    int DataHelper::read_raw_data(const uint64_t server_id, const uint64_t block_id,
        char* data, int32_t& length, const int32_t offset)
    {
      return read_raw_data_ex(server_id, block_id, data, length, offset);
    }

    int DataHelper::write_raw_data(const uint64_t server_id, const uint64_t block_id,
        const char* data, const int32_t length, const int32_t offset)
    {
      return write_raw_data_ex(server_id, block_id, data, length, offset);
    }

    int DataHelper::read_index(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, common::IndexDataV2& index_data)
    {
      return read_index_ex(server_id, block_id, attach_block_id, index_data);
    }

    int DataHelper::write_index(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, common::IndexDataV2& index_data, const int32_t switch_flag)
    {
      return write_index_ex(server_id, block_id, attach_block_id, index_data, switch_flag);
    }

    int DataHelper::query_ec_meta(const uint64_t server_id, const uint64_t block_id,
        common::ECMeta& ec_meta)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id)) ?
          EXIT_PARAMETER_ERROR : TFS_SUCCESS;

       if (TFS_SUCCESS == ret)
       {
         QueryEcMetaMessage req_msg;
         tbnet::Packet* ret_msg;
         req_msg.set_block_id(block_id);
         NewClient* new_client = NewClientManager::get_instance().create_client();
         if (NULL == new_client)
         {
           ret = TFS_ERROR;
         }
         else
         {
           ret = send_msg_to_server(server_id, new_client, &req_msg, ret_msg);
           if (TFS_SUCCESS == ret)
           {
             if (QUERY_EC_META_RESP_MESSAGE == ret_msg->getPCode())
             {
               QueryEcMetaRespMessage* resp_msg = dynamic_cast<QueryEcMetaRespMessage* >(ret_msg);
               ec_meta = resp_msg->get_ec_meta();
             }
             else if (STATUS_MESSAGE == ret_msg->getPCode())
             {
               StatusMessage* resp_msg = dynamic_cast<StatusMessage*>(ret_msg);
               ret = resp_msg->get_status();
             }
             else
             {
               ret = TFS_ERROR;
             }
           }
           NewClientManager::get_instance().destroy_client(new_client);
         }
       }

       return ret;

    }

    int DataHelper::commit_ec_meta(const uint64_t server_id, const uint64_t block_id,
        const common::ECMeta& ec_meta)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id)) ?
          EXIT_PARAMETER_ERROR : TFS_SUCCESS;

       if (TFS_SUCCESS == ret)
       {
         CommitEcMetaMessage req_msg;
         req_msg.set_block_id(block_id);
         req_msg.set_ec_meta(ec_meta);

         int32_t status = TFS_ERROR;
         ret = send_msg_to_server(server_id, &req_msg, status);
         ret = (ret < 0) ? ret : status;
       }

       return ret;
    }

  }
}
