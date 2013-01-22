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
        ret = new_remote_block_ex(server_id, block_id, tmp, family_id, index_num);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "new remote block fail. server: %s, blockid: %"PRI64_PREFIX"u, "
              "tmp: %d, familyid: %"PRI64_PREFIX"u, index_num: %d, ret: %d",
              tbsys::CNetUtil::addrToString(server_id).c_str(), block_id,
              tmp, family_id, index_num, ret);
        }
      }

      return ret;
    }

    int DataHelper::delete_remote_block(const uint64_t server_id, const uint64_t block_id,
        const bool tmp)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id)) ?
          EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ret = delete_remote_block_ex(server_id, block_id, tmp);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "delete remote block fail. server: %s, blockid: %"PRI64_PREFIX"u, tmp: %d, ret: %d",
              tbsys::CNetUtil::addrToString(server_id).c_str(), block_id, tmp, ret);
        }
      }

      return ret;
    }

    int DataHelper::read_raw_data(const uint64_t server_id, const uint64_t block_id,
        char* data, int32_t& length, const int32_t offset)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id) ||
          (NULL == data) || (length <= 0) || (offset < 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      // TODO: add flow control
      if (TFS_SUCCESS == ret)
      {
        ret = read_raw_data_ex(server_id, block_id, data, length, offset);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "read raw data fail. server: %s, blockid: %"PRI64_PREFIX"u, "
              "length: %d, offset: %d, ret: %d",
              tbsys::CNetUtil::addrToString(server_id).c_str(),
              block_id, length, offset, ret);
        }
      }
      return ret;
    }

    int DataHelper::write_raw_data(const uint64_t server_id, const uint64_t block_id,
        const char* data, const int32_t length, const int32_t offset)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id) ||
          (NULL == data) || (length <= 0) || (offset < 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ret =  write_raw_data_ex(server_id, block_id, data, length, offset);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write raw data fail. server: %s, blockid: %"PRI64_PREFIX"u, "
              "length: %d, offset: %d, ret: %d",
              tbsys::CNetUtil::addrToString(server_id).c_str(),
              block_id, length, offset, ret);
        }
      }
      return ret;
    }

    int DataHelper::read_index(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, common::IndexDataV2& index_data)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id) ||
          (INVALID_BLOCK_ID == attach_block_id)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ret = read_index_ex(server_id, block_id, attach_block_id, index_data);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "read index fail. server: %s, blockid: %"PRI64_PREFIX"u, "
              "attach blockid: %"PRI64_PREFIX"u, ret: %d",
              tbsys::CNetUtil::addrToString(server_id).c_str(),
              block_id, attach_block_id, ret);
        }
      }
      return ret;
    }

    int DataHelper::write_index(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, common::IndexDataV2& index_data)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id) ||
          (INVALID_BLOCK_ID == attach_block_id)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ret = write_index_ex(server_id, block_id, attach_block_id, index_data);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write index fail. server: %s, blockid: %"PRI64_PREFIX"u, "
              "attach blockid: %"PRI64_PREFIX"u, ret: %d",
              tbsys::CNetUtil::addrToString(server_id).c_str(),
              block_id, attach_block_id, ret);
        }
      }
      return ret;
    }

    int DataHelper::query_ec_meta(const uint64_t server_id, const uint64_t block_id,
        common::ECMeta& ec_meta)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id)) ?
          EXIT_PARAMETER_ERROR : TFS_SUCCESS;

       if (TFS_SUCCESS == ret)
       {
         ret = query_ec_meta_ex(server_id, block_id, ec_meta);
         if (TFS_SUCCESS == ret)
         {
           TBSYS_LOG(WARN, "query ec meta fail. server: %s, blockid: %"PRI64_PREFIX"u, ret: %d",
               tbsys::CNetUtil::addrToString(server_id).c_str(), block_id, ret);
         }
       }
       return ret;
    }

    int DataHelper::commit_ec_meta(const uint64_t server_id, const uint64_t block_id,
        const common::ECMeta& ec_meta, const int8_t switch_flag)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id)) ?
          EXIT_PARAMETER_ERROR : TFS_SUCCESS;

       if (TFS_SUCCESS == ret)
       {
         ret = commit_ec_meta(server_id, block_id, ec_meta, switch_flag);
         if (TFS_SUCCESS == ret)
         {
           TBSYS_LOG(WARN, "commit ec meta fail. server: %s, blockid: %"PRI64_PREFIX"u, ret: %d",
               tbsys::CNetUtil::addrToString(server_id).c_str(), block_id, ret);
         }
       }
       return ret;
    }

    int DataHelper::read_file(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, const uint64_t file_id,
        char* data, int32_t& len)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id) ||
          (INVALID_BLOCK_ID == attach_block_id) || (INVALID_FILE_ID == file_id) ||
          (NULL == data) || (len <= 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        int32_t offset = 0;
        int32_t length = 0;
        while ((TFS_SUCCESS == ret) && (offset < len))
        {
          length = std::min(len - offset, MAX_READ_SIZE);
          ret = read_file_ex(server_id, block_id, attach_block_id, file_id,
              data + offset, length, offset);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(INFO, "reinstate read file fail. server: %s ,blockid: %"PRI64_PREFIX"u, "
                "attach blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
                "length: %d, offset: %d, ret: %d",
                tbsys::CNetUtil::addrToString(server_id).c_str(),
                block_id, attach_block_id, file_id, length, offset, ret);
          }
          else
          {
            offset += length;
          }
        }
      }
      return ret;
    }

    int DataHelper::write_file(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, const uint64_t file_id,
        const char* data, const int32_t len)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id) ||
          (INVALID_BLOCK_ID == attach_block_id) || (INVALID_FILE_ID == file_id) ||
          (NULL == data) || (len <= 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      uint64_t lease_id = 0;
      if (TFS_SUCCESS == ret)
      {
        int32_t offset = 0;
        int32_t length = 0;
        while ((TFS_SUCCESS == ret) && (offset < len))
        {
          length = std::min(len - offset, MAX_READ_SIZE);
          ret = write_file_ex(server_id, block_id, attach_block_id, file_id,
              data + offset, length, offset, lease_id);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(INFO, "reinstate write file fail. server: %s, blockid: %"PRI64_PREFIX"u, "
                "attach blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
                "length: %d, offset: %d, ret: %d",
                tbsys::CNetUtil::addrToString(server_id).c_str(),
                block_id, attach_block_id, file_id, length, offset, ret);
          }
          else
          {
            offset += length;
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = close_file_ex(server_id, block_id, attach_block_id, file_id, lease_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "reinstate close file fail. server: %s, blockid: %"PRI64_PREFIX"u, "
              "attach blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, ret: %d",
              tbsys::CNetUtil::addrToString(server_id).c_str(),
              block_id, attach_block_id, file_id, ret);
        }
      }

      return ret;
    }

    int DataHelper::new_remote_block_ex(const uint64_t server_id, const uint64_t block_id,
        const bool tmp, const uint64_t family_id, const int32_t index_num)
    {
      int ret = TFS_SUCCESS;
      if (server_id == service_.get_ds_ipport())
      {
        ret = block_manager().new_block(block_id, tmp, family_id, index_num);
      }
      else
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
              "blockid: %"PRI64_PREFIX"u, familyid: %"PRI64_PREFIX"u, index_num: %d",
              block_id, family_id, index_num);
        }
      }
      return ret;
    }

    int DataHelper::delete_remote_block_ex(const uint64_t server_id, const uint64_t block_id,
        const bool tmp)
    {
      int ret = TFS_SUCCESS;
      if (server_id == service_.get_ds_ipport())
      {
        ret = block_manager().del_block(block_id, tmp);
      }
      else
      {
        RemoveBlockMessageV2 req_msg;
        req_msg.set_block_id(block_id);
        req_msg.set_tmp_flag(tmp);

        int32_t status = TFS_ERROR;
        ret = send_msg_to_server(server_id, &req_msg, status);
        ret = (ret < 0) ? ret: status;
      }
      return ret;
    }

    int DataHelper::read_raw_data_ex(const uint64_t server_id, const uint64_t block_id,
        char* data, int32_t& length, const int32_t offset)
    {
      int ret = TFS_SUCCESS;
      // if server_id is self, just read local
      if (server_id == service_.get_ds_ipport())
      {
        ret = block_manager().pread(data, length, offset, block_id);
        ret = (ret < 0) ? ret: TFS_SUCCESS;
      }
      else
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
      int ret = TFS_SUCCESS;
      if (server_id == service_.get_ds_ipport())
      {
        ret = block_manager().pwrite(data, length, offset, block_id);
        ret = (ret < 0) ? ret : TFS_SUCCESS;
      }
      else
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
      int ret = TFS_SUCCESS;
      if (server_id == service_.get_ds_ipport())
      {
        ret = block_manager().traverse(index_data.header_, index_data.finfos_,
            block_id, attach_block_id);
      }
      else
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
        const uint64_t attach_block_id, common::IndexDataV2& index_data)
    {
      int ret = TFS_SUCCESS;
      if (server_id == service_.get_ds_ipport())
      {
        ret = block_manager().write_file_infos(index_data.header_, index_data.finfos_,
            block_id, attach_block_id);
      }
      else
      {
        WriteIndexMessageV2 req_msg;
        req_msg.set_block_id(block_id);
        req_msg.set_attach_block_id(attach_block_id);
        req_msg.set_index_data(index_data);

        int32_t status = TFS_ERROR;
        ret = send_msg_to_server(server_id, &req_msg, status);
        ret = (ret < 0) ? ret : status;
      }

      return ret;
    }

    int DataHelper::query_ec_meta_ex(const uint64_t server_id, const uint64_t block_id,
        common::ECMeta& ec_meta)
    {
      int ret = TFS_SUCCESS;
      if (server_id == service_.get_ds_ipport())
      {
        if (TFS_SUCCESS == ret)
        {
          ret = block_manager().get_family_id(ec_meta.family_id_, block_id);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = block_manager().get_used_offset(ec_meta.used_offset_, block_id);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = block_manager().get_marshalling_offset(ec_meta.mars_offset_, block_id);
        }
      }
      else
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

    int DataHelper::commit_ec_meta_ex(const uint64_t server_id, const uint64_t block_id,
        const common::ECMeta& ec_meta, const int8_t switch_flag)
    {
      int ret = TFS_SUCCESS;
      if (server_id == service_.get_ds_ipport())
      {
        // commit family id
        if ((TFS_SUCCESS == ret) && (ec_meta.family_id_ >= 0))
        {
          ret = block_manager().set_family_id(ec_meta.family_id_, block_id);
        }

        // commit marshalling length
        if ((TFS_SUCCESS == ret) && (ec_meta.mars_offset_ > 0))
        {
          ret = block_manager().set_marshalling_offset(ec_meta.mars_offset_, block_id);
        }
      }
      else
      {
        CommitEcMetaMessage req_msg;
        req_msg.set_block_id(block_id);
        req_msg.set_ec_meta(ec_meta);
        req_msg.set_switch_flag(switch_flag);

        int32_t status = TFS_ERROR;
        ret = send_msg_to_server(server_id, &req_msg, status);
        ret = (ret < 0) ? ret : status;
      }
      return ret;
    }

    int DataHelper::read_file_ex(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, const uint64_t file_id,
        char* data, int32_t& length, const int32_t offset)
    {
      int ret = TFS_SUCCESS;
      // if server_id is self, just read local
      if (server_id == service_.get_ds_ipport())
      {
        ret = block_manager().read(data, length, offset, file_id,
            READ_DATA_OPTION_FLAG_FORCE, block_id, attach_block_id);
        ret = (ret < 0) ? ret: TFS_SUCCESS;
      }
      else
      {
        NewClient* new_client = NewClientManager::get_instance().create_client();
        if (NULL == new_client)
        {
          ret = TFS_ERROR;
        }
        else
        {
          ReadFileMessageV2 req_msg;
          tbnet::Packet* ret_msg;
          req_msg.set_block_id(block_id);
          req_msg.set_attach_block_id(attach_block_id);
          req_msg.set_file_id(file_id);
          req_msg.set_length(length);
          req_msg.set_offset(offset);
          req_msg.set_flag(READ_DATA_OPTION_FLAG_FORCE);

          ret = send_msg_to_server(server_id, new_client, &req_msg, ret_msg);
          if (TFS_SUCCESS == ret)
          {
            if (READ_FILE_RESP_MESSAGE_V2 == ret_msg->getPCode())
            {
              ReadFileRespMessageV2* resp_msg = dynamic_cast<ReadFileRespMessageV2* >(ret_msg);
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

    int DataHelper::write_file_ex(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, const uint64_t file_id,
        const char* data, const int32_t length, const int32_t offset, uint64_t& lease_id)
    {
      int ret = TFS_SUCCESS;
      NewClient* new_client = NewClientManager::get_instance().create_client();
      if (NULL == new_client)
      {
        ret = TFS_ERROR;
      }
      else
      {
        WriteFileMessageV2 req_msg;
        tbnet::Packet* ret_msg;

        vector<uint64_t> dslist;
        dslist.push_back(server_id);
        req_msg.set_block_id(block_id);
        req_msg.set_attach_block_id(attach_block_id);
        req_msg.set_file_id(file_id);
        req_msg.set_lease_id(lease_id);
        req_msg.set_data(data);
        req_msg.set_length(length);
        req_msg.set_offset(offset);
        req_msg.set_master_id(server_id);
        req_msg.set_ds(dslist);
        req_msg.set_version(-1); // won't check version

        ret = send_msg_to_server(server_id, new_client, &req_msg, ret_msg);
        if (TFS_SUCCESS == ret)
        {
          if (WRITE_FILE_RESP_MESSAGE_V2 == ret_msg->getPCode())
          {
            WriteFileRespMessageV2* resp_msg = dynamic_cast<WriteFileRespMessageV2* >(ret_msg);
            lease_id = resp_msg->get_lease_id();
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

      return ret;
    }

    int DataHelper::close_file_ex(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, const uint64_t file_id, const uint64_t lease_id)
    {
      vector<uint64_t> dslist;
      dslist.push_back(server_id);
      CloseFileMessageV2 req_msg;
      req_msg.set_block_id(block_id);
      req_msg.set_attach_block_id(attach_block_id);
      req_msg.set_file_id(file_id);
      req_msg.set_lease_id(lease_id);
      req_msg.set_tmp_flag(true);  // we are writing a tmp block

      int32_t status = TFS_ERROR;
      int ret = send_msg_to_server(server_id, &req_msg, status);
      return (ret < 0) ? ret : status;
    }
  }
}
