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
#include "erasure_code.h"
#include "data_helper.h"
#include "traffic_control.h"

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

    inline BlockManager& DataHelper::get_block_manager()
    {
      return service_.get_block_manager();
    }

    inline TrafficControl& DataHelper::get_traffic_control()
    {
      return service_.get_traffic_control();
    }

    int DataHelper::send_simple_request(uint64_t server_id, common::BasePacket* message, const int32_t timeout)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (NULL == message)) ?
          EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        int32_t status = EXIT_GENERAL_ERROR;
        ret = send_msg_to_server(server_id, message, status, timeout);
        if (TFS_SUCCESS == ret)
        {
          ret = status;
        }
      }

      return ret;
    }

    int DataHelper::new_remote_block(const uint64_t server_id, const uint64_t block_id,
        const bool tmp, const uint64_t family_id, const int32_t index_num, const int32_t expire_time)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id) ||
          (index_num < 0) || (expire_time < 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ret = new_remote_block_ex(server_id, block_id, tmp, family_id, index_num, expire_time);
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
        char* data, int32_t length, const int32_t offset)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id) ||
          (NULL == data) || (length <= 0) || (offset < 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      if ((TFS_SUCCESS == ret) && (server_id == ds_info.information_.id_))
      {
        ret = read_raw_data_ex(server_id, block_id, data, length, offset);
      }
      else if ((TFS_SUCCESS == ret) && (server_id != ds_info.information_.id_))
      {
        if (get_traffic_control().mr_traffic_out_of_threshold(true))
        {
          int64_t now = Func::get_monotonic_time_us();
          int64_t last = get_traffic_control().get_last_mr_traffic_stat_time_us(true);
          int64_t wait_time = last + TRAFFIC_BYTES_STAT_INTERVAL - now;
          TBSYS_LOG(DEBUG, "read data need wait %"PRI64_PREFIX"d us", wait_time);
          if (wait_time > 0)
          {
            // after sleep, we think it's ok to send read request, though it maybe not
            usleep(wait_time);
          }
        }

        int retry = 0;
        do
        {
          // we don't care real read length in this function
          int32_t real_read_length = length;
          ret = read_raw_data_ex(server_id, block_id, data, real_read_length, offset);
          if (EXIT_NETWORK_BUSY_ERROR == ret)
          {
            TBSYS_LOG(DEBUG, "peer network busy, need retry: %d", retry + 1);
            usleep(TRAFFIC_BYTES_STAT_INTERVAL/2);
          }
          else
          {
            if (TFS_SUCCESS == ret)
            {
              get_traffic_control().mr_traffic_stat(true, length);
            }
            break;
          }
        } while (++retry < BUSY_RETRY_TIMES);
      }

      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "read raw data fail. server: %s, blockid: %"PRI64_PREFIX"u, "
            "length: %d, offset: %d, ret: %d",
            tbsys::CNetUtil::addrToString(server_id).c_str(),
            block_id, length, offset, ret);
      }

      return ret;
    }

    int DataHelper::write_raw_data(const uint64_t server_id, const uint64_t block_id,
        const char* data, const int32_t length, const int32_t offset, const bool tmp)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id) ||
          (NULL == data) || (length <= 0) || (offset < 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      if ((TFS_SUCCESS == ret) && (server_id == ds_info.information_.id_))
      {
        ret =  write_raw_data_ex(server_id, block_id, data, length, offset, tmp);
      }
      else if ((TFS_SUCCESS == ret) && (server_id != ds_info.information_.id_))
      {
        if (get_traffic_control().mr_traffic_out_of_threshold(false))
        {
          int64_t now = Func::get_monotonic_time_us();
          int64_t last = get_traffic_control().get_last_mr_traffic_stat_time_us(false);
          int64_t wait_time = last + TRAFFIC_BYTES_STAT_INTERVAL - now;
          TBSYS_LOG(DEBUG, "write data need wait %"PRI64_PREFIX"d us", wait_time);
          if (wait_time > 0)
          {
            usleep(wait_time);
          }
        }

        ret = write_raw_data_ex(server_id, block_id, data, length, offset, tmp);
        if (TFS_SUCCESS == ret)
        {
          get_traffic_control().mr_traffic_stat(false, length);
        }
      }

      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "write raw data fail. server: %s, blockid: %"PRI64_PREFIX"u, "
            "length: %d, offset: %d, tmp: %d, ret: %d",
            tbsys::CNetUtil::addrToString(server_id).c_str(),
            block_id, length, offset, tmp, ret);
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

      TBSYS_LOG(DEBUG, "read index. block_id: %"PRI64_PREFIX"u, finfos size: %zd",
        index_data.header_.info_.block_id_, index_data.finfos_.size());

      return ret;
    }

    int DataHelper::write_index(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, common::IndexDataV2& index_data,
        const bool tmp, const bool partial)
    {
      TBSYS_LOG(DEBUG, "write index. block_id: %"PRI64_PREFIX"u, finfos size: %zd",
        index_data.header_.info_.block_id_, index_data.finfos_.size());

      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id) ||
          (INVALID_BLOCK_ID == attach_block_id)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ret = write_index_ex(server_id, block_id, attach_block_id, index_data, tmp, partial);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write index fail. server: %s, blockid: %"PRI64_PREFIX"u, "
              "attach blockid: %"PRI64_PREFIX"u, tmp: %d, partial: %d, ret: %d",
              tbsys::CNetUtil::addrToString(server_id).c_str(),
              block_id, attach_block_id, tmp, partial, ret);
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
         if (TFS_SUCCESS != ret)
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
         ret = commit_ec_meta_ex(server_id, block_id, ec_meta, switch_flag);
         if (TFS_SUCCESS != ret)
         {
           TBSYS_LOG(WARN, "commit ec meta fail. server: %s, blockid: %"PRI64_PREFIX"u, "
               "switch_flag: %d, ret: %d",
               tbsys::CNetUtil::addrToString(server_id).c_str(), block_id, switch_flag, ret);
         }
       }
       return ret;
    }

    int DataHelper::read_file(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, const uint64_t file_id,
        char* data, const int32_t len, const int32_t off, const int8_t flag)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id) ||
          (INVALID_BLOCK_ID == attach_block_id) || (INVALID_FILE_ID == file_id) ||
          (NULL == data) || (len <= 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        int32_t offset = off;
        int32_t length = 0;
        while ((TFS_SUCCESS == ret) && (offset < len + off))
        {
          length = std::min(len + off - offset, MAX_READ_SIZE);
          ret = read_file_ex(server_id, block_id, attach_block_id, file_id,
              data + offset - off, length, offset, flag);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(INFO, "read file fail. server: %s ,blockid: %"PRI64_PREFIX"u, "
                "attach blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
                "length: %d, offset: %d, ret: %d", tbsys::CNetUtil::addrToString(server_id).c_str(),
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

    /*
     * if status >= 0
     * status will directly set to target file
     */
    int DataHelper::write_file(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, const uint64_t file_id,
        const char* data, const int32_t len, const int32_t status, const bool tmp)
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
            TBSYS_LOG(WARN, "write file fail. server: %s, blockid: %"PRI64_PREFIX"u, "
                "attach blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
                "length: %d, offset: %d, ret: %d", tbsys::CNetUtil::addrToString(server_id).c_str(),
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
        ret = close_file_ex(server_id, block_id, attach_block_id, file_id, lease_id, status, tmp);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "close file fail. server: %s, blockid: %"PRI64_PREFIX"u, "
              "attach blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, ret: %d",
              tbsys::CNetUtil::addrToString(server_id).c_str(),
              block_id, attach_block_id, file_id, ret);
        }
      }

      return ret;
    }

    int DataHelper::unlink_file(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, const uint64_t file_id,
        const int32_t status)
    {
      int ret = ((INVALID_SERVER_ID == server_id) || (INVALID_BLOCK_ID == block_id) ||
          (INVALID_BLOCK_ID == attach_block_id) || (INVALID_FILE_ID == file_id)) ?
        EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ret = unlink_file_ex(server_id, block_id, attach_block_id, file_id, status);
      }

      return ret;
    }

    int DataHelper::get_block_replicas(const uint64_t ns_id, const uint64_t block_id, VUINT64& servers)
    {
      int ret = ((INVALID_SERVER_ID != ns_id) && (INVALID_BLOCK_ID != block_id)) ?
        TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = get_block_replicas_ex(ns_id, block_id, servers);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(DEBUG, "get block replicas fail. ns: %s, blockid: %"PRI64_PREFIX"u, ret: %d",
              tbsys::CNetUtil::addrToString(ns_id).c_str(), block_id, ret);
        }
      }
      return ret;
    }

    int DataHelper::new_remote_block_ex(const uint64_t server_id, const uint64_t block_id,
        const bool tmp, const uint64_t family_id, const int32_t index_num, const int32_t expire_time)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      if (server_id == ds_info.information_.id_)
      {
        ret = get_block_manager().new_block(block_id, tmp, family_id, index_num, expire_time);
      }
      else
      {
        NewBlockMessageV2 req_msg;
        req_msg.set_block_id(block_id);
        req_msg.set_tmp_flag(tmp);
        req_msg.set_family_id(family_id);
        req_msg.set_index_num(index_num);
        req_msg.set_expire_time(expire_time);

        ret = send_simple_request(server_id, &req_msg);
      }
      return ret;
    }

    int DataHelper::delete_remote_block_ex(const uint64_t server_id, const uint64_t block_id,
        const bool tmp)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      if (server_id == ds_info.information_.id_)
      {
        ret = get_block_manager().del_block(block_id, tmp);
      }
      else
      {
        RemoveBlockMessageV2 req_msg;
        req_msg.set_block_id(block_id);
        req_msg.set_tmp_flag(tmp);

        ret = send_simple_request(server_id, &req_msg);
      }
      return ret;
    }

    int DataHelper::read_raw_data_ex(const uint64_t server_id, const uint64_t block_id,
        char* data, int32_t& length, const int32_t offset)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      if (server_id == ds_info.information_.id_)
      {
        ret = get_block_manager().pread(data, length, offset, block_id);
        ret = (ret < 0) ? ret: TFS_SUCCESS;
      }
      else
      {
        NewClient* new_client = NewClientManager::get_instance().create_client();
        if (NULL == new_client)
        {
          ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        }
        else
        {
          ReadRawdataMessageV2 req_msg;
          tbnet::Packet* ret_msg = NULL;
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
              ret = EXIT_UNKNOWN_MSGTYPE;
            }
          }
          NewClientManager::get_instance().destroy_client(new_client);
        }
      }

      return ret;
    }

    int DataHelper::write_raw_data_ex(const uint64_t server_id, const uint64_t block_id,
        const char* data, const int32_t length, const int32_t offset, const bool tmp)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      if (server_id == ds_info.information_.id_)
      {
        ret = get_block_manager().pwrite(data, length, offset, block_id, tmp);
        ret = (ret < 0) ? ret : TFS_SUCCESS;
      }
      else
      {
        WriteRawdataMessageV2 req_msg;
        req_msg.set_block_id(block_id);
        req_msg.set_length(length);
        req_msg.set_offset(offset);
        req_msg.set_data(data);
        req_msg.set_tmp_flag(tmp);
        ret = send_simple_request(server_id, &req_msg);
      }

      return ret;
    }

    int DataHelper::read_index_ex(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, common::IndexDataV2& index_data)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      if (server_id == ds_info.information_.id_)
      {
        ret = get_block_manager().traverse(index_data.header_, index_data.finfos_,
            block_id, attach_block_id);
      }
      else
      {
        ReadIndexMessageV2 req_msg;
        tbnet::Packet* ret_msg = NULL;

        req_msg.set_block_id(block_id);
        req_msg.set_attach_block_id(attach_block_id);

        NewClient* new_client = NewClientManager::get_instance().create_client();
        if (NULL == new_client)
        {
          ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        }
        else
        {
          ret = send_msg_to_server(server_id, new_client, &req_msg, ret_msg);
          if (TFS_SUCCESS == ret)
          {
            if (READ_INDEX_RESP_MESSAGE_V2 == ret_msg->getPCode())
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
              ret = EXIT_UNKNOWN_MSGTYPE;
            }
          }
          NewClientManager::get_instance().destroy_client(new_client);
        }
      }

      return ret;
    }

    int DataHelper::write_index_ex(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, common::IndexDataV2& index_data,
        const bool tmp, const bool partial)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      if (server_id == ds_info.information_.id_)
      {
        UNUSED(partial);
        ret = get_block_manager().write_file_infos(index_data.header_, index_data.finfos_,
            block_id, attach_block_id, tmp, partial);
      }
      else
      {
        WriteIndexMessageV2 req_msg;
        req_msg.set_block_id(block_id);
        req_msg.set_attach_block_id(attach_block_id);
        req_msg.set_index_data(index_data);
        req_msg.set_tmp_flag(tmp);
        req_msg.set_partial_flag(partial);
        ret = send_simple_request(server_id, &req_msg, WRITE_INDEX_TIMEOUT_MS);
      }

      return ret;
    }

    int DataHelper::query_ec_meta_ex(const uint64_t server_id, const uint64_t block_id,
        common::ECMeta& ec_meta)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      if (server_id == ds_info.information_.id_)
      {
        if (TFS_SUCCESS == ret)
        {
          ret = get_block_manager().get_family_id(ec_meta.family_id_, block_id);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = get_block_manager().get_used_offset(ec_meta.used_offset_, block_id);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = get_block_manager().get_marshalling_offset(ec_meta.mars_offset_, block_id);
        }
      }
      else
      {
        QueryEcMetaMessage req_msg;
        tbnet::Packet* ret_msg = NULL;
        req_msg.set_block_id(block_id);
        NewClient* new_client = NewClientManager::get_instance().create_client();
        if (NULL == new_client)
        {
          ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
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
              ret = EXIT_UNKNOWN_MSGTYPE;
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
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      if (server_id == ds_info.information_.id_)
      {
        BaseLogicBlock* logic_block = get_block_manager().get(block_id, switch_flag);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;

        // commit family id
        if ((TFS_SUCCESS == ret) && (ec_meta.family_id_ >= 0))
        {
          ret = logic_block->set_family_id(ec_meta.family_id_);
        }

        // commit marshalling length
        if ((TFS_SUCCESS == ret) && (ec_meta.mars_offset_ > 0))
        {
          ret = logic_block->set_marshalling_offset(ec_meta.mars_offset_);
        }

        // update block version
        if ((TFS_SUCCESS == ret) && (ec_meta.version_step_ > 0))
        {
          ret = logic_block->update_block_version(ec_meta.version_step_);
        }

        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "commit ec meta fail. blockid: %"PRI64_PREFIX"u, switch flag: %d, ret: %d",
              block_id, switch_flag, ret);
        }
        else if (switch_flag) // if need, switch block
        {
          ret = get_block_manager().switch_logic_block(block_id, true);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "switch logic block fail. blockid: %"PRI64_PREFIX"u, "
                "ret: %d", block_id, ret);
          }
        }
      }
      else
      {
        CommitEcMetaMessage req_msg;
        req_msg.set_block_id(block_id);
        req_msg.set_ec_meta(ec_meta);
        req_msg.set_switch_flag(switch_flag);
        ret = send_simple_request(server_id, &req_msg);
      }
      return ret;
    }

    int DataHelper::stat_file_ex(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, const uint64_t file_id, const int8_t flag,
        common::FileInfoV2& finfo)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      if (server_id == ds_info.information_.id_)
      {
        finfo.id_ = file_id;
        ret = get_block_manager().stat(finfo, flag, block_id, attach_block_id);
      }
      else
      {
        NewClient* new_client = NewClientManager::get_instance().create_client();
        if (NULL == new_client)
        {
          ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        }
        else
        {
          StatFileMessageV2 req_msg;
          tbnet::Packet* ret_msg = NULL;
          req_msg.set_block_id(block_id);
          req_msg.set_attach_block_id(attach_block_id);
          req_msg.set_file_id(file_id);
          req_msg.set_flag(flag);

          ret = send_msg_to_server(server_id, new_client, &req_msg, ret_msg);
          if (TFS_SUCCESS == ret)
          {
            if (STAT_FILE_RESP_MESSAGE_V2 == ret_msg->getPCode())
            {
              StatFileRespMessageV2* resp_msg = dynamic_cast<StatFileRespMessageV2* >(ret_msg);
              finfo = resp_msg->get_file_info();
            }
            else if (STATUS_MESSAGE == ret_msg->getPCode())
            {
              StatusMessage* resp_msg = dynamic_cast<StatusMessage* >(ret_msg);
              ret = resp_msg->get_status();
            }
            else
            {
              ret = EXIT_UNKNOWN_MSGTYPE;
            }
          }
          NewClientManager::get_instance().destroy_client(new_client);
        }
      }

      return ret;

    }

    int DataHelper::read_file_ex(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, const uint64_t file_id,
        char* data, int32_t& length, const int32_t offset, const int8_t flag)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      if (server_id == ds_info.information_.id_)
      {
        ret = get_block_manager().read(data, length, offset, file_id,
            READ_DATA_OPTION_FLAG_FORCE, block_id, attach_block_id);
        ret = (ret < 0) ? ret: TFS_SUCCESS;
      }
      else
      {
        NewClient* new_client = NewClientManager::get_instance().create_client();
        if (NULL == new_client)
        {
          ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        }
        else
        {
          ReadFileMessageV2 req_msg;
          tbnet::Packet* ret_msg = NULL;
          req_msg.set_block_id(block_id);
          req_msg.set_attach_block_id(attach_block_id);
          req_msg.set_file_id(file_id);
          req_msg.set_length(length);
          req_msg.set_offset(offset);
          req_msg.set_flag(flag);

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
              ret = EXIT_UNKNOWN_MSGTYPE;
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
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      }
      else
      {
        WriteFileMessageV2 req_msg;
        tbnet::Packet* ret_msg = NULL;

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
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
        }
        NewClientManager::get_instance().destroy_client(new_client);
      }

      return ret;
    }

    int DataHelper::close_file_ex(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, const uint64_t file_id, const uint64_t lease_id,
        const int32_t status, const bool tmp)
    {
      vector<uint64_t> dslist;
      dslist.push_back(server_id);
      CloseFileMessageV2 req_msg;
      req_msg.set_ds(dslist);
      req_msg.set_block_id(block_id);
      req_msg.set_attach_block_id(attach_block_id);
      req_msg.set_file_id(file_id);
      req_msg.set_lease_id(lease_id);
      req_msg.set_master_id(server_id);
      req_msg.set_status(status);
      req_msg.set_tmp_flag(tmp);
      return send_simple_request(server_id, &req_msg);
    }

    int DataHelper::unlink_file_ex(const uint64_t server_id, const uint64_t block_id,
        const uint64_t attach_block_id, const uint64_t file_id,
        const int32_t status)
    {
      uint64_t lease_id = 0;
      vector<uint64_t> dslist;
      dslist.push_back(server_id);

      // prepare unlink
      UnlinkFileMessageV2 req_msg;
      req_msg.set_ds(dslist);
      req_msg.set_block_id(block_id);
      req_msg.set_attach_block_id(attach_block_id);
      req_msg.set_file_id(file_id);
      req_msg.set_lease_id(0);
      req_msg.set_master_id(server_id);
      req_msg.set_prepare_flag(true);

      tbnet::Packet* ret_msg = NULL;
      NewClient* new_client = NewClientManager::get_instance().create_client();
      int ret = NULL != new_client ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = send_msg_to_server(server_id, new_client, &req_msg, ret_msg);
        if (TFS_SUCCESS == ret)
        {
          if (ret_msg->getPCode() == STATUS_MESSAGE)
          {
            StatusMessage* smsg = dynamic_cast<StatusMessage*>(ret_msg);
            ret = smsg->get_status();
            if (TFS_SUCCESS == ret)
            {
              lease_id = strtoul(smsg->get_error(), NULL, 10);
            }
          }
          else
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
        }
        NewClientManager::get_instance().destroy_client(new_client);
      }

      // real unlink
      if (TFS_SUCCESS == ret)
      {
        int32_t action = 0;
        SET_OVERRIDE_FLAG(action, status);
        req_msg.set_lease_id(lease_id);
        req_msg.set_prepare_flag(false);
        req_msg.set_action(action);
        ret = send_simple_request(server_id, &req_msg);
      }

      return ret;
    }

    int DataHelper::prepare_read_degrade(const FamilyInfoExt& family_info, int* erased)
    {
      assert(NULL != erased);
      int ret = (INVALID_FAMILY_ID != family_info.family_id_) ?
        TFS_SUCCESS: EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        const int32_t data_num = GET_DATA_MEMBER_NUM(family_info.family_aid_info_);
        const int32_t check_num = GET_CHECK_MEMBER_NUM(family_info.family_aid_info_);
        if (!CHECK_MEMBER_NUM_V2(data_num, check_num))
        {
          ret = EXIT_PARAMETER_ERROR;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        const int32_t data_num = GET_DATA_MEMBER_NUM(family_info.family_aid_info_);
        const int32_t check_num = GET_CHECK_MEMBER_NUM(family_info.family_aid_info_);
        const int32_t member_num = data_num + check_num;

        int alive = 0;
        for (int32_t i = 0; i < member_num; i++)
        {
          // just need data_num nodes to recovery
          if (INVALID_BLOCK_ID != family_info.members_[i].first &&
              INVALID_SERVER_ID != family_info.members_[i].second)
          {
            erased[i] = ErasureCode::NODE_ALIVE;
            alive++;
          }
          else
          {
            erased[i] = ErasureCode::NODE_DEAD;
          }
        }

        // we need exact data_num nodes to do reinstate
        if (alive < data_num)
        {
          TBSYS_LOG(WARN, "no enough alive node to reinstate, alive: %d", alive);
          ret = EXIT_NO_ENOUGH_DATA;
        }
        else if (alive > data_num)
        {
          // random set alive-data_num nodes to UNUSED status
          for (int32_t i = 0; i < alive - data_num; i++)
          {
            int32_t unused = rand() % member_num;
            if (ErasureCode::NODE_ALIVE != erased[unused])
            {
              while (ErasureCode::NODE_ALIVE != erased[unused])
              {
                unused = (unused + 1) % member_num;
              }
            }
            // got here, erased[unused]  mustbe NODE_ALIVE
            erased[unused] = ErasureCode::NODE_UNUSED;
          }
        }
      }

      return ret;
    }

     int DataHelper::stat_file_degrade(const uint64_t block_id, const uint64_t file_id,
         const int32_t flag, const common::FamilyInfoExt& family_info, common::FileInfoV2& finfo)
     {
      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id)) ?
          EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        const int32_t data_num = GET_DATA_MEMBER_NUM(family_info.family_aid_info_);
        const int32_t check_num = GET_CHECK_MEMBER_NUM(family_info.family_aid_info_);
        const int32_t member_num = data_num + check_num;

        int32_t target = data_num;
        while ((target < member_num) && (INVALID_SERVER_ID == family_info.members_[target].second))
        {
          target++;
        }
        // no check block alive, can't stat degrade
        ret = (target >= member_num) ? EXIT_NO_ENOUGH_DATA : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          ret = stat_file_ex(family_info.members_[target].second,
              family_info.members_[target].first, block_id, file_id, flag, finfo);
        }
      }

      return ret;
    }

    int DataHelper::read_file_degrade(const uint64_t block_id, const FileInfoV2& finfo, char* data,
        const int32_t length, const int32_t offset, const int8_t flag, const FamilyInfoExt& family_info)
    {
      int ret = ((INVALID_BLOCK_ID == block_id) || (NULL == data) || (length <= 0)
          || (offset < 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        const int32_t data_num = GET_DATA_MEMBER_NUM(family_info.family_aid_info_);
        const int32_t check_num = GET_CHECK_MEMBER_NUM(family_info.family_aid_info_);
        const int32_t member_num = data_num + check_num;

        ECMeta ec_metas[MAX_MARSHALLING_NUM];
        int erased[member_num];
        int32_t target = 0;
        ret = prepare_read_degrade(family_info, erased);
        if (TFS_SUCCESS == ret)
        {
          target = data_num;
          while((target < member_num) && (ErasureCode::NODE_ALIVE != erased[target]))
          {
            target++;
          }
          ret = (target >= member_num) ? EXIT_NO_ENOUGH_DATA : TFS_SUCCESS; // no alive check block

          // it can be "optimized" to one network interact, but may lead to disk IO
          if (TFS_SUCCESS == ret)
          {
            ret = async_query_ec_meta(family_info, erased, ec_metas);
          }
        }

        if (TFS_SUCCESS == ret)
        {
          // it's an updated file, read from check block
          if (finfo.offset_ >= ec_metas[target].mars_offset_)
          {
            ret = read_file(family_info.members_[target].second,
                family_info.members_[target].first, block_id, finfo.id_,
                data, length, offset, flag);
          }
          else  // decode from other block's data
          {
            ret = read_file_degrade_ex(block_id,
                finfo, data, length, offset, family_info, erased, ec_metas);
          }
        }
      }

      return ret;
    }

    int DataHelper::read_file_degrade_ex(const uint64_t block_id, const FileInfoV2& finfo, char* buffer,
        const int32_t length, const int32_t offset, const FamilyInfoExt& family_info,
        int* erased, ECMeta* ec_metas)
    {
      int ret = TFS_SUCCESS;
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_info.family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_info.family_aid_info_);
      const int32_t member_num = data_num + check_num;

      int32_t offset_in_buffer = 0;
      int32_t file_offset = finfo.offset_ + offset;
      int32_t file_end = finfo.offset_ + offset + length;

      // padding to align to encode/decode unit
      int32_t unit = ErasureCode::ps_ * ErasureCode::ws_;
      int32_t max_read_size = MAX_READ_SIZE + unit; // avoid dividing one request into two requests
      int32_t real_offset = file_offset;
      int32_t real_end = file_end;
      if (0 != (real_offset % unit))
      {
        real_offset = (real_offset / unit) * unit;
      }
      if (0 != (real_end % unit))
      {
        real_end = (real_end / unit + 1)  * unit;
      }

      ErasureCode decoder;
      char* data[member_num];
      memset(data, 0, member_num * sizeof(char*));

      // find read target index
      int32_t target = 0;
      while ((target < data_num) && (family_info.members_[target].first != block_id))
      {
        target++;
      }
      ret = (target >= data_num) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      // config decoder parameter, alloc buffer
      if (TFS_SUCCESS == ret)
      {
        ret = decoder.config(data_num, check_num, erased);
        if (TFS_SUCCESS == ret)
        {
          for (int32_t i = 0; i < member_num; i++)
          {
            data[i] = (char*)malloc(max_read_size * sizeof(char));
            assert(NULL != data[i]);
          }
          decoder.bind(data, member_num, max_read_size);
        }
      }

      while ((TFS_SUCCESS == ret) && (real_offset < real_end))
      {
        std::pair<int32_t, int32_t> read_infos[member_num];
        int32_t len = std::min(real_end - real_offset, max_read_size);
        for (int32_t i = 0; (TFS_SUCCESS == ret) && (i < member_num); i++)
        {
          if (ErasureCode::NODE_ALIVE != erased[i])
          {
            continue; // not alive, just continue
          }
          read_infos[i].first = len;
          read_infos[i].second = real_offset;
          if ((real_offset < ec_metas[i].mars_offset_))
          {
            // truncate to avoid reading invalid data
            if (real_offset + len > ec_metas[i].mars_offset_)
            {
              memset(data[i], 0, len * sizeof(char));
              read_infos[i].first = ec_metas[i].mars_offset_ - real_offset;
            }
          }
          else
          {
            read_infos[i].first = 0;
            memset(data[i], 0, len * sizeof(char));
          }
        }

        // async read raw data
        if (TFS_SUCCESS == ret)
        {
          ret = async_read_raw_data(family_info, erased, read_infos, data);
        }

        // decode data to buffer
        if (TFS_SUCCESS == ret)
        {
          ret = decoder.decode(len);
        }

        if (TFS_SUCCESS == ret)
        {
          int32_t offset_in_read = 0;
          int32_t length_in_read = len;

          if (real_offset < file_offset)
          {
            offset_in_read = file_offset - real_offset;
            length_in_read -= offset_in_read;
          }

          if (real_offset + len > file_end)
          {
            length_in_read -= (real_end - file_end);
          }

          memcpy(buffer + offset_in_buffer, data[target] + offset_in_read, length_in_read);
          offset_in_buffer += length_in_read;
          real_offset += len;
        }
      }

      for (int i = 0; i < member_num; i++)
      {
        tbsys::gFree(data[i]);
      }

      return ret;
    }

    int DataHelper::get_block_replicas_ex(const uint64_t ns_id, const uint64_t block_id, VUINT64& servers)
    {
      int ret = TFS_SUCCESS;
      tbnet::Packet* resp_msg = NULL;
      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL != client)
      {
        GetBlockInfoMessageV2 msg;
        msg.set_block_id(block_id);
        msg.set_mode(T_READ);
        ret = send_msg_to_server(ns_id, client, &msg, resp_msg);
        if (TFS_SUCCESS == ret)
        {
          if (GET_BLOCK_INFO_RESP_MESSAGE_V2 == resp_msg->getPCode())
          {
            GetBlockInfoRespMessageV2* response = dynamic_cast<GetBlockInfoRespMessageV2*>(resp_msg);
            BlockMeta& meta = response->get_block_meta();
            for (int32_t i = 0; i < meta.size_; i++)
            {
              servers.push_back(meta.ds_[i]);
            }
          }
          else if (STATUS_MESSAGE == resp_msg->getPCode())
          {
            StatusMessage* smsg = dynamic_cast<StatusMessage*>(resp_msg);
            ret = smsg->get_status();
          }
          else
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
        }
        NewClientManager::get_instance().destroy_client(client);
      }
      else
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      }

      return ret;
    }

    void DataHelper::process_fail_response(NewClient* new_client)
    {
      assert(NULL != new_client);
      NewClient::RESPONSE_MSG_MAP* fail_response = new_client->get_fail_response();
      assert(NULL != fail_response);
      NewClient::RESPONSE_MSG_MAP::iterator iter = fail_response->begin();
      for ( ; iter != fail_response->end(); iter++)
      {
        TBSYS_LOG(WARN, "get fail response from server %s",
            tbsys::CNetUtil::addrToString(iter->second.first).c_str());
      }
    }

    int DataHelper::async_read_raw_data(const FamilyInfoExt& family_info, const int* erased,
        const std::pair<int32_t, int32_t>* read_infos, char** data, const int32_t timeout_ms)
    {
      assert(NULL != erased);
      assert(NULL != read_infos);
      assert(NULL != data);
      int ret = TFS_SUCCESS;
      NewClient* new_client = NewClientManager::get_instance().create_client();
      if (NULL != new_client)
      {
        const int32_t data_num = GET_DATA_MEMBER_NUM(family_info.family_aid_info_);
        const int32_t check_num = GET_CHECK_MEMBER_NUM(family_info.family_aid_info_);
        for (int index = 0; (index < data_num + check_num) && (TFS_SUCCESS == ret); index++)
        {
          if (erased[index] != NODE_ALIVE)
          {
            continue; // read data from alive nodes
          }

          // read if needed, at least send one request here
          if (read_infos[index].first > 0)
          {
            uint8_t send_id = 0;
            ReadRawdataMessageV2 req_msg;
            req_msg.set_block_id(family_info.members_[index].first);
            req_msg.set_length(read_infos[index].first);
            req_msg.set_offset(read_infos[index].second);
            req_msg.set_degrade_flag(true); // to tell the peer, don't control flow
            ret = new_client->post_request(family_info.members_[index].second, &req_msg, send_id);
          }
        }
      }
      else
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      }

      if (TFS_SUCCESS == ret)
      {
        new_client->wait(timeout_ms);
        process_fail_response(new_client);
        vector<NewClient::SEND_SIGN_PAIR>& send_id_sign = new_client->get_send_id_sign();
        NewClient::RESPONSE_MSG_MAP* sresponse = new_client->get_success_response();
        NewClient::RESPONSE_MSG_MAP::iterator iter = sresponse->begin();
        assert(NULL != sresponse);
        ret = (sresponse->size() == send_id_sign.size()) ? TFS_SUCCESS : EXIT_TIMEOUT_ERROR;
        for ( ; (iter != sresponse->end()) && (TFS_SUCCESS == ret); iter++)
        {
          uint64_t server_id = iter->second.first;
          tbnet::Packet* ret_msg = iter->second.second;
          int32_t index = family_info.get_index_by_server(server_id);
          ret = (index >= 0) ? TFS_SUCCESS : EXIT_RECVMSG_ERROR;
          if (TFS_SUCCESS == ret)
          {
            if (READ_RAWDATA_RESP_MESSAGE_V2 == ret_msg->getPCode())
            {
              ReadRawdataRespMessageV2* resp_msg = dynamic_cast<ReadRawdataRespMessageV2* >(ret_msg);
              int32_t length = resp_msg->get_length();
              memcpy(data[index], resp_msg->get_data(), length);
            }
            else if (STATUS_MESSAGE == ret_msg->getPCode())
            {
              StatusMessage* resp_msg = dynamic_cast<StatusMessage* >(ret_msg);
              ret = resp_msg->get_status();
            }
            else
            {
              ret = EXIT_UNKNOWN_MSGTYPE;
            }
          }
        }
      }

      NewClientManager::get_instance().destroy_client(new_client);

      return ret;
    }

    int DataHelper::async_query_ec_meta(const common::FamilyInfoExt& family_info, const int* erased,
        ECMeta* ec_metas, const int32_t timeout_ms)
    {
      assert(NULL != erased);
      assert(NULL != ec_metas);
      int ret = TFS_SUCCESS;
      NewClient* new_client = NewClientManager::get_instance().create_client();
      if (NULL != new_client)
      {
        const int32_t data_num = GET_DATA_MEMBER_NUM(family_info.family_aid_info_);
        const int32_t check_num = GET_CHECK_MEMBER_NUM(family_info.family_aid_info_);
        for (int index = 0; (index < data_num + check_num) && (TFS_SUCCESS == ret); index++)
        {
          if (erased[index] != NODE_ALIVE)
          {
            continue; // qeury meta from alive nodes
          }

          uint8_t send_id = 0;
          QueryEcMetaMessage req_msg;
          req_msg.set_block_id(family_info.members_[index].first);
          ret = new_client->post_request(family_info.members_[index].second, &req_msg, send_id);
        }
      }
      else
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      }

      if (TFS_SUCCESS == ret)
      {
        new_client->wait(timeout_ms);
        process_fail_response(new_client);
        vector<NewClient::SEND_SIGN_PAIR>& send_id_sign = new_client->get_send_id_sign();
        NewClient::RESPONSE_MSG_MAP* sresponse = new_client->get_success_response();
        NewClient::RESPONSE_MSG_MAP::iterator iter = sresponse->begin();
        assert(NULL != sresponse);
        ret = (sresponse->size() == send_id_sign.size()) ? TFS_SUCCESS : EXIT_TIMEOUT_ERROR;
        for ( ; (iter != sresponse->end()) && (TFS_SUCCESS == ret); iter++)
        {
          uint64_t server_id = iter->second.first;
          tbnet::Packet* ret_msg = iter->second.second;
          int32_t index = family_info.get_index_by_server(server_id);
          ret = (index >= 0) ? TFS_SUCCESS : EXIT_RECVMSG_ERROR;
          if (TFS_SUCCESS == ret)
          {
            if (QUERY_EC_META_RESP_MESSAGE == ret_msg->getPCode())
            {
              QueryEcMetaRespMessage* resp_msg = dynamic_cast<QueryEcMetaRespMessage* >(ret_msg);
              ec_metas[index] = resp_msg->get_ec_meta();
            }
            else if (STATUS_MESSAGE == ret_msg->getPCode())
            {
              StatusMessage* resp_msg = dynamic_cast<StatusMessage* >(ret_msg);
              ret = resp_msg->get_status();
            }
            else
            {
              ret = EXIT_UNKNOWN_MSGTYPE;
            }
          }
        }
      }

      NewClientManager::get_instance().destroy_client(new_client);

      return ret;

    }

  }
}
