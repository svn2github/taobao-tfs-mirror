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

#include "tbsys.h"
#include "common/error_msg.h"
#include "common/config_item.h"
#include "common/internal.h"
#include "common/base_packet.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "common/client_manager.h"
#include "block_manager.h"
#include "writable_block_manager.h"
#include "lease_managerv2.h"
#include "dataservice.h"
#include "op_manager.h"

using namespace tfs::common;

namespace tfs
{
  namespace dataserver
  {
    OpManager::OpManager(DataService& service): service_(service), base_op_id_(0)
    {
    }

    OpManager::~OpManager()
    {
      OPMETA_MAP_ITER iter = op_metas_.begin();
      for (; iter != op_metas_.end(); iter++)
      {
        tbsys::gDelete(iter->second);
      }
    }

    BlockManager& OpManager::get_block_manager()
    {
      return service_.get_block_manager();
    }

    LeaseManager& OpManager::get_lease_manager()
    {
      return service_.get_lease_manager();
    }

    // prepare_op is called in write/prepare unlink stage
    // a client request directly send to ds when direct flag is set
    int OpManager::prepare_op(uint64_t& block_id, uint64_t& file_id, uint64_t& op_id,
        const OpType type, const bool is_master, const FamilyInfoExt& family_info, VUINT64& servers)
    {
      int ret = TFS_SUCCESS;

      WritableBlock* block = NULL;
      if (is_master && INVALID_OP_ID == op_id)
      {
        if (INVALID_BLOCK_ID == block_id)
        {
          ret = get_lease_manager().alloc_writable_block(block);
        }
        else
        {
          ret = get_lease_manager().alloc_update_block(block_id, block);
        }

        if (TFS_SUCCESS == ret)
        {
          block_id = block->get_block_id();
          block->get_servers(servers);
          assert(servers.size() > 0);
        }
      }

      // run till here, blockid shouldn't be invalid
      if (TFS_SUCCESS == ret)
      {
        ret = (INVALID_BLOCK_ID != block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      }

      // transfor server if block in family
      if ((TFS_SUCCESS == ret) && (INVALID_FAMILY_ID != family_info.family_id_))
      {
        VUINT64 check_servers;
        family_info.get_check_servers(check_servers);
        for (uint32_t i = 0; i < check_servers.size(); i++)
        {
          if (INVALID_SERVER_ID != check_servers[i])
          {
            servers.push_back(check_servers[i]);
          }
        }
      }

      if ((TFS_SUCCESS == ret) && (0 == (file_id & 0xFFFFFFFF)))
      {
        // should keep the high-32 bit of file_id unchanged
        uint64_t alloc_file_id = 0;
        ret = get_block_manager().generation_file_id(alloc_file_id, block_id);
        if (TFS_SUCCESS == ret)
        {
          file_id |= (alloc_file_id & 0xFFFFFFFF);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        if (INVALID_OP_ID == op_id)
        {
          op_id = gen_op_id();
        }

        OpId oid(block_id, file_id, op_id);
        OpMeta* op_meta = NULL;
        ret = get(oid, op_meta);

        // op meta doesn't exist, create
        if (TFS_SUCCESS != ret)
        {
          if (!out_of_limit())
          {
            add(oid, servers, type);
            ret = get(oid, op_meta);
          }
        }

        if (TFS_SUCCESS == ret)
        {
          op_meta->set_members(servers);
        }

        put(op_meta);
      }

      return ret;
    }

    int OpManager::reset_op(const uint64_t block_id, const uint64_t file_id, const uint64_t op_id,
        common::VUINT64& servers)
    {
      int ret = ((INVALID_BLOCK_ID != block_id) && (INVALID_FILE_ID != file_id) &&
          (INVALID_OP_ID != op_id)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;

      if (TFS_SUCCESS == ret)
      {
        ret = get_lease_manager().has_valid_lease(Func::get_monotonic_time()) ?
          TFS_SUCCESS : EXIT_BLOCK_LEASE_INVALID_ERROR;
      }

      if (TFS_SUCCESS == ret)
      {
        OpId oid(block_id, file_id, op_id);
        OpMeta* op_meta = NULL;
        ret = get(oid, op_meta);
        if (TFS_SUCCESS == ret)
        {
          op_meta->reset_members();
          op_meta->get_servers(servers);
        }
        put(op_meta);
      }

      return ret;
    }

    int OpManager::forward_op(tbnet::Packet* message,
        const uint64_t block_id, const int64_t family_id, const VUINT64& servers)
    {
      // post request to slaves
      int ret = TFS_SUCCESS;
      NewClient* client = NewClientManager::get_instance().create_client();
      assert(NULL != client);
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();

      // take master's version to slave
      BlockInfoV2 block_info;
      ret = get_block_manager().get_block_info(block_info, block_id);
      for (uint32_t i = 0; TFS_SUCCESS == ret && i < servers.size(); i++)
      {
        if (servers[i] == ds_info.information_.id_)
        {
          continue;  // exclude self
        }

        if (WRITE_FILE_MESSAGE_V2 == message->getPCode())
        {
          WriteFileMessageV2* msg = dynamic_cast<WriteFileMessageV2*>(message);
          msg->set_version(block_info.version_);
          if (INVALID_FAMILY_ID != family_id)
          {
            FamilyInfoExt& info = msg->get_family_info();
            msg->set_block_id(info.get_block(servers[i]));
            info.family_id_ = INVALID_FAMILY_ID;  // family will not take to slave
          }
        }
        else if (CLOSE_FILE_MESSAGE_V2 == message->getPCode())
        {
          CloseFileMessageV2* msg = dynamic_cast<CloseFileMessageV2*>(message);
          if (INVALID_FAMILY_ID != family_id)
          {
            FamilyInfoExt& info = msg->get_family_info();
            msg->set_block_id(info.get_block(servers[i]));
            info.family_id_ = INVALID_FAMILY_ID;
          }
        }
        else if (UNLINK_FILE_MESSAGE_V2 == message->getPCode())
        {
          UnlinkFileMessageV2* msg = dynamic_cast<UnlinkFileMessageV2*>(message);
          msg->set_version(block_info.version_);
          if (INVALID_FAMILY_ID != family_id)
          {
            FamilyInfoExt& info = msg->get_family_info();
            msg->set_block_id(info.get_block(servers[i]));
            info.family_id_ = INVALID_FAMILY_ID;
          }
        }
        else
        {
          assert(false);
        }

        ret = post_msg_to_server(servers[i], client, message, ds_async_callback);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "forward request to slave fail, ret : %d", ret);
        }
      }

      return ret;
    }

    int OpManager::update_op(const uint64_t block_id, const uint64_t file_id, const uint64_t op_id,
        tbnet::Packet* packet)
    {
      int ret = ((INVALID_BLOCK_ID != block_id) && (INVALID_FILE_ID != file_id) &&
          (INVALID_OP_ID != op_id) && (NULL != packet)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;

      if (TFS_SUCCESS == ret)
      {
        OpId oid(block_id, file_id, op_id);
        OpMeta* op_meta = NULL;
        ret = get(oid, op_meta);
        if (TFS_SUCCESS == ret)
        {
          if (SLAVE_DS_RESP_MESSAGE != packet->getPCode())
          {
            op_meta->update_member();
          }
          else
          {
            SlaveDsRespMessage* smsg = dynamic_cast<SlaveDsRespMessage*>(packet);
            if (TFS_SUCCESS == ret)
            {
              op_meta->update_member(smsg->get_server_id(),
                  smsg->get_block_info(), smsg->get_status());
            }
          }
        }
        put(op_meta);
      }

      return ret;
    }

    int OpManager::update_op(const uint64_t block_id, const uint64_t file_id, const uint64_t op_id,
        const int32_t error_code, const BlockInfoV2& info)
    {
      int ret = ((INVALID_BLOCK_ID != block_id) && (INVALID_FILE_ID != file_id) &&
          (INVALID_OP_ID != op_id)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;

      if (TFS_SUCCESS == ret)
      {
        OpId oid(block_id, file_id, op_id);
        OpMeta* op_meta = NULL;
        ret = get(oid, op_meta);
        if (TFS_SUCCESS == ret)
        {
          DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
          op_meta->update_member(ds_info.information_.id_, info, error_code);
        }
        put(op_meta);
      }

      return ret;
    }

    bool OpManager::check_op(const uint64_t block_id, const uint64_t file_id, const uint64_t op_id,
        OpStat& stat)
    {
      bool all_finish = false;
      int ret = ((INVALID_BLOCK_ID != block_id) && (INVALID_FILE_ID != file_id) &&
          (INVALID_OP_ID != op_id)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;

      if (TFS_SUCCESS == ret)
      {
        OpId oid(block_id, file_id, op_id);
        OpMeta* op_meta = NULL;
        ret = get(oid, op_meta);
        if (TFS_SUCCESS == ret)
        {
          all_finish = op_meta->check(stat);
        }
        put(op_meta);
      }

      // parameter invalid, or lease expired, etc
      // request is finished, just return error to client
      if (TFS_SUCCESS != ret)
      {
        all_finish = true;
        stat.status_ = ret;
        stat.error_ << "prepare fail or meta expired";
        stat.size_ = 0;
        stat.cost_ = 0;
      }

      return all_finish;
    }

    void OpManager::release_op(const uint64_t block_id, const uint64_t file_id, const uint64_t op_id,
        const int32_t status)
    {
      int ret = ((INVALID_BLOCK_ID != block_id) && (INVALID_FILE_ID != file_id) &&
          (INVALID_OP_ID != op_id)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        get_lease_manager().free_writable_block(block_id);
        OpId oid(block_id, file_id, op_id);
        remove(oid);

        // write error, this block no longer writable, expire it
        if (TFS_SUCCESS != status)
        {
          get_lease_manager().expire_block(block_id);
          TBSYS_LOG(DEBUG, "expire block %"PRI64_PREFIX"u because write error, ret: %d", block_id, ret);
        }
      }
    }

    int OpManager::write_file(const uint64_t block_id, const uint64_t attach_block_id,
        const uint64_t file_id, const uint64_t op_id,
        const char* buffer, const int32_t length, const int32_t offset,
        const int32_t remote_version, BlockInfoV2& local)
    {
      int ret = (INVALID_BLOCK_ID != block_id) && (INVALID_BLOCK_ID != attach_block_id) &&
        (INVALID_FILE_ID != file_id) && (INVALID_LEASE_ID != op_id ) &&
        (NULL != buffer) && (offset >= 0) || (length > 0) ?
        TFS_SUCCESS : EXIT_PARAMETER_ERROR;

      if ((TFS_SUCCESS == ret) && (remote_version >= 0))
      {
        ret = get_block_manager().check_block_version(local, remote_version, block_id, attach_block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write version conflict. blockid: %"PRI64_PREFIX"u, version: %d<>%d",
            block_id, remote_version, local.version_);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        OpId oid(attach_block_id, file_id, op_id);
        OpMeta* op_meta = NULL;
        ret = get(oid, op_meta);
        if (TFS_SUCCESS == ret)
        {
          DataFile& data_file = dynamic_cast<WriteOpMeta* >(op_meta)->get_data_file();
          FileInfoInDiskExt none;
          none.version_ = FILE_INFO_EXT_INIT_VERSION;
          ret = data_file.pwrite(none, buffer, length, offset);
          ret = (ret < 0) ? ret : TFS_SUCCESS; // transform return status
          op_meta->update_last_time(Func::get_monotonic_time());
        }
        put(op_meta);
      }

      return  ret;
    }

    int OpManager::close_file(const uint64_t block_id, const uint64_t attach_block_id,
        uint64_t& file_id, const uint64_t op_id, const uint32_t crc, const int32_t status,
        const bool tmp, BlockInfoV2& local)
    {
      int ret = (INVALID_BLOCK_ID != block_id) && (INVALID_BLOCK_ID != attach_block_id) &&
        (INVALID_FILE_ID != file_id) && (INVALID_LEASE_ID != op_id ) ?
        TFS_SUCCESS : EXIT_PARAMETER_ERROR;

      OpMeta* op_meta = NULL;
      if (TFS_SUCCESS == ret)
      {
        OpId oid(attach_block_id, file_id, op_id);
        ret = get(oid, op_meta);
        if ((TFS_SUCCESS == ret) && !tmp) // write tmp block won't check crc
        {
          DataFile& data_file = dynamic_cast<WriteOpMeta* >(op_meta)->get_data_file();
          if (crc != data_file.crc())
          {
            TBSYS_LOG(WARN, "crc error. blockid: %"PRI64_PREFIX"u, "
                "attach_blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, crc: %u<>%u",
                block_id, attach_block_id, file_id, crc, data_file.crc());
            ret = EXIT_CHECK_CRC_ERROR;
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        DataFile& data_file = dynamic_cast<WriteOpMeta* >(op_meta)->get_data_file();
        data_file.set_status(status);  // status will directly set to file
        ret = get_block_manager().write(file_id, data_file, block_id, attach_block_id, tmp);
        ret = (ret < 0) ? ret: TFS_SUCCESS;  // transform return status
        if (TFS_SUCCESS == ret)
        {
          ret = get_block_manager().get_block_info(local, block_id, tmp);
        }
        op_meta->set_file_size(data_file.length());
      }
      put(op_meta);

      return (ret < 0) ? ret: TFS_SUCCESS;
    }

    int OpManager::prepare_unlink_file(const uint64_t block_id, const uint64_t attach_block_id,
        const uint64_t file_id, const int64_t op_id, const int32_t action,
        const int32_t remote_version, BlockInfoV2& local)
    {
      UNUSED(action);
      int ret = (INVALID_BLOCK_ID != block_id) && (INVALID_BLOCK_ID != attach_block_id) &&
        (INVALID_FILE_ID != file_id) && (INVALID_LEASE_ID != op_id ) ?
        TFS_SUCCESS : EXIT_PARAMETER_ERROR;

      if ((TFS_SUCCESS == ret) && (remote_version >= 0))
      {
        ret = get_block_manager().check_block_version(local, remote_version, block_id, attach_block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "unlink version conflict. blockid: %"PRI64_PREFIX"u, version: %d<>%d",
            block_id, remote_version, local.version_);
        }
      }

      return ret;
    }

    int OpManager::unlink_file(const uint64_t block_id, const uint64_t attach_block_id,
        const uint64_t file_id, const int64_t op_id, const int32_t action,
        BlockInfoV2& local)
    {
      int ret = (INVALID_BLOCK_ID != block_id) && (INVALID_BLOCK_ID != attach_block_id) &&
        (INVALID_FILE_ID != file_id) && (INVALID_LEASE_ID != op_id ) ?
        TFS_SUCCESS : EXIT_PARAMETER_ERROR;

      int64_t file_size = 0;
      if (TFS_SUCCESS == ret)
      {
        ret = get_block_manager().unlink(file_size, file_id, action, block_id, attach_block_id);
        if (TFS_SUCCESS == ret)
        {
          ret = get_block_manager().get_block_info(local, block_id);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        OpId oid(attach_block_id, file_id, op_id);
        OpMeta* op_meta = NULL;
        ret = get(oid, op_meta);
        if (TFS_SUCCESS == ret)
        {
          op_meta->set_file_size(file_size);
        }
        put(op_meta);
      }

      return ret;
    }

    int OpManager::update_block_info(const BlockInfoV2& block_info, const common::UpdateBlockInfoType type)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      UpdateBlockInfoMessageV2 req_msg;
      req_msg.set_block_info(block_info);
      req_msg.set_type(type);
      req_msg.set_server_id(ds_info.information_.id_);

      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL == client)
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      }
      else
      {
        tbnet::Packet* ret_msg = NULL;
        ret = send_msg_to_server(ds_info.ns_vip_port_, client, &req_msg, ret_msg);
        if (TFS_SUCCESS == ret)
        {
          if (STATUS_MESSAGE == ret_msg->getPCode())
          {
            StatusMessage* smsg = dynamic_cast<StatusMessage*>(ret_msg);
            ret = smsg->get_status();
            TBSYS_LOG(DEBUG, "update block info. blockid: %"PRI64_PREFIX"u, status: %d %s",
                block_info.block_id_, smsg->get_status(), smsg->get_error());
          }
          else
          {
            ret = EXIT_COMMIT_BLOCK_UPDATE_ERROR;
          }
        }
        NewClientManager::get_instance().destroy_client(client);
      }

      return ret;
    }

    int OpManager::resolve_block_version_conflict(const uint64_t block_id,
       const uint64_t file_id, const uint64_t op_id)
    {
      int ret = ((INVALID_BLOCK_ID != block_id) && (INVALID_FILE_ID != file_id) &&
          (INVALID_OP_ID != op_id)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;

      ResolveBlockVersionConflictMessage req_msg;
      if (TFS_SUCCESS == ret)
      {
        OpId oid(block_id, file_id, op_id);
        OpMeta* op_meta = NULL;
        ret = get(oid, op_meta);
        if (TFS_SUCCESS == ret)
        {
          req_msg.set_block(block_id);
          std::pair<uint64_t, BlockInfoV2>* members = req_msg.get_members();
          ArrayHelper<std::pair<uint64_t, BlockInfoV2> > helper(MAX_REPLICATION_NUM, members);
          ret = op_meta->get_members(helper);
          if (TFS_SUCCESS == ret)
          {
            req_msg.set_size(helper.get_array_index());
          }
        }
        put(op_meta);
      }

      if (TFS_SUCCESS == ret)
      {
        NewClient* client = NewClientManager::get_instance().create_client();
        if (NULL == client)
        {
          ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        }
        else
        {
          DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
          tbnet::Packet* ret_msg = NULL;
          ret = send_msg_to_server(ds_info.ns_vip_port_, client, &req_msg, ret_msg);
          if (TFS_SUCCESS == ret)
          {
            if (RSP_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE == ret_msg->getPCode())
            {
              ResolveBlockVersionConflictResponseMessage* msg =
                dynamic_cast<ResolveBlockVersionConflictResponseMessage*>(ret_msg);
              ret = msg->get_status();
            }
            else
            {
              ret = EXIT_RESOLVE_BLOCK_VERSION_CONFLICT_ERROR;
            }
          }
          NewClientManager::get_instance().destroy_client(client);
        }
      }

      return ret;
    }

    uint64_t OpManager::gen_op_id()
    {
      return atomic_inc(&base_op_id_);
    }

    void OpManager::add(const OpId& oid, const VUINT64& servers, OpType type)
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      OpMeta* op_meta = NULL;
      if (OP_TYPE_UNLINK == type)
      {
        op_meta = new (std::nothrow) OpMeta(oid, servers);
      }
      else
      {
        op_meta = new (std::nothrow) WriteOpMeta(oid, servers);
      }
      assert(NULL != op_meta);
      std::pair<OPMETA_MAP_ITER, bool> res =
        op_metas_.insert(OPMETA_MAP::value_type(oid, op_meta));
      if (!res.second)
      {
        tbsys::gDelete(op_meta);
      }
    }

    void OpManager::remove(const OpId& oid)
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      OPMETA_MAP_ITER iter = op_metas_.find(oid);
      if (iter != op_metas_.end())
      {
        if (iter->second->get_ref() <= 0)
        {
          tbsys::gDelete(iter->second);
          op_metas_.erase(iter);
        }
      }
    }

    int OpManager::get(const OpId& oid, OpMeta*& op_meta)
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      int ret = TFS_SUCCESS;
      OPMETA_MAP_ITER iter = op_metas_.find(oid);
      if (iter != op_metas_.end())
      {
        op_meta = iter->second;
        assert(NULL != op_meta);
        op_meta->inc_ref();
      }
      else
      {
        ret = EXIT_OP_META_ERROR; // op meta already expired
      }
      return ret;
    }

    void OpManager::put(OpMeta* op_meta)
    {
      if (NULL != op_meta)
      {
        op_meta->dec_ref();
      }
    }

    void OpManager::timeout(const time_t now)
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      uint32_t total = op_metas_.size();
      OPMETA_MAP_ITER iter = op_metas_.begin();
      for ( ; iter != op_metas_.end(); )
      {
        if (iter->second->get_ref() <= 0 && iter->second->timeout(now))
        {
          get_lease_manager().free_writable_block(iter->first.block_id_);
          tbsys::gDelete(iter->second);
          op_metas_.erase(iter++);
        }
        else
        {
          iter++;
        }
      }
      TBSYS_LOG(DEBUG, "expire op meta, old: %u, new: %zd", total, op_metas_.size());
    }

    bool OpManager::out_of_limit() const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return op_metas_.size() >= static_cast<uint32_t>(SYSPARAM_DATASERVER.max_datafile_nums_);
    }

  }/** end namespace dataserver**/
}/** end namespace tfs**/
