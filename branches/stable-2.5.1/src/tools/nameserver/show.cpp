/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: show.cpp 868 2011-09-29 05:07:38Z duanfei@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#include "show.h"
#include "common/status_message.h"

using namespace __gnu_cxx;
using namespace tbsys;
using namespace tfs::message;
using namespace tfs::common;
using namespace std;

namespace tfs
{
  namespace tools
  {
    typedef map<string, int32_t> STR_INT_MAP;
    static string LAST_DS_FILE = "";

    int ShowInfo::set_ns_ip(const std::string& ns_ip_port)
    {
      ns_ip_ = get_addr(ns_ip_port);

      char pid_tmp[16];
      sprintf(pid_tmp, "%d", getpid());
      string pid(pid_tmp);
      LAST_DS_FILE = "/tmp/.tfs_last_ds_v2_" + pid;
      // delete old tmp file when exist at the begining, in case load_last_ds to read error data
      if (access(LAST_DS_FILE.c_str(), F_OK) == 0)
      {
        DirectoryOp::delete_file(LAST_DS_FILE.c_str());
      }

      return TFS_SUCCESS;
    }
    void ShowInfo::load_last_ds()
    {
      int32_t fd = open(LAST_DS_FILE.c_str(), O_RDONLY);
      if (fd < 0)
      {
        TBSYS_LOG(DEBUG, "open file(%s) fail,errors(%s)", LAST_DS_FILE.c_str(), strerror(errno));
        return;
      }
      int32_t size = 0;
      if (read(fd, reinterpret_cast<char*>(&size), INT_SIZE) != INT_SIZE)
      {
        close(fd);
        TBSYS_LOG(ERROR, "read size fail");
        return;
      }
      for (int i = 0; i < size; i++)
      {
        ServerShow server;
        int32_t length = 0;
        if (read(fd, &length, sizeof(int32_t)) != sizeof(int32_t))
        {
          close(fd);
          return;
        }
        tbnet::DataBuffer input;
        input.ensureFree(length);
        input.pourData(length);
        if (read(fd, input.getData(), length) != length)
        {
          close(fd);
          return;
        }
        int32_t offset = 0; // useless
        server.deserialize(input, length, offset);
        last_server_map_[server.server_stat_.id_] = server;
      }
      close(fd);
    }
    void ShowInfo::save_last_ds()
    {
      int32_t fd = open(LAST_DS_FILE.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
      if (fd < 0)
      {
        TBSYS_LOG(ERROR, "open file(%s) fail,errors(%s)", LAST_DS_FILE.c_str(), strerror(errno));
        return;
      }
      int32_t size = server_map_.size();
      if (write(fd, reinterpret_cast<char*>(&size), INT_SIZE) != INT_SIZE)
      {
        close(fd);
        return;
      }
      map<uint64_t, ServerShow>::iterator iter = server_map_.begin();
      for (; iter != server_map_.end(); iter++)
      {
        tbnet::DataBuffer output;
        int32_t length = 0;
        (iter->second).serialize(output, length);
        if (write(fd, &length, sizeof(int32_t)) != sizeof(int32_t))
        {
          close(fd);
          TBSYS_LOG(ERROR, "write length fail,errors(%s)", strerror(errno));
          return;
        }
        if (write(fd, output.getData(), length) != length)
        {
          close(fd);
          TBSYS_LOG(ERROR, "write data fail,errors(%s)", strerror(errno));
          return;
        }
      }
      close(fd);
    }
    void ShowInfo::clean_last_file()
    {
      if (access(LAST_DS_FILE.c_str(), 0) == 0)
      {
        DirectoryOp::delete_file(LAST_DS_FILE.c_str());
      }
    }
    int ShowInfo::get_file_handle(const string& filename, FILE** fp)
    {
      if (filename != "")
      {
        *fp = fopen(filename.c_str(), "w");
        if (*fp == NULL)
        {
          TBSYS_LOG(ERROR, "open file(%s) error...", filename.c_str());
          return EXIT_OPEN_FILE_ERROR;
        }
      }
      else
      {
        *fp = stdout;
      }
      return TFS_SUCCESS;
    }

    void ShowInfo::put_file_handle(FILE* fp)
    {
      if (NULL != fp && stdout != fp)
      {
        fclose(fp);
      }
    }

    int ShowInfo::show_server(const int8_t type, const int32_t num, const string& server_ip_port, int32_t count, const int32_t interval, const bool need_family, const string& filename)
    {
      FILE* fp = NULL;
      if (TFS_SUCCESS != get_file_handle(filename, &fp))
      {
        return EXIT_TFS_ERROR;
      }

      interrupt_ = false;
      is_loop_ = (count == 0);

      while ((count > 0 || is_loop_) && !interrupt_)
      {
        last_server_map_.clear();
        load_last_ds();
        int32_t last_server_size = last_server_map_.size();

        server_map_.clear();
        StatStruct stat;

        ShowServerInformationMessage msg;
        SSMScanParameter& param = msg.get_param();
        param.type_ = SSM_TYPE_SERVER;
        param.child_type_ = SSM_CHILD_SERVER_TYPE_INFO;

        if (type & SERVER_TYPE_BLOCK_LIST)
        {
          param.child_type_ |= SSM_CHILD_SERVER_TYPE_HOLD;
        }
        if (type & SERVER_TYPE_BLOCK_WRITABLE)
        {
          param.child_type_ |= SSM_CHILD_SERVER_TYPE_WRITABLE;
        }
        if (type & SERVER_TYPE_BLOCK_MASTER)
        {
          param.child_type_ |= SSM_CHILD_SERVER_TYPE_MASTER;
        }
        bool once = false;
        if (server_ip_port != "")
        {
          param.should_actual_count_ = 0x10000;
          int ret = get_addr(server_ip_port, param.addition_param1_, param.addition_param2_);
          if (ret != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "server(%s) not valid", server_ip_port.c_str());
            put_file_handle(fp);
            return ret;
          }
          once = true;
        }
        else
        {
          param.start_next_position_ = 0x0;//它对SSM_TYPE_SERVER是没用的,因为ns直接使用addition_param
          param.should_actual_count_= (num << 16);
          param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_YES;
        }

        while ((!((param.end_flag_ >> 4) & SSM_SCAN_END_FLAG_YES)) && !interrupt_)
        {
          param.data_.clear();
          tbnet::Packet* ret_msg = NULL;
          NewClient* client = NewClientManager::get_instance().create_client();
          int ret = send_msg_to_server(ns_ip_, client, &msg, ret_msg);
          if (TFS_SUCCESS != ret || ret_msg == NULL)
          {
            TBSYS_LOG(ERROR, "get server info error, ret: %d", ret);
            NewClientManager::get_instance().destroy_client(client);
            put_file_handle(fp);
            return EXIT_TFS_ERROR;
          }
          if(ret_msg->getPCode() != SHOW_SERVER_INFORMATION_MESSAGE)
          {
            TBSYS_LOG(ERROR, "get invalid message type, pcode: %d", ret_msg->getPCode());
            NewClientManager::get_instance().destroy_client(client);
            put_file_handle(fp);
            return EXIT_TFS_ERROR;
          }
          ShowServerInformationMessage* message = dynamic_cast<ShowServerInformationMessage*>(ret_msg);
          SSMScanParameter& ret_param = message->get_param();

          int32_t data_len = ret_param.data_.getDataLen();
          int32_t offset = 0;
          if (data_len > 0)
          {
            print_header(SERVER_TYPE, type, fp);
          }
          else if (once)
          {
            fprintf(fp, "server(%s) not exists\n", server_ip_port.c_str());
            break;
          }
          while ((data_len > offset) && !interrupt_)
          {
            ServerShow server, old_server;
            if (TFS_SUCCESS == server.ServerBase::deserialize(ret_param.data_, data_len, offset, type))
            {
              if (need_family)
              {
                ret = server.fetch_family_set();
                if (TFS_SUCCESS != ret)
                {
                  TBSYS_LOG(WARN, "server(%s) fetch family list fail, ret: %d",
                      tbsys::CNetUtil::addrToString(server.server_stat_.id_).c_str(), ret);
                }
              }
              server.ServerBase::dump();
              if (once && ((tbsys::CNetUtil::addrToString(server.server_stat_.id_)) != server_ip_port))
              {
                break;
              }
              server_map_[server.server_stat_.id_] = server;
              old_server = server;
              if (last_server_size > 0)
              {
                map<uint64_t, ServerShow>::iterator iter = last_server_map_.find(server.server_stat_.id_);
                if (iter != last_server_map_.end())
                {
                  old_server = iter->second;
                  //old_server.ServerBase::dump();
                }
              }
              last_server_map_[server.server_stat_.id_] = server;
              server.calculate(old_server);
              stat.add(server);
              server.dump(type, fp);
            }
          }
          param.addition_param1_ = ret_param.addition_param1_;
          param.addition_param2_ = ret_param.addition_param2_;
          param.end_flag_ = ret_param.end_flag_;
          NewClientManager::get_instance().destroy_client(client);
          save_last_ds();
          if (once)
          {
            break;
          }
        }
        // print total info when print info only
        if (!once)
        {
          stat.dump(SERVER_TYPE, type, fp);
        }
        if ((--count) > 0 || is_loop_)
        {
          sleep(interval);
        }
      }
      put_file_handle(fp);
      return TFS_SUCCESS;
    }

    int ShowInfo::show_machine(const int8_t type, const int32_t num, int32_t count, const int32_t interval, const bool need_family, const string& filename)
    {
      FILE* fp = NULL;
      if (TFS_SUCCESS != get_file_handle(filename, &fp))
      {
        return EXIT_TFS_ERROR;
      }

      interrupt_ = false;
      is_loop_ = (count == 0);

      while ((count > 0 || is_loop_) && !interrupt_)
      {
        last_server_map_.clear();
        load_last_ds();
        int32_t last_server_size = last_server_map_.size();
        server_map_.clear();
        machine_map_.clear();
        StatStruct stat;

        ShowServerInformationMessage msg;
        SSMScanParameter& param = msg.get_param();
        param.type_ = SSM_TYPE_SERVER;
        param.child_type_ = SSM_CHILD_SERVER_TYPE_INFO;

        param.start_next_position_ = 0x0;
        param.should_actual_count_= (num << 16);
        param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_YES;

        while ((!((param.end_flag_ >> 4) & SSM_SCAN_END_FLAG_YES)) && !interrupt_)
        {
          param.data_.clear();
          tbnet::Packet*ret_msg = NULL;
          NewClient* client = NewClientManager::get_instance().create_client();
          int ret = send_msg_to_server(ns_ip_, client, &msg, ret_msg);
          if (TFS_SUCCESS != ret || ret_msg == NULL)
          {
            TBSYS_LOG(ERROR, "get server info error, ret: %d", ret);
            NewClientManager::get_instance().destroy_client(client);
            put_file_handle(fp);
            return EXIT_TFS_ERROR;
          }
          if(ret_msg->getPCode() != SHOW_SERVER_INFORMATION_MESSAGE)
          {
            TBSYS_LOG(ERROR, "get invalid message type, pcode: %d", ret_msg->getPCode());
            NewClientManager::get_instance().destroy_client(client);
            put_file_handle(fp);
            return EXIT_TFS_ERROR;
          }
          ShowServerInformationMessage* message = dynamic_cast<ShowServerInformationMessage*>(ret_msg);
          SSMScanParameter& ret_param = message->get_param();

          int32_t data_len = ret_param.data_.getDataLen();
          int32_t offset = 0;
          print_header(MACHINE_TYPE, type, fp);
          while ((data_len > offset) && !interrupt_)
          {
            ServerShow server;
            if (TFS_SUCCESS == server.ServerBase::deserialize(ret_param.data_, data_len, offset, SERVER_TYPE_SERVER_INFO))
            {
              server_map_[server.server_stat_.id_] = server;
              ServerShow old_server;
              old_server = server;
              if (last_server_size > 0)
              {
                map<uint64_t, ServerShow>::iterator iter = last_server_map_.find(server.server_stat_.id_);
                if (iter != last_server_map_.end())
                {
                  old_server = iter->second;
                }
              }
              last_server_map_[server.server_stat_.id_] = server;
              if (need_family)
              {
                ret = server.fetch_family_set();
                if (TFS_SUCCESS != ret)
                {
                  TBSYS_LOG(WARN, "server(%s) fetch family list fail, ret: %d",
                      tbsys::CNetUtil::addrToString(server.server_stat_.id_).c_str(), ret);
                }
              }
              uint64_t machine_id = get_machine_id(server.server_stat_.id_);
              map<uint64_t, MachineShow>::iterator iter = machine_map_.find(machine_id);
              if (iter != machine_map_.end())
              {
                (iter->second).add(server, old_server);
              }
              else
              {
                MachineShow machine;
                machine.machine_id_ = machine_id;
                machine.init(server, old_server);
                machine.add(server, old_server);
                machine_map_.insert(make_pair<uint64_t, MachineShow> (machine_id, machine));
              }
            }
          }
          param.addition_param1_ = ret_param.addition_param1_;
          param.addition_param2_ = ret_param.addition_param2_;
          param.end_flag_ = ret_param.end_flag_;
          NewClientManager::get_instance().destroy_client(client);
          save_last_ds();
        }

        map<uint64_t, MachineShow>::iterator it = machine_map_.begin();
        for (; it != machine_map_.end(); it++)
        {
          (it->second).calculate();
          (it->second).dump(type, fp);
          stat.add(it->second);
        }
        // first time last_server_size=0, all tps are 0, so not show it for tps monitor
        if (last_server_size > 0 || !(type & MACHINE_TYPE_FOR_MONITOR))
        {
          stat.dump(MACHINE_TYPE, type, fp);
        }
        if (--count || is_loop_)
        {
          sleep(interval);
        }
      }

      put_file_handle(fp);
      return TFS_SUCCESS;

    }

    int ShowInfo::show_block_common(const int32_t num, const uint64_t block_id, int32_t count,
        const int32_t interval, const string& filename, BaseBlockWorker* worker)
    {
      FILE* fp = NULL;
      if (TFS_SUCCESS != get_file_handle(filename, &fp))
      {
        return EXIT_TFS_ERROR;
      }
      interrupt_ = false;
      is_loop_ = (count == 0);

      while ((count > 0 || is_loop_) && !interrupt_)
      {
        if(TFS_SUCCESS != worker->begin())
        {
          put_file_handle(fp);
          return EXIT_TFS_ERROR;
        }
        ShowServerInformationMessage msg;
        SSMScanParameter& param = msg.get_param();
        param.type_ = SSM_TYPE_BLOCK;//遍历ns上的block数据的类型
        param.child_type_ = SSM_CHILD_BLOCK_TYPE_INFO | SSM_CHILD_BLOCK_TYPE_SERVER;//TYPE_INFO只为取block_id, TYPE_SERVER为ds_list
        if (worker->sub_type_ & BLOCK_TYPE_BLOCK_STATUS)
        {
          param.child_type_ |= SSM_CHILD_BLOCK_TYPE_STATUS;
        }
        if (worker->sub_type_ & BLOCK_TYPE_BLOCK_FULL) // additional condition: full block
        {
          param.child_type_ |= SSM_CHILD_BLOCK_TYPE_FULL;
        }

        bool once = false;
        if (block_id > 0)
        {
          param.should_actual_count_ = 0x10000;
          param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_NO;
          param.start_next_position_ = ((block_id % nameserver::MAX_BLOCK_CHUNK_NUMS) << 16);//start是本次在ns上开始扫描blocks的桶的位置
          param.addition_param1_ = block_id;
          once = true;
        }
        else
        {
          param.should_actual_count_ = (num << 16);
          param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_YES;
        }

        print_header(worker->block_type_, worker->sub_type_, fp);
        while ((!((param.end_flag_ >> 4) & SSM_SCAN_END_FLAG_YES)) && !interrupt_)
        {
          param.data_.clear();
          tbnet::Packet*ret_msg = NULL;
          NewClient* client = NewClientManager::get_instance().create_client();
          int ret = send_msg_to_server(ns_ip_, client, &msg, ret_msg);
          if (TFS_SUCCESS != ret || ret_msg == NULL)
          {
            TBSYS_LOG(ERROR, "get block info error, ret: %d", ret);
            NewClientManager::get_instance().destroy_client(client);
            put_file_handle(fp);
            return EXIT_TFS_ERROR;
          }
          if(ret_msg->getPCode() != SHOW_SERVER_INFORMATION_MESSAGE)
          {
            if (ret_msg->getPCode() == STATUS_MESSAGE)
            {
              StatusMessage* msg = dynamic_cast<StatusMessage*>(ret_msg);
              TBSYS_LOG(ERROR, "get invalid message type: error: %s", msg->get_error());
            }
            TBSYS_LOG(ERROR, "get invalid message type, pcode: %d", ret_msg->getPCode());
            NewClientManager::get_instance().destroy_client(client);
            put_file_handle(fp);
            return EXIT_TFS_ERROR;
          }
          ShowServerInformationMessage* message = dynamic_cast<ShowServerInformationMessage*>(ret_msg);
          SSMScanParameter& ret_param = message->get_param();

          int32_t data_len = ret_param.data_.getDataLen();
          int32_t offset = 0;
          //如果指定的blockid不存在，且block表中[start_next_position_, +00)这些桶内没有block,则拉取block个数为0
          while ((data_len > offset) && !interrupt_)
          {
            BlockBase* base_block = worker->create_block();
            ret = base_block->deserialize(ret_param.data_, data_len, offset, param.child_type_);
            if (TFS_SUCCESS == ret)
            {
              base_block->BlockBase::dump();// log info of each block info
              if (once && (base_block->info_.block_id_ != block_id))
              {
                //TBSYS_LOG(ERROR, "block: %"PRI64_PREFIX"u,%"PRI64_PREFIX"u not exists", base_block->info_.block_id_, block_id);
                break;
              }
              worker->process(base_block, fp);// not sort by block
            }
            else
            {
              TBSYS_LOG(ERROR, "Block deserialize error, ret: %d", ret);
              NewClientManager::get_instance().destroy_client(client);
              put_file_handle(fp);
              return ret;
            }
            tbsys::gDelete(base_block);
          }
          param.start_next_position_ = (ret_param.start_next_position_ << 16) & 0xffff0000;
          param.end_flag_ = ret_param.end_flag_;
          if (param.end_flag_ & SSM_SCAN_CUTOVER_FLAG_NO)
          {
            param.addition_param1_ = ret_param.addition_param2_;
          }
          NewClientManager::get_instance().destroy_client(client);
          if (once)
          {
            break;
          }
        }
        if (!once)
        {
          worker->end(fp);
        }
        if (--count)
        {
          sleep(interval);
        }
      }
      put_file_handle(fp);
      return TFS_SUCCESS;
    }

    // show block info
    int ShowInfo::show_block(const int8_t type, const int32_t num, const uint64_t block_id, int32_t count, const int32_t interval, const string& filename)
    {
      BlockInfoWorker worker(type);
      return show_block_common(num, block_id, count, interval, filename, &worker);
    }

    int ShowInfo::show_family(const int8_t type, const int32_t num, const int64_t family_id, int32_t count, const int32_t interval, const string& filename)
    {
      interrupt_ = false;
      is_loop_ = (count == 0);//count表示循环重复拉取的次数,默认是1，0表示一直按照间间隔循环
      FILE* fp = NULL;
      if (TFS_SUCCESS != get_file_handle(filename, &fp))
      {
        return EXIT_TFS_ERROR;
      }

      while ((count > 0 || is_loop_) && !interrupt_)
      {
        ShowServerInformationMessage msg;
        SSMScanParameter& param = msg.get_param();
        param.type_ = SSM_TYPE_FAMILY;

        uint64_t family_count = 0;
        bool once = false;
        if (family_id > 0)
        {
          param.should_actual_count_ = 0x10000;
          param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_NO;
          param.start_next_position_ = ((family_id % nameserver::MAX_FAMILY_CHUNK_NUM) << 16);
          param.addition_param1_ = family_id;
          once = true;
        }
        else if (family_id == 0)
        {
          if (type & BLOCK_TYPE_SERVER_LIST)
          {
            fprintf(fp, "para error, -s must be used with -d\n");
            put_file_handle(fp);
            return EXIT_PARAMETER_ERROR;
          }
          param.should_actual_count_ = (num << 16);
          param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_YES;
          //其余，如param.start_next_position_ 等都初始化为0
        }
        else
        {
          TBSYS_LOG(ERROR, "get invalid family_id: %"PRI64_PREFIX"d < 0", family_id);
          put_file_handle(fp);
          return EXIT_PARAMETER_ERROR;
        }

        while ((!((param.end_flag_ >> 4) & SSM_SCAN_END_FLAG_YES)) && !interrupt_)
        {
          param.data_.clear();
          tbnet::Packet*ret_msg = NULL;
          NewClient* client = NewClientManager::get_instance().create_client();
          int ret = send_msg_to_server(ns_ip_, client, &msg, ret_msg);
          if (TFS_SUCCESS != ret || ret_msg == NULL)
          {
            TBSYS_LOG(ERROR, "get block info error, ret: %d", ret);
            NewClientManager::get_instance().destroy_client(client);
            put_file_handle(fp);
            return EXIT_TFS_ERROR;
          }
          if(ret_msg->getPCode() != SHOW_SERVER_INFORMATION_MESSAGE)
          {
            if (ret_msg->getPCode() == STATUS_MESSAGE)
            {
              StatusMessage* msg = dynamic_cast<StatusMessage*>(ret_msg);
              TBSYS_LOG(ERROR, "get invalid message type: error: %s", msg->get_error());
            }
            TBSYS_LOG(ERROR, "get invalid message type, pcode: %d", ret_msg->getPCode());
            NewClientManager::get_instance().destroy_client(client);
            put_file_handle(fp);
            return EXIT_TFS_ERROR;
          }
          ShowServerInformationMessage* message = dynamic_cast<ShowServerInformationMessage*>(ret_msg);
          SSMScanParameter& ret_param = message->get_param();

          int32_t data_len = ret_param.data_.getDataLen();
          int32_t offset = 0;
          if (data_len > 0)
          {
            print_header(FAMILY_TYPE, type, fp);
          }
          while ((data_len > offset) && !interrupt_)
          {
            FamilyShow family;
            if (TFS_SUCCESS == family.deserialize(ret_param.data_, data_len, offset))
            {
              if (once && (family.family_id_ != family_id))
              {
                //TBSYS_LOG(ERROR, "only get family: %"PRI64_PREFIX"u, but %"PRI64_PREFIX"u not exists", family.family_id_, family_id);
                break;
              }
              if (once && (type & BLOCK_TYPE_SERVER_LIST))
              {
                family.get_members_ds_list(ns_ip_);
              }
              family.dump(type, fp);
              ++family_count;
            }
          }
          param.start_next_position_ = (ret_param.start_next_position_ << 16) & 0xffff0000;
          param.end_flag_ = ret_param.end_flag_;
          if (param.end_flag_ & SSM_SCAN_CUTOVER_FLAG_NO)
          {
            param.addition_param1_ = ret_param.addition_param2_;//next start family_id to scan
          }
          NewClientManager::get_instance().destroy_client(client);
          if (once)
          {
            break;
          }
        }
        if (!once)
        {
          fprintf(fp, "Total Count: %"PRI64_PREFIX"u\n", family_count);
        }
        if (--count)
        {
          sleep(interval);
        }
      }
      put_file_handle(fp);
      return TFS_SUCCESS;
    }

    // show block's unnormal copy, include copy in the same machine or rack
    int ShowInfo::show_block_distribution(const int8_t type,  string& rack_ip_mask, const int32_t num, const uint64_t block_id, int32_t count, const int32_t interval, const string& filename)
    {
      if (type & BLOCK_IP_DISTRIBUTION_TYPE)
      {
        rack_ip_mask = "255.255.255.255";// every ip as a single rack
      }
      uint32_t ip_mask = Func::get_addr(rack_ip_mask.c_str());
      BlockDistWorker worker(ip_mask, type);
      return show_block_common(num, block_id, count, interval, filename, &worker);
    }

    // show block_id list by rack
    int ShowInfo::show_rack_block(const int8_t type, string& rack_ip_mask, string& rack_ip_group, const int32_t num, int32_t count, const int32_t interval, const string& filename)
    {
      count = 1;
      uint32_t ip_mask = Func::get_addr(rack_ip_mask.c_str());
      uint32_t ip_group = Func::get_addr(rack_ip_group.c_str());
      BlockRackWorker worker(ip_mask, ip_group, type);
      return show_block_common(num, 0, count, interval, filename, &worker);
    }

    // check whether exist block in ns block table whose ds_list is [D1, D2..., Dn], but Di(dataserver) not hold the block actually
    int ShowInfo::check_block(const int8_t type, const int32_t num, const std::string& filename)
    {
      BlockCheckWorker worker(ns_ip_, type);
      return show_block_common(num, 0, 1, 0, filename, &worker);
    }

    uint64_t ShowInfo::get_machine_id(uint64_t server_id)
    {
      IpAddr* adr = reinterpret_cast<IpAddr *> (&server_id);
      return adr->ip_;
    }
  }
}
