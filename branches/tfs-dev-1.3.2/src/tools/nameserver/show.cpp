#include "show.h"

using namespace __gnu_cxx;
using namespace tbsys;
using namespace tfs::message;
using namespace tfs::nameserver;
using namespace tfs::common;

namespace tfs
{
  namespace tools
  {
    typedef map<std::string, int32_t> STR_INT_MAP;
    static const std::string LAST_DS_FILE("%s/.tfs_last_ds");
    
    ShowInfo::ShowInfo()
    {
    }
    
    ShowInfo::~ShowInfo()
    {
    }
    
    int ShowInfo::set_ns_ip(std::string ns_ip_port)
    {
      ns_ip_ = get_addr(ns_ip_port);
    
      return TFS_SUCCESS;
    }
    void ShowInfo::load_last_ds()
    {
      char* home = getenv("HOME");
      char path[256];
      sprintf(path, LAST_DS_FILE.c_str(), home ? home : "");
      //printf("%s", path);
      int32_t fd = open(path, O_RDONLY);
      if (fd < 0)
      {
        TBSYS_LOG(ERROR, "open file(%s) fail,errors(%s)", path, strerror(errno));
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
        ServerStruct server;
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
        server.deserialize(input, length);
        last_server_map_[server.id_] = server;
      }
      close(fd);
    }
    void ShowInfo::save_last_ds()
    {
      char* home = getenv("HOME");
      char path[256];
      sprintf(path, LAST_DS_FILE.c_str(), home ? home : "");
      int32_t fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
      if (fd < 0)
      {
        TBSYS_LOG(ERROR, "open file(%s) fail,errors(%s)", path, strerror(errno));
        return;
      }
      int32_t size = server_map_.size();
      if (write(fd, reinterpret_cast<char*>(&size), INT_SIZE) != INT_SIZE)
      {
        close(fd);
        return;
      }
      map<uint64_t, ServerStruct>::iterator iter = server_map_.begin();
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
    
    int ShowInfo::show_server(int8_t type, int32_t num)
    {
      load_last_ds();
      int32_t last_server_size = last_server_map_.size();
      server_map_.clear();
      StatStruct stat;
    
      ShowServerInformationMessage msg;
      SSMScanParameter& param = msg.get_param();
      param.type_ = SSM_TYPE_SERVER;
      param.child_type_ = SSM_CHILD_SERVER_TYPE_INFO;
    
      switch (type)
      {
        case CMD_NOP:
          break;
        case CMD_BLOCK_LIST:
          param.child_type_ |= SSM_CHILD_SERVER_TYPE_HOLD;
          break;
        case CMD_BLOCK_WRITABLE:
          param.child_type_ |= SSM_CHILD_SERVER_TYPE_WRITABLE;
          break;
        case CMD_BLOCK_MASTER:
          param.child_type_ |= SSM_CHILD_SERVER_TYPE_MASTER;
          break;
      }
      param.start_next_position_ = 0x0;
      param.should_actual_count_= (num << 16);
      param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_YES;
    
      while (!((param.end_flag_ >> 4) & SSM_SCAN_END_FLAG_YES))
      {
        param.data_.clear();
        Message *ret_msg = NULL;
        int ret = send_message_to_server(ns_ip_, &msg, &ret_msg);
        if (TFS_SUCCESS != ret || ret_msg == NULL)
        {
          TBSYS_LOG(ERROR, "get server info error");
          return TFS_ERROR;
        }
        if(ret_msg->get_message_type() != SHOW_SERVER_INFORMATION_MESSAGE)
        {
          TBSYS_LOG(ERROR, "get invalid message type");
          return TFS_ERROR;
        }
        ShowServerInformationMessage* message = dynamic_cast<ShowServerInformationMessage*>(ret_msg);
        SSMScanParameter& ret_param = message->get_param();
    
        int32_t data_len = ret_param.data_.getDataLen();
        int32_t offset = 0;
        print_header(PRINT_SERVER, param.child_type_);
        while (data_len > offset)
        {
          ServerStruct server;
          if (TFS_SUCCESS == server.deserialize(ret_param.data_, data_len, offset, param.child_type_))
          {
            //server.dump();
            server_map_[server.id_] = server;
            if (last_server_size > 0)
            {
              ServerStruct old_server;
              map<uint64_t, ServerStruct>::iterator iter = last_server_map_.find(server.id_);
              if (iter != last_server_map_.end())
              {
                old_server = iter->second;
              }
              else
              {
                old_server = server;
              }
              server.calculate(old_server);
            }
            stat.add(server);
            server.dump(param.child_type_);
          }
        }
        param.addition_param1_ = ret_param.addition_param1_;
        param.addition_param2_ = ret_param.addition_param2_;
        param.end_flag_ = ret_param.end_flag_;
      }
      // print total info when print info only
      if (!(param.child_type_ & 0x0f))
      {
        stat.dump(PRINT_SERVER, 0);
      }
      save_last_ds();
      return TFS_SUCCESS;
    }
    int ShowInfo::show_machine(int8_t flag, int32_t num)
    {
      load_last_ds();
      int32_t last_server_size = last_server_map_.size();
      machine_map_.clear();
      StatStruct stat;
    
      ShowServerInformationMessage msg;
      SSMScanParameter& param = msg.get_param();
      param.type_ = SSM_TYPE_SERVER;
      param.child_type_ = SSM_CHILD_SERVER_TYPE_INFO;
    
      param.start_next_position_ = 0x0;
      param.should_actual_count_= (num << 16);
      param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_YES;
    
      while (!((param.end_flag_ >> 4) & SSM_SCAN_END_FLAG_YES))
      {
        param.data_.clear();
        Message *ret_msg = NULL;
        int ret = send_message_to_server(ns_ip_, &msg, &ret_msg);
        if (TFS_SUCCESS != ret || ret_msg == NULL)
        {
          TBSYS_LOG(ERROR, "get server info error");
          return TFS_ERROR;
        }
        if(ret_msg->get_message_type() != SHOW_SERVER_INFORMATION_MESSAGE)
        {
          TBSYS_LOG(ERROR, "get invalid message type");
          return TFS_ERROR;
        }
        ShowServerInformationMessage* message = dynamic_cast<ShowServerInformationMessage*>(ret_msg);
        SSMScanParameter& ret_param = message->get_param();
    
        int32_t data_len = ret_param.data_.getDataLen();
        int32_t offset = 0;
        print_header(PRINT_MACHINE, flag);
        while (data_len > offset)
        {
          ServerStruct server;
          if (TFS_SUCCESS == server.deserialize(ret_param.data_, data_len, offset, param.child_type_))
          {
            server_map_[server.id_] = server;
            ServerStruct old_server;
            if (last_server_size > 0)
            {
              map<uint64_t, ServerStruct>::iterator iter = last_server_map_.find(server.id_);
              if (iter != last_server_map_.end())
              {
                old_server = iter->second;
              }
              else
              {
                old_server = server;
              }
            }
            uint64_t machine_id = get_machine_id(server.id_);
            map<uint64_t, MachineStruct>::iterator iter = machine_map_.find(machine_id);
            if (iter != machine_map_.end())
            {
              (iter->second).add(server, old_server);
            }
            else
            {
              MachineStruct machine;
              machine.machine_id_ = machine_id;
              machine.init(server, old_server);
              machine.add(server, old_server);
              machine_map_.insert(make_pair<uint64_t, MachineStruct> (machine_id, machine));
            }
          }
        }
        param.addition_param1_ = ret_param.addition_param1_;
        param.addition_param2_ = ret_param.addition_param2_;
        param.end_flag_ = ret_param.end_flag_;
      }
    
      map<uint64_t, MachineStruct>::iterator it = machine_map_.begin();
      for (; it != machine_map_.end(); it++)
      {
        (it->second).calculate();
        (it->second).dump(flag);
        stat.add(it->second);
      }
      stat.dump(PRINT_MACHINE, flag);
    
      return TFS_SUCCESS;
    
    }
    
    int ShowInfo::show_block(int8_t type, int32_t num, uint32_t block_id)
    {
      StatStruct stat;
    
      ShowServerInformationMessage msg;
      SSMScanParameter& param = msg.get_param();
      param.type_ = SSM_TYPE_BLOCK;
      param.child_type_ = SSM_CHILD_BLOCK_TYPE_INFO;
      if (type == CMD_SERVER)
      {
        param.child_type_ |= SSM_CHILD_BLOCK_TYPE_SERVER;
      }
    
      bool once = false;
      if (block_id != -1)
      {
        param.should_actual_count_ = 0x10000;
        param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_NO;
        param.should_actual_count_ = 0x10000;
        param.start_next_position_ = ((block_id % 32) << 16);
        param.addition_param1_ = block_id;
        once = true;
      }
      else
      {
        param.should_actual_count_ = (num << 16);
        param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_YES;
      }
    
      while (!((param.end_flag_ >> 4) & SSM_SCAN_END_FLAG_YES))
      {
        param.data_.clear();
        Message *ret_msg = NULL;
        int ret = send_message_to_server(ns_ip_, &msg, &ret_msg);
        if (TFS_SUCCESS != ret || ret_msg == NULL)
        {
          TBSYS_LOG(ERROR, "get block info error");
          return TFS_ERROR;
        }
        if(ret_msg->get_message_type() != SHOW_SERVER_INFORMATION_MESSAGE)
        {
          TBSYS_LOG(ERROR, "get invalid message type");
          return TFS_ERROR;
        }
        ShowServerInformationMessage* message = dynamic_cast<ShowServerInformationMessage*>(ret_msg);
        SSMScanParameter& ret_param = message->get_param();
    
        int32_t data_len = ret_param.data_.getDataLen();
        int32_t offset = 0;
        print_header(PRINT_BLOCK, param.child_type_);
        while (data_len > offset)
        {
          BlockStruct block;
          if (TFS_SUCCESS == block.deserialize(ret_param.data_, data_len, offset, param.child_type_))
          {
            stat.add(block);
            block.dump(param.child_type_);
          }
        }
        param.start_next_position_ = (ret_param.start_next_position_ << 16) & 0xffff0000;
        param.end_flag_ = ret_param.end_flag_;
        if (param.end_flag_ & SSM_SCAN_CUTOVER_FLAG_NO)
        {
          param.addition_param1_ = ret_param.addition_param2_;
        }
        if (once)
        {
          break;
        }
      }
      if (!once)
      {
        stat.dump(PRINT_BLOCK, 0);
      }
      return TFS_SUCCESS;
    }
    
    uint64_t ShowInfo::get_addr(std::string ns_ip_port)
    {
      std::string::size_type pos = ns_ip_port.find_first_of(":");
      std::string tmp = ns_ip_port.substr(0, pos);
      const char* ip = tmp.c_str();
      int32_t port = atoi(ns_ip_port.substr(pos + 1, ns_ip_port.length()).c_str());
      return Func::str_to_addr(ip, port);
    }
    uint64_t ShowInfo::get_machine_id(uint64_t server_id)
    {
      IpAddr* adr = (IpAddr *) (&server_id);
      return adr->ip_;
      //char ipport[64] = {'\0'};
      //char ip[64] = {'\0'};
      //strncpy(ipport, tbsys::CNetUtil::addrToString(server_id).c_str(), 32);
      //strncpy(ip, ipport, strchr(ipport, ':') - ipport);
      //return tbsys::CNetUtil::getAddr(ip);
    }
    
    void ShowInfo::print_header(int8_t print_type, int32_t flag)
    {
      if (print_type == PRINT_SERVER)
      {
        if (flag & SSM_CHILD_SERVER_TYPE_ALL)
        {
          flag |= SSM_CHILD_SERVER_TYPE_HOLD;
          flag |= SSM_CHILD_SERVER_TYPE_WRITABLE;
          flag |= SSM_CHILD_SERVER_TYPE_MASTER;
          flag |= SSM_CHILD_SERVER_TYPE_INFO;
        }
        if ((!(flag & 0x0f)) && (flag & SSM_CHILD_SERVER_TYPE_INFO))
        {
          printf("    SERVER_ADDR       UCAP  / TCAP =  UR  BLKCNT LOAD  TOTAL_WRITE  TOTAL_READ   LAST_WRITE   LAST_READ   STARTUP_TIME\n");
        }
    
        if (flag & SSM_CHILD_SERVER_TYPE_HOLD)
        {
          printf("SERVER_ADDR           CNT BLOCK \n");
        }
        if (flag & SSM_CHILD_SERVER_TYPE_WRITABLE)
        {
          printf("SERVER_ADDR           CNT WRITABLE BLOCK\n");
        }
        if (flag & SSM_CHILD_SERVER_TYPE_MASTER)
        {
          printf("SERVER_ADDR           CNT MASTER BLOCK\n");
        }
      }
      else if (print_type == PRINT_BLOCK)
      {
        if (flag & SSM_CHILD_BLOCK_TYPE_INFO)
        {
          printf("  BLOCK_ID   VERSION    FILECOUNT  SIZE       DEL_FILE   DEL_SIZE   SEQ_NO");
        }
        if (flag & SSM_CHILD_BLOCK_TYPE_SERVER)
        {
          printf("   SERVER_LIST");
        }
        printf("\n");
      }
      else if (print_type == PRINT_MACHINE)
      {
        if (flag == PRINT_ALL)
        {
          printf("  SERVER_IP     NUMS UCAP  / TCAP =  UR  BLKCNT  LOAD TOTAL_WRITE  TOTAL_READ  LAST_WRITE  LAST_READ  MAX_WRITE   MAX_READ\n");
          printf(
              "--------------- ---- ------------------ -------- ---- -----------  ----------  ----------  ---------  --------  ---------\n");
        }
        else if (flag = PRINT_PART)
        {
          printf("  SERVER_IP     NUMS UCAP  / TCAP =  UR  BLKCNT  LOAD LAST_WRITE  LAST_READ  MAX_WRITE  MAX_READ STARTUP_TIME\n");
          printf(
              "--------------- ---- ------------------ -------- ---- ----------  ---------  ---------  -------- ------------\n");
        }
      }
    }
  }
}
