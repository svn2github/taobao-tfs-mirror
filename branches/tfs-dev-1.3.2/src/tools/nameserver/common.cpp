#include "common.h"
using namespace tfs::common;

namespace tfs
{
  namespace tools
  {
    static void compute_tp(Throughput* tp, int32_t time)
    {
      if (time < 1)
      {
        return;
      }
      tp->write_byte_ /= time;
      tp->write_file_count_ /= time;
      tp->read_byte_ /= time;
      tp->read_file_count_ /= time;
    }
    
    static void add_tp(Throughput* atp, Throughput* btp, Throughput* result_tp, int32_t sign)
    {
      result_tp->write_byte_ = atp->write_byte_ + sign * btp->write_byte_;
      result_tp->write_file_count_ = atp->write_file_count_ + sign * btp->write_file_count_;
      result_tp->read_byte_ = atp->read_byte_ + sign * btp->read_byte_;
      result_tp->read_file_count_ = atp->read_file_count_ + sign * btp->read_file_count_;
    }
    
    ServerStruct::ServerStruct():
      id_(0), use_capacity_(0), total_capacity_(0), current_load_(0), block_count_(0),
      last_update_time_(0), startup_time_(0), current_time_(0)
    {
      memset(&total_tp_, 0, sizeof(total_tp_));
      memset(&last_tp_, 0, sizeof(last_tp_));
    }
    ServerStruct::~ServerStruct()
    {
    }
    
    int ServerStruct::serialize(tbnet::DataBuffer& output, int32_t& length)
    {
      output.writeInt64(id_);
      output.writeInt64(use_capacity_);
      output.writeInt64(total_capacity_);
      output.writeInt32(current_load_);
      output.writeInt32(block_count_);
      output.writeInt64(last_update_time_);
      output.writeInt64(startup_time_);
      output.writeInt64(current_time_);
      output.writeInt64(total_tp_.write_byte_);
      output.writeInt64(total_tp_.write_file_count_);
      output.writeInt64(total_tp_.read_byte_);
      output.writeInt64(total_tp_.read_file_count_);
      length += (output.getDataLen());
    
      return TFS_SUCCESS;
    }
    
    int ServerStruct::deserialize(tbnet::DataBuffer& input, int32_t& length)
    {
      if (input.getDataLen() <= 0)
      {
        return TFS_ERROR;
      }
      int32_t len = 0;
      len = input.getDataLen();
      id_ = input.readInt64();
      use_capacity_ = input.readInt64();
      total_capacity_ = input.readInt64();
      current_load_ = input.readInt32();
      block_count_  = input.readInt32();
      last_update_time_ = input.readInt64();
      startup_time_ = input.readInt64();
      current_time_ = input.readInt64();
      total_tp_.write_byte_ = input.readInt64();
      total_tp_.write_file_count_ = input.readInt64();
      total_tp_.read_byte_ = input.readInt64();
      total_tp_.read_file_count_ = input.readInt64();
      length -= (len - input.getDataLen());
    }
    int ServerStruct::deserialize(tbnet::DataBuffer& input, const int32_t length, int32_t& offset, int32_t flag)
    {
      if (input.getDataLen() <= 0 || offset >= length)
      {
        return TFS_ERROR;
      }
      int32_t len = 0;
      if (flag & SSM_CHILD_SERVER_TYPE_ALL)
      {
        flag |= SSM_CHILD_SERVER_TYPE_HOLD;
        flag |= SSM_CHILD_SERVER_TYPE_WRITABLE;
        flag |= SSM_CHILD_SERVER_TYPE_MASTER;
        flag |= SSM_CHILD_SERVER_TYPE_INFO;
      }
      if (flag & SSM_CHILD_SERVER_TYPE_INFO)
      {
        len = input.getDataLen();
        id_ = input.readInt64();
        use_capacity_ = input.readInt64();
        total_capacity_ = input.readInt64();
        current_load_ = input.readInt32();
        block_count_  = input.readInt32();
        last_update_time_ = input.readInt64();
        startup_time_ = input.readInt64();
        total_tp_.write_byte_ = input.readInt64();
        total_tp_.write_file_count_ = input.readInt64();
        total_tp_.read_byte_ = input.readInt64();
        total_tp_.read_file_count_ = input.readInt64();
        current_time_ = input.readInt64();
        status_ = (DataServerLiveStatus)input.readInt32();
        offset += (len - input.getDataLen());
      }
      if (flag & SSM_CHILD_SERVER_TYPE_HOLD)
      {
        len = input.getDataLen();
        int32_t hold_size = input.readInt32();
        while (hold_size > 0)
        {
          hold_.push_back(input.readInt32());
          hold_size--;
        }
        offset += (len - input.getDataLen());
    
      }
    
      if (flag & SSM_CHILD_SERVER_TYPE_WRITABLE)
      {
        len = input.getDataLen();
        int32_t writable_size = input.readInt32();
        while (writable_size > 0)
        {
          writable_.push_back(input.readInt32());
          writable_size--;
        }
        offset += (len - input.getDataLen());
      }
    
      if (flag & SSM_CHILD_SERVER_TYPE_MASTER)
      {
        len = input.getDataLen();
        int32_t master_size = input.readInt32();
        while (master_size > 0)
        {
          master_.push_back(input.readInt32());
          master_size--;
        }
        offset += (len - input.getDataLen());
      }
      return TFS_SUCCESS;
    }
    int ServerStruct::calculate(ServerStruct& old_server)
    {
      int32_t time = current_time_ - old_server.current_time_;
      add_tp(&total_tp_, &old_server.total_tp_, &last_tp_, SUB_OP);
      compute_tp(&last_tp_, time);
    
      time = current_time_ - startup_time_;
      compute_tp(&total_tp_, time);
    }
    void ServerStruct::dump(int32_t flag)
    {
      printf("%17s ", tbsys::CNetUtil::addrToString(id_).c_str());

      if ((!(flag & 0x0f)) && (flag & SSM_CHILD_SERVER_TYPE_INFO))
      {
        printf("%7s %7s %2d%% %6d %6d %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %-19s\n",
            Func::format_size(use_capacity_).c_str(),
            Func::format_size(total_capacity_).c_str(),
            static_cast<int32_t>((use_capacity_ * 100 )/ total_capacity_),
            block_count_,
            current_load_,
            Func::format_size(total_tp_.write_byte_).c_str(),
            total_tp_.write_file_count_,
            Func::format_size(total_tp_.read_byte_).c_str(),
            total_tp_.read_file_count_,
            Func::format_size(last_tp_.write_byte_).c_str(),
            last_tp_.write_file_count_,
            Func::format_size(last_tp_.read_byte_).c_str(),
            last_tp_.read_file_count_,
            Func::time_to_str(startup_time_).c_str()
            );
      }
    
      if (flag & SSM_CHILD_SERVER_TYPE_HOLD)
      {
        printf("%6Zd ", hold_.size());
        std::vector<uint32_t>::iterator iter = hold_.begin();
        int32_t count = 0;
        for (; iter != hold_.end(); iter++)
        {
          if (count < 20)
          {
            printf("%u ",(*iter));
          }
          else
          {
            printf("\n");
            count = 0;
          }
          count++;
        }
      }
      if (flag & SSM_CHILD_SERVER_TYPE_WRITABLE)
      {
        printf("%6Zd ", writable_.size());
        std::vector<uint32_t>::iterator iter = writable_.begin();
        int32_t count = 0;
        for (; iter != writable_.end(); iter++)
        {
          if (count < 20)
          {
            printf("%u ",(*iter));
          }
          else
          {
            printf("\n");
            count = 0;
          }
          count++;
        }
      }
      if (flag & SSM_CHILD_SERVER_TYPE_MASTER)
      {
        printf("%-3Zd ", master_.size());
        std::vector<uint32_t>::iterator iter = master_.begin();
        int32_t count = 0;
        for (; iter != master_.end(); iter++)
        {
          if (count < 20)
          {
            printf("%u ",(*iter));
          }
          else
          {
            printf("\n");
            count = 0;
          }
          count++;
        }
      }
      printf("\n");
    }
    bool ServerStruct::cmp(int8_t type, ServerStruct& a)
    {
      if (type == 0)
      {
        return id_ == a.id_;
      }
      else if (type == 1)
      {
    
      }
    }
    void ServerStruct::dump()
    {
      TBSYS_LOG(INFO, "server_id: %"PRI64_PREFIX"d, use_capacity: %u, total_capacity: %u, current_load: %d, block_count: %d, last_update_time: %s, startup_time: %s, write_byte: %"PRI64_PREFIX"d, write_file_count: %"PRI64_PREFIX"d, read_byte:         %"PRI64_PREFIX"d, read_file_count: %"PRI64_PREFIX"d current_time: %s, status: %d",
          id_,
          use_capacity_,
          total_capacity_,
          current_load_,
          block_count_,
          Func::time_to_str(last_update_time_).c_str(),
          Func::time_to_str(startup_time_).c_str(),
          total_tp_.write_byte_,
          total_tp_.write_file_count_,
          total_tp_.read_byte_,
          total_tp_.read_file_count_,
          Func::time_to_str(time(NULL)).c_str(),
          status_
          );
    }
    
    //************************BLOCK**************************
    BlockStruct::BlockStruct()
    {
      memset(&info_, 0, sizeof(info_));
      server_list_.clear();
    }
    BlockStruct::~BlockStruct()
    {
    }
    int32_t BlockStruct::deserialize(tbnet::DataBuffer& input, const int32_t length, int32_t& offset, int8_t flag)
    {
      if (input.getDataLen() <= 0 || offset >= length)
      {
        return TFS_ERROR;
      }
      if (flag & SSM_CHILD_BLOCK_TYPE_INFO)
      {
        input.readBytes(&info_, sizeof(info_));
        offset += sizeof(info_);
      }
      if (flag & SSM_CHILD_BLOCK_TYPE_SERVER)
      {
        int32_t len = input.getDataLen();
        int8_t server_size = input.readInt8();
        while (server_size > 0)
        {
          ServerInfo server_info;
          server_info.server_id_ = input.readInt64();
          server_list_.push_back(server_info);
          server_size--;
        }
        offset += (len - input.getDataLen());
      }
      return TFS_SUCCESS;
    }
    
    void BlockStruct::dump(int32_t flag)
    {
      if (flag & SSM_CHILD_BLOCK_TYPE_INFO)
      {
        printf("%10u %6d %10d %10d %10d %10d %10u", info_.block_id_, info_.version_, info_.file_count_, info_.size_, info_.del_file_count_, info_.del_size_, info_.seq_no_);
      }
      if (flag & SSM_CHILD_BLOCK_TYPE_SERVER)
      {
        std::string server_str = "";
        std::vector<ServerInfo>::iterator iter = server_list_.begin();
        for (; iter != server_list_.end(); iter++)
        {
          server_str += (" " + static_cast<std::string> (tbsys::CNetUtil::addrToString((*iter).server_id_).c_str()));
        }
        printf(" %s", server_str.c_str());
      }
      printf("\n");
    }
    void BlockStruct::dump()
    {
      TBSYS_LOG(INFO, "block_id: %d, version: %d, file_count: %d, size: %d, del_file_count: %d, del_size: %d, seq_no: %d",
          info_.block_id_, info_.version_, info_.file_count_, info_.size_, info_.del_file_count_, info_.del_size_, info_.seq_no_);
    }
    MachineStruct::MachineStruct() :
      machine_id_(0), use_capacity_(0), total_capacity_(0), current_load_(0), block_count_(0),
      last_startup_time_(0), consume_time_(0), index_(0)
    {
      memset(&total_tp_, 0, sizeof(total_tp_));
      memset(&last_tp_, 0, sizeof(last_tp_));
      memset(&last_tp_, 0, sizeof(max_tp_));
    }
    
    MachineStruct::~MachineStruct()
    {
    }
    
    int MachineStruct::init(ServerStruct& server, ServerStruct& old_server)
    {
      int32_t time = server.current_time_ - old_server.current_time_;
      Throughput tmp_tp_;
      add_tp(&server.total_tp_, &old_server.total_tp_, &tmp_tp_, SUB_OP);
      compute_tp(&tmp_tp_, time);
      memcpy(&max_tp_, &tmp_tp_, sizeof(max_tp_));
      last_startup_time_ = server.startup_time_;
    }
    
    int MachineStruct::add(ServerStruct& server, ServerStruct& old_server)
    {
      use_capacity_ += server.use_capacity_;
      total_capacity_ += server.total_capacity_;
      block_count_ += server.block_count_;
      current_load_ = server.current_load_;
    
      // get max tp, which is the max tp(average) of one process
      int32_t time = server.current_time_ - old_server.current_time_;
      ServerStruct tmp_server;
      add_tp(&server.total_tp_, &old_server.total_tp_, &tmp_server.total_tp_, SUB_OP);
      compute_tp(&tmp_server.total_tp_, time);
    
      if (max_tp_.write_byte_ <= tmp_server.total_tp_.write_byte_)
      {
        max_tp_.write_byte_ = tmp_server.total_tp_.write_byte_;
        max_tp_.write_file_count_ = tmp_server.total_tp_.write_file_count_;
      }
      if (max_tp_.read_byte_ <= tmp_server.total_tp_.read_byte_)
      {
        max_tp_.read_byte_ = tmp_server.total_tp_.read_byte_;
        max_tp_.read_file_count_ = tmp_server.total_tp_.read_file_count_;
      }
      if (last_startup_time_ < server.startup_time_)
      {
        last_startup_time_ = server.startup_time_;
      }
      // just add all value(div time first)
      time = server.current_time_ - server.startup_time_;
      compute_tp(&server.total_tp_, time);
      add_tp(&total_tp_, &server.total_tp_, &total_tp_, ADD_OP);
    
      // last tp, sum all last server value
      add_tp(&last_tp_, &old_server.total_tp_, &last_tp_, ADD_OP);
      consume_time_ += (server.current_time_ - old_server.current_time_);
      index_++;
    }
    int MachineStruct::calculate()
    {
      // then div
      if (consume_time_ > 1 && index > 0)
      {
        int32_t per_time = (consume_time_ / index_);
        if (per_time > 0)
        {
          last_tp_.write_byte_ /= per_time;
          last_tp_.write_file_count_ /= per_time;
          last_tp_.read_byte_ /= per_time;
          last_tp_.read_file_count_ /= per_time;
        }
      }
    }
    void MachineStruct::dump(int8_t flag)
    {
      printf("  %-12s  ", tbsys::CNetUtil::addrToString(machine_id_).c_str());
      if (flag & PRINT_ALL)
      {
        printf("%4d %5s %7s  %2d%%  %6"PRI64_PREFIX"u   %2u %6s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %4s %5"PRI64_PREFIX"d %3s %5"PRI64_PREFIX"d %3s %5"PRI64_PREFIX"d",
            index_,
            Func::format_size(use_capacity_).c_str(),
            Func::format_size(total_capacity_).c_str(),
            static_cast<int32_t> (use_capacity_ * 100 )/ total_capacity_,
            block_count_,
            current_load_,
            Func::format_size(total_tp_.write_byte_).c_str(),
            total_tp_.write_file_count_,
            Func::format_size(total_tp_.read_byte_).c_str(),
            total_tp_.read_file_count_,
            Func::format_size(last_tp_.write_byte_).c_str(),
            last_tp_.write_file_count_,
            Func::format_size(last_tp_.read_byte_).c_str(),
            last_tp_.read_file_count_,
            Func::format_size(max_tp_.write_byte_).c_str(),
            max_tp_.write_file_count_,
            Func::format_size(max_tp_.read_byte_).c_str(),
            max_tp_.read_file_count_
            );
      }
      else if (flag & PRINT_PART)
      {
        printf("%-2d %6s %7s  %2d%%  %6d  %2d %5s %4"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %4s %5"PRI64_PREFIX"d %-19s",
            index_,
            Func::format_size(use_capacity_).c_str(),
            Func::format_size(total_capacity_).c_str(),
            (use_capacity_ * 100 )/ total_capacity_,
            block_count_,
            current_load_,
            Func::format_size(last_tp_.write_byte_).c_str(),
            last_tp_.write_file_count_,
            Func::format_size(last_tp_.read_byte_).c_str(),
            last_tp_.read_file_count_,
            Func::format_size(max_tp_.write_byte_).c_str(),
            max_tp_.write_file_count_,
            Func::format_size(max_tp_.read_byte_).c_str(),
            max_tp_.read_file_count_,
            Func::time_to_str(last_startup_time_).c_str()
            );
      }
      printf("\n");
    }
    StatStruct::StatStruct() :
      server_count_(0), machine_count_(0), use_capacity_(0), total_capacity_(0), current_load_(0), block_count_(0),
      file_count_(0), block_size_(0), delfile_count_(0), block_del_size_(0)
    {
      memset(&total_tp_, 0, sizeof(Throughput));
      memset(&last_tp_, 0, sizeof(Throughput));
    }
    StatStruct::~StatStruct()
    {
    }
    int StatStruct::add(ServerStruct& server)
    {
      server_count_++;
      use_capacity_ += server.use_capacity_;
      total_capacity_ += server.total_capacity_;
      current_load_ += server.current_load_;
      block_count_ += server.block_count_;
    
      add_tp(&total_tp_, &server.total_tp_, &total_tp_, ADD_OP);
      add_tp(&last_tp_, &server.last_tp_, &last_tp_, ADD_OP);
    }
    int StatStruct::add(MachineStruct& machine)
    {
      server_count_ += machine.index_;
      machine_count_++;
      use_capacity_ += machine.use_capacity_;
      total_capacity_ += machine.total_capacity_;
      current_load_ += machine.current_load_;
      block_count_ += machine.block_count_;
    
      add_tp(&total_tp_, &machine.total_tp_, &total_tp_, ADD_OP);
      add_tp(&last_tp_, &machine.last_tp_, &last_tp_, ADD_OP);
    }
    int StatStruct::add(BlockStruct& block)
    {
      block_count_++;
      file_count_ += block.info_.file_count_;
      block_size_ += block.info_.size_;
      delfile_count_ += block.info_.del_file_count_;
      block_del_size_ += block.info_.del_size_;
    }
    void StatStruct::dump(int8_t flag, int8_t sub_flag)
    {
      if (flag == PRINT_SERVER)
      {
        printf("TOTAL: %5d %5s %7s %7s %2d%% %6d %7d %8s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %6s %4"PRI64_PREFIX"d\n",
            server_count_,
            "",
            Func::format_size(use_capacity_).c_str(),
            Func::format_size(total_capacity_).c_str(),
            static_cast<int32_t> (use_capacity_ * 100 / total_capacity_),
            block_count_,
            static_cast<int32_t> (current_load_),
            Func::format_size(total_tp_.write_byte_).c_str(),
            total_tp_.write_file_count_,
            Func::format_size(total_tp_.read_byte_).c_str(),
            total_tp_.read_file_count_,
            Func::format_size(last_tp_.write_byte_).c_str(),
            last_tp_.write_file_count_,
            Func::format_size(last_tp_.read_byte_).c_str(),
            last_tp_.read_file_count_
            );
      }
      else if (flag == PRINT_MACHINE)
      {
        if (sub_flag == PRINT_ALL)
        {
        printf("Total : %-10d %-1d %3s %7s  %2d%%  %6"PRI64_PREFIX"u   %2u %6s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %4s %5"PRI64_PREFIX"d\n",
            server_count_,
            machine_count_,
            Func::format_size(use_capacity_).c_str(),
            Func::format_size(total_capacity_).c_str(),
            static_cast<int32_t> (use_capacity_ * 100 / total_capacity_),
            block_count_,
            static_cast<int32_t> (current_load_/server_count_),
            Func::format_size(total_tp_.write_byte_).c_str(),
            total_tp_.write_file_count_,
            Func::format_size(total_tp_.read_byte_).c_str(),
            total_tp_.read_file_count_,
            Func::format_size(last_tp_.write_byte_).c_str(),
            last_tp_.write_file_count_,
            Func::format_size(last_tp_.read_byte_).c_str(),
            last_tp_.read_file_count_
            );
        }
        else
        {
        printf("Total : %-7d %-4d %2s %7s  %2d%%  %6"PRI64_PREFIX"u   %2u %6s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d\n",
            server_count_,
            machine_count_,
            Func::format_size(use_capacity_).c_str(),
            Func::format_size(total_capacity_).c_str(),
            static_cast<int32_t> (use_capacity_ * 100 / total_capacity_),
            block_count_,
            static_cast<int32_t> (current_load_/server_count_),
            Func::format_size(last_tp_.write_byte_).c_str(),
            last_tp_.write_file_count_,
            Func::format_size(last_tp_.read_byte_).c_str(),
            last_tp_.read_file_count_
            );
        }
      }
      else if (flag == PRINT_BLOCK)
      {
        printf("TOTAL: %-2d %18"PRI64_PREFIX"d %10s %9"PRI64_PREFIX"d %10s\n\n",
            block_count_,
            file_count_,
            Func::format_size(block_size_).c_str(),
            delfile_count_,
            Func::format_size(block_del_size_).c_str());
      }
    }
  }
}
