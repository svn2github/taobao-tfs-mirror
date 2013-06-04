/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: func.cpp 400 2011-06-02 07:26:40Z duanfei@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#include "show_factory.h"

using namespace __gnu_cxx;
using namespace tbsys;
using namespace tfs::message;
using namespace tfs::common;

namespace tfs
{
  namespace tools
  {
    void compute_tp(Throughput* tp, int32_t time)
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
    void add_tp(const Throughput* atp, const Throughput* btp, Throughput* result_tp, const int32_t sign)
    {
      result_tp->write_byte_ = atp->write_byte_ + sign * btp->write_byte_;
      result_tp->write_file_count_ = atp->write_file_count_ + sign * btp->write_file_count_;
      result_tp->read_byte_ = atp->read_byte_ + sign * btp->read_byte_;
      result_tp->read_file_count_ = atp->read_file_count_ + sign * btp->read_file_count_;
    }
    void print_header(const int8_t print_type, const int8_t type, FILE* fp)
    {
      if (print_type & SERVER_TYPE)
      {
        if (type & SERVER_TYPE_SERVER_INFO)
        {
          fprintf(fp, "    SERVER_ADDR       UCAP  / TCAP =  UR  BLKCNT LOAD  TOTAL_WRITE  TOTAL_READ   LAST_WRITE   LAST_READ   STARTUP_TIME\n");
        }
        if (type & SERVER_TYPE_BLOCK_LIST)
        {
          fprintf(fp, "SERVER_ADDR           CNT BLOCK \n");
        }
        if (type & SERVER_TYPE_BLOCK_WRITABLE)
        {
          fprintf(fp, "SERVER_ADDR           CNT WRITABLE BLOCK\n");
        }
        if (type & SERVER_TYPE_BLOCK_MASTER)
        {
          fprintf(fp, "SERVER_ADDR           CNT MASTER BLOCK\n");
        }
      }
      if (print_type & BLOCK_TYPE)
      {
        if (type & BLOCK_TYPE_BLOCK_INFO)
        {
          fprintf(fp, "%-10s %-20s %-8s %-8s %-10s %-8s %-10s %-8s %-10s %-8s\n", "FAMILY_ID", "BLOCK_ID", "VERS", "CNT", "SIZE", "DEL_CNT", "DEL_SIZE", "UP_CNT", "UP_SIZE", "COPYS");
          //fprintf(fp, "  FAMILY_ID BLOCK_ID   VERSION    FILECOUNT  SIZE       DEL_FILE   DEL_SIZE   COPYS\n");
        }
        if (type & BLOCK_TYPE_SERVER_LIST)
        {
          fprintf(fp, "  BLOCK_ID   SERVER_LIST\n");
        }
      }
      if (print_type & MACHINE_TYPE)
      {
        if (type & MACHINE_TYPE_ALL)
        {
          fprintf(fp, "  SERVER_IP     NUMS UCAP  / TCAP =  UR  BLKCNT  LOAD TOTAL_WRITE  TOTAL_READ  LAST_WRITE  LAST_READ  MAX_WRITE   MAX_READ\n");
          fprintf(fp,
              "--------------- ---- ------------------ -------- ---- -----------  ----------  ----------  ---------  --------  ---------\n");
        }
        if (type & MACHINE_TYPE_PART)
        {
          fprintf(fp, "  SERVER_IP     NUMS UCAP  / TCAP =  UR  BLKCNT  LOAD LAST_WRITE  LAST_READ  MAX_WRITE  MAX_READ STARTUP_TIME\n");
          fprintf(fp,
              "--------------- ---- ------------------ -------- ---- ----------  ---------  ---------  -------- ------------\n");
        }
      }
    }
    int ServerShow::serialize(tbnet::DataBuffer& output, int32_t& length)
    {
      output.writeInt64(id_);
      output.writeInt64(use_capacity_);
      output.writeInt64(total_capacity_);
      output.writeInt32(current_load_);
      output.writeInt32(block_count_);
      output.writeInt64(last_update_time_);
      output.writeInt64(startup_time_);

      output.writeInt64(total_tp_.write_byte_);
      output.writeInt64(total_tp_.read_byte_);
      output.writeInt64(total_tp_.write_file_count_);
      output.writeInt64(total_tp_.read_file_count_);
      output.writeInt64(total_tp_.unlink_file_count_);
      output.writeInt64(total_tp_.fail_write_byte_);
      output.writeInt64(total_tp_.fail_read_byte_);
      output.writeInt64(total_tp_.fail_write_file_count_);
      output.writeInt64(total_tp_.fail_read_file_count_);
      output.writeInt64(total_tp_.fail_unlink_file_count_);

      output.writeInt64(current_time_);
      output.writeInt32(status_);

      length += (output.getDataLen());

      return TFS_SUCCESS;
    }
    int ServerShow::deserialize(tbnet::DataBuffer& input, int32_t& length)
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

      total_tp_.write_byte_ = input.readInt64();
      total_tp_.read_byte_ = input.readInt64();
      total_tp_.write_file_count_ = input.readInt64();
      total_tp_.read_file_count_ = input.readInt64();
      total_tp_.unlink_file_count_ = input.readInt64();
      total_tp_.fail_write_byte_ = input.readInt64();
      total_tp_.fail_read_byte_ = input.readInt64();
      total_tp_.fail_write_file_count_ = input.readInt64();
      total_tp_.fail_read_file_count_ = input.readInt64();
      total_tp_.fail_unlink_file_count_ = input.readInt64();

      current_time_ = input.readInt64();
      status_ = static_cast<DataServerLiveStatus>(input.readInt32());

      length -= (len - input.getDataLen());
      return TFS_SUCCESS;
    }
    int ServerShow::calculate(ServerShow& old_server)
    {
      int32_t time = current_time_ - old_server.current_time_;
      add_tp(&total_tp_, &old_server.total_tp_, &last_tp_, SUB_OP);
      compute_tp(&last_tp_, time);

      time = current_time_ - startup_time_;
      compute_tp(&total_tp_, time);
      return TFS_SUCCESS;
    }
    void ServerShow::dump(const uint64_t server_id, const std::set<uint64_t>& blocks, FILE* fp) const
    {
      if (fp == NULL) { return; }

      fprintf(fp, "%17s ", tbsys::CNetUtil::addrToString(server_id).c_str());
      fprintf(fp, "%6Zd ", blocks.size());
      std::set<uint64_t>::const_iterator iter = blocks.begin();
      int32_t count = 0;
      for (; iter != blocks.end(); iter++)
      {
        fprintf(fp, "%12"PRI64_PREFIX"u",(*iter));
        if (count >= MAX_COUNT)
        {
          fprintf(fp, "\n%25s", " ");
          count = 0;
        }
        count++;
      }
      fprintf(fp, "\n");
    }
    void ServerShow::dump(const int8_t type, FILE* fp) const
    {
      if (fp == NULL) { return; }
      if (type & SERVER_TYPE_SERVER_INFO)
      {
        fprintf(fp, "%17s %7s %7s %2d%% %6d %6d %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %-19s\n",
            tbsys::CNetUtil::addrToString(id_).c_str(),
            Func::format_size(use_capacity_).c_str(),
            Func::format_size(total_capacity_).c_str(),
            total_capacity_ > 0 ? static_cast<int32_t> (use_capacity_ * 100 / total_capacity_) : 0,
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
      if (type & SERVER_TYPE_BLOCK_LIST)
      {
        dump(id_, hold_, fp);
      }
      if (type & SERVER_TYPE_BLOCK_WRITABLE)
      {
        dump(id_, writable_, fp);
      }
      if (type & SERVER_TYPE_BLOCK_MASTER)
      {
        dump(id_, master_, fp);
      }
    }

//**********************************************************************
//**************************Block Info**********************************
    void BlockShow::dump(int8_t type, FILE* fp) const
    {
      if (fp == NULL) { return; }
      if (type & BLOCK_TYPE_BLOCK_INFO)
      {
        fprintf(fp, "%-10"PRI64_PREFIX"d %-20"PRI64_PREFIX"u %-8d %-8d %-10d %-8d %-10d %-8d %-10d %-6Zd", info_.family_id_, info_.block_id_, info_.version_, info_.file_count_, info_.size_,
            info_.del_file_count_, info_.del_size_, info_.update_file_count_,
            info_.update_size_, server_list_.size());
      }
      if (type & BLOCK_TYPE_SERVER_LIST)
      {
        fprintf(fp, "%15"PRI64_PREFIX"u", info_.block_id_);
        std::string server_str = "";
        std::vector<ServerInfo>::const_iterator iter = server_list_.begin();
        for (; iter != server_list_.end(); iter++)
        {
          server_str += (" " + static_cast<std::string> (tbsys::CNetUtil::addrToString((*iter).server_id_).c_str()));
        }
        fprintf(fp, " %s", server_str.c_str());
      }
      fprintf(fp, "\n");
    }

//**********************************************************************
//**************************Machine Info**********************************
    MachineShow::MachineShow() :
      machine_id_(0), use_capacity_(0), total_capacity_(0), current_load_(0), block_count_(0),
      last_startup_time_(0), consume_time_(0), index_(0)
    {
      memset(&total_tp_, 0, sizeof(total_tp_));
      memset(&last_tp_, 0, sizeof(last_tp_));
      memset(&last_tp_, 0, sizeof(max_tp_));
    }

    int MachineShow::init(ServerShow& server, ServerShow& old_server)
    {
      int32_t time = server.current_time_ - old_server.current_time_;
      Throughput tmp_tp_;
      add_tp(&server.total_tp_, &old_server.total_tp_, &tmp_tp_, SUB_OP);
      compute_tp(&tmp_tp_, time);
      memcpy(&max_tp_, &tmp_tp_, sizeof(max_tp_));
      last_startup_time_ = server.startup_time_;
      return TFS_SUCCESS;
    }

    int MachineShow::add(ServerShow& server, ServerShow& old_server)
    {
      use_capacity_ += server.use_capacity_;
      total_capacity_ += server.total_capacity_;
      block_count_ += server.block_count_;
      current_load_ += server.current_load_;

      int32_t time = server.current_time_ - old_server.current_time_;
      Throughput tmp_tp_;
      add_tp(&server.total_tp_, &old_server.total_tp_, &tmp_tp_, SUB_OP);

      //last tp, sum all last server value
      add_tp(&last_tp_, &tmp_tp_, &last_tp_, ADD_OP);
      consume_time_ += time;
      index_++;

      //get max tp, which is the max tp(average) of one process
      compute_tp(&tmp_tp_, time);

      if (max_tp_.write_byte_ <= tmp_tp_.write_byte_)
      {
        max_tp_.write_byte_ = tmp_tp_.write_byte_;
        max_tp_.write_file_count_ = tmp_tp_.write_file_count_;
      }
      if (max_tp_.read_byte_ <= tmp_tp_.read_byte_)
      {
        max_tp_.read_byte_ = tmp_tp_.read_byte_;
        max_tp_.read_file_count_ = tmp_tp_.read_file_count_;
      }
      if (last_startup_time_ < server.startup_time_)
      {
        last_startup_time_ = server.startup_time_;
      }

      //just add all value(div time first)
      time = server.current_time_ - server.startup_time_;
      compute_tp(&server.total_tp_, time);
      add_tp(&total_tp_, &server.total_tp_, &total_tp_, ADD_OP);
      return TFS_SUCCESS;

    }
    int MachineShow::calculate()
    {
      // then div
      if (consume_time_ > 0 && index_ > 0)
      {
        int32_t per_time = (consume_time_ / index_);
        compute_tp(&last_tp_, per_time);
      }
      return TFS_SUCCESS;
    }
    void MachineShow::dump(const int8_t flag, FILE* fp) const
    {
      if (fp == NULL) { return; }
      if (flag & MACHINE_TYPE_ALL)
      {
        fprintf(fp, "  %-12s  %4d %5s %7s  %2d%%  %6d  %2d %6s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %4s %5"PRI64_PREFIX"d %3s %5"PRI64_PREFIX"d %3s %5"PRI64_PREFIX"d\n",
            tbsys::CNetUtil::addrToString(machine_id_).c_str(),
            index_,
            Func::format_size(use_capacity_).c_str(),
            Func::format_size(total_capacity_).c_str(),
            total_capacity_ > 0 ? static_cast<int32_t> (use_capacity_ * 100 / total_capacity_) : 0,
            block_count_,
            index_ > 0 ? (current_load_ / index_) : current_load_,
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
      else if (flag & MACHINE_TYPE_PART)
      {
        fprintf(fp, "  %-12s  %4d %6s %7s  %2d%%  %6d  %2d %5s %4"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %4s %5"PRI64_PREFIX"d %-19s\n",
            tbsys::CNetUtil::addrToString(machine_id_).c_str(),
            index_,
            Func::format_size(use_capacity_).c_str(),
            Func::format_size(total_capacity_).c_str(),
            total_capacity_ > 0 ? static_cast<int32_t> (use_capacity_ * 100 / total_capacity_) : 0,
            block_count_,
            index_ > 0 ? current_load_ / index_ : current_load_,
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
    int StatStruct::add(ServerShow& server)
    {
      server_count_++;
      use_capacity_ += server.use_capacity_;
      total_capacity_ += server.total_capacity_;
      current_load_ += server.current_load_;
      block_count_ += server.block_count_;

      add_tp(&total_tp_, &server.total_tp_, &total_tp_, ADD_OP);
      add_tp(&last_tp_, &server.last_tp_, &last_tp_, ADD_OP);
      return TFS_SUCCESS;
    }
    int StatStruct::add(MachineShow& machine)
    {
      server_count_ += machine.index_;
      machine_count_++;
      use_capacity_ += machine.use_capacity_;
      total_capacity_ += machine.total_capacity_;
      current_load_ += machine.current_load_;
      block_count_ += machine.block_count_;

      add_tp(&total_tp_, &machine.total_tp_, &total_tp_, ADD_OP);
      add_tp(&last_tp_, &machine.last_tp_, &last_tp_, ADD_OP);
      return TFS_SUCCESS;
    }
    int StatStruct::add(BlockShow& block)
    {
      block_count_++;
      file_count_ += block.info_.file_count_;
      block_size_ += (block.info_.size_ * (block.server_list_.size()));
      delfile_count_ += block.info_.del_file_count_;
      block_del_size_ += block.info_.del_size_;
      return TFS_SUCCESS;
    }

    void StatStruct::dump(const int8_t type, const int8_t sub_type, FILE* fp) const
    {
      if (fp == NULL) { return; }
      if (type & SERVER_TYPE)
      {
        if (sub_type & SERVER_TYPE_SERVER_INFO)
        {
          fprintf(fp, "TOTAL: %5d %5s %7s %7s %2d%% %6d %7d %8s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %6s %4"PRI64_PREFIX"d\n\n",
              server_count_,
              "",
              Func::format_size(use_capacity_).c_str(),
              Func::format_size(total_capacity_).c_str(),
              total_capacity_ > 0 ? static_cast<int32_t> (use_capacity_ * 100 / total_capacity_):0,
              block_count_,
              server_count_ > 0 ? static_cast<int32_t> (current_load_/server_count_) : 0,
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
      }
      if (type & BLOCK_TYPE)
      {
        if (sub_type & BLOCK_TYPE_BLOCK_INFO)
        {
          fprintf(fp, "TOTAL: %-2d %18"PRI64_PREFIX"d %10s %9"PRI64_PREFIX"d %12s PRE_FILE(%s)\n\n",
              block_count_,
              file_count_,
              Func::format_size(block_size_).c_str(),
              delfile_count_,
              Func::format_size(block_del_size_).c_str(),
              Func::format_size(file_count_ > 0 ? (block_size_ / file_count_) : 0).c_str()
              );
        }
      }

      if (type & MACHINE_TYPE)
      {
        if (sub_type & MACHINE_TYPE_ALL)
        {
          fprintf(fp, "Total : %-10d %-1d %3s %7s  %2d%%  %6d  %2u %6s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %4s %5"PRI64_PREFIX"d\n\n",
              machine_count_,
              server_count_,
              Func::format_size(use_capacity_).c_str(),
              Func::format_size(total_capacity_).c_str(),
              total_capacity_ > 0 ? static_cast<int32_t> (use_capacity_ * 100 / total_capacity_) : 0,
              block_count_,
              server_count_ > 0 ? static_cast<int32_t> (current_load_/server_count_) : 0,
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
        if (sub_type & MACHINE_TYPE_PART)
        {
          fprintf(fp, "Total : %-10d %-d %2s %7s  %2d%%  %6d  %2u %6s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d\n\n",
              machine_count_,
              server_count_,
              Func::format_size(use_capacity_).c_str(),
              Func::format_size(total_capacity_).c_str(),
              total_capacity_ > 0 ? static_cast<int32_t> (use_capacity_ * 100 / total_capacity_) : 0,
              block_count_,
              server_count_ > 0 ? static_cast<int32_t> (current_load_/server_count_) : 0,
              Func::format_size(last_tp_.write_byte_).c_str(),
              last_tp_.write_file_count_,
              Func::format_size(last_tp_.read_byte_).c_str(),
              last_tp_.read_file_count_
              );
        }
        if (sub_type & MACHINE_TYPE_FOR_MONITOR)
        {
          time_t t;
          time(&t);
          struct tm tm;
          ::localtime_r((const time_t*)&t, &tm);

          fprintf(fp, "[%04d-%02d-%02d %02d:%02d:%02d] write_flow(MBps)=%s, write_tps=%"PRI64_PREFIX"d, read_flow(MBps)=%s, read_tps=%"PRI64_PREFIX"d\n",
              tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday,
              tm.tm_hour, tm.tm_min, tm.tm_sec,
              Func::format_size(last_tp_.write_byte_, 'M').c_str(),
              last_tp_.write_file_count_,
              Func::format_size(last_tp_.read_byte_, 'M').c_str(),
              last_tp_.read_file_count_
              );

          if (fp == stdout)
          {
            fflush(stdout);
          }
        }
      }
    }
  }
}
