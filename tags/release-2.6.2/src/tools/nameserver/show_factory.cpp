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
#include "tools/util/tool_util.h"

using namespace __gnu_cxx;
using namespace tbsys;
using namespace tfs::message;
using namespace tfs::common;

namespace tfs
{
  namespace tools
  {
    static const int64_t FILE_COUNT_PRECISION_ADJUST = 1000000;

    int get_all_blocks_from_ds(const VUINT64& ds_list, DS_BLOCKS_MAP& all_ds_blocks_map)
    {
      int ret = TFS_SUCCESS;
      all_ds_blocks_map.clear();
      for (uint32_t i = 0; i < ds_list.size(); ++i)
      {
        uint64_t ds_id = ds_list[i];
        ListBlockMessage req_lb_msg;
        req_lb_msg.set_block_type(LB_BLOCK);
        NewClient* client = NewClientManager::get_instance().create_client();
        tbnet::Packet* ret_msg = NULL;
        ret = send_msg_to_server(ds_id, client, &req_lb_msg, ret_msg);
        if (TFS_SUCCESS == ret )
        {
          if (RESP_LIST_BLOCK_MESSAGE == ret_msg->getPCode())
          {
            RespListBlockMessage* resp_lb_msg = dynamic_cast<RespListBlockMessage*>(ret_msg);
            VUINT64* list_blocks = const_cast<VUINT64*> (resp_lb_msg->get_blocks());
            std::pair<DS_BLOCKS_MAP_ITER, bool> res;
            res = all_ds_blocks_map.insert(DS_BLOCKS_MAP::value_type(ds_id, std::set<uint64_t>()));
            assert(res.second);
            res.first->second.insert(list_blocks->begin(), list_blocks->end());
          }
          else
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "list block from ds: %s error, send message ret: %d\n",
              tbsys::CNetUtil::addrToString(ds_id).c_str(), ret);
        }
        NewClientManager::get_instance().destroy_client(client);
      }

      return TFS_SUCCESS;
    }

    void compute_tp(Throughput* tp, int32_t time)
    {
      if (time < 1)
      {
        return;
      }
      tp->write_byte_ /= time;
      tp->write_file_count_ *= FILE_COUNT_PRECISION_ADJUST;
      tp->write_file_count_ /= time;
      tp->read_byte_ /= time;
      tp->read_file_count_ *= FILE_COUNT_PRECISION_ADJUST;
      tp->read_file_count_ /= time;
    }
    void add_tp(const Throughput* atp, const Throughput* btp, Throughput* result_tp, const int32_t sign)
    {
      result_tp->write_byte_ = atp->write_byte_ + sign * btp->write_byte_;
      result_tp->write_file_count_ = atp->write_file_count_ + sign * btp->write_file_count_;
      result_tp->read_byte_ = atp->read_byte_ + sign * btp->read_byte_;
      result_tp->read_file_count_ = atp->read_file_count_ + sign * btp->read_file_count_;
    }
    // if ds restart, will exist negative number
    bool is_tp_valid(const Throughput* tp)
    {
       return tp->write_byte_ >= 0 && tp->write_file_count_ >= 0
           && tp->read_byte_ >= 0 && tp->read_file_count_ >= 0;
    }
    void print_header(const int8_t print_type, const int8_t type, FILE* fp)
    {
      if(print_type & FAMILY_TYPE)
      {
        if (type & BLOCK_TYPE_SERVER_LIST)
        {
         fprintf(fp, "FAMILY_ID  DATA_CNT CHECK_CNT %18s MEMBERS( BLOCK_ID/SERVER_ID )\n", "");
        }
        else
        {
         fprintf(fp, "FAMILY_ID  DATA_CNT CHECK_CNT %12s MEMBERS( BLOCK_ID )\n", "");
        }
      }
      if (print_type & CHECK_BLOCK_TYPE)
      {
        fprintf(fp, "BLOCK_ID  |  NS_MORE_COPY\n");
      }
      if (print_type & SERVER_TYPE)
      {
        if (type & SERVER_TYPE_SERVER_INFO)
        {
          fprintf(fp, "    SERVER_ADDR       UCAP  / TCAP =  UR     BLKCNT FAMILYCNT  RACK_ID  LOAD  TOTAL_WRITE  TOTAL_READ   LAST_WRITE   LAST_READ   STARTUP_TIME\n");
        }
        if (type & SERVER_TYPE_BLOCK_LIST)
        {
          fprintf(fp, "SERVER_ADDR           CNT   BLOCK \n");
        }
        if (type & SERVER_TYPE_BLOCK_WRITABLE)
        {
          fprintf(fp, "SERVER_ADDR           CNT   WRITABLE BLOCK\n");
        }
        if (type & SERVER_TYPE_BLOCK_MASTER)
        {
          fprintf(fp, "SERVER_ADDR           CNT   MASTER BLOCK\n");
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
          fprintf(fp, "FAMILY_ID   BLOCK_ID       SERVER_LIST (SERVER_ID  FAMILY_ID  VERSION) \n");
        }
        if (type & SSM_CHILD_BLOCK_TYPE_STATUS)
        {
          fprintf(fp, "%-10s %-20s %-8s %-12s %-9s %-14s %-12s %s\n", "FAMILY_ID", "BLOCK_ID", "CREATE", "IN_REPLICATE", "HAS_LEASE", "CHOOSE_MASTER", "EXPIRE_TIME", "LAST_LEAVE_TIME");
        }
      }
      if (print_type & MACHINE_TYPE)
      {
        if (type & MACHINE_TYPE_ALL)
        {
          fprintf(fp, "  SERVER_IP     NUMS UCAP  / TCAP =  UR  BLKCNT FAMILYCNT  RACK_ID  LOAD  TOTAL_WRITE  TOTAL_READ  LAST_WRITE  LAST_READ  MAX_WRITE   MAX_READ\n");
          fprintf(fp,
              "--------------- ---- ------------------ -------- --------- ---- -----------  ----------  ----------  ---------  --------  ---------\n");
        }
        if (type & MACHINE_TYPE_PART)
        {
          fprintf(fp, "  SERVER_IP     NUMS UCAP  / TCAP =  UR  BLKCNT FAMILYCNT  LOAD LAST_WRITE  LAST_READ  MAX_WRITE  MAX_READ STARTUP_TIME\n");
          fprintf(fp,
              "--------------- ---- ------------------ -------- ---- ----------  ---------  ---------  -------- ------------\n");
        }
      }

      if (print_type & BLOCK_DISTRIBUTION_TYPE)
      {
        if (type & BLOCK_IP_DISTRIBUTION_TYPE)
        {
          fprintf(fp, "%-10s %-10s %30s\n", "BLOCK_ID", "IP_COUNT", "DS_IP_PORT");
        }
        else// (type & BLOCK_RACK_DISTRIBUTION_TYPE)
        {
          fprintf(fp, "%-10s %-10s %40s\n", "BLOCK_ID", "RACK_COUNT", "DS_IP_PORT(DS_IP_GROUP)");
        }
      }

      if( (print_type & RACK_BLOCK_TYPE) && (type & RACK_BLOCK_TYPE_RACK_LIST))
      {
        fprintf(fp, "%-20s %-20s\n", "RACK_IP_GROUP", "COUNT");
      }
    }

    int ServerShow::calculate(ServerShow& old_server)
    {
      int32_t time = server_stat_.current_time_ - old_server.server_stat_.current_time_;
      add_tp(&server_stat_.total_tp_, &old_server.server_stat_.total_tp_, &server_stat_.last_tp_, SUB_OP);
      if (is_tp_valid(&server_stat_.last_tp_))
      {
        compute_tp(&server_stat_.last_tp_, time);//计算ssm对该ds两次拉取的时间段的单位流量
      }
      else// it will heppen when dataserver restart
      {
        memset(&server_stat_.last_tp_, 0, sizeof(Throughput));
      }
      time = server_stat_.current_time_ - server_stat_.startup_time_;
      compute_tp(&server_stat_.total_tp_, time);//total_tp_是从ds直接获取的累计字节数
      return TFS_SUCCESS;
    }
    void ServerShow::dump(const uint64_t server_id, const std::set<uint64_t>& blocks, FILE* fp) const
    {
      if (fp == NULL) { return; }

      fprintf(fp, "%17s  ", tbsys::CNetUtil::addrToString(server_id).c_str());
      fprintf(fp, "%-6Zd  ", blocks.size());
      std::set<uint64_t>::const_iterator iter = blocks.begin();
      int32_t count = 0;
      for (; iter != blocks.end(); iter++)
      {
        fprintf(fp, " %12"PRI64_PREFIX"u",(*iter));
        if (++count >= MAX_COUNT)
        {
          fprintf(fp, "\n%25s", " ");
          count = 0;
        }
      }
      fprintf(fp, "\n");
    }
    void ServerShow::dump(const int8_t type, FILE* fp) const
    {
      if (fp == NULL) { return; }
      if (type & SERVER_TYPE_SERVER_INFO)
      {
        fprintf(fp, "%17s %7s %7s %4d%% %7d %9zd %8u %6d %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %4s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %-18s\n",
            tbsys::CNetUtil::addrToString(server_stat_.id_).c_str(),
            Func::format_size(server_stat_.use_capacity_).c_str(),
            Func::format_size(server_stat_.total_capacity_).c_str(),
            server_stat_.total_capacity_ > 0 ? static_cast<int32_t> (server_stat_.use_capacity_ * 100 / server_stat_.total_capacity_) : 0,
            server_stat_.block_count_,
            family_set_.size(),
            server_stat_.rack_id_,
            server_stat_.current_load_,
            Func::format_size(server_stat_.total_tp_.write_byte_).c_str(),
            server_stat_.total_tp_.write_file_count_ / FILE_COUNT_PRECISION_ADJUST,
            Func::format_size(server_stat_.total_tp_.read_byte_).c_str(),
            server_stat_.total_tp_.read_file_count_ / FILE_COUNT_PRECISION_ADJUST,
            Func::format_size(server_stat_.last_tp_.write_byte_).c_str(),
            server_stat_.last_tp_.write_file_count_ / FILE_COUNT_PRECISION_ADJUST,
            Func::format_size(server_stat_.last_tp_.read_byte_).c_str(),
            server_stat_.last_tp_.read_file_count_ / FILE_COUNT_PRECISION_ADJUST,
            Func::time_to_str(server_stat_.startup_time_).c_str()
            );
      }
      if (type & SERVER_TYPE_BLOCK_LIST)
      {
        dump(server_stat_.id_, hold_, fp);
      }
      if (type & SERVER_TYPE_BLOCK_WRITABLE)
      {
        dump(server_stat_.id_, writable_, fp);
      }
      if (type & SERVER_TYPE_BLOCK_MASTER)
      {
        dump(server_stat_.id_, master_, fp);
      }
    }

//**********************************************************************
//**************************Block Info**********************************
    void BlockInfoShow::dump(int8_t type, FILE* fp) const
    {
      if (fp == NULL) { return; }
      if (type & BLOCK_TYPE_BLOCK_INFO)
      {
        fprintf(fp, "%-10"PRI64_PREFIX"d %-20"PRI64_PREFIX"u %-8d %-8d %-10d %-8d %-10d %-8d %-10d %-6Zd",
            info_.family_id_, info_.block_id_, info_.version_, info_.file_count_, info_.size_,
            info_.del_file_count_, info_.del_size_, info_.update_file_count_,
            info_.update_size_, server_list_.size());
      }
      if (type & BLOCK_TYPE_BLOCK_STATUS)
      {
        fprintf(fp, "%-10"PRI64_PREFIX"d %-20"PRI64_PREFIX"u %-8d %-12d %-9d %-14d %-12"PRI64_PREFIX"d %-12"PRI64_PREFIX"d",
            info_.family_id_, info_.block_id_, create_flag_, in_replicate_queue_, has_lease_,
            choose_master_, expire_time_, last_leave_time_);
      }
      if (type & BLOCK_TYPE_SERVER_LIST)
      {
        fprintf(fp, "%-10"PRI64_PREFIX"d  %-15"PRI64_PREFIX"u", info_.family_id_, info_.block_id_);
        std::stringstream server_str;
        std::vector<ServerInfo>::const_iterator iter = server_list_.begin();
        for (; iter != server_list_.end(); iter++)
        {
          server_str << "    " << tbsys::CNetUtil::addrToString((*iter).server_id_) << " " << (*iter).family_id_ << " " << (*iter).version_;
        }
        fprintf(fp, " %s", server_str.str().c_str());
      }
      fprintf(fp, "\n");
    }

//**********************************************************************
//**************************Check Block Info**********************************
    bool BlockCheckShow::check_consistency(DS_BLOCKS_MAP& all_ds_blocks_map)
    {
      bool is_consistent = true;
      std::vector<ServerInfo>::iterator iter = server_list_.begin();
      for (; iter != server_list_.end(); ++iter)
      {
        uint64_t server_id = iter->server_id_;
        std::set<uint64_t>& real_hold = all_ds_blocks_map[server_id];
        if (real_hold.find(info_.block_id_) == real_hold.end())
        {
          is_consistent = false;
          break;
        }
      }
      return is_consistent;
    }

    int BlockCheckShow::check_block_finally(const uint64_t ns_ip,  bool& is_consistent)
    {
      int ret = TFS_SUCCESS;
      is_consistent = true;
      VUINT64 ds_list;
      ret = ToolUtil::get_block_ds_list_v2(ns_ip, info_.block_id_, ds_list);
      if (TFS_SUCCESS == ret)
      {
        BlockInfoV2 block_info;
        for (uint32_t i = 0; i < ds_list.size(); ++i)
        {
          ret = ToolUtil::get_block_info(ds_list[i], info_.block_id_, block_info);
          if (EXIT_NO_LOGICBLOCK_ERROR == ret)
          {
            is_consistent = false;
            ns_more_.push_back(ds_list[i]);
            ret = TFS_SUCCESS;
          }
          else if (TFS_SUCCESS != ret)
          {
            fprintf(stderr, "get block info fail from ds, ret: %d\n", ret);
            break;
          }
        }
      }
      else
      {
        fprintf(stderr, "get block: %"PRI64_PREFIX"u ds_list from ns fail, ret: %d\n",
            info_.block_id_, ret);
      }
      return ret;
    }

    void BlockCheckShow::dump(FILE* fp) const
    {
      if (fp == NULL) { return; }
      fprintf(fp, "%"PRI64_PREFIX"u", info_.block_id_);
      string out = "";
      for (uint32_t i = 0; i < ns_more_.size(); ++i)
      {
        out += "  " + tbsys::CNetUtil::addrToString(ns_more_[i]);
      }
      fprintf(fp, "%s\n", out.c_str());
    }
//**********************************************************************
//**************************Block Unnormal Distribution Info**********************************

    BlockDistributionShow::~BlockDistributionShow()
    {
      std::map<uint32_t, std::vector<uint64_t>*>::iterator it;
      for(it = ip_servers_.begin(); it != ip_servers_.end(); it++)
      {
        tbsys::gDelete(it->second);
      }
      for(it = ip_rack_servers_.begin(); it != ip_rack_servers_.end(); it++)
      {
        tbsys::gDelete(it->second);
      }
    }

    //uint32_t ip_mask
    bool BlockDistributionShow::check_block_rack_distribution(const uint32_t ip_mask)
    {
      int8_t server_size = server_list_.size();
      std::vector<uint64_t>* ipport_list = NULL;
      for(int8_t index = 0; index < server_size; index++)
      {
        uint64_t server = server_list_[index].server_id_;
        assert(INVALID_SERVER_ID != server);
        uint32_t rack_ip = Func::get_lan(server, ip_mask);
        std::map<uint32_t, std::vector<uint64_t>* >::const_iterator it = ip_rack_servers_.find(rack_ip);
        if(it == ip_rack_servers_.end())
        {
          ipport_list = new std::vector<uint64_t>();
          ipport_list->push_back(server);
          ip_rack_servers_.insert(std::make_pair(rack_ip, ipport_list));
        }
        else
        {
          it->second->push_back(server);
          has_same_ip_rack_ = true;
        }
      }
      return has_same_ip_rack_;
    }

    void BlockDistributionShow::dump_rack(FILE* fp) const
    {
      if (fp == NULL) { return; }
      //fprintf(fp, " %-10"PRI64_PREFIX"u    %Zd    ", info_.block_id_, ip_rack_servers_.size());
      std::string server_str = "";
      std::map<uint32_t, std::vector<uint64_t>* >::const_iterator iter = ip_rack_servers_.begin();
      int32_t count = 0;
      for (; iter != ip_rack_servers_.end(); iter++)
      {
        std::vector<uint64_t>::const_iterator iter_ipport = iter->second->begin();
        if(iter->second->size() > 1)
        {
          for(; iter_ipport != iter->second->end(); iter_ipport++)
          {
            server_str += ("    " + static_cast<std::string> (tbsys::CNetUtil::addrToString(*iter_ipport).c_str()) + "(" +
                static_cast<std::string> (Func::addr_to_str(iter->first, false))+ ")");
          }
          count++;
        }
      }
      assert("" != server_str);
      fprintf(fp, " %-10"PRI64_PREFIX"u   %d(%Zd)    ", info_.block_id_,count, ip_rack_servers_.size());
      fprintf(fp, " %s\n", server_str.c_str());
    }


    RackBlockShow::~RackBlockShow()
    {
      std::map<uint32_t, std::vector<uint64_t> *>::iterator it = rack_blocks_.begin();
      for(; it != rack_blocks_.end(); it++)
      {
        tbsys::gDelete(it->second);
      }
    }

    void RackBlockShow::add(BlockInfoShow &block, const uint32_t ip_mask)
    {
      int8_t server_size = block.server_list_.size();
      for(int index = 0; index < server_size; index++)
      {
        uint64_t server = block.server_list_[index].server_id_;
        assert(INVALID_SERVER_ID != server);
        uint32_t server_ip_group =  Func::get_lan(server, ip_mask);//在前面解析命令时就过滤输入不正确的ip格式
        std::map<uint32_t, std::vector<uint64_t> *>::iterator iter =  rack_blocks_.find(server_ip_group);
        if(iter == rack_blocks_.end())
        {
          std::vector<uint64_t>* block_list = new std::vector<uint64_t>();
          block_list->push_back(block.info_.block_id_);
          rack_blocks_.insert(make_pair(server_ip_group, block_list));
        }
        else
        {
          iter->second->push_back(block.info_.block_id_);
        }
      }
      total_block_replicate_count_ += server_size;
    }

    void RackBlockShow::dump(const int8_t type, const uint32_t ip_group, FILE* fp) const
    {
      if (fp == NULL) { return; }
      std::map<uint32_t, std::vector<uint64_t>*>::const_iterator iter = rack_blocks_.begin();
      if (type & RACK_BLOCK_TYPE_RACK_LIST)
      {
        for(; iter != rack_blocks_.end(); iter++)
        {
          unsigned char *bytes = (unsigned char *) &(iter->first);
          fprintf(fp, "%d.%d.%d.%d  %10Zd\n", bytes[0], bytes[1], bytes[2], bytes[3], iter->second->size());//大小端
        }
        fprintf(fp, "rack count: %Zd, total block(replicate) count: %"PRI64_PREFIX"u\n", rack_blocks_.size(), total_block_replicate_count_);
      }
      else
      {
        //必须是ip_group的string 否则找不到对应的block list
        std::string server_str = "";
        uint64_t block_count = 0;
        for(; iter != rack_blocks_.end(); iter++)
        {
          if(iter->first == ip_group)
          {
            std::vector<uint64_t>* block_list = iter->second;
            block_count = block_list->size();
            char tmp[255];
            for(uint64_t index = 0; index < block_count; index++)
            {
              sprintf(tmp, "%-10"PRI64_PREFIX"u", block_list->at(index));
              server_str += static_cast<std::string> (tmp) + " ";
              if((index+1) % 15 == 0)
              {
                server_str += "\n";
              }
            }
          }
        }
        if(!server_str.empty())
        {
          fprintf(fp, "%s\n", server_str.c_str());
        }
        fprintf(fp, "the total block replicates count is %"PRI64_PREFIX"u in rack %s\n",
            block_count, tbsys::CNetUtil::addrToString(ip_group).c_str());//直接原样输出用户输入的ip_group
      }
      fprintf(fp, "\n");
    }


    BlockDistributionStruct::BlockDistributionStruct()
        : total_block_count_(0), ip_same_block_count_(0), ip_rack_same_block_count_(0)
    {}

    BlockDistributionStruct::~BlockDistributionStruct()
    {}

    void BlockDistributionStruct::add(BlockDistributionShow& block)
    {
      total_block_count_++;
      if(block.has_same_ip_)
      {
        ip_same_block_count_++;
      }
      if(block.has_same_ip_rack_)
      {
        ip_rack_same_block_count_++;
      }
    }

    void BlockDistributionStruct::dump(const int8_t type, FILE* fp) const
    {
      if (fp == NULL) { return; }

      if (type & BLOCK_IP_DISTRIBUTION_TYPE)
      {
        fprintf(fp, "Unnormal blocks count: %d\n", ip_same_block_count_);
      }

      if (type & BLOCK_RACK_DISTRIBUTION_TYPE)
      {
        fprintf(fp, "Unmormal blocks count: %d\n", ip_rack_same_block_count_);
      }
      fprintf(fp, "\n");
    }

    //**********************************************************************
    //**************************Machine Info**********************************
    MachineShow::MachineShow() :
      machine_id_(0), use_capacity_(0), total_capacity_(0), current_load_(0), rack_id_(0), block_count_(0),
      last_startup_time_(0), consume_time_(0), index_(0)
    {
      memset(&total_tp_, 0, sizeof(total_tp_));
      memset(&last_tp_, 0, sizeof(last_tp_));
      memset(&max_tp_, 0, sizeof(max_tp_));
    }

    int MachineShow::init(ServerShow& server, ServerShow& old_server)
    {
      int32_t time = server.server_stat_.current_time_ - old_server.server_stat_.current_time_;
      Throughput tmp_tp_;
      add_tp(&server.server_stat_.total_tp_, &old_server.server_stat_.total_tp_, &tmp_tp_, SUB_OP);
      compute_tp(&tmp_tp_, time);
      memcpy(&max_tp_, &tmp_tp_, sizeof(max_tp_));
      rack_id_ = server.server_stat_.rack_id_;
      last_startup_time_ = server.server_stat_.startup_time_;
      return TFS_SUCCESS;
    }

    int MachineShow::add(ServerShow& server, ServerShow& old_server, const int8_t sub_type)
    {
      if ((sub_type & MACHINE_TYPE_ALL)
          || (DATASERVER_DISK_TYPE_FULL == server.server_stat_.disk_type_))
      {
        use_capacity_ += server.server_stat_.use_capacity_;
        total_capacity_ += server.server_stat_.total_capacity_;
        block_count_ += server.server_stat_.block_count_;
        current_load_ += server.server_stat_.current_load_;
      }

      int32_t time = server.server_stat_.current_time_ - old_server.server_stat_.current_time_;
      Throughput tmp_tp_;
      add_tp(&server.server_stat_.total_tp_, &old_server.server_stat_.total_tp_, &tmp_tp_, SUB_OP);
      // if dataserver restart, tmp_tp_ will invalid
      if (is_tp_valid(&tmp_tp_))
      {
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
      }

      if (last_startup_time_ < server.server_stat_.startup_time_)
      {
        last_startup_time_ = server.server_stat_.startup_time_;
      }

      //just add all value(div time first)
      time = server.server_stat_.current_time_ - server.server_stat_.startup_time_;
      compute_tp(&server.server_stat_.total_tp_, time);
      add_tp(&total_tp_, &server.server_stat_.total_tp_, &total_tp_, ADD_OP);
      // add up family id
      family_set_.insert(server.family_set_.begin(), server.family_set_.end());
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
        fprintf(fp, "  %-12s  %4d %5s %7s  %2d%%  %4d %6zd  %8u  %5d %6s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %4s %5"PRI64_PREFIX"d %3s %5"PRI64_PREFIX"d %3s %5"PRI64_PREFIX"d\n",
            tbsys::CNetUtil::addrToString(machine_id_).c_str(),
            index_,
            Func::format_size(use_capacity_).c_str(),
            Func::format_size(total_capacity_).c_str(),
            total_capacity_ > 0 ? static_cast<int32_t> (use_capacity_ * 100 / total_capacity_) : 0,
            block_count_,
            family_set_.size(),
            rack_id_,
            index_ > 0 ? (current_load_ / index_) : current_load_,
            Func::format_size(total_tp_.write_byte_).c_str(),
            total_tp_.write_file_count_ / FILE_COUNT_PRECISION_ADJUST,
            Func::format_size(total_tp_.read_byte_).c_str(),
            total_tp_.read_file_count_ / FILE_COUNT_PRECISION_ADJUST,
            Func::format_size(last_tp_.write_byte_).c_str(),
            last_tp_.write_file_count_ / FILE_COUNT_PRECISION_ADJUST,
            Func::format_size(last_tp_.read_byte_).c_str(),
            last_tp_.read_file_count_ / FILE_COUNT_PRECISION_ADJUST,
            Func::format_size(max_tp_.write_byte_).c_str(),
            max_tp_.write_file_count_ / FILE_COUNT_PRECISION_ADJUST,
            Func::format_size(max_tp_.read_byte_).c_str(),
            max_tp_.read_file_count_ / FILE_COUNT_PRECISION_ADJUST
            );
      }
      else if (flag & MACHINE_TYPE_PART)
      {
        fprintf(fp, "  %-12s  %4d %6s %7s  %2d%%  %6d  %6zd  %5u %5d  %5s %4"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %4s %5"PRI64_PREFIX"d %-19s\n",
            tbsys::CNetUtil::addrToString(machine_id_).c_str(),
            index_,
            Func::format_size(use_capacity_).c_str(),
            Func::format_size(total_capacity_).c_str(),
            total_capacity_ > 0 ? static_cast<int32_t> (use_capacity_ * 100 / total_capacity_) : 0,
            block_count_,
            family_set_.size(),
            rack_id_,
            index_ > 0 ? current_load_ / index_ : current_load_,
            Func::format_size(last_tp_.write_byte_).c_str(),
            last_tp_.write_file_count_ / FILE_COUNT_PRECISION_ADJUST,
            Func::format_size(last_tp_.read_byte_).c_str(),
            last_tp_.read_file_count_ / FILE_COUNT_PRECISION_ADJUST,
            Func::format_size(max_tp_.write_byte_).c_str(),
            max_tp_.write_file_count_ / FILE_COUNT_PRECISION_ADJUST,
            Func::format_size(max_tp_.read_byte_).c_str(),
            max_tp_.read_file_count_ / FILE_COUNT_PRECISION_ADJUST,
            Func::time_to_str(last_startup_time_).c_str()
            );
      }
    }


    FamilyShow::FamilyShow()
    {
      family_id_ = INVALID_FAMILY_ID;
      family_aid_info_ = 0;
    }

    FamilyShow::~FamilyShow()
    {
    }

    int32_t FamilyShow::deserialize(tbnet::DataBuffer& input, const int32_t length, int32_t& offset)
    {
      if (input.getDataLen() <= 0 || offset >= length)
      {
        return TFS_ERROR;
      }
      int32_t len = input.getDataLen();
      family_id_ = input.readInt64();
      family_aid_info_ = input.readInt32();
      const int32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info_) +  GET_CHECK_MEMBER_NUM(family_aid_info_);
      for (int32_t i = 0; i < MEMBER_NUM ; ++i)
      {
        members_[i].first = input.readInt64();//block_id
        input.readInt32();//version
      }
      offset += (len - input.getDataLen());
      return TFS_SUCCESS;
    }

    void FamilyShow::get_members_ds_list(const uint64_t ns_ip)
    {
      int32_t data_member_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_member_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_member_num + check_member_num;
      for(int32_t index = 0; index < member_num; index++)
      {
        uint64_t block_id = members_[index].first;
        common::VUINT64 ds_list;
        int ret = ToolUtil::get_block_ds_list_v2(ns_ip, block_id, ds_list);
        if (TFS_SUCCESS != ret || ds_list.empty())
        {
          fprintf(stderr, "get block: %"PRI64_PREFIX"u ds_list fail, ret: %d\n", block_id, ret);
          members_[index].second = INVALID_SERVER_ID; // fail show 0 for ds_id
        }
        else
        {
          members_[index].second = ds_list[0];
        }
      }
    }

    void FamilyShow::dump(const int8_t type, FILE* fp) const
    {
      int32_t data_member_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_member_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_member_num + check_member_num;
      fprintf(fp, "%-10"PRI64_PREFIX"d %5d %6d %8s", family_id_, data_member_num, check_member_num, "");
      std::ostringstream member_str;
      for(int32_t index = 0; index < member_num; index++)
      {
        member_str << "   " << members_[index].first;
        if(type & BLOCK_TYPE_SERVER_LIST)
        {
          member_str << "/" << tbsys::CNetUtil::addrToString(members_[index].second).c_str();
        }
      }
      fprintf(fp, "%s\n", member_str.str().c_str());
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
      use_capacity_ += server.server_stat_.use_capacity_;
      total_capacity_ += server.server_stat_.total_capacity_;
      current_load_ += server.server_stat_.current_load_;
      block_count_ += server.server_stat_.block_count_;

      add_tp(&total_tp_, &server.server_stat_.total_tp_, &total_tp_, ADD_OP);
      add_tp(&last_tp_, &server.server_stat_.last_tp_, &last_tp_, ADD_OP);

      family_set_.insert(server.family_set_.begin(), server.family_set_.end());
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

      family_set_.insert(machine.family_set_.begin(), machine.family_set_.end());
      return TFS_SUCCESS;
    }
    int StatStruct::add(BlockInfoShow& block)
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
          fprintf(fp, "TOTAL: %5d %5s %7s %7s %4d%% %7d %9zd %8s %6d %6s %5"PRI64_PREFIX"d %6s %5"PRI64_PREFIX"d %4s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d\n\n",
              server_count_,
              "",
              Func::format_size(use_capacity_).c_str(),
              Func::format_size(total_capacity_).c_str(),
              total_capacity_ > 0 ? static_cast<int32_t> (use_capacity_ * 100 / total_capacity_):0,
              block_count_,
              family_set_.size(),
              "-",
              server_count_ > 0 ? static_cast<int32_t> (current_load_/server_count_) : 0,
              Func::format_size(total_tp_.write_byte_).c_str(),
              total_tp_.write_file_count_ / FILE_COUNT_PRECISION_ADJUST,
              Func::format_size(total_tp_.read_byte_).c_str(),
              total_tp_.read_file_count_ / FILE_COUNT_PRECISION_ADJUST,
              Func::format_size(last_tp_.write_byte_).c_str(),
              last_tp_.write_file_count_ / FILE_COUNT_PRECISION_ADJUST,
              Func::format_size(last_tp_.read_byte_).c_str(),
              last_tp_.read_file_count_ / FILE_COUNT_PRECISION_ADJUST
              );
        }
      }
      if (type & BLOCK_TYPE)
      {
        if (sub_type & BLOCK_TYPE_BLOCK_INFO)
        {
          fprintf(fp, "TOTAL: %-2d  %5s  FILE COUNT/SIZE: %"PRI64_PREFIX"d/%-12s  DEL FILE COUNT/SIZE: %"PRI64_PREFIX"d/%-12s  PER_FILE(%s)\n\n",
              block_count_,
              "",//调整显示格式用
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
          fprintf(fp, "Total : %-10d %2d %3s %7s  %2d%%  %4d %6zd  %5s %5d %6s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d %4s %5"PRI64_PREFIX"d\n\n",
              machine_count_,
              server_count_,
              Func::format_size(use_capacity_).c_str(),
              Func::format_size(total_capacity_).c_str(),
              total_capacity_ > 0 ? static_cast<int32_t> (use_capacity_ * 100 / total_capacity_) : 0,
              block_count_,
              family_set_.size(),
              "-",
              server_count_ > 0 ? static_cast<int32_t> (current_load_/server_count_) : 0,
              Func::format_size(total_tp_.write_byte_).c_str(),
              total_tp_.write_file_count_ / FILE_COUNT_PRECISION_ADJUST,
              Func::format_size(total_tp_.read_byte_).c_str(),
              total_tp_.read_file_count_ / FILE_COUNT_PRECISION_ADJUST,
              Func::format_size(last_tp_.write_byte_).c_str(),
              last_tp_.write_file_count_ / FILE_COUNT_PRECISION_ADJUST,
              Func::format_size(last_tp_.read_byte_).c_str(),
              last_tp_.read_file_count_ / FILE_COUNT_PRECISION_ADJUST
              );
        }
        if (sub_type & MACHINE_TYPE_PART)
        {
          fprintf(fp, "Total : %-10d %2d %2s %7s  %2d%%  %4d %6zd  %5d %6s %5"PRI64_PREFIX"d %5s %5"PRI64_PREFIX"d\n\n",
              machine_count_,
              server_count_,
              Func::format_size(use_capacity_).c_str(),
              Func::format_size(total_capacity_).c_str(),
              total_capacity_ > 0 ? static_cast<int32_t> (use_capacity_ * 100 / total_capacity_) : 0,
              block_count_,
              family_set_.size(),
              server_count_ > 0 ? static_cast<int32_t> (current_load_/server_count_) : 0,
              Func::format_size(last_tp_.write_byte_).c_str(),
              last_tp_.write_file_count_ / FILE_COUNT_PRECISION_ADJUST,
              Func::format_size(last_tp_.read_byte_).c_str(),
              last_tp_.read_file_count_ / FILE_COUNT_PRECISION_ADJUST
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
              last_tp_.write_file_count_ / FILE_COUNT_PRECISION_ADJUST,
              Func::format_size(last_tp_.read_byte_, 'M').c_str(),
              last_tp_.read_file_count_ / FILE_COUNT_PRECISION_ADJUST
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
