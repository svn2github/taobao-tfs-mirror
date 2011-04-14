#ifndef TFS_TOOLS_COMMON_H_
#define TFS_TOOLS_COMMON_H_

#include <vector>
#include <tbnet.h>
#include <Handle.h>
#include "common/config.h"
#include "message/client.h"
#include "message/client_pool.h"
#include "message/message.h"
#include "message/message_factory.h"
#include "common/config_item.h"

namespace tfs
{
  namespace tools
  {
    static const int32_t MAX_READ_NUM = 0xA0000;
    
    enum SubCMD
    {
      CMD_NOP = 0,
      CMD_UNKNOWN,
      CMD_NUM = 2,
      CMD_BLOCK_ID,
      CMD_BLOCK_LIST = 4,
      CMD_BLOCK_WRITABLE,
      CMD_BLOCK_MASTER,
      CMD_SERVER,
      CMD_ALL,
      CMD_PART
    };
    enum PrintType
    {
      PRINT_SERVER,
      PRINT_MACHINE,
      PRINT_BLOCK
    };
    enum TpMode
    {
      SUB_OP = -1,
      ADD_OP = 1
    };
    
    enum MachineFlag
    {
      PRINT_ALL = 1,
      PRINT_PART = 2
    };
    
    struct ServerStruct
    {
      uint64_t id_;
      int64_t use_capacity_;
      int64_t total_capacity_;
      tfs::common::Throughput total_tp_;
      tfs::common::Throughput last_tp_;
      int32_t current_load_;
      int32_t block_count_;
      time_t last_update_time_;
      time_t startup_time_;
      time_t current_time_;
      tfs::common::DataServerLiveStatus status_;
      std::vector<uint32_t> hold_;
      std::vector<uint32_t> writable_;
      std::vector<uint32_t> master_;
    
      ServerStruct();
      virtual ~ServerStruct();
      int serialize(tbnet::DataBuffer& output, int32_t& length);
      int deserialize(tbnet::DataBuffer& input, int32_t& length);
      int deserialize(tbnet::DataBuffer& input, const int32_t length, int32_t& offset, int32_t flag);
      int calculate(ServerStruct& old_server);
      bool cmp(int8_t type, ServerStruct& a);
      void dump(int32_t flag);
      void dump();
    };
    
    struct BlockStruct
    {
      tfs::common::BlockInfo info_;
      struct ServerInfo
      {
        uint64_t server_id_;
      };
      vector<ServerInfo> server_list_;
    
      BlockStruct();
      virtual ~BlockStruct();
      int32_t deserialize(tbnet::DataBuffer& input, const int32_t length, int32_t& offset, int8_t flag);
      void dump(int32_t flag);
      void dump();
    };
    struct MachineStruct
    {
      uint64_t machine_id_;
      int64_t use_capacity_;
      int64_t total_capacity_;
      tfs::common::Throughput total_tp_;
      tfs::common::Throughput last_tp_;
      tfs::common::Throughput max_tp_;
      int32_t current_load_;
      int32_t block_count_;
      time_t last_startup_time_;
      time_t consume_time_;
      int32_t index_;
      MachineStruct();
      virtual ~MachineStruct();
      int init(ServerStruct& server, ServerStruct& old_server);
      int add(ServerStruct& server, ServerStruct& old_server);
      int calculate();
      void dump(int8_t flag);
    };
    struct StatStruct
    {
      int32_t server_count_;
      int32_t machine_count_;
      int64_t use_capacity_;
      int64_t total_capacity_;
      tfs::common::Throughput total_tp_;
      tfs::common::Throughput last_tp_;
      int32_t current_load_;
      int32_t block_count_;
      time_t last_update_time_;
      int64_t file_count_;
      int32_t block_size_;
      int64_t delfile_count_;
      int32_t block_del_size_;
      StatStruct();
      virtual ~StatStruct();
      int add(ServerStruct& server);
      int add(MachineStruct& machine);
      int add(BlockStruct& block);
      void dump(int8_t flag, int8_t sub_flag);
    };
    static void compute_tp(tfs::common::Throughput* tp, int32_t time);
    static void add_tp(tfs::common::Throughput* atp, tfs::common::Throughput* btp, tfs::common::Throughput* ,int32_t sign);
  }
}

#endif
