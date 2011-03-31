#ifndef TFS_TOOLS_SHOW_H_
#define TFS_TOOLS_SHOW_H_

#include <stdio.h>
#include <vector>
#include "common.h"

namespace tfs
{
  namespace tools
  {
    class ShowInfo
    {
      public:
        ShowInfo();
        virtual ~ShowInfo();
    
        int set_ns_ip(std::string ns_ip_port);
        int show_server(int8_t flag, int32_t num);
        int show_machine(int8_t flag, int32_t num);
        int show_block(int8_t flag, int32_t num, uint32_t block_id);
    
      private:
        void load_last_ds();
        void save_last_ds();
        uint64_t get_machine_id(uint64_t server_id);
        void print_header(int8_t print_type, int32_t flag);
        uint64_t get_addr(std::string ns_ip_port);
        map<uint64_t, ServerStruct> last_server_map_;
        map<uint64_t, ServerStruct> server_map_;
        map<uint64_t, BlockStruct> block_map_;
        map<uint64_t, MachineStruct> machine_map_;
        uint64_t ns_ip_;
    };
  }
}

#endif

