#ifndef   TFS_RCSERVER_TOOLS_H_
#define   TFS_RCSERVER_TOOLS_H_

#include <vector>
#include <map>

namespace tfs
{
  namespace tools
  {
    using namespace std;
    using namespace common;
    using namespace message;

    class RcStat
    {
      public:
        RcStat();
        ~RcStat();
        int get_rcs_stat();
        int initialize(int argc, char **argv);

      private:
        static void split_string_to_vector(const std::string& str, const std::string& pattern, std::vector<std::string>& vec);
        void parse_rcs_stat(const RspRcStatMessage *rsp_rcstat_msg);
        static inline std::string& trim_space(std::string &s);
        int show_rcs_stat();

        std::vector<string> rc_ips_vec_;
        int32_t   app_id_;
        int32_t   oper_type_;
        int32_t   order_by_;
        bool      is_json_;
        std::multimap<int64_t, AppOperInfo> appoper_result_map_;
    };
  }
}

#endif
