#ifndef   TFS_RCSERVER_TOOLS_H_
#define   TFS_RCSERVER_TOOLS_H_

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

      public:
        int get_rcs_stat();
        int initialize(int argc, char **argv);

      private:
        int parse_rcs_stat(RspRcStatMessage *rsp_rcstat_msg);
        string    str_rc_ip;
        int32_t   app_id;
        int32_t   oper_type;
        int64_t   interval;
    };
  }
}

#endif
