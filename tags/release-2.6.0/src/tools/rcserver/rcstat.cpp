#include <time.h>
#include "common/client_manager.h"
#include "common/new_client.h"
#include "common/func.h"
#include "common/cdefine.h"
#include "message/message_factory.h"
#include "message/rc_session_message.h"
#include "rcstat.h"

namespace tfs
{
  namespace tools
  {
    using namespace std;
    static tfs::message::MessageFactory gfactory;
    static tfs::common::BasePacketStreamer gstreamer;

    RcStat::RcStat()
      :app_id(0), oper_type(0), interval(0)
    {
      gstreamer.set_packet_factory(&gfactory);
      NewClientManager::get_instance().initialize(&gfactory, &gstreamer);
    }

    RcStat::~RcStat()
    {
    }

    int RcStat::initialize(int argc, char **argv)
    {
      int32_t i = 0;

      if (argc < 2)
      {
        fprintf(stderr, "Usage: %s -r 127.0.0.1:6202 -a 0 -t 2\n", argv[0]);
        exit(1);
      }

      while ((i = getopt(argc, argv,"r:a:t:i:")) != EOF)
      {
        switch (i)
        {
          /* rc server address */
          case 'r':
            str_rc_ip = optarg;
            break;
            /* app_id */
          case 'a':
            app_id = atoi(optarg);
            break;
            /* interval */
          case 'i':
            interval = atoi(optarg);
            break;
            /* type  */
          case 't':
            oper_type = atoi(optarg);
            break;
          default:
            fprintf(stderr, "Usage: %s -a 127.0.0.1:6202 -a 0\n", argv[0]);
            return EXIT_SUCCESS;
        }

        if (str_rc_ip.empty())
        {
            fprintf(stderr, "Usage: %s -a 127.0.0.1:6202\n", argv[0]);
        }
      }

      return TFS_SUCCESS;
    }

    int RcStat::parse_rcs_stat(RspRcStatMessage *rsp_rcstat_msg)
    {
      int ret = TFS_SUCCESS;

      const common::AppOperInfoMap& app_oper_info_map_ = rsp_rcstat_msg->get_stat_info();

      AppOperInfoMapConstIter mit = app_oper_info_map_.begin();
      for (; mit != app_oper_info_map_.end(); ++mit)
      {
        fprintf(stdout, "%d,%d,%ld,%ld,%ld,%ld\n", mit->second.oper_app_id_, mit->second.oper_type_, \
            mit->second.oper_times_,mit->second.oper_size_, mit->second.oper_rt_, mit->second.oper_succ_);
      }

      return ret;
    }

    int RcStat::get_rcs_stat()
    {
      int ret = TFS_SUCCESS;

      ReqRcStatMessage req_rcstat_msg;
      uint64_t server_id = Func::get_host_ip(str_rc_ip.c_str());
      req_rcstat_msg.set_app_id(app_id);
      req_rcstat_msg.set_oper_type(oper_type);
      req_rcstat_msg.set_interval(interval);

      NewClient* client = NewClientManager::get_instance().create_client();
      tbnet::Packet *ret_msg = NULL;

      if ((ret = send_msg_to_server(server_id, client, &req_rcstat_msg, ret_msg)) == TFS_SUCCESS)
      {
        if (ret_msg->getPCode() != RSP_RC_REQ_STAT_MESSAGE)
        {
          TBSYS_LOG(ERROR, "cant't get response message from rcserver.");
          ret = TFS_ERROR;
        }
        else
        {
          RspRcStatMessage *rsp_rcstat_msg = dynamic_cast<RspRcStatMessage*>(ret_msg);
          parse_rcs_stat(rsp_rcstat_msg);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "ERROR:send_msg_to_server error. ret: %d.\n", ret);
      }
      return ret;
    }
  }
}

int main(int argc, char **argv)
{
  tfs::tools::RcStat rctools;
  rctools.initialize(argc, argv);
  rctools.get_rcs_stat();

  return 0;
}
