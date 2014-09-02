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
  namespace
  {
    void fill_nodata_col(bool* have_done_oper)
    {
      for (int i=0; i < 3; i++)
      {
        if (!have_done_oper[i])
        {
          switch (i+1)
          {
            case 1:
              fprintf(stdout, ",\"read_times\":0, \"read_bytes\":0, \"read_rt\":0");
              have_done_oper[0] = true;
              break;
            case 2:
              fprintf(stdout, ",\"write_times\":0, \"write_bytes\":0, \"write_rt\":0");
              have_done_oper[1] = true;
              break;
            case 3:
              fprintf(stdout, ",\"rm_times\":0, \"rm_bytes\":0, \"rm_rt\":0");
              have_done_oper[2] = true;
              break;
            default:
              break;
          }
        }
      }
    }
  }
  namespace tools
  {
    static int32_t server_update_interval = 10;
    using namespace std;
    static tfs::message::MessageFactory gfactory;
    static tfs::common::BasePacketStreamer gstreamer;

    RcStat::RcStat()
      :app_id_(0), oper_type_(0), order_by_(0), is_json_(false)
    {
      gstreamer.set_packet_factory(&gfactory);
      NewClientManager::get_instance().initialize(&gfactory, &gstreamer);
    }

    RcStat::~RcStat()
    {
    }

    inline std::string& RcStat::trim_space(std::string &s)
    {
      s.erase(s.begin(), std::find_if(s.begin(), s.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
      s.erase(std::find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), s.end());

      return s;
    }

    void RcStat::split_string_to_vector(const std::string& vec_str, const std::string& pattern, std::vector<std::string>& vec)
    {
      if (vec_str.size() == 0 || pattern.size() == 0)
      {
        return;
      }
      std::string::size_type pos, size, pattern_size;
      std::string tmp_str = vec_str + pattern;

      size = tmp_str.size();
      pattern_size = pattern.size();

      for(std::string::size_type i = 0; i < size; i++)
      {
        pos = tmp_str.find(pattern, i);
        if (pos < size)
        {
          std::string s = tmp_str.substr(i, pos-i);
          vec.push_back(trim_space(s));

          i = pos + pattern_size -1 ;
        }
      }
    }


    int RcStat::initialize(int argc, char **argv)
    {
      int32_t i = 0;

      if (argc < 2)
      {
        fprintf(stderr, "Usage: %s -r 127.0.0.1:6202 -a 0 -t 2\n", argv[0]);
        exit(1);
      }

      string str_rc_ips;

      while ((i = getopt(argc, argv,"r:a:t:o:ij")) != EOF)
      {
        switch (i)
        {
          /* rc server address */
          case 'r':
            str_rc_ips = optarg;
            break;
            /* app_id */
          case 'a':
            app_id_ = atoi(optarg);
            break;
          case 'o':
            order_by_ = atoi(optarg);
            break;
          case 'i':
            server_update_interval = atoi(optarg);
            if (server_update_interval <= 0)
            {
              server_update_interval = 10;
            }
            break;
            /* type  */
          case 't':
            oper_type_ = atoi(optarg);
            break;
          case 'j':
            is_json_ = true;
            break;
          default:
            fprintf(stderr, "Usage: %s -a 127.0.0.1:6202 -a 0\n", argv[0]);
            return EXIT_SUCCESS;
        }
      }

      if (str_rc_ips.empty())
      {
        fprintf(stderr, "Usage: %s -a 127.0.0.1:6202\n", argv[0]);
      }
      else
      {
        split_string_to_vector(str_rc_ips, ",",rc_ips_vec_);
      }

      return TFS_SUCCESS;
    }

    //int RcStat::parse_rcs_stat(const std::vector<RspRcStatMessage *> rsp_rcstat_msg_vec)
    void RcStat::parse_rcs_stat(const RspRcStatMessage* rsp_rcstat_msg)
    {
      int64_t key;

      const common::AppOperInfoMap& app_oper_info_map = rsp_rcstat_msg->get_stat_info();
      if (app_oper_info_map.size() > 0)
      {
        AppOperInfoMapConstIter mit = app_oper_info_map.begin();
        for (; mit != app_oper_info_map.end(); ++mit)
        {
          key = mit->second.oper_app_id_ << 16 | mit->second.oper_type_;

          std::map<int64_t, AppOperInfo>::iterator it = appoper_result_map_.find(key);
          if (it == appoper_result_map_.end())
          {
            appoper_result_map_.insert(make_pair(key, mit->second));
          }
          else
          {
            AppOperInfo& app_oper_info = it->second;
            if(app_oper_info.oper_app_id_ == mit->second.oper_app_id_ && app_oper_info.oper_type_ == mit->second.oper_type_)
            {
              app_oper_info.oper_times_ += mit->second.oper_times_;
              app_oper_info.oper_size_ += mit->second.oper_size_;
              app_oper_info.oper_rt_ += mit->second.oper_rt_;
              app_oper_info.oper_succ_ += mit->second.oper_succ_;
            }
            else
            {
              TBSYS_LOG(ERROR, "rcserver data inconsistent.");
            }
          }
        }
      }
    }

    int RcStat::show_rcs_stat()
    {
      int32_t ret = TFS_SUCCESS;
      std::multimap<int64_t, AppOperInfo> order_appoper_result_map;
      std::multimap<int64_t, AppOperInfo>::iterator it = appoper_result_map_.begin();
      int64_t count_per_sec, succ_count_per_sec;
      if (is_json_)
      {
        order_by_ = 0; //use json format, order is no matter
      }
      for (; it != appoper_result_map_.end(); ++it)
      {
        count_per_sec = it->second.oper_times_ /server_update_interval + 1;
        succ_count_per_sec = it->second.oper_succ_ /server_update_interval + 1;
        it->second.oper_size_ = it->second.oper_size_ / it->second.oper_times_;
        it->second.oper_rt_= it->second.oper_rt_ / it->second.oper_times_;
        it->second.oper_times_ = count_per_sec;
        it->second.oper_succ_ = succ_count_per_sec;

        if (order_by_ == 1)
        {
          order_appoper_result_map.insert(make_pair(it->second.oper_times_, it->second));
        }
        else if (order_by_ == 2)
        {
          order_appoper_result_map.insert(make_pair(it->second.oper_size_, it->second));
        }
        else if (order_by_ == 3)
        {
          order_appoper_result_map.insert(make_pair(it->second.oper_rt_, it->second));
        }
      }

      std::multimap<int64_t, AppOperInfo> *tmp_appoper_result_map = NULL;
      if (order_by_ == 1 || order_by_ == 2 || order_by_ == 3)
      {
        tmp_appoper_result_map = &order_appoper_result_map;
      }
      else
      {
        tmp_appoper_result_map = &appoper_result_map_;
      }

      it = tmp_appoper_result_map->begin();
      if (is_json_)
      {
        int last_app_id = -1;
        bool have_done_oper[3];
        have_done_oper[0] = have_done_oper[1] = have_done_oper[2] = true;
        fprintf(stdout, "{\n");
        for (; it != tmp_appoper_result_map->end(); ++it)
        {
          if (it->second.oper_app_id_ <= 0 ) continue;
          bool first_line_in_app = last_app_id != it->second.oper_app_id_;
          if (first_line_in_app)
          {
            if (last_app_id != -1)
            {
              fill_nodata_col(have_done_oper);
              fprintf(stdout, "\n},\n");
              fprintf(stdout, "{\n");
            }
            last_app_id = it->second.oper_app_id_;
            fprintf(stdout, "\"app_id\":\"%d\"", last_app_id);
            have_done_oper[0] = have_done_oper[1] = have_done_oper[2] = false;
          }

          switch (it->second.oper_type_)
          {
            case 1:
              fprintf(stdout, ",\"read_times\":%ld, \"read_bytes\":%ld, \"read_rt\":%ld",
                  it->second.oper_times_, it->second.oper_size_, it->second.oper_rt_);
              have_done_oper[0] = true;
              break;
            case 2:
              fprintf(stdout, ",\"write_times\":%ld, \"write_bytes\":%ld, \"write_rt\":%ld",
                  it->second.oper_times_, it->second.oper_size_, it->second.oper_rt_);
              have_done_oper[1] = true;
              break;
            case 3:
              fprintf(stdout, ",\"rm_times\":%ld, \"rm_bytes\":%ld, \"rm_rt\":%ld",
                  it->second.oper_times_, it->second.oper_size_, it->second.oper_rt_);
              have_done_oper[2] = true;
              break;
            default:
              break;
          }
        }
        fill_nodata_col(have_done_oper);
        fprintf(stdout, "\n}\n");
      }
      else
      {
        for (; it != tmp_appoper_result_map->end(); ++it)
        {
          fprintf(stdout, "%d,%d,%ld,%ld,%ld,%ld\n", it->second.oper_app_id_, it->second.oper_type_, \
              it->second.oper_times_, it->second.oper_size_, it->second.oper_rt_, succ_count_per_sec);
        }
      }

      return ret;
    }

    int RcStat::get_rcs_stat()
    {
      int ret = TFS_SUCCESS;
      uint64_t server_id = 0;

      std::vector<string>::const_iterator iter = rc_ips_vec_.begin();
      for (; iter != rc_ips_vec_.end(); ++iter)
      {
        string rc_ip = *iter;
        server_id = Func::get_host_ip(rc_ip.c_str());

        tbnet::Packet *ret_msg = NULL;
        ReqRcStatMessage req_rcstat_msg;
        NewClient* client = NewClientManager::get_instance().create_client();

        req_rcstat_msg.set_app_id(app_id_);
        req_rcstat_msg.set_oper_type(oper_type_);

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
        NewClientManager::get_instance().destroy_client(client);
      }

      show_rcs_stat();

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
