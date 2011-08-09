#ifndef TFS_RCSERVER_SESSIONUTIL_H_
#define TFS_RCSERVER_SESSIONUTIL_H_

#include <string>

namespace tfs
{
  namespace rcserver 
  {
    static const char SEPARATOR_KEY = '-';
    class SessionUtil
    {
      public:
        static std::string gene_uuid_str();
        static void gene_session_id(const int32_t app_id, const int64_t session_ip, std::string& session_id);
        static int parse_session_id(const std::string& session_id, int32_t& app_id, int64_t& session_ip);
    };
  }
}
#endif //TFS_RCSERVER_SESSIONUUID_H_
