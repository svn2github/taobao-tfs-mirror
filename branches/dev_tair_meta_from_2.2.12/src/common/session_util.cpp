#include "session_util.h"
#include <uuid/uuid.h>
#include <stdlib.h>
#include <tbsys.h>
#include "internal.h"
#include "error_msg.h"

namespace tfs
{
  namespace common
  {
    using namespace common;
    using namespace std;

    string SessionUtil::gene_uuid_str()
    {
      uuid_t uu;
      uuid_generate(uu);
      char buf[37];
      uuid_unparse(uu, buf);
      return string(buf);
    }

    void SessionUtil::gene_session_id(const int32_t app_id, const int64_t session_ip, string& session_id)
    {
      stringstream tmp_stream;
      tmp_stream << app_id << SEPARATOR_KEY << session_ip << SEPARATOR_KEY << gene_uuid_str();
      tmp_stream >> session_id;
    }

    int SessionUtil::parse_session_id(const string& session_id, int32_t& app_id, int64_t& session_ip)
    {
      int ret = TFS_SUCCESS;
      size_t first_pos = session_id.find_first_of(SEPARATOR_KEY, 0);
      if (string::npos != first_pos)
      {
        app_id = atoi(session_id.substr(0, first_pos).c_str());
        size_t second_pos = session_id.find_first_of(SEPARATOR_KEY, first_pos + 1);
        if (string::npos != second_pos)
        {
          session_ip = atoll(session_id.substr(first_pos + 1, second_pos - first_pos).c_str());
        }
        else
        {
          ret = EXIT_SESSIONID_INVALID_ERROR;
        }
      }
      else
      {
        ret = EXIT_SESSIONID_INVALID_ERROR;
      }

      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "SessionManager::parse_session_id failed, session_id: %s, ret: %d", session_id.c_str(), ret);
      }

      return ret;
    }

    string SessionUtil::gene_access_key_id()
    {
      uuid_t uu;
      uuid_generate(uu);
      char buf[37];
      uuid_unparse(uu, buf);
      char res[21];
      for(int i = 0; i < 8; ++i)
      {
        res[i] = buf[i];
      }
      for(int i = 8; i < 20; ++i)
      {
        res[i] = buf[i+16];
      }
      res[20] = '\0';
      return string(res);
    }

    string SessionUtil::gene_access_secret_key()
    {
      uuid_t uu;
      uuid_generate(uu);
      char buf[37];
      uuid_unparse(uu, buf);
      char res[41];

      for(int i = 0; i < 36; ++i)
      {
        res[i] = buf[i];
      }
      res[36] = buf[2];
      res[37] = buf[7];
      res[38] = buf[11];
      res[39] = buf[17];
      res[40] = '\0';

      return string(res);
    }
  }
}
