#ifndef TFS_NAMEMETASERVER_SQLDEFINE_H_
#define TFS_NAMEMETASERVER_SQLDEFINE_H_

#include <string>
namespace tfs
{
  namespace namemetaserver
  {
    const int32_t MAX_FILE_PATH_LEN = 512;
    const int32_t SOFT_MAX_FRAG_INFO_COUNT = 1024;
    const int32_t MAX_FRAG_INFO_SIZE = 65535;
    const int32_t MAX_OUT_FRAG_INFO = 256;

    struct ConnStr
    {
      static std::string mysql_conn_str_;
      static std::string mysql_user_;
      static std::string mysql_password_;
    };

    enum FileType
    {
      NORMAL_FILE = 1,
      DIRECTORY = 2,
      PWRITE_FILE = 3
    };
  }
}

#endif
