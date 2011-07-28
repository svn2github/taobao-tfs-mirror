#ifndef TFS_NAMEMETASERVER_SQLDEFINE_H_
#define TFS_NAMEMETASERVER_SQLDEFINE_H_

#include <string>
namespace tfs
{
  namespace namemetaserver
  {
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

    enum StoreErrorCode
    {
      UNKNOWN_ERROR = -1,
      TARGET_EXIST_ERROR = -2,
      PARENT_NOT_EXIST_ERROR = -3,
      TARGET_NOT_EXIST_ERROR = -4,
      DELETE_DIR_WITH_FILE_ERROR = -5,
      VERSION_CONFLICT_ERROR = -6
    };
  }
}

#endif
