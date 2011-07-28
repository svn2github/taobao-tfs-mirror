#include "sql_define.h"

namespace tfs
{
  namespace namemetaserver
  {
    std::string ConnStr::mysql_conn_str_ = "10.232.35.41:3306:tfs_name_db";
    std::string ConnStr::mysql_user_ = "root";
    std::string ConnStr::mysql_password_ = "root";
  }
}

