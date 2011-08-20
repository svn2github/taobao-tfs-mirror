#include "meta_store_handler.h"

namespace tfs
{
  namespace namemetaserver
  {
    int MetaStoreHandler::initialize(const int64_t app_id, const int64_t uid)
    {
      mysql_database_helper_.set_conn_param(mysql_conn_str_, mysql_user_, mysql_password_);
      initialize_ = true;
    }
    int MetaStoreHandler::create_dir(const int64_t app_id, const int64_t uid, int64_t ppid, const char* pname, const int32_t pname_len,
            const int64_t pid, const char* name, const int32_t name_len,
            )
    {
      int ret = TFS_ERROR;
      if (!initialize_)
      {
        initialize();
      }
      else
      {
        int64_t id = 0;
        int64_t mysql_proc_ret = 0;
        int tmp_ret = mysql_database_helper_.get_next_val(id);
        if (TFS_SUCCESS == tmp_ret)
        {
          tmp_ret = mysql_database_helper_.create_dir(app_id, uid, ppid, pname, pname_len, pid, id, name, name_len, mysql_proc_ret);
          if (TFS_SUCCESS == tmp_ret)
          {
            ret = TFS_SUCCESS;
          }
        }
      }
      return ret;
    }
  }
}
