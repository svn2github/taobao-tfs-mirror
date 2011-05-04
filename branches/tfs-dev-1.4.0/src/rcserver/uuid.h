#ifndef TFS_RCSERVER_UUID_H_
#define TFS_RCSERVER_UUID_H_

#include <uuid/uuid.h>
#include <string>

namespace tfs
{
  namespace common
  {
    std::string gene_uuid_str()
    {
      uuid_t uu; 
      uuid_generate(uu);
      char buf[37];
      uuid_unparse(uu, buf);
      return string(buf);
    }
  }
}
#endif //TFS_RCSERVER_UUID_H_
