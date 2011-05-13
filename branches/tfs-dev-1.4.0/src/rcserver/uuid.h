#ifndef TFS_RCSERVER_UUID_H_
#define TFS_RCSERVER_UUID_H_

#include <uuid/uuid.h>
#include <string>

namespace tfs
{
  namespace rcserver 
  {
    std::string gene_uuid_str();
  }
}
#endif //TFS_RCSERVER_UUID_H_
