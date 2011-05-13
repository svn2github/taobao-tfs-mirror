#include "uuid.h"

namespace tfs
{
  namespace rcserver 
  {
    std::string gene_uuid_str()
    {
      uuid_t uu; 
      uuid_generate(uu);
      char buf[37];
      uuid_unparse(uu, buf);
      return std::string(buf);
    }
  }
}
