
#ifndef TFS_NAMESERVER_BITMAP_MANAGER_H_
#define TFS_NAMESERVER_BITMAP_MANAGER_H_

#include <stdint.h>
#include "ns_define.h"
#include "common/internal.h"
#include "common/mmap_file_op.h"

namespace tfs
{
  namespace nameserver
  {
   class MBitMapOperation : public common::MMapFileOperation
   {
      public:
        explicit MBitMapOperation(const std::string& filename)
            : common::MMapFileOperation(filename, O_RDWR | O_LARGEFILE | O_CREAT) {}
        ~MBitMapOperation() {}

        bool test(const uint64_t index) const;
        int set(const uint64_t index);
        int  reset(const uint64_t index);

        int check_and_ensure_size(const int64_t need_map_size);

      private:
      DISALLOW_COPY_AND_ASSIGN(MBitMapOperation);

        static const uint32_t SLOT_SIZE = 8 * sizeof(char);
        static const unsigned char BITMAPMASK[SLOT_SIZE];
        mutable common::RWLock rwmutex_;
   };
  }
}
#endif
