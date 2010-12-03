#ifndef TFS_CLIENT_LOCALKEY_H_
#define TFS_CLIENT_LOCALKEY_H_

#include "Mutex.h"

namespace tfs
{
  namespace client
  {
    struct SegmentHead
    {
      uint64_t addr_;           // saved nameserver ip
    };

    struct SegmentInfo
    {
      int32_t block_id_;        // block id
      uint64_t file_id_;        // file id
      int32_t offset_;          // offset in current file
      int32_t size_;            // size of segment
      int32_t crc_;             // crc checksum of segment
    };

    class LocalKey 
    {
    public:
      LocalKey();
      LocalKey(const char* local_key);
      ~LocalKey();

      int initialize(const char* local_key);

    private:
      tbutil::Mutex mutex_;
      int fd_;
      int32_t offset_;
    };
  }
}

#endif  // TFS_CLIENT_LOCALKEY_H_
