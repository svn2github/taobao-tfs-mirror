
#include "bit_map.h"
#include <stdlib.h>
#include <assert.h>

using namespace tfs::common;
namespace tfs
{
  namespace nameserver
  {
    const unsigned char MBitMapOperation::BITMAPMASK[SLOT_SIZE] =
    {
      0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80
    };

    int MBitMapOperation::check_and_ensure_size(const int64_t need_map_size)
    {
      int ret = TFS_SUCCESS;
      int64_t align_size = ALIGN(need_map_size, SSD_BLOCK_SIZE);
      if (align_size > MAX_BITMAP_MAP_SIZE) // protect from passing into huge blockid by mistack
      {
        ret = EXIT_BIT_MAP_OUT_OF_RANGE;
      }
      else if (align_size > length())
      {
        int32_t advise_inc_mmap_size = static_cast<int32_t>(align_size - length());
        rwmutex_.wrlock();
        ret = mremap(advise_inc_mmap_size);// extend mremap first, need write lock due to virtual addr maybe change
        rwmutex_.unlock();
        TBSYS_LOG(INFO, "mremap need extend map size to: %"PRI64_PREFIX"u, ret: %d", need_map_size, ret);
      }
      return ret;
    }

    bool MBitMapOperation::test(const uint64_t index) const
    {
      rwmutex_.rdlock();
      char* data = get_data();
      assert(index < (static_cast<uint64_t>(length()) << 3));
      uint64_t quot = index / SLOT_SIZE;
      uint32_t rem = index % SLOT_SIZE;
      bool ret = (data[quot] & BITMAPMASK[rem]) != 0;
      rwmutex_.unlock();
      return ret;
    }

    int MBitMapOperation::set(const uint64_t index)
    {
      int ret = check_and_ensure_size(ALIGN(index, 8) >> 3);
      if (TFS_SUCCESS == ret)
      {
        rwmutex_.rdlock();
        char* data = get_data();
        assert(index < (static_cast<uint64_t>(length()) << 3));
        uint64_t quot = index / SLOT_SIZE;
        uint32_t rem = index % SLOT_SIZE;
        if (!(data[quot] & BITMAPMASK[rem]))
        {
          data[quot] |= BITMAPMASK[rem];
        }
        rwmutex_.unlock();
      }
      return ret;
    }

    int MBitMapOperation::reset(const uint64_t index)
    {
      int ret = check_and_ensure_size(ALIGN(index, 8) >> 3);
      if (TFS_SUCCESS == ret)
      {
        rwmutex_.rdlock();
        char* data = get_data();
        assert(index < (static_cast<uint64_t>(length()) << 3));
        uint64_t quot = index / SLOT_SIZE;
        uint32_t rem = index % SLOT_SIZE;
        if (data[quot] & BITMAPMASK[rem])
        {
          data[quot] &= ~BITMAPMASK[rem];
        }
        rwmutex_.unlock();
      }
      return ret;
    }
  }
}
