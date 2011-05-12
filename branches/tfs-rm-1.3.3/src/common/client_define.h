#ifndef TFS_COMMON_CLIENT_DEFINE_H_
#define TFS_COMMON_CLIENT_DEFINE_H_

#if __cplusplus
extern "C"
{
#endif
  struct TfsFileStat
  {
    uint64_t file_id_;
    int32_t offset_; // offset in block file
    int64_t size_; // file size
    int64_t usize_; // hold space
    time_t modify_time_; // modify time
    time_t create_time_; // create time
    int32_t flag_; // deleta flag
    uint32_t crc_; // crc value
  };

  enum TfsStatFlag
  {
    NORMAL_STAT = 0,
    FORCE_STAT
  };

  enum TfsUnlinkType
  {
    DELETE = 0,
    UNDELETE = 2,
    CONCEAL = 4,
    REVEAL = 6
  };
#if __cplusplus
}
#endif
#endif
