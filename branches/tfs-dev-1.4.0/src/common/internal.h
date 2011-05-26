/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_INTERVAL_H_
#define TFS_COMMON_INTERVAL_H_

#include <string>
#include <map>
#include <vector>
#include <list>
#include <set>
#include <ext/hash_map>
#include <string.h>
#include <stdint.h>
#include "define.h"
#include <databuffer.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <limits.h>
#include <tbnet.h>
#include "stream.h"


namespace tfs
{
  namespace nameserver
  {
    class BlockCollect;
    class ServerCollect;
  }
  namespace common
  {
    enum OplogFlag
    {
      OPLOG_INSERT = 1,
      OPLOG_UPDATE,
      OPLOG_REMOVE,
      OPLOG_RENAME,
      OPLOG_RELIEVE_RELATION
    };

    enum DataServerLiveStatus
    {
      DATASERVER_STATUS_ALIVE = 0x00,
      DATASERVER_STATUS_DEAD
    };

    //blockinfo message
    enum UpdateBlockType
    {
      UPDATE_BLOCK_NORMAL = 100,
      UPDATE_BLOCK_REPAIR,
      UPDATE_BLOCK_MISSING
    };

    enum ListBlockType
    {
      LB_BLOCK = 1,
      LB_PAIRS = 2,
      LB_INFOS = 4
    };

    enum ListBitmapType
    {
      NORMAL_BIT_MAP,
      ERROR_BIT_MAP
    };

    enum UnlinkFlag
    {
      UNLINK_FLAG_NO = 0x0,
      UNLINK_FLAG_YES
    };

    enum HasBlockFlag
    {
      HAS_BLOCK_FLAG_NO = 0x0,
      HAS_BLOCK_FLAG_YES
    };

    enum GetServerStatusType
    {
      GSS_MAX_VISIT_COUNT,
      GSS_BLOCK_FILE_INFO,
      GSS_BLOCK_RAW_META_INFO,
      GSS_CLIENT_ACCESS_INFO
    };

    //sync
    enum SyncFlag
    {
      TFS_FILE_NO_SYNC_LOG = 1
    };

    enum CheckDsBlockType
    {
      CRC_DS_PATIAL_ERROR = 0,
      CRC_DS_ALL_ERROR,
      CHECK_BLOCK_EIO
    };

    // write data info
    enum ServerRole
    {
      Master_Server_Role = 0,
      Slave_Server_Role
    };

    enum ReplicateBlockMoveFlag
    {
      REPLICATE_BLOCK_MOVE_FLAG_NO = 0x00,
      REPLICATE_BLOCK_MOVE_FLAG_YES
    };

    enum WriteCompleteStatus
    {
      WRITE_COMPLETE_STATUS_YES = 0x00,
      WRITE_COMPLETE_STATUS_NO
    };

    // close write file msg
    enum CloseFileServer
    {
      CLOSE_FILE_MASTER = 100,
      CLOSE_FILE_SLAVER
    };

    enum SsmType
    {
      SSM_BLOCK = 1,
      SSM_SERVER = 2,
      SSM_WBLIST = 4
    };

    enum ServerStatus
    {
      IS_SERVER = 1
    };

    enum ReadDataVersion
    {
      READ_VERSION_2 = 2,
      READ_VERSION_3 = 3
    };

    enum PlanInterruptFlag
    {
      INTERRUPT_NONE= 0x00,
      INTERRUPT_ALL = 0x01
    };

    enum PlanType
    {
      PLAN_TYPE_REPLICATE = 0x00,
      PLAN_TYPE_MOVE,
      PLAN_TYPE_COMPACT,
      PLAN_TYPE_DELETE
    };

    enum PlanStatus
    {
      PLAN_STATUS_NONE = 0x00,
      PLAN_STATUS_BEGIN,
      PLAN_STATUS_TIMEOUT,
      PLAN_STATUS_FAILURE,
      PLAN_STATUS_END
    };

    enum PlanPriority
    {
      PLAN_PRIORITY_NONE = -1,
      PLAN_PRIORITY_NORMAL = 0,
      PLAN_PRIORITY_EMERGENCY = 1 
    };

    enum PlanRunFlag
    {
      PLAN_RUN_FLAG_NONE = 0,
      PLAN_RUN_FLAG_REPLICATE = 1,
      PLAN_RUN_FLAG_MOVE = 1 << 1,
      PLAN_RUN_FLAG_COMPACT = 1 << 2,
      PLAN_RUN_FLAG_DELETE = 1 << 3
    };

    enum DeleteExcessBackupStrategy
    {
      DELETE_EXCESS_BACKUP_STRATEGY_NORMAL = 1,
      DELETE_EXCESS_BACKUP_STRATEGY_BY_GROUP =  1 << 1
    };

    enum SSMType
    {
      SSM_TYPE_BLOCK = 0x01,
      SSM_TYPE_SERVER = 0x02
    };
    enum SSMChildBlockType
    {
      SSM_CHILD_BLOCK_TYPE_INFO   = 0x01,
      SSM_CHILD_BLOCK_TYPE_SERVER = 0x02,
      SSM_CHILD_BLOCK_TYPE_FULL = 0x04
    };

    enum SSMChildServerType
    {
      SSM_CHILD_SERVER_TYPE_ALL = 0x01,
      SSM_CHILD_SERVER_TYPE_HOLD = 0x02,
      SSM_CHILD_SERVER_TYPE_WRITABLE = 0x04,
      SSM_CHILD_SERVER_TYPE_MASTER = 0x08,
      SSM_CHILD_SERVER_TYPE_INFO = 0x10
    };

    enum SSMPacketType
    {
      SSM_PACKET_TYPE_REQUEST = 0x01,
      SSM_PACKET_TYPE_REPLAY  = 0x02
    };
    enum SSMScanEndFlag
    {
      SSM_SCAN_END_FLAG_YES = 0x01,
      SSM_SCAN_END_FLAG_NO  = 0x02
    };
    enum SSMScanCutoverFlag
    {
      SSM_SCAN_CUTOVER_FLAG_YES = 0x01,
      SSM_SCAN_CUTOVER_FLAG_NO  = 0x02
    };

    struct SSMScanParameter
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      mutable tbnet::DataBuffer data_;
      uint32_t addition_param1_;
      uint32_t addition_param2_;
      uint32_t  start_next_position_;//16~32 bit: start, 0~15 bit: next
      uint32_t  should_actual_count_;//16~32 bit: should_count 0~15: actual_count
      int16_t  child_type_;
      int8_t   type_;
      int8_t   end_flag_;
    };
    // common data structure
#pragma pack(4)
    struct BlockInfo
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      uint32_t block_id_;
      int32_t version_;
      int32_t file_count_;
      int32_t size_;
      int32_t del_file_count_;
      int32_t del_size_;
      uint32_t seq_no_;

      BlockInfo()
      {
        memset(this, 0, sizeof(BlockInfo));
      }
      inline bool operator==(const BlockInfo& rhs) const
      {
        return block_id_ == rhs.block_id_ && version_ == rhs.version_ && file_count_ == rhs.file_count_ && size_
            == rhs.size_ && del_file_count_ == rhs.del_file_count_ && del_size_ == rhs.del_size_ && seq_no_
            == rhs.seq_no_;
      }
    };

    struct RawMeta
    {
    public:
      RawMeta()
      {
        init();
      }

      void init()
      {
        fileid_ = 0;
        location_.inner_offset_ = 0;
        location_.size_ = 0;
      }

      RawMeta(const uint64_t file_id, const int32_t in_offset, const int32_t file_size)
      {
        fileid_ = file_id;
        location_.inner_offset_ = in_offset;
        location_.size_ = file_size;
      }

      RawMeta(const RawMeta& raw_meta)
      {
        memcpy(this, &raw_meta, sizeof(RawMeta));
      }

      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      uint64_t get_key() const
      {
        return fileid_;
      }

      void set_key(const uint64_t key)
      {
        fileid_ = key;
      }

      uint64_t get_file_id() const
      {
        return fileid_;
      }

      void set_file_id(const uint64_t file_id)
      {
        fileid_ = file_id;
      }

      int32_t get_offset() const
      {
        return location_.inner_offset_;
      }

      void set_offset(const int32_t offset)
      {
        location_.inner_offset_ = offset;
      }

      int32_t get_size() const
      {
        return location_.size_;
      }

      void set_size(const int32_t file_size)
      {
        location_.size_ = file_size;
      }

      bool operator==(const RawMeta& rhs) const
      {
        return fileid_ == rhs.fileid_ && location_.inner_offset_ == rhs.location_.inner_offset_ && location_.size_
            == rhs.location_.size_;
      }

    private:
      uint64_t fileid_;
      struct
      {
        int32_t inner_offset_;
        int32_t size_;
      } location_;
    };

    struct ReplBlock
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      uint32_t block_id_;
      uint64_t source_id_;
      uint64_t destination_id_;
      int32_t start_time_;
      int32_t is_move_;
      int32_t server_count_;
   };

    struct Throughput
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      int64_t write_byte_;
      int64_t write_file_count_;
      int64_t read_byte_;
      int64_t read_file_count_;
    };

    //dataserver stat info
    struct DataServerStatInfo
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      uint64_t id_;
      int64_t use_capacity_;
      int64_t total_capacity_;
      int32_t current_load_;
      int32_t block_count_;
      int32_t last_update_time_;
      int32_t startup_time_;
      Throughput total_tp_;
      int32_t current_time_;
      DataServerLiveStatus status_;
    };

    struct WriteDataInfo
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      uint32_t block_id_;
      uint64_t file_id_;
      int32_t offset_;
      int32_t length_;
      ServerRole is_server_;
      uint64_t file_number_;
    };

    struct CloseFileInfo
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      uint32_t block_id_;
      uint64_t file_id_;
      CloseFileServer mode_;
      uint32_t crc_;
      uint64_t file_number_;
    };

    struct RenameFileInfo
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      uint32_t block_id_;
      uint64_t file_id_;
      uint64_t new_file_id_;
      ServerRole is_server_;
    };

    struct ServerMetaInfo
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      int32_t capacity_;
      int32_t available_;
    };

    static const int32_t SEGMENT_HEAD_RESERVE_SIZE = 64;
    struct SegmentHead
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      int32_t count_;           // segment count
      int64_t size_;            // total size that segments contain
      char reserve_[SEGMENT_HEAD_RESERVE_SIZE]; // reserve
      SegmentHead() : count_(0), size_(0)
      {
        memset(reserve_, 0, SEGMENT_HEAD_RESERVE_SIZE);
      }
    };

    struct SegmentInfo
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      uint32_t block_id_;       // block id
      uint64_t file_id_;        // file id
      int64_t offset_;          // offset in current file
      int32_t size_;            // size of segment
      int32_t crc_;             // crc checksum of segment
      std::vector<FileInfo> tmp; 

      SegmentInfo()
      {
        memset(this, 0, sizeof(*this));
      }
      SegmentInfo(const SegmentInfo& seg_info)
      {
        memcpy(this, &seg_info, sizeof(SegmentInfo));
      }
      bool operator < (const SegmentInfo& si) const
      {
        return offset_ < si.offset_;
      }
    };
#pragma pack()

    struct CrcCheckFile
    {
      uint32_t block_id_;
      uint64_t file_id_;
      uint32_t crc_;
      CheckDsBlockType flag_;
      VUINT64 fail_servers_;

      CrcCheckFile() :
        block_id_(0), file_id_(0), crc_(0), flag_(CRC_DS_PATIAL_ERROR)
      {
        fail_servers_.clear();
      }

      CrcCheckFile(const uint32_t block_id, const CheckDsBlockType flag) :
        block_id_(block_id), file_id_(0), crc_(0), flag_(flag)
      {
        fail_servers_.clear();
      }
    };

    struct BlockInfoSeg
    {
      common::VUINT64 ds_;
      bool has_lease_;
      uint32_t lease_;
      int32_t version_;
      BlockInfoSeg() : has_lease_(false), lease_(0), version_(0)
      {
        ds_.clear();
      }
      BlockInfoSeg(const common::VUINT64& ds, const bool has_lease = false,
                   const uint32_t lease = 0, const int32_t version = 0) :
        ds_(ds), has_lease_(has_lease), lease_(lease), version_(version)
      {
      }
    };

    static const int32_t BLOCKINFO_SIZE = sizeof(BlockInfo);
    static const int32_t RAW_META_SIZE = sizeof(RawMeta);

    static const int32_t MAX_DEV_NAME_LEN = 64;
    static const int32_t MAX_READ_SIZE = 1048576;

    // typedef
    typedef std::map<std::string, std::string> STRING_MAP; // string => string
    typedef STRING_MAP::iterator STRING_MAP_ITER;

    typedef std::vector<BlockInfo> BLOCK_INFO_LIST;
    typedef std::vector<FileInfo> FILE_INFO_LIST;
    typedef std::map<uint64_t, FileInfo*> FILE_INFO_MAP;
    typedef FILE_INFO_MAP::iterator FILE_INFO_MAP_ITER;

    typedef __gnu_cxx ::hash_map<uint32_t, VINT64> INT_VINT64_MAP; // int => vector<int64>
    typedef INT_VINT64_MAP::iterator INT_VINT64_MAP_ITER;
    typedef __gnu_cxx ::hash_map<uint64_t, VINT, __gnu_cxx ::hash<int> > INT64_VINT_MAP; // int64 => vector<int>
    typedef INT64_VINT_MAP::iterator INT64_VINT_MAP_ITER;
    typedef __gnu_cxx ::hash_map<uint32_t, uint64_t> INT_INT64_MAP; // int => int64
    typedef INT_INT64_MAP::iterator INT_INT64_MAP_ITER;
    typedef __gnu_cxx ::hash_map<uint64_t, uint32_t, __gnu_cxx ::hash<int> > INT64_INT_MAP; // int64 => int
    typedef INT64_INT_MAP::iterator INT64_INT_MAP_ITER;

    typedef __gnu_cxx ::hash_map<uint32_t, uint32_t> INT_MAP;
    typedef INT_MAP::iterator INT_MAP_ITER;

    typedef std::vector<RawMeta> RawMetaVec;
    typedef std::vector<RawMeta>::iterator RawMetaVecIter;

    typedef __gnu_cxx ::hash_map<uint32_t, nameserver::BlockCollect*, __gnu_cxx ::hash<uint32_t> > BLOCK_MAP;
    typedef BLOCK_MAP::iterator BLOCK_MAP_ITER;
    typedef __gnu_cxx ::hash_map<uint64_t, nameserver::ServerCollect*, __gnu_cxx ::hash<uint64_t> > SERVER_MAP;
    typedef SERVER_MAP::iterator SERVER_MAP_ITER;
    typedef std::vector<RawMeta>::const_iterator RawMetaVecConstIter;
  }
}

#endif //TFS_COMMON_DEFINE_H_
