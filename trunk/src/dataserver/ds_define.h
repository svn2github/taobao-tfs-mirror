/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: dataserver_define.h 643 2011-08-02 07:38:33Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_DATASERVER_DEFINE_H_
#define TFS_DATASERVER_DEFINE_H_

#include <string>
#include <assert.h>
#include "common/internal.h"
#include "common/parameter.h"
#include "common/new_client.h"

namespace tfs
{
  namespace dataserver
  {
    static const std::string SUPERBLOCK_NAME = "/fs_super";
    static const std::string MAINBLOCK_DIR_PREFIX = "/";
    static const std::string INDEX_DIR_PREFIX = "/index/";
    static const mode_t DIR_MODE = 0755;
    static const char DEV_TAG[common::MAX_DEV_TAG_LEN] = "TAOBAO";
    static const int32_t COPY_BETWEEN_CLUSTER = -1;
    static const int32_t BLOCK_VERSION_MAGIC_NUM = 2;
    static const int32_t FS_SPEEDUP_VERSION = 2;
    static const int32_t PARITY_INDEX_START = 1024; // index start positioin
    static const int32_t PARITY_INDEX_MMAP_SIZE = PARITY_INDEX_START;
    static const int32_t FILE_INFO_EXT_INIT_VERSION = 1;
    static const int32_t DEFAULT_BLOCK_EXPIRE_TIME = 180; // seconds
    static const int32_t WRITE_INDEX_TIMEOUT_MS = 5000; // millseconds

    static const int32_t EXIT_POST_MSG_RET_NO_OTHER_MEMBER = 0;
    static const int32_t EXIT_POST_MSG_RET_POST_MSG_ERROR  = -1;

    static const int32_t PHYSICAL_BLOCK_ID_INIT_VALUE = 1;
    static const int32_t INDEXFILE_SAFE_MULT = 4;
    //static const int32_t MAX_INITIALIZE_INDEX_SIZE = 8;
    static const int32_t BLOCK_RESERVER_SPACE = 1048576; // reserve 1M space for update
    static const int32_t MAX_INDEX_ELEMENT_NUM = 65535;
    static const int32_t MAX_MMAP_SIZE = (MAX_INDEX_ELEMENT_NUM * common::FILE_INFO_V2_LENGTH ) + common::INDEX_HEADER_V2_LENGTH;

    // flow control parameter
    static const int32_t MB = 1 * 1024 * 1024;
    static const int32_t TRAFFIC_BYTES_STAT_INTERVAL = 1 * 1000 * 1000;//1s
    static const int32_t BUSY_RETRY_TIMES = 3;

    /*#define RW_COUNT_STAT "rw-count-stat"
    #define RW_COUNT_R_SUCCESS "rw-count-r-success"
    #define RW_COUNT_W_SUCCESS "rw-count-w-success"
    #define RW_COUNT_U_SUCCESS "rw-count-w-success"
    #define RW_COUNT_R_FAILED  "rw-count-r-failed"
    #define RW_COUNT_W_FAILED  "rw-count-w-failed"
    #define RW_COUNT_U_FAILED  "rw-count-r-failed"*/

    enum FileinfoFlag
    {
      FI_DELETED = 1,
      FI_INVALID = 2,
      FI_CONCEAL = 4
    };

    struct FileInfoCompare
    {
      bool operator () (const common::FileInfoV2& left, const common::FileInfoV2& right)
      {
        return left.id_ < right.id_;
      }
    };

    struct SuperBlockInfo
    {
      int32_t version_;
      char mount_tag_[common::MAX_DEV_TAG_LEN]; // magic tag
      char mount_point_[common::MAX_DEV_NAME_LEN]; // name of mount point
      int64_t mount_point_use_space_; // the max space of the mount point
      int64_t mount_time_;            // mount time
      int32_t mount_fs_type_;			//file system type,  ext4, ext3,...
      int32_t superblock_reserve_offset_; // super block start offset.
      int32_t block_index_offset_;//block index start offset.
      int32_t max_block_index_element_count_;//block index element count
      int32_t total_main_block_count_;//total count of main block
      int32_t used_main_block_count_; //total count of used main block
      int32_t max_main_block_size_;//max size of main block
      int32_t max_extend_block_size_;//max size of extend block
      int32_t hash_bucket_count_;//total count of hash bucket
      int32_t max_hash_bucket_count_;//max hash bucket
      int32_t main_block_id_seq_;// generate main block id sequence
      int32_t ext_block_id_seq_; // generate ext block id sequence
      common::MMapOption mmap_option_;    //mmap option
      double max_use_block_ratio_;//max use block ratio
      double max_use_hash_bucket_ratio_;//hash slot count / file count ratio
      int64_t reserve_[4];
      int dump(tbnet::DataBuffer& buf) const;
      int dump(std::stringstream& stream) const;
    };

    typedef enum BlockCreateCompleteStatus_
    {
      BLOCK_CREATE_COMPLETE_STATUS_UNCOMPLETE = 0,
      BLOCK_CREATE_COMPLETE_STATUS_COMPLETE = 1
    }BlockCreateCompleteStatus;

    typedef enum BlockSplitFlag_
    {
      BLOCK_SPLIT_FLAG_NO = 0,
      BLOCK_SPLIT_FLAG_YES = 1
    }BlockSplitFlag;

    typedef enum BlockSplitStatus_
    {
      BLOCK_SPLIT_STATUS_UNCOMPLETE = 0,
      BLOCK_SPLIT_STATUS_COMPLETE = 1
    }BlockSplitStatus;

    typedef enum GetSlotType_
    {
      GET_SLOT_TYPE_GEN = 0,
      GET_SLOT_TYPE_QUERY = 1,
      GET_SLOT_TYPE_INSERT = 2
    }GetSlotType;

    struct BlockIndex
    {
      uint64_t logic_block_id_;
      int32_t  physical_block_id_:20;//<=1048575
      int32_t  physical_file_name_id_:20;//<=1048575 number 1~1048575
      int32_t  next_index_:20;//<=1048575
      int32_t  prev_index_:20;//<=1048575
      int8_t   index_:7;// 0 ~36(0: main block, 1~36: ext block)
      int8_t   status_:2;//0: uncomplete, 1: complete
      int8_t   split_flag_:2;//0: unsplit, 1:split
      int8_t   split_status_:2;//0: split uncomplete, 1: split complete
      int8_t   reserve_:3;//reserve
      int dump(tbnet::DataBuffer& buf) const;
      int dump(std::stringstream& stream) const;
      BlockIndex();
    };

    struct DsRuntimeGlobalInformation
    {
      void startup();
      void destroy();
      bool is_destroyed() const;
      void dump(const int32_t level, const char* file, const int32_t line,
            const char* function, const char* format, ...);
      common::DataServerStatInfo information_;
      uint64_t ns_vip_port_;
      int32_t max_mr_network_bandwidth_mb_;
      int32_t max_rw_network_bandwidth_mb_;
      int32_t max_block_size_;
      int32_t max_write_file_count_;
      common::DataServerLiveStatus status_;
      DsRuntimeGlobalInformation();
      static DsRuntimeGlobalInformation& instance();
    };

    typedef enum _OperType
    {
      OPER_NONE   = 0,
      OPER_INSERT = 1,
      OPER_DELETE = 2,
      OPER_UNDELETE = 3,
      OPER_UPDATE = 4
    }OperType;

    class GCObject
    {
    public:
      explicit GCObject(const time_t now):
        last_update_time_(now) {}
      virtual ~GCObject() {}
      virtual void callback() {}
      inline void free(){ delete this;}
      inline time_t get_last_update_time() const { return last_update_time_;}
      inline void update_last_time(const time_t now = common::Func::get_monotonic_time()) { last_update_time_ = now;}
      inline bool can_be_free(const time_t now) const
      {
        return now >= (last_update_time_ + common::SYSPARAM_DATASERVER.object_dead_max_time_);
      }
    protected:
      time_t last_update_time_;
    };

    struct ReplBlockExt
    {
      common::ReplBlock info_;
      int64_t seqno_;
    };

    struct CompactBlkInfo
    {
      int64_t seqno_;
      uint32_t block_id_;
      int32_t preserve_time_;
      int32_t owner_;

      CompactBlkInfo& operator= (const CompactBlkInfo& cpt_blk)
      {
        seqno_ = cpt_blk.seqno_;
        block_id_ = cpt_blk.block_id_;
        preserve_time_ = cpt_blk.preserve_time_;
        owner_ = cpt_blk.owner_;
        return *this;
      }
    };

    int ds_async_callback(common::NewClient* client);
    int post_message_to_server(common::BasePacket* message, const std::vector<uint64_t>& servers);
  }/** end namespace dataserver **/
}/** end namespace tfs **/

#endif //TFS_DATASERVER_DEFINE_H_
