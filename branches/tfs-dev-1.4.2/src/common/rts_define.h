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
 *   duanfei <duanfei @taobao.com>
 *      - initial release
 *
 */

#include <map>
#include <ext/hash_map>
#include <Time.h>

#ifndef TFS_ROOT_SERVER_DEFINE_H_
#define TFS_ROOT_SERVER_DEFINE_H_

namespace tfs
{
  namespace common
  {
    typedef enum _MetaServerKeepaliveTypes
    {
      MS_KEEPALIVE_TYPE_REGISTER = 0,
      MS_KEEPALIVE_TYPE_UNREGISTER = 1,
      MS_KEEPALIVE_TYPE_RENEW_LEASE = 2
    }MetaServerKeepaliveTypes;

    typedef enum _MetaServerStatus
    {
      META_SERVER_STATUS_NONE = 0, /** uninitialize **/
      META_SERVER_STATUS_RW = 1,   /** read & write **/
      META_SERVER_STATUS_READ_ONLY = 2, /** read only **/
    }MetaServerStatus;

    typedef enum _BuildTableInterruptFlag
    {
      BUILD_TABLE_INTERRUPT_WAIT = 0, /** wait **/
      BUILD_TABLE_INTERRUPT_ALL  = 1, /** interrupt all(build table, update table) **/
      BUILD_TABLE_INTERRUPT_REBUILD = 2 /** rebuild table **/
    }BuildTableInterruptFlag;

    struct MetaServerLease
    {
      bool has_valid_lease(const int64_t now) ;
      bool renew(const int32_t step, const int64_t now);
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      uint64_t lease_id_; /** lease id **/
      int64_t  lease_expired_time_; /** lease expire time (ms) */
    };

    struct MetaServerTables
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      int64_t version_; /** table version **/
    };

    struct MetaServerThroughput
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      int64_t read_count_;
      int64_t write_count_;
      int64_t cache_dir_count_;
      int64_t cache_dir_hit_count_;
      int64_t cache_file_count_;
      int64_t cache_file_hit_count_;
    };

    struct NetWorkStatInformation
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      int64_t recv_packet_count_;
      int64_t recv_bytes_count_;
      int64_t send_packet_count_;
      int64_t send_bytes_count_;
    };

    struct MetaServerCapacity
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      int64_t  mem_capacity_;    /** memory total capacity **/
      int64_t  use_mem_capacity_;/** memory use capacity **/
    };

    struct MetaServerBaseInformation
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      uint64_t id_; /** MetaServer id(IP + PORT) **/
      int64_t  start_time_; /** MetaServer start time (ms)**/
      int64_t  last_update_time_;/** MetaServer last update time (ms)**/
      int32_t  current_load_; /** MetaServer current load **/
      int8_t   status_; /** MetaServer status : META_SERVER_STATUS_NONE, META_SERVER_STATUS_RW, META_SERVER_STATUS_READ_ONLY**/
      int8_t   reserve_[3];
      void update(const MetaServerBaseInformation& info, const int64_t now = time(NULL));
    };

    struct MetaServer
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      MetaServerLease lease_;
      MetaServerTables tables_;
      MetaServerThroughput throughput_;
      NetWorkStatInformation net_work_stat_;
      MetaServerCapacity capacity_;
      MetaServerBaseInformation base_info_;
    }; 

    typedef enum _NewTableItemUpdateStatus
    {
      NEW_TABLE_ITEM_UPDATE_STATUS_BEGIN = 0,
      NEW_TABLE_ITEM_UPDATE_STATUS_RUNNING = 1,
      NEW_TABLE_ITEM_UPDATE_STATUS_END = 2,
      NEW_TABLE_ITEM_UPDATE_STATUS_FAILED = 3
    }NewTableItemUpdateStatus;

    typedef enum _UpdateTablePhase
    {
      UPDATE_TABLE_PHASE_1 = 0,
      UPDATE_TABLE_PHASE_2 = 1
    };

    struct NewTableItem
    {
      tbutil::Time begin_time_;
      tbutil::Time send_msg_time_;
      int8_t status_;
      int8_t phase_; 
    };

    typedef enum _DumpTableType
    {
      DUMP_TABLE_TYPE_TABLE = 0,
      DUMP_TABLE_TYPE_ACTIVE_TABLE = 1
    }DumpTableType;

    typedef enum _RsRole
    {
      RS_ROLE_MASTER = 0,
      RS_ROLE_SLAVE  = 1
    }RsRole;

    typedef enum _RsStatus
    {
      RS_STATUS_NONE = -1,
      RS_STATUS_UNINITIALIZE = 0,
      RS_STATUS_OTHERSIDEDEAD = 1,
      RS_STATUS_INITIALIZED = 2
    }RsStatus;

    typedef enum _RsDestroyFlag
    {
      NS_DESTROY_FLAGS_NO = 0,
      NS_DESTROY_FLAGS_YES = 1
    }RsDestroyFlag;

    struct RsRuntimeGlobalInformation
    {
      uint64_t owner_ip_port_;
      uint64_t other_side_ip_port_;
      int64_t  switch_time_;
      uint32_t vip_;
      RsRole owner_role_;
      RsRole other_side_role_;
      RsStatus owner_status_;
      RsStatus other_side_status_;
      RsDestroyFlag destroy_flag_;
      static RsRuntimeGlobalInformation& instance();
      static RsRuntimeGlobalInformation instance_;
    };

    typedef enum _RtsMsKeepAliveType
    {
      RTS_MS_KEEPALIVE_TYPE_LOGIN = 0,
      RTS_MS_KEEPALIVE_TYPE_RENEW = 1,
      RTS_MS_KEEPALIVE_TYPE_LOGOUT = 2 
    }RtsMsKeepAliveType;

    static const int64_t MAX_BUCKET_ITEM_DEFAULT = 10240;
    static const int64_t MAX_SERVER_ITEM_DEFAULT = 1024;

    static const int64_t INVALID_TABLE_VERSION = -1;

    typedef __gnu_cxx::hash_map<uint64_t, MetaServer> META_SERVER_MAPS;
    typedef META_SERVER_MAPS::iterator META_SERVER_MAPS_ITER;
    typedef META_SERVER_MAPS::const_iterator META_SERVER_MAPS_CONST_ITER;
    typedef __gnu_cxx::hash_map<uint64_t, META_SERVER_MAPS_ITER> META_SERVER_LEASE_MAPS;
    //typedef std::pair<std::vector<uint64_t>, bool> NEW_TABLE_ITEM;
    typedef std::map<uint64_t, NewTableItem> NEW_TABLE;
    typedef NEW_TABLE::iterator NEW_TABLE_ITER;
    typedef NEW_TABLE::const_iterator NEW_TABLE_CONST_ITER;
  } /** common **/
} /** tfs **/

#endif
