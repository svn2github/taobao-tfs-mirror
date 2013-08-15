/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: ns_define.h 983 2011-10-31 09:59:33Z duanfei $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com>
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_NAMESERVER_DEFINE_H_
#define TFS_NAMESERVER_DEFINE_H_

#include <Mutex.h>
#include <tbsys.h>
#include "common/internal.h"
#include "common/func.h"
#include "common/lock.h"
#include "common/parameter.h"
#include "common/new_client.h"
#include "common/array_helper.h"

namespace tfs
{
  namespace nameserver
  {
    enum NsRole
    {
      NS_ROLE_NONE = 0x00,
      NS_ROLE_MASTER,
      NS_ROLE_SLAVE
    };

    enum NsStatus
    {
      NS_STATUS_NONE = -1,
      NS_STATUS_UNINITIALIZE = 0x00,
      NS_STATUS_INITIALIZED
    };

    enum NsSwitchFlag
    {
      NS_SWITCH_FLAG_NO = 0x00,
      NS_SWITCH_FLAG_YES
    };

    enum ReportBlockStatus
    {
      REPORT_BLOCK_STATUS_REPORTING = 0x00,
      REPORT_BLOCK_STATUS_COMPLETE
    };

    enum HandleDeleteBlockFlag
    {
      HANDLE_DELETE_BLOCK_FLAG_BOTH = 1,
      HANDLE_DELETE_BLOCK_FLAG_ONLY_RELATION = 2,
      HANDLE_DELETE_BLOCK_FLAG_ONLY_DS = 4,
    };

    enum NsKeepAliveType
    {
      NS_KEEPALIVE_TYPE_LOGIN = 0,
      NS_KEEPALIVE_TYPE_RENEW = 1,
      NS_KEEPALIVE_TYPE_LOGOUT = 2
    };

    enum BlockInReplicateQueueFlag
    {
      BLOCK_IN_REPLICATE_QUEUE_NO  = 0,
      BLOCK_IN_REPLICATE_QUEUE_YES = 1
    };

    enum BlockCreateFlag
    {
      BLOCK_CREATE_FLAG_NO = 0,
      BLOCK_CREATE_FLAG_YES = 1
    };

    enum BlockHasLeaseFlag
    {
      BLOCK_HAS_LEASE_FLAG_NO  = 0,
      BLOCK_HAS_LEASE_FLAG_YES = 1
    };

    enum BlockHasVersionConflictFlag
    {
      BLOCK_HAS_VERSION_CONFLICT_FLAG_NO  = 0,
      BLOCK_HAS_VERSION_CONFLICT_FLAG_YES = 1
    };

    enum FamilyInReinstateOrDissolveQueueFlag
    {
      FAMILY_IN_REINSTATE_OR_DISSOLVE_QUEUE_NO = 0,
      FAMILY_IN_REINSTATE_OR_DISSOLVE_QUEUE_YES = 1
    };

    enum CallbackFlag
    {
      CALL_BACK_FLAG_NONE  = 0,
      CALL_BACK_FLAG_PUSH  = 1,
      CALL_BACK_FLAG_CLEAR = 2
    };

    struct NsGlobalStatisticsInfo
    {
      void dump(int32_t level, const char* file = __FILE__, const int32_t line = __LINE__, const char* function =
          __FUNCTION__, const pthread_t thid = pthread_self()) const;
      volatile int64_t use_capacity_;
      volatile int64_t total_capacity_;
      volatile int64_t total_block_count_;
      volatile int64_t total_load_;
    };

    struct NsRuntimeGlobalInformation
    {
      uint64_t heart_ip_port_;
      uint64_t owner_ip_port_;
      uint64_t peer_ip_port_;
      int64_t switch_time_;
      int64_t discard_newblk_safe_mode_time_;
      int64_t lease_id_;
      int64_t lease_expired_time_;
      int64_t startup_time_;
      uint32_t vip_;
      bool destroy_flag_;
      int8_t owner_role_;
      int8_t peer_role_;
      int8_t owner_status_;
      int8_t peer_status_;

      bool is_destroyed() const;
      bool in_safe_mode_time(const int64_t now) const;
      bool in_discard_newblk_safe_mode_time(const int64_t now) const;
      bool is_master() const;
      bool peer_is_master() const;
      int keepalive(int64_t& lease_id, const uint64_t server,
         const int8_t role, const int8_t status, const int8_t type, const time_t now);
      bool logout();
      bool has_valid_lease(const time_t now) const;
      bool renew(const int32_t step, const time_t now);
      bool renew(const int64_t lease_id, const int32_t step, const time_t now);
      void switch_role(const bool startup = false, const int64_t now = common::Func::get_monotonic_time());
      void update_peer_info(const uint64_t server, const int8_t role, const int8_t status);
      bool own_is_initialize_complete() const;
      void initialize();
      void destroy();
      void dump(const int32_t level, const char* file, const int32_t line,
            const char* function, const pthread_t thid, const char* format, ...);
      NsRuntimeGlobalInformation();
      static NsRuntimeGlobalInformation& instance();
      static NsRuntimeGlobalInformation instance_;
    };

    static const int32_t THREAD_STATCK_SIZE = 16 * 1024 * 1024;
    static const int32_t MAX_SERVER_NUMS = 3000;
    static const int32_t MAX_PROCESS_NUMS = MAX_SERVER_NUMS * 12;
    static const int32_t MAX_BLOCK_CHUNK_NUMS = 10240 * 4;
    static const int32_t MAX_WRITE_FILE_COUNT = 256;

    static const uint64_t GB = 1 * 1024 * 1024 * 1024;
    static const uint64_t MB = 1 * 1024 * 1024;
    static const double PERCENTAGE_MIN = 0.000001;
    static const double PERCENTAGE_MAX = 1.000000;
    static const double PERCENTAGE_MAGIC = 1000000.0;
    double calc_capacity_percentage(const uint64_t capacity, const uint64_t total_capacity);

    static const int32_t MAX_RACK_NUM = 512;
    static const int32_t MAX_SINGLE_RACK_SERVER_NUM = 64;
    static const int32_t MAX_MARSHLLING_QUEUE_ELEMENT_SIZE = 128;//编组队列大小
    static const int32_t MAX_FAMILY_CHUNK_NUM = 10240 * 4;

    static const int32_t MAX_TASK_RESERVE_TIME = 5;

    extern int ns_async_callback(common::NewClient* client);
    extern void print_int64(const common::ArrayHelper<uint64_t>&servers, std::string& result);
    extern void print_int64(const std::vector<uint64_t>& servers, std::string& result);
    extern bool is_equal_group(const uint64_t id);
 }/** nameserver **/
}/** tfs **/

#endif
