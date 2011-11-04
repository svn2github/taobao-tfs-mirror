/* * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: lease_clerk.h 381 2011-05-30 08:07:39Z nayan@taobao.com $
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
#ifndef TFS_NAMESERVER_LEASE_CLERK_H_
#define TFS_NAMESERVER_LEASE_CLERK_H_

#include <tbsys.h>
#include <Monitor.h>
#include <Mutex.h>
#include <Timer.h>
#include <Shared.h>
#include <Handle.h>
#include "ns_define.h"
#include "common/lock.h"

namespace tfs
{
  namespace nameserver
  {
    enum LeaseType
    {
      LEASE_TYPE_WRITE = 0x00       //write lease
    };

    enum LeaseStatus
    {
      LEASE_STATUS_RUNNING= 0x00,    //running
      LEASE_STATUS_FINISH,         //finish
      LEASE_STATUS_FAILED,          //failed
      LEASE_STATUS_EXPIRED,         //expired
      LEASE_STATUS_CANCELED,        //canceled
      LEASE_STATUS_OBSOLETE         //obsolete
    };

    class LeaseClerk;
    typedef tbutil::Handle<LeaseClerk> LeaseClerkPtr;
    class LeaseEntry : public virtual tbutil::Monitor<tbutil::Mutex>,
                       public virtual tbutil::TimerTask
    {
      public:
        LeaseEntry(LeaseClerkPtr clerk, uint32_t lease_id, int64_t client, LeaseType type = LEASE_TYPE_WRITE);
        virtual ~LeaseEntry();
        uint32_t id() const;
        int64_t client() const;
        LeaseType type() const;
        LeaseStatus status() const;
        void runTimerTask();
        void reset(uint32_t lease_id, LeaseType type = LEASE_TYPE_WRITE);
        bool is_valid_lease() const;
        bool wait_for_expire() const;
        void change(LeaseStatus status);
        bool is_remove(tbutil::Time& now, bool check_time = true) const;
        void dump(int64_t id, bool is_valid, int64_t current_wait_count, int64_t max_wait_count) const;
      private:
        DISALLOW_COPY_AND_ASSIGN(LeaseEntry);
        LeaseEntry();
        LeaseClerkPtr clerk_;
        tbutil::Time last_update_time_;
        tbutil::Time expire_time_;
        int64_t client_;
        uint32_t lease_id_;
        int8_t type_; // lease type
        int8_t status_;//lease status
      public:
        static int16_t LEASE_EXPIRE_DEFAULT_TIME_MS;
        static int32_t LEASE_EXPIRE_TIME_MS;
        static int64_t LEASE_EXPIRE_REMOVE_TIME_MS;
    };
    typedef tbutil::Handle<LeaseEntry> LeaseEntryPtr;

    class LeaseClerk : public virtual tbutil::Shared
    {
      typedef __gnu_cxx::hash_map<int64_t, LeaseEntryPtr, __gnu_cxx::hash<int64_t> > LEASE_MAP;
      typedef LEASE_MAP::iterator LEASE_MAP_ITER;
      typedef LEASE_MAP::const_iterator LEASE_MAP_CONST_ITER;
    public:
      LeaseClerk(int32_t remove_threshold);
      virtual ~LeaseClerk();

      uint32_t add(int64_t client);
      bool remove(int64_t client);
      bool obsolete(int64_t client);
      bool finish(int64_t client);
      bool failed(int64_t client);
      bool cancel(int64_t client);
      bool expire(int64_t client);
      bool commit(int64_t client, uint32_t lease_id, LeaseStatus status);
      bool has_valid_lease(int64_t client);
      bool exist(int64_t client);
      void clear(bool check_time = true, bool force = false);
      LeaseEntryPtr find( int64_t id) const;

    private:
      bool change(int64_t id, LeaseStatus status);
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
  public:
#else
  private:
#endif
      DISALLOW_COPY_AND_ASSIGN( LeaseClerk);
      LeaseClerk();
      LEASE_MAP leases_;
      common::RWLock mutex_;
      int32_t remove_threshold_;
    };

    class LeaseFactory
    {
    public:
      LeaseFactory();
      ~LeaseFactory();
      int initialize(int32_t clerk_num = 32);
      int wait_for_shut_down();
      void destroy();
      static uint32_t new_lease_id();
      uint32_t add(int64_t client);
      bool remove(int64_t client);
      bool obsolete(int64_t client);
      bool finish(int64_t client);
      bool failed(int64_t client);
      bool cancel(int64_t client);
      bool expire(int64_t client);
      bool commit(int64_t client, uint32_t lease_id, LeaseStatus status);
      bool has_valid_lease(int64_t client) const;
      bool exist(int64_t client) const;
      void clear();

      static LeaseFactory& instance() { return instance_;}
    public:
      static volatile uint16_t gwait_count_;

    #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
    public:
    #else
    private:
    #endif
      DISALLOW_COPY_AND_ASSIGN(LeaseFactory);
      static volatile uint32_t lease_id_factory_;
      int32_t clerk_num_;
      LeaseClerkPtr* clerk_;
      static LeaseFactory instance_;
    };
  }
}

#endif

