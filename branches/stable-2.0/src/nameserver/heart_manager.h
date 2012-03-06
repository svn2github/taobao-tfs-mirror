/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: heart_manager.h 983 2011-10-31 09:59:33Z duanfei $
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
#ifndef TFS_NAMESERVER_HEART_MANAGEMENT_
#define TFS_NAMESERVER_HEART_MANAGEMENT_

#include <Timer.h>
#include "message/message_factory.h"

namespace tfs
{
  namespace nameserver
  {
    class NameServer;
    class HeartManagement 
    {
    public:
      explicit HeartManagement(NameServer& manager);
      virtual ~HeartManagement();
      int initialize(const int32_t keepalive_thread_count, const int32_t keepalive_queue_size,
                     const int32_t report_block_thread_count, const int32_t report_block_queue_size);
      void wait_for_shut_down();
      void destroy();
      int push(common::BasePacket* msg);

      bool add_report_server(const uint64_t server, const int64_t now);
      int del_report_server(const uint64_t server);
      void cleanup_expired_report_server(const int64_t now);
      bool empty_report_server(const int64_t now);
    private:
      class KeepAliveIPacketQueueHeaderHelper : public tbnet::IPacketQueueHandler
      {
      public:
        KeepAliveIPacketQueueHeaderHelper(HeartManagement& manager): manager_(manager){};
        virtual ~KeepAliveIPacketQueueHeaderHelper() {}
        virtual bool handlePacketQueue(tbnet::Packet* packet, void *args);
      private:
        DISALLOW_COPY_AND_ASSIGN(KeepAliveIPacketQueueHeaderHelper);
        HeartManagement& manager_;
      };
      class ReportBlockIPacketQueueHeaderHelper: public tbnet::IPacketQueueHandler
      {
      public:
        ReportBlockIPacketQueueHeaderHelper(HeartManagement& manager): manager_(manager){};
        virtual ~ReportBlockIPacketQueueHeaderHelper(){}
        virtual bool handlePacketQueue(tbnet::Packet* packet, void *args);
      private:
        DISALLOW_COPY_AND_ASSIGN(ReportBlockIPacketQueueHeaderHelper);
        HeartManagement& manager_;
      };
      class TimeReportBlockTimerTask: public tbutil::TimerTask
      {
      public:
        TimeReportBlockTimerTask(NameServer& manager):manager_(manager){}
        virtual ~TimeReportBlockTimerTask() {}
        void runTimerTask();
      private:
        NameServer& manager_;
        DISALLOW_COPY_AND_ASSIGN(TimeReportBlockTimerTask);
      };
      typedef tbutil::Handle<TimeReportBlockTimerTask> TimeReportBlockTimerTaskPtr;
    private:
      DISALLOW_COPY_AND_ASSIGN(HeartManagement);
      int keepalive(tbnet::Packet* packet);
      int report_block(tbnet::Packet* packet);
      NameServer& meta_mgr_;
      uint32_t keepalive_queue_size_;
      uint32_t report_block_queue_size_;
      tbnet::PacketQueueThread keepalive_threads_;
      tbnet::PacketQueueThread report_block_threads_;
      KeepAliveIPacketQueueHeaderHelper keepalive_queue_header_;
      ReportBlockIPacketQueueHeaderHelper report_block_queue_header_;
      common::RWLock mutex_;
      std::map<uint64_t, int64_t> current_report_servers_;
    };

    class CheckOwnerIsMasterTimerTask: public tbutil::TimerTask
    {
    public:
      explicit CheckOwnerIsMasterTimerTask(LayoutManager* mm);
      virtual ~CheckOwnerIsMasterTimerTask()
      {
      }
      virtual void runTimerTask();

    private:
      DISALLOW_COPY_AND_ASSIGN( CheckOwnerIsMasterTimerTask);
      void master_lost_vip(NsRuntimeGlobalInformation& ngi);
      void check_when_slave_hold_vip(NsRuntimeGlobalInformation& ngi);
      void ns_force_modify_other_side();
      void ns_switch(const NsStatus& other_side_status, const NsSyncDataFlag& ns_sync_flag);
      LayoutManager* meta_mgr_;
    };
    typedef tbutil::Handle<CheckOwnerIsMasterTimerTask> CheckOwnerIsMasterTimerTaskPtr;

    class MasterHeartTimerTask: public tbutil::TimerTask
    {
    public:
      explicit MasterHeartTimerTask(LayoutManager* mm);
      virtual ~MasterHeartTimerTask()
      {
      }
      virtual void runTimerTask();

    private:
      DISALLOW_COPY_AND_ASSIGN(MasterHeartTimerTask);
      LayoutManager* meta_mgr_;
    };
    typedef tbutil::Handle<MasterHeartTimerTask> MasterHeartTimerTaskPtr;

    class SlaveHeartTimerTask: public tbutil::TimerTask
    {
    public:
      SlaveHeartTimerTask(LayoutManager* mm, tbutil::TimerPtr& timer);
      virtual ~SlaveHeartTimerTask()
      {
      }
      virtual void runTimerTask();

    private:
      DISALLOW_COPY_AND_ASSIGN(SlaveHeartTimerTask);
      LayoutManager* meta_mgr_;
      tbutil::TimerPtr& timer_;
    };
    typedef tbutil::Handle<SlaveHeartTimerTask> SlaveHeartTimerTaskPtr;

    class MasterAndSlaveHeartManager: public tbnet::IPacketQueueHandler
    {
    public:
      MasterAndSlaveHeartManager(LayoutManager* mm, tbutil::TimerPtr& timer);
      virtual ~MasterAndSlaveHeartManager();

    public:
      int initialize();
      int wait_for_shut_down();
      int destroy();
      int push(common::BasePacket* message, const int32_t max_queue_size = 0, const bool block = false);
      virtual bool handlePacketQueue(tbnet::Packet *packet, void *args);

    private:
      int do_master_msg(common::BasePacket* message, void* args);
      int do_slave_msg(common::BasePacket* message, void* args);
      int do_heartbeat_and_ns_msg(common::BasePacket* message, void* args);

    private:
      DISALLOW_COPY_AND_ASSIGN( MasterAndSlaveHeartManager);
      LayoutManager* meta_mgr_;
      tbutil::TimerPtr& timer_;
      tbnet::PacketQueueThread work_thread_;
    };
  }
}

#endif 

