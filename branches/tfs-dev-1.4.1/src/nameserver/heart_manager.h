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
 *   qushan<qushan@taobao.com> 
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_NAMESERVER_HEART_MANAGEMENT_
#define TFS_NAMESERVER_HEART_MANAGEMENT_

#include <Timer.h>
#include "layout_manager.h"
#include "message/message_factory.h"

namespace tfs
{
  namespace nameserver
  {
    class HeartManagement : public tbnet::IPacketQueueHandler
    {
    public:
      explicit HeartManagement(LayoutManager& manager);
      virtual ~HeartManagement();
      int initialize(const int32_t thread_count, const int32_t max_queue_size);
      void wait_for_shut_down();
      void destroy();
      int push(common::BasePacket* msg);
      virtual bool handlePacketQueue(tbnet::Packet* packet, void *args);

    private:
      DISALLOW_COPY_AND_ASSIGN(HeartManagement);
      int keepalive(tbnet::Packet* packet);
      LayoutManager& meta_mgr_;
      int32_t max_queue_size_;
      tbnet::PacketQueueThread work_thread_;
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

