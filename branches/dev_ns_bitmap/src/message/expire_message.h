/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: $
 *
 * Authors:
 *   qixiao.zs <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */
#ifndef TFS_MESSAGE_EXPIRE_MESSAGE_H_
#define TFS_MESSAGE_EXPIRE_MESSAGE_H_

#include "common/expire_define.h"
#include "common/base_packet.h"

namespace tfs
{
  namespace message
  {
    class ReqCleanTaskFromRtsMessage : public common::BasePacket
    {
      public:
        ReqCleanTaskFromRtsMessage();
        virtual ~ReqCleanTaskFromRtsMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        common::ExpireTaskInfo get_task() const
        {
          return task_;
        }
        void set_task(const common::ExpireTaskInfo& task)
        {
          task_ = task;
        }

      private:
        common::ExpireTaskInfo task_;
    };

    class ReqFinishTaskFromEsMessage : public common::BasePacket
    {
      public:
        ReqFinishTaskFromEsMessage();
        virtual ~ReqFinishTaskFromEsMessage();
        virtual int serialize(common::Stream& output) const;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        uint64_t get_es_id() const
        {
          return es_id_;
        }

        void set_es_id(const uint64_t es_id)
        {
          es_id_ = es_id;
        }
        void set_task(const common::ExpireTaskInfo& rh)
        {
          task_ = rh;
        }
        common::ExpireTaskInfo get_task() const
        {
          return task_;
        }

      private:
        uint64_t es_id_;
        common::ExpireTaskInfo task_;
    };

    class ReqRtsEsHeartMessage: public common::BasePacket
    {
      public:
        ReqRtsEsHeartMessage();
        virtual ~ReqRtsEsHeartMessage();

        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline common::ExpServerBaseInformation& get_mutable_es(void) { return base_info_;}
        inline void set_es(const common::ExpServerBaseInformation& base_info){ memcpy(&base_info_, &base_info, sizeof(common::ExpServerBaseInformation));}

      private:
        common::ExpServerBaseInformation base_info_;
    };

    class RspRtsEsHeartMessage: public common::BasePacket
    {
      public:
        RspRtsEsHeartMessage();
        virtual ~RspRtsEsHeartMessage();

        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;
        inline int32_t get_time(void) { return heart_interval_;}
        inline void set_time(const int32_t heart_interval){ heart_interval_ = heart_interval;}

      private:
        int32_t heart_interval_;
    };

    class ReqQueryTaskMessage: public common::BasePacket
    {
      public:
        ReqQueryTaskMessage();
        virtual ~ReqQueryTaskMessage();
        virtual int serialize(common::Stream &output) const;
        virtual int deserialize(common::Stream &input);
        virtual int64_t length() const;

        inline uint64_t get_es_id() const {return es_id_;}
        inline void set_es_id(const uint64_t es_id) {es_id_ = es_id;}

      private:
        uint64_t es_id_;
    };

    class RspQueryTaskMessage: public common::BasePacket
    {
      public:
        RspQueryTaskMessage();
        virtual ~RspQueryTaskMessage();
        virtual int serialize(common::Stream &output) const;
        virtual int deserialize(common::Stream &input);
        virtual int64_t length() const;

        const std::vector<common::ServerExpireTask>* get_running_tasks_info() const
        {
          return &res_running_tasks_;
        }

        std::vector<common::ServerExpireTask>* get_mutable_running_tasks_info()
        {
          return &res_running_tasks_;
        }

      private:
        std::vector<common::ServerExpireTask> res_running_tasks_;
    };

  }
}
#endif
