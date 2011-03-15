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
#ifndef TFS_NAMESERVER_PROACTOR_DATA_PIPE_H_
#define TFS_NAMESERVER_PROACTOR_DATA_PIPE_H_

#include <tbsys.h> 

namespace tfs
{
  namespace nameserver
  {
    template<typename Container, typename Executor>
    class ProactorDataPipe: public tbsys::CDefaultRunnable
    {
    public:
      typedef typename Container::value_type value_type;

    public:
      ProactorDataPipe();

      ProactorDataPipe(Executor* executor, int32_t thread_count, void *args);

      virtual ~ProactorDataPipe()
      {
        stop();
      }

      void set_thread_parameter(Executor* executor, int32_t thread_count, void *args);

      void stop(bool wait_for_finish = false);

      /**
       * @param data, process message..
       * @param max_queue_length, max message can be wait
       * @param blocking, if number of waiting message beyond max_queue_length,
       * shoudle be waiting for push util proeccesing finish.
       */
      bool push(const value_type & data, const int64_t max_queue_length = 0, bool blocking = false);

      void run(tbsys::CThread *thread, void *arg);

      void set_stat_speed();

      void set_wait_time(time_t time);

    private:
      Container container_;
      Executor *executor_;
      tbsys::CThreadCond cond_;
      void *args_;
      bool speed_;
      bool wait_for_finish_;
      time_t wait_time_;
      time_t speed_t1_;
      time_t speed_t2_;
      int64_t overage_;
    public:
      bool executing_;
    };

    template<typename Container, typename Executor>
    ProactorDataPipe<Container, Executor>::ProactorDataPipe() :
      tbsys::CDefaultRunnable(), executor_(NULL), args_(NULL), speed_(false), wait_for_finish_(false), wait_time_(0),
          speed_t1_(0), speed_t2_(0), overage_(0), executing_(false)
    {
      _stop = false;
    }

    template<typename Container, typename Executor>
    ProactorDataPipe<Container, Executor>::ProactorDataPipe(Executor* executor, int32_t thread_count, void *args) :
      tbsys::CDefaultRunnable(thread_count), executor_(executor), args_(NULL), speed_(false), wait_for_finish_(false),
          wait_time_(0), speed_t1_(0), speed_t2_(0), overage_(0), executing_(false)
    {

    }

    template<typename Container, typename Executor>
    void ProactorDataPipe<Container, Executor>::set_thread_parameter(Executor* executor, const int32_t thread_count,
        void *args)
    {
      setThreadCount(thread_count);
      executor_ = executor;
      args_ = args;
    }

    template<typename Container, typename Executor>
    void ProactorDataPipe<Container, Executor>::stop(bool wait_for_finish)
    {
      cond_.lock();
      _stop = true;
      wait_for_finish_ = wait_for_finish;
      cond_.broadcast();
      cond_.unlock();
    }

    template<typename Container, typename Executor>
    bool ProactorDataPipe<Container, Executor>::push(const value_type & data, const int64_t max_queue_length,
        bool blocking)
    {
      if (_stop)
      {
        return false;
      }
      cond_.lock();
      if (max_queue_length > 0)
      {
        while (_stop == false && static_cast<int64_t> (container_.size()) >= max_queue_length && blocking)
        {
          cond_.wait();
        }
        if (_stop)
        {
          cond_.unlock();
          return false;
        }
        if (static_cast<int64_t> (container_.size()) >= max_queue_length && !blocking)
        {
          cond_.unlock();
          return false;
        }
      }
      container_.push_back(data);
      cond_.unlock();
      cond_.signal();
      return true;
    }

    template<typename Container, typename Executor>
    void ProactorDataPipe<Container, Executor>::run(tbsys::CThread *, void *)
    {
      int32_t total_count = 0;
      int64_t start_time;
      int64_t end_time;
      start_time = tbsys::CTimeUtil::getTime();
      end_time = start_time;
      speed_t1_ = start_time;
      speed_t2_ = start_time;
      overage_ = 0;

      while (!_stop)
      {
        cond_.lock();
        while (!_stop && container_.size() == 0)
        {
          cond_.wait();
        }
        if (_stop)
        {
          cond_.unlock();
          break;
        }
        if (wait_time_ > 0)
        {
          if (wait_time_ > overage_)
          {
            usleep(wait_time_ - overage_);
          }
          speed_t2_ = tbsys::CTimeUtil::getTime();
          overage_ += (speed_t2_ - speed_t1_) - wait_time_;
          if (overage_ > (wait_time_ << 4))
            overage_ = 0;
          speed_t1_ = speed_t2_;
        }
        value_type data = container_.front();
        container_.pop_front();
        int32_t curr_count = ++total_count;
        executing_ = 1;
        cond_.unlock();
        cond_.signal();

        if (executor_)
        {
          executor_->execute(data, args_);
        }

        cond_.lock();
        executing_ = false;
        cond_.unlock();

        if (speed_ && curr_count % 1000 == 0)
        {
          int64_t end_time = tbsys::CTimeUtil::getTime();
          TBSYS_LOG(INFO, "task execute speed(%"PRI64_PREFIX"d) task/s", (1000000000 / (end_time - start_time)));
          start_time = end_time;
        }
      }

      if (wait_for_finish_)
      {
        cond_.lock();
        while (container_.size() > 0)
        {
          value_type data = container_.front();
          container_.pop_front();
          cond_.unlock();

          if (executor_)
          {
            executor_->execute(data, args_);
          }

          cond_.lock();
        }
        cond_.unlock();
      }
      else
      {
        /*
         cond_.lock();
         while (container_.size() > 0)
         {
         delete container_.pop();
         }
         cond_.unlock();
         */
      }
    }

    template<typename Container, typename Executor>
    void ProactorDataPipe<Container, Executor>::set_stat_speed()
    {
      speed_ = true;
    }

    template<typename Container, typename Executor>
    void ProactorDataPipe<Container, Executor>::set_wait_time(time_t time)
    {
      wait_time_ = time;
      speed_ = false;
    }
  }
}
#endif

