/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 */

#ifndef TFS_TOOLS_UTIL_BASE_WORKER_H_
#define TFS_TOOLS_UTIL_BASE_WORKER_H_

#include <stdio.h>
#include <vector>
#include <algorithm>
#include <TbThread.h>
#include "common/internal.h"

namespace tfs
{
  namespace tools
  {
    class BaseWorker: public tbutil::Thread
    {
      public:
        BaseWorker():
          timestamp_(0), interval_ms_(0),
          succ_fp_(NULL), fail_fp_(NULL), stop_(false)
        {
        }

        virtual ~BaseWorker()
        {
        }

        virtual void destroy()
        {
          stop_ = true;
        }

        virtual void reload(const int flag)
        {
          if (flag == 1)
          {
            interval_ms_ += 1000;
          }
          else if (flag == -1)
          {
            if (interval_ms_ >= 1000)
            {
              interval_ms_ -= 1000;
            }
          }
          TBSYS_LOG(INFO, "set interval to %d ms, flag: %d", interval_ms_, flag);
        }

        // argument passed by -s
        std::string get_src_addr() const
        {
          return src_addr_;
        }

        void set_src_addr(const std::string& src_addr)
        {
          src_addr_ = src_addr;
        }

        // argument passed by -d
        std::string get_dest_addr() const
        {
          return dest_addr_;
        }

        void set_dest_addr(const std::string& dest_addr)
        {
          dest_addr_ = dest_addr;
        }

        // argment passed by -x
        std::string get_extra_arg() const
        {
          return extra_arg_;
        }

        void set_extra_arg(const std::string& extra_arg)
        {
          extra_arg_ = extra_arg;
        }

        // argument passed by -m
        int32_t get_timestamp() const
        {
          return timestamp_;
        }

        void set_timestamp(const int32_t timestamp)
        {
          timestamp_ = timestamp;
        }

        // argument passed by -i
        int32_t get_interval_ms() const
        {
          return interval_ms_;
        }

        void set_interval_ms(const int32_t interval_ms)
        {
          interval_ms_ = interval_ms;
        }

        // argument passed by -c
        int32_t get_retry_count() const
        {
          return retry_count_;
        }

        void set_retry_count(const int32_t retry_count)
        {
          retry_count_ = retry_count;
        }

        FILE* get_succ_fp()
        {
          return succ_fp_;
        }

        void set_succ_fp(FILE* succ_fp)
        {
          succ_fp_ = succ_fp;
        }

        FILE* get_fail_fp()
        {
          return fail_fp_;
        }

        void set_fail_fp(FILE* fail_fp)
        {
          fail_fp_ = fail_fp;
        }

        void add(const std::string& line)
        {
          input_.push(std::make_pair(line, 0));
        }

        virtual int process(std::string& line)
        {
          UNUSED(line);
          return common::TFS_SUCCESS;
        }

        void run()
        {
          while (!input_.empty() && !stop_)
          {
            QueueItem item = input_.front();
            input_.pop();
            int ret = process(item.first);
            item.second++;
            if (common::TFS_SUCCESS == ret)
            {
              fprintf(succ_fp_, "%s\n", item.first.c_str());
            }
            else
            {
              if (item.second >= retry_count_)
              {
                fprintf(fail_fp_, "%s\n", item.first.c_str());
              }
              else
              {
                input_.push(item);
                TBSYS_LOG(DEBUG, "line %s fail, will retry", item.first.c_str());
              }
            }
            usleep(interval_ms_ * 1000);
          }
        }

      protected:
        typedef std::pair<std::string, int32_t> QueueItem;
        std::queue<QueueItem> input_;
        std::string src_addr_;
        std::string dest_addr_;
        std::string extra_arg_;
        int32_t retry_count_;
        int32_t timestamp_;
        int32_t interval_ms_;
        FILE* succ_fp_;
        FILE* fail_fp_;
        bool stop_;
    };

    typedef tbutil::Handle<BaseWorker> BaseWorkerPtr;

    class BaseWorkerManager
    {
      public:
        BaseWorkerManager();
        virtual ~BaseWorkerManager();
        virtual int begin(){ return common::TFS_SUCCESS; }
        virtual void end() {}
        virtual BaseWorker* create_worker() = 0;
        int main(int argc, char* argv[]);

        // argument passed by -s
        std::string get_src_addr() const
        {
          return src_addr_;
        }

        // argument passed by -d
        std::string get_dest_addr() const
        {
          return dest_addr_;
        }

        // argument passed by -o
        std::string get_output_dir() const
        {
          return output_dir_;
        }

        // argment passed by -x
        std::string get_extra_arg() const
        {
          return extra_arg_;
        }

        // argment passed by -c
        int32_t get_retry_count() const
        {
          return retry_count_;
        }

        // argument passed by -m
        int32_t get_timestamp() const
        {
          return timestamp_;
        }

        // argument passed by -i
        int32_t get_interval_ms() const
        {
          return interval_ms_;
        }

        int32_t get_type() const
        {
          return type_;
        }

        bool get_force() const
        {
          return force_;
        }

        const std::string& get_dest_addr_path() const
        {
          return dest_addr_path_;
        }

        FILE* get_succ_fp()
        {
          return succ_fp_;
        }

        FILE* get_fail_fp()
        {
          return fail_fp_;
        }

      protected:
        std::string src_addr_;
        std::string dest_addr_;
        std::string input_file_;
        std::string output_dir_;
        std::string extra_arg_;
        std::string log_level_;
        std::string dest_addr_path_;
        int32_t retry_count_;
        int32_t timestamp_;
        int32_t interval_ms_;
        int32_t type_;
        bool force_;
        FILE* succ_fp_;
        FILE* fail_fp_;

      protected:
        virtual void usage(const char* app_name);
        void version(const char* app_name);
        static void handle_signal(const int signal);
    };
  }
}

#endif
