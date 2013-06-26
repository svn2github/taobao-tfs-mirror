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

namespace tfs
{
  namespace tools
  {
    class BaseWorker: public tbutil::Thread
    {
      public:
        BaseWorker():
          cluster_id_(0), timestamp_(0), interval_ms_(0),
          succ_fp_(NULL), fail_fp_(NULL), stop_(false)
        {
        }

        virtual ~BaseWorker()
        {
        }

        void destroy()
        {
          stop_ = true;
        }

        void reload(const int flag)
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
          TBSYS_LOG(INFO, "set interval to %d ms", interval_ms_);
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
        uint32_t get_timestamp() const
        {
          return timestamp_;
        }

        void set_timestamp(const uint32_t timestamp)
        {
          timestamp_ = timestamp;
        }

        // argument passed by -c
        uint32_t get_cluster_id() const
        {
          return cluster_id_;
        }

        void set_cluster_id(const uint32_t cluster_id)
        {
          cluster_id_ = cluster_id;
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
          input_.push_back(line);
        }

        virtual int process(std::string& line)
        {
          UNUSED(line);
          return common::TFS_SUCCESS;
        }

        void run()
        {
          std::vector<std::string>::iterator iter = input_.begin();
          for ( ; iter != input_.end() && !stop_; iter++)
          {
            int ret = process(*iter);
            if (common::TFS_SUCCESS == ret)
            {
              fprintf(succ_fp_, "%s\n", iter->c_str());
            }
            else
            {
              fprintf(fail_fp_, "%s\n", iter->c_str());
            }
            usleep(interval_ms_ * 1000);
          }
        }

      protected:
        std::vector<std::string> input_;
        std::string src_addr_;
        std::string dest_addr_;
        std::string extra_arg_;
        int32_t cluster_id_;
        uint32_t timestamp_;
        int32_t interval_ms_;
        FILE* succ_fp_;
        FILE* fail_fp_;
        bool stop_;
    };
    typedef BaseWorker* BaseWorkerPtr;

    class BaseWorkerManager
    {
      public:
        BaseWorkerManager();
        virtual ~BaseWorkerManager();
        virtual void begin() {}
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

        // argument passed by -c
        uint32_t get_cluster_id() const
        {
          return cluster_id_;
        }

        // argument passed by -m
        uint32_t get_timestamp() const
        {
          return timestamp_;
        }

        // argument passed by -i
        int32_t get_interval_ms() const
        {
          return interval_ms_;
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
        int32_t cluster_id_;
        uint32_t timestamp_;
        int32_t interval_ms_;
        FILE* succ_fp_;
        FILE* fail_fp_;

      protected:
        void usage(const char* app_name);
        void version(const char* app_name);
        static void handle_signal(const int signal);
    };
  }
}

#endif
