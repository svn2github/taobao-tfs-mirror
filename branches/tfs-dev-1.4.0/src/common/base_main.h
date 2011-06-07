/*
 * (C) 2007-2010 Taobao Inc.
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
 *
 */

#ifndef TFS_COMMON_BASE_MAIN_H
#define TFS_COMMON_BASE_MAIN_H
#include <string>
#include <Mutex.h>
#include <Monitor.h>
namespace tfs
{
  namespace common
  {
    class BaseMain
    {
      public:
        BaseMain();
        virtual ~BaseMain();

        int main(int argc, char* argv[]);

        static BaseMain* instance();    
        //bool service() const;

        int handleInterrupt(int sig);
      protected:
        void stop();
        int shutdown();
        virtual int run( int argc , char*argv[], std::string& errmsg)=0;
        virtual bool destroy()=0;
        virtual void help();
        virtual void version();

        bool stop_;
        std::string config_file_;

      private:
        int waitForShutdown();
        int start(int argc , char* argv[], const bool deamon);
      private:
        tbutil::Monitor<tbutil::Mutex> monitor_;
        static BaseMain* _instance;
    };
  }
}
#endif
