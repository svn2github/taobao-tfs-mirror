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

#include "base_main.h"

#include <tbsys.h>
#include "config_item.h"

using namespace std;

namespace tfs 
{
  namespace common
  {
    BaseMain* BaseMain::_instance=NULL;

    static void ctrlCHandlerCallback( int sig )
    {
      BaseMain* service = BaseMain::instance();
      assert( service != 0 );
      service->handleInterrupt( sig );
    }

    BaseMain::BaseMain():stop_(false)
    {
      assert(_instance == NULL );
      _instance = this;
    }

    BaseMain::~BaseMain()
    {
      _instance = NULL;
    }

    int BaseMain::main(int argc,char*argv[])
    {
      bool daemonize(false);
      int idx(1);
      if (argc < 2 )
      {
        cerr<<":invalid option\n"<<"Try `--help' for more information"<<endl;
        return EXIT_FAILURE;
      }
      while(idx < argc )
      {
        if(strcmp(argv[idx],"-h") == 0 || strcmp(argv[idx],"--help") == 0)
        {
          help();//show help
          return EXIT_SUCCESS;
        }
        else if (strcmp(argv[idx], "-v" ) == 0 || strcmp(argv[idx], "--version" ) == 0)
        {
          version();//show version
          return EXIT_SUCCESS;
        }
        else if(strcmp(argv[idx], "-d")== 0)
        {
          for(int i = idx; i + 1 < argc; ++i)
          {
            argv[i] = argv[i + 1];
          }
          argc -= 1;
          daemonize = true;
        }
        else if(strcmp(argv[idx],"-f" ) == 0)
        {
          if(idx + 1 < argc)
          {
            config_file_=argv[idx + 1];
          }
          else
          {
            cerr<<":--config|-f must be followed by an argument...."<<endl;
            return EXIT_FAILURE;
          }
          for(int i = idx ; i + 2 < argc; ++i)
          {
            argv[i] = argv[i + 2];
          }
          argc -= 2;
          if (config_file_.empty())
          {
            cerr<<":--config|-f must be followed an argument,argument is not null"<<endl;
            return EXIT_FAILURE;
          }
        }
        else
        {
          ++idx;
        }//end if
      }//end while idx<argc

      return start(argc , argv, daemonize);
    }

    BaseMain* BaseMain::instance()
    {
      return _instance;
    }

    //bool BaseMain::service() const
    //{
    //    return _service;
    //}
    //
    int BaseMain::start(int argc , char* argv[], const bool daemon)
    {
      if (EXIT_FAILURE == TBSYS_CONFIG.load(config_file_.c_str()))
      {
        cerr << "load config error config file is " << config_file_ << endl;
        return EXIT_FAILURE;
      }

      const char* sz_pid_file =
        TBSYS_CONFIG.getString(CONF_SN_PUBLIC, CONF_PID_FILE, NULL);
      const char* sz_log_file =
        TBSYS_CONFIG.getString(CONF_SN_PUBLIC, CONF_LOG_FILE, NULL);

      string pid_file;
      string log_file;
      if (NULL == sz_pid_file)
      {
        pid_file = argv[0];
        pid_file += ".pid";
      }
      else
      {
        pid_file = sz_pid_file;

      }

      if (NULL == sz_log_file)
      {
        log_file = argv[0];
        log_file += ".log";
      }
      else
      {
        log_file = sz_log_file;
      }
      {
        // make pid file dir log file dir
        char *p = NULL;
        char dir_path[256];
        snprintf(dir_path, 256, "%s", pid_file.c_str());
        p = strrchr(dir_path, '/');
        if(p != NULL)
        {
          *p = '\0';
        }
        if(p != NULL && !tbsys::CFileUtil::mkdirs(dir_path)) {
          fprintf(stderr, "create dir %s error\n", dir_path);
          return EXIT_FAILURE;
        }
        snprintf(dir_path, 256, "%s", log_file.c_str());
        p = strrchr(dir_path, '/');
        if(p != NULL)
        {
          *p = '\0';
        }
        if(p != NULL && !tbsys::CFileUtil::mkdirs(dir_path)) {
          fprintf(stderr, "create dir %s error\n", dir_path);
          return EXIT_FAILURE;
        }
      }

      int pid = 0;
      if((pid = tbsys::CProcess::existPid(pid_file.c_str()))) {
        fprintf(stderr, "program has been exist: pid=%d\n", pid);
        return EXIT_FAILURE;
      }
      if (0 == access(log_file.c_str(), R_OK))
      {
        TBSYS_LOGGER.rotateLog(log_file.c_str());
      }
      TBSYS_LOGGER.setFileName(log_file.c_str());
      TBSYS_LOGGER.setLogLevel(
          TBSYS_CONFIG.getString(CONF_SN_PUBLIC, CONF_LOG_LEVEL, "debug"));
      TBSYS_LOGGER.setMaxFileSize(
          TBSYS_CONFIG.getInt(CONF_SN_PUBLIC, CONF_LOG_SIZE, 0x40000000));
      TBSYS_LOGGER.setMaxFileIndex(
          TBSYS_CONFIG.getInt(CONF_SN_PUBLIC, CONF_LOG_NUM, 16));
      bool start_ok = true;
      if (daemon) 
      {
        start_ok = (tbsys::CProcess::startDaemon(pid_file.c_str(), log_file.c_str()) == 0);
      }
      if (start_ok)
      {
        signal(SIGHUP, ctrlCHandlerCallback);
        signal(SIGTERM, ctrlCHandlerCallback);
        signal(SIGINT, ctrlCHandlerCallback);
        signal(40, ctrlCHandlerCallback);
        signal(41, ctrlCHandlerCallback);
        signal(42, ctrlCHandlerCallback);
        string errMsg;
        run(argc , argv, errMsg);
        waitForShutdown();
        destroy();
      }
      else
      {
        cerr << "start daemon error\n";
      }
      return EXIT_SUCCESS;
    }

    void BaseMain::stop()
    {
      shutdown();
    }

    int BaseMain::shutdown()
    {
      tbutil::Monitor<tbutil::Mutex>::Lock sync(monitor_);
      if ( !stop_ )
      {
        stop_= true;
        monitor_.notifyAll();
      }
      return EXIT_SUCCESS;
    }

    int BaseMain::waitForShutdown()
    {
      tbutil::Monitor<tbutil::Mutex>::Lock sync(monitor_);
      while( !stop_ )
      {
        monitor_.wait();
      }
      return EXIT_SUCCESS;
    }

    int BaseMain::handleInterrupt(int sig)
    {
      switch (sig) 
      {
        case SIGHUP:
          break;
        case SIGTERM:
        case SIGINT:
            stop();
            break;
        case 40:
            TBSYS_LOGGER.checkFile();
            break;
        case 41:
        case 42:
            if(sig == 41) 
            {
              TBSYS_LOGGER._level++;
            }
            else 
            {
              TBSYS_LOGGER._level--;
            }
            TBSYS_LOG(INFO, "TBSYS_LOGGER._level: %d", TBSYS_LOGGER._level);
            break;
      }
      return EXIT_SUCCESS;
    }

    void BaseMain::help()
    {
      std::string options=
        "Options:\n"
        "-h,--help          Show this message...\n"
        "-v,--version       Show porgram version...\n"
        "-d                 Run as a daemon...\n"
        "-f FILE            Configure files...\n";

        cerr << "Usage:\n" << options;
    }

    void BaseMain::version()
    {
      cerr << "Version:1.0.0\n";
      cerr << "BUILD_TIME " << __DATE__ << __TIME__ << endl;
    }

  }
}

