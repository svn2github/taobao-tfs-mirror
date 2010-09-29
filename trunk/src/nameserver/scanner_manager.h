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
#ifndef TFS_NAMESERVER_SCANNER_MANAGER_H__
#define TFS_NAMESERVER_SCANNER_MANAGER_H__

#include "meta_scanner.h"
#include "meta_manager.h"

namespace tfs
{
  namespace nameserver
  {
    class Launcher
    {
    public:
      virtual ~Launcher()
      {
      }
      virtual bool check(const BlockCollect* blkcol) = 0;
      virtual int build_plan(const common::VUINT32& blocks) = 0;
    };

    class ScannerManager: public MetaScanner
    {
    public:
      struct Scanner
      {
      public:
        Launcher& launcher_;
        common::VUINT32& result_;
        int32_t limits_;
        bool is_check_;
        Scanner(bool is_check, int32_t limits, Launcher& launcher, common::VUINT32& result) :
          launcher_(launcher), result_(result), limits_(limits), is_check_(is_check)
        {

        }
      private:
        Scanner();
        DISALLOW_COPY_AND_ASSIGN( Scanner);
      };
    public:

      ScannerManager(MetaManager & m);
      virtual ~ScannerManager();

      virtual int process(const BlockCollect* blkcol) const;

      bool add_scanner(const int32_t id, Scanner* scanner);
      inline void destroy()
      {
        destroy_ = true;
      }
      int run();
    private:
      DISALLOW_COPY_AND_ASSIGN( ScannerManager);
      MetaManager& meta_mgr_;
      mutable std::map<int32_t, Scanner*> scanners_;
      bool destroy_;
    };
  }
}
#endif 
