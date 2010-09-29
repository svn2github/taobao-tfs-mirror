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
#include "common/interval.h"
#include "scanner_manager.h"

using namespace tfs::common;

namespace tfs
{
  namespace nameserver
  {

    ScannerManager::ScannerManager(MetaManager & m) :
      meta_mgr_(m), destroy_(false)
    {

    }

    ScannerManager::~ScannerManager()
    {

    }

    bool ScannerManager::add_scanner(const int32_t id, Scanner* scanner)
    {
      std::map<int32_t, Scanner*>::iterator iter = scanners_.find(id);
      if (iter == scanners_.end())
      {
        scanners_.insert(make_pair(id, scanner));
        return true;
      }
      iter->second = scanner;
      return true;
    }

    int ScannerManager::run()
    {
      if (destroy_)
      {
        return TFS_SUCCESS;
      }
      meta_mgr_.get_block_ds_mgr().foreach(*this);
      std::map<int32_t, Scanner*>::iterator iter = scanners_.begin();
      for (; iter != scanners_.end(); ++iter)
      {
        Scanner* scanner = iter->second;
        if (scanner == NULL)
        {
          continue;
        }

        if (scanner->result_.size() > 0)
        {
          scanner->launcher_.build_plan(scanner->result_);
        }
        if (destroy_)
        {
          return MetaScanner::BREAK;
        }
      }
      return TFS_SUCCESS;
    }

    int ScannerManager::process(const BlockCollect* block_collect) const
    {
      if (destroy_)
      {
        return MetaScanner::BREAK;
      }

      std::map<int32_t, Scanner*>::iterator it = scanners_.begin();
      for (; it != scanners_.end(); ++it)
      {
        Scanner * scanner = it->second;
        if (scanner == NULL)
        {
          continue;
        }

        if (scanner->is_check_)
        {
          if (static_cast<int32_t> (scanner->result_.size()) > scanner->limits_)
          {
            TBSYS_LOG(DEBUG, "result size(%u) > limits(%d)", scanner->result_.size(), scanner->limits_);
            continue;
          }
          bool bret = scanner->launcher_.check(block_collect);
          if (bret)
          {
            scanner->result_.push_back(block_collect->get_block_info()->block_id_);
          }
        }

        if (destroy_)
        {
          return MetaScanner::BREAK;
        }
      }
      return MetaScanner::CONTINUE;
    }
  }
}
