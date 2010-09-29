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
#ifndef TFS_NAMESERVER_META_SCANNER_H_
#define TFS_NAMESERVER_META_SCANNER_H_

#include "block_collect.h"

namespace tfs
{
  namespace nameserver
  {
    class MetaScanner
    {
    public:
      enum SCANNER_STATUS
      {
        CONTINUE = 0x00,
        BREAK
      };
      virtual int process(const BlockCollect* blkcol) const = 0;
      virtual ~MetaScanner()
      {

      }
    };
  }
}
#endif 

