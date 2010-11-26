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
#ifndef TFS_NAMESERVER_DATA_CONTAINER_H__
#define TFS_NAMESERVER_DATA_CONTAINER_H__

#include <vector>

namespace tfs
{
  namespace nameserver
  {
    template<typename ElementT>
    class PipeDataAdaptor
    {
    public:
      virtual ~PipeDataAdaptor()
      {
      }
      ;
      typedef std::vector<ElementT> value_type;

      void push_back(const value_type & v)
      {
        inventory_.assign(v.begin(), v.end());
      }

      const value_type front() const
      {
        return inventory_;
      }

      void pop_front()
      {
        inventory_.clear();
      }

      uint32_t size() const
      {
        if (inventory_.size())
          return 1;
        else
          return 0;
      }
    private:
      value_type inventory_;
    };
  }
}
#endif 

