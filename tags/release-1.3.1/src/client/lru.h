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
 *
 */
#ifndef TFS_CLIENT_LRU_H_
#define TFS_CLIENT_LRU_H_

#include <list>
#include <ext/hash_map>
#include "common/define.h"

namespace tfs
{
  namespace client
  {
    template<class T1, class T2>
    class lru
    {
    public:
      typedef std::list<std::pair<T1, T2> > List;
      typedef typename List::iterator iterator;
      typedef __gnu_cxx ::hash_map<T1, iterator> Map;

      lru()
      {
        size_ = 1000;
        index_.resize(size_);
        list_.resize(size_);
      }

      ~lru()
      {
        clear();
      }

      void resize(int32_t size)
      {
        size_ = size;
        index_.resize(size_);
        list_.resize(size_);
      }

      T2 *find(const T1 &first)
      {
        typename Map::iterator i = index_.find(first);

        if (i == index_.end())
        {
          return NULL;
        }
        else
        {
          typename List::iterator n = i->second;
          list_.splice(list_.begin(), list_, n);
          return &(list_.front().second);
        }
      }

      void insert(const T1 &first, const T2 &second)
      {
        typename Map::iterator i = index_.find(first);
        if (i != index_.end())
        { // found
          typename List::iterator n = i->second;
          list_.splice(list_.begin(), list_, n);
          index_.erase(n->first);
          n->first = first;
          n->second = second;
          index_[first] = n;
        }
        else if ((int) list_.size() >= size_)
        { // erase the last element
          typename List::iterator n = list_.end();
          --n; // the last element
          list_.splice(list_.begin(), list_, n);
          index_.erase(n->first);
          n->first = first;
          n->second = second;
          index_[first] = n;
        }
        else
        {
          list_.push_front(make_pair(first, second));
          typename List::iterator n = list_.begin();
          index_[first] = n;
        }
      }

      /// Random access to items
      iterator begin()
      {
        return list_.begin();
      }

      iterator end()
      {
        return list_.end();
      }

      int size()
      {
        return index_.size();
      }

      /// Clear cache
      void clear()
      {
        index_.clear();
        list_.clear();
      }

    private:
      int32_t size_;
      List list_;
      Map index_;
    };

  }

}
#endif

