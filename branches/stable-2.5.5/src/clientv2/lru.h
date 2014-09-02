/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENTV2_LRU_H_
#define TFS_CLIENTV2_LRU_H_

#include <list>
#include <ext/hash_map>
#include "client_config.h"
#include "common/internal.h"

namespace tfs
{
  namespace clientv2
  {
    template<class T1, class T2>
    class Lru
    {
    public:
      typedef std::list<std::pair<T1, T2> > List;
      typedef typename List::iterator iterator;
      typedef __gnu_cxx::hash_map<T1, iterator> Map;

      Lru()
      {
        size_ = 1000;
        resize(size_);
      }

      ~Lru()
      {
        clear();
      }

      void resize(int32_t size)
      {
        if (size > 0)
        {
          size_ = size;
        }
      }

      T2* find(const T1& first)
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

      void remove(const T1& first)
      {
        typename Map::iterator i = index_.find(first);
        if (i != index_.end())
        {
          typename List::iterator n = i->second;
          list_.erase(n);
          index_.erase(i);
        }
      }

      void insert(const T1& first, const T2& second)
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
        else if (size() >= size_)
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
          list_.push_front(std::make_pair(first, second));
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

      // Clear cache
      void clear()
      {
        index_.clear();
        list_.clear();
      }

#ifdef TFS_TEST
    public:
#else
    private:
#endif
      int32_t size_;
      List list_;
      Map index_;
    };
  }
}
#endif

