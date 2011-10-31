/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: lru.h 5 2011-09-06 16:00:56Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_NAMEMETASERVER_LRU_H_
#define TFS_NAMEMETASERVER_LRU_H_

#include <list>
#include <ext/hash_map>
#include "common/internal.h"
#include "common/error_msg.h"
namespace tfs
{
  namespace namemetaserver
  {
    template<typename Key, typename Value>
    class BaseStrategy;
    template<typename Key, typename Value>
    class Lru
    {
#ifndef GTEST_INCLUDE_GTEST_GTEST_H_
#else
      friend class LruTest;
      FRIEND_TEST(LruTest, insert);
      FRIEND_TEST(LruTest, get);
      FRIEND_TEST(LruTest, put);
      FRIEND_TEST(LruTest, gc);
#endif

      template <typename T3>
      struct Node;

      typedef std::list<Node<Value> > LRU_LIST;
      typedef typename LRU_LIST::iterator LRU_LIST_ITERATOR;

      //typedef __gnu_cxx::hash_map<Key, LRU_LIST_ITERATOR> LRU_MAP;
      typedef std::map<Key, LRU_LIST_ITERATOR> LRU_MAP;
      typedef typename LRU_MAP::iterator LRU_MAP_ITERATOR;

      template <typename V>
      struct Node
      {
        void inc_ref() { ++ref_count_;}
        void dec_ref() { --ref_count_;}
        void inc_visit_count() { ++visit_count_;}
        V* value_;
        int64_t ref_count_;
        int64_t visit_count_;
        LRU_MAP_ITERATOR iter_;
      };

      template<typename T, typename T2>
      friend class BaseStrategy;
    public:
      Lru(){}
      virtual ~Lru() {}
      Value* get(const Key& key);
      int put(const Key& key);
      int insert(const Key& key, Value* value);
      int gc(const double ratio, BaseStrategy<Key,Value>* st, std::vector<Value*>& values);
      int gc(std::vector<Value*>& values);
      int64_t size() { return index_.size();}
    private:
      Node<Value>* get_node(const Key& key);
      LRU_LIST list_;
      LRU_MAP  index_;
      DISALLOW_COPY_AND_ASSIGN(Lru);
    };
    
    template<typename Key, typename Value>
    class BaseStrategy
    {
    public:
      explicit BaseStrategy(Lru<Key, Value>& lru): lru_(lru){}
      virtual ~BaseStrategy(){}
      virtual int gc(const double ratio, std::vector<Value*>& values);
    private:
      int64_t calc_gc_count(const double ratio);
      Lru<Key, Value>& lru_;
    };

    template <typename Key, typename Value>
    Value* Lru<Key, Value>::get(const Key& key)
    {
      Value* value = NULL;
      LRU_MAP_ITERATOR iter = index_.find(key);
      if (index_.end() != iter)
      {
        list_.splice(list_.end(), list_, iter->second);
        iter->second->inc_visit_count();
        iter->second->inc_ref();
        TBSYS_LOG(DEBUG, "ref_count_ = %l"PRI64_PREFIX"d", iter->second->ref_count_);
        value = iter->second->value_;
      }
      return value;
    }
    
    template<typename Key, typename Value>
    int Lru<Key, Value>::put(const Key& key)
    {
      LRU_MAP_ITERATOR iter = index_.find(key);
      int32_t iret = index_.end() == iter ? common::EXIT_LRU_VALUE_NOT_EXIST : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == iret)
      {
        iter->second->dec_ref();
        TBSYS_LOG(DEBUG, "ref_count_ = %l"PRI64_PREFIX"d", iter->second->ref_count_);
      }
      return iret;
    }

    template<typename Key, typename Value>
    int Lru<Key, Value>::insert(const Key& key, Value* value)
    {
      LRU_MAP_ITERATOR iter = index_.find(key);
      int32_t iret = index_.end() != iter ? common::EXIT_LRU_VALUE_EXIST : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == iret)
      {
        LRU_LIST_ITERATOR it = list_.insert(list_.end(), Node<Value>());
        std::pair<LRU_MAP_ITERATOR, bool> res = index_.insert(std::pair<Key, LRU_LIST_ITERATOR>(key, it));
        iret = res.second ? common::TFS_SUCCESS : common::EXIT_LRU_VALUE_EXIST;
        it->value_ = value;
        it->ref_count_ = 0;
        it->visit_count_ = 0;
        it->iter_ = res.first;
      }
      return iret;
    }

    template<typename Key, typename Value>
    int Lru<Key, Value>::gc(const double ratio, BaseStrategy<Key,Value>* st, std::vector<Value*>& values)
    {
      int32_t iret = NULL == st ? common::TFS_ERROR : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == iret)
      {
        iret = st->gc(ratio, values); 
      }
      return iret;
    }

    template<typename Key, typename Value>
    int Lru<Key, Value>::gc(std::vector<Value*>& values)
    {
      LRU_LIST_ITERATOR iter = list_.begin();
      for (; iter != list_.end(); ++iter)
      {
        values.push_back(iter->value_);
      }
      list_.clear();
      index_.clear();
      return common::TFS_SUCCESS;
    }

    template<typename Key, typename Value>
    Lru<Key, Value>::Node<Value>* Lru<Key, Value>::get_node(const Key& key)
    {
      LRU_MAP_ITERATOR iter = index_.find(key);
      return index_.end() != iter ? (&(*iter->second)) : NULL;
    }

    template<typename Key, typename Value>
    int BaseStrategy<Key, Value>::gc(const double ratio, std::vector<Value*>& values)
    {
      int64_t begin = lru_.size() - calc_gc_count(ratio);
      int32_t iret = begin >= 0 ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        int64_t gc_count = calc_gc_count(ratio);
        int64_t count = 0;
        typename Lru<Key, Value>::LRU_LIST_ITERATOR iter = lru_.list_.begin();
        for(; iter != lru_.list_.end() && count < gc_count;)
        {
          if (0 == iter->ref_count_)
          {
            ++count;
            values.push_back(iter->value_);
            lru_.index_.erase(iter->iter_);
            lru_.list_.erase(iter++);
          }
          else
          {
            ++iter;
          }
        }
      }
      return iret;
    }

    template<typename Key, typename Value>
    int64_t BaseStrategy<Key, Value>::calc_gc_count(const double ratio)
    {
      return static_cast<int64_t>(lru_.size() * ratio) + 1;
    }
  }/** namemetaserver **/
}/** tfs **/
#endif

