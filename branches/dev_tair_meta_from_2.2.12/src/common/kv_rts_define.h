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
 *   qixiao.zs <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */

#ifndef TFS_COMMON_KV_ROOT_SERVER_DEFINE_H_
#define TFS_COMMON_KV_ROOT_SERVER_DEFINE_H_

#include <map>
#include <ext/hash_map>
#include <Time.h>

#include "internal.h"
#include "lock.h"

#ifndef INT64_MAX
#define INT64_MAX 0x7fffffffffffffffLL
#endif

namespace tfs
{
  namespace common
  {
    struct KVRSLease
    {
      bool has_valid_lease(const int64_t now) ;
      bool renew(const int32_t step, const int64_t now);
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      uint64_t lease_id_; /** lease id **/
      int64_t  lease_expired_time_; /** lease expire time (ms) */
    };


    struct KvMetaServerBaseInformation
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      uint64_t id_; /** MetaServer id(IP + PORT) **/
      int64_t  start_time_; /** MetaServer start time (ms)**/
      int64_t  last_update_time_;/** MetaServer last update time (ms)**/
    };

    struct KvMetaServer
    {
      int deserialize(const char* data, const int64_t data_len, int64_t& pos);
      int serialize(char* data, const int64_t data_len, int64_t& pos) const;
      int64_t length() const;
      KVRSLease lease_;
      KvMetaServerBaseInformation base_info_;
    };


    typedef std::map<uint64_t, KvMetaServer> KV_META_SERVER_MAPS;
    typedef KV_META_SERVER_MAPS::iterator KV_META_SERVER_MAPS_ITER;
    typedef KV_META_SERVER_MAPS::const_iterator KV_META_SERVER_MAPS_CONST_ITER;

    struct KvMetaTable
    {
      KvMetaTable();

      int64_t length() const;
      int serialize(char *data, const int64_t data_len, int64_t &pos) const;
      int deserialize(const char *data, const int64_t data_len, int64_t &pos);

      common::VUINT64 v_meta_table_;

      void dump();
    };

  } /** common **/
} /** tfs **/

#endif
