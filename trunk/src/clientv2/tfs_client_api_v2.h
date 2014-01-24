/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENTV2_TFSCLIENTAPI_H_
#define TFS_CLIENTV2_TFSCLIENTAPI_H_

#include <stdio.h>
#include <pthread.h>

#include "common/define.h"

namespace tfs
{
  namespace clientv2
  {
    class TfsClientV2
    {
    public:
      static TfsClientV2* Instance()
      {
        static TfsClientV2 tfs_client_;
        return &tfs_client_;
      }

      /**
       * @brief initialize a tfs client object
       *
       * @param ns_addr: the cluster's ns address
       * @param cache_time: cache expire time(seconds)
       * @param cache_items: max cache items
       * @return TFS_SUCCESS on success, errno on fail.
       */
       int initialize(const char* ns_addr = NULL,
          const int32_t cache_time = common::DEFAULT_BLOCK_CACHE_TIME,
          const int32_t cache_items = common::DEFAULT_BLOCK_CACHE_ITEMS);

      /**
       * @brief destroy a tfs client object
       *
       * @return TFS_SUCCESS on success, errno on fail.
       */
      int destroy();

      /**
       * @brief get a tfs file's status
       *
       * @param file_stat: where to save file status
       * @param file_name: tfs file name
       * @param suffix: file suffix specified when save this file
       * @param ns_addr: nameserver address
       *
       * @return TFS_SUCCESS on success, errno on fail.
       */
      int stat_file(common::TfsFileStat* file_stat, const char* file_name, const char* suffix = NULL,
          const char* ns_addr = NULL);

      /**
       * @brief save local file to tfs
       *
       * @param ret_tfs_name: tfs file name for this saved file
       * @param ret_tfs_name_len: tfs file length
       * @param local_file: local file to save
       * @param mode: save mode
       * @param suffix: file suffix, can be NULL
       * @param ns_addr: nameserver address
       *
       * @return bytes saved on success, errno on fail.
       */
      int64_t save_file(char* ret_tfs_name, const int32_t ret_tfs_name_len,
          const char* local_file, const int32_t mode, const char* suffix = NULL,
          const char* ns_addr = NULL);

      /**
       * @brief update a tfs file
       *
       * @param local_file: the new content of file
       * @param mode: save mode
       * @param tfs_name: tfs file name
       * @param suffix: file suffix, can be NULL
       * @param ns_addr: nameserver address
       *
       * @return bytes saved on success, or errno on fail.
       */
      int64_t save_file_update(const char* local_file, const int32_t mode,
        const char* tfs_name, const char* suffix, const char* ns_addr = NULL);

      /**
       * @brief fetch a tfs file to local file
       *
       * @param local_file: local file name
       * @param file_name: tfs file name
       * @param suffix: file suffix specified when save this file
       * @param ns_addr: nameserver address
       *
       * @return  TFS_SUCCESS on success, errno on fail.
       */
      int fetch_file(const char* local_file, const char* file_name, const char* suffix = NULL,
          const char* ns_addr = NULL);

      /**
       * @brief unlink a tfs file
       *
       * @param file_size: the file size of unlinked file
       * @param file_name: tfs file name to unlink
       * @param suffix: file suffix specified when save this file
       * @param action: unlink action: 0-delete, 2-undelete, 4-hide, 6-unhide
       * @param ns_addr: nameserver address
       *
       * @return TFS_SUCCESS on success, errno on fail.
       */
      int unlink(int64_t& file_size, const char* file_name, const char* suffix = NULL,
          const common::TfsUnlinkType action = common::DELETE, const char* ns_addr = NULL);

      /**
       * @brief  get default ns addr, which passed in initialize
       *
       * @return default ns address
       */
      uint64_t get_server_id();

      /**
       * @brief get default cluster id, which passed in intialize
       *
       * @return default cluster id
       */
      int32_t get_cluster_id(const char* ns_addr = NULL);

    private:
      TfsClientV2();
      DISALLOW_COPY_AND_ASSIGN(TfsClientV2);
      ~TfsClientV2();
    };
  }
}

#endif  // TFS_CLIENTV2_TFSCLIENTAPI_H_
