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
 *   zongdai <zongdai@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_DATASERVER_FILEREPAIR_H_
#define TFS_DATASERVER_FILEREPAIR_H_

#include "dataserver_define.h"
#include "common/internal.h"
#include "new_client/tfs_client_api.h"

namespace tfs
{
  namespace dataserver
  {

    class FileRepair
    {
      public:
        FileRepair() :
          init_status_(false), dataserver_id_(0), tfs_client_(NULL)
        {
        }
        ~FileRepair();

        bool init(const uint64_t dataserver_id);
        int repair_file(const common::CrcCheckFile& crc_check_record, const char* tmp_file);
        int fetch_file(const common::CrcCheckFile& crc_check_record, char* tmp_file);

      private:
        static void get_tmp_file_name(char* buffer, const char* path, const uint32_t block_id, const uint64_t file_id);
        int write_file(const int fd, const char* buffer, const int32_t length);

      private:
        static const int32_t MAX_CONNECT_SERVERS = 5;
        static const int32_t TMP_IPADDR_LEN = 256;

        bool init_status_;
        uint64_t dataserver_id_;
        client::TfsClient* tfs_client_;
    };
  }
}
#endif //TFS_DATASERVER_FILEREPAIR_H_
