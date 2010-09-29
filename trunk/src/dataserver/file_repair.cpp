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
#include "file_repair.h"
#include "common/parameter.h"
#include "common/error_msg.h"
#include "common/func.h"
#include "client/fsname.h"
#include <tbsys.h>

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace client;

    bool FileRepair::init(const uint64_t dataserver_id)
    {
      if (init_status_)
      {
        return true;
      }
      dataserver_id_ = dataserver_id;

      char tmpstr[TMP_IPADDR_LEN];
      memset(tmpstr, 0, TMP_IPADDR_LEN);
      if (SYSPARAM_DATASERVER.local_ns_ip_ != NULL && strlen(SYSPARAM_DATASERVER.local_ns_ip_) > 0
          && SYSPARAM_DATASERVER.local_ns_port_ > 0)
      {
        sprintf(tmpstr, "%s:%d", SYSPARAM_DATASERVER.local_ns_ip_, SYSPARAM_DATASERVER.local_ns_port_);
        TBSYS_LOG(INFO, "file repair init ns address: %s:%d tmpstr: %s\n", SYSPARAM_DATASERVER.local_ns_ip_,
            SYSPARAM_DATASERVER.local_ns_port_, tmpstr);

        tfs_client_ = new TfsClient(tmpstr, DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS);
        tfs_client_->set_option_flag(TFS_FILE_NO_SYNC_LOG);
        init_status_ = true;
        return true;
      }
      return false;
    }

    int FileRepair::fetch_file(const CrcCheckFile& crc_check_record, char* tmp_file)
    {
      if (NULL == tmp_file)
      {
        return TFS_ERROR;
      }
      FSName fsname;
      fsname.set_block_id(crc_check_record.block_id_);
      fsname.set_file_id(crc_check_record.file_id_);
      TBSYS_LOG(INFO, "repair file start, tfsname: %s, blockid: %u, fileid: %" PRI64_PREFIX "u\n", fsname.get_name(),
          crc_check_record.block_id_, crc_check_record.file_id_);

      if (tfs_client_->tfs_open(crc_check_record.block_id_, crc_check_record.file_id_, READ_MODE) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "%s open read fail: %u %" PRI64_PREFIX "u (%s)\n", fsname.get_name(), crc_check_record.block_id_,
            crc_check_record.file_id_, tfs_client_->get_error_message());
        return TFS_ERROR;
      }

      int32_t i = 0;
      for (; i < MAX_CONNECT_SERVERS; ++i)
      {
        // connect id in fail server or self. skip
        if (tfs_client_->get_last_elect_ds_id() == dataserver_id_ || find(crc_check_record.fail_servers_.begin(),
              crc_check_record.fail_servers_.end(), tfs_client_->get_last_elect_ds_id()) != crc_check_record.fail_servers_.end())
        {
          int ret = tfs_client_->tfs_reset_read();
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "copy file, tfs reset read fail. try time: %d, ret: %d, error: %s\n", i + 1, ret,
                tfs_client_->get_error_message());
            return EXIT_DS_CONNECT_ERROR;
          }
          continue;
        }
        else
        {
          break;
        }
      }

      if (MAX_CONNECT_SERVERS == i)
      {
        TBSYS_LOG(ERROR, "copy file, tfs reset read fail. try time: %d\n", i + 1);
        return EXIT_DS_CONNECT_ERROR;
      }

      TBSYS_LOG(INFO, "copy file, blockid: %u, fileid: %" PRI64_PREFIX "u. read from src ds: %s \n",
          crc_check_record.block_id_, crc_check_record.file_id_,
          tbsys::CNetUtil::addrToString(tfs_client_->get_last_elect_ds_id()).c_str());

      FileInfo finfo;
      if (TFS_SUCCESS != tfs_client_->tfs_stat(&finfo))
      {
        TBSYS_LOG(ERROR, "%s stat tfs_file_ fail: %u %" PRI64_PREFIX "u (%s)\n", fsname.get_name(),
            crc_check_record.block_id_, crc_check_record.file_id_, tfs_client_->get_error_message());
        tfs_client_->tfs_close();
        return TFS_ERROR;
      }

      int fd = 0;
      get_tmp_file_name(tmp_file, SYSPARAM_DATASERVER.work_dir_.c_str(), crc_check_record.block_id_, crc_check_record.file_id_);
      if ((fd = open(tmp_file, O_WRONLY | O_CREAT | O_TRUNC, 0660)) == -1)
      {
        TBSYS_LOG(ERROR, "copy file, open file: %s failed.(%s)\n", tmp_file, strerror(errno));
        return TFS_ERROR;
      }

      char data[MAX_READ_SIZE];
      int32_t rlen = 0, total_size = 0;
      uint32_t crc = 0;
      while ((rlen = tfs_client_->tfs_read(data, MAX_READ_SIZE)) > 0)
      {
        if (write_file(fd, data, rlen) != rlen)
        {
          fprintf(stderr, "%s write tfsfile fail: %u %" PRI64_PREFIX "u (%s)\n", fsname.get_name(),
              crc_check_record.block_id_, crc_check_record.file_id_, strerror(errno));
          tfs_client_->tfs_close();
          ::close(fd);
          return TFS_ERROR;
        }

        crc = Func::crc(crc, data, rlen);
        total_size += rlen;
        if (rlen < MAX_READ_SIZE)
          break;
      }
      close(fd);
      tfs_client_->tfs_close();
      if (crc != finfo.crc_ || crc != crc_check_record.crc_ || total_size != finfo.size_)
      {
        TBSYS_LOG(ERROR,
            "file %s crc error. blockid: %u, fileid: %" PRI64_PREFIX "u, %u<>%u, checkfile crc: %u, size: %d<>%d\n",
            fsname.get_name(), crc_check_record.block_id_, crc_check_record.file_id_, crc, finfo.crc_, crc_check_record.crc_,
            total_size, finfo.size_);
        return TFS_ERROR;
      }

      TBSYS_LOG(INFO, "fetch success: %s", fsname.get_name());

      return TFS_SUCCESS;
    }

    int FileRepair::repair_file(const CrcCheckFile& crc_check_record, const char* tmp_file)
    {
      FSName fsname;
      fsname.set_block_id(crc_check_record.block_id_);
      fsname.set_file_id(crc_check_record.file_id_);

      int ret = tfs_client_->save_file(tmp_file, fsname.get_name(), NULL);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "repair file fail, ret: %d, errmsg: %s, tfsname: %s, blockid: %u fileid: %" PRI64_PREFIX "u\n",
            ret, tfs_client_->get_file_name(), crc_check_record.block_id_, crc_check_record.file_id_, tfs_client_->get_error_message());
      }
      else
      {
        TBSYS_LOG(ERROR, "repair file successful, tfsname: %s, blockid: %u fileid: %" PRI64_PREFIX "u\n",
            tfs_client_->get_file_name(), crc_check_record.block_id_, crc_check_record.file_id_);
      }

      return ret;
    }

    void FileRepair::get_tmp_file_name(char* buffer, const char* path, const uint32_t block_id, const uint64_t file_id)
    {
      if (NULL == buffer || NULL == path)
      {
        return;
      }
      FSName fsname;
      fsname.set_block_id(block_id);
      fsname.set_file_id(file_id);
      sprintf(buffer, "%s/tmp/%s", path, fsname.get_name());
    }

    int FileRepair::write_file(const int fd, const char* buffer, const int32_t length)
    {
      int32_t bytes_write = 0;
      while (bytes_write < length)
      {
        int ret = write(fd, buffer + bytes_write, length - bytes_write);
        if (ret < 0)
        {
          TBSYS_LOG(ERROR, "file repair failed when write. error desc: %s\n", strerror(errno));
          return ret;
        }
        bytes_write += ret;
      }
      return bytes_write;
    }
  }
}
