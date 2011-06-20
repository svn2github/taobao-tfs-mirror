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
 *   zongdai <zongdai@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include "tbsys.h"

#include "common/func.h"
#include "common/directory_op.h"
#include "new_client/fsname.h"
#include "sync_backup.h"
#include "logic_block.h"
#include "blockfile_manager.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace client;

    SyncBackup::SyncBackup() : tfs_client_(NULL)
    {
      src_addr_[0] = '\0';
      dest_addr_[0] = '\0';
    }

    SyncBackup::~SyncBackup()
    {
    }

    int SyncBackup::do_sync(const SyncData *sf)
    {
      int ret = TFS_ERROR;
      switch (sf->cmd_)
      {
      case OPLOG_INSERT:
        ret = copy_file(sf->block_id_, sf->file_id_);
        break;
      case OPLOG_REMOVE:
        ret = remove_file(sf->block_id_, sf->file_id_, sf->old_file_id_);
        break;
      case OPLOG_RENAME:
        ret = rename_file(sf->block_id_, sf->file_id_, sf->old_file_id_);
        break;
      }
      return ret;
    }

    int SyncBackup::do_second_sync(const SyncData *sf)
    {
      return do_sync(sf);
    }

    int SyncBackup::copy_file(const uint32_t, const uint64_t)
    {
      return TFS_SUCCESS;
    }

    int SyncBackup::remove_file(const uint32_t, const uint64_t, const int32_t)
    {
      return TFS_SUCCESS;
    }

    int SyncBackup::rename_file(const uint32_t, const uint64_t, const uint64_t)
    {
      return TFS_SUCCESS;
    }

    int SyncBackup::remote_copy_file(const uint32_t, const uint64_t)
    {
      return TFS_SUCCESS;
    }

    NfsMirrorBackup::NfsMirrorBackup()
    {
    }

    NfsMirrorBackup::~NfsMirrorBackup()
    {
    }

    bool NfsMirrorBackup::init()
    {
      bool ret = false;
      const char* nfs_path = TBSYS_CONFIG.getString(CONF_SN_PUBLIC, CONF_BACKUP_PATH, NULL);

      TBSYS_LOG(INFO, "NfsSyncMirror init. source ns addr: %s:%d, nfs path: %s",
                NULL != SYSPARAM_DATASERVER.local_ns_ip_ ? SYSPARAM_DATASERVER.local_ns_ip_ : "none",
                SYSPARAM_DATASERVER.local_ns_port_,
                NULL != nfs_path ? nfs_path : "none");

      if (nfs_path != NULL &&
          strlen(nfs_path) > 0 &&
          SYSPARAM_DATASERVER.local_ns_ip_ != NULL &&
          strlen(SYSPARAM_DATASERVER.local_ns_ip_) > 0 &&
          SYSPARAM_DATASERVER.local_ns_port_ != 0)
      {
        snprintf(backup_path_, MAX_PATH_LENGTH, "%s/storage", nfs_path);
        snprintf(remove_path_, MAX_PATH_LENGTH, "%s/removed", nfs_path);

        snprintf(src_addr_, MAX_PATH_LENGTH, "%s:%d",
                 SYSPARAM_DATASERVER.local_ns_ip_,
                 SYSPARAM_DATASERVER.local_ns_port_);

        tfs_client_ = TfsClient::Instance();
        ret =
          tfs_client_->initialize(NULL, DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS, false) == TFS_SUCCESS ?
          true : false;
      }

      return ret;
    }

    int32_t NfsMirrorBackup::write_n(const int fd, const char* buffer, const int32_t length)
    {
      int32_t bytes_write = 0;
      int wlen = 0;

      while (bytes_write < length)
      {
        wlen = ::write(fd, buffer + bytes_write, length - bytes_write);
        if (wlen < 0)
        {
          TBSYS_LOG(ERROR, "NfsMirrorBackup: write_n failed. error desc: %s\n", strerror(errno));
          bytes_write = wlen;
          break;
        }
        bytes_write += wlen;
      }

      return bytes_write;
    }

    void NfsMirrorBackup::get_backup_path(char* buf, const char* path, const uint32_t block_id)
    {
      snprintf(buf, MAX_PATH_LENGTH, "%s/%d/%u", path, block_id % BLOCK_DIR_NUM, block_id);
    }

    void NfsMirrorBackup::get_backup_file_name(char* buf, const char* path,
                                               const uint32_t block_id, const uint64_t file_id)
    {
      FSName fsname(block_id, file_id);
      snprintf(buf, MAX_PATH_LENGTH, "%s/%d/%u/%s", path, block_id % BLOCK_DIR_NUM, block_id, fsname.get_name());
    }

    int NfsMirrorBackup::copy_file(const uint32_t block_id, const uint64_t file_id)
    {
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        return remote_copy_file(block_id, file_id);
      }

      int ret = TFS_ERROR;
      FileInfo finfo;
      memset(&finfo, 0, FILEINFO_SIZE);
      int fd = -1;

      char copy_file_path[MAX_PATH_LENGTH];
      char copy_file_name[MAX_PATH_LENGTH];
      get_backup_path(copy_file_path, backup_path_, block_id);
      get_backup_file_name(copy_file_name, backup_path_, block_id, file_id);

      if (!DirectoryOp::create_full_path(copy_file_path, false, NFS_MIRROR_DIR_MODE))
      {
        TBSYS_LOG(ERROR, "create mirror directory %s with mode %d fail.", copy_file_path, NFS_MIRROR_DIR_MODE);
      }
      else if ((fd = ::open(copy_file_name, O_WRONLY | O_CREAT | O_TRUNC, 0660)) == -1)
      {
        TBSYS_LOG(ERROR, "copy_file, open file: %s failed. error desc: %s\n", copy_file_name, strerror(errno));
      }
      else
      {
        char data[MAX_READ_SIZE];
        char* real_data = data;
        int32_t rlen = 0, real_rlen = 0;
        int32_t wlen = 0;
        int64_t offset = 0;
        uint32_t crc = 0;

        ret = TFS_ERROR;
        while (1)
        {
          rlen = MAX_READ_SIZE;
          ret = logic_block->read_file(file_id, data, rlen, offset, READ_DATA_OPTION_FLAG_NORMAL);

          if (ret != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR,
                      "read file fail. blockid: %u, fileid: %"PRI64_PREFIX"u, offset: %d, ret: %d",
                      block_id, file_id, offset, ret);
            break;
          }

          real_rlen = rlen;
          real_data = data;

          if (offset == 0)
          {
            if (rlen < FILEINFO_SIZE)
            {
              ret = EXIT_READ_FILE_SIZE_ERROR;
              TBSYS_LOG(ERROR,
                        "read file fail. blockid: %u, fileid: %"PRI64_PREFIX"u, read len: %d < sizeof(FileInfo), ret: %d",
                        block_id, file_id, rlen, ret);
              break;
            }

            memcpy(&finfo, data, FILEINFO_SIZE);
            real_rlen -= FILEINFO_SIZE;
            real_data += FILEINFO_SIZE;
          }

          wlen = write_n(fd, real_data, real_rlen);

          if (wlen != real_rlen)
          {
            TBSYS_LOG(ERROR, "write to nfs fail, write len: %d, ret: %d", real_rlen, wlen);
            break;
          }

          crc = Func::crc(crc, real_data, real_rlen);
          offset += real_rlen;

          // all read size
          if (rlen < MAX_READ_SIZE || offset >= finfo.size_)
          {
            ret = TFS_SUCCESS;
            break;
          }
        }

        if (TFS_SUCCESS == ret)
        {
          if (crc != finfo.crc_ || offset != finfo.size_)
          {
            TBSYS_LOG(ERROR,
                      "crc or file size error. copy file name: %s, blockid: %u, fileid: %"PRI64_PREFIX"u, crc: %u <> %u, size: %d <> %d",
                      copy_file_name, block_id, file_id, crc, finfo.crc_, offset, finfo.size_);
            ret = TFS_ERROR;
          }
        }
        ::close(fd);
      }

      if (TFS_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "%s NfsMirror copy success. blockid: %u, fileid: %"PRI64_PREFIX"u",
                  copy_file_name, block_id, file_id);
      }
      else
      {
        TBSYS_LOG(ERROR, "%s NfsMirror copy success. blockid: %u, fileid: %"PRI64_PREFIX"u, ret: %d",
                  copy_file_name, block_id, file_id, ret);
      }

      return ret;
    }

    int NfsMirrorBackup::move(const char* source, const char* dest)
    {
      struct stat statbuf;
      int ret = 0;

      if (stat(source, &statbuf) < 0)
      {
        TBSYS_LOG(WARN, "NfsMirrorBackup: rename stat %s failed. error desc: %s", source, strerror(errno));
        ret = -2;
      }
      else if (S_ISREG(statbuf.st_mode))
      {
        if (::rename(source, dest) != 0)
        {
          TBSYS_LOG(ERROR, "NfsMirrorBackup::rename remove %s failed. error desc: %s", source, strerror(errno));
          ret = -1;
        }
      }
      else
      {
        TBSYS_LOG(WARN, "NfsMirrorBackup::rename %s is not a file. error desc: %s", source, strerror(errno));
        ret = -2;
      }

      return ret;
    }

    int NfsMirrorBackup::remove_file(const uint32_t block_id, const uint64_t file_id, const TfsUnlinkType action)
    {
      char source[MAX_PATH_LENGTH];
      char dest[MAX_PATH_LENGTH];
      get_backup_file_name(source, backup_path_, block_id, file_id);
      get_backup_file_name(dest, remove_path_, block_id, file_id);

      int ret = TFS_SUCCESS;
      TBSYS_LOG(DEBUG, "remove, blockid: %u, fileid: %"PRI64_PREFIX"u, action: %d, %s, %s",
                block_id, file_id, action, source, dest);
      if (action != 0)
      {
        ret = move(dest, source);
      }
      else
      {
        char dest_dir[MAX_PATH_LENGTH];
        get_backup_path(dest_dir, remove_path_, block_id);

        if (!DirectoryOp::create_full_path(dest_dir))
        {
          TBSYS_LOG(ERROR, "create directory %s fail, error: %s", dest_dir, strerror(errno));
        }
        else
        {
          ret = move(source, dest);
        }
      }

      return  (ret == 0 || ret == -2) ? TFS_SUCCESS : TFS_ERROR;
    }

    int NfsMirrorBackup::rename_file(const uint32_t block_id, const uint64_t file_id, const uint64_t old_file_id)
    {
      char source[MAX_PATH_LENGTH];
      char dest[MAX_PATH_LENGTH];
      get_backup_file_name(source, backup_path_, block_id, old_file_id);
      get_backup_file_name(dest, backup_path_, block_id, file_id);
      return move(source, dest) == 0 ? TFS_SUCCESS : TFS_ERROR;
    }

    int NfsMirrorBackup::remote_copy_file(const uint32_t block_id, const uint64_t file_id)
    {
      TfsFileStat file_stat;
      FSName fsname(block_id, file_id);
      int ret = TFS_SUCCESS;
      int src_fd = tfs_client_->open(fsname.get_name(), NULL, src_addr_, T_READ);

      if (src_fd <= 0)
      {
        TBSYS_LOG(ERROR, "%s open src tfsfile read fail: blockid: %u, fileid: %"PRI64_PREFIX"u, ret: %d",
                  fsname.get_name(), block_id, file_id, src_fd);
        ret = TFS_ERROR;
      }
      else
      {
        if ((ret = tfs_client_->fstat(src_fd, &file_stat)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "%s stat src tfsfile fail: blockid: %u, fileid: %"PRI64_PREFIX"u, ret: %d",
                    fsname.get_name(), block_id, file_id, ret);
        }
        else
        {
          char copy_file_name[MAX_PATH_LENGTH];
          get_backup_file_name(copy_file_name, backup_path_, block_id, file_id);
          int32_t fd = -1;
          if ((fd = ::open(copy_file_name, O_WRONLY | O_CREAT | O_TRUNC, 0660)) == -1)
          {
            TBSYS_LOG(ERROR, "open nfs file %s failed, error: %s", copy_file_name, strerror(errno));
            ret = TFS_ERROR;
          }
          else
          {
            char data[MAX_READ_SIZE];
            int32_t rlen = 0;
            int32_t offset = 0;
            uint32_t crc = 0;

            ret = TFS_ERROR;

            while (1)
            {
              if ((rlen = tfs_client_->read(src_fd, data, MAX_READ_SIZE)) <= 0)
              {
                TBSYS_LOG(ERROR, "%s read src tfsfile fail. blockid: %u, fileid: %"PRI64_PREFIX"u, ret: %d",
                          fsname.get_name(), block_id, file_id, rlen);
                break;
              }

              if (write_n(fd, data, rlen) != rlen)
              {
                TBSYS_LOG(ERROR, "%s write file fail. blockid: %u, fileid: %"PRI64_PREFIX"u, error desc: %s",
                          fsname.get_name(), block_id, file_id, strerror(errno));
                break;
              }

              crc = Func::crc(crc, data, rlen);
              offset += rlen;

              if (rlen < MAX_READ_SIZE || offset >= file_stat.size_)
              {
                ret = TFS_SUCCESS;
                break;
              }
            }

            if (TFS_SUCCESS == ret)
            {
              if (crc != file_stat.crc_ || offset != file_stat.size_)
              {
                TBSYS_LOG(ERROR, "%s crc error. blockid: %u, fileid: %"PRI64_PREFIX"u, crc: %u <> %u, size: %d <> %"PRI64_PREFIX"d",
                          fsname.get_name(), block_id, file_id, crc, file_stat.crc_, offset, file_stat.size_);
                ret = TFS_ERROR;
              }
            }
            ::close(fd);
          }
        }
      }

      tfs_client_->close(src_fd);

      if (TFS_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "%s NfsMirror remote copy success. blockid: %u, fileid: %"PRI64_PREFIX"u",
                  fsname.get_name(), block_id, file_id);
      }
      else
      {
        TBSYS_LOG(ERROR, "%s NfsMirror remote copy success. blockid: %u, fileid: %"PRI64_PREFIX"u, ret: %d",
                  fsname.get_name(), block_id, file_id, ret);
      }

      return ret;
    }

    TfsMirrorBackup::TfsMirrorBackup()
    {
    }

    TfsMirrorBackup::~TfsMirrorBackup()
    {
    }

    bool TfsMirrorBackup::init()
    {
      TBSYS_LOG(INFO, "TfsSyncMirror init. source ns addr: %s:%d, destination ns addr: %s",
                NULL != SYSPARAM_DATASERVER.local_ns_ip_ ? SYSPARAM_DATASERVER.local_ns_ip_ : "none",
                SYSPARAM_DATASERVER.local_ns_port_,
                SYSPARAM_DATASERVER.slave_ns_ip_ ? SYSPARAM_DATASERVER.slave_ns_ip_ : "none");

      bool ret = false;

      if (SYSPARAM_DATASERVER.local_ns_ip_ != NULL &&
          strlen(SYSPARAM_DATASERVER.local_ns_ip_) > 0 &&
          SYSPARAM_DATASERVER.local_ns_port_ != 0 &&
          SYSPARAM_DATASERVER.slave_ns_ip_ != NULL &&
          strlen(SYSPARAM_DATASERVER.slave_ns_ip_) > 0)
      {
        snprintf(src_addr_, MAX_ADDRESS_LENGTH, "%s:%d",
                 SYSPARAM_DATASERVER.local_ns_ip_, SYSPARAM_DATASERVER.local_ns_port_);
        snprintf(dest_addr_, MAX_ADDRESS_LENGTH, "%s",
                 SYSPARAM_DATASERVER.slave_ns_ip_);

        tfs_client_ = TfsClient::Instance();
        ret =
          tfs_client_->initialize(NULL, DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS, false) == TFS_SUCCESS ?
          true : false;
      }

      return ret;
    }

    int TfsMirrorBackup::do_sync(const SyncData *sf)
    {
      return do_sync_ex(sf);
    }

    int TfsMirrorBackup::do_second_sync(const SyncData *sf)
    {
      return do_sync_ex(sf);
    }

    int TfsMirrorBackup::do_sync_ex(const SyncData *sf)
    {
      int ret = TFS_ERROR;
      switch (sf->cmd_)
      {
      case OPLOG_INSERT:
        ret = copy_file(sf->block_id_, sf->file_id_);
        break;
      case OPLOG_REMOVE:
        ret = remove_file(sf->block_id_, sf->file_id_, static_cast<TfsUnlinkType>(sf->old_file_id_));
        break;
      case OPLOG_RENAME:
        ret = rename_file(sf->block_id_, sf->file_id_, sf->old_file_id_);
        break;
      }
      return ret;
    }

    int TfsMirrorBackup::remote_copy_file(const uint32_t block_id, const uint64_t file_id)
    {
      FSName fsname(block_id, file_id);
      int ret = TFS_SUCCESS;
      int dest_fd = -1;
      int src_fd = tfs_client_->open(fsname.get_name(), NULL, src_addr_, T_READ);

      if (src_fd <= 0)
      {
        TBSYS_LOG(ERROR, "%s open src read fail. blockid: %u, fileid: %"PRI64_PREFIX"u, ret: %d",
                  fsname.get_name(), block_id, file_id, src_fd);
        ret = TFS_ERROR;
      }
      else
      {
        TfsFileStat file_stat;

        if ((ret = tfs_client_->fstat(src_fd, &file_stat)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "%s stat src file fail. blockid: %u, fileid: %"PRI64_PREFIX"u, ret: %d",
                    fsname.get_name(), block_id, file_id, ret);
        }
        else if ((dest_fd = tfs_client_-> open(fsname.get_name(), NULL, dest_addr_, T_WRITE|T_NEWBLK)) <= 0)
        {
          TBSYS_LOG(ERROR, "%s open dest write fail. blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d",
                    fsname.get_name(), block_id, file_id, dest_fd);
          ret = TFS_ERROR;
        }
        else                      // source file stat ok, destination file open ok
        {
          char data[MAX_READ_SIZE];
          int32_t rlen = 0;
          int32_t offset = 0;
          uint32_t crc = 0;
          // no sync
          tfs_client_->set_option_flag(dest_fd, TFS_FILE_NO_SYNC_LOG);

          ret = TFS_ERROR;
          while (1)
          {
            if ((rlen = tfs_client_->read(src_fd, data, MAX_READ_SIZE)) > 0)
            {
              TBSYS_LOG(ERROR, "%s read src tfsfile fail. blockid: %u, fileid: %"PRI64_PREFIX"u, ret: %d",
                        fsname.get_name(), block_id, file_id, rlen);
              break;
            }

            if (tfs_client_->write(dest_fd, data, rlen) != rlen)
            {
              TBSYS_LOG(ERROR, "%s write dest tfsfile fail. blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d",
                        fsname.get_name(), block_id, file_id, ret);
              break;
            }

            crc = Func::crc(crc, data, rlen);
            offset += rlen;
            if (rlen < MAX_READ_SIZE || offset >= file_stat.size_)
            {
              break;
              ret = TFS_SUCCESS;
            }
          }

          // close destination file anyway
          int tmp_ret = tfs_client_->close(dest_fd);
          if (tmp_ret != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "%s close dest tfsfile fail. blockid: %u, fileid: %" PRI64_PREFIX "u. ret: %d",
                      fsname.get_name(), block_id, file_id, tmp_ret);
            ret = tmp_ret;
          }

          if (TFS_SUCCESS == ret) // write and close all success
          {
            if (crc != file_stat.crc_ || offset != file_stat.size_)
            {
              TBSYS_LOG(ERROR, "%s crc error. blockid: %u, fileid: %" PRI64_PREFIX "u, crc: %u <> %u, size: %d <> %"PRI64_PREFIX"d",
                        fsname.get_name(), block_id, file_id, crc, file_stat.crc_, offset, file_stat.size_);
              ret = TFS_ERROR;
            }
          }
        }

        // close source file
        tfs_client_->close(src_fd);
      }

      if (TFS_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "tfs remote copy file success: %s, blockid: %u, fileid: %" PRI64_PREFIX "u",
                  fsname.get_name(), block_id, file_id);
      }
      else
      {
        TBSYS_LOG(ERROR, "tfs remote copy file success: %s, blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d",
                  fsname.get_name(), block_id, file_id, ret);
      }

      return ret;
    }

    int TfsMirrorBackup::copy_file(const uint32_t block_id, const uint64_t file_id)
    {
      int ret = TFS_SUCCESS;
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        return remote_copy_file(block_id, file_id);
      }

      FSName fsname(block_id, file_id);
      FileInfo finfo;

      int dest_fd = tfs_client_->open(fsname.get_name(), NULL, dest_addr_, T_WRITE|T_NEWBLK);
      if (dest_fd <= 0)
      {
        TBSYS_LOG(ERROR, "open dest tfsfile fail. ret: %d", dest_fd);
        ret = TFS_ERROR;
      }
      else
      {
        char data[MAX_READ_SIZE];
        char* real_data = data;
        int32_t rlen = 0, real_rlen = 0;
        int32_t wlen = 0;
        int32_t offset = 0;
        uint32_t crc = 0;

        // no sync
        tfs_client_->set_option_flag(dest_fd, TFS_FILE_NO_SYNC_LOG);

        ret = TFS_ERROR;
        while (1)
        {
          rlen = MAX_READ_SIZE;
          ret = logic_block->read_file(file_id, data, rlen, offset, READ_DATA_OPTION_FLAG_NORMAL);
          if (ret != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "read file fail. blockid: %u, fileid: %"PRI64_PREFIX"u, offset: %d, ret: %d",
                      block_id, file_id, offset, ret);
            break;
          }

          real_rlen = rlen;
          real_data = data;

          if (offset == 0)
          {
            if (rlen < FILEINFO_SIZE)
            {
              ret = EXIT_READ_FILE_SIZE_ERROR;
              TBSYS_LOG(ERROR,
                        "read file fail. blockid: %u, fileid: %"PRI64_PREFIX"u, read len: %d < sizeof(FileInfo), ret: %d",
                        block_id, file_id, rlen, ret);
              break;
            }

            memcpy(&finfo, data, FILEINFO_SIZE);
            real_rlen -= FILEINFO_SIZE;
            real_data += FILEINFO_SIZE;
          }

          wlen = tfs_client_->write(dest_fd, real_data, real_rlen);

          if (wlen != real_rlen)
          {
            TBSYS_LOG(ERROR,
                      "write dest tfsfile fail. blockid: %u, fileid: %"PRI64_PREFIX"u, write len: %d <> %d, file size: %d",
                      block_id, file_id, wlen, real_rlen, finfo.size_);
            break;
          }

          crc = Func::crc(crc, real_data, real_rlen);
          offset += real_rlen;

          // all read size
          if (rlen < MAX_READ_SIZE || offset >= finfo.size_)
          {
            ret = TFS_SUCCESS;
            break;
          }
        }

        // close destination tfsfile anyway
        int tmp_ret = tfs_client_->close(dest_fd);
        if (tmp_ret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "close dest tfsfile fail. blockid: %u, fileid :%" PRI64_PREFIX "u, ret: %d",
                    block_id, file_id, ret);
          ret = tmp_ret;
        }

        if (TFS_SUCCESS == ret) // write and close success
        {
          if (crc != finfo.crc_ || offset != finfo.size_)
          {
            TBSYS_LOG(ERROR, "crc error. %s, blockid: %u, fileid :%" PRI64_PREFIX "u, crc: %u <> %u, size: %d <> %d",
                      fsname.get_name(), block_id, file_id, crc, finfo.crc_, offset, finfo.size_);
            ret = TFS_ERROR;
          }
        }
      }

      if (ret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "tfs mirror copy file fail. blockid: %d, fileid: %"PRI64_PREFIX"u, ret: %d",
                  block_id, file_id, ret);
      }
      else
      {
        TBSYS_LOG(INFO, "tfs mirror copy file success. blockid: %d, fileid: %"PRI64_PREFIX"u",
                  block_id, file_id);
      }

      return ret;
    }

    int TfsMirrorBackup::remove_file(const uint32_t block_id, const uint64_t file_id,
                                     const TfsUnlinkType action)
    {
      FSName fsname(block_id, file_id);

      int64_t file_size = 0;
      int ret = tfs_client_->unlink(fsname.get_name(), NULL, dest_addr_, file_size, action, TFS_FILE_NO_SYNC_LOG);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "tfs mirror remove file fail. blockid: %d, fileid: %"PRI64_PREFIX"u, action: %d, ret: %d",
                  block_id, file_id, action, ret);
      }
      else
      {
        TBSYS_LOG(INFO, "tfs mirror remove file success. blockid: %d, fileid: %"PRI64_PREFIX"u, action: %d",
                  block_id, file_id, action);
      }

      return ret;
    }

    int TfsMirrorBackup::rename_file(const uint32_t block_id, const uint64_t file_id,
                                     const uint64_t old_file_id)
    {
      UNUSED(block_id);
      UNUSED(file_id);
      UNUSED(old_file_id);
      // FSName fsname(block_id, file_id);
      // int ret = tfs_client->rename(block_id, old_file_id, file_id);
      // if (TFS_SUCCESS != ret)
      // {
      //   TBSYS_LOG(ERROR, "unlink failure: %s\n", tfs_client->get_error_message());
      // }

      // TBSYS_LOG(
      //     INFO,
      //     "tfs mirror rename file. blockid: %d, fileid: %" PRI64_PREFIX "u, old fileid: %" PRI64_PREFIX "u, ret: %d.\n",
      //     block_id, file_id, old_file_id);
      // return ret;
      return TFS_SUCCESS;
    }

  }
}
