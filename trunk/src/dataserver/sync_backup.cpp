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
#include "sync_backup.h"
#include "logic_block.h"
#include "blockfile_manager.h"
#include "client/fsname.h"
#include <tbsys.h>

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace client;

    SyncBackup::SyncBackup()
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

    bool NfsMirrorBackup::init()
    {
      char tmpstr[TMP_PATH_SIZE];
      char* nfs_path = CONFIG.get_string_value(CONFIG_PUBLIC, CONF_BACKUP_PATH, NULL);
      if (NULL != nfs_path && strlen(nfs_path) > 0)
      {
        sprintf(backup_path_, "%s/storage", nfs_path);
        sprintf(remove_path_, "%s/removed", nfs_path);
        sprintf(tmpstr, "%s:%d", CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_IP_ADDR), CONFIG.get_int_value(
            CONFIG_NAMESERVER, CONF_PORT));
        source_client_ = new TfsClient(tmpstr, DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS);
        return true;
      }
      return false;
    }

    int NfsMirrorBackup::mk_dir(const char* path)
    {
      struct stat statbuf;
      if (stat(path, &statbuf) == -1)
      {
        TBSYS_LOG(INFO, "NfsMirrorBackup::mk_dir stat %s failed, (%s) not exist.\n", path, strerror(errno));
        if (mkdir(path, 0755) < 0)
        {
          TBSYS_LOG(ERROR, "NfsMirrorBackup::mk_dir mkdir %s failed. (%s)\n", path, strerror(errno));
          return TFS_ERROR;
        }
        return TFS_SUCCESS;
      }
      if (!S_ISDIR(statbuf.st_mode))
      {
        TBSYS_LOG(ERROR, "NfsMirrorBackup::mk_dir %s is not a directory.\n", path);
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int NfsMirrorBackup::mk_dirs(const char* path)
    {
      char parent[TMP_PATH_SIZE];
      memset(parent, 0, TMP_PATH_SIZE);

      if (strlen(path) <= 0)
      {
        return TFS_ERROR;
      }
      int32_t start = static_cast<int32_t> (strlen(path)) - 1;
      for (int32_t i = start; i > 0; --i)
      {
        if (path[i] == '/' && i > 0)
        {
          strncpy(parent, path, i);
          if (mk_dir(parent) == TFS_SUCCESS)
          {
            return mk_dir(path);
          }
          else
          {
            return TFS_ERROR;
          }
        }
      }
      return TFS_ERROR;
    }

    int NfsMirrorBackup::write_n(const int fd, const char* buffer, const int32_t length)
    {
      int32_t bytes_write = 0;
      while (bytes_write < length)
      {
        int ret = ::write(fd, buffer + bytes_write, length - bytes_write);
        if (ret < 0)
        {
          TBSYS_LOG(ERROR, "NfsMirrorBackup::write_n failed. error desc: %s\n", strerror(errno));
          if (ret == EINTR)
          {
            return TFS_ERROR;
          }
          return ret;
        }
        bytes_write += ret;
      }
      return bytes_write;
    }

    void NfsMirrorBackup::get_backup_path(char* buf, const char* path, const uint32_t block_id)
    {
      sprintf(buf, "%s/%d/%u", path, block_id % BLOCK_DIR_NUM, block_id);
    }

    void NfsMirrorBackup::get_backup_file_name(char* buf, const char* path, const uint32_t block_id,
        const uint64_t file_id)
    {
      FSName fsname;
      fsname.set_block_id(block_id);
      fsname.set_file_id(file_id);
      sprintf(buf, "%s/%d/%u/%s", path, block_id % BLOCK_DIR_NUM, block_id, fsname.get_name());
    }

    int NfsMirrorBackup::copy_file(const uint32_t block_id, const uint64_t file_id)
    {
      int ret = TFS_SUCCESS;
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        return remote_copy_file(block_id, file_id);
      }

      FileInfo finfo;
      memset(&finfo, 0, sizeof(FileInfo));

      char copy_file_path[TMP_PATH_SIZE];
      char copy_file_name[TMP_PATH_SIZE];
      get_backup_path(copy_file_path, backup_path_, block_id);
      get_backup_file_name(copy_file_name, backup_path_, block_id, file_id);

      if (mk_dirs(copy_file_path) != TFS_SUCCESS)
      {
        return TFS_ERROR;
      }

      int32_t fd = -1;
      if ((fd = ::open(copy_file_name, O_WRONLY | O_CREAT | O_TRUNC, 0660)) == -1)
      {
        TBSYS_LOG(ERROR, "copy_file, open file: %s failed. error desc: %s\n", copy_file_name, strerror(errno));
        return TFS_ERROR;
      }

      char data[MAX_READ_SIZE];
      int32_t rlen = 0;
      int64_t offset = 0;
      uint32_t crc = 0;
      while (1)
      {
        rlen = MAX_READ_SIZE;
        ret = logic_block->read_file(file_id, data, rlen, offset);
        if (ret)
        {
          TBSYS_LOG(ERROR,
              "read file fail. blockid: %u, fileid: %" PRI64_PREFIX "u, offset: %d, ret: %d\n",
              block_id, file_id, offset, ret);
          ::close(fd);
          return ret;
        }

        int32_t wlen = 0;
        if (offset == 0)
        {
          if (rlen < FILEINFO_SIZE)
          {
            TBSYS_LOG(ERROR,
                "read file fail. blockid: %u, fileid: %" PRI64_PREFIX "u, read len: %d < sizeof(FileInfo), ret: %d\n",
                block_id, file_id, rlen, EXIT_READ_FILE_SIZE_ERROR);
            ::close(fd);
            return EXIT_READ_FILE_SIZE_ERROR;
          }
          memcpy(&finfo, data, sizeof(FileInfo));
          crc = Func::crc(crc, data + sizeof(FileInfo), rlen - sizeof(FileInfo));
          wlen = write_n(fd, data + sizeof(FileInfo), rlen - sizeof(FileInfo));
          if (wlen != rlen - FILEINFO_SIZE)
          {
            ret = TFS_ERROR;
          }
        }
        else
        {
          crc = Func::crc(crc, data, rlen);
          wlen = write_n(fd, data, rlen);
          if (wlen != rlen)
          {
            ret = TFS_ERROR;
          }
        }

        if (ret)
        {
          TBSYS_LOG(ERROR, "write tfsfile fail. blockid: %u, fileid: %" PRI64_PREFIX "u, %s\n", block_id, file_id,
              strerror(errno));
          ::close(fd);
          return TFS_ERROR;
        }
        offset += rlen;
        if (rlen < MAX_READ_SIZE)
          break;
      }

      ::close(fd);
      if (crc != finfo.crc_ || offset != finfo.size_)
      {
        TBSYS_LOG(
            ERROR,
            "crc or file size error. copy file name: %s, blockid: %u, fileid: %" PRI64_PREFIX "u, crc: %u<>%u, size: %d<>%d\n",
            copy_file_name, block_id, file_id, crc, finfo.crc_, offset, finfo.size_);
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    int NfsMirrorBackup::move(const char* source, const char* dest)
    {
      struct stat statbuf;
      if (stat(source, &statbuf) < 0)
      {
        TBSYS_LOG(WARN, "NfsMirrorBackup::rename stat %s failed. error desc: %s\n", source, strerror(errno));
        return -2;
      }
      if (S_ISREG(statbuf.st_mode))
      {
        if (::rename(source, dest) != 0)
        {
          TBSYS_LOG(ERROR, "NfsMirrorBackup::rename remove %s failed. error desc: %s\n", source, strerror(errno));
          return -1;
        }
        return 0;
      }
      else
      {
        TBSYS_LOG(WARN, "NfsMirrorBackup::rename %s is not a file. error desc: %s\n", source, strerror(errno));
        return -2;
      }
      return 0;
    }

    int NfsMirrorBackup::remove_file(const uint32_t block_id, const uint64_t file_id, const int32_t undel)
    {
      char source[TMP_PATH_SIZE];
      char dest[TMP_PATH_SIZE];
      get_backup_file_name(source, backup_path_, block_id, file_id);
      get_backup_file_name(dest, remove_path_, block_id, file_id);

      int ret = TFS_SUCCESS;
      TBSYS_LOG(DEBUG, "remove, blockid: %u, fileid: %" PRI64_PREFIX "u, undel:%d, %s, %s\n", block_id, file_id, undel,
          source, dest);
      if (undel)
      {
        ret = move(dest, source);
      }
      else
      {
        char dest_dir[TMP_PATH_SIZE];
        get_backup_path(dest_dir, remove_path_, block_id);
        if (mk_dirs(dest_dir) != TFS_SUCCESS)
        {
          return TFS_ERROR;
        }
        ret = move(source, dest);
      }

      if (ret == 0 || ret == -2)
        return TFS_SUCCESS;
      return TFS_ERROR;
    }

    int NfsMirrorBackup::rename_file(const uint32_t block_id, const uint64_t file_id, const uint64_t old_file_id)
    {
      char source[TMP_PATH_SIZE];
      char dest[TMP_PATH_SIZE];
      get_backup_file_name(source, backup_path_, block_id, old_file_id);
      get_backup_file_name(dest, backup_path_, block_id, file_id);
      return move(source, dest) == 0 ? TFS_SUCCESS : TFS_ERROR;
    }

    int NfsMirrorBackup::remote_copy_file(const uint32_t block_id, const uint64_t file_id)
    {
      FileInfo finfo;
      FSName fsname;
      fsname.set_block_id(block_id);
      fsname.set_file_id(file_id);

      if (source_client_->tfs_open(block_id, file_id, READ_MODE) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "%s open read fail: %u %" PRI64_PREFIX "u (%s)\n", fsname.get_name(), block_id, file_id,
            source_client_->get_error_message());
        return TFS_ERROR;
      }

      if (source_client_->tfs_stat(&finfo) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "%s stat src file fail: %u %" PRI64_PREFIX "u (%s)\n", fsname.get_name(), block_id, file_id,
            source_client_->get_error_message());
        source_client_->tfs_close();
        return TFS_ERROR;
      }

      char copy_file_name[TMP_PATH_SIZE];
      get_backup_file_name(copy_file_name, backup_path_, block_id, file_id);
      int32_t fd = -1;
      if ((fd = ::open(copy_file_name, O_WRONLY | O_CREAT | O_TRUNC, 0660)) == -1)
      {
        TBSYS_LOG(ERROR, "remote copy file, open file: %s failed.(%s)\n", copy_file_name, strerror(errno));
        return TFS_ERROR;
      }

      char data[MAX_READ_SIZE];
      int32_t rlen = 0;
      int32_t total_size = 0;
      uint32_t crc = 0;
      while ((rlen = source_client_->tfs_read(data, MAX_READ_SIZE)) > 0)
      {
        if (write_n(fd, data, rlen) != rlen)
        {
          TBSYS_LOG(ERROR, "%s write tfsfile fail. blockid: %u, fileid: %" PRI64_PREFIX "u, error desc: (%s)\n",
              fsname.get_name(), block_id, file_id, strerror(errno));
          source_client_->tfs_close();
          ::close(fd);
          return TFS_ERROR;
        }

        crc = Func::crc(crc, data, rlen);
        total_size += rlen;
        if (rlen < MAX_READ_SIZE)
          break;
      }
      source_client_->tfs_close();
      ::close(fd);
      if (crc != finfo.crc_ || total_size != finfo.size_)
      {
        TBSYS_LOG(ERROR, "%s crc error. blockid: %u, fileid: %" PRI64_PREFIX "u, crc: %u <> %u, size: %d <> %d\n",
            fsname.get_name(), block_id, file_id, crc, finfo.crc_, total_size, finfo.size_);
        return TFS_ERROR;
      }
      TBSYS_LOG(INFO, "copy success: %s", fsname.get_name());

      return TFS_SUCCESS;
    }


    bool TfsMirrorBackup::init()
    {
      char tmpstr[TMP_PATH_SIZE];
      TBSYS_LOG(INFO, "SyncMirror init slave ns ip: %s\n",
          SYSPARAM_DATASERVER.slave_ns_ip_ ? SYSPARAM_DATASERVER.slave_ns_ip_ : "none");
      if (SYSPARAM_DATASERVER.slave_ns_ip_ != NULL && strlen(SYSPARAM_DATASERVER.slave_ns_ip_) > 0)
      {
        tfs_client_ = new TfsClient(SYSPARAM_DATASERVER.slave_ns_ip_, DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS);
        tfs_client_->set_option_flag(TFS_FILE_NO_SYNC_LOG);

        second_tfs_client_ = new TfsClient(SYSPARAM_DATASERVER.slave_ns_ip_, DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS);
        second_tfs_client_->set_option_flag(TFS_FILE_NO_SYNC_LOG);

        sprintf(tmpstr, "%s:%d", SYSPARAM_DATASERVER.local_ns_ip_, SYSPARAM_DATASERVER.local_ns_port_);
        source_client_ = new TfsClient(tmpstr, DEFAULT_BLOCK_CACHE_TIME, DEFAULT_BLOCK_CACHE_ITEMS);

        TBSYS_LOG(INFO, "tfs mirror init slave ns ip: %s, local ns ip: %s, ns port: %d, source ip: %s\n",
            SYSPARAM_DATASERVER.slave_ns_ip_, SYSPARAM_DATASERVER.local_ns_ip_, SYSPARAM_DATASERVER.local_ns_port_, tmpstr);

        return true;
      }
      return false;
    }

    int TfsMirrorBackup::do_sync(const SyncData *sf)
    {
      return do_sync(tfs_client_, sf);
    }

    int TfsMirrorBackup::do_second_sync(const SyncData *sf)
    {
      return do_sync(second_tfs_client_, sf);
    }

    int TfsMirrorBackup::do_sync(TfsClient* tfs_client, const SyncData *sf)
    {
      int ret = TFS_ERROR;
      switch (sf->cmd_)
      {
      case OPLOG_INSERT:
        ret = copy_file(tfs_client, sf->block_id_, sf->file_id_);
        break;
      case OPLOG_REMOVE:
        ret = remove_file(tfs_client, sf->block_id_, sf->file_id_, sf->old_file_id_);
        break;
      case OPLOG_RENAME:
        ret = rename_file(tfs_client, sf->block_id_, sf->file_id_, sf->old_file_id_);
        break;
      }
      return ret;
    }

    int TfsMirrorBackup::remote_copy_file(TfsClient* tfs_client, const uint32_t block_id, const uint64_t file_id)
    {
      FileInfo finfo;
      FSName fsname;
      fsname.set_block_id(block_id);
      fsname.set_file_id(file_id);

      if (source_client_->tfs_open(block_id, file_id, READ_MODE) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "%s open read fail. blockid: %u, fileid: %" PRI64_PREFIX "u, error desc: (%s)\n",
            fsname.get_name(), block_id, file_id, source_client_->get_error_message());
        return TFS_ERROR;
      }

      if (source_client_->tfs_stat(&finfo) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "%s stat src file fail. blockid: %u, fileid: %" PRI64_PREFIX "u, error desc: (%s)\n",
            fsname.get_name(), block_id, file_id, source_client_->get_error_message());
        source_client_->tfs_close();
        return TFS_ERROR;
      }

      if (tfs_client->tfs_open(block_id, file_id, (WRITE_MODE | NEWBLK_MODE)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "%s open write fail. blockid: %u, fileid: %" PRI64_PREFIX "u, error desc: (%s)\n",
            fsname.get_name(), block_id, file_id, tfs_client->get_error_message());
        source_client_->tfs_close();
        return TFS_ERROR;
      }

      char data[MAX_READ_SIZE];
      int32_t rlen = 0;
      int32_t total_size = 0;
      uint32_t crc = 0;
      while ((rlen = source_client_->tfs_read(data, MAX_READ_SIZE)) > 0)
      {
        if (tfs_client->tfs_write(data, rlen) != rlen)
        {
          TBSYS_LOG(ERROR, "%s write tfsfile fail. blockid: %u, fileid: %" PRI64_PREFIX "u, error desc: (%s)\n",
              fsname.get_name(), block_id, file_id, tfs_client->get_error_message());
          source_client_->tfs_close();
          tfs_client->tfs_close();
          return TFS_ERROR;
        }

        crc = Func::crc(crc, data, rlen);
        total_size += rlen;
        if (rlen < MAX_READ_SIZE)
          break;
      }

      source_client_->tfs_close();
      int ret = tfs_client->tfs_close();
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "%s close fail. blockid: %u, fileid: %" PRI64_PREFIX "u. error desc: (%s)\n",
            fsname.get_name(), block_id, file_id, tfs_client->get_error_message());
        return TFS_ERROR;
      }
      if (crc != finfo.crc_ || total_size != finfo.size_)
      {
        TBSYS_LOG(ERROR, "%s crc error. blockid: %u, fileid: %" PRI64_PREFIX "u, crc: %u <> %u, size: %d <> %d\n",
            fsname.get_name(), block_id, file_id, crc, finfo.crc_, total_size, finfo.size_);
        return TFS_ERROR;
      }
      TBSYS_LOG(INFO, "tfs remote copy file: %s, blockid: %d, fileid: %" PRI64_PREFIX "u", fsname.get_name(), block_id,
          file_id);

      return TFS_SUCCESS;
    }

    int TfsMirrorBackup::copy_file(TfsClient* tfs_client, const uint32_t block_id, const uint64_t file_id)
    {
      int ret = TFS_SUCCESS;
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        return remote_copy_file(tfs_client, block_id, file_id);
      }

      FileInfo finfo;
      memset(&finfo, 0, sizeof(FileInfo));
      if (tfs_client->tfs_open(block_id, file_id, (WRITE_MODE | NEWBLK_MODE)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "open tfsfile fail: %s\n", tfs_client->get_error_message());
        return TFS_ERROR;
      }

      char data[MAX_READ_SIZE];
      int32_t rlen = 0;
      int32_t offset = 0;
      uint32_t crc = 0;
      while (1)
      {
        rlen = MAX_READ_SIZE;
        ret = logic_block->read_file(file_id, data, rlen, offset);
        if (ret)
        {
          TBSYS_LOG(ERROR,
              "read file fail. blockid: %u, fileid: %" PRI64_PREFIX "u, offset: %d, ret: %d\n",
              block_id, file_id, offset, ret);
          return ret;
        }

        int32_t wlen = 0;
        if (offset == 0)
        {
          if (rlen < FILEINFO_SIZE)
          {
            TBSYS_LOG(
                ERROR,
                "read file fail. blockid: %u, fileid: %" PRI64_PREFIX "u, read len: %d < sizeof(FileInfo), ret: %d\n",
                block_id, file_id, rlen, EXIT_READ_FILE_SIZE_ERROR);
            return EXIT_READ_FILE_SIZE_ERROR;
          }

          memcpy(&finfo, data, sizeof(FileInfo));
          crc = Func::crc(crc, data + sizeof(FileInfo), rlen - sizeof(FileInfo));
          wlen = tfs_client->tfs_write(data + sizeof(FileInfo), rlen - sizeof(FileInfo));
          if (wlen != rlen - FILEINFO_SIZE)
          {
            TBSYS_LOG(ERROR,
                "tfswrite file fail. blockid: %u, fileid: %" PRI64_PREFIX "u, write len: %d <> %d, file size: %d\n",
                block_id, file_id, rlen - sizeof(FileInfo), wlen, finfo.size_);
            ret = TFS_ERROR;
          }
        }
        else
        {
          crc = Func::crc(crc, data, rlen);
          wlen = tfs_client->tfs_write(data, rlen);
          if (wlen != rlen)
          {
            TBSYS_LOG(ERROR,
                "tfswrite file fail. blockid: %u, fileid: %" PRI64_PREFIX "u, write len: %d <> %d, file size: %d\n",
                block_id, file_id, rlen, wlen, finfo.size_);
            ret = TFS_ERROR;
          }
        }

        if (ret)
        {
          TBSYS_LOG(ERROR, "write tfsfile fail. blockid: %u, fileid: %" PRI64_PREFIX "u, %s\n", block_id, file_id,
              tfs_client->get_error_message());
          tfs_client->tfs_close();
          return TFS_ERROR;
        }

        offset += rlen;
        if (rlen < MAX_READ_SIZE)
          break;
      }
      ret = tfs_client->tfs_close();
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "close tfsfile fail. error msg: %s, filename: %s\n", tfs_client->get_error_message(),
            tfs_client->get_file_name());
      }
      if (crc != finfo.crc_ || offset != finfo.size_)
      {
        TBSYS_LOG(ERROR, "crc error.  %s, blockid: %u, fileid :%" PRI64_PREFIX "u, crc: %u <> %u, size: %d <> %d\n",
            tfs_client->get_file_name(), block_id, file_id, crc, finfo.crc_, offset, finfo.size_);
        ret = TFS_ERROR;
      }

      TBSYS_LOG(INFO, "tfs mirror copy file. blockid: %d, fileid: %" PRI64_PREFIX "u, ret: %d\n", block_id, file_id,
          ret);
      return ret;
    }

    int TfsMirrorBackup::remove_file(TfsClient* tfs_client, const uint32_t block_id, const uint64_t file_id,
        const int32_t undel)
    {
      int ret = tfs_client->unlink(block_id, file_id, undel);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "unlink failure: %s\n", tfs_client->get_error_message());
      }

      TBSYS_LOG(INFO, "tfs mirror remove file. blockid: %d, fileid: %" PRI64_PREFIX "u, ret: %d\n", block_id, file_id,
          undel);
      return ret;
    }

    int TfsMirrorBackup::rename_file(TfsClient* tfs_client, const uint32_t block_id, const uint64_t file_id,
        const uint64_t old_file_id)
    {
      int ret = tfs_client->rename(block_id, old_file_id, file_id);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "unlink failure: %s\n", tfs_client->get_error_message());
      }

      TBSYS_LOG(
          INFO,
          "tfs mirror rename file. blockid: %d, fileid: %" PRI64_PREFIX "u, old fileid: %" PRI64_PREFIX "u, ret: %d.\n",
          block_id, file_id, old_file_id);
      return ret;
    }

  }
}
