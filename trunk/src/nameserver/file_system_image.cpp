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
#include <tbsys.h>
#include <Memory.hpp>
#include "common/interval.h"
#include "common/file_queue.h"
#include "common/config_item.h"
#include "common/error_msg.h"
#include "file_system_image.h"
#include "ns_define.h"
#include "nameserver.h"

using namespace tfs::common;

namespace tfs
{
  namespace nameserver
  {

    FileSystemImage::FileSystemImage(MetaManager* mm) :
      fd_(-1), meta_(mm)
    {
      ::memset((char*) &header_, 0, sizeof(header_));
    }

    FileSystemImage::~FileSystemImage()
    {

    }

    int FileSystemImage::initialize(LayoutManager & block_ds_map, const OpLogRotateHeader& head, FileQueue* file_queue)
    {
      char default_work_dir[MAX_PATH_LENGTH];
      snprintf(default_work_dir, MAX_PATH_LENGTH, "%s/nameserver",  CONFIG.get_string_value(CONFIG_PUBLIC, CONF_WORK_DIR));
      const char *work_dir = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_WORK_DIR, default_work_dir);
      snprintf(image_file_path_, MAX_PATH_LENGTH, "%s/fs_image", work_dir);
      snprintf(image_file_new_path_, MAX_PATH_LENGTH, "%s/fs_image.new", work_dir);

      bool has_log = false;
      file_queue->set_delete_file_flag(true);
      int iret = file_queue->load_queue_head();
      if (iret != TFS_SUCCESS)
      {
        TBSYS_LOG(DEBUG, "load header file of file_queue errors(%s)", strerror(errno));
        return iret;
      }
      QueueInformationHeader* qhead = file_queue->get_queue_information_header();
      QueueInformationHeader tmp = *qhead;
      TBSYS_LOG(DEBUG, "befor load queue header: read seqno(%d), read offset(%d), write seqno(%d),"
        "write file size(%d), queue size(%d). oplog header:, rotate seqno(%d)"
        "rotate offset(%d)", qhead->read_seqno_, qhead->read_offset_, qhead->write_seqno_, qhead->write_filesize_,
          qhead->queue_size_, head.rotate_seqno_, head.rotate_offset_);
      if (qhead->read_seqno_ > 0x01 && qhead->write_seqno_ > 0x01 && head.rotate_seqno_ > 0)
      {
        has_log = true;
        if (tmp.read_seqno_ <= head.rotate_seqno_)
        {
          tmp.read_seqno_ = head.rotate_seqno_;
          if (tmp.read_seqno_ == head.rotate_seqno_)
            tmp.read_offset_ = head.rotate_offset_;
          file_queue->update_queue_information_header(&tmp);
        }
      }
      TBSYS_LOG(DEBUG, "after load queue header: read seqno(%d), read offset(%d), write seqno(%d),"
        "write file size(%d), queue size(%d). oplog header:, rotate seqno(%d)"
        "rotate offset(%d)", qhead->read_seqno_, qhead->read_offset_, qhead->write_seqno_, qhead->write_filesize_,
          qhead->queue_size_, head.rotate_seqno_, head.rotate_offset_);

      iret = file_queue->initialize();
      if (iret != TFS_SUCCESS)
      {
        TBSYS_LOG(DEBUG, "call FileQueue::finishSetup errors(%s)", strerror(errno));
        return iret;
      }

      if (access(image_file_new_path_, R_OK) == 0)
      {
        if (has_log)
        {
          if (unlink(image_file_new_path_))
            TBSYS_LOG(WARN, "can't remove image file '%s'", image_file_new_path_);
        }
        else
        {
          if (rename(image_file_new_path_, image_file_path_))
            TBSYS_LOG(WARN, "rename fail '%s' => '%s'", image_file_new_path_, image_file_path_);
        }
      }

      int32_t fd = -1;
      BlockCollect* block_collect = NULL;
      BlockInfo* b = NULL;
      int64_t total_byte = 0;
      int64_t total_file_count = 0;
      //TODO
      const NsRuntimeGlobalInformation* ngi = meta_->get_fs_name_system()->get_ns_global_info();

      if (access(image_file_path_, R_OK) == 0)
      {
        fd = ::open(image_file_path_, O_RDONLY);
        if (fd == -1)
        {
          TBSYS_LOG(WARN, "can't open image file '%s'", image_file_path_);
          return TFS_SUCCESS;
        }
        if (::read(fd, &header_, sizeof(ImageHeader)) != sizeof(ImageHeader))
        {
          TBSYS_LOG(ERROR, "read fail");
          ::close(fd);
          return EXIT_READ_FILE_ERROR;
        }
        if (header_.flag_ != IMAGE_FLAG)
        {
          TBSYS_LOG(ERROR, "file format is error");
          ::close(fd);
          return EXIT_FILE_FORMAT_ERROR;
        }

        for (int64_t i = 0; i < header_.block_count_ && ngi->destroy_flag_ != NS_DESTROY_FLAGS_YES; ++i)
        {
          block_collect = new BlockCollect();
          b = const_cast<BlockInfo*> (block_collect->get_block_info());
          if (read(fd, b, BLOCKINFO_SIZE) != BLOCKINFO_SIZE)
          {
            TBSYS_LOG(ERROR, "read block fail");
            close(fd);
            return EXIT_READ_FILE_ERROR;
          }
          // TODO  check insert success?
          block_ds_map.insert(block_collect, false);
          total_byte += b->size_;
          total_file_count += b->file_count_;
        }

        close(fd);
        if (header_.total_bytes_ != total_byte || header_.total_file_count_ != total_file_count)
        {
          TBSYS_LOG(ERROR, "total_byte(%"PRI64_PREFIX"d) , (%"PRI64_PREFIX"d), total_file_count(%"PRI64_PREFIX"d), (%"PRI64_PREFIX"d)", header_.total_bytes_, total_byte,
                    header_.total_file_count_, total_file_count);
          return EXIT_RECORD_SIZE_ERROR;
        }
      }

      if (has_log)
      {
        int64_t ilength = 0;
        int64_t offset = 0;
        int64_t ds_size = 0;
        BlockInfo *block_info = NULL;
        BlockInfo* dest_block_info = NULL;
        BlockCollect* block_collect = NULL;
        do
        {
          QueueItem* item = file_queue->pop();
          if (item == NULL)
            continue;
          const char* data = item->data_;
          ilength = item->length_;
          offset = ds_size = 0;
          block_info = dest_block_info = NULL;
          block_collect = NULL;
          do
          {
            OpLogHeader* header = reinterpret_cast<OpLogHeader*> (const_cast<char*> (data + offset));
            offset += sizeof(OpLogHeader);
            block_info = reinterpret_cast<BlockInfo*> (const_cast<char*> (data + offset));
            offset += BLOCKINFO_SIZE;
            ds_size = data[offset];
            offset += 0x01;
            for (int64_t i = 0; i < ds_size; i++)
            {
              offset += INT64_SIZE;
            }
            switch (header->cmd_)
            {
            case OPLOG_UPDATE:
            case OPLOG_INSERT:
              block_info = reinterpret_cast<BlockInfo*> (header->data_);
              block_collect = block_ds_map.get_block_collect(block_info->block_id_);
              if (!block_collect)
                block_collect = block_ds_map.create_block_collect(block_info->block_id_);
              dest_block_info = const_cast<BlockInfo*> (block_collect->get_block_info());
              memcpy(dest_block_info, block_info, BLOCKINFO_SIZE);
              TBSYS_LOG(DEBUG, "cmd(%d), blockinof block(%u)", header->cmd_, block_info->block_id_);
              break;
            default:
              break;
            }
          }
          while ((ilength > static_cast<int64_t> (sizeof(OpLogHeader))) && (ilength > offset) &&
                 (ngi->destroy_flag_!= NS_DESTROY_FLAGS_YES));
          free(item);
          item = NULL;
        }
        while ((qhead->read_seqno_ != qhead->write_seqno_) ||
               (((qhead->read_seqno_ == qhead->write_seqno_) && (qhead->read_offset_ != qhead->write_filesize_)) &&
                (ngi->destroy_flag_ != NS_DESTROY_FLAGS_YES)));
        save(block_ds_map);
      }
      return TFS_SUCCESS;
    }

    int32_t FileSystemImage::save(const LayoutManager & block_ds_map)
    {
      int32_t ret = save_new_image(block_ds_map);
      if (ret != TFS_SUCCESS)
      {
        unlink( image_file_new_path_);
        TBSYS_LOG(ERROR, "save file fail: %s.", image_file_new_path_);
        return ret;
      }
      if (rename(image_file_new_path_, image_file_path_) != 0)
      {
        TBSYS_LOG(ERROR, "%s => %s rename fail.", image_file_new_path_, image_file_path_);
        return EXIT_FILE_OP_ERROR;
      }
      return TFS_SUCCESS;
    }

    int FileSystemImage::process(const BlockCollect* block_collect) const
    {
      if (-1 == fd_)
        return MetaScanner::BREAK;

      const BlockInfo* block_info = block_collect->get_block_info();
      if (write(fd_, block_info, BLOCKINFO_SIZE) != BLOCKINFO_SIZE)
      {
        TBSYS_LOG(ERROR, "write block fail");
        close( fd_);
        fd_ = -1;
        return MetaScanner::BREAK;
      }
      header_.total_bytes_ += block_info->size_;
      header_.total_file_count_ += block_info->file_count_;
      header_.block_count_ += 1;
      return MetaScanner::CONTINUE;
    }

    int FileSystemImage::save_new_image(const LayoutManager & block_ds_map)
    {
      if (image_file_new_path_[0] == '\0')
      {
        return TFS_SUCCESS;
      }

      TBSYS_LOG(DEBUG, "image_file_new_path_(%s)", image_file_new_path_);
      fd_ = open(image_file_new_path_, O_WRONLY | O_CREAT | O_TRUNC, 0600);
      if (fd_ == -1)
      {
        TBSYS_LOG(WARN, "can't open image file write, '%s'", image_file_new_path_);
        return EXIT_READ_FILE_ERROR;
      }
      memset(&header_, 0, sizeof(ImageHeader));
      header_.flag_ = IMAGE_FLAG;
      if (write(fd_, &header_, sizeof(ImageHeader)) != sizeof(ImageHeader))
      {
        TBSYS_LOG(ERROR, "write header fail");
        close( fd_);
        fd_ = -1;
        return EXIT_WRITE_FILE_ERROR;
      }

      int ret = block_ds_map.foreach(*this);
      if (ret != TFS_SUCCESS)
        return ret;

      if (lseek(fd_, SEEK_SET, 0) == (off_t) - 1)
      {
        TBSYS_LOG(ERROR, "write block fail");
        close( fd_);
        fd_ = -1;
        return EXIT_FILE_OP_ERROR;
      }
      if (write(fd_, &header_, sizeof(ImageHeader)) != sizeof(ImageHeader))
      {
        TBSYS_LOG(ERROR, "write header fail");
        close( fd_);
        fd_ = -1;
        return EXIT_WRITE_FILE_ERROR;
      }
      close( fd_);
      TBSYS_LOG(INFO, "fsimage saved (%s)", image_file_new_path_);
      return TFS_SUCCESS;
    }
  }
}
