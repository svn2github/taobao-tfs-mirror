/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *  linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#include "common/func.h"
#include "common/client_manager.h"
#include "common/base_packet.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "clientv2/tfs_client_impl_v2.h"
#include "clientv2/fsname.h"
#include "sync_util.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::clientv2;
using namespace std;

namespace tfs
{
  namespace requester
  {
    int SyncUtil::read_file_infos(const std::string& ns_addr, const uint64_t block, std::multiset<std::string>& files, const int32_t version)
    {
      int32_t ret = (INVALID_BLOCK_ID == block) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        std::set<FileInfo, CompareFileInfoByFileId> file_sets;
        ret = NsRequester::read_file_infos(ns_addr, block, file_sets, version);
        if (TFS_SUCCESS == ret)
        {
          std::set<FileInfo, CompareFileInfoByFileId>::const_iterator iter = file_sets.begin();
          for (; iter != file_sets.end(); ++iter)
          {
            FSName fsname(block, (*iter).id_,  TfsClientImplV2::Instance()->get_cluster_id());
            files.insert(fsname.get_name());
          }
        }
      }
      return ret;
    }

    int SyncUtil::read_file_info(const std::string& ns_addr, const std::string& filename, FileInfo& info)
    {
      int32_t fd = TfsClientImplV2::Instance()->open(filename.c_str(), NULL, ns_addr.c_str(), T_READ);
      int32_t ret = fd < 0 ? fd : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        TfsFileStat stat;
        ret = TfsClientImplV2::Instance()->set_option_flag(fd, FORCE_STAT);
        if (TFS_SUCCESS == ret)
        {
          ret = TfsClientImplV2::Instance()->fstat(fd, &stat);
        }
        if (TFS_SUCCESS == ret)
        {
          info.id_ = stat.file_id_;
          info.offset_ = stat.offset_;
          info.size_ = stat.size_;
          info.usize_ = stat.usize_;
          info.modify_time_ = stat.modify_time_;
          info.create_time_ = stat.create_time_;
          info.flag_ = stat.flag_;
          info.crc_ = stat.crc_;
          ret = (stat.flag_ == 1 || stat.flag_ == 4 || stat.flag_ == 5) ? META_FLAG_ABNORMAL : TFS_SUCCESS;
        }
        TfsClientImplV2::Instance()->close(fd);
      }
      return ret;
    }

    int SyncUtil::read_file_real_crc(const std::string& ns_addr, const std::string& filename, uint32_t& crc)
    {
      crc = 0;
      int32_t fd = TfsClientImplV2::Instance()->open(filename.c_str(), NULL, ns_addr.c_str(), T_READ);
      int32_t ret = fd < 0 ? fd : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        int32_t total = 0;
        char data[MAX_READ_SIZE]={'\0'};
        TfsFileStat stat;
        while (true)
        {
          int32_t rlen = TfsClientImplV2::Instance()->readv2(fd, data, MAX_READ_SIZE, &stat);
          ret = rlen < 0 ? rlen : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            if (0 == rlen)
              break;
            total += rlen;
            crc = Func::crc(crc, data, rlen);
            //Func::hex_dump(data, 10, true, TBSYS_LOG_LEVEL_INFO);
            //TBSYS_LOG(INFO, "FILENAME : %s, READ LENGTH: %d, crc: %u", filename.c_str(), rlen, crc);
          }
          else
          {
            break;
          }
        }
        TfsClientImplV2::Instance()->close(fd);
      }
      return ret;
    }

    int SyncUtil::read_file_info_v2(const std::string& ns_addr, const std::string& filename, FileInfoV2& info)
    {
      TfsFileStat stat;
      memset(&info, 0, sizeof(info));
      memset(&stat, 0, sizeof(stat));
      int32_t ret = TfsClientImplV2::Instance()->stat_file(&stat, filename.c_str(), NULL, FORCE_STAT, ns_addr.c_str());
      if (TFS_SUCCESS == ret)
        FileStatToFileInfoV2(stat, info);
      else
        TBSYS_LOG(INFO, "stat file %s fail, ns addr:%s, ret:%d", filename.c_str(), ns_addr.c_str(), ret);
      return ret;
    }

    int SyncUtil::read_file_stat(const std::string& ns_addr, const std::string& filename, common::TfsFileStat& info)
    {
      int32_t ret = TfsClientImplV2::Instance()->stat_file(&info, filename.c_str(), NULL, FORCE_STAT, ns_addr.c_str());
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "stat file:%s fail, ns:%s, ret:%d", filename.c_str(), ns_addr.c_str(), ret);
      }
      return ret;
    }

    int SyncUtil::read_file_real_crc_v2(const std::string& ns_addr, const std::string& filename, common::FileInfoV2& info, const bool force)
    {
      info.crc_ = 0;
      int32_t fd = TfsClientImplV2::Instance()->open(filename.c_str(), NULL, ns_addr.c_str(), T_READ);
      int32_t ret = fd < 0 ? fd : TFS_SUCCESS;
      if (TFS_SUCCESS == ret && force)
      {
        ret = TfsClientImplV2::Instance()->set_option_flag(fd, READ_DATA_OPTION_FLAG_FORCE);
      }
      if (TFS_SUCCESS == ret)
      {
        int32_t total = 0;
        char data[MAX_READ_SIZE]={'\0'};
        TfsFileStat stat;
        while (true)
        {
          int32_t rlen = TfsClientImplV2::Instance()->readv2(fd, data, MAX_READ_SIZE, &stat);
          ret = rlen < 0 ? rlen : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            if (0 == total)
            {
              FileStatToFileInfoV2(stat, info);
              info.crc_ = 0;
            }
            total += rlen;
            info.crc_ = Func::crc(info.crc_, data, rlen);
            //Func::hex_dump(data, 10, true, TBSYS_LOG_LEVEL_INFO);
            //TBSYS_LOG(INFO, "FILENAME : %s, READ LENGTH: %d, crc: %u", filename.c_str(), rlen, crc);
            if(rlen < MAX_READ_SIZE)
              break;
          }
          else
          {
            TBSYS_LOG(ERROR, "read file fail, filename:%s, ns:%s, ret:%d", filename.c_str(), ns_addr.c_str(), ret);
            break;
          }
        }
        TfsClientImplV2::Instance()->close(fd);
      }
      return ret;
    }

    int SyncUtil::write_file(const std::string& ns_addr, const std::string& filename, const char* data, const int32_t size, const int32_t status)
    {
      int32_t fd = -1;
      int32_t ret = (!ns_addr.empty() && !filename.empty()  && NULL != data && size > 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        fd = TfsClientImplV2::Instance()->open(filename.c_str(), NULL, ns_addr.c_str(), T_WRITE | T_NEWBLK);
        ret = fd < 0 ? fd : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = TfsClientImplV2::Instance()->set_option_flag(fd, TFS_FILE_NO_SYNC_LOG);
      }
      if (TFS_SUCCESS == ret)
      {
        int64_t wlen = TfsClientImplV2::Instance()->write(fd, data, size);
        ret = (wlen == size) ? TFS_SUCCESS : EXIT_WRITE_FILE_ERROR;
      }

      if (fd > 0)
      {
        int32_t result = TfsClientImplV2::Instance()->close(fd, NULL, 0, status);
        if (TFS_SUCCESS != result)
        {
          TBSYS_LOG(INFO, "close file %s failed, ret: %d, ns_addr: %s", filename.c_str(), ret, ns_addr.c_str());
        }
      }
      return ret;
    }

    int SyncUtil::copy_file(const string& src_ns_addr, const string& dest_ns_addr, const string& file_name, const int32_t status)
    {
      int32_t ret = (!src_ns_addr.empty() && !dest_ns_addr.empty() && !file_name.empty()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int32_t source_fd = -1, dest_fd = -1;
        source_fd = TfsClientImplV2::Instance()->open(file_name.c_str(), NULL, src_ns_addr.c_str(), T_READ | T_FORCE);
        if (source_fd < 0)
        {
          ret = source_fd;
          TBSYS_LOG(INFO, "open source %s fail when copy file, ns_addr: %s ret:%d", file_name.c_str(), src_ns_addr.c_str(), ret);
        }
        if (TFS_SUCCESS == ret)
        {
          dest_fd = TfsClientImplV2::Instance()->open(file_name.c_str(), NULL, dest_ns_addr.c_str(), T_WRITE | T_NEWBLK);
          if (dest_fd < 0)
          {
            ret = dest_fd;
            TBSYS_LOG(INFO, "open dest %s fail when copy file, ns_addr: %s ret:%d", file_name.c_str(), dest_ns_addr.c_str(), ret);
          }
        }
        if (TFS_SUCCESS == ret)
        {
          ret = TfsClientImplV2::Instance()->set_option_flag(dest_fd, TFS_FILE_NO_SYNC_LOG);
        }
        if (TFS_SUCCESS == ret)
        {
          int32_t rlen = 0, wlen = 0;
          const int32_t MAX_READ_DATA_SIZE = 4 * 1024 * 1024;
          char data[MAX_READ_DATA_SIZE];
          for (;;)
          {
            rlen = TfsClientImplV2::Instance()->read(source_fd, data, MAX_READ_DATA_SIZE);
            if (rlen < 0)
            {
              ret = rlen;
              TBSYS_LOG(INFO, "read %s fail when copy file, ns_addr: %s ret:%d", file_name.c_str(), src_ns_addr.c_str(), ret);
              break;
            }
            if (rlen == 0)
            {
              break;
            }

            wlen = TfsClientImplV2::Instance()->write(dest_fd, data, rlen);
            if (wlen != rlen)
            {
              ret = wlen;
              TBSYS_LOG(INFO, "write %s fail when copy file, ns_addr: %s ret:%d", file_name.c_str(), dest_ns_addr.c_str(), ret);
              break;
            }

            if (rlen < MAX_READ_DATA_SIZE)
            {
              break;
            }
          }
        }

        if (source_fd > 0)
        {
          TfsClientImplV2::Instance()->close(source_fd);
        }
        if (dest_fd > 0)
        {
          ret = TfsClientImplV2::Instance()->close(dest_fd, NULL, 0, status);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(INFO, "close %s fail when copy file, ns_addr: %s ret:%d", file_name.c_str(), dest_ns_addr.c_str(), ret);
          }
        }
      }
      return ret;
    }

    int SyncUtil::sync_file(const std::string& saddr, const std::string& daddr, const std::string& filename,
      const common::FileInfoV2& sfinfo, const common::FileInfoV2& dfinfo, const int64_t timestamp, const bool force)
    {
      //1. 首先处理MODIFY TIME， 只有符合MODIFY TIME才进行处理
      //2. 处理强制标记，设置了强制标记的不需要关注源和目标的任何信息直接同步
      //3. 处理CRC，SIZE不一致的情况(包含了目标不存在的情况)
      //4. 处理状态不一致的情况
      int32_t ret = (!saddr.empty() && !daddr.empty() && !filename.empty()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = dfinfo.modify_time_ < timestamp ? TFS_SUCCESS : EXIT_SYNC_FILE_NOTHING;
        if (TFS_SUCCESS == ret)
        {
          if (force)//如果强制同步标记被设置，不需要关注源和目标的任何状态直接同步
          {
            ret = copy_file(saddr, daddr, filename, sfinfo.status_);
          }
          else
          {
            if (dfinfo.size_ != sfinfo.size_ || dfinfo.crc_ != sfinfo.crc_)
            {
              if (common::INVALID_FILE_ID != dfinfo.id_)
              {
                TBSYS_LOG(WARN, "%s size or crc conflict! fileid: %"PRI64_PREFIX"u <> %"PRI64_PREFIX"u, size: %d <> %d , crc %u <> %u , status: %d <> %d",
                  filename.c_str(), sfinfo.id_, dfinfo.id_, sfinfo.size_, dfinfo.size_, sfinfo.crc_, dfinfo.crc_, sfinfo.status_, dfinfo.status_);
              }
              ret = (0 == (sfinfo.status_ & FILE_STATUS_DELETE)) ? TFS_SUCCESS : EXIT_SYNC_FILE_NOTHING;
              if (TFS_SUCCESS == ret)
                ret = copy_file(saddr, daddr, filename, sfinfo.status_);
              else
                TBSYS_LOG(WARN, "ignore filename: %s althought it is not exist in %s, its status is 'DELETE' in %s , unlink is set 'false', ret: %d",
                    filename.c_str(), daddr.c_str(), saddr.c_str(), ret);
            }
            if (dfinfo.status_ != sfinfo.status_
              && common::INVALID_FILE_ID != dfinfo.id_)
            {
              TBSYS_LOG(WARN, "%s status conflict! size: %d <> %d , crc %u <> %u , status: %d <> %d , create_time: %s <> %s , modify_time: %s <> %s",
                  filename.c_str(), sfinfo.size_, dfinfo.size_, sfinfo.crc_, dfinfo.crc_, sfinfo.status_, dfinfo.status_,
                  Func::time_to_str(sfinfo.create_time_).c_str(), Func::time_to_str(dfinfo.create_time_).c_str(),
                  Func::time_to_str(sfinfo.modify_time_).c_str(), Func::time_to_str(dfinfo.modify_time_).c_str());
              int64_t file_size = 0;
              int32_t override_action = 0;
              SET_OVERRIDE_FLAG(override_action, sfinfo.status_);
              ret = TfsClientImplV2::Instance()->unlink(file_size, filename.c_str(), NULL,
                  static_cast<TfsUnlinkType>(override_action), daddr.c_str(), TFS_FILE_NO_SYNC_LOG);
              if (TFS_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "sync %s status %s, ret: %d, size: %d <> %d , crc %u <> %u , status: %d <> %d , create_time: %s <> %s , modify_time: %s <> %s",
                    filename.c_str(), TFS_SUCCESS == ret ? "successful" : "failed", ret, sfinfo.size_, dfinfo.size_, sfinfo.crc_, dfinfo.crc_, sfinfo.status_, dfinfo.status_,
                    Func::time_to_str(sfinfo.create_time_).c_str(), Func::time_to_str(dfinfo.create_time_).c_str(),
                    Func::time_to_str(sfinfo.modify_time_).c_str(), Func::time_to_str(dfinfo.modify_time_).c_str());
              }
            }
          }
        }
        else
        {
          TBSYS_LOG(WARN, "dest file %s has been modifyed, do nothing %s > %s, ret: %d",
              filename.c_str(), Func::time_to_str(dfinfo.modify_time_).c_str(), Func::time_to_str(timestamp).c_str(), ret);
        }
      }
      return ret;
    }

    int SyncUtil::cmp_and_sync_file(const std::string& src_ns_addr, const std::string& dest_ns_addr, const std::string& file_name,
        const int64_t timestamp, const bool force, common::FileInfoV2& left, common::FileInfoV2& right, const bool confirm)
    {
      int32_t ret = (!src_ns_addr.empty() && !dest_ns_addr.empty() && !file_name.empty()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret && confirm)
      {
        memset(&left, 0, sizeof(left));
        ret =read_file_info_v2(src_ns_addr, file_name, left);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "read %s file info fail from %s, ret: %d", file_name.c_str(), src_ns_addr.c_str(), ret);
        }
      }
      if (TFS_SUCCESS == ret && confirm)
      {
        memset(&right, 0, sizeof(right));
        ret =read_file_info_v2(dest_ns_addr, file_name, right);
        if (EXIT_BLOCK_NOT_FOUND == ret || EXIT_NO_DATASERVER == ret || EXIT_META_NOT_FOUND_ERROR == ret)
        {
          ret = TFS_SUCCESS;
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret = sync_file(src_ns_addr, dest_ns_addr, file_name, left, right, timestamp, force);
      }
      return ret;
    }

  }
}

