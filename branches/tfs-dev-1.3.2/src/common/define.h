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
 *
 */
#ifndef TFS_COMMON_DEFINE_H_
#define TFS_COMMON_DEFINE_H_

#include <string>
#include <map>
#include <vector>
#include <set>
#include <ext/hash_map>
#include <string.h>
#include <stdint.h>

#if __WORDSIZE == 32
namespace __gnu_cxx
{
  template<> struct hash<uint64_t>
  {
    uint64_t operator()(uint64_t __x) const
    {
      return __x;
    }
  };
}
#endif

namespace tfs
{
  namespace common
  {
#define DISALLOW_COPY_AND_ASSIGN(TypeName)\
      TypeName(const TypeName&);\
      void operator=(const TypeName&)

#if __WORDSIZE == 64
#define PRI64_PREFIX "l"
#else
#define PRI64_PREFIX "ll"
#endif

#define CLIENT_POOL ClientManager::gClientManager
#define CLIENT_SEND_PACKET(serverId,packet,packetHandler,args) ClientManager::gClientManager.m_connmgr->sendPacket(serverId,packet,packetHandler,args)

    //typedef base type
    typedef std::vector<int64_t> VINT64;
    typedef std::vector<uint64_t> VUINT64;
    typedef std::vector<int32_t> VINT32;
    typedef std::vector<uint32_t> VUINT32;
    typedef std::vector<int32_t> VINT;
    typedef std::vector<uint32_t> VUINT;
    typedef std::vector<std::string> VSTRING;

#pragma pack(4)
    struct FileInfo
    {
      uint64_t id_; // file id
      int32_t offset_; // offset in block file
      int32_t size_; // file size
      int32_t usize_; // hold space
      int32_t modify_time_; // modify time
      int32_t create_time_; // create time
      int32_t flag_; // deleta flag
      uint32_t crc_; // crc value
    };

    struct IpAddr
    {
      uint32_t ip_;
      int32_t port_;
    };

#pragma pack()

    enum OperationMode
    {
      READ_MODE = 1,
      WRITE_MODE = 2,
      APPEND_MODE = 4,
      UNLINK_MODE = 8,
      NEWBLK_MODE = 16,
      NOLEASE_MODE = 32
    };

		enum UnlinkType
    {
      DELETE = 0,
      UNDELETE = 2,
      CONCEAL = 4,
      REVEAL = 6
    };

    enum
    {
      T_SEEK_SET = 0,
      T_SEEK_CUR,
      T_SEEK_END
    };

    enum OpenFlag
    {
      T_READ = 1,
      T_WRITE = 2,
      T_CREATE = 4,
      T_NEWBLK = 8,
      T_NOLEASE = 16,
      T_LARGE = 32
    };

    enum StatFlag
    {
      NORMAL_STAT = 0,
      FORCE_STAT
    };

    static const int TFS_SUCCESS = EXIT_SUCCESS;
    static const int TFS_ERROR = EXIT_FAILURE;

    static const int EXIT_INVALIDFD_ERROR = -1; // temporary use

    static const int32_t INT_SIZE = 4;
    static const int32_t INT64_SIZE = 8;
    static const int32_t MAX_PATH_LENGTH = 256;
    static const int32_t FILEINFO_SIZE = sizeof(FileInfo);
    static const int64_t TFS_MALLOC_MAX_SIZE = 0x00A00000;//10M

    static const int32_t SPEC_LEN = 32;
    static const int32_t MAX_RESPONSE_TIME = 30000;
    //fsname
    static const int32_t FILE_NAME_LEN = 18;
    static const int32_t FILE_NAME_EXCEPT_SUFFIX_LEN = 12;
    static const int32_t TFS_FILE_LEN = FILE_NAME_LEN + 1;
    static const int32_t ERR_MSG_SIZE = 512;
    static const int32_t MAX_FILE_NAME_LEN = 128;
    static const int32_t STANDARD_SUFFIX_LEN = 4;

    static const int32_t DEFAULT_BLOCK_CACHE_TIME = 5;
    static const int32_t DEFAULT_BLOCK_CACHE_ITEMS = 500000;

    static const int32_t ADMIN_WARN_DEAD_COUNT = 1;

    typedef std::vector<FileInfo*> FILE_INFO_LIST;
  }
}

#endif //TFS_COMMON_DEFINE_H_
