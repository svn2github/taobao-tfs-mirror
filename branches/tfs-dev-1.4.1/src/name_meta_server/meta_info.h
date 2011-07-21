/*
* (C) 2007-2010 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   daoan <daoan@taobao.com>
*      - initial release
*
*/

#ifndef TFS_NAMEMETASERVER_METAINFO_H_
#define TFS_NAMEMETASERVER_METAINFO_H_
#include <string>
namespace tfs
{
  namespace namemetaserver
  {
    class SlideInfo
    {
    };
    class MetaInfo
    {
      public:
        MetaInfo():
          pid_(-1), id_(0), create_time_(0), modify_time_(0),
          size_(0), ver_no_(0)
      {
      }
        int64_t pid_;
        int64_t id_;
        int32_t create_time_;
        int32_t modify_time_;
        int64_t size_;
        int16_t ver_no_;
        std::string name_;
        SlideInfo slide_info_;
    };
  }
}

#endif
