/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: meta_server_service.h 49 2011-08-08 09:58:57Z nayan@taobao.com $
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *      - initial release
 *
 */


#include "kvengine_helper.h"

namespace tfs
{
  using namespace common;
  namespace kvmetaserver
  {
    KvKey::KvKey()
      :key_(NULL), key_size_(0), key_type_(0)
    {
    }
    const char KvKey::DELIMITER = 7;

    KvValue::KvValue()
    {
    }
    KvValue::~KvValue()
    {
    }
    void KvValue::free()
    {
      delete this;
    }

    KvMemValue::KvMemValue()
    :data_(NULL), size_(0), is_owner_(false)
    {
    }
    KvMemValue::~KvMemValue()
    {
    }

    int32_t KvMemValue::get_size() const
    {
      return size_;
    }
    const char* KvMemValue::get_data() const
    {
      return data_;
    }
    void KvMemValue::set_data(char* data, const int32_t size)
    {
      if (is_owner_)
      {
        ::free(data_);
        data_ = NULL;
        is_owner_ = false;
      }
      data_ = data;
      size_ = size;
    }
    char* KvMemValue::malloc_data(const int32_t buffer_size)
    {
      if (NULL != data_ && is_owner_)
      {
        ::free(data_);
      }
      data_ = (char*) malloc(buffer_size);
      if (data_ == NULL)
      {
        TBSYS_LOG(ERROR, "malloc fail");
      }
      else
      {
        size_ = buffer_size;
        is_owner_ = true;
      }
      return data_;
    }
    void KvMemValue::free()
    {
      if(is_owner_)
      {
        ::free(data_);
        data_ = NULL;
        is_owner_ = false;
      }
      delete this;
    }

  }
}

