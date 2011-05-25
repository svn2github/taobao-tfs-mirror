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
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#include "rc_session_message.h"
#include "common/base_packet.h"

namespace tfs
{
  namespace message 
  {
    using namespace common;
    using namespace std;

    ReqRcLoginMessage::ReqRcLoginMessage() :
      app_ip_(0), length_(0)
    {
      app_key_[0] = '\0';
      _packetHeader._pcode = REQ_RC_LOGIN_MESSAGE;
    }

    ReqRcLoginMessage::~ReqRcLoginMessage()
    {
    }

    int ReqRcLoginMessage::set_app_key(const char* app_key)
    {
      int ret = TFS_SUCCESS;
      if (NULL == app_key)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "set_app_key fail, app_key is null, ret: %d", ret);
      }
      else
      {
        length_ = strlen(app_key) + 1; // add '\0'
        if (length_ > MAX_PATH_LENGTH)
        {
          ret = EXIT_INVALID_ARGU;
          TBSYS_LOG(ERROR, "set_app_key fail. app_key: %s, len: %d, ret: %d", app_key, length_, ret);
        }
        else
        {
          strncpy(app_key_, app_key, length_);
        }
      }
      return ret;
    }

    void ReqRcLoginMessage::set_app_ip(const int64_t app_ip)
    {
      app_ip_ = app_ip;
    }

    int ReqRcLoginMessage::serialize(Stream& output)
    {
      int ret = TFS_SUCCESS;
      if (length_ > 0)
      {
        ret = output.set_int32(length_);
        if (TFS_SUCCESS == ret)
        {
          if ((ret = output.set_bytes(app_key_, length_)) == TFS_SUCCESS)
          {
            ret = output.set_int64(app_ip_);
          }
        }
      }
      return ret;
    }

    int ReqRcLoginMessage::deserialize(Stream& input)
    {
      int ret = input.get_int32(&length_);
      if (TFS_SUCCESS == ret)
      {
        if (length_ > MAX_PATH_LENGTH || length_ <= 0)
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "error msg length: %d invalid. define length: %d", length_, MAX_ERROR_MSG_LENGTH);
        }
        else
        {
          ret = input.get_bytes(app_key_, length_);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = input.get_int64(&app_ip_);
        }
      }
      return ret;
    }

    int64_t ReqRcLoginMessage::length() const
    {
      int64_t tmp = length_ > 0 ? INT_SIZE + length_ : INT_SIZE;
      return INT64_SIZE + tmp;
    }

    void ReqRcLoginMessage::dump() const
    {
      TBSYS_LOG(DEBUG, "app_key: %s, app_ip: %"PRI64_PREFIX"d", app_key_, app_ip_); 
    }

    BasePacket* ReqRcLoginMessage::create(const int32_t type)
    {
      return new (std::nothrow) ReqRcLoginMessage();
    }

    const char* ReqRcLoginMessage::get_app_key() const
    {
      return app_key_;
    }

    int64_t ReqRcLoginMessage::get_app_ip() const
    {
      return app_ip_;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    RspRcLoginMessage::RspRcLoginMessage()
    {
      session_id_[0] = '\0';
      _packetHeader._pcode = RSP_RC_LOGIN_MESSAGE;
    }

    RspRcLoginMessage::~RspRcLoginMessage()
    {
    }

    int RspRcLoginMessage::set_session_id(const char* session_id)
    {
      int ret = TFS_SUCCESS;
      if (NULL == session_id)
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "set_session_id fail, session_id is null, ret: %d", ret);
      }
      else
      {
        length_ = strlen(session_id) + 1;
        if (length_ > MAX_PATH_LENGTH)
        {
          ret = EXIT_INVALID_ARGU;
          TBSYS_LOG(ERROR, "set_session_id fail. session_id: %s, len: %d, ret: %d", session_id, length_, ret);
        }
        else
        {
          strncpy(session_id_, session_id, length_);
        }
      }
      return ret;
    }

    void RspRcLoginMessage::set_base_info(const BaseInfo& base_info)
    {
      base_info_ = base_info;
    }

    int RspRcLoginMessage::serialize(Stream& output)
    {
      int ret = TFS_SUCCESS;
      if (length_ > 0)
      {
        ret = output.set_int32(length_);
        if (TFS_SUCCESS == ret)
        {
          ret = output.set_bytes(session_id_, length_);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = base_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          output.pour(base_info_.length());
        }
      }
      return ret;
    }

    int RspRcLoginMessage::deserialize(Stream& input)
    {
      int ret = input.get_int32(&length_);
      if (TFS_SUCCESS == ret)
      {
        if (length_ > MAX_PATH_LENGTH || length_ <= 0)
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "error msg length: %d invalid. define length: %d", length_, MAX_ERROR_MSG_LENGTH);
        }
        else
        {
          ret = input.get_bytes(session_id_, length_);
        }

        if (TFS_SUCCESS == ret)
        {
          int64_t pos = 0;
          ret = base_info_.deserialize(input.get_data(), input.get_data_length(), pos);
          if (TFS_SUCCESS == ret)
          {
            input.drain(base_info_.length());
          }
        }
      }
      return ret;
    }

    int64_t RspRcLoginMessage::length() const
    {
      int64_t tmp = length_ > 0 ? INT_SIZE + length_ : INT_SIZE;
      return INT64_SIZE + tmp;
    }

    void RspRcLoginMessage::dump() const
    {
      TBSYS_LOG(DEBUG, "session_id: %s", session_id_); 
    }

    BasePacket* RspRcLoginMessage::create(const int32_t type)
    {
      return new (std::nothrow) RspRcLoginMessage();
    }

    const char* RspRcLoginMessage::get_session_id() const
    {
      return session_id_;
    }

    const BaseInfo& RspRcLoginMessage::get_base_info() const
    {
      return base_info_;
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    ReqRcKeepAliveMessage::ReqRcKeepAliveMessage()
    {
      _packetHeader._pcode = REQ_RC_KEEPALIVE_MESSAGE;
    }
    ReqRcKeepAliveMessage::~ReqRcKeepAliveMessage()
    {
    }

    void ReqRcKeepAliveMessage::set_ka_info(const KeepAliveInfo& ka_info)
    {
      ka_info_ = ka_info;
    }

    int ReqRcKeepAliveMessage::serialize(Stream& output)
    {
      int ret = TFS_SUCCESS;
      int64_t pos = 0;
      ret = ka_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        output.pour(ka_info_.length());
      }
      return ret;
    }

    int ReqRcKeepAliveMessage::deserialize(Stream& input)
    {
      int ret = TFS_SUCCESS;
      int64_t pos = 0;
      ret = ka_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drain(ka_info_.length());
      }
      return ret;
    }
   
    int64_t ReqRcKeepAliveMessage::length() const
    {
      return ka_info_.length();
    }

    const KeepAliveInfo& ReqRcKeepAliveMessage::get_ka_info() const
    {
      return ka_info_;
    }

    void ReqRcKeepAliveMessage::dump() const
    {
      //Todo
      return;
    }

    BasePacket* ReqRcKeepAliveMessage::create(const int32_t type)
    {
      return new (std::nothrow) ReqRcKeepAliveMessage();
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    RspRcKeepAliveMessage::RspRcKeepAliveMessage()
      : update_flag_(KA_FLAG)
    {
      _packetHeader._pcode = RSP_RC_KEEPALIVE_MESSAGE;
    }
    RspRcKeepAliveMessage::~RspRcKeepAliveMessage()
    {
    }

    void RspRcKeepAliveMessage::set_update_flag(const UpdateFlag update_flag)
    {
      update_flag_ = update_flag;
    }
    void RspRcKeepAliveMessage::set_base_info(const BaseInfo& base_info)
    {
      base_info_ = base_info;
    }

    int RspRcKeepAliveMessage::serialize(Stream& output)
    {
      int ret = TFS_SUCCESS;
      ret = output.set_int8(update_flag_);
      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = base_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          output.pour(base_info_.length());
        }
      }
      return ret;
    }
    int RspRcKeepAliveMessage::deserialize(Stream& input)
    {
      int ret = input.get_int8(reinterpret_cast<int8_t*>(&update_flag_));
      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = base_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          input.drain(base_info_.length());
        }
      }
      return ret;
    }

    int64_t RspRcKeepAliveMessage::length() const
    {
      return INT8_SIZE + base_info_.length();
    }

    bool RspRcKeepAliveMessage::get_update_flag() const
    {
      return update_flag_;
    }

    const BaseInfo& RspRcKeepAliveMessage::get_base_info() const
    {
      return base_info_;
    }

    void RspRcKeepAliveMessage::dump() const
    {
    }

    BasePacket* RspRcKeepAliveMessage::create(const int32_t type)
    {
      return new (std::nothrow) RspRcKeepAliveMessage();
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    ReqRcLogoutMessage::ReqRcLogoutMessage()
    {
      _packetHeader._pcode = REQ_RC_LOGOUT_MESSAGE;
    }
    ReqRcLogoutMessage::~ReqRcLogoutMessage()
    {
    }

    BasePacket* ReqRcLogoutMessage::create(const int32_t type)
    {
      return new (std::nothrow) ReqRcLogoutMessage();
    }
  }
}
