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
#ifndef TFS_MESSAGE_METANAMESERVERCLIENTMESSAGE_H_
#define TFS_MESSAGE_METANAMESERVERCLIENTMESSAGE_H_
#include "common/base_packet.h"
#include "name_meta_server/meta_info.h"
#include "name_meta_server/meta_server_service.h"
namespace tfs
{
  namespace message
  {
    class FilepathActionMessage: public common::BasePacket
    {
      public:
        FilepathActionMessage();
        virtual ~FilepathActionMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        inline void set_app_id(const int64_t app_id)
        {
          app_id_ = app_id;
        }
        inline int64_t get_app_id() const
        {
          return app_id_;
        }

        inline void set_user_id(const int64_t user_id)
        {
          user_id_ = user_id;
        }
        inline int64_t get_user_id() const
        {
          return user_id_;
        }

        inline void set_file_path(const char* file_path)
        {
          file_path_ = std::string(file_path);
        }
        inline const char* const get_file_path() const
        {
          return file_path_.c_str();
        }

        inline void set_new_file_path(const char* new_file_path)
        {
          new_file_path_ = std::string(new_file_path);
        }
        inline const char* get_new_file_path() const
        {
          return new_file_path_.c_str();
        }

        inline void set_action(const common::MetaActionOp action)
        {
          action_ = action;
        }
        inline common::MetaActionOp get_action() const
        {
          return action_;
        }

      protected:
        int64_t app_id_;
        int64_t user_id_;
        std::string file_path_;
        std::string new_file_path_;
        common::MetaActionOp action_;
    };

    class WriteFilepathMessage: public common::BasePacket
    {
      public:
        WriteFilepathMessage();
        virtual ~WriteFilepathMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        inline void set_app_id(const int64_t app_id)
        {
          app_id_ = app_id;
        }
        inline int64_t get_app_id() const
        {
          return app_id_;
        }

        inline void set_user_id(const int64_t user_id)
        {
          user_id_ = user_id;
        }
        inline int64_t get_user_id() const
        {
          return user_id_;
        }

        inline void set_file_path(const char* file_path)
        {
          file_path_ = std::string(file_path);
        }
        inline const char* get_file_path() const
        {
          return file_path_.c_str();
        }

        inline namemetaserver::FragInfo& get_frag_info()
        {
          return frag_info_;
        }
        inline void set_frag_info(const namemetaserver::FragInfo& frag_info)
        {
          frag_info_ = frag_info;
        }

      protected:
        int64_t app_id_;
        int64_t user_id_;
        std::string file_path_;
        namemetaserver::FragInfo frag_info_;
    };

    class ReadFilepathMessage: public common::BasePacket
    {
      public:
        ReadFilepathMessage();
        virtual ~ReadFilepathMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        inline void set_app_id(const int64_t app_id)
        {
          app_id_ = app_id;
        }
        inline int64_t get_app_id() const
        {
          return app_id_;
        }

        inline void set_user_id(const int64_t user_id)
        {
          user_id_ = user_id;
        }
        inline int64_t get_user_id() const
        {
          return user_id_;
        }

        inline void set_file_path(const char* file_path)
        {
          file_path_ = std::string(file_path);
        }
        inline const char* get_file_path() const
        {
          return file_path_.c_str();
        }

        inline void set_offset(const int64_t offset)
        {
          offset_ = offset;
        }
        inline int64_t get_offset() const
        {
          return offset_;
        }

        inline void set_size(const int64_t size)
        {
          size_ = size;
        }
        inline int64_t get_size() const
        {
          return size_;
        }

      protected:
        int64_t app_id_;
        int64_t user_id_;
        std::string file_path_;
        int64_t offset_;
        int64_t size_;
    };

    class RespReadFilepathMessage: public common::BasePacket
    {
      public:
        RespReadFilepathMessage();
        virtual ~RespReadFilepathMessage();
        virtual int serialize(common::Stream& output) const ;
        virtual int deserialize(common::Stream& input);
        virtual int64_t length() const;

        inline namemetaserver::FragInfo& get_frag_info()
        {
          return frag_info_;
        }
        inline void set_frag_info(const namemetaserver::FragInfo frag_info)
        {
          frag_info_ = frag_info;
        }

        inline void set_still_have(const bool still_have)
        {
          still_have_ = still_have;
        }
        inline bool get_still_have() const
        {
          return still_have_;
        }

      protected:
        namemetaserver::FragInfo frag_info_;
        bool still_have_;
    };
  }
}
#endif
