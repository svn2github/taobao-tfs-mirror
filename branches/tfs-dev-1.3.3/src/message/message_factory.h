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
#ifndef TFS_MESSAGE_MESSAGEFACTORY_H_
#define TFS_MESSAGE_MESSAGEFACTORY_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <errno.h>

#include "message.h"
#include "dataserver_message.h"
#include "status_message.h"
#include "block_info_message.h"
#include "close_file_message.h"
#include "read_data_message.h"
#include "write_data_message.h"
#include "unlink_file_message.h"
#include "replicate_block_message.h"
#include "compact_block_message.h"
#include "server_status_message.h"
#include "file_info_message.h"
#include "rename_file_message.h"
#include "client_cmd_message.h"
#include "create_filename_message.h"
#include "rollback_message.h"
#include "heart_message.h"
#include "reload_message.h"
#include "server_meta_info_message.h"
#include "oplog_sync_message.h"
#include "crc_error_message.h"
#include "admin_cmd_message.h"
#include "dump_plan_message.h"

namespace tfs
{
  namespace message
  {
    class MessageFactory: public tbnet::IPacketFactory
    {
      public:
        MessageFactory();
        virtual ~MessageFactory();
        static int send_error_message(Message* packet, int32_t level, char* file, int32_t line, const char* function,
            uint64_t server_id, char* fmt, ...);
        static int send_error_message(Message* packet, int32_t level, char* file, int32_t line, const char* function,
            int32_t err_code, uint64_t server_id, char* fmt, ...);

      public:
        tbnet::Packet* createPacket(int32_t pcode);
        Message* clone_message(Message* message, int32_t version = 2, bool deserialize = false);

      protected:
        Message* create_message(int32_t type);
        CREATE_MESSAGE_MAP create_message_map_;
    };
  }
}
#endif //TFS_MESSAGE_MESSAGEFACTORY_H_
