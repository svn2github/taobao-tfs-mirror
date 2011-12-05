/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */


#include "new_client/fsname.h"
#include "common/internal.h"
#include "common/status_message.h"
#include "common/client_manager.h"
#include "common/directory_op.h"
#include "message/server_status_message.h"
#include "message/block_info_message.h"
#include "message/message_factory.h"
#include "tools/util/tool_util.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::tools;

int remove_block(const string& ns_ip, const string& file_path, const string& out_file_path)
{
  int ret = TFS_ERROR;
  FILE* fp = fopen(file_path.c_str(), "r");
  if (NULL == fp)
  {
    TBSYS_LOG(ERROR, "open %s failed, %s %d", file_path.c_str(), strerror(errno), errno);
  }
  else
  {
    FILE* fp_out = fopen(out_file_path.c_str(), "r");
    if (NULL == fp_out)
    {
      TBSYS_LOG(ERROR, "open %s failed, %s %d", out_file_path.c_str(), strerror(errno), errno);
    }
    else
    {
      uint32_t block_id = 0;
      while (fscanf(fp, "%u\n", &block_id) != EOF)
      {
        sleep(2);
        VUINT64 ds_list;
        ToolUtil::get_block_ds_list(Func::get_host_ip(ns_ip.c_str()), block_id, ds_list);
        if (ds_list.empty())
        {
          TBSYS_LOG(ERROR, "remove block: %u failed, ds_list empty", block_id);
        }
        else
        {
          NewClient* client = NULL;
          tbnet::Packet* ret_msg= NULL;
          ds_list.push_back(tbsys::CNetUtil::strToAddr("172.24.80.8", 38000));
          VUINT64::const_iterator iter = ds_list.begin();
          for (ret = TFS_SUCCESS; iter != ds_list.end() && TFS_SUCCESS == ret; ++iter)
          {
            ClientCmdMessage ns_req_msg;
            ns_req_msg.set_value3(block_id);
            ns_req_msg.set_value1((*iter));
            client = NewClientManager::get_instance().create_client();
            ret = NULL != client ? TFS_SUCCESS : TFS_ERROR;
            if (TFS_SUCCESS == ret)
            {
              TBSYS_LOG(ERROR, "remove block: %u failed from nameserver: %s", block_id, ns_ip.c_str());
            }
            else
            {
              ret = send_msg_to_server((*iter), client, &ns_req_msg, ret_msg);
              if (TFS_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "remove block: %u failed from nameserver: %s, send msg error: ret: %d", block_id, ns_ip.c_str(), ret);
              }
              else
              {
                ret = ret_msg->getPCode() == STATUS_MESSAGE ? TFS_SUCCESS : TFS_ERROR;
                if (TFS_SUCCESS != ret)
                {
                  TBSYS_LOG(ERROR, "remove block: %u failed from nameserver: %s, get error response, pcode: %d",
                    block_id, ns_ip.c_str(), ret_msg->getPCode());
                }
                else
                {
                  TBSYS_LOG(INFO, "remove block: %u successful from nameserver: %s", block_id, ns_ip.c_str());
                }
              }
              NewClientManager::get_instance().destroy_client(client);
            }
          }

          ret_msg = NULL;
          iter = ds_list.begin();
          for (ret = TFS_SUCCESS; iter != ds_list.end() - 1 && TFS_SUCCESS == ret; ++iter)
          {
            RemoveBlockMessage req_msg;
            req_msg.add_remove_id(block_id);
            client = NewClientManager::get_instance().create_client();
            ret = NULL != client ? TFS_SUCCESS : TFS_ERROR;
            if (TFS_SUCCESS == ret)
            {
              TBSYS_LOG(ERROR, "remove block: %u failed from dataserver: %s", block_id, tbsys::CNetUtil::addrToString((*iter)).c_str());
            }
            else
            {
              ret = send_msg_to_server((*iter), client, &req_msg, ret_msg);
              if (TFS_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "remove block: %u failed from dataserver: %s,send msg error", block_id, tbsys::CNetUtil::addrToString((*iter)).c_str());
              }
              else
              {
                TBSYS_LOG(INFO, "remove block: %u successful from nameserver: %s", block_id, ns_ip.c_str());
              }
              NewClientManager::get_instance().destroy_client(client);
            }
          }
        }
      }
      fclose(fp_out);
    }
    fclose(fp);
  }
  return ret;
}

void usage(const char* name)
{
  fprintf(stderr, "Usage: %s -s nameserver ip -f file paths -o out file paths -l loglevel\n", name);
  fprintf(stderr, "       -d nameserver ip\n");
  fprintf(stderr, "       -f file path\n");
  fprintf(stderr, "       -o out file path\n");
  fprintf(stderr, "       -l log level\n");
  fprintf(stderr, "       -h help\n");
  exit(0);
}

int main(int argc, char** argv)
{
  int iret = TFS_ERROR;
  if(argc < 3)
  {
    usage(argv[0]);
  }

  string file_path;
  string dest_ns_ip;
  string out_file_path;
  string log_level("error");
  int i ;
  while ((i = getopt(argc, argv, "o:d:f:l:h")) != EOF)
  {
    switch (i)
    {
      case 'o':
        out_file_path = optarg;
        break;
      case 'f':
        file_path = optarg;
        break;
      case 'l':
        log_level = optarg;
        break;
      case 'd':
        dest_ns_ip = optarg;
        break;
      case 'h':
      default:
        usage(argv[0]);
    }
  }

  iret = !out_file_path.empty() && !file_path.empty() && dest_ns_ip.c_str() ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS != iret)
  {
    usage(argv[0]);
  }

  TBSYS_LOGGER.setLogLevel(log_level.c_str());

  iret = access(file_path.c_str(), R_OK|F_OK);
  if (0 != iret)
  {
    TBSYS_LOG(ERROR, "open %s failed, error: %s ", file_path.c_str(), strerror(errno));
  }
  else
  {
    iret = access(out_file_path.c_str(), R_OK|F_OK);
    if (0 != iret)
    {
      TBSYS_LOG(ERROR, "open %s failed, error: %s ", out_file_path.c_str(), strerror(errno));
    }
    else
    {
      MessageFactory* factory = new MessageFactory();
      BasePacketStreamer* streamer = new BasePacketStreamer(factory);
      NewClientManager::get_instance().initialize(factory, streamer);
      iret = remove_block(dest_ns_ip, file_path, out_file_path);
      NewClientManager::get_instance().destroy();
      tbsys::gDelete(factory);
      tbsys::gDelete(streamer);
    }
  }
  return iret;
}
