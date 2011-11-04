/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: compare_crc.cpp 445 2011-06-08 09:27:48Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "common/status_message.h"
#include "message/server_status_message.h"
#include "message/block_info_message.h"
#include "tools/util/tool_util.h"
#include "compare_crc.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::client;
using namespace tfs::message;
using namespace tfs::tools;

TfsClient* g_tfs_client = NULL;
FILE *g_succ_file = NULL, *g_fail_file = NULL, *g_error_file = NULL, *g_unsync_file = NULL;

static const int32_t META_FLAG_ABNORMAL = -9800;
struct log_file g_log_fp[] =
{
  {&g_succ_file, "/succ_file"},
  {&g_fail_file, "/fail_file"},
  {&g_error_file, "/error_file"},
  {&g_unsync_file, "/unsync_file"},
  {NULL, NULL}
};

int init_log_file(char* dir_path)
{
  char log_path[256];
  snprintf(log_path, 256, "%s%s", dirname(dir_path), "/cmp_log");
  DirectoryOp::create_directory(log_path);
  for (int i = 0; g_log_fp[i].file_; i++)
  {
    char file_path[256];
    snprintf(file_path, 256, "%s%s", log_path, g_log_fp[i].file_);
    *g_log_fp[i].fp_ = fopen(file_path, "w");
    if (!*g_log_fp[i].fp_)
    {
      TBSYS_LOG(ERROR, "open file fail %s : %s:", g_log_fp[i].file_, strerror(errno));
      return TFS_ERROR;
    }
  }
  return TFS_SUCCESS;
}
int get_meta_crc(const string& tfs_client, const char* tfs_file_name, TfsFileStat* finfo)
{
  if (finfo == NULL)
  {
    TBSYS_LOG(WARN, "invalid pointer");
    return TFS_ERROR;
  }
  int tfs_fd = 0;
  if ((tfs_fd = g_tfs_client->open(tfs_file_name, NULL, tfs_client.c_str(), T_READ)) < 0)
  {
    TBSYS_LOG(WARN, "open tfsfile fail");
    return TFS_ERROR;
  }

  if (g_tfs_client->fstat(tfs_fd, finfo, FORCE_STAT) != TFS_SUCCESS)
  {
    TBSYS_LOG(WARN, "fstat tfsfile fail: %s", tfs_file_name);
    g_tfs_client->close(tfs_fd);
    return TFS_ERROR;
  }

  if (finfo->flag_ == 1 || finfo->flag_ == 4 || finfo->flag_ == 5)
  {
    g_tfs_client->close(tfs_fd);
    return META_FLAG_ABNORMAL;
  }
  g_tfs_client->close(tfs_fd);
  return TFS_SUCCESS;
}

int get_real_crc(const string& tfs_client, const char* tfs_file_name, uint32_t* crc)
{
  if(crc == NULL)
  {
    TBSYS_LOG(ERROR,"Pointer to real crc value is NULL");
    return TFS_ERROR;
  }

  int tfs_fd = 0;
  if ((tfs_fd = g_tfs_client->open(tfs_file_name, NULL, tfs_client.c_str(), T_READ)) < 0)
  {
    TBSYS_LOG(ERROR, "open tfsfile fail file name %s", tfs_file_name);
    return TFS_ERROR;
  }

  char data[MAX_READ_SIZE]={'\0'};
  uint32_t crc_tmp = 0;
  int total_size = 0,rlen=0;
  for(;;)
  {
    rlen = g_tfs_client->read(tfs_fd, data, MAX_READ_SIZE);
    if (rlen < 0)
    {
      TBSYS_LOG(ERROR, "read tfsfile fail: file_name %s", tfs_file_name);
      g_tfs_client->close(tfs_fd);
      return TFS_ERROR;
    }
    if (rlen == 0)
      break;
    crc_tmp = Func::crc(crc_tmp, data, rlen);
    total_size += rlen;
    if (rlen != MAX_READ_SIZE)
      break;
  }
  *crc = crc_tmp;
  g_tfs_client->close(tfs_fd);
  return TFS_SUCCESS;
}

int get_crc_from_filename(const string& old_tfs_client, const string& new_tfs_client,
    const char* tfs_file_name, string& modify_time)
{
  int ret = TFS_SUCCESS;
  uint32_t old_real_crc = 0;
  uint32_t new_real_crc = 0;
  TfsFileStat old_info, new_info;
  memset(&old_info, 0, sizeof(TfsFileStat));
  memset(&new_info, 0, sizeof(TfsFileStat));

  bool skip_flag = false;
  cmp_stat_.total_count_++;
  // failed to get meta crc in old cluster
  int src_ret = get_meta_crc(old_tfs_client, tfs_file_name, &old_info);
  if (TFS_SUCCESS != src_ret && META_FLAG_ABNORMAL != src_ret)  //no need to stat old file
  {
    TBSYS_LOG(ERROR, "%s failed in old cluster, ret: %d", tfs_file_name, src_ret);
    ret = TFS_ERROR;
  }
  else
  {
    int dest_ret = get_meta_crc(new_tfs_client, tfs_file_name, &new_info);
    //check src modify time
    if (old_info.modify_time_ >= tbsys::CTimeUtil::strToTime(const_cast<char*>(modify_time.c_str())))
    {
      //skip
      skip_flag = true;
      cmp_stat_.skip_count_++;
    }
    else
    {
      if (META_FLAG_ABNORMAL == src_ret) // flag modify in old cluster, check new flag
      {
        if (TFS_ERROR == dest_ret) //dest file is not exist
        {
          if (old_info.flag_ & 1) //src is delete
          {
          }
          else
          {
            ret = TFS_ERROR;
            TBSYS_LOG(ERROR, "%s not exist in new cluster, src flag: %d",
                tfs_file_name, old_info.flag_);
          }
        }
        else if (META_FLAG_ABNORMAL == dest_ret) //dest file flag is abnormal
        {
          if (old_info.flag_ & 1 && !(new_info.flag_ & 1)) //fail
          {
            ret = TFS_ERROR;
          }
          else if (!(old_info.flag_ & 1) && (old_info.flag_ & 4) && !(new_info.flag_ & 4))
          {
            ret = TFS_ERROR;
          }

          if (TFS_ERROR == ret)
          {
            TBSYS_LOG(ERROR, "%s flag is conflict, src flag: %d, dest flag: %d",
                tfs_file_name, old_info.flag_, new_info.flag_);
          }
        }
        else //dest file is normal
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "%s flag is conflict, src flag: %d, dest flag: %d",
              tfs_file_name, old_info.flag_, new_info.flag_);
        }
      }
      else // success
      {
        if (TFS_SUCCESS != dest_ret)
        {
          ret = TFS_ERROR;
          TBSYS_LOG(ERROR, "%s fail, src flag: %d, dest flag: %d, dest ret: %d",
              tfs_file_name, old_info.flag_, new_info.flag_, dest_ret);
        }
        // compare real crc
      }
    }
  }

  if (TFS_ERROR == ret)
  {
    if (TFS_SUCCESS != src_ret && META_FLAG_ABNORMAL != src_ret)
    {
      fprintf(g_error_file, "%s\n", tfs_file_name);
      cmp_stat_.error_count_++;
    }
    else
    {
      fprintf(g_fail_file, "%s\n", tfs_file_name);
      cmp_stat_.fail_count_++;
    }
  }
  else if (META_FLAG_ABNORMAL == src_ret)
  {
    //do nothing
  }
  else
  {
    if (!skip_flag)
    {
      if (get_real_crc(new_tfs_client, tfs_file_name, &new_real_crc) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "get dest file fail: %s",tfs_file_name);
        fprintf(g_fail_file, "%s\n", tfs_file_name);
        cmp_stat_.error_count_++;
      }
      else
      {
        if ((old_info.crc_ == new_info.crc_) && (old_info.crc_ == new_real_crc))
        {
          TBSYS_LOG(DEBUG, "%s success",tfs_file_name);
          fprintf(g_succ_file, "%s\n", tfs_file_name);
          cmp_stat_.succ_count_++;
        }
        else
        {
          if (get_real_crc(old_tfs_client, tfs_file_name, &old_real_crc) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "get src file fail: %s",tfs_file_name);
            fprintf(g_error_file, "%s\n", tfs_file_name);
            cmp_stat_.error_count_++;
          }
          else
          {
            if (old_info.crc_ != old_real_crc)
            {
              TBSYS_LOG(ERROR, "%s failed as : old cluster crc not same",tfs_file_name);
              fprintf(g_error_file, "%s\n", tfs_file_name);
              cmp_stat_.error_count_++;
              ret = TFS_ERROR;
            }
            else
            {
              int tfs_fd_old = 0;
              tfs_fd_old = g_tfs_client->open(tfs_file_name, NULL, old_tfs_client.c_str(), T_READ);
              TfsFileStat finfo;
              g_tfs_client->fstat(tfs_fd_old, &finfo);
              FSName fsname;
              fsname.set_name(tfs_file_name);

              if(finfo.modify_time_ <= tbsys::CTimeUtil::strToTime(const_cast<char*>(modify_time.c_str())))
              {
                TBSYS_LOG(ERROR, "%s failed with Other reason, at block : %u : %s: %u %u %u %u",tfs_file_name,
                    fsname.get_block_id(),
                    Func::time_to_str(finfo.modify_time_).c_str(),
                    old_info.crc_, old_real_crc,
                    new_info.crc_, new_real_crc);
                fprintf(g_fail_file, "%s\n", tfs_file_name);
                cmp_stat_.fail_count_++;
              }
              else
              {
                TBSYS_LOG(ERROR, "%s failed as modified at %s",tfs_file_name,Func::time_to_str(finfo.modify_time_).c_str());
                fprintf(g_unsync_file, "%s\n", tfs_file_name);
                cmp_stat_.unsync_count_++;
              }
              g_tfs_client->close(tfs_fd_old);
            }
          }
        }
      }
    }
  }
  return ret;
}

int get_crc_from_tfsname_list(const string& old_tfs_client, const string& new_tfs_client, const char* filename_list, string& modify_time)
{
  int ret = TFS_ERROR;
  FILE *fp;
  fp = fopen(filename_list, "r");
  if (fp != NULL)
  {
    TBSYS_LOG(INFO,"Open file name list %s",filename_list);
    char tmp_tfs_file_name[32]={'\0'};
    while (fscanf(fp,"%s\n",tmp_tfs_file_name) != EOF)
    {
      get_crc_from_filename(old_tfs_client, new_tfs_client, tmp_tfs_file_name, modify_time);
    }
    fclose(fp);
    ret = TFS_SUCCESS;
  }
  else
  {
    TBSYS_LOG(ERROR,"Open file name list failed");
  }
  return ret;
}

int get_crc_from_block_list(const string& old_tfs_client, const string& new_tfs_client, const char* block_list, string& modify_time)
{
  int ret = TFS_ERROR;
  FILE *fp;
  fp = fopen(block_list, "r");
  if (fp != NULL)
  {
    TBSYS_LOG(INFO,"open block list %s", block_list);
    uint32_t block_id = 0;
    while (fscanf(fp, "%u\n", &block_id) != EOF)
    {
      VUINT64 ds_list;
      ToolUtil::get_block_ds_list(Func::get_host_ip(old_tfs_client.c_str()), block_id, ds_list);
      if(ds_list.size() > 0)
      {
        uint64_t ds_id = ds_list[0];

        FILE_INFO_LIST file_list;
        GetServerStatusMessage gss_message;
        gss_message.set_status_type(GSS_BLOCK_FILE_INFO);
        gss_message.set_return_row(block_id);

        NewClient* client = NewClientManager::get_instance().create_client();
        tbnet::Packet* ret_message = NULL;
        if (NULL != client)
        {
          if (TFS_SUCCESS == send_msg_to_server(ds_id, client, &gss_message, ret_message))
          {
            if (ret_message->getPCode() == BLOCK_FILE_INFO_MESSAGE)
            {
              BlockFileInfoMessage *bfi_message = (BlockFileInfoMessage*)ret_message;
              FILE_INFO_LIST* file_info_list = bfi_message->get_fileinfo_list();
              FILE_INFO_LIST::iterator vit = file_info_list->begin();
              for ( ; vit != file_info_list->end(); ++vit)
              {
                file_list.push_back(*vit);
              }
            }
            else if (ret_message->getPCode() == STATUS_MESSAGE)
            {
              StatusMessage* sm = reinterpret_cast<StatusMessage*> (ret_message);
              if (sm->get_error() != NULL)
              {
                TBSYS_LOG(ERROR, "%s", sm->get_error());
              }
            }
          }
          NewClientManager::get_instance().destroy_client(client);
        }
        else
        {
          TBSYS_LOG(ERROR,"create client error");
        }

        if (TFS_ERROR != ret)
        {
          FILE_INFO_LIST::iterator vit = file_list.begin();
          int i = 0;
          for ( ; vit != file_list.end(); ++vit)
          {
            ++i;
            FSName fsname;
            fsname.set_block_id(block_id);
            fsname.set_file_id(vit->id_);
            TBSYS_LOG(DEBUG, "gene file i: %d, blockid: %u, fileid: %"PRI64_PREFIX"u, %s",
                i, block_id, (vit)->id_, fsname.get_name());
            get_crc_from_filename(old_tfs_client, new_tfs_client, fsname.get_name(), modify_time);
          }

          file_list.clear();
        }
      }
      else
      {
        TBSYS_LOG(ERROR,"get block info for blockid: %u failure.",block_id);
      }
    }
    fclose(fp);
  }
  else
  {
    TBSYS_LOG(ERROR,"open block list failed");
  }
  return ret;
}

void usage(const char* name)
{
  TBSYS_LOG(ERROR,"Usage:%s -o old_ns_ip -n new_ns_ip -f file_list -d modify_time (20110315|20110315183500) [-h]", name);
  TBSYS_LOG(ERROR,"Usage:%s -o old_ns_ip -n new_ns_ip -b block_list -d modify_time(20110315|20110315183500) [-h]", name);
  exit(TFS_ERROR);
}

int main(int argc, char** argv)
{
  int iret = TFS_ERROR;
  if(argc < 9)
  {
    usage(argv[0]);
  }

  string old_ns_ip = "", new_ns_ip = "";
  string tfs_file_name = "";
  string modify_time = "";
  string block_list = "";
  int i ;
  while ((i = getopt(argc, argv, "o:n:f:b:d:h")) != EOF)
  {
    switch (i)
    {
      case 'o':
        old_ns_ip = optarg;
        break;
      case 'n':
        new_ns_ip = optarg;
        break;
      case 'f':
        tfs_file_name = optarg;
        break;
      case 'b':
        block_list = optarg;
        break;
      case 'd':
        modify_time = optarg;
        break;
      case 'h':
      default:
        usage(argv[0]);
    }
  }

  int time_len = modify_time.length();
  if (old_ns_ip.empty() || old_ns_ip == " "
      || old_ns_ip.empty() || old_ns_ip == " "
      || ((block_list.empty() || block_list == " ") && (tfs_file_name.empty() || tfs_file_name == " "))
      || modify_time.empty() || time_len < 8 || (time_len > 8 && time_len < 14))
  {
    usage(argv[0]);
  }
  if (modify_time.length() == 8)
  {
    modify_time += "000000";
  }

  memset(&cmp_stat_, 0, sizeof(cmp_stat_));

  if (init_log_file(argv[0]) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "init log file fail");
    return TFS_ERROR;
  }

  TBSYS_LOGGER.setLogLevel("warn");
  string old_tfs_client, new_tfs_client;
  old_tfs_client = old_ns_ip;
  new_tfs_client = new_ns_ip;
  g_tfs_client = TfsClient::Instance();
  iret = g_tfs_client->initialize(old_ns_ip.c_str());
  if (iret != TFS_SUCCESS)
  {
    if (iret != TFS_SUCCESS)
    {
      if ((!block_list.empty()) && (block_list != " "))
      {
        get_crc_from_block_list(old_tfs_client, new_tfs_client, block_list.c_str(), modify_time);
      }
      else if((!tfs_file_name.empty()) && (tfs_file_name != " "))
      {
        get_crc_from_tfsname_list(old_tfs_client, new_tfs_client, tfs_file_name.c_str(), modify_time);
      }
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "initialize old nameserver(%s) client failed", old_ns_ip.c_str());
  }

  for (i = 0; g_log_fp[i].fp_; i++)
  {
    fclose(*g_log_fp[i].fp_);
  }

  TBSYS_LOG(WARN, "total_count: %"PRI64_PREFIX"d, succ_count: %"PRI64_PREFIX"d, fail_count: %"PRI64_PREFIX"d"
      ", error_count: %"PRI64_PREFIX"d, unsync_count: %"PRI64_PREFIX"d, skip_count: %"PRI64_PREFIX"d",
      cmp_stat_.total_count_, cmp_stat_.succ_count_, cmp_stat_.fail_count_, cmp_stat_.error_count_, cmp_stat_.unsync_count_,
      cmp_stat_.skip_count_);
  return iret;
}
