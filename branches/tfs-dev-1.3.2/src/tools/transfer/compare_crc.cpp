#include "compare_crc.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::client;
using namespace tfs::message;

FILE *g_succ_file = NULL, *g_fail_file = NULL, *g_error_file = NULL, *g_unsync_file = NULL;

struct log_file g_log_fp[] =
{
  {&g_succ_file, "./cmp_log/succ_file"},
  {&g_fail_file, "./cmp_log/fail_file"},
  {&g_error_file, "./cmp_log/error_file"},
  {&g_unsync_file, "./cmp_log/unsync_file"},
  {NULL, NULL}
};

int init_log_file()
{
  DirectoryOp::create_directory("cmp_log");
  for (int i = 0; g_log_fp[i].file_; i++)
  {
    *g_log_fp[i].fp_ = fopen(g_log_fp[i].file_, "w");
    if (!*g_log_fp[i].fp_)
    {
      printf("open file fail %s : %s\n:", g_log_fp[i].file_, strerror(errno));
      return TFS_ERROR;
    }
  }
  return TFS_SUCCESS;
}
int get_meta_crc(TfsClient& tfs_client, const char* tfs_file_name, uint32_t* crc)
{
  if(crc == NULL)
  {
    TBSYS_LOG(WARN,"Pointer to meta crc value is NULL");
    return TFS_ERROR;
  }

  FileInfo finfo;

  if (tfs_client.tfs_open(tfs_file_name, NULL, READ_MODE) != TFS_SUCCESS)
  {
    TBSYS_LOG(WARN, "open tfsfile fail: %s\n", tfs_client.get_error_message());
    return TFS_ERROR;
  }

  if (tfs_client.tfs_stat(&finfo, 1) != TFS_SUCCESS)
  {
    TBSYS_LOG(WARN, "fstat tfsfile fail: %s\n", tfs_client.get_error_message());
    return TFS_ERROR;
  }
  if (finfo.flag_ == 1 || finfo.flag_ == 4 || finfo.flag_ == 5)
  {
    return -2;
  }
  *crc = finfo.crc_;
  tfs_client.tfs_close();
  return TFS_SUCCESS;
}

int get_real_crc(TfsClient& tfs_client, const char* tfs_file_name, uint32_t* crc)
{
  if(crc == NULL)
  {
    fprintf(stderr,"Pointer to real crc value is NULL");
    return TFS_ERROR;
  }

  if (tfs_client.tfs_open(tfs_file_name, NULL, READ_MODE) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "open tfsfile fail: %s\n", tfs_client.get_error_message());
    return TFS_ERROR;
  }

  char data[MAX_READ_SIZE]={'\0'};
  uint32_t crc_tmp = 0;
  int total_size = 0,rlen=0;
  for(;;)
  {
    rlen = tfs_client.tfs_read(data, MAX_READ_SIZE);
    if (rlen < 0)
    {
      fprintf(stderr, "read tfsfile fail: %s\n", tfs_client.get_error_message());
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
  tfs_client.tfs_close();
  return TFS_SUCCESS;
}

int get_crc_from_filename(TfsClient& old_tfs_client, TfsClient& new_tfs_client, const char* tfs_file_name, string& modify_time)
{
  int ret = TFS_ERROR;
  uint32_t old_meta_crc = 0, old_real_crc = 0;
  uint32_t new_meta_crc = 0, new_real_crc = 0;

  cmp_stat_.total_count_++;
  if (-2 != get_meta_crc(old_tfs_client, tfs_file_name, &old_meta_crc))
  {
    if (-2 != get_meta_crc(new_tfs_client, tfs_file_name, &new_meta_crc))
    {
      ret = TFS_SUCCESS;
    }
    else
    {
      printf ("%s failed as : deleted in new cluster\n",tfs_file_name);
    }
  }
  else
  {
    printf ("%s failed as : deleted in old cluster\n",tfs_file_name);
  }

  if (ret != TFS_ERROR)
  {
    get_real_crc(new_tfs_client, tfs_file_name, &new_real_crc);
    if( (old_meta_crc == new_meta_crc) && (old_meta_crc == new_real_crc))
    {
      printf ("%s success\n",tfs_file_name);
      fprintf(g_succ_file, "%s\n", tfs_file_name);
      cmp_stat_.succ_count_++;
    }
    else
    {
      get_real_crc(old_tfs_client, tfs_file_name, &old_real_crc);
      if(old_meta_crc != old_real_crc)
      {
        printf ("%s failed as : old cluster crc not same\n",tfs_file_name);
        fprintf(g_error_file, "%s\n", tfs_file_name);
        cmp_stat_.error_count_++;
        ret = TFS_ERROR;
      }
      else
      {
        old_tfs_client.tfs_open(tfs_file_name, NULL, READ_MODE);
        FileInfo finfo;
        old_tfs_client.tfs_stat(&finfo,READ_MODE);
        FSName fsname;
        fsname.set_name(tfs_file_name);

        if(finfo.modify_time_ <= tbsys::CTimeUtil::strToTime(const_cast<char*>(modify_time.c_str())))
        {
          printf ("%s failed with Other reason, at block : %u : %s: %u %u %u %u\n",tfs_file_name,
              fsname.get_block_id(),
              Func::time_to_str(finfo.modify_time_).c_str(),
              old_meta_crc, old_real_crc,
              new_meta_crc, new_real_crc);
          fprintf(g_fail_file, "%s\n", tfs_file_name);
          cmp_stat_.fail_count_++;
        }
        else
        {
          printf ("%s failed as modified at %s\n",tfs_file_name,Func::time_to_str(finfo.modify_time_).c_str());
          fprintf(g_unsync_file, "%s\n", tfs_file_name);
          cmp_stat_.unsync_count_++;
        }
        old_tfs_client.tfs_close();
      }
    }
  }
  else
  {
    fprintf(g_error_file, "%s\n", tfs_file_name);
    cmp_stat_.error_count_++;
  }
  return ret;
}

int get_crc_from_tfsname_list(TfsClient& old_tfs_client, TfsClient& new_tfs_client, const char* filename_list, string& modify_time)
{
  int ret = TFS_ERROR;
  FILE *fp;
  fp = fopen(filename_list, "r");
  if (fp != NULL)
  {
    fprintf(stdout,"Open file name list %s\n",filename_list);
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
    fprintf(stderr,"Open file name list failed\n");
  }
  return ret;
}

int get_crc_from_block_list(TfsClient& old_tfs_client, TfsClient& new_tfs_client, const char* block_list, string& modify_time)
{
  int ret = TFS_ERROR;
  FILE *fp;
  fp = fopen(block_list, "r");
  if (fp != NULL)
  {
    fprintf(stdout,"open block list %s\n", block_list);
    uint32_t block_id = 0;
    while (fscanf(fp, "%u\n", &block_id) != EOF)
    {
      VUINT64 ds_list;
      old_tfs_client.get_block_info(block_id, ds_list);
      if(ds_list.size() > 0)
      {
        uint64_t ds_id = ds_list[0];

        FILE_INFO_LIST file_list;
        GetServerStatusMessage gss_message;
        gss_message.set_status_type(GSS_BLOCK_FILE_INFO);
        gss_message.set_return_row(block_id);

        Message* ret_message = NULL;
        ret = send_message_to_server(ds_id, &gss_message, &ret_message);
        if ((TFS_ERROR != ret) && ret_message != NULL)
        {
          if (ret_message->get_message_type() == BLOCK_FILE_INFO_MESSAGE)
          {
            BlockFileInfoMessage *bfi_message = (BlockFileInfoMessage*) ret_message;
            file_list = *(bfi_message->get_fileinfo_list());
          }
          else if (ret_message->get_message_type() == STATUS_MESSAGE)
          {
            StatusMessage* sm = reinterpret_cast<StatusMessage*> (ret_message);
            if (sm->get_error() != NULL)
            {
              fprintf(stderr,"%s\n", sm->get_error());
            }
          }
          delete ret_message;
        }
        else
        {
          fprintf(stderr,"get file list message returns null\n");
        }

        if (TFS_ERROR != ret)
        {
          int32_t list_size = static_cast<int32_t>(file_list.size());
          for(int i=0; i < list_size; i++)
          {
            FileInfo* fi = file_list.at(i);
            FSName fsname;
            fsname.set_block_id(block_id);
            fsname.set_file_id(fi->id_);
            //printf("fileList at %d:%llu",i,fi->id);
            get_crc_from_filename(old_tfs_client, new_tfs_client, fsname.get_name(), modify_time);
          }
          file_list.clear();
        }
      }
      else
      {
        fprintf(stderr,"get block info for block id :%u failure\n",block_id);
      }
    }
    fclose(fp);
  }
  else
  {
    fprintf(stderr,"open block list failed\n");
  }
  return ret;
}

void usage(const char* name)
{
  fprintf(stderr,"Usage:%s -o old_ns_ip -n new_ns_ip -f file_list -d modify_time (20110315|20110315183500) [-h]\n", name);
  fprintf(stderr,"Usage:%s -o old_ns_ip -n new_ns_ip -b block_list -d modify_time(20110315|20110315183500) [-h]\n", name);
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

  if (init_log_file() != TFS_SUCCESS)
  {
    printf("init log file fail\n");
    return TFS_ERROR;
  }

  TfsClient old_tfs_client, new_tfs_client;
  iret = old_tfs_client.initialize(old_ns_ip.c_str());
  if (iret != TFS_ERROR)
  {
    iret = new_tfs_client.initialize(new_ns_ip.c_str());
    if (iret != TFS_ERROR)
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
    else
    {
      fprintf(stderr, "initialize new nameserver(%s) client failed\n", old_ns_ip.c_str());
    }
  }
  else
  {
    fprintf(stderr, "initialize old nameserver(%s) client failed\n", old_ns_ip.c_str());
  }

  for (i = 0; g_log_fp[i].fp_; i++)
  {
    fclose(*g_log_fp[i].fp_);
  }

  fprintf(stdout, "total_count: %"PRI64_PREFIX"d, succ_count: %"PRI64_PREFIX"d, fail_count: %"PRI64_PREFIX"d"
      ", error_count: %"PRI64_PREFIX"d, unsync_count: %"PRI64_PREFIX"d\n",
      cmp_stat_.total_count_, cmp_stat_.succ_count_, cmp_stat_.fail_count_, cmp_stat_.error_count_, cmp_stat_.unsync_count_);
  return iret;
}
