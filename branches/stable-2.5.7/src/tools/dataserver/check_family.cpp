#include <stdio.h>
#include <vector>
#include <algorithm>
#include <string>
#include "common/parameter.h"
#include "common/internal.h"
#include "common/error_msg.h"
#include "common/version.h"
#include "common/func.h"
#include "common/base_packet.h"
#include "common/base_packet_streamer.h"
#include "message/message_factory.h"
#include "tools/util/tool_util.h"
#include "tools/util/ds_lib.h"
#include "tools/util/base_worker.h"
#include "dataserver/ds_define.h"
#include "dataserver/dataservice.h"
#include "dataserver/data_helper.h"
#include "dataserver/erasure_code.h"

using namespace tfs::dataserver;
using namespace tfs::message;
using namespace tfs::common;
using namespace tfs::tools;
using namespace std;

static DataService service;

FILE * result_fp = NULL;

class CheckFamily : public BaseWorker
{
  public:
    CheckFamily():
      data_num_(0), check_num_(0), member_num_(0), data_helper_(service)
    {
    }

    int read_raw_data(const uint64_t server_id, const uint64_t block_id, char* data, const int32_t length, const int32_t offset);

    int encode_and_compare();

    int do_check_family();

    int process(string& line)
    {
      int64_t family_id = strtoll(line.c_str(), NULL, 10);
      int32_t family_aid_info = 0;

      int ret = ToolUtil::get_family_info(family_id, Func::get_host_ip(src_addr_.c_str()), members_, family_aid_info);

      if(ret == TFS_SUCCESS)
      {
        data_num_ = GET_DATA_MEMBER_NUM(family_aid_info);
        check_num_ = GET_CHECK_MEMBER_NUM(family_aid_info);
        member_num_ = data_num_ + check_num_;
        if((member_num_ != static_cast<int32_t>(members_.size())) || (members_.size() == 0) || (member_num_ <= 0) ||
            (member_num_ > MAX_MARSHALLING_NUM) || (data_num_ <= 0) || (check_num_ <= 0))
        {
          ret = EXIT_FAMILY_MEMBER_NUM_ERROR;
        }
        if(ret == TFS_SUCCESS)
        {
          ret = do_check_family();
        }
      }

      if(ret != TFS_SUCCESS && ret != EXIT_CHECK_CRC_ERROR)
      {
        TBSYS_LOG(ERROR, "check family fail, family id: %"PRI64_PREFIX"d, ret: %d", family_id, ret);
        fprintf(result_fp, "%-11"PRI64_PREFIX"d%-11d%-14d%-10d\n", family_id, 0, 0, ret);
      }

      return ret;
    }
  private:
    vector<pair<uint64_t, uint64_t> > members_;
    vector<ECMeta> ec_metas_;
    int32_t data_num_;
    int32_t check_num_;
    int32_t member_num_;
    DataHelper data_helper_;
};

int CheckFamily::read_raw_data(const uint64_t server_id, const uint64_t block_id, char* data, const int32_t length, const int32_t offset)
{
  int ret = TFS_SUCCESS;
  NewClient* new_client = NewClientManager::get_instance().create_client();

  if (NULL == new_client)
  {
    ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
  }
  else
  {
    create_msg_ref(ReadRawdataMessageV2, req_msg);
    tbnet::Packet* ret_msg = NULL;
    req_msg.set_block_id(block_id);
    req_msg.set_length(length);
    req_msg.set_offset(offset);
    req_msg.set_degrade_flag(true);

    ret = send_msg_to_server(server_id, new_client, &req_msg, ret_msg);
    if (TFS_SUCCESS == ret)
    {
      if (READ_RAWDATA_RESP_MESSAGE_V2 == ret_msg->getPCode())
      {
        ReadRawdataRespMessageV2* resp_msg = dynamic_cast<ReadRawdataRespMessageV2* >(ret_msg);
        if(length == resp_msg->get_length())
        {
          memcpy(data, resp_msg->get_data(), length);
        }
        else
        {
          ret = EXIT_READ_FILE_SIZE_ERROR;
        }
      }
      else if (STATUS_MESSAGE == ret_msg->getPCode())
      {
        StatusMessage* resp_msg = dynamic_cast<StatusMessage* >(ret_msg);
        ret = resp_msg->get_status();
      }
      else
      {
        ret = EXIT_UNKNOWN_MSGTYPE;
      }
    }
    NewClientManager::get_instance().destroy_client(new_client);
  }

  return ret;
}

int CheckFamily::encode_and_compare()
{
  ErasureCode encoder;
  int32_t offset = 0;
  int32_t read_len = 0;
  int32_t check_size = MAX_READ_SIZE;
  int32_t check_size_speed = 2 * MAX_READ_SIZE;
  char* data[member_num_];
  memset(data, 0, member_num_ * sizeof(char*));

  int32_t marshalling_len = ec_metas_[data_num_].mars_offset_;
  vector<ECMeta>::iterator min_ele = min_element(ec_metas_.begin(), ec_metas_.begin() + data_num_, ECMeta::m_compare);
  int32_t min_len = min_ele->mars_offset_;
  min_len /= check_size_speed;
  min_len *= check_size_speed;

  int encode_unit = ErasureCode::ws_ * ErasureCode::ps_;
  int encode_unit_speed = 2 * encode_unit;
  if(0 != (marshalling_len % encode_unit))
  {
    marshalling_len = (marshalling_len / encode_unit + 1) * encode_unit;
  }

  int ret = encoder.config(data_num_, check_num_);
  if(ret == TFS_SUCCESS)
  {
    for(int32_t i = 0; i < member_num_; ++i)
    {
      data[i] = (char*)malloc(encode_unit_speed * sizeof(char));
      assert(data[i]);
    }
    encoder.bind(data, member_num_, encode_unit_speed);
  }

  bool has_error = false;
  uint32_t inner_offset = check_size - encode_unit;
  char* check = (char*)malloc(encode_unit_speed * sizeof(char));
  assert(check);

  //check 2KB of 2MB everytime
  while((ret == TFS_SUCCESS) && (offset < min_len) && !has_error)
  {
    for(int32_t i = 0; (ret == TFS_SUCCESS) && (i < data_num_); ++i)
    {
      ret = read_raw_data(members_[i].second, members_[i].first, data[i], encode_unit_speed, offset + inner_offset);
    }

    if(ret == TFS_SUCCESS)
    {
      ret = encoder.encode(encode_unit_speed);
    }

    for(int32_t i = data_num_; (ret == TFS_SUCCESS) && (i < member_num_) && !has_error; ++i)
    {
      ret = read_raw_data(members_[i].second, members_[i].first, check, encode_unit_speed, offset + inner_offset);
      if((ret == TFS_SUCCESS) && (memcmp(data[i], check, encode_unit_speed) != 0))
      {
        ret = EXIT_CHECK_CRC_ERROR;
        has_error = true;
      }
    }

    if(ret == TFS_SUCCESS)
    {
      offset += check_size_speed;
    }
  }

  //check 1KB of 1MB or less everytime
  while((ret == TFS_SUCCESS) && (offset < marshalling_len) && !has_error)
  {
    for(int32_t i = 0; (ret == TFS_SUCCESS) && (i < data_num_); ++i)
    {
      if(ec_metas_[i].mars_offset_ > offset)
      {
        read_len = min(ec_metas_[i].mars_offset_ - offset, encode_unit);
        if(read_len < encode_unit)
        {
          memset(data[i], 0, encode_unit * sizeof(char));
        }
        ret = read_raw_data(members_[i].second, members_[i].first, data[i], read_len, offset);
      }
      else
      {
        memset(data[i], 0, encode_unit * sizeof(char));
      }
    }

    if(ret == TFS_SUCCESS)
    {
      ret = encoder.encode(encode_unit);
    }

    for(int32_t i = data_num_; (ret == TFS_SUCCESS) && (i < member_num_) && !has_error; ++i)
    {
      ret = read_raw_data(members_[i].second, members_[i].first, check, encode_unit, offset);
      if((ret == TFS_SUCCESS) && (memcmp(data[i], check, encode_unit) != 0))
      {
        ret = EXIT_CHECK_CRC_ERROR;
        has_error = true;
      }
    }

    if(ret == TFS_SUCCESS)
    {
      offset += check_size;
    }
  }

  if(ret == TFS_SUCCESS || ret == EXIT_CHECK_CRC_ERROR)
  {
    fprintf(result_fp, "%-11"PRI64_PREFIX"d%-11d%-14d%-10d\n",
        ec_metas_[data_num_].family_id_, has_error, offset, ret);
  }

  for(int32_t i = 0; i < member_num_; ++i)
  {
    tbsys::gFree(data[i]);
  }

  tbsys::gFree(check);

  return ret;
}

int CheckFamily::do_check_family()
{
  ec_metas_.resize(member_num_);
  int ret = TFS_SUCCESS;

  //query all members_' ECMeta, no lock
  for(int32_t i = 0; (ret == TFS_SUCCESS) && (i < member_num_); ++i)
  {
    if(members_[i].first == INVALID_BLOCK_ID || members_[i].second == INVALID_SERVER_ID)
    {
      ret = EXIT_FAMILY_MEMBER_INFO_ERROR;
    }
    else
    {
      ret = data_helper_.query_ec_meta(members_[i].second, members_[i].first, ec_metas_[i], 0);
    }
  }

  //get data from data_blocks, encode them then compare with check_blocks' data
  if(ret == TFS_SUCCESS)
  {
    ret = encode_and_compare();
  }
  return ret;
}

class CheckFamilyManager : public BaseWorkerManager
{
  public:
    BaseWorker* create_worker()
    {
      return new CheckFamily();
    }

    int begin()
    {
      string result_path = output_dir_ += "/check_family_result";
      result_fp = fopen(result_path.c_str(), "a");
      assert(result_fp != NULL);

      fprintf(result_fp, "family_id  has_error  error_offset  ret\n");

      return TFS_SUCCESS;
    }

    void end()
    {
      if(result_fp != NULL)
        fclose(result_fp);
    }
};

int main(int argc, char* argv[])
{
  CheckFamilyManager manager;
  return manager.main(argc, argv);
}
