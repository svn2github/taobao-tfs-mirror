#include "erasure_code_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    ECMarshallingMessage::ECMarshallingMessage():
      family_members_(NULL),
      family_id_(0),
      family_aid_info_(0)
    {
      _packetHeader._pcode = common::REQ_EC_MARSHALLING_MESSAGE;
    }

    ECMarshallingMessage::~ECMarshallingMessage()
    {
      tbsys::gDeleteA(family_members_);
    }

    int ECMarshallingMessage::serialize(common::Stream& output) const
    {
      const int32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info_) + GET_CHECK_MEMBER_NUM(family_aid_info_);
      int32_t ret = output.set_int64(family_id_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(seqno_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int32(expire_time_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = (NULL != family_members_ && MEMBER_NUM > 0 && MEMBER_NUM <= MAX_MARSHALLING_NUM) ? common::TFS_SUCCESS : common::EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = output.set_int32(family_aid_info_);
        }
      }
      if (common::TFS_SUCCESS == ret)
      {
        for (int32_t index = 0; index < MEMBER_NUM && TFS_SUCCESS == ret; ++index)
        {
          int64_t pos = 0;
          ret = family_members_[index].serialize(output.get_free(), output.get_free_length() , pos);
          if (TFS_SUCCESS == ret)
            output.pour(family_members_[index].length());
        }
      }
     return ret;
    }

    int ECMarshallingMessage::deserialize(common::Stream& input)
    {
      int32_t ret = input.get_int64(&family_id_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&seqno_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&expire_time_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&family_aid_info_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        const int32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info_) + GET_CHECK_MEMBER_NUM(family_aid_info_);
        ret = (MEMBER_NUM > 0 && MEMBER_NUM <= MAX_MARSHALLING_NUM) ? common::TFS_SUCCESS : common::EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          family_members_ = new (std::nothrow)FamilyMemberInfo[MEMBER_NUM];
          assert(family_members_);
          for (int32_t index = 0; index < MEMBER_NUM && TFS_SUCCESS == ret; ++index)
          {
            int64_t pos = 0;
            ret = family_members_[index].deserialize(input.get_data(), input.get_data_length() , pos);
            if (TFS_SUCCESS == ret)
              input.drain(family_members_[index].length());
          }
        }
      }
      return ret;
    }

    int64_t ECMarshallingMessage::length() const
    {
      int64_t len = 0;
      const int32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info_) + GET_CHECK_MEMBER_NUM(family_aid_info_);
      for (int32_t index = 0; index < MEMBER_NUM; ++index)
      {
        len += family_members_[index].length();
      }
      return len + INT64_SIZE * 2 + INT_SIZE * 2;
    }

    void ECMarshallingMessage::dump(void) const
    {

    }

    int ECMarshallingMessage::set_family_member_info(const FamilyMemberInfo* members, const int32_t family_aid_info)
    {
      const int32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info) + GET_CHECK_MEMBER_NUM(family_aid_info);
      int32_t ret = (NULL != members && MEMBER_NUM > 0 && MEMBER_NUM <= MAX_MARSHALLING_NUM * 2) ? common::TFS_SUCCESS : common::EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        family_aid_info_ = family_aid_info;
        family_members_ = new (std::nothrow)FamilyMemberInfo[MEMBER_NUM];
        assert(family_members_);
        memcpy(family_members_, members, MEMBER_NUM * sizeof(FamilyMemberInfo));
      }
      return ret;
    }

    ECMarshallingCommitMessage::ECMarshallingCommitMessage():
      status_(PLAN_STATUS_FAILURE)
    {
      _packetHeader._pcode = common::REQ_EC_MARSHALLING_COMMIT_MESSAGE;
    }

    ECMarshallingCommitMessage::~ECMarshallingCommitMessage()
    {

    }

    int ECMarshallingCommitMessage::serialize(common::Stream& output) const
    {
      int32_t ret = ECMarshallingMessage::serialize(output);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int8(status_);
      }
      return ret;
    }

    int ECMarshallingCommitMessage::deserialize(common::Stream& input)
    {
      int32_t ret = ECMarshallingMessage::deserialize(input);
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int8(&status_);
      }
      return ret;
    }

    int64_t ECMarshallingCommitMessage::length() const
    {
      return ECMarshallingMessage::length() + INT8_SIZE;
    }

    void ECMarshallingCommitMessage::dump(void) const
    {

    }

    ECReinstateMessage::ECReinstateMessage()
    {
      _packetHeader._pcode = common::REQ_EC_REINSTATE_MESSAGE;
    }

    ECReinstateMessage::~ECReinstateMessage()
    {

    }

    ECReinstateCommitMessage::ECReinstateCommitMessage()
    {
      _packetHeader._pcode = common::REQ_EC_REINSTATE_COMMIT_MESSAGE;
    }

    ECReinstateCommitMessage::~ECReinstateCommitMessage()
    {

    }

    int ECReinstateCommitMessage::serialize(common::Stream& output) const
    {
      int ret = ECMarshallingCommitMessage::serialize(output);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(reinstate_num_);
      }

      for (int i = 0; (TFS_SUCCESS == ret) && (i < reinstate_num_); i++)
      {
        int64_t pos = 0;
        ret = block_infos_[i].serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          output.pour(block_infos_[i].length());
        }
      }

      return ret;
    }

    int ECReinstateCommitMessage::deserialize(common::Stream& input)
    {
      int ret = ECMarshallingCommitMessage::deserialize(input);
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&reinstate_num_);
      }

      for (int i = 0; (TFS_SUCCESS == ret) && (i < reinstate_num_); i++)
      {
        int64_t pos = 0;
        ret = block_infos_[i].deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          input.drain(block_infos_[i].length());
        }
      }

      return ret;
    }

    int64_t ECReinstateCommitMessage::length() const
    {
      int64_t len = ECMarshallingCommitMessage::length() + INT_SIZE;
      if (reinstate_num_ > 0)
      {
        BlockInfoV2 tmp;
        len += reinstate_num_ * tmp.length();
      }
      return len;
    }

    int32_t ECReinstateCommitMessage::get_reinstate_num() const
    {
      return reinstate_num_;
    }

    common::BlockInfoV2* ECReinstateCommitMessage::get_reinstate_block_info()
    {
      return block_infos_;
    }

    int ECReinstateCommitMessage::set_reinstate_block_info(common::BlockInfoV2* block_infos, const int32_t reinstate_num)
    {
      int ret = ((NULL == block_infos) || (reinstate_num < 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        reinstate_num_ = 0;
        for (int i = 0; i < reinstate_num; i++)
        {
          block_infos_[reinstate_num_++] = block_infos[i];
        }
      }

      return ret;
    }

    ECDissolveMessage::ECDissolveMessage()
    {
      _packetHeader._pcode = common::REQ_EC_DISSOLVE_MESSAGE;
    }

    ECDissolveMessage::~ECDissolveMessage()
    {

    }

    ECDissolveCommitMessage::ECDissolveCommitMessage()
    {
      _packetHeader._pcode = common::REQ_EC_DISSOLVE_COMMIT_MESSAGE;
    }

    ECDissolveCommitMessage::~ECDissolveCommitMessage()
    {

    }
  }
}
