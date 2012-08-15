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
        int64_t pos = 0;
        for (int32_t index = 0; index < MEMBER_NUM && TFS_SUCCESS == ret; ++index)
        {
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
          int64_t pos = 0;
          for (int32_t index = 0; index < MEMBER_NUM && TFS_SUCCESS == ret; ++index)
          {
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
      int32_t ret = (NULL != members && MEMBER_NUM > 0 && MEMBER_NUM <= MAX_MARSHALLING_NUM) ? common::TFS_SUCCESS : common::EXIT_PARAMETER_ERROR;
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
