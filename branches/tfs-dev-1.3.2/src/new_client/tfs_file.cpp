#include "tfs_file.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;

TfsFile::TfsFile() : flags_(-1), is_open_(TFS_FILE_OPEN_NO), eof_(TFS_FILE_EOF_NO), offset_(0), tfs_session_(NULL)
{
  error_message_[0] = '\0';
}

TfsFile::~TfsFile()
{
  for (int32_t i = 0; i < processing_seg_list_.size(); i++)
  {
    tbsys::gDelete(processing_seg_list_[i]);
  }
}

/*
int TfsFile::open_ex(const char* file_name, const char* suffix, int32_t flags)
{
  // need ?
  if (NULL == tfs_session_)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "session is not initialized");
    return TFS_ERROR;
  }

  flags_ = flags;
  fsname_.set_name(file_name, suffix);
  fsname_.set_cluster_id(tfs_session_->get_cluster_id());
  uint32_t block_id = fsname_.get_block_id();
  uint64_t file_id = fsname_.get_file_id();
  int ret = TFS_ERROR;
  //ds_list_.clear();

  if ((ret = tfs_session_->get_block_info(block_id, ds_list_, flags_)) != TFS_SUCCESS)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "tfs open fail: get block info fail, blockid: %u, fileid: %"
             PRI64_PREFIX "u, mode: %d, ret: %d", block_id, file_id, flags, ret);
    return ret;
  }

  // get primary ds index
  if ((T_READ == flags_))
  {
    if (0 == file_id)
    {
      //TBSYS_LOG(WARN, "blockId(%u) read fileid is 0.", block_id_);
    }
    pri_ds_index_ = static_cast<int32_t>(file_id % ds_list_.size());
  }
  else
  {
    pri_ds_index_ = 0;
  }

  if ((ret = connect_ds()) != TFS_SUCCESS)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "connect to dataserver: %"PRI64_PREFIX"u fail, ret: %d",
             ds_list_[pri_ds_index_], ret);
    return ret;
  }

  if ((flags_ & T_WRITE))
  {
    if ((ret = create_filename()) != TFS_SUCCESS)
    {
      // create_filename log error message
      TBSYS_LOG(ERROR, "create file name fail, fileid: %"PRI64_PREFIX"u, ret: %d", file_id, ret);
      return ret;
    }
  }

  offset_ = 0;
  eof_ = TFS_FILE_EOF_NO;
  crc_ = 0;
  is_open_ = TFS_FILE_OPEN_YES;
  return ret;
}
*/

ssize_t TfsFile::read_ex(void* buf, size_t count)
{

}

ssize_t TfsFile::write_ex(const void* buf, size_t count)
{

}

off_t TfsFile::lseek_ex(off_t offset, int whence)
{

}

ssize_t TfsFile::pread_ex(void* buf, size_t count, off_t offset)
{

}

ssize_t TfsFile::pwrite_ex(const void* buf, size_t count, off_t offset)
{

}

int TfsFile::close_ex()
{

}

void TfsFile::set_session(TfsSession* tfs_session)
{
  tfs_session_ = tfs_session;
}

const char* TfsFile::get_file_name()
{

}

/*
int TfsFile::create_filename()
{
  int ret = TFS_ERROR;
  CreateFilenameMessage dsmessage;
  dsmessage.set_block_id(fsname_.get_block_id());
  dsmessage.set_file_id(fsname_.get_file_id());
  Message* message = client_->call(&dsmessage);

  if (!message)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "createfilename send request fail");
  }
  else if (message->get_message_type() != RESP_CREATE_FILENAME_MESSAGE)
  {
    if (message->get_message_type() == STATUS_MESSAGE)
    {
      StatusMessage* msg = dynamic_cast<StatusMessage*>(message);
      ret = msg->get_status();
      snprintf(error_message_, ERR_MSG_SIZE, "create file name fail, get error msg: %s, ret: %d, from: %s",
               msg->get_error(), ret, tbsys::CNetUtil::addrToString(client_->get_mip()).c_str());
    }
    else
    {
      snprintf(error_message_, ERR_MSG_SIZE, "create file name fail: unexpected message recieved");
    }
  }
  else
  {
    RespCreateFilenameMessage* msg = dynamic_cast<RespCreateFilenameMessage*>(message);
    fsname_.set_file_id(msg->get_file_id());
    file_number_ = msg->get_file_number();
    if (0 == fsname_.get_file_id())
    {
      snprintf(error_message_, ERR_MSG_SIZE, "create file name fail. fileid: 0");
    }
    else
    {
      ret = TFS_SUCCESS;
    }
  }

  tbsys::gDelete(message);
  return ret;
}

int TfsFile::connect_ds()
{
  int ret = TFS_ERROR;
  uint32_t ds_list_size = ds_list_.size();
  uint32_t j = 0;
  uint64_t ds_id = 0;
  for (uint32_t i = 0; i < ds_list_size; i++)
  {
    j = (pri_ds_index_ + i) % ds_list_size;
    ds_id = ds_list_[j];
    if (ds_id == ULONG_LONG_MAX)
    {
      break;
    }

    client_ = message::CLIENT_POOL.get_client(ds_id);
    if (client_ == NULL)
    {
      break;
    }
    if (client_->connect() == TFS_SUCCESS)
    {
      pri_ds_index_ = j;
      last_elect_ds_id_ = ds_id;
      ret = TFS_SUCCESS;
      break;
    }

    message::CLIENT_POOL.release_client(client_);
    client_ = NULL;

    if (flags_ != T_READ)
      break;
  }

  return ret;
}
*/

int TfsFile::process(const InnerFilePhase file_phase)
{
  // ClientManager singleton
  int ret = TFS_SUCCESS;
  int64_t wait_id = 0;
  int32_t size = processing_seg_list_.size();
  global_client_manager.get_wait_id(wait_id);
  for (int32_t i = 0; i < size; ++i)
  {
    if ((ret = do_async_request(file_phase, wait_id, i)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "request %d fail", i);
      return ret;
    }
  }

  std::map<int64_t, tbnet::Packet*> packets;
  if ((ret = global_client_manager.get_response(wait_id, size, WAIT_TIME_OUT, packets)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "get respose fail, ret: %d", ret);
    return ret;
  }
  else
  {
    std::map<int64_t, tbnet::Packet*>::iterator mit = packets.begin();
    for ( ; mit != packets.end(); ++mit)
    {
      ret = do_async_response(file_phase, mit->second, mit->first);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "response fail, ret: %d", ret);
        return ret;
      }
    }
  }

  return ret;
}

int TfsFile::do_async_request(const InnerFilePhase file_phase, const int64_t wait_id, const int32_t index)
{
  int ret = TFS_SUCCESS;
  switch (file_phase)
  {
  case FILE_PHASE_CREATE_FILE:
    ret = async_req_create_file(wait_id, index);
    break;
  case FILE_PHASE_READ_FILE:
    ret = async_req_read_file(wait_id, index);
    break;
  case FILE_PHASE_WRITE_DATA:
    ret = async_req_write_data(wait_id, index);
    break;
  case FILE_PHASE_CLOSE_FILE:
    ret = async_req_close_file(wait_id, index);
    break;
  default:
    //add log
    ret = TFS_ERROR;
    break;
  }

  return ret;
}

int TfsFile::do_async_response(const InnerFilePhase file_phase, tbnet::Packet* packet, const int32_t index)
{
  int ret = TFS_SUCCESS;
  switch (file_phase)
  {
  case FILE_PHASE_CREATE_FILE:
    ret = async_rsp_create_file(packet, index);
    break;
  case FILE_PHASE_READ_FILE:
    ret = async_rsp_read_file(packet, index);
    break;
  case FILE_PHASE_WRITE_DATA:
    ret = async_rsp_write_data(packet, index);
    break;
  case FILE_PHASE_CLOSE_FILE:
    ret = async_rsp_close_file(packet, index);
    break;
  }
}

int TfsFile::async_req_create_file(const int64_t wait_id, const int32_t index)
{
  int ret = TFS_ERROR;
  SegmentData* seg_data = processing_seg_list_[index];
  CreateFilenameMessage cf_message;
  cf_message.set_block_id(seg_data->seg_info_.block_id_);
  cf_message.set_file_id(seg_data->seg_info_.file_id_);

  if (0 == seg_data->ds_.size())
  {
    TBSYS_LOG(ERROR, "create file fail: ds list is empty. blockid: %u, fileid: %" PRI64_PREFIX "u",
        seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_);
    seg_data->status_ = EXIT_INVALID_ARGU;
  }
  else
  {
    ret = global_client_manager.post_request(seg_data->ds_[0], &cf_message, wait_id);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "create file post request fail. ret: %d, waitid: %d, blockid: %u, fileid: %" PRI64_PREFIX "u",
          ret, wait_id, seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_);
      seg_data->status_ = ret;
    }
  }

  return ret;
}

int TfsFile::async_rsp_create_file(tbnet::Packet* packet, const int32_t index)
{
  int ret = TFS_ERROR;
  SegmentData* seg_data = processing_seg_list_[index];
  Message* rsp = dynamic_cast<Message*>(packet);
  if (NULL == rsp)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "create file name rsp null");
    seg_data->status_ = EXIT_TIMEOUT_ERROR;
  }
  else if (RESP_CREATE_FILENAME_MESSAGE != rsp->get_message_type())
  {
    if (STATUS_MESSAGE == rsp->get_message_type())
    {
      StatusMessage* msg = dynamic_cast<StatusMessage*>(rsp);
      ret = msg->get_status();
      snprintf(error_message_, ERR_MSG_SIZE, "create file name fail. get error msg: %s, ret: %d, from: %s",
               msg->get_error(), ret, tbsys::CNetUtil::addrToString(seg_data->ds_[0]).c_str());
      seg_data->status_ = ret;
    }
    else
    {
      snprintf(error_message_, ERR_MSG_SIZE, "create file name fail: unexpected message recieved");
      seg_data->status_ = EXIT_UNKNOWN_MSGTYPE;
    }
  }
  else
  {
    RespCreateFilenameMessage* msg = dynamic_cast<RespCreateFilenameMessage*>(rsp);
    //do set fsname at upper level
    //fsname_.set_file_id(msg->get_file_id());
    //file_number_ = msg->get_file_number();
    
    if (0 == msg->get_file_id())
    {
      snprintf(error_message_, ERR_MSG_SIZE, "create file name fail. fileid: 0");
      seg_data->status_ = EXIT_INVALID_ARGU;
    }
    else
    {
      seg_data->seg_info_.file_id_ = msg->get_file_id();
      seg_data->file_number_ = msg->get_file_number();
      ret = TFS_SUCCESS;
    }
  }

  tbsys::gDelete(rsp);
  return ret;
}

int TfsFile::async_req_write_data(const int64_t wait_id, const int32_t index)
{
  SegmentData* seg_data = processing_seg_list_[index];
  CreateFilenameMessage cf_message;
  cf_message.set_block_id(seg_data->seg_info_.block_id_);
  cf_message.set_file_id(seg_data->seg_info_.file_id_);

  WriteDataMessage wd_message;
  wd_message.set_file_number(seg_data->file_number_);
  wd_message.set_block_id(seg_data->seg_info_.block_id_);
  wd_message.set_file_id(seg_data->seg_info_.file_id_);
  wd_message.set_offset(seg_data->cur_offset_);
  wd_message.set_length(seg_data->cur_size_);
  wd_message.set_ds_list(seg_data->ds_);
  wd_message.set_data(seg_data->buf_);

  // no not need to estimate the ds number is zero
  int ret = global_client_manager.post_request(seg_data->ds_[0], &wd_message, wait_id);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "write data post request fail. ret: %d, waitid: %d, blockid: %u, fileid: %" PRI64_PREFIX "u, dsip: %s",
          ret, wait_id, seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_,
          tbsys::CNetUtil::addrToString(seg_data->ds_[0]).c_str());
    seg_data->status_ = ret;
  }

  return ret;
}

int TfsFile::async_rsp_write_data(tbnet::Packet* packet, const int32_t index)
{
  int ret = TFS_ERROR;
  SegmentData* seg_data = processing_seg_list_[index];
  Message* rsp = dynamic_cast<Message*>(packet);
  if (NULL == rsp)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "write data rsp null. dsip: %s",
        tbsys::CNetUtil::addrToString(seg_data->ds_[0]).c_str());
    seg_data->status_ = EXIT_TIMEOUT_ERROR;
//#ifdef __CLIENT_METRICS__
//    write_metrics_.incr_timeout_count();
//#endif
  }
  else
  {
    if (STATUS_MESSAGE == rsp->get_message_type())
    {
      StatusMessage* msg = dynamic_cast<StatusMessage*>(rsp);
      if (STATUS_MESSAGE_OK == msg->get_status())
      {
        int32_t& crc_ref = seg_data->seg_info_.crc_;
        crc_ref = Func::crc(crc_ref, seg_data->buf_, seg_data->cur_size_);
        seg_data->cur_offset_ += seg_data->cur_size_;
        ret = TFS_SUCCESS;
      }
      else
      {
//#ifdef __CLIENT_METRICS__
//        write_metrics_.incr_failed_count();
//#endif
        snprintf(error_message_, ERR_MSG_SIZE, "tfs write data, get error msg: %s, dsip: %s",
            msg->get_error(), tbsys::CNetUtil::addrToString(seg_data->ds_[0]).c_str());
        seg_data->status_ = msg->get_status();
      }
    }
    else
    {
      seg_data->status_ = EXIT_UNKNOWN_MSGTYPE;
    }
  }

  tbsys::gDelete(rsp);
  return ret;
}

//close file for write
int TfsFile::async_req_close_file(const int64_t wait_id, const int32_t index)
{
  SegmentData* seg_data = processing_seg_list_[index];
  CloseFileMessage cf_message;
  cf_message.set_file_number(seg_data->file_number_);
  cf_message.set_block_id(seg_data->seg_info_.block_id_);
  cf_message.set_file_id(seg_data->seg_info_.file_id_);
  //set sync flag
  cf_message.set_option_flag(option_flag_);

  cf_message.set_ds_list(seg_data->ds_);
  cf_message.set_crc(seg_data->seg_info_.crc_);

  // no not need to estimate the ds number is zero
  int ret = global_client_manager.post_request(seg_data->ds_[0], &cf_message, wait_id);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "close file post request fail. ret: %d, waitid: %d, blockid: %u, fileid: %" PRI64_PREFIX "u, dsip: %s",
          ret, wait_id, seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_,
          tbsys::CNetUtil::addrToString(seg_data->ds_[0]).c_str());
    seg_data->status_ = ret;
  }

  return ret;
}

int TfsFile::async_rsp_close_file(tbnet::Packet* packet, const int32_t index)
{
  int ret = TFS_ERROR;
  SegmentData* seg_data = processing_seg_list_[index];
  Message* rsp = dynamic_cast<Message*>(packet);

  if (NULL != rsp)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "tfs file close, send msg to dataserver time out, blockid: %u, fileid: %"
        PRI64_PREFIX "u, dsip: %s", 
        seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_, tbsys::CNetUtil::addrToString(seg_data->ds_[0]).c_str());
    seg_data->status_ = EXIT_TIMEOUT_ERROR;
//#ifdef __CLIENT_METRICS__
//    close_metrics_.incr_failed_count();
//#endif
  }
  else
  {
    if (STATUS_MESSAGE == rsp->get_message_type())
    {
      StatusMessage* msg = dynamic_cast<StatusMessage*>(rsp);
      if (STATUS_MESSAGE_OK == msg->get_status())
      {
        ret = TFS_SUCCESS;
      }
      else
      {
//#ifdef __CLIENT_METRICS__
//        close_metrics_.incr_failed_count();
//#endif
        snprintf(error_message_, ERR_MSG_SIZE, "tfs file close, get errow msg: %s, dsip: %s",
            msg->get_error(), tbsys::CNetUtil::addrToString(seg_data->ds_[0]).c_str());
        seg_data->status_ = msg->get_status();
      }
    }
    tbsys::gDelete(rsp);
  }

//#ifdef __CLIENT_METRICS__
//  if (close_metrics_.total_count % 50 == 0)
//  {
//    TBSYS_LOG(DEBUG, "write file (%s) on server(%s)", file_name_, tbsys::CNetUtil::addrToString(last_elect_ds_id_).c_str());
//  }
//#endif
  return ret;
}

int TfsFile::async_req_read_file(const int64_t wait_id, const int32_t index)
{
  int ret = TFS_ERROR;
  SegmentData* seg_data = processing_seg_list_[index];
  ReadDataMessageV2 rd_message;
  rd_message.set_block_id(seg_data->seg_info_.block_id_);
  rd_message.set_file_id(seg_data->seg_info_.file_id_);
  rd_message.set_offset(seg_data->cur_offset_);
  rd_message.set_length(seg_data->cur_size_);

  int32_t ds_size = seg_data->ds_.size();
  if (0 == seg_data->seg_info_.file_id_ || 0 == ds_size)
  {
    TBSYS_LOG(ERROR, "create file fail: ds list is empty. blockid: %u, fileid: %" PRI64_PREFIX "u, size: %d",
        seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_, ds_size);
    seg_data->status_ = EXIT_INVALID_ARGU;
  }
  else
  {
    if (-1 == seg_data->pri_ds_index_)
    {
      seg_data->pri_ds_index_ = static_cast<int32_t>(seg_data->seg_info_.file_id_ % ds_size);
    }

    int32_t retry_count = ds_size; 
    while (retry_count > 0)
    {
      int32_t selected_ds_index = seg_data->pri_ds_index_;
      ret = global_client_manager.post_request(seg_data->ds_[selected_ds_index], &rd_message, wait_id);
      if (EXIT_SENDMSG_ERROR == ret)
      {
        TBSYS_LOG(ERROR, "post read file req fail. blockid: %u, fileid: %" PRI64_PREFIX "u, dsip: %s",
            seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_,
            tbsys::CNetUtil::addrToString(seg_data->ds_[selected_ds_index]).c_str());
        seg_data->pri_ds_index_ = (seg_data->pri_ds_index_ + 1) % ds_size;
        --retry_count;
      }
      else
      {
        break;
      }
    }

    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "create file fail: ds list is empty. blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d",
          seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_, ret);
      seg_data->status_ = ret;
    }
  }

  return ret;
}

int TfsFile::async_rsp_read_file(tbnet::Packet* packet, const int32_t index)
{
  int ret = TFS_ERROR;
  SegmentData* seg_data = processing_seg_list_[index];
  Message* rsp = dynamic_cast<Message*>(packet);

  int ret_len = -1;
  if (NULL == rsp)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "tfs file read fail, send msg to dataserver time out, blockid: %u, fileid: %"
        PRI64_PREFIX "u, dsip: %s", 
        seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_,
        tbsys::CNetUtil::addrToString(seg_data->ds_[seg_data->pri_ds_index_]).c_str());
    seg_data->status_ = EXIT_TIMEOUT_ERROR;
    ret = EXIT_TIMEOUT_ERROR;
  }
  else
  {
    if (RESP_READ_DATA_MESSAGE_V2 == rsp->get_message_type())
    {
      ret = TFS_SUCCESS;
      RespReadDataMessageV2* msg = dynamic_cast<RespReadDataMessageV2*>(rsp);
      if (/*0 == seg_data->seg_info_.offset_ && */ 0 == seg_data->cur_offset_ && NULL == msg->get_file_info())
      {
        TBSYS_LOG(WARN, "tfs read, get file info error, blockid: %u, fileid: %"PRI64_PREFIX"u, form dataserver: %s",
            seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_,
            tbsys::CNetUtil::addrToString(seg_data->ds_[seg_data->pri_ds_index_]).c_str());
        seg_data->status_ = EXIT_TIMEOUT_ERROR;
        ret = EXIT_TIMEOUT_ERROR;
      }
      else
      {
        if (NULL != msg->get_file_info())
        {
          seg_data->file_info_ = new FileInfo();
          memcpy(seg_data->file_info_, msg->get_file_info(), FILEINFO_SIZE);
        }
        if (seg_data->file_info_->id_ != seg_data->seg_info_.file_id_)
        {
          TBSYS_LOG(WARN, "tfs read, blockid: %u, return fileid: %"
              PRI64_PREFIX"u, require fileid: %"PRI64_PREFIX"u not match, from dataserver: %s",
              seg_data->seg_info_.block_id_, seg_data->file_info_->id_, seg_data->seg_info_.file_id_,
              tbsys::CNetUtil::addrToString(seg_data->ds_[seg_data->pri_ds_index_]).c_str());
          ret = EXIT_FILE_INFO_ERROR;
          //tbsys::gDelete(rsp);
          //continue;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret_len = msg->get_length();

        if (ret_len >= 0)
        {
          if (ret_len < seg_data->cur_size_)
          {
            seg_data->eof_ = TFS_FILE_EOF_FLAG_YES;
          }

          if (ret_len > 0)
          {
            memcpy(seg_data->buf_, msg->get_data(), ret_len);
            seg_data->cur_offset_ += ret_len;
          }

          ret = TFS_SUCCESS;
        }
        else
        {
          //#ifdef __CLIENT_METRICS__
          //        read_metrics_.incr_failed_count();
          //#endif
          snprintf(error_message_, ERR_MSG_SIZE, "tfs read read fail from dataserver: %s, ret len: %d",
              tbsys::CNetUtil::addrToString(seg_data->ds_[seg_data->pri_ds_index_]).c_str(), ret_len);
          //set errno
          ret = TFS_ERROR;
        }
      }
    }
    else if (STATUS_MESSAGE == rsp->get_message_type())
    {
      //#ifdef __CLIENT_METRICS__
      //      read_metrics_.incr_failed_count();
      //#endif
      StatusMessage* msg = dynamic_cast<StatusMessage*>(rsp);
      snprintf(error_message_, ERR_MSG_SIZE, "tfs read, get status msg: %s", msg->get_error());
      if (STATUS_MESSAGE_ACCESS_DENIED == msg->get_status())
      {
        // if access denied, return directly
        //tbsys::gDelete(rsp);
        //return ret_len;
      }
    }
    else
    {
      //#ifdef __CLIENT_METRICS__
      //      read_metrics_.incr_failed_count();
      //#endif
      snprintf(error_message_, ERR_MSG_SIZE, "message type is error.");
    }
    tbsys::gDelete(rsp);
  }

   return ret;
}
