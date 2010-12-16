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
  destroy_seg();
}

void TfsFile::destroy_seg()
{
  for (size_t i = 0; i < processing_seg_list_.size(); i++)
  {
    tbsys::gDelete(processing_seg_list_[i]);
  }
  processing_seg_list_.clear();
}

int TfsFile::open_ex(const char* file_name, const char* suffix, int32_t flags)
{
  // need ?
  if (tfs_session_ == NULL)
  {
    TBSYS_LOG(ERROR, "session is not initialized");
    return TFS_ERROR;
  }

  flags_ = flags;
  fsname_.set_name(file_name, suffix);
  fsname_.set_cluster_id(tfs_session_->get_cluster_id());
  uint32_t block_id = fsname_.get_block_id();
  uint64_t file_id = fsname_.get_file_id();
  int ret = TFS_ERROR;

  meta_seg_ = new SegmentData();
  if ((ret = tfs_session_->get_block_info(block_id, meta_seg_->ds_, flags_)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "tfs open fail: get block info fail, blockid: %u, fileid: %"
             PRI64_PREFIX "u, mode: %d, ret: %d", block_id, file_id, flags, ret);
    return ret;
  }

  if ((flags_ & T_WRITE))
  {
    if ((ret = create_filename()) != TFS_SUCCESS) // TODO ...
    {
      // create_filename log error message
      TBSYS_LOG(ERROR, "create file name fail, fileid: %"PRI64_PREFIX"u, ret: %d", file_id, ret);
      return ret;
    }
  }

  offset_ = 0;
  eof_ = TFS_FILE_EOF_NO;
  // crc_ = 0;
  is_open_ = TFS_FILE_OPEN_YES;
  return ret;
}

int64_t TfsFile::read_ex(void* buf, int64_t count, int64_t offset, bool modify)
{
  // TODO .. negative error code needed ..
  int ret = TFS_ERROR;
  if (flags_ & T_WRITE)
  {
    TBSYS_LOG(ERROR, "file open without read flag");
    return ret;
  }

  if (is_open_ != TFS_FILE_OPEN_YES)
  {
    TBSYS_LOG(ERROR, "file not open");
    return ret;
  }

  if ((ret = get_segment_for_read(offset, reinterpret_cast<char*>(buf), count)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "get segment for read fail, ret:%d", ret);
    return ret;
  }

  if ((ret = read_process()) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "read data fail, ret:%d", ret);
    return ret;
  }

  if (modify)                   // TODO, lock
  {
    offset_ += count;
  }

  return ret;
}

int64_t TfsFile::write_ex(const void* buf, int64_t count, int64_t offset, bool modify)
{
  //do not consider the update operation now
  int ret = TFS_ERROR;
  if (!(flags_ & T_WRITE))
  {
    TBSYS_LOG(ERROR, "file open without write flag");
    return ret;
  }

  if (is_open_ != TFS_FILE_OPEN_YES)
  {
    TBSYS_LOG(ERROR, "file not open");
    return ret;
  }

  if ((ret = get_segment_for_write(offset, reinterpret_cast<const char*>(buf), count)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "get segment error, ret: %d", ret);
    return ret;
  }

  int32_t seg_count = processing_seg_list_.size();
  if (!seg_count)
  {
    TBSYS_LOG(DEBUG, "data already written, offset:%d, size: %d", offset, count);
    return count;
  }

  if ((ret = write_process()) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "write fail, ret: %d");
    return ret;
  }

  if (modify)                   // TODO, lock
  {
    offset_ += count;
  }
  return ret;
}

int64_t TfsFile::lseek_ex(int64_t offset, int whence)
{
  int ret = -1;                 // new negative error code
  if (!(flags_ & READ_MODE))
  {
    TBSYS_LOG(ERROR, "file open without read flag");
    return ret;
  }
  if (is_open_ == TFS_FILE_OPEN_NO)
  {
    TBSYS_LOG(ERROR, "file not open");
    return ret;
  }

  switch (whence)
  {
  case T_SEEK_SET:
    offset_ = offset;
    ret = offset_;
    break;
  case T_SEEK_CUR:
    offset_ += offset;
    ret = offset_;
    break;
  default:
    TBSYS_LOG(ERROR, "unknown seek flag: %d", whence);
    break;
  }
  return ret;
}

int64_t TfsFile::pread_ex(void* buf, int64_t count, int64_t offset)
{
  return read_ex(buf, count, offset, false);
}

int64_t TfsFile::pwrite_ex(const void* buf, int64_t count, int64_t offset)
{
  return write_ex(buf, count, offset, false);
}

int TfsFile::stat_ex(FileInfo* file_info, int32_t mode)
{

}

int TfsFile::close_ex()
{
  int ret = TFS_SUCCESS;
  if (is_open_ == TFS_FILE_OPEN_NO)
  {
    TBSYS_LOG(INFO, "close tfs file successful , buf tfs file not open");
  }
  else if (!((flags_ & WRITE_MODE) && offset_))
  {
    TBSYS_LOG(INFO, "close tfs file successful");
  }
  else if ((ret = close_process()) != TFS_SUCCESS) // write mode
  {
    TBSYS_LOG(ERROR, "close tfs file fail, ret:%d", ret);
  }

  if (TFS_SUCCESS == ret)
  {
    is_open_ = TFS_FILE_OPEN_NO;
    offset_ = 0;
  }
  return ret;
}

void TfsFile::set_session(TfsSession* tfs_session)
{
  tfs_session_ = tfs_session;
}

const char* TfsFile::get_file_name()
{
  return fsname_.get_name();
}

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
  case FILE_PHASE_STAT_FILE:
    ret = async_req_stat_file(wait_id, index);
    break;
  default:
    TBSYS_LOG(ERROR, "unknow file phase, phase: %d", file_phase);
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
  case FILE_PHASE_STAT_FILE:
    ret = async_rsp_stat_file(packet, index);
    break;
  default:
    TBSYS_LOG(ERROR, "unknow file phase, phase: %d", file_phase);
    ret = TFS_ERROR;
    break;
  }
  return ret;
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
    seg_data->status_ = SEG_STATUS_FAIL;
  }
  else
  {
    ret = global_client_manager.post_request(seg_data->ds_[0], &cf_message, wait_id);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "create file post request fail. ret: %d, waitid: %d, blockid: %u, fileid: %" PRI64_PREFIX "u",
          ret, wait_id, seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_);
      seg_data->status_ = SEG_STATUS_FAIL;
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
    TBSYS_LOG(ERROR, "create file name rsp null");
    seg_data->status_ = SEG_STATUS_FAIL;
  }
  else if (RESP_CREATE_FILENAME_MESSAGE != rsp->get_message_type())
  {
    if (STATUS_MESSAGE == rsp->get_message_type())
    {
      StatusMessage* msg = dynamic_cast<StatusMessage*>(rsp);
      ret = msg->get_status();
      TBSYS_LOG(ERROR, "create file name fail. get error msg: %s, ret: %d, from: %s",
               msg->get_error(), ret, tbsys::CNetUtil::addrToString(seg_data->ds_[0]).c_str());
      seg_data->status_ = SEG_STATUS_FAIL;
    }
    else
    {
      TBSYS_LOG(ERROR, "create file name fail: unexpected message recieved");
      seg_data->status_ = SEG_STATUS_FAIL;
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
      TBSYS_LOG(ERROR, "create file name fail. fileid: 0");
      seg_data->status_ = SEG_STATUS_FAIL;
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
    seg_data->status_ = SEG_STATUS_FAIL;
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
    TBSYS_LOG(ERROR, "write data rsp null. dsip: %s",
        tbsys::CNetUtil::addrToString(seg_data->ds_[0]).c_str());
    seg_data->status_ = SEG_STATUS_FAIL;
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
        TBSYS_LOG(ERROR, "tfs write data, get error msg: %s, dsip: %s",
            msg->get_error(), tbsys::CNetUtil::addrToString(seg_data->ds_[0]).c_str());
        seg_data->status_ = SEG_STATUS_FAIL;
      }
    }
    else
    {
      seg_data->status_ = SEG_STATUS_FAIL;
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
    seg_data->status_ = SEG_STATUS_FAIL;
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
    TBSYS_LOG(ERROR, "tfs file close, send msg to dataserver time out, blockid: %u, fileid: %"
        PRI64_PREFIX "u, dsip: %s",
        seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_, tbsys::CNetUtil::addrToString(seg_data->ds_[0]).c_str());
    seg_data->status_ = SEG_STATUS_FAIL;
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
        TBSYS_LOG(ERROR, "tfs file close, get errow msg: %s, dsip: %s",
            msg->get_error(), tbsys::CNetUtil::addrToString(seg_data->ds_[0]).c_str());
        seg_data->status_ = SEG_STATUS_FAIL;
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
    seg_data->status_ = SEG_STATUS_FAIL;
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
      TBSYS_LOG(ERROR, "create file fail. blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d",
          seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_, ret);
      seg_data->status_ = SEG_STATUS_FAIL;
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
    TBSYS_LOG(ERROR, "tfs file read fail, send msg to dataserver time out, blockid: %u, fileid: %"
        PRI64_PREFIX "u, dsip: %s",
        seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_,
        tbsys::CNetUtil::addrToString(seg_data->ds_[seg_data->pri_ds_index_]).c_str());
    seg_data->status_ = SEG_STATUS_FAIL;
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
        seg_data->status_ = SEG_STATUS_FAIL;
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
          TBSYS_LOG(ERROR, "tfs read read fail from dataserver: %s, ret len: %d",
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
      TBSYS_LOG(ERROR, "tfs read, get status msg: %s", msg->get_error());
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
      TBSYS_LOG(ERROR, "message type is error.");
    }
    tbsys::gDelete(rsp);
  }

   return ret;
}

int TfsFile::async_req_stat_file(const int64_t wait_id, const int32_t index)
{
  int ret = TFS_ERROR;
  SegmentData* seg_data = processing_seg_list_[index];
  FileInfoMessage stat_message;
  stat_message.set_block_id(seg_data->seg_info_.block_id_);
  stat_message.set_file_id(seg_data->seg_info_.file_id_);
  stat_message.set_mode(flags_);

  int32_t ds_size = seg_data->ds_.size();
  if (0 == ds_size)
  {
    TBSYS_LOG(ERROR, "stat file fail: ds list is empty. blockid: %u, fileid: %" PRI64_PREFIX "u, size: %d",
        seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_, ds_size);
    seg_data->status_ = SEG_STATUS_FAIL;
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
      ret = global_client_manager.post_request(seg_data->ds_[selected_ds_index], &stat_message, wait_id);
      if (EXIT_SENDMSG_ERROR == ret)
      {
        TBSYS_LOG(ERROR, "post stat file req fail. blockid: %u, fileid: %" PRI64_PREFIX "u, dsip: %s",
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
      TBSYS_LOG(ERROR, "stat file fail. blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d",
          seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_, ret);
      seg_data->status_ = SEG_STATUS_FAIL;
    }
  }

  return ret;
}

int TfsFile::async_rsp_stat_file(tbnet::Packet* packet, const int32_t index)
{
  int ret = TFS_ERROR;
  SegmentData* seg_data = processing_seg_list_[index];
  Message* rsp = dynamic_cast<Message*>(packet);

  if (NULL == rsp)
  {
    TBSYS_LOG(ERROR, "tfs file stat fail, send msg to dataserver time out, blockid: %u, fileid: %"
        PRI64_PREFIX "u, dsip: %s",
        seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_,
        tbsys::CNetUtil::addrToString(seg_data->ds_[seg_data->pri_ds_index_]).c_str());
    seg_data->status_ = SEG_STATUS_FAIL;
    ret = EXIT_TIMEOUT_ERROR;
  }
  else
  {
    if (RESP_FILE_INFO_MESSAGE != rsp->get_message_type())
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      if (STATUS_MESSAGE != rsp->get_message_type())
      {
        TBSYS_LOG(ERROR, "stat file fail. blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d, erorr msg: %s",
            seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_, ret, dynamic_cast<StatusMessage*>(rsp)->get_error());
      }
      else
      {
        TBSYS_LOG(ERROR, "stat file fail. blockid: %u, fileid: %" PRI64_PREFIX "u, erorr msg: %s",
            seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_, "msg type error");
      }
    }
    else
    {
      RespFileInfoMessage *msg = dynamic_cast<RespFileInfoMessage*> (rsp);
      if (NULL != msg->get_file_info())
      {
        if (msg->get_file_info()->id_ != seg_data->seg_info_.file_id_)
        {
          TBSYS_LOG(ERROR, "tfs stat fail. msg fileid: %"PRI64_PREFIX"u, require fileid: %"PRI64_PREFIX"u not match",
             msg->get_file_info()->id_, seg_data->seg_info_.file_id_);
          ret = EXIT_FILE_INFO_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          if (NULL == seg_data->file_info_)
          {
            seg_data->file_info_ = new FileInfo();
          }
          memcpy(seg_data->file_info_, msg->get_file_info(), FILEINFO_SIZE);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "tfs stat fail. blockid: %u, fileid: %"PRI64_PREFIX"u is not exist",
            seg_data->seg_info_.block_id_, seg_data->seg_info_.file_id_,
            tbsys::CNetUtil::addrToString(seg_data->ds_[seg_data->pri_ds_index_]).c_str());
        ret = TFS_ERROR;
      }
    }
  }

  tbsys::gDelete(rsp);
  return ret;
}
