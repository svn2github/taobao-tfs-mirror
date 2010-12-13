#include "tfs_file.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;

TfsFile::TfsFile() : tfs_session_(NULL), flags_(-1), is_open_(TFS_FILE_OPEN_NO), eof_(TFS_FILE_EOF_NO), offset_(0)
{
  error_message_[0] = '\0';
}

TfsFile::~TfsFile()
{
  for (int32_t i = 0; i < seg_list_.size(); i++)
  {
    tbsys::gDelete(seg_list_[i]);
  }
}

int TfsFile::open_ex(const char* file_name, const char* suffix, int32_t flags)
{
  // need ?
  if (tfs_session_ == NULL)
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
  ds_list_.clear();

  if ((ret = tfs_session_->get_block_info(block_id, ds_list_, flags_)) != TFS_SUCCESS)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "tfs open fail: get block info fail, blockid(%u), fileid(%"
             PRI64_PREFIX "u), mode(%d), ret(%d)", block_id, file_id, flags, ret);
    return ret;
  }

  // get primary ds index
  if ((flags_ == T_READ))
  {
    if (file_id == 0)
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
    snprintf(error_message_, ERR_MSG_SIZE, "connect to dataserver(%"PRI64_PREFIX"u) fail, ret(%d)",
             ds_list_[pri_ds_index_], ret);
    return ret;
  }

  if ((flags_ & T_WRITE))
  {
    if ((ret = create_filename()) != TFS_SUCCESS)
    {
      // create_filename log error message
      TBSYS_LOG(ERROR, "create file name fail, fileid(%"PRI64_PREFIX"u), ret(%d)", file_id, ret);
      return ret;
    }
  }

  offset_ = 0;
  eof_ = TFS_FILE_EOF_NO;
  crc_ = 0;
  is_open_ = TFS_FILE_OPEN_YES;
  return ret;

}

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

int TfsFile::create_filename()
{
  int ret = TFS_ERROR;
  CreateFilenameMessage dsmessage;
  dsmessage.set_block_id(fsname_.get_block_id());
  dsmessage.set_file_id(fsname_.get_file_id());
  Message *message = client_->call(&dsmessage);

  if (!message)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "createfilename send request fail");
  }
  else if (message->get_message_type() != RESP_CREATE_FILENAME_MESSAGE)
  {
    if (message->get_message_type() == STATUS_MESSAGE)
    {
      StatusMessage* msg = dynamic_cast<StatusMessage*> (message);
      ret = msg->get_status();
      snprintf(error_message_, ERR_MSG_SIZE, "create file name fail, get error msg(%s), ret(%d) from(%s)",
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
    if (!fsname_.get_file_id())
    {
      snprintf(error_message_, ERR_MSG_SIZE, "create file name fail: fileid(0)");
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

int TfsFile::process(const InnerFilePhase file_phase)
{
  // ClientManager singleton
  int ret = TFS_SUCCESS;
  int64_t wait_id = 0;
  int32_t size = seg_list_.size();
  global_client_manager.get_wait_id(wait_id);
  for (int32_t i = 0; i < size; ++i)
  {
    if ((ret = do_async_request(file_phase, wait_id, *seg_list_[i])) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "request %d fail", file_phase);
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
      ret = do_async_response(file_phase, mit->second);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "response fail, ret:%d", ret);
        return ret;
      }
    }
  }

  return ret;
}

int TfsFile::do_async_request(const InnerFilePhase file_phase, const int64_t wait_id, SegmentData& seg_data)
{
  int ret = TFS_SUCCESS;
  switch (file_phase)
  {
  case FILE_PHASE_CREATE_FILE:
    ret = async_req_create_file(wait_id, seg_data);
    break;
  case FILE_PHASE_READ_FILE:
    ret = async_req_read_file(wait_id, seg_data);
    break;
  case FILE_PHASE_WRITE_DATA:
    ret = async_req_write_data(wait_id, seg_data);
    break;
  case FILE_PHASE_CLOSE_FILE:
    ret = async_req_close_file(wait_id, seg_data);
    break;
  }
}

int TfsFile::do_async_response(const InnerFilePhase file_phase, tbnet::Packet* packet)
{

}
