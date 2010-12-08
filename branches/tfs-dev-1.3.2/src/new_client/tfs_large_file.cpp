#include "tfs_large_file.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;


TfsLargeFile::TfsLargeFile()
{
}

TfsLargeFile::~TfsLargeFile()
{
}

int TfsLargeFile::open(const char* file_name, const char *suffix, int flags, ... )
{
  int ret = TFS_ERROR;

  flag_ = flags;
  va_list args;
  va_start(args, flags);
  char* local_key = va_arg(args, char*);
  if (!local_key)
  {
    TBSYS_LOG(ERROR, "open with large mode occur null key");
    return ret;
  }
  ret = local_key_.initialize(local_key);
  if (ret != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "initialize local key fail, ret: %d", ret);
  }
  return ret;
}

int TfsLargeFile::read(void* buf, size_t count)
{

}

int TfsLargeFile::write(const void* buf, size_t count)
{
  //do not consider the update operation now
  int ret = TFS_SUCCESS;
  int64_t current_count = 0;
  int64_t remain_count = count;
  while (remain_count > 0)
  {
    if (remain_count < BATCH_SIZE)
    {
      current_count = remain_count;
    }
    else
    {
      current_count = BATCH_SIZE;
    }

    int64_t remainder = current_count % SEGMENT_SIZE;
    int64_t block_count = ((0 == remainder) ? current_count / SEGMENT_SIZE : current_count / SEGMENT_SIZE + 1);

    // open
    std::multimap<uint32_t, VUINT64> segments;
    int ret = batch_open(block_count, segments);
    if (TFS_SUCCESS != ret)
    {
      //do retry?
    }
    else
    {
      //create tfs file
      int index = 0;
      std::multimap<uint32_t, VUINT64>::iterator mit = segments.begin();
      current_files_.clear();
      int64_t current_remain_size = current_count;
      for ( ; mit != segments.end() && current_remain_size > 0; ++mit)
      {
        int64_t local_block_size = (current_remain_size >= SEGMENT_SIZE) ? SEGMENT_SIZE : current_remain_size;
        TfsSmallFile* small_file = new TfsSmallFile(mit->first, mit->second, buf + index * SEGMENT_SIZE, local_block_size);
        current_files_.push_back(small_file);
        ++index;
        current_remain_size -= local_block_size;
      }
    }

    // create file
    ret = process(FILE_PHASE_CREATE_FILE);
    if (TFS_SUCCESS != ret)
    {
      //do retry?
    }

    // write data
    ret = process(FILE_PHASE_WRITE_DATA);
    if (TFS_SUCCESS != ret)
    {
      //do retry?
    }

    // close file
    ret = process(FILE_PHASE_CLOSE_FILE);
    if (TFS_SUCCESS != ret)
    {
      //do retry?
    }

    remain_count -= current_count;
  }
}

off_t TfsLargeFile::lseek(off_t offset, int whence)
{

}

ssize_t TfsLargeFile::pread(void* buf, size_t count, off_t offset)
{

}

ssize_t TfsLargeFile::pwrite(const void* buf, size_t count, off_t offset)
{

}

int TfsLargeFile::close()
{

}

int TfsLargeFile::batch_open(const int64_t count, std::multimap<uint32_t, VUINT64>& segments)
{
}

int TfsLargeFile::process(const InnerFilePhase file_phase)
{
  // ClientManager singleton
  int ret = TFS_SUCCESS;
  int64_t wait_id = 0;
  int64_t size = current_files_.size();
  global_client_manager.get_wait_id(wait_id); 
  std::vector<TfsFile*>::iterator vit = current_files_.begin();
  for (int64_t i = 0; i < size; ++i)
  {
    current_files_[i]->do_async_request(file_phase, wait_id);
    if (TFS_SUCCESS != ret)
    {
      // wrong
    }
  }

  std::map<int64_t, tbnet::Packet*> packets;
  ret = global_client_manager.get_response(wait_id, current_files_.size(), WAIT_TIME_OUT, packets);
  if (TFS_SUCCESS != ret)
  {
    //wrong
  }
  else
  {
    std::map<int64_t, tbnet::Packet*>::iterator mit = packets.begin(); 
    for ( ; mit != packets.end(); ++mit)
    {
      ret = current_files_[mit->first]->do_async_response(file_phase, mit->second);
      if (TFS_SUCCESS != ret)
      {
        // wrong
      }
    }
  }

  return ret;
}
