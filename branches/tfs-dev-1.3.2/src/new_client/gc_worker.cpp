#include <sys/file.h>
#include <sys/types.h>
#include <dirent.h>
#include <string>

#include "common/directory_op.h"
#include "common/func.h"

#include "gc_worker.h"
#include "fsname.h"

using namespace tfs::common;
using namespace tfs::client;

GcWorker::GcWorker() : tfs_client_(NULL)
{
}

GcWorker::~GcWorker()
{
}

int GcWorker::start()
{
  int ret = TFS_SUCCESS;

  // gc expired local key and garbage gc file sequencially, maybe use thread

  // no need to init
  tfs_client_ = TfsClient::Instance();

  // gc expired local key
  if ((ret = start_gc(GC_EXPIRED_LOCAL_KEY)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "gc expired local key fail, ret: %d", ret);
  }
  else
  {
    TBSYS_LOG(INFO, "gc expired local key success");
  }

  // gc file
  if ((ret = start_gc(GC_GARBAGE_FILE)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "gc garbage file fail, ret: %d", ret);
  }
  else
  {
    TBSYS_LOG(INFO, "gc garbage file success");
  }
  return ret;
}

int GcWorker::start_gc(GcType gc_type)
{
  const char* gc_path = NULL;
  int ret = TFS_SUCCESS;

  if (GC_EXPIRED_LOCAL_KEY == gc_type)
  {
    gc_path = LOCAL_KEY_PATH;
  }
  else if (GC_GARBAGE_FILE == gc_type)
  {
    gc_path = GC_FILE_PATH;
  }

  // private use, gc_type is valid
  if (!DirectoryOp::is_directory(gc_path))
  {
    TBSYS_LOG(ERROR, "gc path doesn't exist: %s");
    ret = TFS_ERROR;            // TFS_SUCCESS ?
  }
  else
  {
    // gc expired file
    if ((ret = get_expired_file(gc_path)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get expired file fail, ret: %d", ret);
    }
    else if (0 == file_.size())
    {
      TBSYS_LOG(INFO, "no expired file, no gc");
    }
    else
    {
      if ((ret = do_gc(gc_type)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "gc fail, ret: %d");
      }
    }
  }
  return ret;
}


int GcWorker::get_expired_file(const char* path)
{
  int ret = TFS_SUCCESS;
  DIR* dir = opendir(path);
  if (NULL == dir)
  {
    TBSYS_LOG(ERROR, "can not open directory: %s, error: %s", path, strerror(errno));
    ret = TFS_ERROR;
  }
  else
  {
    struct dirent* dir_entry = readdir(dir);
    time_t now = time(NULL);
    while(dir_entry != NULL)
    {
      check_file(path, dir_entry->d_name, now);
      dir_entry = readdir(dir);
    }
  }
  return ret;
}

int GcWorker::check_file(const char* path, const char* file, time_t now)
{
  int ret = TFS_SUCCESS;
  char file_path[MAX_PATH_LENGTH];
  int32_t len = strlen(file);

  strncpy(file_path, path, MAX_PATH_LENGTH - len - 1);
  strncpy(file_path + strlen(path), file, len + 1);

  struct stat64 file_info;
  if (stat64(file_path, &file_info) != 0)
  {
    TBSYS_LOG(ERROR, "stat file fail: %s, error: %s", file_path, strerror(errno));
    ret = TFS_ERROR;
  }
  else if (S_ISREG(file_info.st_mode) && now - file_info.st_mtime > GC_EXPIRED_TIME)
  {
    TBSYS_LOG(INFO, "file need gc: %s, last modify time: %s",
              file_path, Func::time_to_str(file_info.st_mtime).c_str());
    file_.push_back(file_path);
  }
  else
  {
    TBSYS_LOG(INFO, "file no need to gc: %s, last modify time: %s",
              file_path, Func::time_to_str(file_info.st_mtime).c_str());
  }
  return ret;
}

int GcWorker::do_gc(GcType gc_type)
{
  int ret = TFS_SUCCESS;
  string::size_type id_pos = 0;
  int fd = -1;

  for (size_t i = 0; i < file_.size(); i++)
  {
    string& file_name = file_[i];
    if ((id_pos = file_name.rfind('!')) != string::npos)
    {
      TBSYS_LOG(ERROR, "file name is not valid, no server id: %s", file_name.c_str());
    }
    else if ((fd = check_lock(file_name.c_str())) < 0)
    {
      TBSYS_LOG(INFO, "file is locked, maybe other gc process is master it");
    }
    else
    {
      string addr = tbsys::CNetUtil::addrToString(atoll(file_[i].substr(id_pos).c_str()));
      // expired local key
      if (GC_EXPIRED_LOCAL_KEY == gc_type)
      {
        if ((ret = do_gc_ex(local_key_, file_name.c_str(), addr.c_str())) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "gc local key fail, file name: %s, ret: %d", file_name.c_str(), ret);
        }
      }
      // garbage file
      else if (GC_GARBAGE_FILE == gc_type)
      {
        if ((ret = do_gc_ex(gc_file_, file_name.c_str(), addr.c_str())) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "gc garbage file fail, file name: %s, ret: %d", file_name.c_str(), ret);
        }
      }

      // release lock, maybe add to FileOperation
      flock(fd, LOCK_UN);
      close(fd);
    }
  }

  return ret;
}

template<class T> int GcWorker::do_gc_ex(T meta, const char* file_name, const char* addr)
{
  int ret = TFS_SUCCESS;
  if ((ret = meta.load_file(file_name)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "load fail, ret: %d", ret);
  }
  else if ((ret = do_unlink(meta.get_seg_info(), addr)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "do unlink fail, ret: %d", ret);
  }

  // ignore if unlink all file success, just remove ?
  if ((ret = meta.remove()) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "remove file fail, file: %s, ret: %d", file_name, ret);
  }

  return ret;
}

template<class T> int GcWorker::do_unlink(T seg_info, const char* addr)
{
  int ret = TFS_SUCCESS;
  FSName fsname;

  for (typename T::iterator it = seg_info.begin(); it != seg_info.end(); it++)
  {
    // just not to depend on inner module, use TfsCliet directely
    // little dummy
    fsname.set_block_id(it->block_id_);
    fsname.set_file_id(it->file_id_);
    if ((ret = tfs_client_->unlink(fsname.get_name(), NULL, addr))
        != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "gc segment fail, block id: %u, fileid: %"PRI64_PREFIX"u",
                it->block_id_, it->file_id_);
    }
    else
    {
      TBSYS_LOG(DEBUG, "gc segment success, block id: %u, fileid: %"PRI64_PREFIX"u",
                it->block_id_, it->file_id_);
    }
  }
  // just ignore return code
  return ret;
}

// maybe add to FileOperation
int GcWorker::check_lock(const char* file)
{
  int fd = open(file, O_RDONLY);
  // can not open recognize as lock
  if (fd > 0 && flock(fd, LOCK_EX|LOCK_NB) < 0)
  {
    fd = -1;
  }
  return fd;
}
