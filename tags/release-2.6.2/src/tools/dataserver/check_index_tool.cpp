/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
#include <iostream>
#include <string>
#include "common/internal.h"
#include "dataserver/ds_define.h"
#include "clientv2/fsname.h"
#include "common/file_opv2.h"
#include "common/version.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::dataserver;
using namespace tfs::clientv2;

struct FileInfoOffsetCompare
{
  bool operator () (const FileInfoV2& left, const FileInfoV2& right)
  {
    return left.offset_ < right.offset_;
  }
};

void dump_index_header(const IndexHeaderV2& header)
{
  printf("block id:                  %"PRI64_PREFIX"u\n", header.info_.block_id_);
  printf("family id:                 %"PRI64_PREFIX"d\n", header.info_.family_id_);
  printf("version:                   %d\n", header.info_.version_);
  printf("file count:                %d\n", header.info_.file_count_);
  printf("file size:                 %d\n", header.info_.size_);
  printf("del file count:            %d\n", header.info_.del_file_count_);
  printf("del file size:             %d\n", header.info_.del_size_);
  printf("update file count:         %d\n", header.info_.update_file_count_);
  printf("update file size:          %d\n",  header.info_.update_size_);
  printf("used offset:               %d\n", header.used_offset_);
  printf("seq no:                    %d\n", header.seq_no_);
  printf("marshalling offset:        %d\n", header.marshalling_offset_);
  printf("available offset:          %d\n", header.avail_offset_);
  printf("hash slot(index num):      %d\n", header.file_info_bucket_size_);
  printf("used hash slot:            %d\n", header.used_file_info_bucket_size_);
  printf("last check time:           %d\n", header.last_check_time_);
  printf("data crc:                  %u\n", header.data_crc_);
  printf("\n");
}

void dump_file_info(const FileInfoV2& file_info, const int slot)
{
  printf("%-6d%-28"PRI64_PREFIX"u%-10d%-10d%-5d%-10d%-12u%-20s%-20s\n",
      slot,
      file_info.id_,
      file_info.offset_,
      file_info.size_,
      file_info.status_,
      file_info.next_,
      file_info.crc_,
      Func::time_to_str(file_info.create_time_).c_str(),
      Func::time_to_str(file_info.create_time_).c_str());
}

void traverse(FileOperation& file_op, const int32_t offset, const int32_t items, std::vector<FileInfoV2>& infos)
{
  int ret = TFS_SUCCESS;
  FileInfoV2 file_info;
  for (int i = 0; i < items; i++)
  {
    ret = file_op.pread((char*)(&file_info), FILE_INFO_V2_LENGTH,
        offset + i * FILE_INFO_V2_LENGTH);
    if (FILE_INFO_V2_LENGTH == ret)
    {
      if (INVALID_FILE_ID != file_info.id_)
      {
        // dump_file_info(file_info, i);
        infos.push_back(file_info);
      }
    }
    else
    {
      printf("file %s format error\n", file_op.get_path().c_str());
      break;
    }
  }
}

int main(int argc, char* argv[])
{
  if (argc != 2)
  {
    cout << Version::get_build_description() << endl;
    cout << "Usage: " << argv[0] << " filename " << endl;
    return -1;
  }

  string file = argv[1];
  FileOperation* file_op = new FileOperation(argv[1]);

  IndexHeaderV2 header;
  std::vector<FileInfoV2> infos;
  int ret = file_op->pread((char*)(&header), INDEX_HEADER_V2_LENGTH, 0);
  if (INDEX_HEADER_V2_LENGTH == ret)
  {
    // dump_index_header(header);
  }

  // normal block
  if (!IS_VERFIFY_BLOCK(header.info_.block_id_))
  {
    traverse(*file_op,
        INDEX_HEADER_V2_LENGTH,
        header.file_info_bucket_size_,
        infos);
    if (infos.size() > 0)
    {
      uint64_t last_id = 0;
      int32_t last_offset = -1;
      std::sort(infos.begin(), infos.end(), FileInfoOffsetCompare());
      std::vector<FileInfoV2>::iterator it = infos.begin();
      for ( ; it != infos.end(); it++)
      {
        if (it->offset_ == last_offset)
        {
          FSName fsname(header.info_.block_id_, last_id);
          printf("%lu %lu %s\n", header.info_.block_id_, last_id, fsname.get_name());
          break;
        }
        last_offset = it->offset_;
        last_id = it->id_;
      }

      if (it == infos.end())
      {
        if (last_offset >= header.used_offset_)
        {
          FSName fsname(header.info_.block_id_, last_id);
          printf("%lu %lu %s\n", header.info_.block_id_, last_id, fsname.get_name());
        }
      }
    }
  }

  return 0;
}
