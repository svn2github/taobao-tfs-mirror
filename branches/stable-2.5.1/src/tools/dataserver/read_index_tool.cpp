/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: read_index_tool.cpp 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   chuyu <chuyu@taobao.com>
 *      - modify 2010-03-20
 *
 */
#include <iostream>
#include <string>
#include "common/internal.h"
#include "dataserver/ds_define.h"
#include "common/file_opv2.h"
#include "common/version.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::dataserver;

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
  printf("marshalling offset:        %d\n", header.marshalling_offset_);
  printf("available offset:          %d\n", header.avail_offset_);
  printf("hash slot(index num):      %d\n", header.file_info_bucket_size_);
  printf("used hash slot:            %d\n", header.used_file_info_bucket_size_);
  printf("\n");
}

void dump_file_info_header()
{
  printf("%-6s%-28s%-10s%-10s%-5s%-10s%-12s%-20s%-20s\n",
      "SLOT",
      "ID",
      "OFFSET",
      "SIZE",
      "STAT",
      "NEXT",
      "CRC",
      "CREATE TIME",
      "MODIFY TIME");
}

struct InnerIndex
{
  uint64_t logic_block_id_;
  int32_t offset_;
  int32_t size_;
};

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

void dump_inner_index_header()
{
  printf("%-20s%-10s%-10s\n",
      "ATTACH BLOCKID",
      "OFFSET",
      "SIZE");
}

void dump_inner_index(const InnerIndex& index)
{
  printf("%-20"PRI64_PREFIX"u%-10d%-10d\n",
      index.logic_block_id_,
      index.offset_,
      index.size_);
}

void dump_all_file_infos(FileOperation& file_op, const int32_t offset, const int32_t items)
{
  int ret = TFS_SUCCESS;
  dump_file_info_header();
  FileInfoV2 file_info;
  for (int i = 0; i < items; i++)
  {
    ret = file_op.pread((char*)(&file_info), FILE_INFO_V2_LENGTH,
        offset + i * FILE_INFO_V2_LENGTH);
    if (FILE_INFO_V2_LENGTH == ret)
    {
      if (INVALID_FILE_ID != file_info.id_)
      {
        dump_file_info(file_info, i);
        while (file_info.next_ != 0)
        {
          int32_t slot = file_info.next_;
          ret = file_op.pread((char*)(&file_info), FILE_INFO_V2_LENGTH,
              offset + slot * FILE_INFO_V2_LENGTH);
          if (FILE_INFO_V2_LENGTH == ret)
          {
            dump_file_info(file_info, slot);
          }
          else
          {
            break;
          }
        }
      }
    }
    else
    {
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
  int ret = file_op->pread((char*)(&header), INDEX_HEADER_V2_LENGTH, 0);
  if (INDEX_HEADER_V2_LENGTH == ret)
  {
    dump_index_header(header);
  }

  // normal block
  if (!IS_VERFIFY_BLOCK(header.info_.block_id_))
  {
    dump_all_file_infos(*file_op,
        INDEX_HEADER_V2_LENGTH,
        header.file_info_bucket_size_);
  }
  else  // check block
  {
    InnerIndex inner_index[16]; // no more than 16 index
    dump_inner_index_header();
    for (int i = 0; i < header.index_num_; i++)
    {
      ret = file_op->pread((char*)(&inner_index[i]), sizeof(InnerIndex),
          INDEX_HEADER_V2_LENGTH + i * sizeof(InnerIndex));
      if (sizeof(InnerIndex) == ret)
      {
        dump_inner_index(inner_index[i]);
      }
      else
      {
        break;
      }
    }

    printf("\n==============================================================\n");

    for (int i = 0; i < header.index_num_; i++)
    {
      IndexHeaderV2 inner_header;
      ret = file_op->pread((char*)(&inner_header), INDEX_HEADER_V2_LENGTH,
          inner_index[i].offset_);
      if (INDEX_HEADER_V2_LENGTH == ret)
      {
        dump_index_header(inner_header);
      }
      else
      {
        break;
      }

      dump_all_file_infos(*file_op,
          INDEX_HEADER_V2_LENGTH + inner_index[i].offset_,
          inner_header.file_info_bucket_size_);

      printf("\n==============================================================\n");
    }
  }

  return 0;
}
