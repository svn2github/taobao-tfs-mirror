/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_common_utils.cpp 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */
#include "test_common_utils.h"
#include "test_gfactory.h"

int TestCommonUtils::generateData(char *data, int size)
{
	srand(time(NULL) + rand() + pthread_self());

	for(int i = 0; i < size; i ++) 
  {
		data[ i ] = rand() % 90 + 32;
	}
  data[ size ] = 0;

	return size;
}

int TestCommonUtils::readFilelist(char *filelist, VUINT32& crc_set, VSTRING& filename_set)
{
  FILE *fp = NULL;
  if((fp = fopen(filelist, "r")) == NULL)
  {
    TBSYS_LOG(DEBUG,"open file_list failed.");
    return -1;
  }
  uint32_t crc = 0;
  char filename[64];
  while (fgets(filename, sizeof(filename), fp))
  {
    if (filename[strlen(filename) - 1] == '\n')
    {
      filename[strlen(filename) - 1] = '\0';
    }
    else 
    {
      filename[strlen(filename)] = '\0';
    }
    char *p = strchr(filename, ' ');
    if (NULL != p)
    {
      *p++ = '\0';
      sscanf(p, "%u", &crc);

      filename_set.push_back(filename);
      crc_set.push_back(crc);
    }
  }
  return 0;
}


int TestCommonUtils::getFilelist(int part_no, int part_size, VUINT32& crc_set, 
      VUINT32& crc_set_per_thread, VSTRING& filename_set, VSTRING& filename_set_per_thread)
{
  // total size less than thread count
  if (part_size == 0)
  {
    part_size = 1; 
  }
  
  int offset = part_no * part_size;
  int end = (offset + part_size) > static_cast<int>(crc_set.size()) ? crc_set.size() : (offset + part_size); 
  for (; offset < end; offset++)
  {
    crc_set_per_thread.push_back(crc_set[offset]);
    filename_set_per_thread.push_back(filename_set.at(offset).c_str());
  }

  // the last thread eat the left
  if (part_no == (TestGFactory::_threadCount - 1) )
  {
    for (; offset < static_cast<int>(crc_set.size()); offset++)
    {
      crc_set_per_thread.push_back(crc_set[offset]);
      filename_set_per_thread.push_back(filename_set.at(offset).c_str());
    }
  }

  //debug
  /*
  char fileListPerThread[20] = {0};
  sprintf(fileListPerThread, "./read_file_list_%d.txt", part_no);
  FILE *fp = fopen(fileListPerThread, "w");
  VSTRING::iterator it = filename_set_per_thread.begin();
  for (; it != filename_set_per_thread.end(); it++)
  {
     fprintf(fp, "%s\n", it->c_str());
  } 
  fflush(fp);
  fclose(fp);
  */
  return 0;

}
