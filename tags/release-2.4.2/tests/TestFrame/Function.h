/*
* (C) 2007-2011 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   yiming.czw <yiming.czw@taobao.com>
*      - initial release
*
*/
#ifndef FUNCTION_H_
#define FUNCTION_H_

#include <sys/time.h>
#include <stdio.h>

static const int Save_File_Raw_Type = 1;
static const int Save_File_Meta_Type = 2;
static const int Fetch_File_Raw_Type = 3;
static const int Fetch_File_Meta_Type = 4;

char *create_random_file()
{
  return "fff";
}

char *CreateLocalFile(const int thread_num, const int count)
{
  char *Name = new char [16];
  char *Thread = new char [4];
  char *val = new char[2];

  val[0] = 'b';
  val[1] = '\0';

  sprintf(Name, "%d", count);
  sprintf(Thread, "%d", thread_num);

  Name = strcat(Name, val);
  Name = strcat(Name, Thread);

  delete Thread;
  Thread = NULL;

  delete val;
  val = NULL;

  return Name;
}


char *CreateMetaName(const int thread_num, const int count)
{
  char *Name = new char [16];
  char *Thread = new char [4];
  char *val = new char[2];

  val[0] = 'b';
  val[1] = '\0';

  sprintf(Name, "/%d", count);
  sprintf(Thread, "%d", thread_num);

  Name = strcat(Name, val);
  Name = strcat(Name, Thread);

  delete Thread;
  Thread = NULL;

  delete val;
  val = NULL;

  return Name;
}
#endif
