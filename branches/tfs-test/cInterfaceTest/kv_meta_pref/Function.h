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

char * CreateName(int num,int count)
{
  char * Name = new char [16];
  char * Thread = new char [4];
  char * val =new char[2];
  val[0]='A';
  val[1]='\0';
  sprintf(Name, "%d", count);
  sprintf(Thread, "%d", num);

  Name=strcat(Name,val);
  Name=strcat(Name,Thread);
  delete [] Thread;
  delete [] val;
  Thread =NULL;
  val =NULL;
  return Name;
}
#endif
