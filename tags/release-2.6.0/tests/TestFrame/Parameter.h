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
#ifndef PARAMETER_H_
#define PARAMETER_H_
class Parameter
{
  public:
   const char *rs_addr;
   const char *app_key;

   const char *local_file;

   int thread_num;
   int interior_loop;
};
#endif
