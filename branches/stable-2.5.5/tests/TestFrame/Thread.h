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
#ifndef THREAD_H_
#define THREAD_H_
class Thread
{
  public:
    virtual void run (void ) = 0;
    virtual ~Thread(){}
  protected:
    int interior_loop;
};
#endif
