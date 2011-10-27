#ifndef TFS_INIT_FOR_TEST_H
#define TFS_INIT_FOR_TEST_H


#include<tfs_rc_client_api.h>
#include<string>
#include<gtest/gtest.h>
#include<limits.h>

using namespace std;
using namespace tfs::client;
using namespace tfs::common;

class TfsInitTest: public testing::Test
{
  protected:
    RcClient* tfsclient;  
    bool ret;
    virtual void SetUp()
  	{
	    tfsclient=new RcClient();
   	}
  	virtual void TearDown()
  	{
    }
};
#endif
