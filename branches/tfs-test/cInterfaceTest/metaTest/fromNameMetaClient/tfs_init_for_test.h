#ifndef TFS_INIT_FOR_TEST_H
#define TFS_INIT_FOR_TEST_H


#include<tfs_meta_client_api.h>
#include<string>
#include<gtest/gtest.h>
#include<limits.h>

using namespace std;
using namespace tfs::client;
using namespace tfs::common;

class TfsInitTest: public testing::Test
{
    protected:
    NameMetaClient* tfsclient;  
    bool ret;
    virtual void SetUp()
  	{
	    tfsclient=new NameMetaClient();
   	}
  	virtual void TearDown()
  	{
        
        }
};
#endif
