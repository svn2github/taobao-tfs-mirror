package com.taobao.common.tfs.testcase.Interface.normal.meta;

import org.junit.Test;
import org.junit.Ignore;

import com.taobao.common.tfs.testcase.metaTfsBaseCase;

import junit.framework.Assert;



public class tfsManager_07_mvFile extends metaTfsBaseCase 
{
	@Test
	public void test_01_mvFile_right()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/textmvFile1src");
       Assert.assertEquals(Ret,0);
	   Ret=tfsManager.mvFile(appId, userId, "/textmvFile1src", "/textmvFile1tar");
       Assert.assertEquals("mvFile with right path should be true", Ret,0);
       tfsManager.rmFile(appId, userId, "/textmvFile1tar");
	   Ret=tfsManager.rmFile(appId, userId, "/textmvFile1src");
	   Assert.assertEquals("mvFile with remove path should be false", Ret,-14001);
	}
	@Test
	public void test_02_mvFile_null_destFilePath()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/textmvFile1src");
       Assert.assertEquals(Ret,0);
	   Ret=tfsManager.mvFile(appId, userId, "/textmvFile1src",null);
       Assert.assertEquals("mvFile with null destFilePath should be false", Ret,-14010);
       Ret=tfsManager.rmFile(appId, userId, "/textmvFile1src");
       Assert.assertEquals(Ret,0);
	}
	@Test
	public void test_03_mvFile_empty_destFilePath()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/text1");
	   Ret=tfsManager.mvFile(appId, userId, "/text1","");
       Assert.assertEquals("mvFile with empty destFilePath should be false", Ret,-14010);
       Ret=tfsManager.rmFile(appId, userId, "/text1");
	}
	@Test
	public void test_04_mvFile_wrong_destFilePath_1()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/text1");
	   Ret=tfsManager.mvFile(appId, userId, "/text1","/");
       Assert.assertEquals("mvFile with wrong_1 destFilePath should be false", Ret,1);
       Ret=tfsManager.rmFile(appId, userId, "/text1");
	}
	@Test
	public void test_05_mvFile_wrong_destFilePath_2()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/text1");
	   Ret=tfsManager.mvFile(appId, userId, "/text1","text2");
       Assert.assertEquals("mvFile with wrong_2 destFilePath should be false", Ret,-14010);
       Ret=tfsManager.rmFile(appId, userId, "/text1");
	}
	@Test
	public void test_06_mvFile_wrong_destFilePath_3()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/textmvFile6src");
	   Ret=tfsManager.mvFile(appId, userId, "/textmvFile6src","///textmvFile6tar///");
       Assert.assertEquals("mvFile with wrong_3 destFilePath be true", Ret,0);
       Ret=tfsManager.rmFile(appId, userId, "/textmvFile6tar");
	}
	@Test
	public void test_07_mvFile_leap_Filename()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/text1");
	   Ret=tfsManager.mvFile(appId, userId, "/text1","/text2/text");
       Assert.assertEquals("mvFile with leap destFilePath should be false", Ret,-14002);
       Ret=tfsManager.rmFile(appId, userId, "/text1");
	}
	@Test
	public void test_08_mvFile_exist_Filename()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/text1");
	   Ret=tfsManager.createFile(appId, userId, "/text2");
	   Ret=tfsManager.mvFile(appId, userId, "/text1","/text2");
       Assert.assertEquals("mvFile with exist destFilePath should be false", Ret,1);
       Ret=tfsManager.rmFile(appId, userId, "/text1");
       Ret=tfsManager.rmFile(appId, userId, "/text2");
	}
	@Test
	public void test_09_mvFile_null_srcFilePath()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.mvFile(appId, userId, null,"/text2");
       Assert.assertEquals("mvFile with null srcFilePath should be false", Ret,-14010);
    }
	@Test
	public void test_10_mvFile_empty_srcFilePath()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.mvFile(appId, userId, "","/text2");
       Assert.assertEquals("mvFile with empty srcFilePath should be false", Ret,-14010);
    }
	@Test
	public void test_11_mvFile_wrong_srcFilePath_1()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.mvFile(appId, userId, "/","/text2");
       Assert.assertEquals("mvFile with wrong-1 srcFilePath should be false", Ret,1);
    }
	@Test
	public void test_12_mvFile_wrong_srcFilePath_2()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/text1");
	   Ret=tfsManager.mvFile(appId, userId, "text1","/text2");
       Assert.assertEquals("mvFile with wrong-2 srcFilePath should be false", Ret,-14010);
       tfsManager.rmFile(appId, userId, "/text1");
    }
	@Test
	public void test_13_mvFile_with_same_name()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/text1");
	   Ret=tfsManager.mvFile(appId, userId, "/text1","/text1");
       Assert.assertEquals("mvFile with the same name should be false", Ret,-14010);
       tfsManager.rmFile(appId, userId, "/text1");
    }
	@Test
	public void test_14_mvFile_dif_uid()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.mvFile(appId, 7, "/text1", "/text2");
       Assert.assertEquals("mvFile with different uid should be false", Ret,-14002);
	}

}
