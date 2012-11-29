package com.taobao.common.tfs.testcase.Interface.normal.meta;


import org.junit.Test;
import org.junit.Ignore;

import com.taobao.common.tfs.testcase.metaTfsBaseCase;

import junit.framework.Assert;




public class tfsManager_02_mvDir extends metaTfsBaseCase 
{
	@Test
	public void test_01_mvDir_right()
	{
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());

	   Ret=tfsManager.createDir(appId, userId, "/textmvDir1rsrc");
	   Ret=tfsManager.mvDir(appId, userId, "/textmvDir1rsrc", "/textmvDir1rtar");
       Assert.assertEquals("mvDir with right path should be true", Ret,0);
       
       Ret = tfsManager.rmDir(appId, userId, "/textmvDir1rtar");
       Assert.assertEquals("mvDir with remove tarpath should be true", Ret,0);
	   Ret=tfsManager.rmDir(appId, userId, "/textmvDir1rsrc");
	   Assert.assertEquals("mvDir with remove srcpath should be false", Ret,-14001);
	}
	@Test
	public void test_02_mvDir_srcFilePath_with_File()
	{
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());

	   Ret=tfsManager.createDir(appId, userId, "/textmvDir2wfsrc");
       Assert.assertEquals(Ret,0);
	   Ret=tfsManager.createFile(appId, userId, "/textmvDir2wfsrc/text");
       Assert.assertEquals(Ret,0);

	   Ret=tfsManager.mvDir(appId, userId, "/textmvDir2wfsrc", "/textmvDir2wftar");
       Assert.assertEquals("mvDir with right path should be true", Ret,0);
       
       Ret = tfsManager.rmFile(appId, userId, "/textmvDir2wftar/text");
       Assert.assertEquals(Ret,0);

       Ret = tfsManager.rmDir(appId, userId, "/textmvDir2wftar");
       Assert.assertEquals(Ret,0);

	   Ret=tfsManager.rmDir(appId, userId, "/textmvDir2wfsrc");
	   Assert.assertEquals("mvDir with remove path should be false", Ret,-14001);
	}
	@Test
	public void test_03_mvDir_srcFilePath_with_File_and_Dir()
	{
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createDir(appId, userId, "/textmvDir1");
	   Ret=tfsManager.createFile(appId, userId, "/textmvDir1/textmvDir");
	   Ret=tfsManager.createDir(appId, userId, "/textmvDir1/textmvDir1");
	   Ret=tfsManager.mvDir(appId, userId, "/textmvDir1", "/textmvDir2");
       Assert.assertEquals("mvDir with right path should be true", Ret,0);
       
       tfsManager.rmDir(appId, userId, "/textmvDir2/textmvDir1");
       tfsManager.rmFile(appId, userId, "/textmvDir2/textmvDir");
       tfsManager.rmDir(appId, userId, "/textmvDir2");
	   Ret=tfsManager.rmDir(appId, userId, "/textmvDir1");
	   Assert.assertEquals("mvDir with remove path should be false", Ret,-14001);
	}
	@Test
	public void test_04_mvDir_exit_destFilePath()
	{
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createDir(appId, userId, "/textmvDir1");
	   Ret=tfsManager.createDir(appId, userId, "/textmvDir2");
	   Ret=tfsManager.mvDir(appId, userId, "/textmvDir1", "/textmvDir2");
       Assert.assertEquals("mvDir with created path should be false", Ret,-14001);
       
       tfsManager.rmDir(appId, userId, "/textmvDir2");
	   tfsManager.rmDir(appId, userId, "/textmvDir1");
	}
	@Test
	public void test_05_mvDir_empty_srcFilePath()
	{
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.mvDir(appId, userId, "", "/textmvDir2");
	   Assert.assertEquals("mvDir with empty srcFilePath should be false", Ret,-14010);
	}
	@Test
	public void test_06_mvDir_null_srcFilePath()
	{
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.mvDir(appId, userId, null, "/textmvDir2");
	   Assert.assertEquals("mvDir with null srcFilePath should be false", Ret,-14010);
	}
	@Test
	public void test_07_mvDir_wrong_srcFilePath_1()
	{
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   tfsManager.createDir(appId, userId, "/textmvDir1");
	   Ret=tfsManager.mvDir(appId, userId, "textmvDir1", "/textmvDir2");
	   Assert.assertEquals("mvDir with wrong1 srcFilePath should be false", Ret,-14010);
	   tfsManager.rmDir(appId, userId, "/textmvDir1");
	}
	@Test
	public void test_08_mvDir_wrong_srcFilePath_2()
	{
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.mvDir(appId, userId, "/", "/textmvDir2");
	   Assert.assertEquals("mvDir with wrong1 srcFilePath should be false", Ret,-14011);
	}
	@Test
	public void test_09_mvDir_empty_destFilePath()
	{
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createDir(appId, userId, "/textmvDir1");
	   Ret=tfsManager.mvDir(appId, userId, "/textmvDir1", "");
	   Assert.assertEquals("mvDir with empty destFilePath should be false", Ret,-14010);
       tfsManager.rmDir(appId, userId, "/textmvDir1");
	}
	@Test
	public void test_10_mvDir_null_destFilePath()
	{
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createDir(appId, userId, "/textmvDir1");
	   Ret=tfsManager.mvDir(appId, userId, "/textmvDir1", null);
	   Assert.assertEquals("mvDir with null destFilePath should be false", Ret,-14010);
       tfsManager.rmDir(appId, userId, "/textmvDir1");
	}
	@Test
	public void test_11_mvDir_wrong_destFilePath_1()
	{
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createDir(appId, userId, "/textmvDir1");
	   Ret=tfsManager.mvDir(appId, userId, "/textmvDir1", "/");
	   Assert.assertEquals("mvDir with wrong destFilePath should be false", Ret,-14017);
       tfsManager.rmDir(appId, userId, "/textmvDir1");
	}
	@Test
	public void test_12_mvDir_wrong_destFilePath_2()
	{
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createDir(appId, userId, "/textmvDir1");
	   Ret=tfsManager.mvDir(appId, userId, "/textmvDir1", "textmvDir2");
	   Assert.assertEquals("mvDir with wrong 2 destFilePath should be false", Ret,-14010);
       tfsManager.rmDir(appId, userId, "/textmvDir1");
	}
	@Test
	public void test_13_mvDir_wrong_destFilePath_3()
	{
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createDir(appId, userId, "/textmvDir1");
	   Ret=tfsManager.mvDir(appId, userId, "/textmvDir1", "///textmvDir2///");
	   Assert.assertEquals("mvDir with wrong 3 destFilePath be true", Ret,0);
       tfsManager.rmDir(appId, userId, "/textmvDir2");
	}
	@Test
	public void test_14_mvDir_with_leap_destFilePath()
	{
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createDir(appId, userId, "/textmvDir1");
	   Ret=tfsManager.mvDir(appId, userId, "/textmvDir1", "/textmvDir2/textmvDir");
	   Assert.assertEquals("mvDir with leap path should be false", Ret,-14002);
       tfsManager.rmDir(appId, userId, "/textmvDir1");
	}
	@Test
	public void test_15_mvDir_with_same_File_destFilePath()
	{
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createDir(appId, userId, "/textmvDir1");
	   Ret=tfsManager.createFile(appId, userId, "/textmvDir2");
	   Ret=tfsManager.mvDir(appId, userId, "/textmvDir1", "/textmvDir2");
	   Assert.assertEquals("mvDir with the same path with File name should be false", Ret,-14001);
       tfsManager.rmDir(appId, userId, "/textmvDir1");
       tfsManager.rmFile(appId, userId, "/textmvDir2");
	}
	@Test
	public void test_16_mvDir_srcFilePath_same_destFilePath()
	{
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createDir(appId, userId, "/textmvDir1");
	   Ret=tfsManager.mvDir(appId, userId, "/textmvDir1", "/textmvDir1");
	   Assert.assertEquals("mvDir with the same path should be false", Ret,-14010);
	   System.out.println("@@@@@@@@@@@@@@@@@+"+Ret);
       tfsManager.rmDir(appId, userId, "/textmvDir1");
	}
	@Test
	public void test_17_mvDir_complex()
	{
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   tfsManager.createDir(appId, userId, "/textmvDir1");
	   tfsManager.createDir(appId, userId, "/textmvDir1/textmvDir1");
	   tfsManager.createFile(appId, userId, "/textmvDir1/textmvDir2");
	   tfsManager.createDir(appId, userId, "/textmvDir1/textmvDir1/textmvDir1");
	   tfsManager.createFile(appId, userId, "/textmvDir1/textmvDir1/textmvDir2");

	   tfsManager.createDir(appId, userId, "/textmvDir2");
	   tfsManager.createDir(appId, userId, "/textmvDir2/textmvDir2");
	   tfsManager.createFile(appId, userId, "/textmvDir2/textmvDir3");

       Ret=tfsManager.mvDir(appId, userId, "/textmvDir1/textmvDir1", "/textmvDir2/textmvDir4");
       Assert.assertEquals("mvDir with right path should be true", Ret,0);
       
       tfsManager.rmFile(appId, userId, "/textmvDir1/textmvDir2");
       tfsManager.rmDir(appId, userId, "/textmvDir1");
       tfsManager.rmFile(appId, userId, "/textmvDir2/textmvDir4/textmvDir2");
       tfsManager.rmDir(appId, userId, "/textmvDir2/textmvDir4/textmvDir1");
       tfsManager.rmDir(appId, userId, "/textmvDir2/textmvDir4");
       tfsManager.rmDir(appId, userId, "/textmvDir2/textmvDir2");
       tfsManager.rmFile(appId, userId, "/textmvDir2/textmvDir3");
       tfsManager.rmDir(appId, userId, "/textmvDir2");
   }
	@Test
	public void test_18_mvDir_with_dif_uid()
   {
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.mvDir(appId, 7, "/textmvDir1", "/textmvDir2");
       Assert.assertEquals("mvDir with different uid should be false", Ret,-14002);
   }
	@Test
	public void test_19_mvDir_circle()
	{
	   int Ret;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createDir(appId, userId, "/textmvDir1");
	   Ret=tfsManager.createDir(appId, userId, "/textmvDir1/textmvDir2");
	   Ret=tfsManager.createDir(appId, userId, "/textmvDir1/textmvDir2/textmvDir3");
	   
	   Ret=tfsManager.mvDir(appId, userId, "/textmvDir1", "/textmvDir1/textmvDir2/textmvDir3/textmvDir4");
       Assert.assertEquals("mvDir with circle path should be false", Ret,-14010);
       
       tfsManager.rmDir(appId, userId, "/textmvDir1/textmvDir2/textmvDir3");
       tfsManager.rmDir(appId, userId, "/textmvDir1/textmvDir2");
       tfsManager.rmDir(appId, userId, "/textmvDir1");
    }
    
}
