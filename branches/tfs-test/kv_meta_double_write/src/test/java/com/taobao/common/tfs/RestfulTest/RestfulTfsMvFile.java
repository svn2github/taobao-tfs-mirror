package com.taobao.common.tfs.RestfulTest;

import org.junit.Test;
import org.junit.Ignore;

import junit.framework.Assert;

import com.taobao.common.tfs.RestfulTfsBaseCase;
import com.taobao.common.tfs.TfsConstant;


public class RestfulTfsMvFile extends RestfulTfsBaseCase 
{
	@Test
	public void test_01_mvFile_right()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/textmvFile1src");
	   
       Assert.assertEquals(Ret,TfsConstant.TFS_SUCCESS);
	   Ret=tfsManager.mvFile(appId, userId, "/textmvFile1src", "/textmvFile1tar");	   
       Assert.assertEquals("mvFile with right path should be true", Ret,TfsConstant.TFS_SUCCESS);
;
	   
       tfsManager.rmFile(appId, userId, "/textmvFile1tar");
	   Ret=tfsManager.rmFile(appId, userId, "/textmvFile1src");
	   Assert.assertEquals("mvFile with remove path should be false", Ret,TfsConstant.EXIT_TARGET_EXIST_ERROR);
	   
	}
	@Test
	public void test_02_mvFile_null_destFilePath()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/textmvFile1src");
       Assert.assertEquals(Ret,TfsConstant.TFS_SUCCESS);
	   Ret=tfsManager.mvFile(appId, userId, "/textmvFile1src",null);
       Assert.assertEquals("mvFile with null destFilePath should be false", Ret,TfsConstant.EXIT_INVALID_FILE_NAME);
       
       Ret=tfsManager.rmFile(appId, userId, "/textmvFile1src");
       Assert.assertEquals(Ret,TfsConstant.TFS_SUCCESS);

	}
	@Test
	public void test_03_mvFile_empty_destFilePath()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/text1");
	   Ret=tfsManager.mvFile(appId, userId, "/text1","");
       Assert.assertEquals("mvFile with empty destFilePath should be false", Ret,TfsConstant.EXIT_INVALID_FILE_NAME);

       
       Ret=tfsManager.rmFile(appId, userId, "/text1");

	}
	@Test
	public void test_04_mvFile_wrong_destFilePath_1()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/text1");
	   Ret=tfsManager.mvFile(appId, userId, "/text1","/");

	   
       Assert.assertEquals("mvFile with wrong_1 destFilePath should be false", Ret,TfsConstant.EXIT_INVALID_FILE_NAME);
       Ret=tfsManager.rmFile(appId, userId, "/text1");

	}
	@Test
	public void test_05_mvFile_wrong_destFilePath_2()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/text1");
	   Ret=tfsManager.mvFile(appId, userId, "/text1","text2");
       Assert.assertEquals("mvFile with wrong_2 destFilePath should be false", Ret,TfsConstant.EXIT_INVALID_FILE_NAME);

       
       Ret=tfsManager.rmFile(appId, userId, "/text1");

	}
	@Test
	public void test_06_mvFile_wrong_destFilePath_3()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/textmvFile6src");
	   Ret=tfsManager.mvFile(appId, userId, "/textmvFile6src","///textmvFile6tar///");
       Assert.assertEquals("mvFile with wrong_3 destFilePath be true", Ret,TfsConstant.TFS_SUCCESS);
       Ret=tfsManager.rmFile(appId, userId, "/textmvFile6tar");
	}
	@Test
	public void test_07_mvFile_leap_Filename()
	{
		   int Ret ;
		   log.info(new Throwable().getStackTrace()[0].getMethodName());
		   Ret=tfsManager.rmFile(appId, userId, "/text2");
		   Ret=tfsManager.createFile(appId, userId, "/text1");
		   
		   Ret=tfsManager.mvFile(appId, userId, "/text1","/text2/text");
	       Assert.assertEquals("mvFile with leap destFilePath should be true", Ret,TfsConstant.TFS_SUCCESS);
;
	       
	       tfsManager.rmFile(appId, userId, "/text2/text");

	       tfsManager.rmDir(appId, userId, "/text2");
	}
	@Test
	public void test_08_mvFile_exist_Filename()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/text1");
	   Ret=tfsManager.createFile(appId, userId, "/text2");
	   Ret=tfsManager.mvFile(appId, userId, "/text1","/text2");
       Assert.assertEquals("mvFile with exist destFilePath should be false", Ret,TfsConstant.EXIT_TARGET_EXIST_ERROR);
       
       
       Ret=tfsManager.rmFile(appId, userId, "/text1");
       Ret=tfsManager.rmFile(appId, userId, "/text2");
	}
	@Test
	public void test_09_mvFile_null_srcFilePath()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.mvFile(appId, userId, null,"/text2");
       Assert.assertEquals("mvFile with null srcFilePath should be false", Ret,TfsConstant.EXIT_INVALID_FILE_NAME);
    }
	@Test
	public void test_10_mvFile_empty_srcFilePath()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.mvFile(appId, userId, "","/text2");
       Assert.assertEquals("mvFile with empty srcFilePath should be false", Ret,TfsConstant.EXIT_INVALID_FILE_NAME);
    }
	@Test
	public void test_11_mvFile_wrong_srcFilePath_1()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.mvFile(appId, userId, "/","/text2");
       Assert.assertEquals("mvFile with wrong-1 srcFilePath should be false", Ret,TfsConstant.EXIT_TFS_INTERNAL_ERROR);
    }
	@Test
	public void test_12_mvFile_wrong_srcFilePath_2()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/text1");
	   Ret=tfsManager.mvFile(appId, userId, "text1","/text2");
       Assert.assertEquals("mvFile with wrong-2 srcFilePath should be false", Ret,TfsConstant.EXIT_INVALID_FILE_NAME);
       
       tfsManager.rmFile(appId, userId, "/text1");
    }
	@Test
	public void test_13_mvFile_with_same_name()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/text1");
	   Ret=tfsManager.mvFile(appId, userId, "/text1","/text1");
       Assert.assertEquals("mvFile with the same name should be false", Ret,TfsConstant.EXIT_INVALID_ARGU_ERROR);
       
       tfsManager.rmFile(appId, userId, "/text1");
    }
	
	@Test
	public void test_14_mvFile_dif_uid()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.mvFile(appId, 7, "/text1", "/text2");
       Assert.assertEquals("mvFile with different uid should be false", Ret,TfsConstant.EXIT_INVALID_FILE_NAME);
	}

}
