package com.taobao.common.tfs.RestfulTest;


import org.junit.Test;
import org.junit.Ignore;

import junit.framework.Assert;

import com.taobao.common.tfs.RestfulTfsBaseCase;
import com.taobao.common.tfs.TfsConstant;


public class RestfulTfsCreateFile extends RestfulTfsBaseCase 
{
	@Test
	public void  test_01_createFile_right_filePath()
	{
	   int Ret ;
	   String File_name = "textcreateFile";
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/"+File_name);
	   Assert.assertTrue(HeadObject(buecket_name,File_name,0));
	   Assert.assertEquals("Create File with right path should be true", Ret,TfsConstant.TFS_SUCCESS);
	   tfsManager.rmFile(appId, userId, "/"+File_name);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
	@Test
	public void  test_02_createFile_null_filePath()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, null);
	   Assert.assertEquals("Create File with null path should be false", Ret,TfsConstant.EXIT_INVALID_FILE_NAME);
	}
	@Test
	public void  test_03_createFile_empty_filePath()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "");
	   Assert.assertEquals("Create File with empty path should be false", Ret,TfsConstant.EXIT_INVALID_FILE_NAME);
	}
	@Test
	public void  test_04_createFile_wrong_filePath_1()
	{
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/");
	   Assert.assertEquals("Create File with wrong 1 path should be false", Ret,TfsConstant.EXIT_INVALID_FILE_NAME);
	}
	@Test
	public void  test_05_createFile_wrong_filePath_2()
	{
	   int Ret ;
	   String File_name = "textcreateFile";
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, File_name);
	   Assert.assertEquals("Create File with wrong 2 path should be false",Ret, TfsConstant.EXIT_INVALID_FILE_NAME);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
	@Test
	public void  test_06_createFile_wrong_filePath_3()
	{
	   int Ret ;
	   String File_name ="//textcreateFile///";
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/"+File_name);
	   Assert.assertTrue(HeadObject(buecket_name,"textcreateFile",0));
	   Assert.assertEquals("Create File with wrong 3 path be true", Ret,TfsConstant.TFS_SUCCESS);
	   tfsManager.rmFile(appId, userId, "/textcreateFile");
	   Assert.assertFalse(HeadObject(buecket_name,"textcreateFile"));
	}
	@Test
	public void  test_07_createFile_leap_filePath()
	{
	   int Ret ;
	   String File_name = "textcreateFile/textcreateFile";
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/"+File_name);
	   Assert.assertEquals("Create File with leap path should be false", Ret,TfsConstant.EXIT_PARENT_EXIST_ERROR);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
	@Test
	public void  test_08_createFile_same_File_name()
	{
	   int Ret ;
	   String File_name = "textcreateFile";
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createFile(appId, userId, "/"+File_name);
	   Assert.assertEquals("Create File with right path should be true", Ret,TfsConstant.TFS_SUCCESS);
	   Ret=tfsManager.createFile(appId, userId, "/"+File_name);
	   Assert.assertEquals("Create File two times should be false", Ret,TfsConstant.EXIT_TARGET_EXIST_ERROR);
	   Assert.assertTrue(HeadObject(buecket_name,File_name,0));
	   Ret=tfsManager.rmFile(appId, userId, "/"+File_name);
	   Assert.assertEquals(Ret,TfsConstant.TFS_SUCCESS);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
	@Test
	public void  test_09_createFile_with_same_Dir()
	{
	   int Ret ;
	   String File_name = "text1";
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.createDir(appId, userId, "/"+File_name);
	   Ret=tfsManager.createFile(appId, userId, "/"+File_name);
	   Assert.assertEquals("Create File with the same name to Dir should be true", Ret,TfsConstant.TFS_SUCCESS);
	   Assert.assertTrue(HeadObject(buecket_name,File_name,0));
	   Ret=tfsManager.rmDir(appId, userId, "/"+File_name);
	   Assert.assertEquals(Ret,TfsConstant.TFS_SUCCESS);
	   Ret=tfsManager.rmFile(appId, userId, "/"+File_name);
	   Assert.assertEquals(Ret,TfsConstant.TFS_SUCCESS);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
}
