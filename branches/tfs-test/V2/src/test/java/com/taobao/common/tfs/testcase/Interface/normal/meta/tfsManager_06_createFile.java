package com.taobao.common.tfs.testcase.Interface.normal.meta;


import org.junit.Test;
import org.junit.Ignore;

import com.taobao.common.tfs.testcase.metaTfsBaseCase;

import junit.framework.Assert;



public class tfsManager_06_createFile extends metaTfsBaseCase 
{
	@Test
	public void  test_01_createFile_right_filePath()
	{
	   int Ret ;
	   log.info( "test_01_createFile_right_filePath" );
	   Ret=tfsManager.createFile(appId, userId, "/textcreateFile");
	   Assert.assertEquals("Create File with right path should be true", Ret,0);
	   tfsManager.rmFile(appId, userId, "/textcreateFile");
	}
	@Test
	public void  test_02_createFile_null_filePath()
	{
	   int Ret ;
	   log.info( "test_02_createFile_null_filePath" );
	   Ret=tfsManager.createFile(appId, userId, null);
	   Assert.assertEquals("Create File with null path should be false", Ret,-14010);
	}
	@Test
	public void  test_03_createFile_empty_filePath()
	{
	   int Ret ;
	   log.info( "test_03_createFile_empty_filePath" );
	   Ret=tfsManager.createFile(appId, userId, "");
	   Assert.assertEquals("Create File with empty path should be false", Ret,-14010);
	}
	@Test
	public void  test_04_createFile_wrong_filePath_1()
	{
	   int Ret ;
	   log.info( "test_04_createFile_wrong_filePath_1" );
	   Ret=tfsManager.createFile(appId, userId, "/");
	   Assert.assertEquals("Create File with wrong 1 path should be false", Ret,-14010);
	}
	@Test
	public void  test_05_createFile_wrong_filePath_2()
	{
	   int Ret ;
	   log.info( "test_05_createFile_wrong_filePath_2" );
	   Ret=tfsManager.createFile(appId, userId, "textcreateFile");
	   Assert.assertEquals("Create File with wrong 2 path should be false",Ret, -14010);
	}
	@Test
	public void  test_06_createFile_wrong_filePath_3()
	{
	   int Ret ;
	   log.info( "test_06_createFile_wrong_filePath_3" );
	   Ret=tfsManager.createFile(appId, userId, "///text///");
	   Assert.assertEquals("Create File with wrong 3 path be true", Ret,0);
	   tfsManager.rmFile(appId, userId, "/text");
	}
	@Test
	public void  test_07_createFile_leap_filePath()
	{
	   int Ret ;
	   log.info( "test_07_createFile_leap_filePath" );
	   Ret=tfsManager.createFile(appId, userId, "/textcreateFile/textcreateFile");
	   Assert.assertEquals("Create File with leap path should be false", Ret,-14002);
	}
	@Test
	public void  test_08_createFile_same_File_name()
	{
	   int Ret ;
	   log.info( "test_08_createFile_same_File_name" );
	   Ret=tfsManager.createFile(appId, userId, "/textcreateFile");
	   Assert.assertEquals("Create File with right path should be true", Ret,0);
	   Ret=tfsManager.createFile(appId, userId, "/textcreateFile");
	   Assert.assertEquals("Create File two times should be false", Ret,-14001);
	   Ret=tfsManager.rmFile(appId, userId, "/textcreateFile");
	   Assert.assertEquals(Ret,0);
	}
	@Test
	public void  test_09_createFile_with_same_Dir()
	{
	   int Ret ;
	   log.info( "test_09_createFile_with_same_Dir" );
	   Ret=tfsManager.createDir(appId, userId, "/text1");
	   Ret=tfsManager.createFile(appId, userId, "/text1");
	   Assert.assertEquals("Create File with the same name to Dir should be true", Ret,0);
	   Ret=tfsManager.rmDir(appId, userId, "/text1");
	   Assert.assertEquals(Ret,0);
	   Ret=tfsManager.rmFile(appId, userId, "/text1");
	   Assert.assertEquals(Ret,0);
	}
}
