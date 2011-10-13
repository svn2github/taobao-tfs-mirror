package com.taobao.common.tfs.itest;

import org.junit.Test;

import junit.framework.Assert;

import com.taobao.common.tfs.function.tfsNameBaseCase;


public class tfsManager_06_createFile extends tfsNameBaseCase 
{
	@Test
	public void  test_01_createFile_right_filePath()
	{
	   boolean bRet;
	   log.info( "test_01_createFile_right_filePath" );
	   bRet=tfsManager.createFile( userId, "/text");
	   Assert.assertTrue("Create File with right path should be true", bRet);
	   tfsManager.rmFile( userId, "/text");
	}
	@Test
	public void  test_02_createFile_null_filePath()
	{
	   boolean bRet;
	   log.info( "test_02_createFile_null_filePath" );
	   bRet=tfsManager.createFile( userId, null);
	   Assert.assertFalse("Create File with null path should be false", bRet);
	}
	@Test
	public void  test_03_createFile_empty_filePath()
	{
	   boolean bRet;
	   log.info( "test_03_createFile_empty_filePath" );
	   bRet=tfsManager.createFile( userId, "");
	   Assert.assertFalse("Create File with empty path should be false", bRet);
	}
	@Test
	public void  test_04_createFile_wrong_filePath_1()
	{
	   boolean bRet;
	   log.info( "test_04_createFile_wrong_filePath_1" );
	   bRet=tfsManager.createFile( userId, "/");
	   Assert.assertFalse("Create File with wrong 1 path should be false", bRet);
	}
	@Test
	public void  test_05_createFile_wrong_filePath_2()
	{
	   boolean bRet;
	   log.info( "test_05_createFile_wrong_filePath_2" );
	   bRet=tfsManager.createFile( userId, "text");
	   Assert.assertFalse("Create File with wrong 2 path should be false", bRet);
	}
	@Test
	public void  test_06_createFile_wrong_filePath_3()
	{
	   boolean bRet;
	   log.info( "test_06_createFile_wrong_filePath_3" );
	   bRet=tfsManager.createFile( userId, "///text///");
	   Assert.assertTrue("Create File with wrong 3 path be true", bRet);
	   tfsManager.rmFile( userId, "/text");
	}
	@Test
	public void  test_07_createFile_leap_filePath()
	{
	   boolean bRet;
	   log.info( "test_07_createFile_leap_filePath" );
	   bRet=tfsManager.createFile( userId, "/text/text");
	   Assert.assertFalse("Create File with leap path should be false", bRet);
	}
	@Test
	public void  test_08_createFile_same_File_name()
	{
	   boolean bRet;
	   log.info( "test_08_createFile_same_File_name" );
	   bRet=tfsManager.createFile( userId, "/text");
	   Assert.assertTrue("Create File with right path should be true", bRet);
	   bRet=tfsManager.createFile( userId, "/text");
	   Assert.assertFalse("Create File two times should be false", bRet);
	   tfsManager.rmFile( userId, "/text");
	}
	@Test
	public void  test_09_createFile_with_same_Dir()
	{
	   boolean bRet;
	   log.info( "test_09_createFile_with_same_Dir" );
	   bRet=tfsManager.createDir( userId, "/text");
	   bRet=tfsManager.createFile( userId, "/text");
	   //Assert.assertFalse("Create File with the same name to Dirshould be false", bRet);
	   tfsManager.rmDir( userId, "/text");
	   tfsManager.rmFile( userId, "/text");
	}
}