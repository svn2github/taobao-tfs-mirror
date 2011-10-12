package com.taobao.common.tfs.testcaseForInterface;


import org.junit.Test;

import junit.framework.Assert;

import com.taobao.common.tfs.tfsNameBaseCase;


public class tfsManager_03_rmDir extends tfsNameBaseCase 
{
	@Test
	public void test_01_rmDir_right_filePath()
	{	   
	   boolean bRet;
	   log.info( "test_01_rmDir_right_filePath" );
	   tfsManager.createDir( userId, "/text");
	   bRet=tfsManager.rmDir( userId, "/text");
	   Assert.assertTrue("Remove Dir with right path should be true", bRet);
	}
	@Test
	public void test_02_rmDir_double_times()
	{	   
	   boolean bRet;
	   log.info( "test_02_rmDir_double_times" );
	   tfsManager.createDir( userId, "/text");
	   bRet=tfsManager.rmDir( userId, "/text");
	   Assert.assertTrue("Remove Dir with right path should be true", bRet);
	   bRet=tfsManager.rmDir( userId, "/text");
	   Assert.assertFalse("Remove Dir two times should be falae", bRet);
	}
	@Test
	public void test_03_rmDir_not_exist_filePath()
	{	   
	   boolean bRet;
	   log.info( "test_03_rmDir_not_exist_filePath" );
	   bRet=tfsManager.rmDir( userId, "/text");
	   Assert.assertFalse("Remove Dir not exist should be false", bRet);
	}
	@Test
	public void test_04_rmDir_null_filePath()
	{	   
	   boolean bRet;
	   log.info( "test_04_rmDir_null_filePath" );
	   bRet=tfsManager.rmDir( userId, null);
	   Assert.assertFalse("Remove Dir null should be falae", bRet);
	}
	@Test
	public void test_05_rmDir_empty_filePath()
	{	   
	   boolean bRet;
	   log.info( "test_05_rmDir_empty_filePath" );
	   bRet=tfsManager.rmDir( userId, "");
	   Assert.assertFalse("Remove Dir empty should be falae", bRet);
	}
	@Test
	public void test_06_rmDir_wrong_filePath_1()
	{	   
	   boolean bRet;
	   log.info( "test_06_rmDir_wrong_filePath_1" );
	   tfsManager.createDir( userId, "/text");
	   bRet=tfsManager.rmDir( userId, "text");
	   Assert.assertFalse("Remove wrong Dir should be falae", bRet);
	}
	@Test
	public void test_07_rmDir_wrong_filePath_2()
	{	   
	   boolean bRet;
	   log.info( "test_07_rmDir_wrong_filePath_2" );
	   bRet=tfsManager.rmDir( userId, "/");
	   Assert.assertFalse("Remove wrong Dir should be falae", bRet);
	}
	@Test
	public void test_08_rmDir_wrong_filePath_3()
	{	   
	   boolean bRet;
	   log.info( "test_08_rmDir_wrong_filePath_3" );
	   tfsManager.createDir( userId, "/text");
	   bRet=tfsManager.rmDir( userId, "////text/////");
	   Assert.assertTrue("Remove wrong Dir be true", bRet);
	}
	@Test
	public  void test_09_rmDir_filePath_with_File()
	{	   
	   boolean bRet;
	   log.info( "test_09_rmDir_filePath_with_File" );
	   tfsManager.createDir(userId, "/text");
	   tfsManager.createFile(userId, "/text/text");
	   bRet=tfsManager.rmDir(userId, "/text");
	   Assert.assertFalse("Remove Dir with File should be falae", bRet);
	   tfsManager.rmFile(userId, "/text/text");
	   tfsManager.rmDir(userId, "/text");
	}
	@Test
	public void test_10_rmDir_filePath_with_Dir()
	{	   
	   boolean bRet;
	   log.info( "test_10_rmDir_filePath_with_Dir" );
	   tfsManager.createDir( userId, "/text");
	   tfsManager.createDir( userId, "/text/text");
	   bRet=tfsManager.rmDir( userId, "/text");
	   Assert.assertFalse("Remove Dir with Dir should be falae", bRet);
	   tfsManager.rmDir( userId, "/text/text");
	   bRet=tfsManager.rmDir( userId, "/text");
	}
}