package com.taobao.common.tfs.itest;


import org.junit.Test;
import org.junit.Ignore;

import junit.framework.Assert;

import com.taobao.common.tfs.tfsNameBaseCase;


public class tfsManager_02_mvDir extends tfsNameBaseCase 
{
	@Test
	public void test_01_mvDir_right()
	{
	   boolean bRet;
	   log.info( "test_01_mvDir_right" );

	   bRet=tfsManager.createDir(appId, userId, "/textmvDir1rsrc");
	   bRet=tfsManager.mvDir(appId, userId, "/textmvDir1rsrc", "/textmvDir1rtar");
       Assert.assertTrue("mvDir with right path should be true", bRet);
       
       bRet = tfsManager.rmDir(appId, userId, "/textmvDir1rtar");
       Assert.assertTrue("mvDir with remove tarpath should be true", bRet);
	   bRet=tfsManager.rmDir(appId, userId, "/textmvDir1rsrc");
	   Assert.assertFalse("mvDir with remove srcpath should be false", bRet);
	}
	@Test
	public void test_02_mvDir_srcFilePath_with_File()
	{
	   boolean bRet;
	   log.info( "test_02_mvDir_srcFilePath_with_File" );

	   bRet=tfsManager.createDir(appId, userId, "/textmvDir2wfsrc");
       Assert.assertTrue(bRet);
	   bRet=tfsManager.createFile(appId, userId, "/textmvDir2wfsrc/text");
       Assert.assertTrue(bRet);

	   bRet=tfsManager.mvDir(appId, userId, "/textmvDir2wfsrc", "/textmvDir2wftar");
       Assert.assertTrue("mvDir with right path should be true", bRet);
       
       bRet = tfsManager.rmFile(appId, userId, "/textmvDir2wftar/text");
       Assert.assertTrue(bRet);

       bRet = tfsManager.rmDir(appId, userId, "/textmvDir2wftar");
       Assert.assertTrue(bRet);

	   bRet=tfsManager.rmDir(appId, userId, "/textmvDir2wfsrc");
	   Assert.assertFalse("mvDir with remove path should be false", bRet);
	}
	@Test
	public void test_03_mvDir_srcFilePath_with_File_and_Dir()
	{
	   boolean bRet;
	   log.info( "test_03_mvDir_srcFilePath_with_File_and_Dir" );
	   bRet=tfsManager.createDir(appId, userId, "/textmvDir1");
	   bRet=tfsManager.createFile(appId, userId, "/textmvDir1/textmvDir");
	   bRet=tfsManager.createDir(appId, userId, "/textmvDir1/textmvDir1");
	   bRet=tfsManager.mvDir(appId, userId, "/textmvDir1", "/textmvDir2");
       Assert.assertTrue("mvDir with right path should be true", bRet);
       
       tfsManager.rmDir(appId, userId, "/textmvDir2/textmvDir1");
       tfsManager.rmFile(appId, userId, "/textmvDir2/textmvDir");
       tfsManager.rmDir(appId, userId, "/textmvDir2");
	   bRet=tfsManager.rmDir(appId, userId, "/textmvDir1");
	   Assert.assertFalse("mvDir with remove path should be false", bRet);
	}
	@Test
	public void test_04_mvDir_exit_destFilePath()
	{
	   boolean bRet;
	   log.info( "test_04_mvDir_exit_destFilePath" );
	   bRet=tfsManager.createDir(appId, userId, "/textmvDir1");
	   bRet=tfsManager.createDir(appId, userId, "/textmvDir2");
	   bRet=tfsManager.mvDir(appId, userId, "/textmvDir1", "/textmvDir2");
       Assert.assertFalse("mvDir with created path should be false", bRet);
       
       tfsManager.rmDir(appId, userId, "/textmvDir2");
	   tfsManager.rmDir(appId, userId, "/textmvDir1");
	}
	@Test
	public void test_05_mvDir_empty_srcFilePath()
	{
	   boolean bRet;
	   log.info( "test_05_mvDir_empty_srcFilePath" );
	   bRet=tfsManager.mvDir(appId, userId, "", "/textmvDir2");
	   Assert.assertFalse("mvDir with empty srcFilePath should be false", bRet);
	}
	@Test
	public void test_06_mvDir_null_srcFilePath()
	{
	   boolean bRet;
	   log.info( "test_06_mvDir_null_srcFilePath" );
	   bRet=tfsManager.mvDir(appId, userId, null, "/textmvDir2");
	   Assert.assertFalse("mvDir with null srcFilePath should be false", bRet);
	}
	@Test
	public void test_07_mvDir_wrong_srcFilePath_1()
	{
	   boolean bRet;
	   log.info( "test_07_mvDir_wrong_srcFilePath_1" );
	   tfsManager.createDir(appId, userId, "/textmvDir1");
	   bRet=tfsManager.mvDir(appId, userId, "textmvDir1", "/textmvDir2");
	   Assert.assertFalse("mvDir with wrong1 srcFilePath should be false", bRet);
	   tfsManager.rmFile(appId, userId, "/textmvDir1");
	}
	@Test
	public void test_08_mvDir_wrong_srcFilePath_2()
	{
	   boolean bRet;
	   log.info( "test_08_mvDir_wrong_srcFilePath_2" );
	   bRet=tfsManager.mvDir(appId, userId, "/", "/textmvDir2");
	   Assert.assertFalse("mvDir with wrong1 srcFilePath should be false", bRet);
	}
	@Test
	public void test_09_mvDir_empty_destFilePath()
	{
	   boolean bRet;
	   log.info( "test_09_mvDir_empty_destFilePath" );
	   bRet=tfsManager.createDir(appId, userId, "/textmvDir1");
	   bRet=tfsManager.mvDir(appId, userId, "/textmvDir1", "");
	   Assert.assertFalse("mvDir with empty destFilePath should be false", bRet);
       tfsManager.rmDir(appId, userId, "/textmvDir1");
	}
	@Test
	public void test_10_mvDir_null_destFilePath()
	{
	   boolean bRet;
	   log.info( "test_10_mvDir_null_destFilePath" );
	   bRet=tfsManager.createDir(appId, userId, "/textmvDir1");
	   bRet=tfsManager.mvDir(appId, userId, "/textmvDir1", null);
	   Assert.assertFalse("mvDir with null destFilePath should be false", bRet);
       tfsManager.rmDir(appId, userId, "/textmvDir1");
	}
	@Test
	public void test_11_mvDir_wrong_destFilePath_1()
	{
	   boolean bRet;
	   log.info( "test_11_mvDir_wrong_destFilePath_1" );
	   bRet=tfsManager.createDir(appId, userId, "/textmvDir1");
	   bRet=tfsManager.mvDir(appId, userId, "/textmvDir1", "/");
	   Assert.assertFalse("mvDir with wrong 1 destFilePath should be false", bRet);
       tfsManager.rmDir(appId, userId, "/textmvDir1");
	}
	@Test
	public void test_12_mvDir_wrong_destFilePath_2()
	{
	   boolean bRet;
	   log.info( "test_12_mvDir_wrong_destFilePath_2" );
	   bRet=tfsManager.createDir(appId, userId, "/textmvDir1");
	   bRet=tfsManager.mvDir(appId, userId, "/textmvDir1", "textmvDir2");
	   Assert.assertFalse("mvDir with wrong 2 destFilePath should be false", bRet);
       tfsManager.rmDir(appId, userId, "/textmvDir1");
	}
	@Test
	public void test_13_mvDir_wrong_destFilePath_3()
	{
	   boolean bRet;
	   log.info( "test_13_mvDir_wrong_destFilePath_3" );
	   bRet=tfsManager.createDir(appId, userId, "/textmvDir1");
	   bRet=tfsManager.mvDir(appId, userId, "/textmvDir1", "///textmvDir2///");
	   Assert.assertTrue("mvDir with wrong 3 destFilePath be true", bRet);
       tfsManager.rmDir(appId, userId, "/textmvDir2");
	}
	@Test
	public void test_14_mvDir_with_leap_destFilePath()
	{
	   boolean bRet;
	   log.info( "test_14_mvDir_with_leap_destFilePath" );
	   bRet=tfsManager.createDir(appId, userId, "/textmvDir1");
	   bRet=tfsManager.mvDir(appId, userId, "/textmvDir1", "/textmvDir2/textmvDir");
	   Assert.assertFalse("mvDir with leap path should be false", bRet);
       tfsManager.rmDir(appId, userId, "/textmvDir1");
	}
	@Test
	public void test_15_mvDir_with_same_File_destFilePath()
	{
	   boolean bRet;
	   log.info( "test_15_mvDir_with_same_File_destFilePath" );
	   bRet=tfsManager.createDir(appId, userId, "/textmvDir1");
	   bRet=tfsManager.createFile(appId, userId, "/textmvDir2");
	   bRet=tfsManager.mvDir(appId, userId, "/textmvDir1", "/textmvDir2");
	   //Assert.assertFalse("mvDir with the same path with File name should be false", bRet);
       tfsManager.rmDir(appId, userId, "/textmvDir1");
       tfsManager.rmFile(appId, userId, "/textmvDir2");
	}
	@Test
	public void test_16_mvDir_srcFilePath_same_destFilePath()
	{
	   boolean bRet;
	   log.info( "test_16_mvDir_srcFilePath_same_destFilePath" );
	   bRet=tfsManager.createDir(appId, userId, "/textmvDir1");
	   bRet=tfsManager.mvDir(appId, userId, "/textmvDir1", "/textmvDir1");
	   Assert.assertFalse("mvDir with the same path should be false", bRet);
       tfsManager.rmDir(appId, userId, "/textmvDir1");
	}
	@Test
	public void test_17_mvDir_complex()
	{
	   boolean bRet;
	   log.info( "test_17_mvDir_complex" );
	   tfsManager.createDir(appId, userId, "/textmvDir1");
	   tfsManager.createDir(appId, userId, "/textmvDir1/textmvDir1");
	   tfsManager.createFile(appId, userId, "/textmvDir1/textmvDir2");
	   tfsManager.createDir(appId, userId, "/textmvDir1/textmvDir1/textmvDir1");
	   tfsManager.createFile(appId, userId, "/textmvDir1/textmvDir1/textmvDir2");

	   tfsManager.createDir(appId, userId, "/textmvDir2");
	   tfsManager.createDir(appId, userId, "/textmvDir2/textmvDir2");
	   tfsManager.createFile(appId, userId, "/textmvDir2/textmvDir3");

       bRet=tfsManager.mvDir(appId, userId, "/textmvDir1/textmvDir1", "/textmvDir2/textmvDir4");
       Assert.assertTrue("mvDir with right path should be true", bRet);
       
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
	   boolean bRet;
	   log.info( "test_18_mvDir_with_dif_uid" );
	   bRet=tfsManager.mvDir(appId, userId, "/textmvDir1", "/textmvDir2");
       Assert.assertFalse("mvDir with different uid should be false", bRet);
   }
	@Test
	public void test_19_mvDir_circle()
	{
	   boolean bRet;
	   log.info( "test_19_mvDir_circle" );
	   bRet=tfsManager.createDir(appId, userId, "/textmvDir1");
	   bRet=tfsManager.createDir(appId, userId, "/textmvDir1/textmvDir2");
	   bRet=tfsManager.createDir(appId, userId, "/textmvDir1/textmvDir2/textmvDir3");
	   
	   bRet=tfsManager.mvDir(appId, userId, "/textmvDir1", "/textmvDir1/textmvDir2/textmvDir3/textmvDir4");
       Assert.assertFalse("mvDir with circle path should be false", bRet);
       
       tfsManager.rmDir(appId, userId, "/textmvDir1/textmvDir2/textmvDir3");
       tfsManager.rmDir(appId, userId, "/textmvDir1/textmvDir2");
       tfsManager.rmDir(appId, userId, "/textmvDir1");
    }
    
}
