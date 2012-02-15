package com.taobao.common.tfs.itest;

import java.util.Random;

import org.junit.Test;

import junit.framework.Assert;

import com.taobao.common.tfs.tfsNameBaseCase;

public class tfsManager_08_rmFile extends tfsNameBaseCase 
{
	@Test
	public void test_01_rmFile_right_filePath()
	{	   
	   boolean bRet;
	   log.info( "test_01_rmFile_right_filePath" );
	   tfsManager.createFile(appId, userId, "/text");
	   bRet=tfsManager.rmFile(appId, userId, "/text");
	   Assert.assertTrue("Remove File with right path should be true", bRet);
	}
	@Test
	public void test_02_rmFile_double_times()
	{	   
	   boolean bRet;
	   log.info( "test_02_rmFile_double-times" );
	   tfsManager.createFile(appId, userId, "/text");
	   bRet=tfsManager.rmFile(appId, userId, "/text");
	   Assert.assertTrue("Remove File with right path should be true", bRet);
	   bRet=tfsManager.rmFile(appId, userId, "/text");
	   Assert.assertFalse("Remove File two times should be false", bRet);
	}
	@Test
	public void test_03_rmFile_not_exist()
	{	   
	   boolean bRet;
	   log.info( "test_03_rmFile_not_exist" );
	   bRet=tfsManager.rmFile(appId, userId, "/text");
	   Assert.assertFalse("Remove File not exist should be false", bRet);
	}
	@Test
	public void test_04_rmFile_null_filePath()
	{	   
	   boolean bRet;
	   log.info( "test_04_rmFile_null_filePath" );
	   bRet=tfsManager.rmFile(appId, userId, null);
	   Assert.assertFalse("Remove null File should be false", bRet);
	}
	@Test
	public void test_05_rmFile_empty_filePath()
	{	   
	   boolean bRet;
	   log.info( "test_05_rmFile_empty_filePath" );
	   bRet=tfsManager.rmFile(appId, userId, "");
	   Assert.assertFalse("Remove empty File should be false", bRet);
	}
	@Test
	public void test_06_rmFile_wrong_filePath_1()
	{	   
	   boolean bRet;
	   log.info( "test_06_rmFile_wrong_filePath_1" );
	   tfsManager.createFile(appId, userId, "/text");
	   bRet=tfsManager.rmFile(appId, userId, "text");
	   Assert.assertFalse("Remove wrong-1 File Path should be false", bRet);
	}
	@Test
	public void test_07_rmFile_wrong_filePath_2()
	{	   
	   boolean bRet;
	   log.info( "test_07_rmFile_wrong_filePath_2" );
	   tfsManager.createFile(appId, userId, "/text");
	   bRet=tfsManager.rmFile(appId, userId, "///text///");
	   Assert.assertTrue("Remove wrong-2 File Path be true", bRet);
	}
	@Test
	public  void test_08_rmFile_wrong_filePath_3()
	{	   
	   boolean bRet;
	   log.info( "test_08_rmFile_wrong_filePath_3" );
	   bRet=tfsManager.rmFile(appId, userId, "/");
	   Assert.assertFalse("Remove wrong-3 File Path should be false", bRet);
	}
	@Test
	public void test_09_rmFile_small_File()
	{	   
	   boolean bRet;
	   log.info( "test_01_rmFile_right_filePath" );
	   tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/text");
	   bRet=tfsManager.rmFile(appId, userId, "/text");
	   Assert.assertTrue("Remove File with right path should be true", bRet);
	}
	@Test
	public void test_10_rmFile_large_File()
	{	   
	   boolean bRet;
	   log.info( "test_10_rmFile_large_File" );
	   tfsManager.saveFile(appId, userId, resourcesPath+"1G","/text");
	   bRet=tfsManager.rmFile(appId, userId, "/text");
	   Assert.assertTrue("Remove File with right path should be true", bRet);
	}
	@Test
	public void test_11_rmFile_large_File_inanition()
	{	   
	   boolean bRet;
	   log.info( "test_10_rmFile_large_File" );
	   tfsManager.saveFile(appId, userId, resourcesPath+"3M","/text");
	   int len = 1024*1024*8;
       byte[] data = new byte[len];
       Random rd = new Random();
       rd.nextBytes(data);
       tfsManager.write(appId, userId,"/text", 8*(1<<20),data,0,8*(1<<20));
	   bRet=tfsManager.rmFile(appId, userId, "/text");
	   Assert.assertTrue("Remove File with right path should be true", bRet);
	}
}