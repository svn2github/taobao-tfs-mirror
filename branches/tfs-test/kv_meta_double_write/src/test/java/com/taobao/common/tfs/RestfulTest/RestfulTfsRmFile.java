package com.taobao.common.tfs.RestfulTest;

import java.util.Random;

import org.junit.Test;
import org.junit.Ignore;

import junit.framework.Assert;

import com.taobao.common.tfs.RestfulTfsBaseCase;
import com.taobao.common.tfs.TfsConstant;
;

public class RestfulTfsRmFile extends RestfulTfsBaseCase 
{
	@Test
	public void test_01_rmFile_right_filePath()
	{	   
	   int Ret ;
	   String File_name = "textrmFile";
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   tfsManager.createFile(appId, userId, "/"+File_name);
	   Assert.assertTrue(HeadObject(buecket_name,File_name,0));
	   
	   Ret=tfsManager.rmFile(appId, userId, "/"+File_name);
	   Assert.assertEquals("Remove File with right path should be true", Ret,TfsConstant.TFS_SUCCESS);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
	@Test
	public void test_02_rmFile_double_times()
	{	   
	   int Ret ;
	   String File_name = "textrmFile";
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   tfsManager.createFile(appId, userId, "/"+File_name);
	   Assert.assertTrue(HeadObject(buecket_name,File_name,0));
	   
	   Ret=tfsManager.rmFile(appId, userId, "/"+File_name);
	   Assert.assertEquals("Remove File with right path should be true", Ret,TfsConstant.TFS_SUCCESS);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
	   Ret=tfsManager.rmFile(appId, userId, "/"+File_name);
	   Assert.assertEquals("Remove File two times should be false", Ret,TfsConstant.EXIT_TARGET_EXIST_ERROR);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
	@Test
	public void test_03_rmFile_not_exist()
	{	   
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.rmFile(appId, userId, "/textrmFile");
	   Assert.assertEquals("Remove File not exist should be false", Ret,TfsConstant.EXIT_TARGET_EXIST_ERROR);
	}
	@Test
	public void test_04_rmFile_null_filePath()
	{	   
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.rmFile(appId, userId, null);
	   Assert.assertEquals("Remove null File should be false", Ret,TfsConstant.EXIT_INVALID_FILE_NAME);
	}
	@Test
	public void test_05_rmFile_empty_filePath()
	{	   
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.rmFile(appId, userId, "");
	   Assert.assertEquals("Remove empty File should be false", Ret,TfsConstant.EXIT_INVALID_FILE_NAME);
	}
	@Test
	public void test_06_rmFile_wrong_filePath_1()
	{	   
	   int Ret ;
	   String File_name = "textrmFile";
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   tfsManager.createFile(appId, userId, "/"+File_name);
	   Assert.assertTrue(HeadObject(buecket_name,File_name,0));
	   
	   Ret=tfsManager.rmFile(appId, userId, File_name);
	   Assert.assertEquals("Remove wrong-1 File Path should be false", Ret,TfsConstant.EXIT_INVALID_FILE_NAME);
	   Assert.assertTrue(HeadObject(buecket_name,File_name,0));
	   
	   Ret=tfsManager.rmFile(appId, userId, "/"+File_name);
	   Assert.assertEquals("Remove File Path be true", Ret,TfsConstant.TFS_SUCCESS);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
	@Test
	public void test_07_rmFile_wrong_filePath_2()
	{	   
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   tfsManager.createFile(appId, userId, "/textrmFile");
	   Ret=tfsManager.rmFile(appId, userId, "///textrmFile///");
	   Assert.assertEquals("Remove wrong-2 File Path be true", Ret,TfsConstant.TFS_SUCCESS);
	}
	@Test
	public  void test_08_rmFile_wrong_filePath_3()
	{	   
	   int Ret ;
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   Ret=tfsManager.rmFile(appId, userId, "/");
	   Assert.assertEquals("Remove wrong-3 File Path should be false", Ret,TfsConstant.EXIT_INVALID_FILE_NAME);
	}
	@Test
	public void test_09_rmFile_small_File()
	{	   
	   int Ret ;
	   String File_name = "textrmFile";
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/"+File_name);
	   Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<10)));
	   
	   Ret=tfsManager.rmFile(appId, userId, "/textrmFile");
	   Assert.assertEquals("Remove File with right path should be true", Ret,TfsConstant.TFS_SUCCESS);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
	@Test
	public void test_10_rmFile_large_File()
	{	   
	   int Ret ;
	   String File_name = "textrm100MFile";
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   tfsManager.saveFile(appId, userId, resourcesPath+"100M.jpg","/"+File_name);
	   Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<20)));
	   
	   Ret=tfsManager.rmFile(appId, userId, "/"+File_name);
	   Assert.assertEquals("Remove File with right path should be true", Ret,TfsConstant.TFS_SUCCESS);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
	@Test
	public void test_11_rmFile_large_File_inanition()
	{	   
	   int Ret ;
	   String File_name = "textrmFile";
	   log.info(new Throwable().getStackTrace()[0].getMethodName());
	   tfsManager.saveFile(appId, userId, resourcesPath+"3M.jpg","/"+File_name);
	   Assert.assertTrue(HeadObject(buecket_name,File_name,3*(1<<20)));
	   
	   int len = 1024*1024*8;
       byte[] data = new byte[len];
       Random rd = new Random();
       rd.nextBytes(data);
       tfsManager.write(appId, userId,"/"+File_name, 8*(1<<20),data,0,8*(1<<20));
       Assert.assertTrue(HeadObject(buecket_name,File_name,16*(1<<20)));
       
	   Ret=tfsManager.rmFile(appId, userId, "/"+File_name);
	   Assert.assertEquals("Remove File with right path should be true", Ret,TfsConstant.TFS_SUCCESS);
	   Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
}
