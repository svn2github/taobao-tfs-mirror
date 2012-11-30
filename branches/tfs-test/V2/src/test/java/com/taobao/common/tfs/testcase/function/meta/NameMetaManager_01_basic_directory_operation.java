package com.taobao.common.tfs.testcase.function.meta;

import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import org.junit.Test;

import com.taobao.common.tfs.testcase.metaTfsBaseCase;
import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.namemeta.FileMetaInfo;

public class NameMetaManager_01_basic_directory_operation extends  metaTfsBaseCase
{
	
	public static List<FileMetaInfo>  fileInfoList;
	public static FileMetaInfo fileInfo;
	public static String localFile = resourcesPath+"100K.jpg";
    public static String rootDir = "/NameMetaTest1";

  @BeforeClass
  public  static void setUpOnce() throws Exception 
  {
    //rmDirRecursive(appId, userId, rootDir); 
	tfsManager.createDir(appId, userId, rootDir);
  }

  @AfterClass
  public static void tearDownOnce() throws Exception 
  {
    rmDirRecursive(appId, userId, rootDir); 
  }

	@Test
	public void test_00_ls_top_directory()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		int iRet = -1;
		int oldSize = 0;
		int newSize = 0;
		String filepath1 = rootDir + "/00test";

		fileInfoList = tfsManager.lsDir(appId, userId, rootDir);
		oldSize = fileInfoList.size();

		iRet = tfsManager.createDir(appId, userId, filepath1);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		fileInfoList = tfsManager.lsDir(appId, userId, rootDir);
		newSize = fileInfoList.size();
		Assert.assertEquals(1, newSize - oldSize); 
     
		iRet = tfsManager.rmDir(appId, userId, filepath1);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
	}
	
	@Test
	public void test_01_create_directory()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		int iRet = -1;
		String filepath1 = rootDir + "/01test";

		iRet = tfsManager.createDir(appId, userId, filepath1);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		String filepath2 = filepath1 + "/01test";
		iRet = tfsManager.createDir(appId, userId, filepath2);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		fileInfoList= tfsManager.lsDir(appId, userId, filepath1);
		Assert.assertFalse(fileInfoList.isEmpty()); 

		iRet = tfsManager.rmDir(appId, userId, filepath2);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		iRet = tfsManager.rmDir(appId, userId, filepath1);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
	}
	
	@Test
	public void test_02_remove_directory()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		int iRet = -1;
		String filepath1 = rootDir + "/02test";

		iRet = tfsManager.createDir(appId, userId, filepath1);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		String filepath2 = filepath1 + "/02test";
		iRet = tfsManager.createDir(appId, userId, filepath2);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);  

		fileInfoList= tfsManager.lsDir(appId, userId, filepath1);
		Assert.assertFalse(fileInfoList.isEmpty());

		fileInfo = fileInfoList.get(0);

		long  pid  = fileInfo.getPid();
		Assert.assertTrue(pid > 0);

		String dirname = fileInfo.getFileName();
		Assert.assertNotNull(dirname);

		fileInfoList= tfsManager.lsDir(appId, userId, filepath2);
		Assert.assertTrue(fileInfoList.isEmpty()); 

		iRet = tfsManager.rmDir(appId, userId, filepath2);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		iRet = tfsManager.rmDir(appId, userId, filepath1);
    	Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
	}

	@Test
	public void test_03_rename_directory()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		int iRet = -1;
		String filepath1 = rootDir + "/03test01";
		String filepath2 = rootDir + "/03test02";

		iRet = tfsManager.createDir(appId, userId, filepath1);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		iRet = tfsManager.mvDir(appId, userId, filepath1, filepath2);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		iRet = tfsManager.rmDir(appId, userId, filepath1);
		Assert.assertFalse(iRet==TfsConstant.TFS_SUCCESS);

		iRet = tfsManager.rmDir(appId, userId, filepath2);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
	}
	
	@Test
	public void test_04_move_directory()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		int iRet = -1;
		String filepath1 = rootDir + "/04test";

		iRet = tfsManager.createDir(appId, userId, filepath1);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		String filepath2 = filepath1 + "/04test";
		iRet = tfsManager.createDir(appId, userId, filepath2);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		String filepath3 = filepath2 + "/04test";
		iRet = tfsManager.createDir(appId, userId, filepath3);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		String filepath4 = filepath1 + "/04test01";
		iRet = tfsManager.mvDir(appId, userId, filepath3, filepath4);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		iRet = tfsManager.rmDir(appId, userId, filepath3);
		Assert.assertFalse(iRet==TfsConstant.TFS_SUCCESS);

		iRet = tfsManager.rmDir(appId, userId, filepath4);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		iRet = tfsManager.rmDir(appId, userId, filepath2);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		iRet = tfsManager.rmDir(appId, userId, filepath1);
    	Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
  }
	
	@Test
	public void test_05_save_file()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean ret = false;
		int iRet = -1;
		String filepath1 = rootDir + "/05test";
		String filepath2 = filepath1 + "/05test";
		String filepath3 = filepath2 + "/05test";

		iRet= tfsManager.createDir(appId, userId, filepath1);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		iRet= tfsManager.createDir(appId, userId, filepath2);
		Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		ret = tfsManager.saveFile(appId, userId, localFile, filepath3);
		Assert.assertTrue(ret);

	}
}
