package com.taobao.common.tfs.testcase.Interface.normal.meta;


import org.junit.Test;
import org.junit.Ignore;

import com.taobao.common.tfs.testcase.metaTfsBaseCase;
import com.taobao.common.tfs.utility.FileUtility;

import junit.framework.Assert;




public class tfsManager_15_fetchFile extends metaTfsBaseCase 
{
	@Test
    public  void  test_01_fetchFile_right()
	{  
		log.info("test_01_fetchFile_right");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","/textfetchFile");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(FileUtility.getCrc(resourcesPath+"temp"),FileUtility.getCrc(resourcesPath+"100K.jpg"));
		tfsManager.rmFile(appId, userId, "/textfetchFile");
	}
	@Test
    public  void  test_02_fetchFile_null_localFile()
	{  
		log.info("test_02_fetchFile_null_localFile");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile(appId, userId, null,"/textfetchFile");
		Assert.assertFalse("Fetch File null path should be false", bRet);
		tfsManager.rmFile(appId, userId, "/textfetchFile");
	}
	@Test
    public  void  test_03_fetchFile_empty_localFile()
	{  
		log.info("test_03_fetchFile_empty_localFile");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile(appId, userId, "","/textfetchFile");
		Assert.assertFalse("Fetch File empty path should be false", bRet);
		tfsManager.rmFile(appId, userId, "/textfetchFile");
	}
	@Test
    public  void  test_04_fetchFile_wrong_localFile()
	{  
		log.info("test_04_fetchFile_wrong_localFile");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile(appId, userId, "hdsfhksdhf","/textfetchFile");
		Assert.assertTrue("Fetch File wrong path  be true", bRet);
		tfsManager.rmFile(appId, userId, "/textfetchFile");
	}
	@Test
    public  void  test_05_fetchFile_null_fileName()
	{  
		log.info("test_05_fetchFile_null_fileName");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
		Assert.assertTrue("Save File right path should be true", bRet);
        String fileName = null;
	    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp", fileName);
		Assert.assertFalse("Fetch File null fileName should be false", bRet);
		tfsManager.rmFile(appId, userId, "/textfetchFile");
	}
	@Test
    public  void  test_06_fetchFile_empty_fileName()
	{  
		log.info("test_06_fetchFile_empty_fileName");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","");
		Assert.assertFalse("Fetch File empty fileName should be false", bRet);
		tfsManager.rmFile(appId, userId, "/textfetchFile");
	}
	@Test
    public  void  test_07_fetchFile_wrong_fileName_1()
	{  
		log.info("test_07_fetchFile_wrong_fileName_1");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","textfetchFile");
		Assert.assertFalse("Fetch File wrong 1 fileName should be false", bRet);
		tfsManager.rmFile(appId, userId, "/textfetchFile");
	}
	@Test
    public  void  test_08_fetchFile_wrong_fileName_2()
	{  
		log.info("test_08_fetchFile_wrong_fileName_2");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","///textfetchFile///");
		Assert.assertTrue("Fetch File wrong 2 fileName be true", bRet);
		tfsManager.rmFile(appId, userId, "/textfetchFile");
	}
	@Test
    public  void  test_09_fetchFile_wrong_fileName_3()
	{  
		log.info("test_09_fetchFile_wrong_fileName_3");
		boolean bRet;
		//bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
		//Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","/");
	    //System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"+bRet+"$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		Assert.assertFalse("Fetch File wrong 3 fileName should be false", bRet);
		//tfsManager.rmFile(appId, userId, "/textfetchFile");
	}
	@Test
    public  void  test_10_fetchFile_not_exist_fileName()
	{  
		log.info("test_10_fetchFile_not_exist_fileName");
		boolean bRet;
	    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","/textfetchFile");
		Assert.assertFalse("Fetch File not exist should be false", bRet);
		tfsManager.rmFile(appId, userId, "/textfetchFile");
	}
	@Test
    public  void  test_11_fetchFile_with_Dir()
	{  
		log.info("test_11_fetchFile_with_Dir");
		boolean bRet;
		tfsManager.createDir(appId, userId, "/textfetchFile");
	    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","/textfetchFile");
		Assert.assertFalse("Fetch File with the Dir name should be false", bRet);
		tfsManager.rmDir(appId, userId, "/textfetchFile");
	}
	@Test
    public  void  test_12_fetchFile_large()
	{  
		log.info("test_12_fetchFile_large");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"1G.jpg","/textfetchFile");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","/textfetchFile");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(FileUtility.getCrc(resourcesPath+"temp"),FileUtility.getCrc(resourcesPath+"1G.jpg"));
		tfsManager.rmFile(appId, userId, "/textfetchFile");
	}
	
}
