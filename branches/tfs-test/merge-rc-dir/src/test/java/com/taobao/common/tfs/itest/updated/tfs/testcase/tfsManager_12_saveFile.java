package com.taobao.common.tfs.itest.updated.tfs.testcase;


import org.junit.Test;

import junit.framework.Assert;

import com.taobao.common.tfs.itest.updated.tfs.tfsNameBaseCase;


public class tfsManager_12_saveFile extends tfsNameBaseCase 
{
	@Test
    public  void  test_01_saveFile_right() throws InterruptedException
	{  
		log.info("test_01_saveFile_right");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/text6");
		Assert.assertTrue("Save File right path should be true", bRet);
		Thread.sleep(10000);
	    //bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","/text2");
		//Assert.assertTrue("Fetch File right path should be true", bRet);
		//Assert.assertEquals(getCrc(resourcesPath+"temp"),getCrc(resourcesPath+"100K"));
		//tfsManager.rmFile(appId, userId, "/text");
	}	
	@Test
    public  void  test_02_saveFile_null_localFile()
	{  
		log.info("test_02_saveFile_null_localFile");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, null,"/text");
		Assert.assertFalse("Save File null path should be false", bRet);
	}
	@Test
    public  void  test_03_saveFile_empty_localFile()
	{  
		log.info("test_03_saveFile_empty_localFile");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, "","/text");
		Assert.assertFalse("Save File empty path should be false", bRet);
	}
	@Test
    public  void  test_04_saveFile_wrong_localFile()
	{  
		log.info("test_04_saveFile_wrong_localFile");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, "sdklafhksdh","/text");
		Assert.assertFalse("Save File wrong path should be false", bRet);
	}
	@Test
    public  void  test_05_saveFile_null_fileName()
	{  
		log.info("test_05_saveFile_null_fileName");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K",null);
		Assert.assertFalse("Save File null fileName should be false", bRet);	    
	}
	@Test
    public  void  test_06_saveFile_empty_fileName()
	{  
		log.info("test_06_saveFile_empty_fileName");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","");
		Assert.assertFalse("Save File null fileName should be false", bRet);	    
	}
	@Test
    public  void  test_07_saveFile_wrong_fileName_1()
	{  
		log.info("test_07_saveFile_wrong_fileName_1");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/text/");
		Assert.assertTrue("Save File wrong 1 fileName should be false", bRet);	
		tfsManager.rmFile(appId, userId, "/text");
	}
	@Test
    public  void  test_08_saveFile_wrong_fileName_2()
	{  
		log.info("test_08_saveFile_wrong_fileName_2");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","text");
		Assert.assertFalse("Save File wrong 2 fileName should be false", bRet);	    
	}
	@Test
    public  void  test_09_saveFile_wrong_fileName_3()
	{  
		log.info("test_09_saveFile_wrong_fileName_3");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/");
		Assert.assertFalse("Save File wrong 3 fileName should be false", bRet);	    
	}
	@Test
    public  void  test_10_saveFile_leap_fileName()
	{  
		log.info("test_10_saveFile_leap_fileName");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/text/text");
		Assert.assertFalse("Save File leap fileName should be false", bRet);	    
	}
	@Test
    public  void  test_11_saveFile_with_Dir()
	{  
		log.info("test_11_saveFile_with_Dir");
		boolean bRet;
		tfsManager.createDir(appId, userId, "/text");
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/text");
		Assert.assertFalse("Save File with the same name Dir should be false", bRet);
		tfsManager.rmDir(appId, userId, "/text");
	}
	@Test
    public  void  test_12_saveFile_large()
	{  
		log.info("test_12_saveFile_large");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"1G","/text");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","/text");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath+"temp"),getCrc(resourcesPath+"1G"));
		tfsManager.rmFile(appId, userId, "/text");
	}	
	@Test
    public  void  test_13_saveFile_many_times()
	{  
		log.info("test_13_saveFile_many_times");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/text");
		Assert.assertTrue("Save File right path should be true", bRet);
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/text");
		Assert.assertFalse("Save File two times should be false", bRet);
		tfsManager.rmFile(appId, userId, "/text");
	}
}