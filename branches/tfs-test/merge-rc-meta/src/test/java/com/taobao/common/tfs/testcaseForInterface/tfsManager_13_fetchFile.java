package com.taobao.common.tfs.testcaseForInterface;


import org.junit.Test;

import junit.framework.Assert;

import com.taobao.common.tfs.tfsNameBaseCase;


public class tfsManager_13_fetchFile extends tfsNameBaseCase 
{
	@Test
    public  void  test_01_fetchFile_right()
	{  
		log.info("test_01_fetchFile_right");
		boolean bRet;
		bRet=tfsManager.saveFile( userId, resourcesPath+"100K","/text");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile( userId, resourcesPath+"temp","/text");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath+"temp"),getCrc(resourcesPath+"100K"));
		tfsManager.rmFile( userId, "/text");
	}
	@Test
    public  void  test_02_fetchFile_null_localFile()
	{  
		log.info("test_02_fetchFile_null_localFile");
		boolean bRet;
		bRet=tfsManager.saveFile( userId, resourcesPath+"100K","/text");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile( userId, null,"/text");
		Assert.assertFalse("Fetch File null path should be false", bRet);
		tfsManager.rmFile( userId, "/text");
	}
	@Test
    public  void  test_03_fetchFile_empty_localFile()
	{  
		log.info("test_03_fetchFile_empty_localFile");
		boolean bRet;
		bRet=tfsManager.saveFile( userId, resourcesPath+"100K","/text");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile( userId, "","/text");
		Assert.assertFalse("Fetch File empty path should be false", bRet);
		tfsManager.rmFile( userId, "/text");
	}
	@Test
    public  void  test_04_fetchFile_wrong_localFile()
	{  
		log.info("test_04_fetchFile_wrong_localFile");
		boolean bRet;
		bRet=tfsManager.saveFile( userId, resourcesPath+"100K","/text");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile( userId, "hdsfhksdhf","/text");
		Assert.assertTrue("Fetch File wrong path  be true", bRet);
		tfsManager.rmFile( userId, "/text");
	}
	@Test
    public  void  test_05_fetchFile_null_fileName()
	{  
		log.info("test_05_fetchFile_null_fileName");
		boolean bRet;
		bRet=tfsManager.saveFile( userId, resourcesPath+"100K","/text");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile( userId, resourcesPath+"temp",null);
		Assert.assertFalse("Fetch File null fileName should be false", bRet);
		tfsManager.rmFile( userId, "/text");
	}
	@Test
    public  void  test_06_fetchFile_empty_fileName()
	{  
		log.info("test_06_fetchFile_empty_fileName");
		boolean bRet;
		bRet=tfsManager.saveFile( userId, resourcesPath+"100K","/text");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile( userId, resourcesPath+"temp","");
		Assert.assertFalse("Fetch File empty fileName should be false", bRet);
		tfsManager.rmFile( userId, "/text");
	}
	@Test
    public  void  test_07_fetchFile_wrong_fileName_1()
	{  
		log.info("test_07_fetchFile_wrong_fileName_1");
		boolean bRet;
		bRet=tfsManager.saveFile( userId, resourcesPath+"100K","/text");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile( userId, resourcesPath+"temp","text");
		Assert.assertFalse("Fetch File wrong 1 fileName should be false", bRet);
		tfsManager.rmFile( userId, "/text");
	}
	@Test
    public  void  test_08_fetchFile_wrong_fileName_2()
	{  
		log.info("test_08_fetchFile_wrong_fileName_2");
		boolean bRet;
		bRet=tfsManager.saveFile( userId, resourcesPath+"100K","/text");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile( userId, resourcesPath+"temp","///text///");
		Assert.assertTrue("Fetch File wrong 2 fileName be true", bRet);
		tfsManager.rmFile( userId, "/text");
	}
	@Test
    public  void  test_09_fetchFile_wrong_fileName_3()
	{  
		log.info("test_09_fetchFile_wrong_fileName_3");
		boolean bRet;
		bRet=tfsManager.saveFile( userId, resourcesPath+"100K","/text");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile( userId, resourcesPath+"temp","/");
	    //System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"+bRet+"$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		Assert.assertFalse("Fetch File wrong 3 fileName should be false", bRet);
		tfsManager.rmFile( userId, "/text");
	}
	@Test
    public  void  test_10_fetchFile_not_exist_fileName()
	{  
		log.info("test_10_fetchFile_not_exist_fileName");
		boolean bRet;
	    bRet=tfsManager.fetchFile( userId, resourcesPath+"temp","/text");
		Assert.assertFalse("Fetch File not exist should be false", bRet);
		//	Assert.assertTrue("Fetch File not exist should be false", bRet);
		tfsManager.rmFile( userId, "/text");
	}
	@Test
    public  void  test_11_fetchFile_with_Dir()
	{  
		log.info("test_11_fetchFile_with_Dir");
		boolean bRet;
		tfsManager.createDir( userId, "/text");
	    bRet=tfsManager.fetchFile( userId, resourcesPath+"temp","/text");
		Assert.assertFalse("Fetch File with the Dir name should be false", bRet);
		tfsManager.rmDir( userId, "/text");
	}
	//@Test
    public  void  test_12_fetchFile_large()
	{  
		log.info("test_12_fetchFile_large");
		boolean bRet;
		bRet=tfsManager.saveFile( userId, resourcesPath+"1G","/text");
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile( userId, resourcesPath+"temp","/text");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath+"temp"),getCrc(resourcesPath+"1G"));
		tfsManager.rmFile( userId, "/text");
	}
	
}