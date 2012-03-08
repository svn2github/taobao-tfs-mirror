package com.taobao.common.tfs.MetaITest_2_2_3;


import org.junit.Test;
import org.junit.Ignore;

import junit.framework.Assert;

import com.taobao.common.tfs.tfsNameBaseCase;


public class tfsManager_12_saveFile extends tfsNameBaseCase 
{
	@Test
    public  void  test_01_saveFile_right() throws InterruptedException
	{  
		log.info("test_01_saveFile_right");
		boolean bRet;
        tfsManager.rmFile(appId, userId, "/textsaveFile10");
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textsaveFile10");
		Assert.assertTrue("Save File right path should be true", bRet);
		tfsManager.rmFile(appId, userId, "/textsaveFile10");
	}	
	@Test
    public  void  test_02_saveFile_null_localFile()
	{  
		log.info("test_02_saveFile_null_localFile");

		boolean bRet = false;

        String localFile = null;

		bRet = tfsManager.saveFile(appId, userId, localFile, "/textsaveFile2");
		Assert.assertFalse("Save File null path should be false", bRet);
	}
	@Test
    public  void  test_03_saveFile_empty_localFile()
	{  
		log.info("test_03_saveFile_empty_localFile");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, "","/textsaveFile3");
		Assert.assertFalse("Save File empty path should be false", bRet);
	}
	@Test
    public  void  test_04_saveFile_wrong_localFile()
	{  
		log.info("test_04_saveFile_wrong_localFile");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, "sdklafhksdh","/textsaveFile4");
		Assert.assertFalse("Save File wrong path should be false", bRet);
	}
	@Test
    public  void  test_05_saveFile_null_fileName()
	{  
		log.info("test_05_saveFile_null_fileName");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg",null);
		Assert.assertFalse("Save File null fileName should be false", bRet);	    
	}
	@Test
    public  void  test_06_saveFile_empty_fileName()
	{  
		log.info("test_06_saveFile_empty_fileName");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","");
		Assert.assertFalse("Save File null fileName should be false", bRet);	    
	}
	@Test
    public  void  test_07_saveFile_wrong_fileName_1()
	{  
		log.info("test_07_saveFile_wrong_fileName_1");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textsaveFile7/");
		Assert.assertTrue("Save File wrong 1 fileName should be false", bRet);	
		tfsManager.rmFile(appId, userId, "/textsaveFile7");
	}
	@Test
    public  void  test_08_saveFile_wrong_fileName_2()
	{  
		log.info("test_08_saveFile_wrong_fileName_2");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","textsaveFile8");
		Assert.assertFalse("Save File wrong 2 fileName should be false", bRet);	    
	}
	@Test
    public  void  test_09_saveFile_wrong_fileName_3()
	{  
		log.info("test_09_saveFile_wrong_fileName_3");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/");
		Assert.assertFalse("Save File wrong 3 fileName should be false", bRet);	    
	}
	@Test
    public  void  test_10_saveFile_leap_fileName()
	{  
		log.info("test_10_saveFile_leap_fileName");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/test/test");
		Assert.assertTrue("Save File leap fileName should be true", bRet);
		
		tfsManager.rmFile(appId, userId, "/test/test");
		tfsManager.rmDir(appId, userId, "/test");
	}

	@Test
    public  void  test_11_saveFile_with_Dir()
	{  
		log.info("test_11_saveFile_with_Dir");
		boolean bRet;
         
        tfsManager.createDir(appId, userId, "/textsaveFile11");
        
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textsaveFile11");
		Assert.assertTrue("Save File with the same name Dir should be true", bRet);

		tfsManager.rmDir(appId, userId, "/textsaveFile11");
        
		tfsManager.rmFile(appId, userId, "/textsaveFile11");
	}

  @Test
  public  void  test_12_saveFile_large()
  {  
  	log.info("test_12_saveFile_large");
  	boolean bRet;
  //	tfsManager.rmFile(appId, userId, "/textsaveFile12");
  	bRet=tfsManager.saveFile(appId, userId, resourcesPath+"1G.jpg","/textsaveFile12");
  	Assert.assertTrue("Save File right path should be true", bRet);
      bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","/textsaveFile12");
  	Assert.assertTrue("Fetch File right path should be true", bRet);
  	Assert.assertEquals(getCrc(resourcesPath+"temp"),getCrc(resourcesPath+"1G.jpg"));
  	tfsManager.rmFile(appId, userId, "/textsaveFile12");
  }	


	@Test
    public  void  test_13_saveFile_many_times()
	{  
		log.info("test_13_saveFile_many_times");
		boolean bRet;
		int Ret;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textsaveFile13");
		Assert.assertTrue("Save File right path should be true", bRet);
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textsaveFile13");
		Assert.assertFalse("Save File two times should be false", bRet);
		Ret=tfsManager.rmFile(appId, userId, "/textsaveFile13");
		Assert.assertEquals(Ret,0);
	}
 
}
