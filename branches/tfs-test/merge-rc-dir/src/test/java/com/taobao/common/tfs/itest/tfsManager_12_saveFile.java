package com.taobao.common.tfs.itest;


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
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/textsaveFile10");
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
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/textsaveFile7/");
		Assert.assertTrue("Save File wrong 1 fileName should be false", bRet);	
		tfsManager.rmFile(appId, userId, "/textsaveFile7");
	}
	@Test
    public  void  test_08_saveFile_wrong_fileName_2()
	{  
		log.info("test_08_saveFile_wrong_fileName_2");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","textsaveFile8");
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
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/text10/text");
		Assert.assertTrue("Save File leap fileName should be false", bRet);	    
	}
//the case test_11_saveFile_with_Dir should be error
	@Test
    public  void  test_11_saveFile_with_Dir()
	{  
		log.info("test_11_saveFile_with_Dir");
		boolean bRet;

        bRet = tfsManager.createDir(appId, userId, "/textsaveFile11");
        Assert.assertTrue(bRet);
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/textsaveFile11");
		Assert.assertTrue("Save File with the same name Dir should be true", bRet);

		bRet = tfsManager.rmDir(appId, userId, "/textsaveFile11");
        Assert.assertTrue(bRet);

		bRet = tfsManager.rmFile(appId, userId, "/textsaveFile11");
        Assert.assertTrue(bRet);
	}

  @Test
  public  void  test_12_saveFile_large()
  {  
  	log.info("test_12_saveFile_large");
  	boolean bRet;
  //	tfsManager.rmFile(appId, userId, "/textsaveFile12");
  	bRet=tfsManager.saveFile(appId, userId, resourcesPath+"1G","/textsaveFile12");
  	Assert.assertTrue("Save File right path should be true", bRet);
      bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","/textsaveFile12");
  	Assert.assertTrue("Fetch File right path should be true", bRet);
  	Assert.assertEquals(getCrc(resourcesPath+"temp"),getCrc(resourcesPath+"1G"));
  	tfsManager.rmFile(appId, userId, "/textsaveFile12");
  }	


	@Test
    public  void  test_13_saveFile_many_times()
	{  
		log.info("test_13_saveFile_many_times");
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/textsaveFile13");
		Assert.assertTrue("Save File right path should be true", bRet);
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/textsaveFile13");
		Assert.assertFalse("Save File two times should be false", bRet);
		tfsManager.rmFile(appId, userId, "/textsaveFile13");
	}
 
    @Test
    public  void  test_14_saveFile_filePath_with_middle_null()
    {
        log.info("test_14_saveFile_filePath_with_middle_null");
        boolean bRet;
        String test1 = null;
        String test2 = null;

        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","//"+test1+"///"+test2+"/textsaveFile14");
        Assert.assertFalse("Save File right path should be true", bRet);
        tfsManager.rmFile(appId, userId, "/textsaveFile14");
     }

   @Test
    public  void  test_15_saveFile_filePath_with_null()
    {
        log.info("test_15_saveFile_filePath_with_null");
        boolean bRet;
        String test = "t00000t";
        String test1 = "t11111t";
        
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","//"+test+"///"+test1+"/textsaveFile15");
        Assert.assertFalse("Save File right path should be true", bRet);
        tfsManager.rmFile(appId, userId, "/textsaveFile15");
    }
}
