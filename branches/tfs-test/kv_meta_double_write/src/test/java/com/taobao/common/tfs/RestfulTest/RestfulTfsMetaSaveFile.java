package com.taobao.common.tfs.RestfulTest;


import org.junit.Test;
import org.junit.Ignore;

import junit.framework.Assert;

import com.taobao.common.tfs.RestfulTfsBaseCase;
import com.taobao.common.tfs.TfsConstant;



public class RestfulTfsMetaSaveFile extends RestfulTfsBaseCase 
{
	@Test
    public  void  test_01_saveFile_right() throws InterruptedException
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		String File_name = "textsaveFile01";
        tfsManager.rmFile(appId, userId, "/"+File_name);
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/"+File_name);
		Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<10)));
		Assert.assertTrue("Save File right path should be true", bRet);
		
		bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","/"+File_name);
		Assert.assertEquals(getCrc(resourcesPath+"temp"),getCrc(resourcesPath+"100K.jpg"));
		
		GetObject(buecket_name,File_name,TempFile);
		int kvmetaCrc = getCrc(TempFile);
		Assert.assertEquals(kvmetaCrc,getCrc(resourcesPath+"100K.jpg"));
		
		tfsManager.rmFile(appId, userId, "/"+File_name);
		Assert.assertFalse(HeadObject(buecket_name,File_name));
	}	
	@Test
    public  void  test_02_saveFile_null_localFile()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());

		boolean bRet = false;

        String localFile = null;

		bRet = tfsManager.saveFile(appId, userId, localFile, "/textsaveFile2");
		Assert.assertFalse("Save File null path should be false", bRet);
	}
	@Test
    public  void  test_03_saveFile_empty_localFile()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, "","/textsaveFile3");
		Assert.assertFalse("Save File empty path should be false", bRet);
	}
	@Test
    public  void  test_04_saveFile_wrong_localFile()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, "sdklafhksdh","/textsaveFile4");
		Assert.assertFalse("Save File wrong path should be false", bRet);
	}
	@Test
    public  void  test_05_saveFile_null_fileName()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg",null);
		Assert.assertFalse("Save File null fileName should be false", bRet);	    
	}
	@Test
    public  void  test_06_saveFile_empty_fileName()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","");
		Assert.assertFalse("Save File null fileName should be false", bRet);	    
	}
	@Test
    public  void  test_07_saveFile_wrong_fileName_1()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textsaveFile7/");
		Assert.assertTrue("Save File wrong 1 fileName should be false", bRet);	
		tfsManager.rmFile(appId, userId, "/textsaveFile7");
	}
	@Test
    public  void  test_08_saveFile_wrong_fileName_2()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","textsaveFile8");
		Assert.assertFalse("Save File wrong 2 fileName should be false", bRet);	    
	}
	@Test
    public  void  test_09_saveFile_wrong_fileName_3()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/");
		Assert.assertFalse("Save File wrong 3 fileName should be false", bRet);	    
	}
	@Test
    public  void  test_10_saveFile_leap_fileName()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/test/test");
		Assert.assertTrue("Save File leap fileName should be true", bRet);
		Assert.assertTrue(HeadObject(buecket_name,"test/test",100*(1<<10)));
		
		tfsManager.rmFile(appId, userId, "/test/test");
		Assert.assertTrue(HeadObject(buecket_name,"test/test"));
		tfsManager.rmDir(appId, userId, "/test");
	}

	@Test
    public  void  test_11_saveFile_with_Dir()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		String File_name = "textsaveFile11";
        tfsManager.createDir(appId, userId, "/"+File_name);
        Assert.assertTrue(HeadObject(buecket_name,File_name,0));
        
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/"+File_name);
		Assert.assertTrue("Save File with the same name Dir should be true", bRet);
		Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<10)));
		
		tfsManager.rmDir(appId, userId, "/"+File_name);
		tfsManager.rmFile(appId, userId, "/"+File_name);
		Assert.assertFalse(HeadObject(buecket_name,File_name));
	}

  @Test
  public  void  test_12_saveFile_large()
  {  
  	log.info(new Throwable().getStackTrace()[0].getMethodName());
  	boolean bRet;
  	String File_name = "textsaveFile12";
  	tfsManager.rmFile(appId, userId, "/"+File_name);
  	bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100M.jpg","/"+File_name);
  	Assert.assertTrue("Save File right path should be true", bRet);
  	Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<20)));
  	
    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","/"+File_name);
  	Assert.assertTrue("Fetch File right path should be true", bRet);
 	//Assert.assertEquals(getCrc(resourcesPath+"temp"),getCrc(resourcesPath+"1G.jpg"));
  	tfsManager.rmFile(appId, userId, "/"+File_name);
  	Assert.assertFalse(HeadObject(buecket_name,File_name));
  }	


	@Test
    public  void  test_13_saveFile_many_times()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		int Ret;
		String File_name = "textsaveFile13";
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/"+File_name);
		Assert.assertTrue("Save File right path should be true", bRet);
		Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<10)));
		
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/"+File_name);
		Assert.assertFalse("Save File two times should be false", bRet);
		Ret=tfsManager.rmFile(appId, userId, "/"+File_name);
		Assert.assertEquals(Ret,TfsConstant.TFS_SUCCESS);
		Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
 
}
