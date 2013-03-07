package com.taobao.common.tfs.RestfulTest;


import org.junit.AfterClass;
import org.junit.Test;
import org.junit.Ignore;

import junit.framework.Assert;

import com.taobao.common.tfs.RestfulTfsBaseCase;



public class RestfulTfsMetaFetchFile extends RestfulTfsBaseCase 
{
	@Test
    public  void  test_01_fetchFile_right()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		String File_name = "textfetchFile";
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/"+File_name);
		Assert.assertTrue("Save File right path should be true", bRet);
		Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<10)));
		
	    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","/textfetchFile");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath+"temp"),getCrc(resourcesPath+"100K.jpg"));
		
		GetObject(buecket_name,File_name,TempFile);
		int kvmetaCrc = getCrc(TempFile);
		Assert.assertEquals(kvmetaCrc,getCrc(resourcesPath+"100K.jpg"));
		
		tfsManager.rmFile(appId, userId, "/"+File_name);
		Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
	@Test
    public  void  test_02_fetchFile_null_localFile()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		String File_name = "textfetchFile";
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
		Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<10)));
		Assert.assertTrue("Save File right path should be true", bRet);
	    bRet=tfsManager.fetchFile(appId, userId, null,"/textfetchFile");
		Assert.assertFalse("Fetch File null path should be false", bRet);
		tfsManager.rmFile(appId, userId, "/textfetchFile");
		Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
	@Test
    public  void  test_03_fetchFile_empty_localFile()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		String File_name = "textfetchFile";
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
		Assert.assertTrue("Save File right path should be true", bRet);
		Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<10)));
		
	    bRet=tfsManager.fetchFile(appId, userId, "","/textfetchFile");
		Assert.assertFalse("Fetch File empty path should be false", bRet);
		tfsManager.rmFile(appId, userId, "/textfetchFile");
		Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
	@Test
    public  void  test_04_fetchFile_wrong_localFile()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		String File_name = "textfetchFile";
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
		Assert.assertTrue("Save File right path should be true", bRet);
		Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<10)));
		
	    bRet=tfsManager.fetchFile(appId, userId, "hdsfhksdhf","/textfetchFile");
		Assert.assertTrue("Fetch File wrong path  be true", bRet);
		tfsManager.rmFile(appId, userId, "/textfetchFile");
		Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
	@Test
    public  void  test_05_fetchFile_null_fileName()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		String File_name = "textfetchFile";
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
		Assert.assertTrue("Save File right path should be true", bRet);
		Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<10)));
		
        String fileName = null;
	    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp", fileName);
		Assert.assertFalse("Fetch File null fileName should be false", bRet);
		tfsManager.rmFile(appId, userId, "/textfetchFile");
		Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
	@Test
    public  void  test_06_fetchFile_empty_fileName()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		String File_name = "textfetchFile";
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
		Assert.assertTrue("Save File right path should be true", bRet);
		Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<10)));
		
	    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","");
		Assert.assertFalse("Fetch File empty fileName should be false", bRet);
		tfsManager.rmFile(appId, userId, "/textfetchFile");
		Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
	@Test
    public  void  test_07_fetchFile_wrong_fileName_1()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		String File_name = "textfetchFile";
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
		Assert.assertTrue("Save File right path should be true", bRet);
		Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<10)));
		
	    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","textfetchFile");
		Assert.assertFalse("Fetch File wrong 1 fileName should be false", bRet);
		tfsManager.rmFile(appId, userId, "/textfetchFile");
		Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
	@Test
    public  void  test_08_fetchFile_wrong_fileName_2()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		String File_name = "textfetchFile";
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
		Assert.assertTrue("Save File right path should be true", bRet);
		Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<10)));
		
	    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","///textfetchFile///");
		Assert.assertTrue("Fetch File wrong 2 fileName be true", bRet);
		tfsManager.rmFile(appId, userId, "/textfetchFile");
		Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
	@Test
    public  void  test_09_fetchFile_wrong_fileName_3()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
	    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","/");
		Assert.assertFalse("Fetch File wrong 3 fileName should be false", bRet);
	}
	@Test
    public  void  test_10_fetchFile_not_exist_fileName()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
	    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","/textfetchFile");
		Assert.assertFalse("Fetch File not exist should be false", bRet);
		tfsManager.rmFile(appId, userId, "/textfetchFile");
	}
	@Test
    public  void  test_11_fetchFile_with_Dir()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		tfsManager.createDir(appId, userId, "/textfetchFile");
	    bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","/textfetchFile");
		Assert.assertFalse("Fetch File with the Dir name should be false", bRet);
		tfsManager.rmDir(appId, userId, "/textfetchFile");
	}
	@Test
    public  void  test_12_fetchFile_large()
	{  
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		boolean bRet;
		String File_name = "textfetchFile";
		bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100M.jpg","/textfetchFile");
		Assert.assertTrue("Save File right path should be true", bRet);
		Assert.assertTrue(HeadObject(buecket_name,File_name,100*(1<<20)));
		
		bRet=tfsManager.fetchFile(appId, userId, resourcesPath+"temp","/textfetchFile");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath+"temp"),getCrc(resourcesPath+"100M.jpg"));
		
		GetObject(buecket_name,File_name,TempFile);
		int kvmetaCrc = getCrc(TempFile);
		Assert.assertEquals(kvmetaCrc,getCrc(resourcesPath+"100M.jpg"));
		
		tfsManager.rmFile(appId, userId, "/textfetchFile");
		Assert.assertFalse(HeadObject(buecket_name,File_name));
	}
	
}
