package com.taobao.common.tfs.testcase.Interface.normal.meta;


import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;
import org.junit.Ignore;

import com.taobao.common.tfs.testcase.metaTfsBaseCase;
import com.taobao.common.tfs.utility.FileUtility;



public class tfsManager_13_saveFile_data extends metaTfsBaseCase 
{
	@Test
    public  void  test_01_saveFile_byte() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());

        String tfsFile = "/test01SFB";
		boolean bRet = false;
		int Ret ;

		byte data[] = null;    
		data = FileUtility.getByte(resourcesPath+"100K.jpg");

		bRet = tfsManager.saveFile(appId, userId, data, tfsFile);
		Assert.assertTrue(bRet);
        
		Ret = tfsManager.rmFile(appId, userId, tfsFile);
        Assert.assertEquals(Ret,0);
	}

    @Test
    public  void  test_02_saveFile_byte_with_empty_data() throws IOException
    {
        log.info(new Throwable().getStackTrace()[0].getMethodName());
        
        String fileName = "/test02SFBWED";
        boolean bRet = false;
        int Ret ;
        
        byte data[] = null;
        String buf = "";
        data = buf.getBytes();

        bRet = tfsManager.saveFile(appId, userId, data, fileName);
        Assert.assertTrue(bRet);
        
        Ret = tfsManager.rmFile(appId, userId, fileName);
        Assert.assertEquals(Ret,0);
    }

    @Test
    public  void  test_03_saveFile_byte_with_null_data() throws IOException
    {
        log.info(new Throwable().getStackTrace()[0].getMethodName());

        boolean bRet = false;

        byte data[] = null;

        bRet = tfsManager.saveFile(appId, userId, data, "/test03SFBWND");
        Assert.assertFalse(bRet);
    }

    @Test
    public  void  test_04_saveFile_byte_with_file_exist() throws IOException
    {
        log.info(new Throwable().getStackTrace()[0].getMethodName());

        String tfsFile = "/test04SFBWFE";
        boolean bRet = false;
        int Ret;
        byte data[] = null;
        data = FileUtility.getByte(resourcesPath+"100K.jpg");

        bRet = tfsManager.saveFile(appId, userId, data, tfsFile);
        Assert.assertTrue(bRet);

        bRet = tfsManager.saveFile(appId, userId, data, tfsFile);
        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@"+bRet);
        Assert.assertFalse(bRet);

        Ret = tfsManager.rmFile(appId, userId, tfsFile);
        Assert.assertEquals(Ret,0);
    }	

	@Test
    public  void  test_05_saveFile_byte_with_wrong_fileName_1() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());

		boolean bRet = false;

		byte data[] = null;
		data = FileUtility.getByte(resourcesPath+"100K.jpg");

		bRet = tfsManager.saveFile(appId, userId, data, "test05SFBWWTN");
		Assert.assertFalse(bRet);
	}
	
	@Test
    public  void  test_06_saveFile_byte_with_wrong_fileName_2() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());

		boolean bRet = false;

		byte data[] = null;
		data = FileUtility.getByte(resourcesPath+"100K.jpg");

		bRet = tfsManager.saveFile(appId, userId, data, "/");
		Assert.assertFalse(bRet);
	}
	
	@Test
    public  void  test_07_saveFile_byte_with_empty_fileName() throws IOException
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());

		boolean bRet = false;

		byte data[] = null;
		data = FileUtility.getByte(resourcesPath+"100K.jpg");

		bRet = tfsManager.saveFile(appId, userId, data, "");
		Assert.assertFalse(bRet);
	}
    
    @Test
    public  void  test_08_saveFile_byte_with_null_fileName() throws IOException
    {
        log.info(new Throwable().getStackTrace()[0].getMethodName());

        boolean bRet = false;

        byte data[] = null;
        data = FileUtility.getByte(resourcesPath+"100K.jpg");
        
        bRet = tfsManager.saveFile(appId, userId, data, null);
        Assert.assertFalse(bRet);
    }
}
