package com.taobao.common.tfs.itest;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import junit.framework.Assert;

import com.taobao.common.tfs.tfsNameBaseCase;

/*test_13-------test_21 is interface for fetchFile(long appId, long userId, String fileName, OutputStream output)*/
/*test_22-------test_31 is interface for fetchFile(long appId, long userId, String fileName, long fileOffset, long length, OutputStream output)*/
public class tfsManager_16_fetchFile_offset_output extends tfsNameBaseCase 
{
    @Before
    public void tearDown()
    {
        tfsManager.rmFile(appId, userId, "/textfetchFile");
        tfsManager.rmFile(appId, userId, "/textfetchFileLarge");
    }

    /*test_13-------test_21 is interface for fetchFile(long appId, long userId, String fileName, OutputStream output)*/
    @Ignore
    public  void  test_13_fetchFile_with_stream_right() throws FileNotFoundException
    {  
        log.info("test_13_fetchFile_with_stream_right");
        boolean bRet;
        long count=100*(1<<10);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);

        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", output);
        Assert.assertTrue("Fetch File right path should be true", bRet);
        Assert.assertEquals(getCrc(resourcesPath+"temp.jpg"),getCrc(resourcesPath+"100K"));

        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }

    @Ignore
    public void test_14_fetchFile_with_stream_wrong_localFile() throws FileNotFoundException
    {  
        log.info("test_14_fetchFile_with_stream_wrong_localFile");
        boolean bRet;
        long count=100*(1<<10);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);

        OutputStream output = new FileOutputStream("hdsfhksdhf");
        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", output);
        Assert.assertTrue("Fetch File wrong path  be true", bRet);

        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }
    @Ignore
    public  void  test_15_fetchFile_with_stream_null_fileName() throws FileNotFoundException
    {  
        log.info("test_15_fetchFile_with_stream_null_fileName");
        boolean bRet;
        long count=100*(1<<10);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);

        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, null, output);
        Assert.assertFalse("Fetch File null fileName should be false", bRet);
        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }
    @Ignore
    public  void  test_16_fetchFile_with_stream_empty_fileName() throws FileNotFoundException
    {  
        log.info("test_16_fetchFile_with_stream_empty_fileName");
        boolean bRet;
        long count=100*(1<<10);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);

        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "", output);
        Assert.assertFalse("Fetch File empty fileName should be false", bRet);
        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }
    @Ignore
    public  void  test_17_fetchFile_with_stream_wrong_fileName_1() throws FileNotFoundException
    {  
        log.info("test_17_fetchFile_wrong_fileName_1");
        boolean bRet;
        long count=100*(1<<10);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);
        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "textfetchFile", output);
        Assert.assertFalse("Fetch File wrong 1 fileName should be false", bRet);
        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }
    @Ignore
    public  void  test_18_fetchFile_with_stream_wrong_fileName_3() throws FileNotFoundException
    {  
        log.info("test_18_fetchFile_with_stream_wrong_fileName_3");
        boolean bRet;
        long count=100*(1<<10);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);
        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "/", output);
        Assert.assertFalse("Fetch File wrong 3 fileName should be false", bRet);
        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }
    @Ignore
    public  void  test_19_fetchFile_with_stream_not_exist_fileName() throws FileNotFoundException
    {  
        log.info("test_11_fetchFile_with_stream_not_exist_fileName");
        boolean bRet;
        long count=100*(1<<10);
        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", output);
        Assert.assertFalse("Fetch File not exist should be false", bRet);
        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }
    @Ignore
    public void test_20_fetchFile_with_stream_with_Dir() throws FileNotFoundException
    {
        log.info("test_11_fetchFile_with_stream_with_Dir");
        boolean bRet;
        long count=100*(1<<10);
        tfsManager.createDir(appId, userId, "/textfetchFile");
        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", output);
        Assert.assertFalse("Fetch File with the Dir name should be false", bRet);
        tfsManager.rmDir(appId, userId, "/textfetchFile");
    }

    @Test
    public  void  test_22_fetchFile_with_stream_right() throws FileNotFoundException
    {  
        log.info("test_22_fetchFile_with_stream_right");
        boolean bRet;
        long count = 100*(1<<10);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);

        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");

        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", 0, count, output);
        Assert.assertTrue("Fetch File right path should be true", bRet);
        Assert.assertEquals(getCrc(resourcesPath+"temp.jpg"),getCrc(resourcesPath+"100K"));

        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", 1024, count, output);
        Assert.assertTrue("Fetch File right path should be true", bRet);

        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile",count+1, count, output);
        Assert.assertTrue("Fetch File right path should be true", bRet);

        bRet = tfsManager.rmFile(appId, userId, "/textfetchFile");
        Assert.assertTrue(bRet);
    }

    @Ignore
    public void test_23_fetchFile_with_stream_wrong_localFile() throws FileNotFoundException
    {  
        log.info("test_14_fetchFile_with_stream_wrong_localFile");
        boolean bRet;
        long count=100*(1<<10);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);
        OutputStream output = new FileOutputStream("hdsfhksdhf");
        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", 0, count, output);
        Assert.assertTrue("Fetch File wrong path  be true", bRet);

        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", 1024, count, output);
        Assert.assertTrue("Fetch File wrong path  be true", bRet);

        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }
    @Ignore
    public  void  test_24_fetchFile_with_stream_null_fileName() throws FileNotFoundException
    {  
        log.info("test_15_fetchFile_with_stream_null_fileName");
        boolean bRet;
        long count=100*(1<<10);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);
        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, null, 0, count, output);
        Assert.assertFalse("Fetch File null fileName should be false", bRet);
        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }
    @Ignore
    public  void  test_25_fetchFile_with_stream_empty_fileName() throws FileNotFoundException
    {  
        log.info("test_16_fetchFile_with_stream_empty_fileName");
        boolean bRet;
        long count=100*(1<<10);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);
        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "", 0, count, output);
        Assert.assertFalse("Fetch File empty fileName should be false", bRet);

        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }
    @Ignore
    public  void  test_26_fetchFile_with_stream_wrong_fileName_1() throws FileNotFoundException
    {  
        log.info("test_17_fetchFile_wrong_fileName_1");
        boolean bRet;
        long count=100*(1<<10);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);
        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "textfetchFile", 0, count, output);
        Assert.assertFalse("Fetch File wrong 1 fileName should be false", bRet);
        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }
    @Ignore
    public  void  test_27_fetchFile_with_stream_wrong_fileName_3() throws FileNotFoundException
    {  
        log.info("test_18_fetchFile_with_stream_wrong_fileName_3");
        boolean bRet;
        long count=100*(1<<10);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);
        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "/", 0, count, output);
        Assert.assertFalse("Fetch File wrong 3 fileName should be false", bRet);
        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }
    @Ignore
    public  void  test_28_fetchFile_with_stream_not_exist_fileName() throws FileNotFoundException
    {  
        log.info("test_11_fetchFile_with_stream_not_exist_fileName");
        boolean bRet;
        long count=100*(1<<10);
        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", 0, count, output);
        Assert.assertFalse("Fetch File not exist should be false", bRet);
        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }
    @Ignore
    public void test_29_fetchFile_with_stream_with_Dir() throws FileNotFoundException
    {
        log.info("test_11_fetchFile_with_stream_with_Dir");
        boolean bRet;
        long count=100*(1<<10);
        tfsManager.createDir(appId, userId, "/textfetchFile");
        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", 0, count, output);
        Assert.assertFalse("Fetch File with the Dir name should be false", bRet);

        tfsManager.rmDir(appId, userId, "/textfetchFile");
    }
    @Ignore
    public  void  test_30_fetchFile_with_stream_large() throws FileNotFoundException 
    {  
        log.info("test_21_fetchFile_with_stream_large");
        boolean bRet;
        long count=1*(1<<30);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"1G","/textfetchFileLarge");
        Assert.assertTrue("Save File right path should be true", bRet);
        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFileLarge", 0, count, output);
        Assert.assertTrue("Fetch File right path should be true", bRet);
        Assert.assertEquals(getCrc(resourcesPath+"temp.jpg"),getCrc(resourcesPath+"1G"));

        tfsManager.rmFile(appId, userId, "/textfetchFileLarge");
    }


    @Ignore
    public  void  test_21_fetchFile_with_stream_large() throws FileNotFoundException
    {  
        log.info("test_21_fetchFile_with_stream_large");
        boolean bRet;
        long count=1*(1<<30);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"1G","/textfetchFileLarge");
        Assert.assertTrue("Save File right path should be true", bRet);

        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFileLarge", output);
        Assert.assertTrue("Fetch File right path should be true", bRet);
        Assert.assertEquals(getCrc(resourcesPath+"temp.jpg"),getCrc(resourcesPath+"1G"));
        tfsManager.rmFile(appId, userId, "/textfetchFileLarge");
    }

}

