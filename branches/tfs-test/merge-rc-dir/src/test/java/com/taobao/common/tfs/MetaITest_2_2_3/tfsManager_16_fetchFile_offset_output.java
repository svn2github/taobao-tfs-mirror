package com.taobao.common.tfs.MetaITest_2_2_3;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import junit.framework.Assert;

import com.taobao.common.tfs.tfsNameBaseCase;


public class tfsManager_16_fetchFile_offset_output extends tfsNameBaseCase 
{


    @Test
    public  void  test_01_fetchFile_with_stream_right() throws FileNotFoundException
    {  
        log.info("test_01_fetchFile_with_stream_right");
        boolean bRet;
        
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);

        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", output);
        Assert.assertTrue("Fetch File right path should be true", bRet);
        Assert.assertEquals(getCrc(resourcesPath+"temp.jpg"),getCrc(resourcesPath+"100K.jpg"));

        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }

    @Test
    public void test_02_fetchFile_with_stream_wrong_localFile() throws FileNotFoundException
    {  
        log.info("test_02_fetchFile_with_stream_wrong_localFile");
        boolean bRet;
     
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);

        OutputStream output = new FileOutputStream("hdsfhksdhf");
        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", output);
        Assert.assertTrue("Fetch File wrong path  be true", bRet);

        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }
    @Test
    public  void  test_03_fetchFile_with_stream_null_fileName() throws FileNotFoundException
    {  
        log.info("test_03_fetchFile_with_stream_null_fileName");
        boolean bRet;
        
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);

        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, null, output);
        Assert.assertFalse("Fetch File null fileName should be false", bRet);
        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }
    @Test
    public  void  test_04_fetchFile_with_stream_empty_fileName() throws FileNotFoundException
    {  
        log.info("test_04_fetchFile_with_stream_empty_fileName");
        boolean bRet;
        
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);

        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "", output);
        Assert.assertFalse("Fetch File empty fileName should be false", bRet);
        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }
    @Test
    public  void  test_05_fetchFile_with_stream_wrong_fileName_1() throws FileNotFoundException
    {  
        log.info("test_05_fetchFile_with_stream_wrong_fileName_1");
        boolean bRet;
        
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);
        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "textfetchFile", output);
        Assert.assertFalse("Fetch File wrong 1 fileName should be false", bRet);
        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }
    @Test
    public  void  test_06_fetchFile_with_stream_wrong_fileName_3() throws FileNotFoundException
    {  
        log.info("test_06_fetchFile_with_stream_wrong_fileName_3");
        boolean bRet;
        
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);
        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "/", output);
        Assert.assertFalse("Fetch File wrong 3 fileName should be false", bRet);
        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }
    @Test
    public  void  test_07_fetchFile_with_stream_not_exist_fileName() throws FileNotFoundException
    {  
        log.info("test_07_fetchFile_with_stream_not_exist_fileName");
        boolean bRet;
        
        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", output);
        Assert.assertFalse("Fetch File not exist should be false", bRet);
        tfsManager.rmFile(appId, userId, "/textfetchFile");
    }
    @Test
    public void test_08_fetchFile_with_stream_with_Dir() throws FileNotFoundException
    {
        log.info("test_08_fetchFile_with_stream_with_Dir");
        boolean bRet;
        
        tfsManager.createDir(appId, userId, "/textfetchFile");
        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", output);
        Assert.assertFalse("Fetch File with the Dir name should be false", bRet);
        tfsManager.rmDir(appId, userId, "/textfetchFile");
    }

    @Test
    public  void  test_09_fetchFile_with_stream_large() throws FileNotFoundException
    {  
        log.info("test_09_fetchFile_with_stream_large");
        boolean bRet;
        
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"1G.jpg","/textfetchFileLarge");
        Assert.assertTrue("Save File right path should be true", bRet);

        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");
        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFileLarge", output);
        Assert.assertTrue("Fetch File right path should be true", bRet);
        Assert.assertEquals(getCrc(resourcesPath+"temp.jpg"),getCrc(resourcesPath+"1G.jpg"));
        tfsManager.rmFile(appId, userId, "/textfetchFileLarge");
    }

    @Test
    public  void  test_10_fetchFile_with_stream_right_with_fileOffset() throws FileNotFoundException
    {  
        log.info("test_10_fetchFile_with_stream_right_with_fileOffset");
        boolean bRet;
        long count = 100*(1<<10);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);

        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");

        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", 1024, count, output);
        Assert.assertTrue("Fetch File right path should be true", bRet);

        tfsManager.rmFile(appId, userId, "/textfetchFile");    
    }
    
    @Test
    public  void  test_11_fetchFile_with_stream_right_more_fileOffset() throws FileNotFoundException
    {  
        log.info("test_11_fetchFile_with_stream_right_more_fileOffset");
        boolean bRet;
        long count = 100*(1<<10);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
        //Assert.assertTrue("Save File right path should be true", bRet);

        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");

        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", count+1, count, output);
        Assert.assertFalse("Fetch File more fileOffset should be false", bRet);

        tfsManager.rmFile(appId, userId, "/textfetchFile");    
    }
    
    @Test
    public  void  test_12_fetchFile_with_stream_right_wrong_fileOffset() throws FileNotFoundException
    {  
        log.info("test_12_fetchFile_with_stream_right_wrong_fileOffset");
        boolean bRet;
        long count = 100*(1<<10);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);

        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");

        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", -1, count, output);
        Assert.assertFalse("Fetch File wrong fileOffset should be false", bRet);

        tfsManager.rmFile(appId, userId, "/textfetchFile");    
    }
    
    @Test
    public  void  test_13_fetchFile_with_stream_right_more_length() throws FileNotFoundException
    {  
        log.info("test_13_fetchFile_with_stream_right_more_length");
        boolean bRet;
        long count = 100*(1<<10);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);

        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");

        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", 0, count+1, output);
        Assert.assertTrue("Fetch File more length should be true", bRet);

        tfsManager.rmFile(appId, userId, "/textfetchFile");    
    }
    
    @Test
    public  void  test_14_fetchFile_with_stream_right_wrong_length() throws FileNotFoundException
    {  
        log.info("test_14_fetchFile_with_stream_right_wrong_length");
        boolean bRet;
        
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);

        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");

        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", 0, -1, output);
        Assert.assertFalse("Fetch File more length should be false", bRet);

        tfsManager.rmFile(appId, userId, "/textfetchFile");    
    }
    
    @Test
    public  void  test_15_fetchFile_with_stream_right_with_length() throws FileNotFoundException
    {  
        log.info("test_15_fetchFile_with_stream_right_with_length");
        boolean bRet;
        long count = 100*(1<<10);
        bRet=tfsManager.saveFile(appId, userId, resourcesPath+"100K.jpg","/textfetchFile");
        Assert.assertTrue("Save File right path should be true", bRet);

        OutputStream output = new FileOutputStream(resourcesPath+"temp.jpg");

        bRet=tfsManager.fetchFile(appId, userId, "/textfetchFile", 0, count-1024, output);
        Assert.assertTrue("Fetch File right path should be true", bRet);

        tfsManager.rmFile(appId, userId, "/textfetchFile");    
    }
}

