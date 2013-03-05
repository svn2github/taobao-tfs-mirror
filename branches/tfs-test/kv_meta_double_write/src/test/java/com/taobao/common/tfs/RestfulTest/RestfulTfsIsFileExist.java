package com.taobao.common.tfs.RestfulTest;

import java.util.Random;


import org.junit.Test;
import org.junit.Ignore;
import junit.framework.Assert;

import com.taobao.common.tfs.RestfulTfsBaseCase;
import com.taobao.common.tfs.TfsConstant;


public class RestfulTfsIsFileExist extends RestfulTfsBaseCase 
{
    @Test
    public  void  test_01_isFileExist_right_filePath()
    {
    	int Ret;
    	String File_name = "test";
    	boolean bRet = false ;
	    log.info(new Throwable().getStackTrace()[0].getMethodName());
        Ret=tfsManager.createFile(appId, userId, "/"+File_name);
        Assert.assertEquals("Create File with right path should be true",Ret, TfsConstant.TFS_SUCCESS);
        Assert.assertTrue(HeadObject(buecket_name,File_name,0));
        bRet=tfsManager.isFileExist(appId, userId, "/"+File_name);
        Assert.assertTrue("Right File should be exist",bRet);
        
        tfsManager.rmFile(appId, userId, "/"+File_name);
        Assert.assertFalse(HeadObject(buecket_name,File_name));
     }
    
    @Test
    public  void  test_02_isFileExist_null_filePath()
    {
    	boolean bRet = false ; 
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
        bRet=tfsManager.isFileExist(appId, userId, null);
        Assert.assertFalse("Null File should not be exist",bRet);
    }
    
    @Test
    public  void  test_03_isFileExist_empty_filePath()
    {
    	boolean bRet = false ;        
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
        bRet=tfsManager.isFileExist(appId, userId, "");
        Assert.assertFalse("empty File should not be exist",bRet);
    }
    
    @Test
    public  void  test_04_isFileExist_wrong_filePath()
    {
    	boolean bRet = false ;      
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
        bRet=tfsManager.isFileExist(appId, userId, "fhsklajfkls");
        Assert.assertFalse("wrong File should not be exist",bRet);
    }
    
    @Test
    public  void  test_05_isFileExist_dirPath()
    {
    	int Ret;
    	boolean bRet = false ;
	    log.info(new Throwable().getStackTrace()[0].getMethodName());
        Ret=tfsManager.createDir(appId, userId, "/test");
        Assert.assertEquals("Create File with right path should be true",Ret, TfsConstant.TFS_SUCCESS);
        
        bRet=tfsManager.isFileExist(appId, userId, "/test");
        Assert.assertFalse("wrong File should not be exist",bRet);
        Assert.assertFalse(HeadObject(buecket_name,"test"));
        
        tfsManager.rmDir(appId, userId, "/test");
     }
    
    @Test
    public  void  test_06_isFileExist_rmFile()
    {
    	int Ret;
    	String File_name = "test";
    	boolean bRet = false ;
	    log.info(new Throwable().getStackTrace()[0].getMethodName());
        Ret=tfsManager.createFile(appId, userId, "/"+File_name);
        Assert.assertEquals("Create File with right path should be true",Ret, TfsConstant.TFS_SUCCESS);
        Assert.assertTrue(HeadObject(buecket_name,File_name,0));
        tfsManager.rmFile(appId, userId, "/"+File_name);
        bRet=tfsManager.isFileExist(appId, userId, "/"+File_name);
        Assert.assertFalse("not exist File should be exist",bRet);   
        Assert.assertFalse(HeadObject(buecket_name,File_name));
     }   
    
    @Test
    public  void  test_07_isFileExist_wrong_filePath_1()
    {
    	int Ret;
    	boolean bRet = false ;
	    log.info(new Throwable().getStackTrace()[0].getMethodName());
        Ret=tfsManager.createFile(appId, userId, "/test");
        Assert.assertEquals("Create File with right path should be true",Ret, TfsConstant.TFS_SUCCESS);
        
        bRet=tfsManager.isFileExist(appId, userId, "test");
        Assert.assertFalse("wrong File should not be exist",bRet);
        
        tfsManager.rmFile(appId, userId, "/test");
     }
    
    @Test
    public  void  test_08_isFileExist_wrong_filePath_2()
    {
    	int Ret;
    	boolean bRet = false ;
	    log.info(new Throwable().getStackTrace()[0].getMethodName());
        Ret=tfsManager.createFile(appId, userId, "/test");
        Assert.assertEquals("Create File with right path should be true",Ret, TfsConstant.TFS_SUCCESS);
        
        bRet=tfsManager.isFileExist(appId, userId, "///test///");
        Assert.assertTrue("right File should be exist",bRet);
        
        tfsManager.rmFile(appId, userId, "/test");
     }
    
    @Test
    public  void  test_09_isFileExist_root_dir()
    {
    	boolean bRet = false ; 
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
        bRet=tfsManager.isFileExist(appId, userId, "/");
        Assert.assertFalse("root dir can not be find",bRet);
    }
    
    @Test
    public  void  test_10_isFileExist_right_filePath_com()
    {
    	
    	boolean bRet = false ;
	    log.info(new Throwable().getStackTrace()[0].getMethodName());
        
        tfsManager.createDir(appId, userId, "/test1");
        
        tfsManager.createDir(appId, userId, "/test1/test1");
        tfsManager.createDir(appId, userId, "/test1/test2");
        tfsManager.createDir(appId, userId, "/test1/test3");
        
        tfsManager.createDir(appId, userId, "/test1/test2/test1");
        tfsManager.createDir(appId, userId, "/test1/test2/test2");
        tfsManager.createDir(appId, userId, "/test1/test2/test3");
        
        tfsManager.createFile(appId, userId, "/test1/test2/test2/test1");
        
        Assert.assertTrue(HeadObject(buecket_name,"test1/test2/test2/test1",0));
        
        bRet=tfsManager.isFileExist(appId, userId, "/test1/test2/test2/test1");
        Assert.assertTrue("Right File should be exist",bRet);
        
        tfsManager.rmFile(appId, userId, "/test1/test2/test2/test1");
        Assert.assertFalse(HeadObject(buecket_name,"/test1/test2/test2/test1"));
        
        tfsManager.rmDir(appId, userId, "/test1/test2/test1");
        tfsManager.rmDir(appId, userId, "/test1/test2/test2");
        tfsManager.rmDir(appId, userId, "/test1/test2/test3");
        
        tfsManager.rmDir(appId, userId, "/test1/test1");
        tfsManager.rmDir(appId, userId, "/test1/test2");
        tfsManager.rmDir(appId, userId, "/test1/test3");
        
        tfsManager.rmDir(appId, userId, "/test1");
     }
}