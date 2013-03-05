package com.taobao.common.tfs.RestfulTest;

import java.util.Random;


import org.junit.Test;
import org.junit.Ignore;
import junit.framework.Assert;

import com.taobao.common.tfs.RestfulTfsBaseCase;
import com.taobao.common.tfs.TfsConstant;


public class RestfulTfsIsDirExist extends RestfulTfsBaseCase 
{
    @Test
    public  void  test_01_isDirExist_right_filePath()
    {
    	int Ret;
    	boolean bRet = false ;
	    log.info(new Throwable().getStackTrace()[0].getMethodName());
        Ret=tfsManager.createDir(appId, userId, "/test");
        Assert.assertEquals("Create Dir with right path should be true",Ret, TfsConstant.TFS_SUCCESS);
        
        bRet=tfsManager.isDirExist(appId, userId, "/test");
        Assert.assertTrue("Right Dir should be exist",bRet);
        
        tfsManager.rmDir(appId, userId, "/test");
     }
    
    @Test
    public  void  test_02_isDirExist_null_filePath()
    {
    	boolean bRet = false ; 
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
        bRet=tfsManager.isDirExist(appId, userId, null);
        Assert.assertFalse("Null Dir should not be exist",bRet);
    }
    
    @Test
    public  void  test_03_isDirExist_empty_filePath()
    {
    	boolean bRet = false ;        
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
        bRet=tfsManager.isDirExist(appId, userId, "");
        Assert.assertFalse("empty Dir should not be exist",bRet);
    }
    
    @Test
    public  void  test_04_isDirExist_wrong_filePath()
    {
    	boolean bRet = false ;      
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
        bRet=tfsManager.isDirExist(appId, userId, "sjfdlksahfkj");
        Assert.assertFalse("wrong Dir should not be exist",bRet);
    }
    
    @Test
    public  void  test_05_isDirExist_dirPath()
    {
    	int Ret;
    	boolean bRet = false ;
	    log.info(new Throwable().getStackTrace()[0].getMethodName());
        Ret=tfsManager.createFile(appId, userId, "/test");
        Assert.assertEquals("Create File with right path should be true",Ret, TfsConstant.TFS_SUCCESS);
        
        bRet=tfsManager.isDirExist(appId, userId, "/test");
        Assert.assertFalse("wrong Dir should not be exist",bRet);
        
        tfsManager.rmFile(appId, userId, "/test");
     }
    
    @Test
    public  void  test_06_isDirExist_rmFile()
    {
    	int Ret;
    	boolean bRet = false ;
	    log.info(new Throwable().getStackTrace()[0].getMethodName());
        Ret=tfsManager.createDir(appId, userId, "/test");
        Assert.assertEquals("Create Dir with right path should be true",Ret, TfsConstant.TFS_SUCCESS);
        tfsManager.rmDir(appId, userId, "/test");
        
        bRet=tfsManager.isDirExist(appId, userId, "/test");
        Assert.assertFalse("not exist File should not be exist",bRet);      
     } 
    
    @Test
    public  void  test_07_isDirExist_wrong_filePath_1()
    {
    	int Ret;
    	boolean bRet = false ;
	    log.info(new Throwable().getStackTrace()[0].getMethodName());
        Ret=tfsManager.createDir(appId, userId, "/test");
        Assert.assertEquals("Create Dir with right path should be true",Ret, TfsConstant.TFS_SUCCESS);
        
        bRet=tfsManager.isDirExist(appId, userId, "test");
        Assert.assertFalse("wrong Dir should not be exist",bRet);
        
        tfsManager.rmDir(appId, userId, "/test");
     }
    
    @Test
    public  void  test_08_isDirExist_wrong_filePath_2()
    {
    	int Ret;
    	boolean bRet = false ;
	    log.info(new Throwable().getStackTrace()[0].getMethodName());
        Ret=tfsManager.createDir(appId, userId, "/test");
        Assert.assertEquals("Create Dir with right path should be true",Ret, TfsConstant.TFS_SUCCESS);
        
        bRet=tfsManager.isDirExist(appId, userId, "////test/////");
        Assert.assertTrue("Right Dir should be exist",bRet);
        
        tfsManager.rmDir(appId, userId, "/test");
     }
    
    @Test // 注意：这次的userId和以往的都不一样
    public  void  test_09_isDirExist_root_filePath()
    {
    	boolean bRet = false ; 
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
        bRet=tfsManager.isDirExist(appId, 99, "/");
        Assert.assertFalse("Null Dir should not be exist",bRet);
    }
    
    @Test
    public  void  test_10_isDirExist_right_filePath_com()
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
        
        bRet=tfsManager.isDirExist(appId, userId, "/test1/test2/test2");
        Assert.assertTrue("Right Dir should be exist",bRet);
        
        tfsManager.rmDir(appId, userId, "/test1/test2/test1");
        tfsManager.rmDir(appId, userId, "/test1/test2/test2");
        tfsManager.rmDir(appId, userId, "/test1/test2/test3");
        
        tfsManager.rmDir(appId, userId, "/test1/test1");
        tfsManager.rmDir(appId, userId, "/test1/test2");
        tfsManager.rmDir(appId, userId, "/test1/test3");
        
        tfsManager.rmDir(appId, userId, "/test1");
     }
}