package com.taobao.common.tfs.testcase.function.ds;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.testcase.rcTfsBaseCase;
import com.taobao.common.tfs.utility.FileUtility;

public class SmallFileCacheTest extends rcTfsBaseCase
{

  
    
	
    @Test
    public void localExsitRemoteExistSamllFile() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = FileUtility.getByte(localFile);

        log.info(new Throwable().getStackTrace()[0].getMethodName()+ "===> start");
        tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);
        
        String sRet;
        /* set local remote cache */
        tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
        /* Read file */   
        tfsManager.removeLocalCache(sRet);
        tfsManager.removeRemoteCache(sRet);
        Assert.assertFalse(tfsManager.isHitLocalCache(sRet)); 
        Assert.assertFalse(tfsManager.isHitRemoteCache(sRet));
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp")); 
        Assert.assertTrue(tfsManager.isHitLocalCache(sRet)); 
        Assert.assertTrue(tfsManager.isHitRemoteCache(sRet));
        
     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
         
        Assert.assertTrue(tfsManager.isHitLocalCache(sRet)); 
        Assert.assertTrue(tfsManager.isHitRemoteCache(sRet));
        
        log.info(new Throwable().getStackTrace()[0].getMethodName()+"===========> end");
    }
    @Test
    public void localExistRemoteNonexsitentSamllFile() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = FileUtility.getByte(this.localFile);

        log.info(new Throwable().getStackTrace()[0].getMethodName()+ "===> start");
        tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);
        
        String sRet;
        /* set  cache */
        tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp")); 
        Assert.assertTrue(tfsManager.isHitLocalCache(sRet));
        Assert.assertTrue(tfsManager.isHitRemoteCache(sRet));
        //remove remote cache 
        tfsManager.removeRemoteCache(sRet);
        
        //read file  form local cache
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        Assert.assertTrue(tfsManager.isHitLocalCache(sRet));
        Assert.assertFalse(tfsManager.isHitRemoteCache(sRet));
        
        log.info(new Throwable().getStackTrace()[0].getMethodName()+"===========> end");
    }
    @Test
    public void localNonexsitentRemoteNonexsitentSamllFile() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = FileUtility.getByte(this.localFile);

        log.info(new Throwable().getStackTrace()[0].getMethodName()+ "===> start");
        tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);
        
        String sRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet); 
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));        
        
        //remove remote local  cache 
        tfsManager.removeRemoteCache(sRet);
        tfsManager.removeLocalCache(sRet);
        Assert.assertFalse(tfsManager.isHitLocalCache(sRet));
        Assert.assertFalse(tfsManager.isHitRemoteCache(sRet));
        //read file  form ds
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        Assert.assertTrue(tfsManager.isHitLocalCache(sRet));
        Assert.assertTrue(tfsManager.isHitRemoteCache(sRet));
        //tfsManager.destroy();
        log.info(new Throwable().getStackTrace()[0].getMethodName()+"===========> end");
    }
    @Test
    public void localNonexsitentRemoteExistSamllFile() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = FileUtility.getByte(this.localFile);

        log.info(new Throwable().getStackTrace()[0].getMethodName()+ "===> start");
        tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);      
        String sRet;
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
         
        /* set local remote cache */
        tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp")); 
        Assert.assertTrue(tfsManager.isHitLocalCache(sRet));
        Assert.assertTrue(tfsManager.isHitRemoteCache(sRet));
        
        
        //remove  local  cache 
        tfsManager.removeLocalCache(sRet);
        //read file  form ds
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        Assert.assertTrue(tfsManager.isHitLocalCache(sRet));
        Assert.assertTrue(tfsManager.isHitRemoteCache(sRet));
        //tfsManager.destroy();
        log.info(new Throwable().getStackTrace()[0].getMethodName()+"===========> end");
    }
}