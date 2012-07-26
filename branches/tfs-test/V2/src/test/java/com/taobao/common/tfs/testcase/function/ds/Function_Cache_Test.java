package com.taobao.common.tfs.testcase.function.ds;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.testcase.rcTfsBaseCase;
import com.taobao.common.tfs.utility.FileUtility;

public class Function_Cache_Test extends rcTfsBaseCase
{

    public String localFile = "100k.jpg";
    public int offset = 0;
    public int length = 100*(1<<10);
    
    
    public String tairMasterAddr = "10.232.12.141:5198";
    public String tairSlaveAddr = "10.232.12.141:5198";
    public String tairGroupName = "group_1";
    
    public List<Long> dsList = new ArrayList<Long>();
    
//    public void init ()
//    {
//    	System.out.println("start tfsManager init");
//    	tfsManager = new DefaultTfsManager();
////        tfsManager.setRcAddr(rcAddr);
////        tfsManager.setAppKey(appKey);
////        tfsManager.setAppIp(appIp);
//        Assert.assertTrue(tfsManager.init());
//    }
    
    @Test
    public void Function_01_localcache_samll_file() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = FileUtility.getByte(localFile);

        caseName = "Function_01_localcache_samll_file";
        log.info(caseName + "===> start");
       
        
        tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);
        
        String sRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(false);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
              
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(sRet, null);
        Assert.assertTrue(bRet);
        
        tfsManager.destroy();
        log.info(caseName+"===========> end");
    }
  
}