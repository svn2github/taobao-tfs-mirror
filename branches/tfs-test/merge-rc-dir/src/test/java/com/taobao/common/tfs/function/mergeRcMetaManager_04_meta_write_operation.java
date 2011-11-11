package com.taobao.common.tfs.function;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


import com.taobao.common.tfs.DefaultTfsManager;

public class mergeRcMetaManager_04_meta_write_operation extends RcBaseCase 
{
  @Before
    public void setUp()
    {
    }  
  @After
    public void tearDown()
    {
    }

  @Test
    public void Function_01_write_1k() throws IOException
    {
      caseName = "Function_01_write_1k";
      log.info(caseName + "===> start");

      String localFile = "1k.jpg";
      String filePath = "/wirte_1k" + d.format(new Date());
      boolean bRet = false;
      List<String> rootServerAddrList = new ArrayList<String>();
      rootServerAddrList.add(rsAddr);
      tfsManager = new DefaultTfsManager();
      long oldUsedCapacity = getUsedCapacity(appKey);
      long oldFileCount = getFileCount(appKey);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      bRet=tfsManager.createFile(appId, userId, filePath);
      Assert.assertTrue(bRet);

      byte data[]=null;
      data=getByte(localFile);
      long len = data.length;
      long offset=0;
      long dataOffset=0;
      long Ret;
      Ret=tfsManager.write(appId, userId, filePath, offset,data, dataOffset, len);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      String sessionId = tfsManager.getSessionId();
      log.debug("sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 2);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 2);
      Assert.assertTrue(getFileSize(sessionId, 2) == 1024);

      long newUsedCapacity = getUsedCapacity(appKey);
      long newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 1024, newUsedCapacity);
      //	    Assert.assertEquals(oldFileCount + 1, newFileCount);
      tfsManager.destroy();

      //	    int savecrc = 0;
      //	    int readcrc = 1;
      //	    savecrc = getCrc(localFile);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);
      sessionId = tfsManager.getSessionId();

      ByteArrayOutputStream output = new ByteArrayOutputStream();
      Ret=tfsManager.read(appId, userId, filePath, dataOffset, len, output);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      //	    Assert.assertTrue(getOperTimes(sessionId, 1) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 1) == 1);
      Assert.assertTrue(getFileSize(sessionId, 1) == 1024);
      //	    readcrc = getCrc(retLocalFile);
      //	    Assert.assertEquals(savecrc,readcrc);
      tfsManager.destroy();

      log.info(caseName + "===> end");
    }

  @Test
    public void Function_02_write_2M() throws IOException
    {
      caseName = "Function_02_write_2M";
      log.info(caseName + "===> start");

      String localFile = "2M.jpg";
      String filePath = "/wirte_2M" + d.format(new Date());
      boolean bRet = false;
      List<String> rootServerAddrList = new ArrayList<String>();
      rootServerAddrList.add(rsAddr);
      tfsManager = new DefaultTfsManager();
      long oldUsedCapacity = getUsedCapacity(appKey);
      long oldFileCount = getFileCount(appKey);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);	    

      bRet=tfsManager.createFile(appId, userId, filePath);
      Assert.assertTrue(bRet);

      byte data[]=null;
      data=getByte(localFile);
      long len = data.length;
      long offset=0;
      long dataOffset=0;
      long Ret;
      Ret=tfsManager.write(appId, userId, filePath, offset,data, dataOffset, len);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      String sessionId = tfsManager.getSessionId();
      log.debug("@ sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 2);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 2);
      Assert.assertTrue(getFileSize(sessionId, 2) == 2*1024*1024);

      long newUsedCapacity = getUsedCapacity(appKey);
      long newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 2*1024*1024, newUsedCapacity);
      //	    Assert.assertEquals(oldFileCount + 1, newFileCount);
      tfsManager.destroy();

      int savecrc = 0;
      int readcrc = 1;
      savecrc = getCrc(localFile);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      ByteArrayOutputStream output = new ByteArrayOutputStream();
      Ret=tfsManager.read(appId, userId, filePath, dataOffset, len, output);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      sessionId = tfsManager.getSessionId();
      log.debug("@ sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 1) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 1) == 1);
      Assert.assertTrue(getFileSize(sessionId, 1) == 2*1024*1024);
      //	    readcrc =  getCrc(retLocalFile);

      //	    Assert.assertEquals(savecrc,readcrc);
      tfsManager.destroy();

      log.info(caseName + "===> end");
    }

  @Test
    public void Function_03_write_3M() throws IOException
    {
      caseName = "Function_03_write_3M";
      log.info(caseName + "===> start");

      String localFile = "3M.jpg";
      String filePath = "/wirte_3M" + d.format(new Date());
      boolean bRet = false;
      List<String> rootServerAddrList = new ArrayList<String>();
      rootServerAddrList.add(rsAddr);

      tfsManager = new DefaultTfsManager();
      long oldUsedCapacity = getUsedCapacity(appKey);
      long oldFileCount = getFileCount(appKey);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      bRet=tfsManager.createFile(appId, userId, filePath);
      Assert.assertTrue(bRet);

      byte data[]=null;
      data=getByte(localFile);
      long len = data.length;
      long offset=0;
      long dataOffset=0;
      long Ret;
      Ret=tfsManager.write(appId, userId, filePath, offset,data, dataOffset, len);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      String sessionId = tfsManager.getSessionId();
      log.debug("@ sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 2);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 2);
      Assert.assertTrue(getFileSize(sessionId, 2) == 3*1024*1024);

      long newUsedCapacity = getUsedCapacity(appKey);
      long newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 3*1024*1024, newUsedCapacity);
      //	    Assert.assertEquals(oldFileCount + 1, newFileCount);
      tfsManager.destroy();

      int savecrc = 0;
      int readcrc = 1;
      savecrc = getCrc(localFile);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      ByteArrayOutputStream output = new ByteArrayOutputStream();
      Ret=tfsManager.read(appId, userId, filePath, dataOffset, len, output);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      sessionId = tfsManager.getSessionId();
      //	    Assert.assertTrue(getOperTimes(sessionId, 1) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 1) == 1);
      Assert.assertTrue(getFileSize(sessionId, 1) == 3*1024*1024);
      readcrc =  getCrc(retLocalFile);

      //  Assert.assertEquals(savecrc,readcrc);
      tfsManager.destroy();

      log.info(caseName + "===> end");
    }

}
