package com.taobao.common.tfs.function.metatest;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.RcBaseCase;

public class mergeRcMetaManager_03_meta_operation_info extends RcBaseCase 
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
    public void Function_01_write_many_time_parts() throws IOException
    {
      caseName = "Function_01_write_many_time_parts";
      log.info(caseName + "===> start");

      String localFile = "10k.jpg";
      String filePath = "/pwirte_10k" + d.format(new Date());
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
      Ret=tfsManager.write(appId, userId,filePath, offset,data, dataOffset, len);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      String sessionId = tfsManager.getSessionId();
      log.debug("sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 2);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 2);
      Assert.assertTrue(getFileSize(sessionId, 2) == 10*1024);

      long newUsedCapacity = getUsedCapacity(appKey);
      long newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 10*1024, newUsedCapacity);
      //  Assert.assertEquals(oldFileCount + 1, newFileCount);
      tfsManager.destroy();
      /******************************************/

      localFile = "10k.jpg";
      bRet = false;
      tfsManager = new DefaultTfsManager();

      oldUsedCapacity = getUsedCapacity(appKey);
      oldFileCount = getFileCount(appKey);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      data=getByte(localFile);
      len = data.length;
      offset=10*(1<<10);
      dataOffset=0;
      Ret=tfsManager.write(appId, userId, filePath, offset,data, dataOffset, len);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      sessionId = tfsManager.getSessionId();
      log.debug("sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
      Assert.assertTrue(getFileSize(sessionId, 2) == 10*1024);

      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 10*1024, newUsedCapacity);
      // Assert.assertEquals(oldFileCount, newFileCount);
      tfsManager.destroy();
      /******************************************/

      localFile = "10k.jpg";
      bRet = false;
      tfsManager = new DefaultTfsManager();

      oldUsedCapacity = getUsedCapacity(appKey);
      oldFileCount = getFileCount(appKey);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      data=getByte(localFile);
      len = data.length;
      offset=20*(1<<10)+1;
      dataOffset=0;
      Ret=tfsManager.write(appId, userId,filePath, offset,data, dataOffset, len);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      sessionId = tfsManager.getSessionId();
      log.debug("sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
      Assert.assertTrue(getFileSize(sessionId, 2) == 10*1024);

      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 10*1024, newUsedCapacity);
      // Assert.assertEquals(oldFileCount, newFileCount);
      tfsManager.destroy();
      /******************************************/

      localFile = "1b.jpg";
      bRet = false;
      tfsManager = new DefaultTfsManager();

      oldUsedCapacity = getUsedCapacity(appKey);
      oldFileCount = getFileCount(appKey);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);

      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      data=getByte(localFile);
      len = data.length;
      offset=20*(1<<10);
      dataOffset=0;
      Ret=tfsManager.write(appId, userId, filePath, offset,data, dataOffset, len);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      sessionId = tfsManager.getSessionId();
      log.debug("sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
      // Assert.assertTrue(getFileSize(sessionId, 2) == 1);

      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 1, newUsedCapacity);
      //  Assert.assertEquals(oldFileCount, newFileCount);
      tfsManager.destroy();
      /******************************************/

      localFile = "1b.jpg";
      bRet = false;
      tfsManager = new DefaultTfsManager();

      oldUsedCapacity = getUsedCapacity(appKey);
      oldFileCount = getFileCount(appKey);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);

      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      data=getByte(localFile);
      len = data.length;
      offset=20*(1<<10);
      dataOffset=0;
      Ret=tfsManager.write(appId, userId, filePath, offset,data, dataOffset, len);
      Assert.assertTrue(Ret==0);

      sleep(MAX_STAT_TIME);
      sessionId = tfsManager.getSessionId();
      log.debug("sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 0);
      //	    Assert.assertTrue(getFileSize(sessionId, 2) == 0);

      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity, newUsedCapacity);
      //  Assert.assertEquals(oldFileCount, newFileCount);
      tfsManager.destroy();
      /******************************************/
      log.info(caseName + "===> end");
    }

  @Test
    public void Function_02_write_large_many_times_parts() throws IOException
    {
      caseName = "Function_02_write_large_many_times_parts";
      log.info(caseName + "===> start");

      /******************************************/
      String localFile = "3M.jpg";
      String filePath = "/pwite_3M" + d.format(new Date());
      boolean bRet = false;
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
      Ret=tfsManager.write(appId, userId,filePath, offset, data, dataOffset, len);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      String sessionId = tfsManager.getSessionId();
      log.debug("sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 2);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 2);
      Assert.assertTrue(getFileSize(sessionId, 2) == 3*1024*1024);

      long newUsedCapacity = getUsedCapacity(appKey);
      long newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 3*1024*1024, newUsedCapacity);
      //   Assert.assertEquals(oldFileCount + 1, newFileCount);
      tfsManager.destroy();
      /******************************************/

      localFile = "3M.jpg";
      bRet = false;
      tfsManager = new DefaultTfsManager();

      oldUsedCapacity = getUsedCapacity(appKey);
      oldFileCount = getFileCount(appKey);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      data=getByte(localFile);
      len = data.length;
      offset=3*(1<<20);
      dataOffset=0;
      Ret=tfsManager.write(appId, userId, filePath, offset, data, dataOffset, len);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      sessionId = tfsManager.getSessionId();
      log.debug("sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
      Assert.assertTrue(getFileSize(sessionId, 2) == 3*1024*1024);

      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 3*1024*1024, newUsedCapacity);
      //  Assert.assertEquals(oldFileCount, newFileCount);
      tfsManager.destroy();
      /******************************************/

      localFile = "3M.jpg";
      bRet = false;
      tfsManager = new DefaultTfsManager();

      oldUsedCapacity = getUsedCapacity(appKey);
      oldFileCount = getFileCount(appKey);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      data=getByte(localFile);
      len = data.length;
      offset=6*(1<<20)+1;
      dataOffset=0;
      Ret=tfsManager.write(appId, userId, filePath, offset,data, dataOffset, len);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      sessionId = tfsManager.getSessionId();
      log.debug("sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
      Assert.assertTrue(getFileSize(sessionId, 2) == 3*1024*1024);

      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 3*1024*1024, newUsedCapacity);
      //  Assert.assertEquals(oldFileCount, newFileCount);
      tfsManager.destroy();
      /******************************************/

      localFile = "1b.jpg";
      bRet = false;
      tfsManager = new DefaultTfsManager();

      oldUsedCapacity = getUsedCapacity(appKey);
      oldFileCount = getFileCount(appKey);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      data=getByte(localFile);
      len = data.length;
      offset=6*(1<<20);
      dataOffset=0;
      Ret=tfsManager.write(appId, userId, filePath, offset,data, dataOffset, len);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      sessionId = tfsManager.getSessionId();
      log.debug("sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
      //  Assert.assertTrue(getFileSize(sessionId, 2) == 1);

      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 1, newUsedCapacity);
      //  Assert.assertEquals(oldFileCount, newFileCount);
      tfsManager.destroy();
      /******************************************/

      localFile = "1b.jpg";
      bRet = false;
      tfsManager = new DefaultTfsManager();

      oldUsedCapacity = getUsedCapacity(appKey);
      oldFileCount = getFileCount(appKey);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      data=getByte(localFile);
      len = data.length;
      offset=6*(1<<20);
      dataOffset=0;

      Ret=tfsManager.write(appId, userId, filePath, offset,data, dataOffset, len);
      Assert.assertTrue(Ret==0);

      sleep(MAX_STAT_TIME);
      sessionId = tfsManager.getSessionId();
      log.debug("sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 0);
      //  Assert.assertTrue(getFileSize(sessionId, 2) == 0);

      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity, newUsedCapacity);
      //   Assert.assertEquals(oldFileCount, newFileCount);
      //   tfsManager.destroy();
      /******************************************/
      log.info(caseName + "===> end");
    }

  @Test
    public void Function_03_write_many_times_parts_com() throws IOException
    {
      caseName = "Function_03_write_many_times_parts_com";
      log.info(caseName + "===> start");

      /******************************************/
      String localFile = "10k.jpg";
      String filePath = "/pwrite_10k" + d.format(new Date());
      boolean bRet = false;
      long dataOffset = 0;
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
      long dataoffset=0;
      long Ret;
      Ret=tfsManager.write(appId, userId,filePath, offset,data, dataOffset, len);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      String sessionId = tfsManager.getSessionId();
      log.debug("sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
      Assert.assertTrue(getFileSize(sessionId, 2) == 10*1024);

      long newUsedCapacity = getUsedCapacity(appKey);
      long newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 10*1024, newUsedCapacity);
      //	    Assert.assertEquals(oldFileCount + 1, newFileCount);
      tfsManager.destroy();
      /******************************************/

      localFile = "2M.jpg";
      bRet = false;
      tfsManager = new DefaultTfsManager();

      oldUsedCapacity = getUsedCapacity(appKey);
      oldFileCount = getFileCount(appKey);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      data=getByte(localFile);
      len = data.length;
      offset=20*(1<<10);
      dataOffset=0;
      Ret=tfsManager.write(appId, userId, filePath, offset,data, dataOffset, len);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      sessionId = tfsManager.getSessionId();
      log.debug("sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
      Assert.assertTrue(getFileSize(sessionId, 2) == 2*1024*1024);

      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 2*1024*1024, newUsedCapacity);
      //   Assert.assertEquals(oldFileCount, newFileCount);
      tfsManager.destroy();
      /******************************************/

      localFile = "3M.jpg";
      bRet = false;
      tfsManager = new DefaultTfsManager();

      oldUsedCapacity = getUsedCapacity(appKey);
      oldFileCount = getFileCount(appKey);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      data=getByte(localFile);
      len = data.length;
      offset=8*(1<<20)+20*(1<<10);
      dataOffset=0;
      Ret=tfsManager.write(appId, userId,filePath, offset,data, dataOffset, len);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      sessionId = tfsManager.getSessionId();
      log.debug("sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
      Assert.assertTrue(getFileSize(sessionId, 2) == 3*1024*1024);

      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 3*1024*1024, newUsedCapacity);
      // Assert.assertEquals(oldFileCount, newFileCount);
      tfsManager.destroy();
      /******************************************/

      localFile = "10k.jpg";
      bRet = false;
      tfsManager = new DefaultTfsManager();

      oldUsedCapacity = getUsedCapacity(appKey);
      oldFileCount = getFileCount(appKey);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      data=getByte(localFile);
      len = data.length;
      offset=10*(1<<10);
      dataOffset=0;
      Ret=tfsManager.write(appId, userId, filePath, offset,data, dataOffset, len);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      sessionId = tfsManager.getSessionId();
      log.debug("sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
      Assert.assertTrue(getFileSize(sessionId, 2) == 10*1024);

      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 10*1024, newUsedCapacity);
      // Assert.assertEquals(oldFileCount, newFileCount);
      tfsManager.destroy();
      /******************************************/

      localFile = "10k.jpg";
      bRet = false;
      tfsManager = new DefaultTfsManager();

      oldUsedCapacity = getUsedCapacity(appKey);
      oldFileCount = getFileCount(appKey);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      data=getByte(localFile);
      len = data.length;
      offset=10*(1<<10);
      dataOffset=0;
      Ret=tfsManager.write(appId, userId, filePath, offset,data, dataOffset, len);
      Assert.assertTrue(Ret==0);

      sleep(MAX_STAT_TIME);
      sessionId = tfsManager.getSessionId();
      log.debug("sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 0);
      //Assert.assertTrue(getFileSize(sessionId, 2) == 0);

      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      //  Assert.assertEquals(oldUsedCapacity, newUsedCapacity);
      Assert.assertEquals(oldFileCount, newFileCount);
      tfsManager.destroy();
      /******************************************/

      localFile = "3M.jpg";
      bRet = false;
      tfsManager = new DefaultTfsManager();

      oldUsedCapacity = getUsedCapacity(appKey);
      oldFileCount = getFileCount(appKey);
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      data=getByte(localFile);
      len = data.length;
      offset=2*(1<<20)+20*(1<<10);
      dataOffset=0;
      Ret=tfsManager.write(appId, userId,filePath, offset,data, dataOffset, len);
      Assert.assertEquals(Ret,len);

      sleep(MAX_STAT_TIME);
      sessionId = tfsManager.getSessionId();
      log.debug("sessionId: " + sessionId);
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
      Assert.assertTrue(getFileSize(sessionId, 2) == 3*1024*1024);

      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 3*1024*1024, newUsedCapacity);
      //  Assert.assertEquals(oldFileCount, newFileCount);
      tfsManager.destroy();
      /******************************************/
      log.info(caseName + "===> end");
    }

  @Test
    public void Function_04_operation_info_of_read()
    {
      caseName = "Function_04_operation_info_of_read";
      log.info(caseName + "===> start");

      boolean bRet = false;
      String localFile = "100M.jpg";
      String filePath = "/pwrite_100M" + d.format(new Date());

      long oldUsedCapacity = getUsedCapacity(appKey);
      long oldFileCount = getFileCount(appKey);
      tfsManager = new DefaultTfsManager();
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      bRet=tfsManager.saveFile(appId,userId,localFile,filePath);
      Assert.assertTrue(bRet);

      sleep(MAX_STAT_TIME);
      String sessionId = tfsManager.getSessionId();
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
      Assert.assertTrue(getFileSize(sessionId, 2) == 100*1024*1024);
      long newUsedCapacity = getUsedCapacity(appKey);
      long newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 100*1024*1024, newUsedCapacity);
      //	    Assert.assertEquals(oldFileCount + 1, newFileCount);
      tfsManager.destroy();

      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);
      sessionId = tfsManager.getSessionId();
      ByteArrayOutputStream output = new ByteArrayOutputStream();

      long totalReadLength = 0;
      long readLength;
      long len=5*1024;
      long dataOffset=0;
      for(int i = 0; i < 5; i++)
      {

        readLength=tfsManager.read(appId, userId,filePath, dataOffset, len, output);
        Assert.assertEquals(readLength,len);

        totalReadLength += readLength;
        dataOffset+=readLength;
        sleep(MAX_STAT_TIME);
        //	      Assert.assertTrue(getOperTimes(sessionId, 1) == i + 1);
        //	      Assert.assertTrue(getSuccTimes(sessionId, 1) == i + 1);
        Assert.assertTrue(getFileSize(sessionId, 1) == totalReadLength);
        newUsedCapacity = getUsedCapacity(appKey);
        newFileCount = getFileCount(appKey);
        Assert.assertEquals(oldUsedCapacity + 100*1024*1024, newUsedCapacity);
        //     Assert.assertEquals(oldFileCount + 1, newFileCount);
      }
      for(int i = 0; i < 5; i++)
      {
        readLength=tfsManager.read(appId, userId,"/unknown", dataOffset, len, output);
        Assert.assertTrue(readLength<0);
        sleep(MAX_STAT_TIME);
        //	      Assert.assertTrue(getOperTimes(sessionId, 1) == 5 + i + 1);
        //	      Assert.assertTrue(getSuccTimes(sessionId, 1) == 5);
        Assert.assertTrue(getFileSize(sessionId, 1) == totalReadLength);
        newUsedCapacity = getUsedCapacity(appKey);
        newFileCount = getFileCount(appKey);
        Assert.assertEquals(oldUsedCapacity + 100*1024*1024, newUsedCapacity);
        //   Assert.assertEquals(oldFileCount + 1, newFileCount);
      }
      tfsManager.destroy();

      log.info(caseName + "===> end");
    }

  @Test
    public void Function_05_operation_info_of_writeLargeFile() throws FileNotFoundException
    {
      caseName = "Function_05_operation_info_of_writeLargeFile";
      log.info(caseName + "===> start");
      boolean bRet = false;
      String localFile = "100M.jpg";
      String filePath = "/pwite_100M" + d.format(new Date());
      List<String> rootServerAddrList = new ArrayList<String>();
      rootServerAddrList.add(rsAddr);

      long oldUsedCapacity = getUsedCapacity(appKey);
      long oldFileCount = getFileCount(appKey);
      tfsManager = new DefaultTfsManager();
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      bRet=tfsManager.saveFile(appId,userId,localFile,filePath);
      Assert.assertTrue(bRet);

      sleep(MAX_STAT_TIME);
      String sessionId = tfsManager.getSessionId();
      //	    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
      Assert.assertTrue(getFileSize(sessionId, 2) == 100*1024*1024);
      long newUsedCapacity = getUsedCapacity(appKey);
      long newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 100*1024*1024, newUsedCapacity);
      //    Assert.assertEquals(oldFileCount + 1, newFileCount);
      tfsManager.destroy();

      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);
      sessionId = tfsManager.getSessionId();
      OutputStream output = new FileOutputStream("empty.jpg");

      long totalReadLength = 0;
      long readLength;
      long len=90*1024*1024;
      long dataOffset=0;
      for(int i = 0; i < 5; i++)
      {

        readLength=tfsManager.read(appId, userId,filePath, dataOffset, len, output);
        Assert.assertEquals(readLength,len);

        totalReadLength += readLength;
        sleep(MAX_STAT_TIME);
        //	      Assert.assertTrue(getOperTimes(sessionId, 1) == i + 1);
        //	      Assert.assertTrue(getSuccTimes(sessionId, 1) == i + 1);
        Assert.assertTrue(getFileSize(sessionId, 1) == totalReadLength);
        newUsedCapacity = getUsedCapacity(appKey);
        newFileCount = getFileCount(appKey);
        Assert.assertEquals(oldUsedCapacity + 100*1024*1024, newUsedCapacity);
        //	   Assert.assertEquals(oldFileCount + 1, newFileCount);
      }
      for(int i = 0; i < 5; i++)
      {
        readLength=tfsManager.read(appId, userId,"/unknown", dataOffset, len, output);
        Assert.assertTrue(readLength<0);
        sleep(MAX_STAT_TIME);
        //	      Assert.assertTrue(getOperTimes(sessionId, 1) == 5 + i + 1);
        //	      Assert.assertTrue(getSuccTimes(sessionId, 1) == 5);
        Assert.assertTrue(getFileSize(sessionId, 1) == totalReadLength);
        newUsedCapacity = getUsedCapacity(appKey);
        newFileCount = getFileCount(appKey);
        Assert.assertEquals(oldUsedCapacity + 100*1024*1024, newUsedCapacity);
        //  Assert.assertEquals(oldFileCount + 1, newFileCount);
      }
      tfsManager.destroy();

      log.info(caseName + "===> end");
    }

  @Test
    public void Function_06_operation_info_of_saveSmallFile()
    {
      caseName = "Function_06_operation_info_of_saveSmallFile";
      log.info(caseName + "===> start");

      boolean bRet = false;
      String localFile = "2M.jpg";
      String filePath = "/savesmallfile2M" + d.format(new Date());
      //      String filePath ="/text_06_info_";
      tfsManager = new DefaultTfsManager();
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      long oldUsedCapacity = getUsedCapacity(appKey);
      long oldFileCount = getFileCount(appKey);
      long newUsedCapacity = 0; 
      long newFileCount = 0;
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);
      String sessionId = tfsManager.getSessionId();
      for(int i = 0; i < 5; i++)
      {

        bRet=tfsManager.saveFile(appId,userId,localFile,filePath + "_" + i);
        Assert.assertTrue(bRet);

        sleep(MAX_STAT_TIME);
        //	      Assert.assertTrue(getOperTimes(sessionId, 2) == i + 1);
        //	      Assert.assertTrue(getSuccTimes(sessionId, 2) == i + 1);
        Assert.assertTrue(getFileSize(sessionId, 2) == (i+1)*2*1024*1024);
        newUsedCapacity = getUsedCapacity(appKey);
        newFileCount = getFileCount(appKey);
        Assert.assertEquals(oldUsedCapacity + (i+1)*2*1024*1024, newUsedCapacity);
        //      Assert.assertEquals(oldFileCount + i + 1, newFileCount);
      }

      localFile = "unknown";
      for(int i = 1; i < 6; i++)
      {
        sleep(1);
        bRet=tfsManager.saveFile(appId,userId,localFile, filePath + "_" + i);
        Assert.assertFalse(bRet);
        sleep(MAX_STAT_TIME);

        //	      Assert.assertTrue(getOperTimes(sessionId, 2) == 5+i);//????
        //	      Assert.assertTrue(getSuccTimes(sessionId, 2) == 5);
        Assert.assertTrue(getFileSize(sessionId, 2) == 5*2*1024*1024);
        newUsedCapacity = getUsedCapacity(appKey);
        newFileCount = getFileCount(appKey);
        Assert.assertEquals(oldUsedCapacity + 5*2*1024*1024, newUsedCapacity);
        //    Assert.assertEquals(oldFileCount + 5, newFileCount);
      }
      tfsManager.destroy();

      log.info(caseName + "===> end");
    }

  @Test
    public void Function_07_operation_info_of_unlinkFile()
    {
      caseName = "Function_07_operation_info_of_unlinkFile";
      log.info(caseName + "===> start");

      boolean bRet = false;
      String localFile = "2M.jpg";
      String filePath = "/operation_" + d.format(new Date());

      long oldUsedCapacity = 0;
      long oldFileCount = 0;
      long newUsedCapacity = 0; 
      long newFileCount = 0;
      tfsManager = new DefaultTfsManager();
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);

      oldUsedCapacity = getUsedCapacity(appKey);
      oldFileCount = getFileCount(appKey);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);
      String sessionId = tfsManager.getSessionId();
      String []sRet = new String[5];

      for(int i = 0; i < 5; i++)
      {
        sleep(1);
        sRet[i] = filePath + "_" + i;
        bRet=tfsManager.saveFile(appId,userId,localFile,sRet[i]);
        Assert.assertTrue(bRet);
        sleep(MAX_STAT_TIME);
        //	      Assert.assertTrue(getOperTimes(sessionId, 2) == i + 1);
        //	      Assert.assertTrue(getSuccTimes(sessionId, 2) == i + 1);
        Assert.assertTrue(getFileSize(sessionId, 2) == (i+1)*2*1024*1024);
        newUsedCapacity = getUsedCapacity(appKey);
        newFileCount = getFileCount(appKey);
        Assert.assertEquals(oldUsedCapacity + (i+1)*2*1024*1024, newUsedCapacity);
        //    Assert.assertEquals(oldFileCount + i + 1, newFileCount);
      }
      tfsManager.destroy();

      tfsManager = new DefaultTfsManager();
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      oldUsedCapacity = getUsedCapacity(appKey);
      oldFileCount = getFileCount(appKey);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);
      sessionId = tfsManager.getSessionId();
      for(int i = 0; i < 5; i++)
      {
        bRet = tfsManager.rmFile(appId, userId, sRet[i]);
        Assert.assertTrue(bRet);

        if(i < 3)
        {
          bRet = tfsManager.rmFile(appId, userId, sRet[i]);
          Assert.assertFalse(bRet);
        }
      }
      bRet = tfsManager.rmFile(appId, userId, "unknown1");
      Assert.assertFalse(bRet);
      bRet = tfsManager.rmFile(appId, userId, "unknown2");
      Assert.assertFalse(bRet);

      sleep(MAX_STAT_TIME);
      //	    Assert.assertTrue(getOperTimes(sessionId, 4) == 10);
      //	    Assert.assertTrue(getSuccTimes(sessionId, 4) == 5);
      Assert.assertTrue(getFileSize(sessionId, 4) == 5*2*1024*1024);
      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity-5*2*1024*1024, newUsedCapacity);
      //  Assert.assertEquals(oldFileCount-5, newFileCount);
      tfsManager.destroy();

      log.info(caseName + "===> end");
    }
}
