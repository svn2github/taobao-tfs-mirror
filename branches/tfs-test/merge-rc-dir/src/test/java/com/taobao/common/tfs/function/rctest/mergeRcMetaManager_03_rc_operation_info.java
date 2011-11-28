/**
 * 
 */
package com.taobao.common.tfs.function.rctest;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.Ignore;
import java.io.File;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import com.taobao.gaia.KillTypeEnum;
import com.taobao.common.tfs.DefaultTfsManager; 
import com.taobao.common.tfs.RcBaseCase;


/**
 * @author Administrator/chuyu
 *
 */
public class mergeRcMetaManager_03_rc_operation_info extends RcBaseCase {
  
  @Before
  public void setUp(){
  
  }
  
  @After
  public void tearDown(){
  }

  @Test
  public void Function_15_operation_info_of_read(){

    caseName = "Function_15_operation_info_of_read";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "100M.jpg";
   // List<String> rootServerAddrList = new ArrayList<String>();
   // rootServerAddrList.add(rsAddr);

    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    tfsManager = new DefaultTfsManager();
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);

    String sRet = null;
    sRet=tfsManager.saveLargeFile(localFile,null,null);
    Assert.assertNotNull(sRet);
    sleep(MAX_STAT_TIME);
    String sessionId = tfsManager.getSessionId();
//(sessionId, 2) == 1);
//(sessionId, 2) == 1);
    Assert.assertTrue(getFileSize(sessionId, 2) == 100*1024*1024);
    long newUsedCapacity = getUsedCapacity(appKey);
    long newFileCount = getFileCount(appKey);
    Assert.assertEquals(oldUsedCapacity + 100*1024*1024, newUsedCapacity);
    Assert.assertEquals(oldFileCount + 1, newFileCount);
    tfsManager.destroy();

    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    sessionId = tfsManager.getSessionId();
    byte[] data = new byte[5*1024];
    long totalReadLength = 0;
    for(int i = 0; i < 5; i++)
    {
      int fd = -1;
      fd = tfsManager.openReadFile(sRet, null);
      Assert.assertTrue(fd>0);
      int readLength = tfsManager.readFile(fd, data, 0, (i+1)*1024);
      Assert.assertTrue(readLength == (i+1)*1024);
      totalReadLength += readLength;
      String tfsFileName = tfsManager.closeFile(fd);
      Assert.assertNotNull(tfsFileName);
      sleep(MAX_STAT_TIME);
//(sessionId, 1) == i + 1);
//(sessionId, 1) == i + 1);
      Assert.assertTrue(getFileSize(sessionId, 1) == totalReadLength);
      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 100*1024*1024, newUsedCapacity);
      Assert.assertEquals(oldFileCount + 1, newFileCount);
    }
    for(int i = 0; i < 5; i++)
    {
      int fd = -1;
      fd = tfsManager.openReadFile("unknown", null);
      Assert.assertTrue(fd<0);
      int readLength = tfsManager.readFile(fd, data, 0, 2*1024);
      Assert.assertTrue(readLength<0);
      String tfsFileName = tfsManager.closeFile(fd);
      tfsManager.closeFile(fd);
      Assert.assertNull(tfsFileName);
      sleep(MAX_STAT_TIME);
//(sessionId, 1) == 5 + i + 1);
//(sessionId, 1) == 5);
      Assert.assertTrue(getFileSize(sessionId, 1) == totalReadLength);
      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 100*1024*1024, newUsedCapacity);
      Assert.assertEquals(oldFileCount + 1, newFileCount);
    }
    tfsManager.destroy();

    log.info(caseName + "===> end");
  }

 // @Test
  public void Function_15_operation_info_of_writeLargeFile(){

    caseName = "Function_15_operation_info_of_writeLargeFile";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "100M.jpg";
    List<String> rootServerAddrList = new ArrayList<String>();
    rootServerAddrList.add(rsAddr);

    tfsManager = new DefaultTfsManager();
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    String sRet = null;
    sRet=tfsManager.saveLargeFile(localFile, null, null);
    Assert.assertNotNull(sRet);
    sleep(MAX_STAT_TIME);
    String sessionId = tfsManager.getSessionId();
//(sessionId, 2) == 1);
//(sessionId, 2) == 1);
    Assert.assertTrue(getFileSize(sessionId, 2) == 100*1024*1024);
    long newUsedCapacity = getUsedCapacity(appKey);
    long newFileCount = getFileCount(appKey);
    Assert.assertEquals(oldUsedCapacity + 100*1024*1024, newUsedCapacity);
    Assert.assertEquals(oldFileCount + 1, newFileCount);

    DefaultTfsManager tfsManager2 = new DefaultTfsManager();
    tfsManager2.setRcAddr(rcAddr);
    tfsManager2.setAppKey(appKey);
    tfsManager2.setAppIp(appIp);
    bRet = tfsManager2.init();
    Assert.assertTrue(bRet);
    sessionId = tfsManager2.getSessionId();
    byte[] data = new byte[90*1024*1024];
    long totalWriteLength = 0;
    for(int i = 0; i < 5; i++){
      int fd = -1;
      fd = tfsManager.openReadFile(sRet, null);
      Assert.assertTrue(fd>0);
      int readLength = tfsManager.readFile(fd, data, 0, (i+80)*1024*1024);
      Assert.assertTrue(readLength == (i+80)*1024*1024);
      String tfsFileName = tfsManager.closeFile(fd);
      Assert.assertNotNull(tfsFileName);
      fd = -1;
      fd = tfsManager2.openWriteFile(null, null, retLocalFile);
      Assert.assertTrue(fd>0);
      long writeLength = tfsManager2.writeFile(fd, data, 0, (i+80)*1024*1024);
      Assert.assertTrue(writeLength == (i+80)*1024*1024);
      totalWriteLength += writeLength;
      String writedTfsFileName = tfsManager2.closeFile(fd);
      Assert.assertNotNull(writedTfsFileName);
      sleep(MAX_STAT_TIME);
//(sessionId, 2) == i + 1);
//(sessionId, 2) == i + 1);
      Assert.assertTrue(getFileSize(sessionId, 2) == totalWriteLength);
      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 100*1024*1024 + totalWriteLength, newUsedCapacity);
      Assert.assertEquals(oldFileCount + i + 2, newFileCount);
    }

    localFile = "unknown";
    for(int i = 1; i < 6; i++){
      sRet = null;
      sRet=tfsManager.saveLargeFile(localFile, null, null);
      Assert.assertNull(sRet);
      sleep(MAX_STAT_TIME);

//(sessionId, 2) == 5);
//(sessionId, 2) == 5);
      System.out.println(totalWriteLength);
      Assert.assertTrue(getFileSize(sessionId, 2) == totalWriteLength);
      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 100*1024*1024 + totalWriteLength, newUsedCapacity);
      Assert.assertEquals(oldFileCount + 6, newFileCount);
    }

    tfsManager.destroy();
    tfsManager2.destroy();

    log.info(caseName + "===> end");
  }

  @Test
  public void Function_16_operation_info_of_saveSmallFile(){

    caseName = "Function_16_operation_info_of_saveSmallFile";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "2M.jpg";

    tfsManager = new DefaultTfsManager();
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);

    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    long newUsedCapacity = 0; 
    long newFileCount = 0;
    String sessionId = tfsManager.getSessionId();
    String sRet = null;
    for(int i = 0; i < 5; i++){
      sRet = null;
      sRet = tfsManager.saveFile(localFile, null, null, false);
      Assert.assertNotNull(sRet);
      sleep(MAX_STAT_TIME);
      Assert.assertTrue(getFileSize(sessionId, 2) == (i+1)*2*1024*1024);
      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + (i+1)*2*1024*1024, newUsedCapacity);
      Assert.assertEquals(oldFileCount + i + 1, newFileCount);
    }

    localFile = "unknown";
    for(int i = 1; i < 6; i++){
      sleep(1);
      sRet = null;
      sRet = tfsManager.saveFile(localFile, null, null, false);
      Assert.assertNull(sRet);
      sleep(MAX_STAT_TIME);

      Assert.assertTrue(getFileSize(sessionId, 2) == 5*2*1024*1024);
      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 5*2*1024*1024, newUsedCapacity);
      Assert.assertEquals(oldFileCount + 5, newFileCount);
    }
    tfsManager.destroy();

    log.info(caseName + "===> end");
  }


  @Test
  public void Function_17_operation_info_of_unlinkFile(){

    caseName = "Function_17_operation_info_of_unlinkFile";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "2M.jpg";

    long oldUsedCapacity = 0;
    long oldFileCount = 0;
    long newUsedCapacity = 0; 
    long newFileCount = 0;
    tfsManager = new DefaultTfsManager();
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);

    oldUsedCapacity = getUsedCapacity(appKey);
    oldFileCount = getFileCount(appKey);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    String sessionId = tfsManager.getSessionId();
    String []sRet = new String[5];
    for(int i = 0; i < 5; i++){
      sleep(1);
      sRet[i] = null;
      sRet[i] = tfsManager.saveFile(localFile, null, null, false);
      Assert.assertNotNull(sRet[i]);
      sleep(MAX_STAT_TIME);
//(sessionId, 2) == i + 1);
//(sessionId, 2) == i + 1);
      Assert.assertTrue(getFileSize(sessionId, 2) == (i+1)*2*1024*1024);
      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + (i+1)*2*1024*1024, newUsedCapacity);
      Assert.assertEquals(oldFileCount + i + 1, newFileCount);
    }
    tfsManager.destroy();

    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    sessionId = tfsManager.getSessionId();
    for(int i = 0; i < 5; i++){
      bRet = tfsManager.unlinkFile(sRet[i], null);
      Assert.assertTrue(bRet);
      if(i < 3){
        bRet = tfsManager.unlinkFile(sRet[i], null);
        Assert.assertFalse(bRet);
      }
    }
    bRet = tfsManager.unlinkFile("unknown1", null);
    Assert.assertFalse(bRet);
    bRet = tfsManager.unlinkFile("unknown2", null);
    Assert.assertFalse(bRet);
    sleep(MAX_STAT_TIME);
//(sessionId, 4) == 10);
//(sessionId, 4) == 5);
    Assert.assertTrue(getFileSize(sessionId, 4) == 5*2*1024*1024);
    newUsedCapacity = getUsedCapacity(appKey);
    newFileCount = getFileCount(appKey);
    Assert.assertEquals(oldUsedCapacity, newUsedCapacity);
    Assert.assertEquals(oldFileCount, newFileCount);
    tfsManager.destroy();

    log.info(caseName + "===> end");
  }

  @Test
  public void Function_18_operation_info_of_mix_operation(){

    caseName = "Function_18_operation_info_of_mix_operation";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "2M.jpg";
    List<String> rootServerAddrList = new ArrayList<String>();
    rootServerAddrList.add(rsAddr);

    tfsManager = new DefaultTfsManager();
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    String []sRet = new String[10];
    //small files
    log.debug("=== smallfile ");
    for(int i = 0; i < 2; i++){
      sleep(1);
      sRet[i] = null;
      sRet[i] = tfsManager.saveFile(localFile, null, null, false);
      Assert.assertNotNull(sRet[i]);
      bRet = tfsManager.fetchFile(sRet[i], null, retLocalFile);
      Assert.assertTrue(bRet);
      bRet = tfsManager.unlinkFile(sRet[i], null);
      Assert.assertTrue(bRet);
    }
    //fetch after unlink
    for(int i = 2; i < 4; i++){
      sleep(1);
      sRet[i] = null;
      sRet[i] = tfsManager.saveFile(localFile, null, null, false);
      Assert.assertNotNull(sRet[i]);
      bRet = tfsManager.unlinkFile(sRet[i], null);
      Assert.assertTrue(bRet);
      bRet = tfsManager.fetchFile(sRet[i], null, retLocalFile);
      Assert.assertFalse(bRet);
    }
    //unlink twice
    for(int i = 0; i < 2; i++){
      sleep(1);
      sRet[i] = null;
      sRet[i] = tfsManager.saveFile(localFile, null, null, false);
      Assert.assertNotNull(sRet[i]);
      bRet = tfsManager.unlinkFile(sRet[i], null);
      Assert.assertTrue(bRet);
      bRet = tfsManager.unlinkFile(sRet[i], null);
      Assert.assertFalse(bRet);
    }

    log.debug("=== largefile ");

    localFile = "100M.jpg";
    //large file
    for(int i = 4; i < 6; i++){
      sleep(1);
      sRet[i] = null;
      sRet[i] = tfsManager.saveLargeFile(localFile, null, null);
      Assert.assertNotNull(sRet[i]);
      log.debug("=== savelarge " + i + " " + sRet[i]);
      bRet = tfsManager.fetchFile(sRet[i], null, retLocalFile);
      Assert.assertTrue(bRet);
      bRet = tfsManager.unlinkFile(sRet[i], null);
      Assert.assertTrue(bRet);
    }
    //fetch after unlink
    for(int i = 6; i < 8; i++){
      sleep(1);
      sRet[i] = null;
      sRet[i] = tfsManager.saveLargeFile(localFile, null, null);
      Assert.assertNotNull(sRet[i]);
      bRet = tfsManager.unlinkFile(sRet[i], null);
      Assert.assertTrue(bRet);
      bRet = tfsManager.fetchFile(sRet[i], null, retLocalFile);
      Assert.assertFalse(bRet);
    }
    //unlink twice
    for(int i = 6; i < 8; i++){
      sleep(1);
      sRet[i] = null;
      sRet[i] = tfsManager.saveLargeFile(localFile, null, null);
      Assert.assertNotNull(sRet[i]);
      bRet = tfsManager.unlinkFile(sRet[i], null);
      Assert.assertTrue(bRet);
      bRet = tfsManager.unlinkFile(sRet[i], null);
      Assert.assertFalse(bRet);
    }
    //read file
    byte [] data = new byte[4*1024*1024];
    for(int i = 8; i < 10; i++){
      sleep(1);
      sRet[i] = null;
      sRet[i] = tfsManager.saveLargeFile(localFile, null, null);
      Assert.assertNotNull(sRet[i]);
      int fd = -1;
      fd = tfsManager.openReadFile(sRet[i], null);
      Assert.assertTrue(fd>0);
      int length = tfsManager.readFile(fd, data, 0, 4*1024*1021);
      Assert.assertTrue(length == 4*1024*1021);
      bRet = tfsManager.unlinkFile(sRet[i], null);
      Assert.assertTrue(bRet);
    }
    //read file after unlink
    for(int i = 8; i < 10; i++){
      sleep(1);
      sRet[i] = null;
      sRet[i] = tfsManager.saveLargeFile(localFile, null, null);
      Assert.assertNotNull(sRet[i]);
      log.debug("@ tfsname: " + sRet[i]);
      bRet = tfsManager.unlinkFile(sRet[i], null);
      Assert.assertTrue(bRet);
      int fd = -1; 
      fd = tfsManager.openReadFile(sRet[i], null);
      Assert.assertTrue(fd<0);
      int length = tfsManager.readFile(fd, data, 0, 4*1024*1021);
      Assert.assertTrue(length<0);
    }
    tfsManager.destroy();

    log.info(caseName + "===> end");
  }
}
