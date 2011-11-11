/**
 * 
 */
package com.taobao.common.tfs.function;

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


/**
 * @author Administrator/chuyu
 *
 */
public class mergeRcMetaManager_02_rc_unlinkFile_operation extends RcBaseCase {
  
  @Before
  public void setUp(){
  
  }
  
  @After
  public void tearDown(){
  }

  @Ignore
  public void Function_09_unlinkFile_1k(){

    caseName = "Function_09_unlinkFile_1k";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "1k.jpg";
    List<String> rootServerAddrList = new ArrayList<String>();
    rootServerAddrList.add(rsAddr);

    tfsManager = new DefaultTfsManager();
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    String sRet = null;
    sRet=tfsManager.saveFile(localFile, null, null, false);
    Assert.assertNotNull(sRet);
    sleep(MAX_STAT_TIME);
    String sessionId = tfsManager.getSessionId();
    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
    Assert.assertTrue(getFileSize(sessionId, 2) == 1024);
    long newUsedCapacity = getUsedCapacity(appKey);
    long newFileCount = getFileCount(appKey);
    Assert.assertEquals(oldUsedCapacity + 1024, newUsedCapacity);
    Assert.assertEquals(oldFileCount + 1, newFileCount);
    tfsManager.destroy();

    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    bRet = tfsManager.unlinkFile(sRet, null);
    Assert.assertTrue(bRet);
    sleep(MAX_STAT_TIME);
    sessionId = tfsManager.getSessionId();
    Assert.assertTrue(getOperTimes(sessionId, 4) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 4) == 1);
    Assert.assertTrue(getFileSize(sessionId, 4) == 1024);
    newUsedCapacity = getUsedCapacity(appKey);
    newFileCount = getFileCount(appKey);
    Assert.assertEquals(oldUsedCapacity, newUsedCapacity);
    Assert.assertEquals(oldFileCount, newFileCount);

    bRet = tfsManager.fetchFile(sRet, null, retLocalFile);
    Assert.assertFalse(bRet);
    sleep(MAX_STAT_TIME);
    Assert.assertTrue(getOperTimes(sessionId, 1) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 1) == 0);
    Assert.assertTrue(getFileSize(sessionId, 1) == 0);
    tfsManager.destroy();

    log.info(caseName + "===> end");
  }

  @Ignore
  public void Function_10_unlinkFile_2M(){

    caseName = "Function_10_unlinkFile_2M";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "2M.jpg";
    List<String> rootServerAddrList = new ArrayList<String>();
    rootServerAddrList.add(rsAddr);

    tfsManager = new DefaultTfsManager();
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    String sRet = null;
    sRet=tfsManager.saveFile(localFile, null, null, false);
    Assert.assertNotNull(sRet);
    sleep(MAX_STAT_TIME);
    String sessionId = tfsManager.getSessionId();
    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
    Assert.assertTrue(getFileSize(sessionId, 2) == 2*1024*1024);
    long newUsedCapacity = getUsedCapacity(appKey);
    long newFileCount = getFileCount(appKey);
    Assert.assertEquals(oldUsedCapacity + 2*1024*1024, newUsedCapacity);
    Assert.assertEquals(oldFileCount + 1, newFileCount);
    tfsManager.destroy();

    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    bRet = tfsManager.unlinkFile(sRet, null);
    Assert.assertTrue(bRet);
    sleep(MAX_STAT_TIME);
    sessionId = tfsManager.getSessionId();
    Assert.assertTrue(getOperTimes(sessionId, 4) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 4) == 1);
    Assert.assertTrue(getFileSize(sessionId, 4) == 2*1024*1024);
    newUsedCapacity = getUsedCapacity(appKey);
    newFileCount = getFileCount(appKey);
    Assert.assertEquals(oldUsedCapacity, newUsedCapacity);
    Assert.assertEquals(oldFileCount, newFileCount);

    bRet = tfsManager.fetchFile(sRet, null, retLocalFile);
    Assert.assertFalse(bRet);
    sleep(MAX_STAT_TIME);
    Assert.assertTrue(getOperTimes(sessionId, 1) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 1) == 0);
    Assert.assertTrue(getFileSize(sessionId, 1) == 0);
    tfsManager.destroy();

    log.info(caseName + "===> end");
  }

  @Ignore
  public void Function_11_unlinkFile_3M(){

    caseName = "Function_11_unlinkFile_3M";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "3M.jpg";
    List<String> rootServerAddrList = new ArrayList<String>();
    rootServerAddrList.add(rsAddr);

    tfsManager = new DefaultTfsManager();
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    String sRet = null;
    sRet=tfsManager.saveFile(localFile,null,null, false);
    Assert.assertNotNull(sRet);
    sleep(MAX_STAT_TIME);
    String sessionId = tfsManager.getSessionId();
    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
    Assert.assertTrue(getFileSize(sessionId, 2) == 3*1024*1024);
    long newUsedCapacity = getUsedCapacity(appKey);
    long newFileCount = getFileCount(appKey);
    Assert.assertEquals(oldUsedCapacity + 3*1024*1024, newUsedCapacity);
    Assert.assertEquals(oldFileCount + 1, newFileCount);
    tfsManager.destroy();

    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    bRet = tfsManager.unlinkFile(sRet, null);
    Assert.assertTrue(bRet);
    sleep(MAX_STAT_TIME);
    sessionId = tfsManager.getSessionId();
    Assert.assertTrue(getOperTimes(sessionId, 4) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 4) == 1);
    Assert.assertTrue(getFileSize(sessionId, 4) == 3*1024*1024);
    newUsedCapacity = getUsedCapacity(appKey);
    newFileCount = getFileCount(appKey);
    Assert.assertEquals(oldUsedCapacity, newUsedCapacity);
    Assert.assertEquals(oldFileCount, newFileCount);
    bRet = tfsManager.fetchFile(sRet, null, retLocalFile);
    Assert.assertFalse(bRet);
    sleep(MAX_STAT_TIME);
    Assert.assertTrue(getOperTimes(sessionId, 1) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 1) == 0);
    Assert.assertTrue(getFileSize(sessionId, 1) == 0);
    tfsManager.destroy();

    log.info(caseName + "===> end");
  }

  @Ignore
  public void Function_12_unlinkFile_6G(){

    caseName = "Function_12_unlinkFile_6G";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "6G.jpg";
    long expectSize = (long) (6 * ((long) 1 << 30));
    List<String> rootServerAddrList = new ArrayList<String>();
    rootServerAddrList.add(rsAddr);

    tfsManager = new DefaultTfsManager();
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    String sRet = null;
    sRet=tfsManager.saveLargeFile(localFile, null, null);
    Assert.assertNotNull(sRet);
    sleep(MAX_STAT_TIME);
    String sessionId = tfsManager.getSessionId();
    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
    Assert.assertEquals(expectSize, getFileSize(sessionId, 2));
    long newUsedCapacity = getUsedCapacity(appKey);
    long newFileCount = getFileCount(appKey);
    Assert.assertEquals(oldUsedCapacity + expectSize, newUsedCapacity);
    Assert.assertEquals(oldFileCount + 1, newFileCount);
    tfsManager.destroy();

    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    bRet = tfsManager.unlinkFile(sRet, null);
    Assert.assertTrue(bRet);
    sleep(MAX_STAT_TIME);
    sessionId = tfsManager.getSessionId();
    Assert.assertTrue(getOperTimes(sessionId, 4) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 4) == 1);
    Assert.assertEquals(expectSize, getFileSize(sessionId, 4));
    newUsedCapacity = getUsedCapacity(appKey);
    newFileCount = getFileCount(appKey);

    Assert.assertEquals(oldUsedCapacity, newUsedCapacity);
    Assert.assertEquals(oldFileCount, newFileCount);
    bRet = tfsManager.fetchFile(sRet, null, retLocalFile);
    Assert.assertFalse(bRet);
    sleep(MAX_STAT_TIME);
    Assert.assertTrue(getOperTimes(sessionId, 1) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 1) == 0);
    Assert.assertTrue(getFileSize(sessionId, 1) == 0);
    tfsManager.destroy();

    log.info(caseName + "===> end");
  }
  @Test
    public void Function_04_rmFile_6G()  {
      caseName = "Function_04_rmFile_6G";
      log.info(caseName + "===> start");

      boolean bRet = false;
      String localFile = "6G.jpg";
      String filePath = "/daodaoanan";
      long expectSize = (long) (6 * ((long) 1 << 30));
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
      log.debug(" oldUsedCapacity: " + oldUsedCapacity + " oldFileCount: " + oldFileCount);

      bRet=tfsManager.saveFile(appId,userId,localFile, filePath);
      Assert.assertTrue(bRet);

      sleep(MAX_STAT_TIME);
      String sessionId = tfsManager.getSessionId();
      Assert.assertEquals(expectSize, getFileSize(sessionId, 2));
      long newUsedCapacity = getUsedCapacity(appKey);
      long newFileCount = getFileCount(appKey);
      log.debug(" newUsedCapacity: " + newUsedCapacity + " newFileCount: " + newFileCount);
      tfsManager.destroy();

      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(appKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);

      bRet = tfsManager.rmFile(appId,userId,filePath);
      Assert.assertTrue(bRet);

      sleep(MAX_STAT_TIME);
      tfsManager.destroy();

      log.info(caseName + "===> end");

    }
}
