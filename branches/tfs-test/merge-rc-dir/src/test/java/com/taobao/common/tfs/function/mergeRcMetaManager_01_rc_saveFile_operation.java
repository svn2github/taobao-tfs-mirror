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
public class mergeRcMetaManager_01_rc_saveFile_operation extends RcBaseCase {
  
  @Before
  public void setUp(){
  
  }
  
  @After
  public void tearDown(){
  }
  
  @Ignore
  public void Function_05_saveFile_1k(){

    caseName = "Function_05_saveFile_1k";
    log.info(caseName + "===> start");

    String localFile = "1k.jpg";
    boolean bRet = false;
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
    log.debug("sessionId: " + sessionId);
    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
    Assert.assertTrue(getFileSize(sessionId, 2) == 1024);

    long newUsedCapacity = getUsedCapacity(appKey);
    long newFileCount = getFileCount(appKey);
    Assert.assertEquals(oldUsedCapacity + 1024, newUsedCapacity);
    Assert.assertEquals(oldFileCount + 1, newFileCount);
    tfsManager.destroy();

    int savecrc = 0;
    int readcrc = 1;
    savecrc = getCrc(localFile);
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    sessionId = tfsManager.getSessionId();
    bRet = tfsManager.fetchFile(sRet, null, retLocalFile);
    Assert.assertTrue(bRet);
    sleep(MAX_STAT_TIME);
    Assert.assertTrue(getOperTimes(sessionId, 1) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 1) == 1);
    Assert.assertTrue(getFileSize(sessionId, 1) == 1024);
    readcrc = getCrc(retLocalFile);
    Assert.assertEquals(savecrc,readcrc);
    tfsManager.destroy();

    log.info(caseName + "===> end");
  }
  
  @Ignore
  public void Function_06_saveFile_2M(){

    caseName = "Function_06_saveFile_2M";
    log.info(caseName + "===> start");

    String localFile = "2M.jpg";
    boolean bRet = false;
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
    log.debug("@ sessionId: " + sessionId);
    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
    Assert.assertTrue(getFileSize(sessionId, 2) == 2*1024*1024);

    long newUsedCapacity = getUsedCapacity(appKey);
    long newFileCount = getFileCount(appKey);
    Assert.assertEquals(oldUsedCapacity + 2*1024*1024, newUsedCapacity);
    Assert.assertEquals(oldFileCount + 1, newFileCount);
    tfsManager.destroy();

    int savecrc = 0;
    int readcrc = 1;
    savecrc = getCrc(localFile);
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    bRet = tfsManager.fetchFile(sRet, null, retLocalFile);
    Assert.assertTrue(bRet);
    sleep(MAX_STAT_TIME);
    sessionId = tfsManager.getSessionId();
    log.debug("@ sessionId: " + sessionId);
    Assert.assertTrue(getOperTimes(sessionId, 1) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 1) == 1);
    Assert.assertTrue(getFileSize(sessionId, 1) == 2*1024*1024);
    readcrc =  getCrc(retLocalFile);

    Assert.assertEquals(savecrc,readcrc);
    tfsManager.destroy();

    log.info(caseName + "===> end");
  }

  @Ignore
  public void Function_07_saveFile_3M(){

    caseName = "Function_07_saveFile_3M";
    log.info(caseName + "===> start");

    String localFile = "3M.jpg";
    boolean bRet = false;
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
    log.debug("@ sessionId: " + sessionId);
    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
    Assert.assertTrue(getFileSize(sessionId, 2) == 3*1024*1024);

    long newUsedCapacity = getUsedCapacity(appKey);
    long newFileCount = getFileCount(appKey);
    Assert.assertEquals(oldUsedCapacity + 3*1024*1024, newUsedCapacity);
    Assert.assertEquals(oldFileCount + 1, newFileCount);
    tfsManager.destroy();

    int savecrc = 0;
    int readcrc = 1;
    savecrc = getCrc(localFile);
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    bRet = tfsManager.fetchFile(sRet, null, retLocalFile);
    Assert.assertTrue(bRet);
    sleep(MAX_STAT_TIME);
    sessionId = tfsManager.getSessionId();
    Assert.assertTrue(getOperTimes(sessionId, 1) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 1) == 1);
    Assert.assertTrue(getFileSize(sessionId, 1) == 3*1024*1024);
    readcrc =  getCrc(retLocalFile);

    Assert.assertEquals(savecrc,readcrc);
    tfsManager.destroy();

    log.info(caseName + "===> end");
  }

  @Ignore
  public void Function_08_saveFile_6G(){

    caseName = "Function_08_saveFile_6G";
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
    Assert.assertEquals(1, getOperTimes(sessionId, 2));
    Assert.assertEquals(1, getSuccTimes(sessionId, 2));
    Assert.assertEquals(expectSize, getFileSize(sessionId, 2));

    long newUsedCapacity = getUsedCapacity(appKey);
    long newFileCount = getFileCount(appKey);
    Assert.assertEquals(oldUsedCapacity + expectSize, newUsedCapacity);
    Assert.assertEquals(oldFileCount + 1, newFileCount);
    tfsManager.destroy();

    int savecrc = 0;
    int readcrc = 1;
    savecrc = getCrc(localFile);
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    bRet = tfsManager.fetchFile(sRet, null, retLocalFile);
    Assert.assertTrue(bRet);
    sleep(MAX_STAT_TIME);
    sessionId = tfsManager.getSessionId();
    Assert.assertEquals(1, getOperTimes(sessionId, 1));
    Assert.assertEquals(1, getSuccTimes(sessionId, 1));
    Assert.assertEquals(expectSize, getFileSize(sessionId, 1));
    readcrc =  getCrc(retLocalFile);

    Assert.assertEquals(savecrc,readcrc);
    tfsManager.destroy();

    log.info(caseName + "===> end");
  }

  @Ignore
  public void Function_13_update_file_2M_to_3M(){

    caseName = "Function_13_update_file_2M_to_3M";
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
    sRet=tfsManager.saveFile(localFile,null,null, false);
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

    localFile = "3M.jpg";
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    String new_sRet = null;
    new_sRet=tfsManager.saveFile(localFile, sRet, null, false);
    Assert.assertEquals(new_sRet, sRet);
    sleep(MAX_STAT_TIME);
    sessionId = tfsManager.getSessionId();
    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
    Assert.assertTrue(getFileSize(sessionId, 2) == 3*1024*1024);
    newUsedCapacity = getUsedCapacity(appKey);
    newFileCount = getFileCount(appKey);
/*
  should be oldUsedCapacity + 3*1024*1024 and oldFileCount + 1
*/
    Assert.assertEquals(oldUsedCapacity + 5*1024*1024, newUsedCapacity);
    Assert.assertEquals(oldFileCount + 2, newFileCount);
    bRet = tfsManager.fetchFile(new_sRet, null, retLocalFile);
    Assert.assertTrue(bRet);
    int savecrc = 0;
    int readcrc = 1;
    savecrc = getCrc(localFile);
    readcrc =  getCrc(retLocalFile);
    Assert.assertEquals(savecrc,readcrc);
    tfsManager.destroy();

    log.info(caseName + "===> end");
  }


  @Ignore
  public void Function_14_update_file_3M_to_2M(){

    caseName = "Function_14_update_file_3M_to_2M";
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

    localFile = "2M.jpg";
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    String new_sRet = null;
    new_sRet=tfsManager.saveFile(localFile, sRet, null, false);
    Assert.assertEquals(new_sRet, sRet);
    sleep(MAX_STAT_TIME);
    sessionId = tfsManager.getSessionId();
    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
    Assert.assertTrue(getFileSize(sessionId, 2) == 2*1024*1024);
    newUsedCapacity = getUsedCapacity(appKey);
    newFileCount = getFileCount(appKey);
/*
  should be oldUsedCapacity + 3*1024*1024 and oldFileCount + 1
*/
    Assert.assertEquals(oldUsedCapacity + 5*1024*1024, newUsedCapacity);
    Assert.assertEquals(oldFileCount + 2, newFileCount);
    bRet = tfsManager.fetchFile(new_sRet, null, retLocalFile);
    Assert.assertTrue(bRet);
    int savecrc = 0;
    int readcrc = 1;
    savecrc = getCrc(localFile);
    readcrc =  getCrc(retLocalFile);
    Assert.assertEquals(savecrc,readcrc);
    tfsManager.destroy();

    log.info(caseName + "===> end");
  }

  @Test
  public void Function_08_saveMetaFile_1k(){

    caseName = "Function_08_saveMetaFile_1k";
    log.info(caseName + "===> start");

    String localFile = "1k.jpg";
    boolean bRet = false;
    List<String> rootServerAddrList = new ArrayList<String>();
    rootServerAddrList.add(rsAddr);

    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    log.debug("oldUsedCapacity: " + oldUsedCapacity + ", oldFileCount: " + oldFileCount);

    tfsManager = new DefaultTfsManager();
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    tfsManager.setUseNameMeta(true);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);

    boolean sRet = false;
    sRet=tfsManager.saveFile(appId, userId, localFile, "/cymetatest_new");
    Assert.assertTrue(sRet);
    sleep(MAX_STAT_TIME);

    String sessionId = tfsManager.getSessionId();
    log.debug("sessionId: " + sessionId);
    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
    Assert.assertTrue(getFileSize(sessionId, 2) == 1024);
    //tfsManager.rmFile(appId, userId, "/cymetatest_yes");

    long newUsedCapacity = getUsedCapacity(appKey);
    long newFileCount = getFileCount(appKey);
    log.debug("newUsedCapacity: " + newUsedCapacity + ", newFileCount: " + newFileCount);
    Assert.assertEquals(oldUsedCapacity + 1024, newUsedCapacity);
    Assert.assertEquals(oldFileCount + 1, newFileCount);
    //tfsManager.destroy();

    //int savecrc = 0;
    //int readcrc = 1;
    //savecrc = getCrc(localFile);
    //tfsManager.setRcAddr(rcAddr);
    //tfsManager.setAppKey(appKey);
    //tfsManager.setAppIp(appIp);
    //bRet = tfsManager.init();
    //Assert.assertTrue(bRet);
    //sessionId = tfsManager.getSessionId();
    //bRet = tfsManager.fetchFile(sRet, null, retLocalFile);
    //Assert.assertTrue(bRet);
    //sleep(MAX_STAT_TIME);
    //Assert.assertTrue(getOperTimes(sessionId, 1) == 1);
    //Assert.assertTrue(getSuccTimes(sessionId, 1) == 1);
    //Assert.assertTrue(getFileSize(sessionId, 1) == 1024);
    //readcrc = getCrc(retLocalFile);
    //Assert.assertEquals(savecrc,readcrc);
    tfsManager.destroy();

    log.info(caseName + "===> end");
  }

}
