package com.taobao.common.tfs.function;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.taobao.common.tfs.DefaultTfsManager;

public class mergeRcMetaManager_01_meta_saveFile_operation extends RcBaseCase 
{
	  @Before
	  public void setUp()
	  {
            System.out.println("!!!!!!");
	  }  
	  @After
	  public void tearDown()
	  {
	  }
	  
	  @Test
	  public void Function_01_saveFile_1k()
	  {
	    caseName = "Function_01_saveFile_1k";
	    log.info(caseName + "===> start");
            System.out.println("!!!!!!");

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
            tfsManager.setUseNameMeta(true);
	    tfsManager.setRootServerAddrList(rootServerAddrList);
	    bRet = tfsManager.init();
	    Assert.assertTrue(bRet);
	   
	    bRet=tfsManager.saveFile(appId,userId,localFile,"/1k_yes_3");
	    Assert.assertTrue(bRet);
	    
	    sleep(MAX_STAT_TIME);
	    String sessionId = tfsManager.getSessionId();
	    log.debug("sessionId: " + sessionId);
	  //  Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
	   // Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
	    Assert.assertTrue(getFileSize(sessionId, 2) == 1024);

	    long newUsedCapacity = getUsedCapacity(appKey);
	    long newFileCount = getFileCount(appKey);
	    Assert.assertEquals(oldUsedCapacity + 1024, newUsedCapacity);
	   // Assert.assertEquals(oldFileCount + 1, newFileCount);
	    tfsManager.destroy();

	    int savecrc = 0;
	    int readcrc = 1;
	    savecrc = getCrc(localFile);
	    tfsManager.setRcAddr(rcAddr);
	    tfsManager.setAppKey(appKey);
	    tfsManager.setAppIp(appIp);
            tfsManager.setUseNameMeta(true);
	    tfsManager.setRootServerAddrList(rootServerAddrList);
	    bRet = tfsManager.init();
	    Assert.assertTrue(bRet);
	    sessionId = tfsManager.getSessionId();
	    
	    bRet = tfsManager.fetchFile(appId,userId,"TEMP","/1k_yes_3");
	    Assert.assertTrue(bRet);
	    
	    sleep(MAX_STAT_TIME);
	   // Assert.assertTrue(getOperTimes(sessionId, 1) == 1);
	   // Assert.assertTrue(getSuccTimes(sessionId, 1) == 1);
	    Assert.assertTrue(getFileSize(sessionId, 1) == 1024);
	    readcrc = getCrc(retLocalFile);
	    tfsManager.destroy();

	    log.info(caseName + "===> end");
	  }
	  
	  @Test
	  public void Function_02_saveFile_2M()
	  {
	    caseName = "Function_02_saveFile_2M";
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
            tfsManager.setUseNameMeta(true);
	    tfsManager.setRootServerAddrList(rootServerAddrList);
	    bRet = tfsManager.init();
	    Assert.assertTrue(bRet);
	    
	   
	    bRet=tfsManager.saveFile(appId,userId,localFile,"/test_2M");
	    Assert.assertTrue(bRet);
	    
	    sleep(MAX_STAT_TIME);
	    String sessionId = tfsManager.getSessionId();
	    log.debug("@ sessionId: " + sessionId);
	  //  Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
	   // Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
	    Assert.assertTrue(getFileSize(sessionId, 2) == 2*1024*1024);

	    long newUsedCapacity = getUsedCapacity(appKey);
	    long newFileCount = getFileCount(appKey);
	    Assert.assertEquals(oldUsedCapacity + 2*1024*1024, newUsedCapacity);
	   // Assert.assertEquals(oldFileCount + 1, newFileCount);
	    tfsManager.destroy();

	    int savecrc = 0;
	    int readcrc = 1;
	    savecrc = getCrc(localFile);
	    tfsManager.setRcAddr(rcAddr);
	    tfsManager.setAppKey(appKey);
	    tfsManager.setAppIp(appIp);
            tfsManager.setUseNameMeta(true);	   
            tfsManager.setRootServerAddrList(rootServerAddrList);
	    bRet = tfsManager.init();
	    Assert.assertTrue(bRet);
	    
	    bRet = tfsManager.fetchFile(appId,userId,"TEMP","/test_2M");
	    Assert.assertTrue(bRet);
	    
	    sleep(MAX_STAT_TIME);
	    sessionId = tfsManager.getSessionId();
	    log.debug("@ sessionId: " + sessionId);
	   // Assert.assertTrue(getOperTimes(sessionId, 1) == 1);
	   // Assert.assertTrue(getSuccTimes(sessionId, 1) == 1);
	    Assert.assertTrue(getFileSize(sessionId, 1) == 2*1024*1024);
	    readcrc =  getCrc(retLocalFile);

	   // Assert.assertEquals(savecrc,readcrc);
	    tfsManager.destroy();

	    log.info(caseName + "===> end");
	  }

	  @Test
	  public void Function_03_saveFile_3M()
	  {
	    caseName = "Function_03_saveFile_3M";
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
	    tfsManager.setUseNameMeta(true);
            tfsManager.setRootServerAddrList(rootServerAddrList);
	    bRet = tfsManager.init();
	    Assert.assertTrue(bRet); 
	    bRet=tfsManager.saveFile(appId,userId,localFile,"/test_3M4");
	    Assert.assertTrue(bRet);
	    
	    sleep(MAX_STAT_TIME);
	    String sessionId = tfsManager.getSessionId();
	    log.debug("@ sessionId: " + sessionId);
	   // Assert.assertTrue(getOperTimes(sessionId, 2) == 2);
	   // Assert.assertTrue(getSuccTimes(sessionId, 2) == 2);
	    Assert.assertTrue(getFileSize(sessionId, 2) == 3*1024*1024);

	    long newUsedCapacity = getUsedCapacity(appKey);
	    long newFileCount = getFileCount(appKey);
	    Assert.assertEquals(oldUsedCapacity + 3*1024*1024, newUsedCapacity);
	 //   Assert.assertEquals(oldFileCount + 2, newFileCount);
	    tfsManager.destroy();

	    int savecrc = 0;
	    int readcrc = 1;
	    savecrc = getCrc(localFile);
	    tfsManager.setRcAddr(rcAddr);
	    tfsManager.setAppKey(appKey);
	    tfsManager.setAppIp(appIp);
	    tfsManager.setUseNameMeta(true);
            tfsManager.setRootServerAddrList(rootServerAddrList);
	    bRet = tfsManager.init();
	    Assert.assertTrue(bRet);
	    bRet = tfsManager.fetchFile(appId,userId,"TEMP","/test_3M4");
	    Assert.assertTrue(bRet);
	    
	    sleep(MAX_STAT_TIME);
	    sessionId = tfsManager.getSessionId();
	   // Assert.assertTrue(getOperTimes(sessionId, 1) == 2);
	 //   Assert.assertTrue(getSuccTimes(sessionId, 1) == 2);
	    Assert.assertTrue(getFileSize(sessionId, 1) == 3*1024*1024);
	    readcrc =  getCrc(retLocalFile);

	    tfsManager.destroy();

	    log.info(caseName + "===> end");
	  }

	  @Test
	  public void Function_04_saveFile_6G()
	  {
	    caseName = "Function_04_saveFile_6G";
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
	    tfsManager.setUseNameMeta(true);
            tfsManager.setRootServerAddrList(rootServerAddrList);
	    bRet = tfsManager.init();
	    Assert.assertTrue(bRet);
	    
	    bRet=tfsManager.saveFile(appId,userId,localFile,"/test_6G22");
	    Assert.assertTrue(bRet);
	    
	    sleep(MAX_STAT_TIME*5);
	    String sessionId = tfsManager.getSessionId();
		System.out.println("sessionId is :" + sessionId);
	//    Assert.assertEquals(1, getOperTimes(sessionId, 2));
	  //  Assert.assertEquals(1, getSuccTimes(sessionId, 2));
	      Assert.assertEquals(expectSize, getFileSize(sessionId, 2));

	    long newUsedCapacity = getUsedCapacity(appKey);
	    long newFileCount = getFileCount(appKey);
	    Assert.assertEquals(oldUsedCapacity + expectSize, newUsedCapacity);
	   // Assert.assertEquals(oldFileCount + 1, newFileCount);
	    tfsManager.destroy();

	    int savecrc = 0;
	    int readcrc = 1;
	    savecrc = getCrc(localFile);
	    tfsManager.setRcAddr(rcAddr);
	    tfsManager.setAppKey(appKey);
	    tfsManager.setAppIp(appIp);
            tfsManager.setUseNameMeta(true);
	    tfsManager.setRootServerAddrList(rootServerAddrList);
	    bRet = tfsManager.init();
	    Assert.assertTrue(bRet);
	    
	    bRet = tfsManager.fetchFile(appId,userId,"TEMP","/test_6G22");
	    Assert.assertTrue(bRet);
	    
	    sleep(MAX_STAT_TIME*5);

	    sessionId = tfsManager.getSessionId();
//            system.out.println("getOperTimes()"+getOperTimes);
	   // Assert.assertEquals(1, getOperTimes(sessionId, 1));
	   // Assert.assertEquals(1, getSuccTimes(sessionId, 1));
	    Assert.assertEquals(expectSize, getFileSize(sessionId, 1));
	    readcrc =  getCrc(retLocalFile);
	    tfsManager.destroy();

	    log.info(caseName + "===> end");
	  }
}
