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
public class mergeRcMetaManager_00_rc_login_operation extends RcBaseCase {
  
  @Before
  public void setUp(){
  
  }
  
  @After
  public void tearDown(){
  }
  
  @Test
  public void Function_01_init_with_client_restart(){

    caseName = "Function_01_init_with_client_restart";
    log.info(caseName + "===> start");

    boolean bRet = false;
 //   List<String> rootServerAddrList = new ArrayList<String>();
 //   rootServerAddrList.add(rsAddr);

    tfsManager = new DefaultTfsManager();
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    sleep(MAX_STAT_TIME);
    String sessionId = tfsManager.getSessionId();
    log.debug("@@ sessionId: " + sessionId);
    Assert.assertTrue(checkSessionId(sessionId));
    tfsManager.destroy();

    tfsManager = new DefaultTfsManager();
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    sleep(MAX_STAT_TIME);
    String sessionId2 = tfsManager.getSessionId();
    Assert.assertFalse(sessionId.equals(sessionId2));
    Assert.assertTrue(checkSessionId(sessionId2));
    tfsManager.destroy();

    log.info(caseName + "===> end");

  }
  
  @Test
  public void Function_02_init_several_clients_with_same_key(){

    caseName = "Function_02_init_several_clients_with_same_key";
    log.info(caseName + "===> start");

    boolean bRet = false;
//    List<String> rootServerAddrList = new ArrayList<String>();
//    rootServerAddrList.add(rsAddr);

    tfsManager = new DefaultTfsManager();
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    sleep(MAX_STAT_TIME);
    String sessionId = tfsManager.getSessionId();
    Assert.assertTrue(checkSessionId(sessionId));

    DefaultTfsManager tfsManager2 = new DefaultTfsManager();
    tfsManager2.setRcAddr(rcAddr);
    tfsManager2.setAppKey(appKey);
    tfsManager2.setAppIp(appIp);
    bRet = tfsManager2.init();
    Assert.assertTrue(bRet);
    sleep(MAX_STAT_TIME);
    String sessionId2 = tfsManager2.getSessionId();
    Assert.assertTrue(checkSessionId(sessionId2));

    System.out.println("sessionId 1:" + sessionId);
    System.out.println("sessionId 2:" + sessionId2);
    Assert.assertFalse(sessionId.equals(sessionId2));

    tfsManager.destroy();
    tfsManager2.destroy();
    log.info(caseName + "===> end");
  }
  
  @Test
  public void Function_03_init_a_client_with_same_key(){

    caseName = "Function_03_init_a_client_with_same_key";
    log.info(caseName + "===> start");

    boolean bRet = false;
//    List<String> rootServerAddrList = new ArrayList<String>();
//    rootServerAddrList.add(rsAddr);

    tfsManager = new DefaultTfsManager();
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    String sessionId = tfsManager.getSessionId();

    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet); // with log info: rc manager is already inited
    String sessionId2 = tfsManager.getSessionId();

    Assert.assertTrue(sessionId.equals(sessionId2));
    log.info(caseName + "===> end");
  }
  
  @Test
  public void Function_04_init_a_client_with_different_key(){

    caseName = "Function_04_init_a_client_with_different_key";
    log.info(caseName + "===> start");

    boolean bRet = false;
 //   List<String> rootServerAddrList = new ArrayList<String>();
 //   rootServerAddrList.add(rsAddr);

    tfsManager = new DefaultTfsManager();
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet);
    String sessionId = tfsManager.getSessionId();

    String appKey2 = "tappkey00002";
    tfsManager.setRcAddr(rcAddr);
    tfsManager.setAppKey(appKey2);
    tfsManager.setAppIp(appIp);
    bRet = tfsManager.init();
    Assert.assertTrue(bRet); // with log info: rc manager is already inited
    String sessionId2 = tfsManager.getSessionId();

    Assert.assertTrue(sessionId.equals(sessionId2));

    log.info(caseName + "===> end");
  }

}
