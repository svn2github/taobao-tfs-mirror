/**
 * 
 */
package com.taobao.tfstest;

import java.util.HashMap;
import java.util.ArrayList;
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
public class Function_rc_test extends RcBaseCase {

  private DefaultTfsManager tfsManager = null;
  private static String appKey = "tappkey00003";
  private static String rcAddr = "10.232.36.203:6100";
  private static String appIp = "10.232.36.203";
  private static int MAX_UPDATE_TIME = 10;
  private static int MAX_STAT_TIME = 10;
  private static int INVALID_MODE = 0;
  private static int R_MODE = 1;
  private static int RW_MODE = 2;
  private static int RC_VALID_MODE = 1;
  private static int RC_INVALID_MODE = 2;

  private static int WITHOUT_DUPLICATE = 0;
  private static int WITH_DUPLICATE = 1;
  
  private static int RC_DEFAULT_INDEX= 0;
  private static int RC_NEW_INDEX= 1;
  private static int RC_INVALID_INDEX = 2;
  
  private static String retLocalFile = "/home/admin/tmp";
  private static ArrayList<String> testFileList = new ArrayList<String>(); 
  
  @BeforeClass
  public static void BeforeSetup(){
    
    String localFile = ""; 
    // 1b file
    localFile = "1b.jpg";
    if (createFile(localFile, 1))
    {
      testFileList.add(localFile);
    }
    // 1k file
    localFile = "1k.jpg";
    if (createFile(localFile, 1 << 10))
    {
      testFileList.add(localFile);
    }
    // 2M file
    localFile = "2M.jpg";
    if (createFile(localFile, 2 * (1 << 20)))
    {
      testFileList.add(localFile);
    }
    // 3M file
    localFile = "3M.jpg";
    if (createFile(localFile, 3 * (1 << 20)))
    {
      testFileList.add(localFile);
    }
    // 100M file
    localFile = "100M.jpg";
    if (createFile(localFile, 100 * (1 << 20)))
    {
      testFileList.add(localFile);
    }
    // 1G file
    localFile = "1G.jpg";
    if (createFile(localFile, (long) (1 << 30)))
    {
      testFileList.add(localFile);
    }
    // 6G file
    localFile = "6G.jpg";
    if (createFile(localFile, (long) (6 * ((long) 1 << 30))))
    {
      testFileList.add(localFile);
    }
  }
  
  @AfterClass
  public static void AfterTearDown(){
  
    int size = testFileList.size();
    for (int i = 0; i < size; i++)
    {
      File file = new File(testFileList.get(i));
      if(file.exists()){
        file.delete();
      }
    }
  }
  
  @Before
  public void setUp(){
  
    tfsManager= new DefaultTfsManager();
    ClassPathXmlApplicationContext appContext =
    	new ClassPathXmlApplicationContext(new String[] { "tfs.rc.xml" }); 
    System.out.println("@@@@@@@@@@@get bean begin");
    tfsManager = (DefaultTfsManager) appContext.getBean("tfsManager");
  
  }
  
  @After
  public void tearDown(){
    tfsManager.destroy();
  }
  
  @Test
  public void Function_01_init_with_client_restart(){

    caseName = "Function_01_init_with_client_restart";
    log.info(caseName + "===> start");

    boolean bRet = false;
    tfsManager = new DefaultTfsManager();
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
    Assert.assertTrue(bRet);
    sleep(MAX_STAT_TIME);
    String sessionId = tfsManager.getSessionId();
    Assert.assertTrue(checkSessionId(sessionId));
    tfsManager.destroy();

    tfsManager = new DefaultTfsManager();
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
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
    tfsManager = new DefaultTfsManager();
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
    Assert.assertTrue(bRet);
    sleep(MAX_STAT_TIME);
    String sessionId = tfsManager.getSessionId();
    Assert.assertTrue(checkSessionId(sessionId));

    DefaultTfsManager tfsManager2 = new DefaultTfsManager();
    bRet = tfsManager2.init(rcAddr, appKey, appIp, 600000, 10000);
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
    tfsManager = new DefaultTfsManager();
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
    Assert.assertTrue(bRet);
    String sessionId = tfsManager.getSessionId();

    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
    Assert.assertFalse(bRet);
    String sessionId2 = tfsManager.getSessionId();

    Assert.assertTrue(sessionId.equals(sessionId2));
    log.info(caseName + "===> end");
  }
  
  @Test
  public void Function_04_init_a_client_with_different_key(){

    caseName = "Function_04_init_a_client_with_different_key";
    log.info(caseName + "===> start");

    boolean bRet = false;
    tfsManager = new DefaultTfsManager();
    String appKey2 = "tappkey00002";
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
    Assert.assertTrue(bRet);
    String sessionId = tfsManager.getSessionId();

    bRet = tfsManager.init(rcAddr, appKey2, appIp, 600000, 10000);
    Assert.assertFalse(bRet);
    String sessionId2 = tfsManager.getSessionId();

    Assert.assertTrue(sessionId.equals(sessionId2));

    log.info(caseName + "===> end");
  }
  
  @Test
  public void Function_05_saveFile_1k(){

    caseName = "Function_05_saveFile_1k";
    log.info(caseName + "===> start");

    String localFile = "1k.jpg";
    boolean bRet = false;

    tfsManager = new DefaultTfsManager();
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
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

    int savecrc = 0;
    int readcrc = 1;
    savecrc = getCrc(localFile);
    bRet = tfsManager.init(rcAddr, appKey, appIp, 800000, 20000);
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
  
  @Test
  public void Function_06_saveFile_2M(){

    caseName = "Function_06_saveFile_2M";
    log.info(caseName + "===> start");

    String localFile = "2M.jpg";
    boolean bRet = false;

    tfsManager = new DefaultTfsManager();
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
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
    bRet = tfsManager.init(rcAddr, appKey, appIp, 800000, 20000);
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

  @Test
  public void Function_07_saveFile_3M(){

    caseName = "Function_07_saveFile_3M";
    log.info(caseName + "===> start");

    String localFile = "3M.jpg";
    boolean bRet = false;

    tfsManager = new DefaultTfsManager();
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
    Assert.assertTrue(bRet);
    String sRet = null;
    sRet=tfsManager.saveFile(localFile, null, null, false);
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

    int savecrc = 0;
    int readcrc = 1;
    savecrc = getCrc(localFile);
    bRet = tfsManager.init(rcAddr, appKey, appIp, 800000, 20000);
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

  @Test
  public void Function_08_saveFile_6G(){

    caseName = "Function_08_saveFile_6G";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "6G.jpg";
    long expectSize = (long) (6 * ((long) 1 << 30));

    tfsManager = new DefaultTfsManager();

    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
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
    bRet = tfsManager.init(rcAddr, appKey, appIp, 800000, 20000);
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

  @Test
  public void Function_09_unlinkFile_1k(){

    caseName = "Function_09_unlinkFile_1k";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "1k.jpg";

    tfsManager = new DefaultTfsManager();
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
    Assert.assertTrue(bRet);
    String sRet = null;
    sRet=tfsManager.saveFile(localFile,null,null, false);
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

    bRet = tfsManager.init(rcAddr, appKey, appIp, 800000, 20000);
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

    newUsedCapacity = getUsedCapacity(appKey);
    newFileCount = getFileCount(appKey);

    Assert.assertEquals(oldUsedCapacity, newUsedCapacity);
    Assert.assertEquals(oldFileCount, newFileCount);
    tfsManager.destroy();

    log.info(caseName + "===> end");
  }

  @Test
  public void Function_10_unlinkFile_2M(){

    caseName = "Function_10_unlinkFile_2M";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "2M.jpg";

    tfsManager = new DefaultTfsManager();
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
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

    bRet = tfsManager.init(rcAddr, appKey, appIp, 800000, 20000);
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

  @Test
  public void Function_11_unlinkFile_3M(){

    caseName = "Function_11_unlinkFile_3M";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "3M.jpg";

    tfsManager = new DefaultTfsManager();
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
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

    bRet = tfsManager.init(rcAddr, appKey, appIp, 800000, 20000);
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

  @Test
  public void Function_12_unlinkFile_6G(){

    caseName = "Function_12_unlinkFile_6G";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "6G.jpg";
    long expectSize = (long) (6 * ((long) 1 << 30));

    tfsManager = new DefaultTfsManager();
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
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

    bRet = tfsManager.init(rcAddr, appKey, appIp, 800000, 20000);
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
  public void Function_13_update_file_2M_to_3M(){

    caseName = "Function_13_update_file_2M_to_3M";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "2M.jpg";

    tfsManager = new DefaultTfsManager();
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
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
    bRet = tfsManager.init(rcAddr, appKey, appIp, 800000, 20000);
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


  @Test
  public void Function_14_update_file_3M_to_2M(){

    caseName = "Function_14_update_file_3M_to_2M";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "3M.jpg";

    tfsManager = new DefaultTfsManager();
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
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
    bRet = tfsManager.init(rcAddr, appKey, appIp, 800000, 20000);
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
  public void Function_15_operation_info_of_read(){

    caseName = "Function_14_update_file_3M_to_2M";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "100M.jpg";

    tfsManager = new DefaultTfsManager();
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
    Assert.assertTrue(bRet);
    String sRet = null;
    sRet=tfsManager.saveLargeFile(localFile,null,null);
    Assert.assertNotNull(sRet);
    sleep(MAX_STAT_TIME);
    String sessionId = tfsManager.getSessionId();
    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
    Assert.assertTrue(getFileSize(sessionId, 2) == 100*1024*1024);
    long newUsedCapacity = getUsedCapacity(appKey);
    long newFileCount = getFileCount(appKey);
    Assert.assertEquals(oldUsedCapacity + 100*1024*1024, newUsedCapacity);
    Assert.assertEquals(oldFileCount + 1, newFileCount);
    tfsManager.destroy();

    bRet = tfsManager.init(rcAddr, appKey, appIp, 800000, 20000);
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
      Assert.assertTrue(getOperTimes(sessionId, 1) == i + 1);
      Assert.assertTrue(getSuccTimes(sessionId, 1) == i + 1);
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
      Assert.assertTrue(getOperTimes(sessionId, 1) == 5 + i + 1);
      Assert.assertTrue(getSuccTimes(sessionId, 1) == 5);
      Assert.assertTrue(getFileSize(sessionId, 1) == totalReadLength);
      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 100*1024*1024, newUsedCapacity);
      Assert.assertEquals(oldFileCount + 1, newFileCount);
    }
    tfsManager.destroy();

    log.info(caseName + "===> end");
  }

  @Test
  public void Function_15_operation_info_of_writeLargeFile(){

    caseName = "Function_15_operation_info_of_writeLargeFile";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "100M.jpg";

    tfsManager = new DefaultTfsManager();
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
    Assert.assertTrue(bRet);
    String sRet = null;
    sRet=tfsManager.saveLargeFile(localFile, null, null);
    Assert.assertNotNull(sRet);
    sleep(MAX_STAT_TIME);
    String sessionId = tfsManager.getSessionId();
    Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
    Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
    Assert.assertTrue(getFileSize(sessionId, 2) == 100*1024*1024);
    long newUsedCapacity = getUsedCapacity(appKey);
    long newFileCount = getFileCount(appKey);
    Assert.assertEquals(oldUsedCapacity + 100*1024*1024, newUsedCapacity);
    Assert.assertEquals(oldFileCount + 1, newFileCount);

    DefaultTfsManager tfsManager2 = new DefaultTfsManager();
    bRet = tfsManager2.init(rcAddr, appKey, appIp, 800000, 20000);
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
      Assert.assertTrue(getOperTimes(sessionId, 2) == i + 1);
      Assert.assertTrue(getSuccTimes(sessionId, 2) == i + 1);
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

      Assert.assertTrue(getOperTimes(sessionId, 2) == 5);
      Assert.assertTrue(getSuccTimes(sessionId, 2) == 5);
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
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    long newUsedCapacity = 0; 
    long newFileCount = 0;
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
    Assert.assertTrue(bRet);
    String sessionId = tfsManager.getSessionId();
    String sRet = null;
    for(int i = 0; i < 5; i++){
      sRet = null;
      sRet = tfsManager.saveFile(localFile, null, null, false);
      Assert.assertNotNull(sRet);
      sleep(MAX_STAT_TIME);
      Assert.assertTrue(getOperTimes(sessionId, 2) == i + 1);
      Assert.assertTrue(getSuccTimes(sessionId, 2) == i + 1);
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

      Assert.assertTrue(getOperTimes(sessionId, 2) == 5);
      Assert.assertTrue(getSuccTimes(sessionId, 2) == 5);
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

    oldUsedCapacity = getUsedCapacity(appKey);
    oldFileCount = getFileCount(appKey);
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
    Assert.assertTrue(bRet);
    String sessionId = tfsManager.getSessionId();
    String []sRet = new String[5];
    for(int i = 0; i < 5; i++){
      sleep(1);
      sRet[i] = null;
      sRet[i] = tfsManager.saveFile(localFile, null, null, false);
      Assert.assertNotNull(sRet[i]);
      sleep(MAX_STAT_TIME);
      Assert.assertTrue(getOperTimes(sessionId, 2) == i + 1);
      Assert.assertTrue(getSuccTimes(sessionId, 2) == i + 1);
      Assert.assertTrue(getFileSize(sessionId, 2) == (i+1)*2*1024*1024);
      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + (i+1)*2*1024*1024, newUsedCapacity);
      Assert.assertEquals(oldFileCount + i + 1, newFileCount);
    }
    tfsManager.destroy();

    bRet = tfsManager.init(rcAddr, appKey, appIp, 800000, 20000);
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
    Assert.assertTrue(getOperTimes(sessionId, 4) == 10);
    Assert.assertTrue(getSuccTimes(sessionId, 4) == 5);
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

    tfsManager = new DefaultTfsManager();
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
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

  @Test
  public void Function_19_operation_with_duplicate_server(){

    caseName = "Function_19_operation_with_duplicate_server";
    log.info(caseName + "===> start");

    boolean bRet = false;
    String localFile = "2M.jpg";
    //String localFile = "list";
    String[] retFile = new String[5];

    setDuplicateStatus(appKey, WITH_DUPLICATE);
    sleep(MAX_UPDATE_TIME);

    tfsManager = new DefaultTfsManager();
    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
    Assert.assertTrue(bRet);
    String []sRet = new String[50];
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    //small files
    for(int i = 0; i < 5; i++){
      System.out.println("----------------------Round " + i + ":-----------------------");
      sleep(1);
      sRet[i] = null;
      sRet[i] = tfsManager.saveFile(localFile, null, null, false);
      Assert.assertNotNull(sRet[i]);
      System.out.println("tfsName:" + sRet[i]);
      retFile[i] = retLocalFile + "_" + i;
      bRet = tfsManager.fetchFile(sRet[i], null, retFile[i]);
      Assert.assertTrue(bRet);
      sleep(MAX_STAT_TIME);
      long newUsedCapacity = getUsedCapacity(appKey);
      long newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity + 2*1024*1024, newUsedCapacity);
      Assert.assertEquals(oldFileCount + 1, newFileCount);
      bRet = tfsManager.unlinkFile(sRet[i], null);
      Assert.assertTrue(bRet);
      sleep(MAX_STAT_TIME);
      newUsedCapacity = getUsedCapacity(appKey);
      newFileCount = getFileCount(appKey);
      Assert.assertEquals(oldUsedCapacity, newUsedCapacity);
      Assert.assertEquals(oldFileCount, newFileCount);
    }
    Assert.assertTrue(sRet[0].equals(sRet[4]));
    int crc1 = getCrc(retFile[0]);
    int crc2 = getCrc(retFile[4]);
    Assert.assertEquals(crc1, crc2);
    sleep(MAX_STAT_TIME);
    long newUsedCapacity = getUsedCapacity(appKey);
    long newFileCount = getFileCount(appKey);
    Assert.assertEquals(oldUsedCapacity, newUsedCapacity);
    Assert.assertEquals(oldFileCount, newFileCount);
    tfsManager.destroy();

    setDuplicateStatus(appKey, WITHOUT_DUPLICATE);
    sleep(MAX_UPDATE_TIME);

    log.info(caseName + "===> end");
  }

  @Test
  public void Function_20_operation_with_duplicate_server_addr_changing(){

    caseName = "Function_20_operation_with_duplicate_server_addr_changing";
    log.info(caseName + "===> start");

    setDuplicateStatus(appKey, WITH_DUPLICATE);
    sleep(MAX_UPDATE_TIME);

    tfsManager = new DefaultTfsManager();
    boolean bRet = false;
    String localFile = "2M.jpg";
    String[] retFile = new String[50];

    bRet = tfsManager.init(rcAddr, appKey, appIp, 600000, 10000);
    Assert.assertTrue(bRet);
    System.out.println("+++++++++++session_id:" + tfsManager.getSessionId());
    long oldUsedCapacity = getUsedCapacity(appKey);
    long oldFileCount = getFileCount(appKey);
    String []sRet = new String[50];
    //small files
    int saveTimes = 50;
    for(int i = 0; i < saveTimes; i++){
      System.out.println("----------------------Round " + i + ":-----------------------");
      sleep(1);
      sRet[i] = null;
      sRet[i] = tfsManager.saveFile(localFile, null, null);
      Assert.assertNotNull(sRet[i]);
      System.out.println("+++++++++++++++++++" + sRet[i]);
      retFile[i] = retLocalFile + "_" + i;
      bRet = tfsManager.fetchFile(sRet[i], null, retFile[i]);
      Assert.assertTrue(bRet);
      if(i == 20){
        boolean tempRet = changeDuplicateServerAddr(5, "10.232.12.141:5198;10.232.12.142:5198;group_1;1");
        Assert.assertTrue(tempRet);
      }
    }
    for(int i = 0; i < saveTimes; i++){
      bRet = tfsManager.unlinkFile(sRet[i], null);
      Assert.assertTrue(bRet);
    }
    boolean tempRet = changeDuplicateServerAddr(5, "10.232.16.24:5198;10.232.16.25:5198;group_1;1");
    Assert.assertTrue(tempRet);
    Assert.assertTrue(sRet[0].equals(sRet[3]));
    int crc1 = getCrc(retFile[0]);
    int crc2 = getCrc(retFile[30]);
    Assert.assertEquals(crc1, crc2);
    System.out.println("+++++++++++session_id:" + tfsManager.getSessionId());
    sleep(MAX_STAT_TIME);
    long newUsedCapacity = getUsedCapacity(appKey);
    long newFileCount = getFileCount(appKey);
    Assert.assertEquals(oldUsedCapacity, newUsedCapacity);
    Assert.assertEquals(oldFileCount, newFileCount);
    tfsManager.destroy();

    setDuplicateStatus(appKey, WITHOUT_DUPLICATE);
    sleep(MAX_UPDATE_TIME);

    log.info(caseName + "===> end");
  }

	@Test
        public void Function_01_quote_less_than_max(){

          caseName = "Function_01_quote_less_than_max";
          log.info(caseName + "===> start");

          resetCurQuote(appKey);
	  boolean bRet = false;
	  long max_quote = getMaxQuote(appKey);
	  long old_quote = 0;
	  long new_quote = 0;
          long expect_quote = 0;
          long actual_quote = 0;
	  String localFile = "";
          ArrayList<String> name_list = new ArrayList<String>(); 

          // small file
          localFile = "3M.jpg";
	  old_quote = getCurrentQuote(appKey);
	  for (int i = 0; i < 10; i++)
	  {
	  	String tfsname = tfsManager.saveFile(localFile, null, null);
	  	if (tfsname == null)
	  	{
	  		log.warn("save small file failed");
	  		break;
	  	}
	  	log.debug("@@ tfsname: " + tfsname);
                name_list.add(tfsname);
	  }
	  sleep(MAX_STAT_TIME);
	  new_quote = getCurrentQuote(appKey);
	  log.debug("max_quote: " + max_quote + ", old_quote: " + old_quote + ", new_quote: " + new_quote);

	  bRet = (new_quote < max_quote);
	  Assert.assertTrue(bRet);
          expect_quote = (long) 30*1024*1024;
          actual_quote = (new_quote - old_quote);
          log.debug("@@ expect quote: " + expect_quote + ", actual quote: " + actual_quote);
	  Assert.assertEquals(expect_quote, actual_quote);

          old_quote = new_quote;
	  for (int i = 0; i < 5; i++)
	  {
	    boolean ret = tfsManager.unlinkFile(name_list.get(i), null);
            if (!ret)
            {
              log.warn("unlinkFile " + name_list.get(i) + " failed.");
              break;
            }
	  }
	  sleep(MAX_STAT_TIME);
	  new_quote = getCurrentQuote(appKey);
	  log.debug("max_quote: " + max_quote + ", old_quote: " + old_quote + ", new_quote: " + new_quote);

	  bRet = (new_quote < max_quote);
	  Assert.assertTrue(bRet);
          expect_quote = (long) 30*1024*1024;
          actual_quote = (new_quote - old_quote);
          log.debug("@@ expect quote: " + expect_quote + ", actual quote: " + actual_quote);
	  Assert.assertEquals(expect_quote, actual_quote);

          // large file
          localFile = "1G.jpg";
	  old_quote = getCurrentQuote(appKey);
	  for (int i = 0; i < 4; i++)
	  {
	  	String tfsname = tfsManager.saveLargeFile(localFile, null, null);
	  	if (tfsname == null)
	  	{
	  		log.warn("save large file failed");
	  		break;
	  	}
	  	log.debug("@@ tfsname: " + tfsname);
                name_list.add(tfsname);
	  }
	  sleep(MAX_STAT_TIME);
	  new_quote = getCurrentQuote(appKey);
	  log.debug("max_quote: " + max_quote + ", old_quote: " + old_quote + ", new_quote: " + new_quote);

	  bRet = (new_quote < max_quote);
	  Assert.assertTrue(bRet);
          expect_quote = (long) 4*1024*1024*1024;
          actual_quote = (new_quote - old_quote);
          log.debug("@@ expect quote: " + expect_quote + ", actual quote: " + actual_quote);
	  Assert.assertEquals(expect_quote, actual_quote);

          old_quote = new_quote;
	  for (int i = 0; i < 2; i++)
	  {
	    boolean ret = tfsManager.unlinkFile(name_list.get(i), null);
            if (!ret)
            {
              log.warn("unlinkFile " + name_list.get(i) + " failed.");
              break;
            }
	  }
	  sleep(MAX_STAT_TIME);
	  new_quote = getCurrentQuote(appKey);
	  log.debug("max_quote: " + max_quote + ", old_quote: " + old_quote + ", new_quote: " + new_quote);

	  bRet = (new_quote < max_quote);
	  Assert.assertTrue(bRet);
          expect_quote = (long) 2*1024*1024*1024;
          actual_quote = (new_quote - old_quote);
          log.debug("@@ expect quote: " + expect_quote + ", actual quote: " + actual_quote);
	  Assert.assertEquals(expect_quote, actual_quote);

          log.info(caseName + "===> end");

	  return ;
        }

	@Test
        public void Function_02_quote_more_than_max(){

          caseName = "Function_02_quote_more_than_max";
          log.info(caseName + "===> start");

          resetCurQuote(appKey);
	  boolean bRet = false;
	  long maxQuote = getMaxQuote(appKey);
	  long oldQuote = getCurrentQuote(appKey);
	  long newQuote = 0;
          long writeQuote = 0;
	  String localFile = "1G.jpg";
          ArrayList<String> name_list = new ArrayList<String>(); 

	  while (writeQuote <= maxQuote) 
	  {
	  	String tfsname = tfsManager.saveLargeFile(localFile, null, null);
	  	if (tfsname == null)
	  	{
	  		log.warn("savefile failed");
	  		break;
	  	}
	  	log.debug("@@ tfsname: " + tfsname);
                name_list.add(tfsname);
                writeQuote += (long) 1024*1024*1024;
	  }
	  sleep(MAX_STAT_TIME);
	  newQuote = getCurrentQuote(appKey);
	  log.debug("max_quote: " + maxQuote + ", old_quote: " + oldQuote + ", new_quote: " + newQuote);

	  bRet = (newQuote >= maxQuote);
	  Assert.assertTrue(bRet);
	  Assert.assertEquals(writeQuote, (newQuote - oldQuote));

          log.info(caseName + "===> end");
	  return ;
        }

	@Test
        public void Function_03_modify_quote(){

          caseName = "Function_03_modify_quote";
          log.info(caseName + "===> start");

          resetCurQuote(appKey);
	  boolean bRet = false;
	  long maxQuote = getMaxQuote(appKey);
	  long oldQuote = getCurrentQuote(appKey);
	  long newQuote = 0;
          long oldMaxQuote = maxQuote;
          long newMaxQuote = 20971520;
          long writeQuote = 0;
	  String localFile = "3M.jpg";
          String last_name = "";

	  for (int i = 0; i < 30; i++)
	  {
            if (i % 3 != 2)
            {
	  	String tfsname = tfsManager.saveFile(localFile, null, null);
	  	if (tfsname == null)
	  	{
	  		log.warn("savefile failed");
	  		break;
	  	}
	  	log.debug("@@ tfsname: " + tfsname);
                last_name = tfsname;
                writeQuote += (long) 3*1024*1024;
            }
            else
            {
	      boolean ret = tfsManager.unlinkFile(last_name, null);
              if (!ret)
              {
                log.warn("unlinkFile " + last_name + " failed.");
                break;
              }
              writeQuote -= 3*1024*1024;
            }
            if (i == 20)
            {
              modifyMaxQuote(appKey, newMaxQuote);
            }
	  }

	  sleep(MAX_STAT_TIME);
	  maxQuote = getMaxQuote(appKey);
	  newQuote = getCurrentQuote(appKey);
	  log.debug("max_quote: " + maxQuote + ", old_quote: " + oldQuote + ", new_quote: " + newQuote);

	  bRet = (newQuote >= maxQuote);
	  Assert.assertTrue(bRet);
	  Assert.assertEquals(writeQuote, (newQuote - oldQuote));

          modifyMaxQuote(appKey, oldMaxQuote);
          sleep(MAX_STAT_TIME);

          log.info(caseName + "===> end");

	  return ;
        }

        @Test
        public void Function_04_group_invalid_cluster_rw(){

          caseName = "Function_04_group_invalid_cluster_rw";
          log.info(caseName + "===> start");

          boolean ret = false;
	  String localFile = "3M.jpg";
          String tfsname = "";
          ArrayList<String> nameList = new ArrayList<String>(); 

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNotNull(tfsname);
	    log.debug("@@ tfsname: " + tfsname);
            nameList.add(tfsname);
          }

          setGroupPermission(appKey, INVALID_MODE);
          setClusterPermission(appKey, RW_MODE);
          sleep(MAX_UPDATE_TIME);

          long startTime=System.currentTimeMillis();
          while (tfsname != null) 
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
          }
          long endTime=System.currentTimeMillis();
          log.debug("change time: " + (endTime - startTime));
          Assert.assertTrue( (endTime - startTime)/1000 < MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNull(tfsname);

	    ret = tfsManager.fetchFile(nameList.get(i), null, retLocalFile);
	    Assert.assertFalse(ret);

	    ret = tfsManager.unlinkFile(nameList.get(i), null);
	    Assert.assertFalse(ret);
          }

          setGroupPermission(appKey, RW_MODE);
          setClusterPermission(appKey, RW_MODE);
          sleep(MAX_UPDATE_TIME);

          log.info(caseName + "===> end");
        }

        @Test
        public void Function_05_group_invalid_cluster_r(){

          caseName = "Function_05_group_invalid_cluster_r";
          log.info(caseName + "===> start");

          boolean ret = false;
	  String localFile = "3M.jpg";
          String tfsname = "";
          ArrayList<String> nameList = new ArrayList<String>(); 

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNotNull(tfsname);
	    log.debug("@@ tfsname: " + tfsname);
            nameList.add(tfsname);
          }

          setGroupPermission(appKey, INVALID_MODE);
          setClusterPermission(appKey, RW_MODE);
          sleep(MAX_UPDATE_TIME);

          long startTime=System.currentTimeMillis();
          while (tfsname != null) 
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
          }
          long endTime=System.currentTimeMillis();
          log.debug("change time: " + (endTime - startTime));
          Assert.assertTrue( (endTime - startTime)/1000 < MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNull(tfsname);

	    ret = tfsManager.fetchFile(nameList.get(i), null, retLocalFile);
	    Assert.assertFalse(ret);

	    ret = tfsManager.unlinkFile(nameList.get(i), null);
	    Assert.assertFalse(ret);
          }

          setGroupPermission(appKey, RW_MODE);
          setClusterPermission(appKey, RW_MODE);
          sleep(MAX_UPDATE_TIME);

          log.info(caseName + "===> end");

        }

        @Test
        public void Function_06_group_r_cluster_invalid(){

          caseName = "Function_06_group_r_cluster_invalid";
          log.info(caseName + "===> start");

          boolean ret = false;
	  String localFile = "3M.jpg";
          String tfsname = "";
          ArrayList<String> nameList = new ArrayList<String>(); 

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNotNull(tfsname);
	    log.debug("@@ tfsname: " + tfsname);
            nameList.add(tfsname);
          }

          setGroupPermission(appKey, R_MODE);
          setClusterPermission(appKey, INVALID_MODE);
          sleep(MAX_UPDATE_TIME);

          long startTime=System.currentTimeMillis();
          while (tfsname != null) 
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
          }
          long endTime=System.currentTimeMillis();
          log.debug("change time: " + (endTime - startTime));
          Assert.assertTrue( (endTime - startTime)/1000 < MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNull(tfsname);

	    ret = tfsManager.fetchFile(nameList.get(i), null, retLocalFile);
	    Assert.assertFalse(ret);

	    ret = tfsManager.unlinkFile(nameList.get(i), null);
	    Assert.assertFalse(ret);
          }

          setGroupPermission(appKey, RW_MODE);
          setClusterPermission(appKey, RW_MODE);
          sleep(MAX_UPDATE_TIME);
        }

        @Test
        public void Function_07_group_r_cluster_rw(){

          caseName = "Function_07_group_r_cluster_rw";
          log.info(caseName + "===> start");

          boolean ret = false;
	  String localFile = "3M.jpg";
          String tfsname = "";
          ArrayList<String> nameList = new ArrayList<String>(); 

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNotNull(tfsname);
	    log.debug("@@ tfsname: " + tfsname);
            nameList.add(tfsname);
          }

          setGroupPermission(appKey, R_MODE);
          setClusterPermission(appKey, RW_MODE);
          sleep(MAX_UPDATE_TIME);

          long startTime=System.currentTimeMillis();
          while (tfsname != null) 
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
          }
          long endTime=System.currentTimeMillis();
          log.debug("change time: " + (endTime - startTime));
          Assert.assertTrue( (endTime - startTime)/1000 < MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNull(tfsname);

	    ret = tfsManager.fetchFile(nameList.get(i), null, retLocalFile);
	    Assert.assertTrue(ret);

	    ret = tfsManager.unlinkFile(nameList.get(i), null);
	    Assert.assertTrue(ret);
          }

          setGroupPermission(appKey, RW_MODE);
          setClusterPermission(appKey, RW_MODE);
          sleep(MAX_UPDATE_TIME);

        }
        
        @Test
        public void Function_08_group_rw_cluster_invalid(){

          caseName = "Function_08_group_rw_cluster_invalid";
          log.info(caseName + "===> start");

          boolean ret = false;
	  String localFile = "3M.jpg";
          String tfsname = "";
          ArrayList<String> nameList = new ArrayList<String>(); 

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNotNull(tfsname);
	    log.debug("@@ tfsname: " + tfsname);
            nameList.add(tfsname);
          }

          setGroupPermission(appKey, RW_MODE);
          setClusterPermission(appKey, INVALID_MODE);
          sleep(MAX_UPDATE_TIME);

          long startTime=System.currentTimeMillis();
          while (tfsname != null) 
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
          }
          long endTime=System.currentTimeMillis();
          log.debug("change time: " + (endTime - startTime));
          Assert.assertTrue( (endTime - startTime)/1000 < MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNull(tfsname);

	    ret = tfsManager.fetchFile(nameList.get(i), null, retLocalFile);
	    Assert.assertFalse(ret);

	    ret = tfsManager.unlinkFile(nameList.get(i), null);
	    Assert.assertFalse(ret);
          }

          setGroupPermission(appKey, RW_MODE);
          setClusterPermission(appKey, RW_MODE);
          sleep(MAX_UPDATE_TIME);
        }

        @Test
        public void Function_09_group_rw_cluster_r(){

          caseName = "Function_09_group_rw_cluster_r";
          log.info(caseName + "===> start");

          boolean ret = false;
	  String localFile = "3M.jpg";
          String tfsname = "";
          ArrayList<String> nameList = new ArrayList<String>(); 

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNotNull(tfsname);
	    log.debug("@@ tfsname: " + tfsname);
            nameList.add(tfsname);
          }

          setGroupPermission(appKey, RW_MODE);
          setClusterPermission(appKey, R_MODE);
          sleep(MAX_UPDATE_TIME);

          long startTime=System.currentTimeMillis();
          while (tfsname != null) 
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
          }
          long endTime=System.currentTimeMillis();
          log.debug("change time: " + (endTime - startTime));
          Assert.assertTrue( (endTime - startTime)/1000 < MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNull(tfsname);

	    ret = tfsManager.fetchFile(nameList.get(i), null, retLocalFile);
	    Assert.assertTrue(ret);

	    ret = tfsManager.unlinkFile(nameList.get(i), null);
	    Assert.assertTrue(ret);
          }

          setGroupPermission(appKey, RW_MODE);
          setClusterPermission(appKey, RW_MODE);
          sleep(MAX_UPDATE_TIME);

        }
  
        @Test
        public void Function_10_cluster_invalid() {
          boolean ret = false;
	  String localFile = "1b.jpg";
          String tfsname = "";
          ArrayList<String> nameList = new ArrayList<String>(); 

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNotNull(tfsname);
            nameList.add(tfsname);
          }

          setClusterPermission(appKey, INVALID_MODE);

          long startTime=System.currentTimeMillis();
          while (tfsname != null) 
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
          }
          long endTime=System.currentTimeMillis();
          log.debug("change time: " + (endTime - startTime));
          Assert.assertTrue( (endTime - startTime)/1000 < MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNull(tfsname);

	    tfsname = tfsManager.saveFile(localFile, nameList.get(i), null);
	    Assert.assertNull(tfsname);

	    ret = tfsManager.fetchFile(nameList.get(i), null, retLocalFile);
	    Assert.assertFalse(ret);

	    ret = tfsManager.unlinkFile(nameList.get(i), null);
	    Assert.assertFalse(ret);
          }

          setClusterPermission(appKey, RW_MODE);
          sleep(MAX_UPDATE_TIME);
        }

        @Test
        public void Function_11_cluster_r() {
          boolean ret = false;
	  String localFile = "1b.jpg";
          String tfsname = "";
          ArrayList<String> nameList = new ArrayList<String>(); 

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNotNull(tfsname);
            nameList.add(tfsname);
          }

          setClusterPermission(appKey, R_MODE);

          long startTime=System.currentTimeMillis();
          while (tfsname != null) 
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
          }
          long endTime=System.currentTimeMillis();
          log.debug("change time: " + (endTime - startTime));
          Assert.assertTrue( (endTime - startTime)/1000 < MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNull(tfsname);

	    tfsname = tfsManager.saveFile(localFile, nameList.get(i), null);
	    Assert.assertNotNull(tfsname);

	    ret = tfsManager.fetchFile(nameList.get(i), null, retLocalFile);
	    Assert.assertTrue(ret);

	    ret = tfsManager.unlinkFile(nameList.get(i), null);
	    Assert.assertTrue(ret);
          }

          setClusterPermission(appKey, RW_MODE);
          sleep(MAX_UPDATE_TIME);
        }

        @Test
        public void Function_12_cluster_rw() {

          boolean ret = false;
	  String localFile = "1b.jpg";
	  String newlocalFile = "3M.jpg";
          String tfsname = "";
          int cluster_id = -1;

          setClusterPermission(appKey, RW_MODE);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);

	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);

	    tfsname = tfsManager.saveFile(newlocalFile, tfsname, null);
            log.debug("@@ update tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);

	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);

	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }
        }

        @Test
        public void Function_13_cluster_rw_other_invalid() {

          boolean ret = false;
	  String localFile = "1b.jpg";
	  //String newlocalFile = "100k";
          String tfsname = "";

          setGroupPermission(appKey, RW_MODE);
          setClusterPermission(appKey, 0, RW_MODE);
          setClusterPermission(appKey, 1, INVALID_MODE);
          setClusterPermission(appKey, 2, INVALID_MODE);
          sleep(MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);

	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);

	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }

          setClusterPermission(appKey, 0, INVALID_MODE);
          setClusterPermission(appKey, 1, RW_MODE);
          setClusterPermission(appKey, 2, RW_MODE);
          sleep(MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);

	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);

	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }

          setClusterPermission(appKey, 0, RW_MODE);
          setClusterPermission(appKey, 1, RW_MODE);
          setClusterPermission(appKey, 2, INVALID_MODE);
          sleep(MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);

	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);

	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }

        }

        @Test
        public void Function_14_cluster_rw_to_invalid() {

          boolean ret = false;
	  String localFile = "1b.jpg";
          String tfsname = "";
          ArrayList<String> nameList = new ArrayList<String>(); 

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ normal cluster tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);

	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);

	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }
          // file for fetch and unlink when invalid
          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNotNull(tfsname);
            nameList.add(tfsname);
          }

          // change permission
          setClusterPermission(appKey, INVALID_MODE);

          // test time
          long startTime=System.currentTimeMillis();
          while (tfsname != null) 
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
          }
          long endTime=System.currentTimeMillis();
          log.debug("@@ change time: " + (endTime - startTime));
          Assert.assertTrue( (endTime - startTime)/1000 < MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNull(tfsname);

	    ret = tfsManager.fetchFile(nameList.get(i), null, retLocalFile);
	    Assert.assertFalse(ret);

	    ret = tfsManager.unlinkFile(nameList.get(i), null);
	    Assert.assertFalse(ret);
          }

          setClusterPermission(appKey, RW_MODE);
          sleep(MAX_UPDATE_TIME);
        }

        @Test
        public void Function_15_cluster_rw_to_r() {

          boolean ret = false;
	  String localFile = "1b.jpg";
          String tfsname = "";
          ArrayList<String> nameList = new ArrayList<String>(); 

          setClusterPermission(appKey, RW_MODE);
          sleep(MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);

	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);

	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }

          // file for fetch and unlink when invalid
          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNotNull(tfsname);
            nameList.add(tfsname);
          }

          setClusterPermission(appKey, R_MODE);

          // test time
          long startTime=System.currentTimeMillis();
          while (tfsname != null) 
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
          }
          long endTime=System.currentTimeMillis();
          log.debug("@@ change time: " + (endTime - startTime));
          Assert.assertTrue( (endTime - startTime)/1000 < MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNull(tfsname);

	    ret = tfsManager.fetchFile(nameList.get(i), null, retLocalFile);
	    Assert.assertTrue(ret);

	    ret = tfsManager.unlinkFile(nameList.get(i), null);
	    Assert.assertTrue(ret);
          }

          setClusterPermission(appKey, RW_MODE);
          sleep(MAX_UPDATE_TIME);

        }


        @Test
        public void Function_16_cluster_invalid_to_r() {

          boolean ret = false;
	  String localFile = "1b.jpg";
          String tfsname = "";
          ArrayList<String> nameList = new ArrayList<String>(); 

          // file for fetch and unlink when invalid
          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNotNull(tfsname);
            nameList.add(tfsname);
          }

          setClusterPermission(appKey, INVALID_MODE);
          sleep(MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNull(tfsname);

	    ret = tfsManager.fetchFile(nameList.get(i), null, retLocalFile);
	    Assert.assertFalse(ret);

	    ret = tfsManager.unlinkFile(nameList.get(i), null);
	    Assert.assertFalse(ret);
          }

          setClusterPermission(appKey, R_MODE);

          // test time
          int n = 0;
          long startTime=System.currentTimeMillis();
          do
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNull(tfsname);
	    ret = tfsManager.fetchFile(nameList.get(n % 10), null, retLocalFile);
            n++;
          }
          while (ret != true);
          long endTime=System.currentTimeMillis();
          log.debug("@@ change time: " + (endTime - startTime));
          Assert.assertTrue( (endTime - startTime)/1000 < MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNull(tfsname);

	    ret = tfsManager.fetchFile(nameList.get(i), null, retLocalFile);
	    Assert.assertTrue(ret);

	    ret = tfsManager.unlinkFile(nameList.get(i), null);
	    Assert.assertTrue(ret);
          }

          setClusterPermission(appKey, RW_MODE);
          sleep(MAX_UPDATE_TIME);

        }

        @Test
        public void Function_17_cluster_invalid_to_rw() {

          boolean ret = false;
	  String localFile = "1b.jpg";
          String tfsname = "";
          ArrayList<String> nameList = new ArrayList<String>(); 

          // file for fetch and unlink when invalid
          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNotNull(tfsname);
            nameList.add(tfsname);
          }

          setClusterPermission(appKey, INVALID_MODE);
          sleep(MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNull(tfsname);

	    ret = tfsManager.fetchFile(nameList.get(i), null, retLocalFile);
	    Assert.assertFalse(ret);

	    ret = tfsManager.unlinkFile(nameList.get(i), null);
	    Assert.assertFalse(ret);
          }

          setClusterPermission(appKey, RW_MODE);

          // test time
          int n = 0;
          long startTime=System.currentTimeMillis();
          do
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    ret = tfsManager.fetchFile(nameList.get(n % 10), null, retLocalFile);
            n++;
          }
          while (tfsname == null && ret == false);
          long endTime=System.currentTimeMillis();
          log.debug("@@ change time: " + (endTime - startTime));
          Assert.assertTrue( (endTime - startTime)/1000 < MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNotNull(tfsname);

	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);

	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }

        }

        @Test
        public void Function_18_cluster_r_to_invalid() {

          boolean ret = false;
	  String localFile = "1b.jpg";
          String tfsname = "";
          ArrayList<String> nameList = new ArrayList<String>(); 

          // file for fetch and unlink when invalid
          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNotNull(tfsname);
            nameList.add(tfsname);
          }

          setClusterPermission(appKey, R_MODE);
          sleep(MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNull(tfsname);

	    ret = tfsManager.fetchFile(nameList.get(i), null, retLocalFile);
	    Assert.assertTrue(ret);

	    ret = tfsManager.unlinkFile(nameList.get(i), null);
	    Assert.assertTrue(ret);
          }

          setClusterPermission(appKey, INVALID_MODE);

          // test time
          int n = 0;
          long startTime=System.currentTimeMillis();
          do
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    ret = tfsManager.fetchFile(nameList.get(n % 10), null, retLocalFile);
            n++;
          }
          while (tfsname == null && ret == true);
          long endTime=System.currentTimeMillis();
          log.debug("@@ change time: " + (endTime - startTime));
          Assert.assertTrue( (endTime - startTime)/1000 < MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNull(tfsname);

	    ret = tfsManager.fetchFile(nameList.get(i), null, retLocalFile);
	    Assert.assertFalse(ret);

	    ret = tfsManager.unlinkFile(nameList.get(i), null);
	    Assert.assertFalse(ret);
          }

          setClusterPermission(appKey, RW_MODE);
          sleep(MAX_UPDATE_TIME);

        }

        @Test
        public void Function_19_cluster_r_to_rw() {

          boolean ret = false;
	  String localFile = "1b.jpg";
          String tfsname = "";
          ArrayList<String> nameList = new ArrayList<String>(); 

          setClusterPermission(appKey, RW_MODE);
          sleep(MAX_UPDATE_TIME);

          // file for fetch and unlink when invalid
          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNotNull(tfsname);
            nameList.add(tfsname);
          }

          setClusterPermission(appKey, R_MODE);
          sleep(MAX_UPDATE_TIME);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNull(tfsname);

	    ret = tfsManager.fetchFile(nameList.get(i), null, retLocalFile);
	    Assert.assertTrue(ret);
          }

          setClusterPermission(appKey, RW_MODE);

          // test time
          int n = 0;
          long startTime=System.currentTimeMillis();
          do
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    ret = tfsManager.fetchFile(nameList.get(n % 10), null, retLocalFile);
            n++;
          }
          while (tfsname == null && ret == true);
          long endTime=System.currentTimeMillis();
          log.debug("@@ change time: " + (endTime - startTime));
          Assert.assertTrue( (endTime - startTime)/1000 < MAX_UPDATE_TIME);

          // new stat check
          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
	    Assert.assertNotNull(tfsname);

	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);

	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }

        }

        // remove cluster and add cluster case must be exec together
        @Test
        public void Function_20_remove_cluster() {

          caseName = "Function_20_remove_cluster";
          log.info(caseName + "===> start");
          boolean ret = false;
	  String localFile = "1b.jpg";
          String tfsname = "";

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);

	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);

	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }

          // remove default tfs cluster
          removeTfsCluster(appKey, 1);
          setClusterPermission(appKey, 0, RW_MODE);
          sleep(MAX_UPDATE_TIME);

          // back to default tfs cluster
          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);

	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);

	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }
          log.info(caseName + "===> end");

        }

        // remove cluster and add cluster case must be exec together
        @Test
        public void Function_21_add_cluster() {

          caseName = "Function_21_add_cluster";
          log.info(caseName + "===> start");

          boolean ret = false;
	  String localFile = "1b.jpg";
          String tfsname = "";

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);

	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);

	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }
           
          // nearer than default tfs cluster
          addNewTfsCluster(appKey, 1, "T2B");
          setClusterPermission(appKey, 0, RW_MODE);
          setClusterPermission(appKey, 1, RW_MODE);
          setClusterPermission(appKey, 2, RW_MODE);
          sleep(MAX_UPDATE_TIME);

          // change cluster
          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);

	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);

	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }

          log.info(caseName + "===> end");

        }

        @Test
        public void Function_26_modify_other_rc_server() {

          caseName = "Function_26_modify_other_rc_server";
          log.info(caseName + "===> start");

          boolean ret = false;
	  String localFile = "1b.jpg";
          String tfsname = "";
          long startTime = 0;
          long endTime = 0;

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);
	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);
	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }

          modifyRc(RC_DEFAULT_INDEX, RC_NEW_INDEX);

          // check log to test the result, both new and old rc will do nothing
          log.debug("@@ modify rc status begin -->");
          long oldCapacity = getUsedCapacity(appKey);
          long count = 0;
          startTime=System.currentTimeMillis();
          do
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);
	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);
            endTime=System.currentTimeMillis();
            log.debug("starttime: " + startTime + "endTime: " + endTime + "change time: " + (endTime - startTime)/1000); 
            count++;
          }
          while ((endTime - startTime)/1000 < MAX_UPDATE_TIME);
          log.debug("@@ modify rc status end -->");
          sleep (MAX_STAT_TIME);
          long newCapacity = getUsedCapacity(appKey);
          log.debug("@@ count: " + count);
	  Assert.assertEquals(count, (newCapacity - oldCapacity));

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);
	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);
	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }

          modifyRc(RC_NEW_INDEX, RC_DEFAULT_INDEX);
          sleep(MAX_UPDATE_TIME);
        }

        @Test
        public void Function_27_modify_invalid_rc_server() {

          caseName = "Function_27_modify_invalid_rc_server";
          log.info(caseName + "===> start");

          boolean ret = false;
	  String localFile = "1b.jpg";
          String tfsname = "";
          long startTime = 0;
          long endTime = 0;

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);
	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);
	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }

          modifyRc(RC_DEFAULT_INDEX, RC_INVALID_INDEX);

          // check log to test the result, old rc will do nothing
          log.debug("@@ modify invalid rc server begin -->");
          long oldCapacity = getUsedCapacity(appKey);
          long count = 0;
          startTime=System.currentTimeMillis();
          do
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);
	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);
            endTime=System.currentTimeMillis();
            log.debug("starttime: " + startTime + "endTime: " + endTime + "change time: " + (endTime - startTime)/1000); 
            count++;
          }
          while ((endTime - startTime)/1000 < MAX_UPDATE_TIME);
          log.debug("@@ modify invalid rc server end -->");
          sleep (MAX_STAT_TIME);
          long newCapacity = getUsedCapacity(appKey);
          log.debug("@@ count: " + count);
          ret = (count < (newCapacity - oldCapacity));
	  Assert.assertTrue(ret);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);
	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);
	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }

          modifyRc(RC_INVALID_INDEX, RC_DEFAULT_INDEX);
          sleep(MAX_UPDATE_TIME);
        }

        @Test
        public void Function_28_change_all_rc_server_mode() {

          caseName = "Function_28_change_all_rc_server_mode";
          log.info(caseName + "===> start");

          boolean ret = false;
	  String localFile = "1b.jpg";
          String tfsname = "";
          long startTime = 0;
          long endTime = 0;

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);
	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);
	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }

          addOneRc(1, RC_VALID_MODE);
          setRcMode(RC_INVALID_MODE);

          // check log to test the result, all rc will do nothing
          log.debug("@@ modify rc status begin -->");
          startTime=System.currentTimeMillis();
          do
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);
	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);
	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
            endTime=System.currentTimeMillis();
            log.debug("starttime: " + startTime + "endTime: " + endTime + "change time: " + (endTime - startTime)/1000);            
          }
          while ((endTime - startTime)/1000 < MAX_UPDATE_TIME);
          log.debug("@@ modify rc status end -->");

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);
	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);
	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }

          setRcMode(RC_VALID_MODE);
          sleep(MAX_UPDATE_TIME);
        }

        @Test
        public void Function_29_change_rc_server_mode() {

          caseName = "Function_29_change_rc_server_mode";
          log.info(caseName + "===> start");

          boolean ret = false;
	  String localFile = "1b.jpg";
          String tfsname = "";
          long startTime = 0;
          long endTime = 0;

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);
	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);
	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }

          setRcMode(0, RC_INVALID_MODE);

          // check log to test the result, rc will change to another 
          log.debug("@@ modify rc status begin -->");
          long oldCapacity = getUsedCapacity(appKey);
          long count = 0;
          startTime=System.currentTimeMillis();
          do
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);
	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);
	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
            endTime=System.currentTimeMillis();
            log.debug("starttime: " + startTime + "endTime: " + endTime + "change time: " + (endTime - startTime)/1000); 
            count++;
          }
          while ((endTime - startTime)/1000 < MAX_UPDATE_TIME);
          log.debug("@@ modify rc status end -->");
          sleep (MAX_STAT_TIME);
          long newCapacity = getUsedCapacity(appKey);
          log.debug("@@ count: " + count);
	  Assert.assertEquals(count, (newCapacity - oldCapacity));

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);
	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);
	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }

          setRcMode(RC_VALID_MODE);
          removeOneRc(1);
          sleep(MAX_UPDATE_TIME);

          log.info(caseName + "===> end");
        }

        @Test
        public void Function_30_add_rc_server() {

          caseName = "Function_30_add_rc_server";
          log.info(caseName + "===> start");

          boolean ret = false;
	  String localFile = "1b.jpg";
          String tfsname = "";
          long startTime = 0;
          long endTime = 0;

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);
	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);
	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }

          addOneRc(1, RC_VALID_MODE);

          // check log to test the result
          log.debug("@@ add one rc begin -->");
          startTime=System.currentTimeMillis();
          do
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);
	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);
	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
            endTime=System.currentTimeMillis();
            log.debug("starttime: " + startTime + "endTime: " + endTime + "change time: " + (endTime - startTime)/1000); 
          }
          while ((endTime - startTime)/1000 < MAX_UPDATE_TIME);
          log.debug("@@ add one rc end -->");

          removeOneRc(0);
          killOneRc(0);

          // check log to test the result
          log.debug("@@ rm one rc begin -->");
          long oldCapacity = getUsedCapacity(appKey);
          long count = 0;
          startTime=System.currentTimeMillis();
          do
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);
	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);
	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
            endTime=System.currentTimeMillis();
            log.debug("starttime: " + startTime + "endTime: " + endTime + "change time: " + (endTime - startTime)/1000); 
            count++;
          }
          while ((endTime - startTime)/1000 < MAX_UPDATE_TIME); 
          log.debug("@@ rm one rc end -->");
          sleep (MAX_STAT_TIME);
          long newCapacity = getUsedCapacity(appKey);
          log.debug("@@ count: " + count);
          ret = (count > (newCapacity - oldCapacity));
	  Assert.assertTrue(ret);

          for (int i = 0; i < 10; i++)
          {
	    tfsname = tfsManager.saveFile(localFile, null, null);
            log.debug("@@ tfsname: " + tfsname);
	    Assert.assertNotNull(tfsname);
	    ret = tfsManager.fetchFile(tfsname, null, retLocalFile);
	    Assert.assertTrue(ret);
	    ret = tfsManager.unlinkFile(tfsname, null);
	    Assert.assertTrue(ret);
          }

          addOneRc(0, RC_VALID_MODE);
          startOneRc(0);
          removeOneRc(1);
          sleep (MAX_UPDATE_TIME);

          log.info(caseName + "===> end");
        }

}
