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
import com.taobao.common.tfs.NameMetaBaseCase;


/**
 * @author Administrator/chuyu
 *
 */
public class mergeRcMetaManager_11_visit_meta_server extends NameMetaBaseCase {

  public int CHECK_TIME = 60;
  public static String rcAddr = "10.232.36.203:6100";
  public static String rsAddr = "10.232.36.203:3600";
  public static String appIp = "10.232.36.203";
  public DefaultTfsManager tfsManager = null;
  String testLocalFile = "1k.jpg";

  @Before
    public void setUp(){
      // 1k file
      createFile(testLocalFile, 1 << 10);

    }

  @After
    public void tearDown(){
      File file = new File(testLocalFile);
      if(file.exists()){
        file.delete();
      }
    }

  @Test
    public void Function_00_appid_diff_uid_diff() {

      caseName = "Function_00_appid_diff_uid_diff";
      log.info(caseName + "===> start");

      String newAppKey = "tappkey00003";
      long appId = 3;
      long userId = 311013878;
      String localFile = "1k.jpg";
      String filePath = "/cytest_11";
      boolean bRet = false;
      List<String> rootServerAddrList = new ArrayList<String>();
      rootServerAddrList.add(rsAddr);

      tfsManager = new DefaultTfsManager();
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(newAppKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);
      bRet=tfsManager.saveFile(appId, userId, localFile, filePath);
      bRet=tfsManager.rmFile(appId, userId, filePath);
      int index = getServingIndexFromMS(appId, userId);
      log.debug("@@ index: " + index);

      tfsManager.destroy();

      newAppKey = "381";
      appId = 9;
      userId = 78934556;
      tfsManager = new DefaultTfsManager();
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(newAppKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);
      bRet=tfsManager.saveFile(appId, userId, localFile, filePath);
      bRet=tfsManager.rmFile(appId, userId, filePath);
      index = getServingIndexFromMS(appId, userId);
      log.debug("@@ index: " + index);

      tfsManager.destroy();

    } 

  @Test
    public void Function_01_appid_same_uid_diff() {

      caseName = "Function_01_appid_same_uid_diff";
      log.info(caseName + "===> start");

      String newAppKey = "tappkey00003";
      long appId = 3;
      long userId = 312019934;
      String localFile = "1k.jpg";
      String filePath = "/cytest_11";
      boolean bRet = false;
      List<String> rootServerAddrList = new ArrayList<String>();
      rootServerAddrList.add(rsAddr);

      tfsManager = new DefaultTfsManager();
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(newAppKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);
      bRet=tfsManager.saveFile(appId, userId, localFile, filePath);
      bRet=tfsManager.rmFile(appId, userId, filePath);
      int index = getServingIndexFromMS(appId, userId);
      log.debug("@@ index: " + index);

      tfsManager.destroy();

      appId = 3;
      userId = 789019898;
      tfsManager = new DefaultTfsManager();
      tfsManager.setRcAddr(rcAddr);
      tfsManager.setAppKey(newAppKey);
      tfsManager.setAppIp(appIp);
      tfsManager.setUseNameMeta(true);
      bRet = tfsManager.init();
      Assert.assertTrue(bRet);
      bRet=tfsManager.saveFile(appId, userId, localFile, filePath);
      bRet=tfsManager.rmFile(appId, userId, filePath);
      index = getServingIndexFromMS(appId, userId);
      log.debug("@@ index: " + index);

      tfsManager.destroy();
    } 

}
