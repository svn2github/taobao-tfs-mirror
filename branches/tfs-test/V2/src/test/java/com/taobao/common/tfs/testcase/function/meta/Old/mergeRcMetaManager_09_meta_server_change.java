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
import java.text.SimpleDateFormat;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import com.taobao.gaia.KillTypeEnum;
import com.taobao.common.tfs.DefaultTfsManager; 
import com.taobao.common.tfs.NameMetaBaseCase;


/**
 * @author Administrator/chuyu
 *
 */
public class mergeRcMetaManager_09_meta_server_change extends NameMetaBaseCase {

  public long appId = 3;
  public long userId = 2711;
  public int CHECK_TIME = 60;
  public int MIN_ERROR_TIME = 0; // empiric value
  public int MAX_ERROR_TIME = 30; // empiric value
  public int LEASE_TIME = 15; // the same to rootserver lease time
  public int DIFF_TIME = 15; // empiric value, Error Threshold
  @Before
    public void setUp(){

    }

  @After
    public void tearDown(){
    }

  @Ignore
    public void Function_00_remove_unservice_meta_server() {

      caseName = "Function_00_remove_unservice_meta_server";
      log.info(caseName + "===> start");

      // test client error time
      long currentTime = 0;
      long currentRowNum = 0;
      long failStartTime = 0;
      long failEndTime = 0;
      // test rootserver version
      long oldVersion = 0;
      long newVersion = 0;
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

      mixOpCmd();
      sleep(WAIT_TIME);
      int msIndex = getUnServingMSIndex(appId, userId);
      log.debug(" @@ msIndex: " + msIndex);
      Assert.assertTrue(msIndex > -1);

      // block server

      oldVersion = getRsCurrentVersion();
      log.debug(" @@ oldVersion: " + oldVersion);
      currentTime = getClientCurrentTime();
      log.debug(" @@ currentTime: " + df.format(new java.util.Date(currentTime * 1000)));
      currentRowNum = getClientCurrentRowNum();
      log.debug(" @@ currentRow: " + currentRowNum);

      killOneMetaserver(msIndex); 
      sleep(CHECK_TIME);
     
      newVersion = getRsCurrentVersion();
      log.debug(" @@ newVersion: " + newVersion);
      Assert.assertTrue(1 == (newVersion - oldVersion));

      failStartTime = getFailStartTime(currentRowNum);
      failEndTime = getFailEndTime(currentRowNum);
      // server can still do service in lease time
      log.debug("operation start faied between " + df.format(new java.util.Date(currentTime * 1000)) + " and " + df.format(new java.util.Date(failStartTime * 1000)));
      Assert.assertTrue(failStartTime - currentTime < LEASE_TIME);
      //should check log, maybe sometimes error not occured by update rootserver tables
      log.debug("operation faied between " + df.format(new java.util.Date(failStartTime * 1000)) + " and " + df.format(new java.util.Date(failEndTime * 1000)));
      Assert.assertTrue(failEndTime - failStartTime < MAX_ERROR_TIME);
      Assert.assertTrue(failEndTime - failStartTime >= MIN_ERROR_TIME);

      mixOpCmdStop();
    } 

  // test with Function_00_remove_unservice_meta_server
  @Ignore
    public void Function_01_add_meta_server() {

      caseName = "Function_01_add_meta_server";
      log.info(caseName + "===> start");

      // test client error time
      long currentTime = 0;
      long currentRowNum = 0;
      long failStartTime = 0;
      long failEndTime = 0;
      // test rootserver version
      long oldVersion = 0;
      long newVersion = 0;
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

      mixOpCmd();
      sleep(WAIT_TIME);
      int msIndex = getUnServingMSIndex(appId, userId);
      log.debug(" @@ msIndex: " + msIndex);
      Assert.assertTrue(msIndex > -1);

      // block server

      oldVersion = getRsCurrentVersion();
      log.debug(" @@ oldVersion: " + oldVersion);
      currentTime = getClientCurrentTime();
      log.debug(" @@ currentTime: " + df.format(new java.util.Date(currentTime * 1000)));
      currentRowNum = getClientCurrentRowNum();
      log.debug(" @@ currentRow: " + currentRowNum);

      startOneMetaserver(msIndex); 

      sleep(CHECK_TIME);
     
      newVersion = getRsCurrentVersion();
      log.debug(" @@ newVersion: " + newVersion);
      Assert.assertTrue(1 == (newVersion - oldVersion));

      failStartTime = getFailStartTime(currentRowNum);
      failEndTime = getFailEndTime(currentRowNum);
      log.debug("operation start faied between " + df.format(new java.util.Date(currentTime * 1000)) + " and " + df.format(new java.util.Date(failStartTime * 1000)));
      Assert.assertTrue(failStartTime - currentTime < LEASE_TIME);
      //should check log, maybe sometimes error not occured by update rootserver tables
      log.debug("operation faied between " + df.format(new java.util.Date(failStartTime * 1000)) + " and " + df.format(new java.util.Date(failEndTime * 1000)));
      Assert.assertTrue(failEndTime - failStartTime < MAX_ERROR_TIME);
      Assert.assertTrue(failEndTime - failStartTime >= MIN_ERROR_TIME);

      mixOpCmdStop();
    } 

  @Ignore
    public void Function_02_remove_service_meta_server() {

      caseName = "Function_02_remove_service_meta_server";
      log.info(caseName + "===> start");

      // test client error time
      long currentTime = 0;
      long currentRowNum = 0;
      long failStartTime = 0;
      long failEndTime = 0;
      // test rootserver version
      long oldVersion = 0;
      long newVersion = 0;
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

      mixOpCmd();
      sleep(WAIT_TIME);
      int msIndex = getServingMSIndex(appId, userId);
      log.debug(" @@ msIndex: " + msIndex);
      Assert.assertTrue(msIndex > -1);

      // block server

      oldVersion = getRsCurrentVersion();
      log.debug(" @@ oldVersion: " + oldVersion);
      currentTime = getClientCurrentTime();
      log.debug(" @@ currentTime: " + df.format(new java.util.Date(currentTime * 1000)));
      currentRowNum = getClientCurrentRowNum();
      log.debug(" @@ currentRow: " + currentRowNum);

      killOneMetaserver(msIndex); 
      sleep(CHECK_TIME);
     
      newVersion = getRsCurrentVersion();
      log.debug(" @@ newVersion: " + newVersion);
      Assert.assertTrue(1 == (newVersion - oldVersion));

      failStartTime = getFailStartTime(currentRowNum);
      failEndTime = getFailEndTime(currentRowNum);
      // server can still do service in lease time
      log.debug("operation start faied between " + df.format(new java.util.Date(currentTime * 1000)) + " and " + df.format(new java.util.Date(failStartTime * 1000)));
      Assert.assertTrue(failStartTime - currentTime < LEASE_TIME);
      //should check log, maybe sometimes error not occured by update rootserver tables
      log.debug("operation faied between " + df.format(new java.util.Date(failStartTime * 1000)) + " and " + df.format(new java.util.Date(failEndTime * 1000)));
      Assert.assertTrue(failEndTime - failStartTime < MAX_ERROR_TIME);
      Assert.assertTrue(failEndTime - failStartTime >= MIN_ERROR_TIME);

      mixOpCmdStop();
      startOneMetaserver(msIndex); 
      sleep(WAIT_TIME);

    }

  @Test
    public void Function_03_restart_service_meta_server() {

      caseName = "Function_03_restart_service_meta_server";
      log.info(caseName + "===> start");

      // test client error time
      long currentTime = 0;
      long currentRowNum = 0;
      long failStartTime = 0;
      long failEndTime = 0;
      // test rootserver version
      long oldVersion = 0;
      long newVersion = 0;
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

      mixOpCmd();
      sleep(WAIT_TIME);
      int msIndex = getServingMSIndex(appId, userId);
      log.debug(" @@ msIndex: " + msIndex);
      Assert.assertTrue(msIndex > -1);

      // stop server

      oldVersion = getRsCurrentVersion();
      log.debug(" @@ oldVersion: " + oldVersion);
      currentTime = getClientCurrentTime();
      log.debug(" @@ currentTime: " + df.format(new java.util.Date(currentTime * 1000)));
      currentRowNum = getClientCurrentRowNum();
      log.debug(" @@ currentRow: " + currentRowNum);

      killOneMetaserver(msIndex); 
      sleep(CHECK_TIME);
     
      newVersion = getRsCurrentVersion();
      log.debug(" @@ newVersion: " + newVersion);
      Assert.assertTrue(1 == (newVersion - oldVersion));

      failStartTime = getFailStartTime(currentRowNum);
      failEndTime = getFailEndTime(currentRowNum);
      // server can still do service in lease time
      log.debug("operation start faied between " + df.format(new java.util.Date(currentTime * 1000)) + " and " + df.format(new java.util.Date(failStartTime * 1000)));
      Assert.assertTrue(failStartTime - currentTime < LEASE_TIME);
      //should check log, maybe sometimes error not occured by update rootserver tables
      log.debug("operation faied between " + df.format(new java.util.Date(failStartTime * 1000)) + " and " + df.format(new java.util.Date(failEndTime * 1000)));
      Assert.assertTrue(failEndTime - failStartTime < MAX_ERROR_TIME);
      Assert.assertTrue(failEndTime - failStartTime >= MIN_ERROR_TIME);

      // start server
      oldVersion = newVersion;
      currentTime = getClientCurrentTime();
      log.debug(" @@ currentTime: " + df.format(new java.util.Date(currentTime * 1000)));
      currentRowNum = getClientCurrentRowNum();
      log.debug(" @@ currentRow: " + currentRowNum);

      startOneMetaserver(msIndex); 
      sleep(CHECK_TIME);
     
      newVersion = getRsCurrentVersion();
      log.debug(" @@ newVersion: " + newVersion);
      Assert.assertTrue(1 == (newVersion - oldVersion));

      failStartTime = getFailStartTime(currentRowNum);
      failEndTime = getFailEndTime(currentRowNum);
      log.debug("operation start faied between " + df.format(new java.util.Date(currentTime * 1000)) + " and " + df.format(new java.util.Date(failStartTime * 1000)));
      Assert.assertTrue(failStartTime - currentTime < LEASE_TIME);
      //should check log, maybe sometimes error not occured by update rootserver tables
      log.debug("operation faied between " + df.format(new java.util.Date(failStartTime * 1000)) + " and " + df.format(new java.util.Date(failEndTime * 1000)));
      Assert.assertTrue(failEndTime - failStartTime < MAX_ERROR_TIME);
      Assert.assertTrue(failEndTime - failStartTime >= MIN_ERROR_TIME);
    } 

  @Test
    public void Function_04_remove_two_meta_server() {

      caseName = "Function_04_remove_two_meta_server";
      log.info(caseName + "===> start");

      // test client error time
      long currentTime = 0;
      long currentRowNum = 0;
      long failStartTime = 0;
      long failEndTime = 0;
      // test rootserver version
      long oldVersion = 0;
      long newVersion = 0;
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

      mixOpCmd();
      sleep(WAIT_TIME);

      // stop server

      oldVersion = getRsCurrentVersion();
      log.debug(" @@ oldVersion: " + oldVersion);
      currentTime = getClientCurrentTime();
      log.debug(" @@ currentTime: " + df.format(new java.util.Date(currentTime * 1000)));
      currentRowNum = getClientCurrentRowNum();
      log.debug(" @@ currentRow: " + currentRowNum);

      int serveringMsIndex = getServingMSIndex(appId, userId);
      log.debug("serveringMsIndex: " + serveringMsIndex);
      Assert.assertTrue(serveringMsIndex > -1);
      killOneMetaserver(serveringMsIndex); 
      sleep(CHECK_TIME);
     
      newVersion = getRsCurrentVersion();
      log.debug(" @@ newVersion: " + newVersion);
      //Assert.assertTrue(1 == (newVersion - oldVersion));

      failStartTime = getFailStartTime(currentRowNum);
      failEndTime = getFailEndTime(currentRowNum);
      // server can still do service in lease time
      log.debug("operation start faied between " + df.format(new java.util.Date(currentTime * 1000)) + " and " + df.format(new java.util.Date(failStartTime * 1000)));
      //Assert.assertTrue(failStartTime - currentTime < LEASE_TIME);
      //should check log, maybe sometimes error not occured by update rootserver tables
      log.debug("operation faied between " + df.format(new java.util.Date(failStartTime * 1000)) + " and " + df.format(new java.util.Date(failEndTime * 1000)));
      //Assert.assertTrue(failEndTime - failStartTime < MAX_ERROR_TIME);
      //Assert.assertTrue(failEndTime - failStartTime >= MIN_ERROR_TIME);

      // stop other server
      oldVersion = newVersion;
      currentTime = getClientCurrentTime();
      log.debug(" @@ currentTime: " + df.format(new java.util.Date(currentTime * 1000)));
      currentRowNum = getClientCurrentRowNum();
      log.debug(" @@ currentRow: " + currentRowNum);

      int unserveringMsIndex = getUnServingMSIndex(appId, userId);
      log.debug("unserveringMsIndex: " + unserveringMsIndex);
      Assert.assertTrue(unserveringMsIndex > -1);
      killOneMetaserver(unserveringMsIndex); 
      sleep(CHECK_TIME);
     

      failStartTime = getFailStartTime(currentRowNum);
      failEndTime = getFailEndTime(currentRowNum);
      // server can still do service in lease time
      log.debug("operation start faied between " + df.format(new java.util.Date(currentTime * 1000)) + " and " + df.format(new java.util.Date(failStartTime * 1000)));
      //Assert.assertTrue(failStartTime - currentTime < LEASE_TIME);
      //should check log, maybe sometimes error not occured by update rootserver tables
      log.debug("operation faied between " + df.format(new java.util.Date(failStartTime * 1000)) + " and " + df.format(new java.util.Date(failEndTime * 1000)));
      //Assert.assertTrue(failEndTime - failStartTime < MAX_ERROR_TIME);
      //Assert.assertTrue(failEndTime - failStartTime >= MIN_ERROR_TIME);

      newVersion = getRsCurrentVersion();
      log.debug(" @@ newVersion: " + newVersion);
      //Assert.assertTrue(1 == (newVersion - oldVersion));

      mixOpCmdStop();

      startOneMetaserver(serveringMsIndex);
      startOneMetaserver(unserveringMsIndex);
    } 
}
