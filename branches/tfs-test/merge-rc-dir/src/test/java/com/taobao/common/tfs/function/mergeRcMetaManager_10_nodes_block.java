/**
 * 
 */
package com.taobao.common.tfs.function;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.text.SimpleDateFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.Ignore;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import com.taobao.gaia.KillTypeEnum;
import com.taobao.common.tfs.DefaultTfsManager; 


/**
 * @author Administrator/chuyu
 *
 */
public class mergeRcMetaManager_10_nodes_block extends NameMetaBaseCase {

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

  @Test
    public void Function_00_block_unserveringMs_and_rs() {

      caseName = "Function_00_block_unserveringMs_and_rs";
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

      fullBlockMetaServerAndRS(msIndex);

      currentTime = getClientCurrentTime();
      log.debug(" @@ currentTime: " + df.format(new java.util.Date(currentTime * 1000)));
      currentRowNum = getClientCurrentRowNum();
      log.debug(" @@ currentRow: " + currentRowNum);

      sleep(30);
     
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

      // unblock server
      oldVersion = newVersion;

      unblockMetaServerToRS(msIndex);

      currentTime = getClientCurrentTime();
      log.debug(" @@ currentTime: " + df.format(new java.util.Date(currentTime * 1000)));
      currentRowNum = getClientCurrentRowNum();
      log.debug(" @@ currentRow: " + currentRowNum);

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
      //Assert.assertTrue(1 <= newVersion - oldVersion); // metaserver may send packets to rootserver

      mixOpCmdStop();
    } 

  @Ignore
    public void Function_01_block_serveringMs_and_rs() {

      caseName = "Function_01_block_serveringMs_and_rs";
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

      fullBlockMetaServerAndRS(msIndex);

      currentTime = getClientCurrentTime();
      log.debug(" @@ currentTime: " + df.format(new java.util.Date(currentTime * 1000)));
      currentRowNum = getClientCurrentRowNum();
      log.debug(" @@ currentRow: " + currentRowNum);

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

      // unblock server
      oldVersion = newVersion;

      unblockMetaServerToRS(msIndex);

      currentTime = getClientCurrentTime();
      log.debug(" @@ currentTime: " + df.format(new java.util.Date(currentTime * 1000)));
      currentRowNum = getClientCurrentRowNum();
      log.debug(" @@ currentRow: " + currentRowNum);

      sleep(CHECK_TIME);

      failStartTime = getFailStartTime(currentRowNum);
      failEndTime = getFailEndTime(currentRowNum);
      // server can still do service in lease time
      log.debug("operation start faied between " + df.format(new java.util.Date(currentTime * 1000)) + " and " + df.format(new java.util.Date(failStartTime * 1000)));
      Assert.assertTrue(failStartTime - currentTime < LEASE_TIME);
      //should check log, maybe sometimes error not occured by update rootserver tables
      log.debug("operation faied between " + df.format(new java.util.Date(failStartTime * 1000)) + " and " + df.format(new java.util.Date(failEndTime * 1000)));
      Assert.assertTrue(failEndTime - failStartTime < MAX_ERROR_TIME);
      Assert.assertTrue(failEndTime - failStartTime >= MIN_ERROR_TIME);

      newVersion = getRsCurrentVersion();
      log.debug(" @@ newVersion: " + newVersion);
      Assert.assertTrue(1 <= newVersion - oldVersion); // metaserver may send packets to rootserver

      mixOpCmdStop();
    } 

  @Ignore
    public void Function_02_block_serveringMs_and_client() {

      caseName = "Function_02_block_serveringMs_and_client";
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

      fullBlockClientrToMS(msIndex);

      currentTime = getClientCurrentTime();
      log.debug(" @@ currentTime: " + df.format(new java.util.Date(currentTime * 1000)));
      currentRowNum = getClientCurrentRowNum();
      log.debug(" @@ currentRow: " + currentRowNum);

      sleep(CHECK_TIME);
     
      newVersion = getRsCurrentVersion();
      log.debug(" @@ newVersion: " + newVersion);
      Assert.assertTrue(newVersion == oldVersion);

      failStartTime = getFailStartTime(currentRowNum);
      failEndTime = getFailEndTime(currentRowNum);
      // server can still do service in lease time
      log.debug("operation start faied between " + df.format(new java.util.Date(currentTime * 1000)) + " and " + df.format(new java.util.Date(failStartTime * 1000)));
      //should check log, maybe sometimes error not occured by update rootserver tables
      log.debug("operation faied between " + df.format(new java.util.Date(failStartTime * 1000)) + " and " + df.format(new java.util.Date(failEndTime * 1000)));
      Assert.assertTrue(failEndTime - failStartTime < CHECK_TIME + DIFF_TIME);
      Assert.assertTrue(failEndTime - failStartTime > CHECK_TIME - DIFF_TIME);

      // unblock server
      oldVersion = newVersion;

      unblockClientToMS();

      currentTime = getClientCurrentTime();
      log.debug(" @@ currentTime: " + df.format(new java.util.Date(currentTime * 1000)));
      currentRowNum = getClientCurrentRowNum();
      log.debug(" @@ currentRow: " + currentRowNum);

      sleep(CHECK_TIME);

      newVersion = getRsCurrentVersion();
      log.debug(" @@ newVersion: " + newVersion);
      Assert.assertTrue(newVersion == oldVersion);

      failStartTime = getFailStartTime(currentRowNum);
      failEndTime = getFailEndTime(currentRowNum);
      // server can still do service in lease time
      log.debug("operation start faied between " + df.format(new java.util.Date(currentTime * 1000)) + " and " + df.format(new java.util.Date(failStartTime * 1000)));
      //should check log, maybe sometimes error not occured by update rootserver tables
      log.debug("operation faied between " + df.format(new java.util.Date(failStartTime * 1000)) + " and " + df.format(new java.util.Date(failEndTime * 1000)));
      Assert.assertTrue(failStartTime == 0);
      Assert.assertTrue(failEndTime == 0);

      mixOpCmdStop();
    } 
}
