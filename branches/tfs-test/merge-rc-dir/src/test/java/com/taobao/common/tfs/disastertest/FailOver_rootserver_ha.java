/**
 * 
 */
package com.taobao.common.tfs.disastertest;

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
public class FailOver_rootserver_ha extends NameMetaBaseCase {

   public int CHECK_TIME = 3;

  @Before
    public void setUp(){

    }

  @After
    public void tearDown(){
    }

  @Ignore
    public void Function_00_ha_change_time() {

      caseName = "Function_00_ha_change_time";
      log.info(caseName + "===> start");

      boolean ret = false;
      //mixOpCmd();
      int oldRsIndex = getHaMasterRsIndex();
      log.debug("oldRsIndex: " + oldRsIndex);
      ret = killOneRs(oldRsIndex);
      Assert.assertTrue(ret);

      //sleep(MIGRATE_TIME);
      long newRsIndex = 0;
      long startTime=System.currentTimeMillis();
      do {
        newRsIndex = getHaMasterRsIndex();
        log.debug("newRsIndex: " + newRsIndex);
      }
      while (newRsIndex == oldRsIndex);
      long endTime=System.currentTimeMillis();
      log.debug("change time: " + (endTime - startTime));
      Assert.assertTrue( (endTime - startTime)/1000 < MIGRATE_TIME);

      resetRsFailCount(oldRsIndex);

      startOneRs(oldRsIndex);

      //mixOpCmdStop();

      log.info(caseName + "===> end");
    } 

  @Ignore
    public void Function_01_ha_version_when_update_table() {

      caseName = "Function_01_ha_version_when_update_table";
      log.info(caseName + "===> start");

      boolean ret = false;
      long mVersion = 0;
      long sVersion = 0;

      int oldMasterRsIndex = getHaMasterRsIndex();
      log.debug("oldMasterRsIndex: " + oldMasterRsIndex);
      int oldSlaveRsIndex = getHaSlaveRsIndex();
      log.debug("oldSlaveRsIndex: " + oldSlaveRsIndex);

      mVersion = getRsCurrentVersion(oldMasterRsIndex);
      sVersion = getRsCurrentVersion(oldSlaveRsIndex);
      log.debug("mVersion: " + mVersion + ", sVersion: " + sVersion);
      Assert.assertEquals(mVersion, sVersion);

      killOneMetaserver(0);
      sleep(CHECK_TIME);

      mVersion = getRsCurrentVersion(oldMasterRsIndex);
      sVersion = getRsCurrentVersion(oldSlaveRsIndex);
      log.debug("mVersion: " + mVersion + ", sVersion: " + sVersion);
      Assert.assertEquals(mVersion, sVersion);

      startOneMetaserver(0);
      sleep(CHECK_TIME);

      mVersion = getRsCurrentVersion(oldMasterRsIndex);
      sVersion = getRsCurrentVersion(oldSlaveRsIndex);
      log.debug("mVersion: " + mVersion + ", sVersion: " + sVersion);
      Assert.assertEquals(mVersion, sVersion);

      log.info(caseName + "===> end");
    }

  @Test
    public void Function_02_ha_version_when_migrate() {
      boolean bRet = false;
      long mVersion = 0;
      long sVersion = 0;

      int oldMasterRsIndex = getHaMasterRsIndex();
      log.debug("oldMasterRsIndex: " + oldMasterRsIndex);
      int oldSlaveRsIndex = getHaSlaveRsIndex();
      log.debug("oldSlaveRsIndex: " + oldSlaveRsIndex);

      mVersion = getRsCurrentVersion(oldMasterRsIndex);
      sVersion = getRsCurrentVersion(oldSlaveRsIndex);
      log.debug("mVersion: " + mVersion + ", sVersion: " + sVersion);
      Assert.assertEquals(mVersion, sVersion);

      bRet = killOneRs(oldMasterRsIndex);
      Assert.assertTrue(bRet);
      bRet = clearOneRs(oldMasterRsIndex);
      Assert.assertTrue(bRet);
      
      migrateHaVip(oldSlaveRsIndex, oldMasterRsIndex);
      sleep(CHECK_TIME);

      int newMasterRsIndex = getHaMasterRsIndex();
      int newSlaveRsIndex = getHaSlaveRsIndex();
      Assert.assertEquals(oldSlaveRsIndex, newMasterRsIndex); 
      Assert.assertEquals(oldMasterRsIndex, newSlaveRsIndex); 

      bRet = killOneRs(newMasterRsIndex);
      Assert.assertTrue(bRet);
      bRet = clearOneRs(newMasterRsIndex);
      Assert.assertTrue(bRet);

      migrateHaVip(newSlaveRsIndex, newMasterRsIndex);
      sleep(CHECK_TIME);

      bRet = startOneRs(oldMasterRsIndex);
      Assert.assertTrue(bRet);

      bRet = startOneRs(oldSlaveRsIndex);
      Assert.assertTrue(bRet);
      sleep(CHECK_TIME);

      mVersion = getRsCurrentVersion(newMasterRsIndex);
      sVersion = getRsCurrentVersion(newSlaveRsIndex);
      log.debug("mVersion: " + mVersion + ", sVersion: " + sVersion);
      Assert.assertEquals(mVersion, sVersion);
   }
}
