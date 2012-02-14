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
public class mergeRcMetaManager_08_rc_server_change extends RcBaseCase {

  @Before
    public void setUp(){

      tfsManager= new DefaultTfsManager();
      ClassPathXmlApplicationContext appContext =
      new ClassPathXmlApplicationContext(new String[] { "tfs.xml" }); 
      System.out.println("@@@@@@@@@@@get bean begin");
      tfsManager = (DefaultTfsManager) appContext.getBean("tfsManager");

    }

  @After
    public void tearDown(){
      tfsManager.destroy();
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
      log.debug("@@ count: " + count + ", newCapacity: " + newCapacity + ", oldCapacity: " + oldCapacity);
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
      log.debug("@@ count: " + count + ", newCapacity: " + newCapacity + ", oldCapacity: " + oldCapacity);
      ret = (count >= (newCapacity - oldCapacity));
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
      log.debug("@@ count: " + count + ", newCapacity: " + newCapacity + ", oldCapacity: " + oldCapacity);
      Assert.assertTrue(count > (newCapacity - oldCapacity));

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
      log.debug("@@ count: " + count + ", newCapacity: " + newCapacity + ", oldCapacity" + oldCapacity);
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
