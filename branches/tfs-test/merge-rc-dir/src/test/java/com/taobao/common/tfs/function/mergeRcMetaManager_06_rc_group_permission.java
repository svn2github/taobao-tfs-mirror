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
public class mergeRcMetaManager_06_rc_group_permission extends RcBaseCase {

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
}
