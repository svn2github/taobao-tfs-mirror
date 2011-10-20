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
public class mergeRcMetaManager_07_rc_cluster_permission extends RcBaseCase {

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
}
