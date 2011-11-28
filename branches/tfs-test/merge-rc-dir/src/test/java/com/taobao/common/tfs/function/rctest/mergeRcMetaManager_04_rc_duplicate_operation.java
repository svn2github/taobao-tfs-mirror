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
public class mergeRcMetaManager_04_rc_duplicate_operation extends RcBaseCase {

  @Before
    public void setUp(){

      tfsManager= new DefaultTfsManager();
      ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext("tfs.xml"); 
      System.out.println("@@@@@@@@@@@get bean begin");
      tfsManager = (DefaultTfsManager) appContext.getBean("tfsManager");

    }

  @After
    public void tearDown(){
      tfsManager.destroy();
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

      String []sRet = new String[50];
      long oldUsedCapacity = getUsedCapacity(appKey);
      long oldFileCount = getFileCount(appKey);
      int saveTimes = 5;
      //small files
      for(int i = 0; i < saveTimes; i++){
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

      for(int i = 0; i < saveTimes; i++){
        (new File(retFile[i])).delete();
      }

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

      boolean bRet = false;
      String localFile = "2M.jpg";
      String[] retFile = new String[50];

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

      for(int i = 0; i < saveTimes; i++){
        (new File(retFile[i])).delete();
      }

      setDuplicateStatus(appKey, WITHOUT_DUPLICATE);
      sleep(MAX_UPDATE_TIME);

      log.info(caseName + "===> end");
    }
}
