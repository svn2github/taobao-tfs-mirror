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
public class mergeRcMetaManager_05_rc_quote_calc extends RcBaseCase {

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
    public void Function_01_quote_less_than_max(){

      caseName = "Function_01_quote_less_than_max";
      log.info(caseName + "===> start");

      resetCurQuote(appKey);
      sleep(MAX_STAT_TIME);
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
      expect_quote = (long) 15*1024*1024;
      actual_quote = (old_quote - new_quote);
      log.debug("@@ expect quote: " + expect_quote + ", actual quote: " + actual_quote);
      Assert.assertEquals(expect_quote, actual_quote);

      // large file
      name_list.clear();
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
      actual_quote = (old_quote - new_quote);
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
      sleep(MAX_STAT_TIME);
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
}
