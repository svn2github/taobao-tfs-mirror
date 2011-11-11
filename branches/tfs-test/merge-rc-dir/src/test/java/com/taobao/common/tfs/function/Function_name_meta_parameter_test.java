
package com.taobao.common.tfs.function;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Before;

import com.taobao.gaia.AppCluster;
import com.taobao.gaia.AppServer;
import com.taobao.gaia.KillTypeEnum;

// all case only use one metaServer
public class Function_name_meta_parameter_test extends NameMetaBaseCase {
  
  long appId = 9;
  long userId = 381;
  int userCount = 15;
  static int metaServerIndex = 1;
  String metaServerIp = nameMetaGrid.getCluster(MSINDEX).getServer(metaServerIndex).getIp(); 

  @Test
  public void Function_01_modify_max_cache_size_and_gc_ratio() {
    boolean bRet = false;
    caseName = "Function_01_modify_max_cache_size";
    log.info(caseName + "====> start");

    int [] maxCacheSize = {150, 200, 1025}; // unit MB
    float [] gcRatio = {(float)0.5, (float)0.8, (float)1.0};

    bRet = setClientThreadCount(userCount);
    Assert.assertTrue(bRet);

    for (int i = 0; i < 1; i++) {
      bRet = setMaxCacheSize(metaServerIp, maxCacheSize[i]);
      Assert.assertTrue(bRet);

      bRet = setGcRatio(metaServerIp, gcRatio[i]);
      Assert.assertTrue(bRet);

      bRet = cleanOneMetaServer(metaServerIndex);
      Assert.assertTrue(bRet);

      bRet = startOneMetaserver(metaServerIndex);
      Assert.assertTrue(bRet);

      bRet = createDirCmd(userId);
      Assert.assertTrue(bRet);

      sleep(15);

      int gcPoint = (int)(maxCacheSize[i] * 1024 * 1024 * (1 - gcRatio[i]/2));
      bRet = chkCacheGc(metaServerIp, appId, userId, gcPoint);
      Assert.assertTrue(bRet);

      bRet = createDirCmd(userId);
      Assert.assertTrue(bRet);

      sleep(1000);

      bRet = createDirCmdStop();
      Assert.assertTrue(bRet);

      sleep(15);

      int gcCount = (int)(userCount * gcRatio[i]) + 1;

      // check gc hannpend and gc the right number of users
      bRet = chkGcHappened(metaServerIp, appId, gcCount);
      Assert.assertTrue(bRet);

      bRet = clearDB(appId);
      Assert.assertTrue(bRet);

    }
 
    log.info(caseName + "====> end");
  }

  //@Test
  public void Function_02_modify_sub_dirs_files_count_deep() {
    boolean bRet = false;
    caseName = "Function_02_modify_sub_dirs_files_count_deep";
    log.info(caseName + "====> start");

    int [] maxSubDirsCount = {15, 200, 1024};
    int [] maxSubFilesCount = {30, 200, 1024};
    int [] maxSubDirsDeep = {5, 10, 20};
 
    for (int i = 0; i < 3; i++) {
      bRet = setMaxSubDirsCount(metaServerIp, maxSubDirsCount[i]);
      Assert.assertTrue(bRet);

      bRet = setMaxSubFilesCount(metaServerIp, maxSubFilesCount[i]);
      Assert.assertTrue(bRet);

      bRet = setMaxSubDirsDeep(metaServerIp, maxSubDirsDeep[i]);
      Assert.assertTrue(bRet);

      bRet = cleanOneMetaServer(metaServerIndex);
      Assert.assertTrue(bRet);

      bRet = startOneMetaserver(metaServerIndex);
      Assert.assertTrue(bRet);

      // set client config so that autoGenDir is off 
      bRet = setClientAutoGenDir(CONF_CREATE_DIR, false);
      Assert.assertTrue(bRet);

      bRet = setClientAutoGenDir(CONF_SAVE_SMALL_FILE, false);
      Assert.assertTrue(bRet);

      bRet = genDirTree(metaServerIp, userId, maxSubDirsCount[i], maxSubFilesCount[i], maxSubDirsDeep[i]);
      Assert.assertTrue(bRet);

      bRet = createDirCmd(userId);
      Assert.assertTrue(bRet);

      bRet = createDirMon();
      Assert.assertTrue(bRet);

      bRet = chkRateEnd(SUCCESS_RATE, OPER_CREATE_DIR, userId);
      Assert.assertTrue(bRet);

      bRet = saveSmallFileCmd(userId);
      Assert.assertTrue(bRet);

      bRet = saveSmallFileMon();
      Assert.assertTrue(bRet);

      bRet = chkRateEnd(SUCCESS_RATE, OPER_SAVE_SMALL_FILE, userId);
      Assert.assertTrue(bRet);

      // now will failed
      bRet = genDirTree(metaServerIp, userId, maxSubDirsCount[i], maxSubFilesCount[i], maxSubDirsDeep[i]);
      Assert.assertTrue(bRet);

      bRet = createDirCmd(userId);
      Assert.assertTrue(bRet);

      bRet = createDirMon();
      Assert.assertTrue(bRet);

      bRet = chkRateEnd(FAIL_RATE, OPER_CREATE_DIR, userId);
      Assert.assertTrue(bRet);

      bRet = saveSmallFileCmd(userId);
      Assert.assertTrue(bRet);

      bRet = saveSmallFileMon();
      Assert.assertTrue(bRet);

      bRet = chkRateEnd(FAIL_RATE, OPER_SAVE_SMALL_FILE, userId);
      Assert.assertTrue(bRet);

      bRet = clearDB(appId);
      Assert.assertTrue(bRet);

    }

    log.info(caseName + "====> end");
  }

  //@Test
  public void Function_03_modify_max_mutex_size() {
    boolean bRet = false;
    caseName = "Function_03_modify_max_mutex_size";
    log.info(caseName + "====> start");

    int [] maxMutexSize = {5, 10, 20};
    int threadCount = 20;
 
    for (int i = 0; i < 3; i++) {
      bRet = setMaxMutexSize(metaServerIp, maxMutexSize[i]);
      Assert.assertTrue(bRet);

      bRet = cleanOneMetaServer(metaServerIndex);
      Assert.assertTrue(bRet);

      bRet = startOneMetaserver(metaServerIndex);
      Assert.assertTrue(bRet);

      bRet = setClientThreadCount(threadCount);
      Assert.assertTrue(bRet);

      bRet = createDirCmd(userId);
      Assert.assertTrue(bRet);

      sleep(120);

      bRet = createDirCmdStop();
      Assert.assertTrue(bRet);

      bRet = getMutexResult(metaServerIp, maxMutexSize[i]);
      Assert.assertTrue(bRet);

    }

    log.info(caseName + "====> end");
  }

  //@Test
  public void Function_04_modify_free_list_count() {
    boolean bRet = false;
    caseName = "Function_04_modify_free_list_count";
    log.info(caseName + "====> start");

    int [] freeListCount = {256, 512};
    int threadCount = 20;
 
    for (int i = 0; i < 2; i++) {
      bRet = setFreeListCount(metaServerIp, freeListCount[i]);
      Assert.assertTrue(bRet);

      bRet = cleanOneMetaServer(metaServerIndex);
      Assert.assertTrue(bRet);

      bRet = startOneMetaserver(metaServerIndex);
      Assert.assertTrue(bRet);

      bRet = setClientThreadCount(threadCount);
      Assert.assertTrue(bRet);

      bRet = createDirCmd(userId);
      Assert.assertTrue(bRet);

      sleep(15);

      bRet = createDirCmdStop();
      Assert.assertTrue(bRet);

      bRet = killOneMetaserver(metaServerIndex);
      Assert.assertTrue(bRet);

      bRet = chkFreeListCap(metaServerIp, freeListCount[i]);
      Assert.assertTrue(bRet);

    }

    log.info(caseName + "====> end");
  }

  //@Test
  public void Function_05_modify_max_spool_size() {
    boolean bRet = false;
    caseName = "Function_05_modify_max_spool_size";
    log.info(caseName + "====> start");

    int [] maxSpoolSize = {10, 20, 50};
 
    for (int i = 0; i < 3; i++) {
      bRet = setMaxSpoolSize(metaServerIp, maxSpoolSize[i]);
      Assert.assertTrue(bRet);

      bRet = cleanOneMetaServer(metaServerIndex);
      Assert.assertTrue(bRet);

      bRet = startOneMetaserver(metaServerIndex);
      Assert.assertTrue(bRet);

      bRet = chkDbPoolSize(metaServerIp, maxSpoolSize[i]);
      Assert.assertTrue(bRet);

    }
}

  @After
  public void tearDown() {

    boolean bRet = false;

    bRet = clearDB(appId);
    Assert.assertTrue(bRet);

    bRet = mvLogFile(metaServerIp, MS_LOG, caseName);
    Assert.assertTrue(bRet);

    bRet = cleanOneMetaServer(metaServerIndex);
    Assert.assertTrue(bRet);

    bRet = startOneMetaserver(metaServerIndex);
    Assert.assertTrue(bRet);

  }

  @Before
  public void setUp() {
  }

  @BeforeClass
  public static void setUpOnce() throws Exception {

    NameMetaBaseCase.setUpOnce();

    boolean bRet = false;

    /* kill the other meta servers */
    AppCluster csCluster = nameMetaGrid.getCluster(MSINDEX);
    for(int iLoop = 0; iLoop < csCluster.getServerList().size(); iLoop ++) {
        if (iLoop != metaServerIndex) {
          AppServer cs = csCluster.getServer(iLoop);
          bRet = cs.stop(KillTypeEnum.NORMALKILL, WAIT_TIME);
          if (bRet == false) {
              break;
          }
        }
    }

  }

  @AfterClass
  public static void tearDownOnce() throws Exception {
  }

}

