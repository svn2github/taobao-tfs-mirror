
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

public class Function_name_meta_cache_test extends NameMetaBaseCase{
  
  long appId = 9;
  long userId = 381;
  static int metaServerIndex = 1;
  String metaServerIp = nameMetaGrid.getCluster(MSINDEX).getServer(metaServerIndex).getIp(); 

  //@Test
  public void Function_01_happy_path() {
    boolean bRet = false;
    caseName = "Function_01_happy_path";
    log.info(caseName + "====> start");

    bRet = createDirCmd(userId);
    Assert.assertTrue(bRet);

    sleep(30);

    bRet = createDirCmdStop();
    Assert.assertTrue(bRet);

    bRet = queryDB(CREATED_DIR_LIST, userId);
    Assert.assertTrue(bRet);

    log.info(caseName + "====> end");
  }

  @Test
  public void Function_02_clear_db_ls_small_file() {
    boolean bRet = false;
    caseName = "Function_02_clear_db_ls_small_file";
    log.info(caseName + "====> start");

    userId = 407;
    bRet = saveSmallFileCmd(userId);
    Assert.assertTrue(bRet);

    sleep(30);

    bRet = saveSmallFileCmdStop();
    Assert.assertTrue(bRet);

    bRet = clearDB(SAVED_FILE_LIST, userId);
    Assert.assertTrue(bRet);

    bRet = mvLogFile(CLIENT_IP, CLIENT_LOG + caseName, "saveSmallFile");
    Assert.assertTrue(bRet);

    bRet = lsFileCmd(userId);
    Assert.assertTrue(bRet);

    bRet = lsFileMon();
    Assert.assertTrue(bRet);

    bRet = chkRateEnd(SUCCESS_RATE, OPER_LS_FILE, userId);
    Assert.assertTrue(bRet);

    bRet = mvLogFile(CLIENT_IP, CLIENT_LOG + caseName, "lsFile");
    Assert.assertTrue(bRet);

    log.info(caseName + "====> end");
  }

  @Test
  public void Function_03_clear_db_read_small_file() {
    boolean bRet = false;
    caseName = "Function_03_clear_db_read_small_file";
    log.info(caseName + "====> start");

    userId = 408;
    bRet = saveSmallFileCmd(userId);
    Assert.assertTrue(bRet);

    sleep(30);

    bRet = saveSmallFileCmdStop();
    Assert.assertTrue(bRet);

    bRet = clearDB(SAVED_FILE_LIST, userId);
    Assert.assertTrue(bRet);

    bRet = mvLogFile(CLIENT_IP, CLIENT_LOG + caseName, "saveSmallFile");
    Assert.assertTrue(bRet);

    bRet = fetchFileCmd(userId);
    Assert.assertTrue(bRet);

    bRet = fetchFileMon();
    Assert.assertTrue(bRet);

    bRet = chkRateEnd(SUCCESS_RATE, OPER_FETCH_FILE, userId);
    Assert.assertTrue(bRet);

    bRet = mvLogFile(CLIENT_IP, CLIENT_LOG + caseName, "fetchSmallFile");
    Assert.assertTrue(bRet);

    log.info(caseName + "====> end");
  }

  @Test
  public void Function_04_clear_db_read_large_file() {
    boolean bRet = false;
    caseName = "Function_04_clear_db_read_large_file";
    log.info(caseName + "====> start");

    userId = 409;
    bRet = saveLargeFileCmd(userId);
    Assert.assertTrue(bRet);

    sleep(720); // speed: about 1 files(2.1G)/120s

    bRet = saveLargeFileCmdStop();
    Assert.assertTrue(bRet);

    bRet = clearDB(SAVED_FILE_LIST, userId);
    Assert.assertTrue(bRet);

    bRet = mvLogFile(CLIENT_IP, CLIENT_LOG + caseName, "saveLargeFile");
    Assert.assertTrue(bRet);

    bRet = fetchFileCmd(userId);
    Assert.assertTrue(bRet);

    bRet = fetchFileMon();
    Assert.assertTrue(bRet);

    bRet = chkRateEnd(FAIL_RATE, OPER_FETCH_FILE, userId);
    Assert.assertTrue(bRet);

    bRet = mvLogFile(CLIENT_IP, CLIENT_LOG + caseName, "fetchLargeFile");
    Assert.assertTrue(bRet);

    log.info(caseName + "====> end");
  }

  @Test
  // only one metaServer
  public void Function_05_check_cache_size() {
    boolean bRet = false;
    caseName = "Function_05_check_cache_size";
    log.info(caseName + "====> start");

    long userIdOne = 381;

    int cacheSize = 64;
    bRet = setMaxCacheSize(metaServerIp, cacheSize);
    Assert.assertTrue(bRet);

    float gcRatio = (float)0.9;
    bRet = setGcRatio(metaServerIp, gcRatio);
    Assert.assertTrue(bRet);

    bRet = restartOneMetaServer(metaServerIndex);
    Assert.assertTrue(bRet);

    bRet = createDirCmd(userIdOne);
    Assert.assertTrue(bRet);

    sleep(15);

    int gcPoint = (int)(cacheSize * 1024 * 1024 * (1 - gcRatio/2));
    bRet = chkCacheGc(metaServerIp, appId, userIdOne, gcPoint);
    Assert.assertTrue(bRet);

    long userIdTwo = 183;
    bRet = createDirCmd(userIdTwo);
    Assert.assertTrue(bRet);

    sleep(540); // write more than increment

    bRet = createDirCmdStop();
    Assert.assertTrue(bRet);

    bRet = chkGcHappened(metaServerIp, appId, userIdOne);
    Assert.assertTrue(bRet);
 
    int usedSize = getUsedSize(metaServerIp);
    log.debug("get used cache size: " + usedSize);
    Assert.assertTrue(usedSize < (gcPoint));

    log.info(caseName + "====> end");
  }

  @Test
  // make sure there is only one metaServer
  public void Function_06_check_lru() {
    boolean bRet = false;
    caseName = "Function_06_check_lru";
    log.info(caseName + "====> start");

    int cacheSize = 64;
    bRet = setMaxCacheSize(metaServerIp, cacheSize);
    Assert.assertTrue(bRet);

    float gcRatio = (float)0.9;
    bRet = setGcRatio(metaServerIp, gcRatio);
    Assert.assertTrue(bRet);

    bRet = restartOneMetaServer(metaServerIndex);
    Assert.assertTrue(bRet);

    // real write
    int userCount = 3;
    long[] users = new long[userCount];
    for (int i = 0; i < userCount; i++) {
      long userId = 381 + i;
      users[i] = userId;

      // write file
      bRet = saveSmallFileCmd(userId);
      Assert.assertTrue(bRet);

      sleep(120);

      bRet = saveSmallFileCmdStop();
      Assert.assertTrue(bRet);

      bRet = mvLogFile(CLIENT_IP, CLIENT_LOG + caseName, "saveSmallFile." + userId);
      Assert.assertTrue(bRet);

      // gc not happened
      bRet = chkGcHappened(metaServerIp, appId, userId);
      Assert.assertFalse(bRet);
   
      // make suer user 1's files are in cache
      bRet = clearDB(SAVED_FILE_LIST, userId);
      Assert.assertTrue(bRet);

      bRet = lsFileCmd(userId);
      Assert.assertTrue(bRet);

      bRet = lsFileMon();
      Assert.assertTrue(bRet);

      bRet = chkRateEnd(SUCCESS_RATE, OPER_LS_FILE, userId);
      Assert.assertTrue(bRet);

      bRet = mvLogFile(CLIENT_IP, CLIENT_LOG + caseName, "lsFile." + userId);
      Assert.assertTrue(bRet);

    }

    // the last user use createDir until cache gc point is reached
    long userIdLast = users[userCount - 1];

    bRet = createDirCmd(userIdLast);
    Assert.assertTrue(bRet);

    sleep(15);

    int gcPoint = (int)(cacheSize * 1024 * 1024 * (1 - gcRatio/2));
    bRet = waitUntilCacheGc(metaServerIp, gcPoint);  // gc will happen within 360 seconds
    Assert.assertTrue(bRet);

    sleep(540);  // 540 > 360 so that now gc must have happened

    bRet = createDirCmdStop();
    Assert.assertTrue(bRet);

    sleep(60);

    int gcCount = (int)(userCount * gcRatio) + 1;

    // get users being gc
    long gcUsers[] = new long[gcCount];
    for (int i = 0; i < gcCount; i++) {
      gcUsers[i] = users[i];
    }

    // check gc hannpend and happening right in sequence
    bRet = chkGcHappened(metaServerIp, appId, gcUsers);
    Assert.assertTrue(bRet);
    
    // now these users' files are not in cache
    for (int i = 0; i < gcCount; i++) {
      long userId = gcUsers[i];
 
      bRet = lsFileCmd(userId);
      Assert.assertTrue(bRet);

      bRet = lsFileMon();
      Assert.assertTrue(bRet);

      bRet = chkRateEnd(FAIL_RATE, OPER_LS_FILE, userId);
      Assert.assertTrue(bRet);
    }

    log.info(caseName + "====> end");
  }

  @After
  public void tearDown() {

    boolean bRet = false;

    bRet = clearDB(appId);
    Assert.assertTrue(bRet);

    bRet = mvLogFile(metaServerIp, MS_LOG, caseName);
    Assert.assertTrue(bRet);

    bRet = cleanOneMetaServerForce(metaServerIndex);
    Assert.assertTrue(bRet);

    bRet = startOneMetaServer(metaServerIndex);
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

